"""
Shared base for the "one JSON API per company" ATS platforms (Oracle, UKG, Paylocity, ...).

A platform scraper only says how to list one company's postings and how to turn a posting into a job;
this class does the rest the same way for every platform:
  - paced, retried requests (one request budget per host; a 429 slows every worker on that host)
  - the post-time window (details are fetched only for postings inside it; with no window they are skipped)
  - de-duplication, and a listing proof per company (listing_proof.py) when the list was read to the end

Companies come from a CSV with the columns name, slug, url (the slug is whatever the platform needs, e.g.
"host/site" for Oracle), like the Workday and Teamtailor lists.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit

import httpx

from listing_proof import proof_for
from scraper_runtime import bounded_map, paced_request, retry_after_seconds
from scraper_utils import enrich_raw_job

log = logging.getLogger(__name__)

POST_TIMES = {"hour": timedelta(hours=1), "day": timedelta(days=1), "week": timedelta(weeks=1),
              "month": timedelta(days=30), "year": timedelta(days=365)}


class NotFound(Exception):
    """The company's site does not exist (HTTP 404)."""


class FetchFailed(Exception):
    """A request could not be completed after its retries."""


@dataclass
class Listing:
    """One company's postings. complete=True only when its list was read to the end."""
    postings: list[dict[str, Any]] = field(default_factory=list)
    complete: bool = True


def parse_date(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


class PlatformScraper:
    BOARD = ""                 # source_board and listing-proof board, e.g. "Oracle"
    RATE = 20.0                # requests per second to one host
    RETRIES = 4
    TIMEOUT = 25.0

    def __init__(self, companies: list[dict[str, str]]):
        key = self.BOARD.upper()
        self.companies = [c for c in companies if (c.get("slug") or "").strip()]
        self.company_concurrency = int(os.getenv(f"{key}_COMPANY_CONCURRENCY", "40"))
        self.detail_concurrency = int(os.getenv(f"{key}_DETAIL_CONCURRENCY", "20"))
        self.stats: dict[str, int] = {}

    # ----- what a platform provides -------------------------------------------------------------------------
    async def list_company(self, client: httpx.AsyncClient, company: dict[str, str]) -> Listing:
        raise NotImplementedError

    def posting_url(self, company: dict[str, str], posting: dict[str, Any]) -> str:
        """The URL the saved job carries (its proof key is read from it)."""
        raise NotImplementedError

    def posted_at(self, posting: dict[str, Any]) -> datetime | None:
        return None

    async def fetch_detail(self, client: httpx.AsyncClient, company: dict[str, str],
                           posting: dict[str, Any]) -> dict[str, Any] | None:
        """Extra fields (description, ...) for one posting; None when the platform lists everything already."""
        return None

    def to_job(self, company: dict[str, str], posting: dict[str, Any], detail: dict[str, Any] | None) -> dict[str, Any]:
        """A job dict with at least job_title, location, description; the base adds the rest."""
        raise NotImplementedError

    # ----- shared plumbing ----------------------------------------------------------------------------------
    async def get_json(self, client: httpx.AsyncClient, url: str, **kwargs) -> Any:
        host = urlsplit(url).netloc
        for attempt in range(self.RETRIES):
            try:
                response = await paced_request(client, "get", url, board=f"{self.BOARD.upper()}@{host}",
                                               rate=self.RATE, timeout=self.TIMEOUT, **kwargs)
            except httpx.HTTPError as exc:
                self._count(f"error_{type(exc).__name__}")
                response = None
            if response is not None:
                if response.status_code == 404:
                    raise NotFound(url)
                if response.status_code < 300:
                    try:
                        return response.json()
                    except ValueError:
                        self._count("invalid_json")
                else:
                    self._count(f"status_{response.status_code}")
            if attempt + 1 < self.RETRIES:
                delay = retry_after_seconds(response.headers.get("Retry-After")) if response is not None else None
                await asyncio.sleep(min(delay if delay is not None else 0.5 * 2 ** attempt + random.random(), 30))
        raise FetchFailed(url)

    def _count(self, name: str) -> None:
        self.stats[name] = self.stats.get(name, 0) + 1

    def _window(self) -> timedelta | None:
        # pipeline.py sets the run's window in the platform module's SEARCH_ATTRIBUTES; then the environment
        attributes = getattr(sys.modules.get(type(self).__module__), "SEARCH_ATTRIBUTES", {}) or {}
        return POST_TIMES.get((attributes.get("post_time") or os.getenv(f"{self.BOARD.upper()}_POST_TIME")
                               or os.getenv("API_POST_TIME") or os.getenv("DEFAULT_POST_TIME") or "any").lower())

    async def _fetch_company(self, client: httpx.AsyncClient, company: dict[str, str]) -> list[dict[str, Any]]:
        name, proof = company["name"], proof_for(self.BOARD)
        try:
            listing = await self.list_company(client, company)
        except NotFound:
            proof.partial(name)
            self._count("companies_not_found")
            return []
        except Exception as exc:
            proof.partial(name)
            self._count("companies_failed")
            log.warning("%s: %s could not be listed: %s", self.BOARD, name, exc)
            return []

        if listing.complete:
            proof.listed(name, (self.posting_url(company, p) for p in listing.postings))
        else:
            proof.partial(name)
            self._count("companies_incomplete")

        window = self._window()
        cutoff = datetime.now(timezone.utc) - window if window else None
        wanted = []
        for posting in listing.postings:
            posted = self.posted_at(posting)
            if cutoff and posted and posted < cutoff:
                continue  # listed (the proof has it) but older than the window: not saved again
            wanted.append(posting)

        async def build(posting):
            try:
                return await build_one(posting)
            except Exception as exc:  # one bad posting must not lose the company's other jobs
                self._count("jobs_failed")
                log.debug("%s: %s: posting skipped: %s", self.BOARD, name, exc)
                return None

        async def build_one(posting):
            # Details only inside a window: with none (post time "any") they would be one request per job
            detail = None
            if window:
                try:
                    detail = await self.fetch_detail(client, company, posting)
                except Exception as exc:
                    self._count("details_failed")
                    log.debug("%s: detail failed: %s", self.BOARD, exc)
            job = self.to_job(company, posting, detail)
            url = self.posting_url(company, posting)
            job.update({
                "id": hashlib.md5(f"{self.BOARD.lower()}:{company['slug']}:{url}".encode()).hexdigest(),
                "company": name, "job_url": url, "apply_url": job.get("apply_url") or url,
                "source_board": self.BOARD, "scraper_type": "api", "also_on": [],
                "scraped_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            })
            posted = self.posted_at(posting)
            if posted:
                job["posted_at"] = posted.isoformat()
            return enrich_raw_job(job)

        return [job for job in await bounded_map(build, wanted, self.detail_concurrency) if job]

    async def scrape(self, write_queue: "asyncio.Queue | None" = None) -> list[dict[str, Any]]:
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
                   "Accept": "application/json"}
        async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=self.TIMEOUT,
                                     limits=httpx.Limits(max_connections=200, max_keepalive_connections=80)) as client:
            results = await bounded_map(lambda c: self._fetch_company(client, c), self.companies, self.company_concurrency)

        jobs, seen = [], set()
        for batch in results:
            for job in batch or []:
                if job["job_url"] in seen:
                    continue
                seen.add(job["job_url"])
                jobs.append(job)
        if write_queue is not None:
            await write_queue.put(jobs)
        log.info("%s: %d companies, %d jobs; %s", self.BOARD, len(self.companies), len(jobs), self.stats)
        return jobs
