"""
Oracle Recruiting Cloud (Oracle Fusion / ORC) career sites, e.g. JPMorgan, Goldman Sachs, Oracle itself.

The company's slug is "host/site", like "jpmc.fa.oraclecloud.com/CX_1001". Both endpoints are public JSON:
the requisition list (carries no description) and one requisition's detail (description, fetched only for
postings inside the post-time window). Paging offset goes INSIDE the finder clause: a top-level &offset=
is ignored and would return page one forever.
"""
from __future__ import annotations

import html
import os
import re
import sys
from typing import Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from platform_scraper import Listing, PlatformScraper, parse_date  # noqa: E402

PAGE = 100
SEARCH_ATTRIBUTES = {"post_time": ""}  # pipeline.py sets the run's window here
WORK_MODES = {"ORA_REMOTE": "Remote", "ORA_HYBRID": "Hybrid", "ORA_ON_SITE": "Onsite"}
_TAG = re.compile(r"<[^>]+>")


def split_slug(slug: str) -> tuple[str, str]:
    host, _, site = slug.strip().partition("/")
    if not host or not site:
        raise ValueError(f'Oracle slug {slug!r} must be "host/site"')
    return host, site


def text_of(fragment: str | None) -> str:
    return re.sub(r"\s+", " ", html.unescape(_TAG.sub(" ", fragment or ""))).strip()


class DirectOracleScraper(PlatformScraper):
    BOARD = "Oracle"
    RATE = 15.0

    async def list_company(self, client, company) -> Listing:
        host, site = split_slug(company["slug"])
        postings: dict[str, dict[str, Any]] = {}
        total, offset = 0, 0
        while True:
            url = (f"https://{host}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
                   f"?onlyData=true&expand=requisitionList.secondaryLocations,requisitionList.workLocation"
                   f"&finder=findReqs;siteNumber={site},limit={PAGE},offset={offset},sortBy=POSTING_DATES_DESC")
            items = (await self.get_json(client, url)).get("items") or []
            if not items:
                break
            page = items[0]
            total = int(page.get("TotalJobsCount") or total)
            batch = page.get("requisitionList") or []
            if not batch:
                break
            for posting in batch:
                postings[str(posting["Id"])] = posting
            offset += len(batch)
            if offset >= total:
                break
        # A live board shifts while it is read: a few jobs either way is a complete read, a big gap is not
        complete = total - len(postings) <= max(3, total // 200)
        return Listing(list(postings.values()), complete)

    def posting_url(self, company, posting) -> str:
        host, site = split_slug(company["slug"])
        return f"https://{host}/hcmUI/CandidateExperience/en/sites/{site}/job/{posting['Id']}"

    def posted_at(self, posting):
        return parse_date(posting.get("PostedDate"))

    async def fetch_detail(self, client, company, posting):
        host, site = split_slug(company["slug"])
        url = (f"https://{host}/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails"
               f'?onlyData=true&expand=all&finder=ById;Id="{posting["Id"]}",siteNumber={site}')
        items = (await self.get_json(client, url)).get("items") or []
        return items[0] if items else None

    def to_job(self, company, posting, detail) -> dict[str, Any]:
        detail = detail or {}
        body = " ".join(detail.get(k) or "" for k in ("ExternalDescriptionStr", "ExternalResponsibilitiesStr",
                                                      "ExternalQualificationsStr"))
        if not text_of(body):  # some templates keep the real text in these two
            body = " ".join(detail.get(k) or "" for k in ("CorporateDescriptionStr", "OrganizationDescriptionStr"))
        if not text_of(body):  # no detail (outside a window): the list's snippet is all there is
            body = posting.get("ShortDescriptionStr") or ""
        description = text_of(body)
        mode = WORK_MODES.get(posting.get("WorkplaceTypeCode") or "", "")
        schedule = (detail.get("JobSchedule") or posting.get("JobSchedule") or "").strip().lower()
        return {
            "job_title": (posting.get("Title") or "").strip(),
            "location": (posting.get("PrimaryLocation") or "").strip(),
            "description": description,
            "description_raw": body,
            "work_mode": mode, "is_remote": mode == "Remote" if mode else None,
            "job_type": {"full time": "full_time", "part time": "part_time"}.get(schedule, ""),
            "department": posting.get("JobFamily") or "",
        }
