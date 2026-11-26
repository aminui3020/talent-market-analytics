#!/usr/bin/env python3
"""
Universal Multi-Platform ATS Live Job Extractor.

Extracts live job postings and precise posting timestamps across all major ATS platforms:
- Greenhouse
- Lever
- Ashby
- Workable
- SmartRecruiters
- Recruitee
- Breezy HR
- Rippling
- BambooHR
- Pinpoint

Features:
- High-concurrency async fetching with per-host rate limiting (prevents 429s).
- Direct extraction of exact job publication & update timestamps.
- Normalized ISO 8601 UTC timestamps + raw timestamp preservation.
- Resilient SQLite storage + automated CSV export.
- Can ingest single slug, platform-specific CSVs, or master multi-ATS CSVs.
"""

import sys
import os
import csv
import json
import asyncio
import argparse
import sqlite3
import html
import re
import time
import random
import email.utils
import glob
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

# Ensure local packages (scrapers, matching) are importable
_CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if _CURR_DIR not in sys.path:
    sys.path.insert(0, _CURR_DIR)

try:
    from scrapers.job_extraction.engine import extract_job
    from scrapers.job_extraction.jd_enricher import parse_salary, detect_work_mode
    from matching.jd_facts import extract_jd_facts
    HAS_ENRICHMENT = True
except Exception as e:
    HAS_ENRICHMENT = False
    print(f"Notice: JD Enrichment engine disabled: {e}", file=sys.stderr)


# US State identification regex & sets
US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri",
    "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey",
    "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
    "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
    "DC": "District of Columbia", "PR": "Puerto Rico",
}
US_STATE_CODES = set(US_STATES.keys())
US_STATE_NAMES = {name.lower(): code for code, name in US_STATES.items()}

US_CITIES_SAMPLE = {
    "new york", "san francisco", "los angeles", "chicago", "austin", "seattle", "boston",
    "denver", "atlanta", "dallas", "houston", "san diego", "san jose", "miami", "philadelphia",
    "phoenix", "portland", "salt lake city", "raleigh", "charlotte", "minneapolis", "nashville"
}

NON_US_COUNTRIES = {
    "united kingdom", "london", "canada", "germany", "berlin", "france", "paris",
    "spain", "madrid", "barcelona", "italy", "milan", "rome", "india", "bangalore",
    "hyderabad", "pune", "mumbai", "delhi", "australia", "sydney", "melbourne",
    "netherlands", "amsterdam", "brazil", "poland", "warsaw", "ukraine", "kyiv",
    "denmark", "copenhagen", "sweden", "stockholm", "finland", "helsinki",
    "ireland", "dublin", "japan", "tokyo", "singapore", "mexico", "austria", "vienna",
    "belgium", "antwerp", "brussels", "greece", "athens", "portugal", "lisbon",
    "switzerland", "zurich", "norway", "oslo", "czech", "prague", "philippines",
    "manila", "argentina", "buenos aires", "colombia", "bogota", "chile", "santiago",
    "israel", "tel aviv", "new zealand", "auckland", "south africa", "emea", "apac", "latam"
}


def is_us_location(loc_input: Any, extra_text: str = "") -> bool:
    """
    Robust heuristic to determine if a job is US-based or US-Remote.
    Checks location string, job title, and office names.
    """
    if isinstance(loc_input, dict):
        loc_str = " ".join(str(v) for v in loc_input.values() if v)
    else:
        loc_str = str(loc_input or "")

    clean = loc_str.lower().strip()
    combined = f"{loc_str} {extra_text}".lower().strip()

    # If explicitly foreign and doesn't mention US
    has_foreign = any(re.search(r"\b" + re.escape(fc) + r"\b", combined) for fc in NON_US_COUNTRIES)
    has_explicit_us = any(u in combined for u in ["united states", ", us", "usa", "(us)", "- us", "us -"])

    if has_foreign and not has_explicit_us:
        return False

    # Direct US indicators
    if clean in ("us", "usa", "united states", "united states of america") or clean.startswith("us-"):
        return True
    if has_explicit_us or re.search(r"\bus-[a-z]{2}\b", combined):
        return True
    if any(tag in combined for tag in ["us remote", "remote - us", "remote, us", "remote (us)", "remote us"]):
        return True

    # Generic remote for US companies (without non-US tags)
    if clean in ("remote", "work from home", "anywhere"):
        return True

    # State code pattern e.g. "San Francisco, CA" or "Austin, TX"
    match = re.search(r",\s*([a-zA-Z]{2})(?:\s+|$|\b)", combined)
    if match:
        code = match.group(1).upper()
        if code in US_STATE_CODES:
            return True

    # State name match
    for st_name in US_STATE_NAMES:
        if re.search(r"\b" + re.escape(st_name) + r"\b", combined):
            return True

    # Major US tech cities
    for city in US_CITIES_SAMPLE:
        if re.search(r"\b" + re.escape(city) + r"\b", combined):
            return True

    return False


INDIA_CITIES_SAMPLE = {
    "bengaluru", "bangalore", "hyderabad", "pune", "mumbai", "delhi", "new delhi",
    "gurgaon", "gurugram", "noida", "greater noida", "chennai", "kolkata", "ahmedabad",
    "jaipur", "kochi", "coimbatore", "chandigarh", "indore", "surat", "vadodara",
    "nagpur", "visakhapatnam", "vizag", "thiruvananthapuram", "trivandrum", "bhopal",
    "patna", "lucknow", "bhubaneswar", "mysore", "mysuru", "faridabad", "ghaziabad",
    "navi mumbai", "secunderabad", "mohali"
}

INDIA_STATES = {
    "karnataka", "telangana", "maharashtra", "tamil nadu", "delhi", "haryana",
    "uttar pradesh", "gujarat", "west bengal", "kerala", "rajasthan",
    "madhya pradesh", "punjab", "andhra pradesh", "odisha", "bihar", "assam", "goa",
    "in-ka", "in-mh", "in-ts", "in-tn", "in-dl", "in-hr", "in-up", "in-gj", "in-wb", "in-kl"
}


def is_india_location(loc_input: Any, extra_text: str = "") -> bool:
    """
    Robust heuristic to determine if a job is India-based or India-Remote.
    Checks location string, job title, and office names.
    """
    if isinstance(loc_input, dict):
        loc_str = " ".join(str(v) for v in loc_input.values() if v)
    else:
        loc_str = str(loc_input or "")

    clean = loc_str.lower().strip()
    combined = f"{loc_str} {extra_text}".lower().strip()

    # Direct India indicators
    if clean in ("in", "ind", "india") or clean.startswith("in-") or clean.endswith("-in"):
        return True
    if any(tag in combined for tag in [
        "india", ", in", "(in)", "- in", "in -", "in, ", "/ in", "remote - india",
        "india remote", "remote, india", "remote (india)", "anywhere in india"
    ]):
        return True

    # Major Indian tech cities
    for city in INDIA_CITIES_SAMPLE:
        if re.search(r"\b" + re.escape(city) + r"\b", combined):
            return True

    # Major Indian states
    for st in INDIA_STATES:
        if re.search(r"\b" + re.escape(st) + r"\b", combined):
            return True

    return False


def parse_to_iso(val: Any) -> str:
    """Standardizes various ATS timestamp formats into an ISO 8601 UTC string."""
    if not val:
        return ""
    # Epoch milliseconds (Lever style)
    if isinstance(val, (int, float)):
        try:
            return datetime.fromtimestamp(val / 1000.0, tz=timezone.utc).isoformat()
        except Exception:
            return ""

    s = str(val).strip()
    if not s:
        return ""

    # Recruitee format: "2026-09-23 19:02:33 UTC"
    if s.endswith(" UTC"):
        s = s[:-4].replace(" ", "T") + "Z"

    # Try ISO parser
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        # Match YYYY-MM-DD
        m = re.match(r"^(\d{4}-\d{2}-\d{2})", s)
        if m:
            try:
                dt = datetime.fromisoformat(m.group(1)).replace(tzinfo=timezone.utc)
                return dt.isoformat()
            except Exception:
                pass
        return s


def clean_html(raw_html: str) -> str:
    """Unescapes HTML entities and strips basic tags for plaintext storage."""
    if not raw_html:
        return ""
    unescaped = html.unescape(raw_html)
    # Strip scripts & styles
    clean = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", unescaped, flags=re.DOTALL | re.IGNORECASE)
    # Convert <br>, <p>, <li> to newlines
    clean = re.sub(r"<(?:br|p|li|tr)[^>]*>", "\n", clean, flags=re.IGNORECASE)
    # Strip remaining HTML tags
    clean = re.sub(r"<[^>]+>", "", clean)
    # Collapse multiple whitespaces
    clean = re.sub(r"[ \t]+", " ", clean)
    clean = re.sub(r"\n\s*\n+", "\n\n", clean)
    return clean.strip()


class AsyncHTTPClient:
    """Asynchronous HTTP client with per-host token pacing, circuit-breaker backoff on 429, and exponential jitter."""
    def __init__(
        self,
        concurrency: int = 80,
        per_host_concurrency: int = 15,
        min_host_interval: float = 0.03,  # Up to 33 req/sec per host
        timeout: float = 12.0,
    ):
        self.semaphore = asyncio.Semaphore(concurrency)
        self.per_host_concurrency = per_host_concurrency
        self.min_host_interval = min_host_interval
        self.timeout = timeout
        self._host_semaphores: Dict[str, asyncio.Semaphore] = {}
        self._host_locks: Dict[str, asyncio.Lock] = {}
        self._host_last_request_time: Dict[str, float] = {}
        self._host_cooldown_until: Dict[str, float] = {}
        self._session = None

    def _get_host(self, url: str) -> str:
        try:
            return (urlparse(url).hostname or "default").lower()
        except Exception:
            return "default"

    def _get_host_sem(self, host: str) -> asyncio.Semaphore:
        if host not in self._host_semaphores:
            self._host_semaphores[host] = asyncio.Semaphore(self.per_host_concurrency)
        return self._host_semaphores[host]

    def _get_host_lock(self, host: str) -> asyncio.Lock:
        if host not in self._host_locks:
            self._host_locks[host] = asyncio.Lock()
        return self._host_locks[host]

    async def _wait_for_host(self, host: str):
        """Enforces host-wide cooldowns on 429 and paces inter-request intervals."""
        # 1. Cooldown check: if host encountered a 429/503, wait until cooldown expires (outside lock)
        now = time.monotonic()
        cooldown = self._host_cooldown_until.get(host, 0.0)
        if now < cooldown:
            wait_s = cooldown - now + random.uniform(0.1, 0.3)
            await asyncio.sleep(wait_s)

        # 2. Minimum inter-request pacing
        lock = self._get_host_lock(host)
        async with lock:
            now = time.monotonic()
            last_time = self._host_last_request_time.get(host, 0.0)
            elapsed = now - last_time
            if elapsed < self.min_host_interval:
                await asyncio.sleep(self.min_host_interval - elapsed)
            self._host_last_request_time[host] = time.monotonic()

    def _parse_retry_after(self, retry_after_val: Optional[str]) -> Optional[float]:
        if not retry_after_val:
            return None
        val_str = str(retry_after_val).strip()
        # Check integer seconds
        try:
            return max(1.0, float(val_str))
        except ValueError:
            pass
        # Check HTTP-date format
        try:
            dt = email.utils.parsedate_to_datetime(val_str)
            now_dt = datetime.now(timezone.utc)
            diff = (dt - now_dt).total_seconds()
            return max(1.0, diff)
        except Exception:
            return None

    async def __aenter__(self):
        if HAS_AIOHTTP:
            timeout_cfg = aiohttp.ClientTimeout(total=self.timeout)
            connector = aiohttp.TCPConnector(
                limit=self.semaphore._value,
                limit_per_host=self.per_host_concurrency,
                ssl=False,
                ttl_dns_cache=300,
            )
            self._session = aiohttp.ClientSession(
                timeout=timeout_cfg,
                connector=connector,
                headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"},
            )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()

    async def get_json(self, url: str, headers: Optional[Dict[str, str]] = None, max_retries: int = 2) -> Tuple[int, Any]:
        """Performs async GET and parses JSON with host-wide circuit breaker and exponential jitter on 429/503."""
        host = self._get_host(url)
        host_sem = self._get_host_sem(host)

        req_headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
        if headers:
            req_headers.update(headers)

        for attempt in range(max_retries + 1):
            async with self.semaphore:
                async with host_sem:
                    await self._wait_for_host(host)

                    if not HAS_AIOHTTP or not self._session:
                        import urllib.request
                        try:
                            req = urllib.request.Request(url, headers=req_headers)
                            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                                status = resp.status
                                data = json.loads(resp.read().decode("utf-8", errors="ignore"))
                                return status, data
                        except urllib.error.HTTPError as e:
                            if e.code in (429, 503) and attempt < max_retries:
                                retry_after = self._parse_retry_after(e.headers.get("Retry-After"))
                                backoff = retry_after or (1.5 * (2 ** attempt) + random.uniform(0.3, 1.0))
                                backoff = min(6.0, max(1.5, backoff))
                                self._host_cooldown_until[host] = time.monotonic() + backoff
                                await asyncio.sleep(backoff)
                                continue
                            return e.code, None
                        except Exception:
                            return 0, None

                    try:
                        async with self._session.get(url, headers=headers) as resp:
                            if resp.status == 200:
                                try:
                                    data = await resp.json(content_type=None)
                                    return 200, data
                                except Exception:
                                    text = await resp.text()
                                    return 200, json.loads(text)
                            elif resp.status in (429, 503) and attempt < max_retries:
                                retry_after = self._parse_retry_after(resp.headers.get("Retry-After"))
                                backoff = retry_after or (1.5 * (2 ** attempt) + random.uniform(0.3, 1.0))
                                backoff = min(6.0, max(1.5, backoff))
                                self._host_cooldown_until[host] = time.monotonic() + backoff
                                await asyncio.sleep(backoff)
                                continue
                            else:
                                return resp.status, None
                    except (asyncio.TimeoutError, aiohttp.ClientError):
                        # Fail-fast on dead/unresponsive hosts: do not retry dead domains!
                        return 0, None
                    except Exception:
                        return 0, None
        return 0, None

    async def get_text(self, url: str, headers: Optional[Dict[str, str]] = None, max_retries: int = 2) -> Tuple[int, Optional[str]]:
        """Performs async GET and returns raw text / HTML with host pacing and circuit breaker."""
        host = self._get_host(url)
        host_sem = self._get_host_sem(host)

        req_headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
        if headers:
            req_headers.update(headers)

        for attempt in range(max_retries + 1):
            async with self.semaphore:
                async with host_sem:
                    await self._wait_for_host(host)

                    if not HAS_AIOHTTP or not self._session:
                        import urllib.request
                        try:
                            req = urllib.request.Request(url, headers=req_headers)
                            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                                return resp.status, resp.read().decode("utf-8", errors="ignore")
                        except urllib.error.HTTPError as e:
                            if e.code in (429, 503) and attempt < max_retries:
                                retry_after = self._parse_retry_after(e.headers.get("Retry-After"))
                                backoff = retry_after or (1.5 * (2 ** attempt) + random.uniform(0.3, 1.0))
                                backoff = min(6.0, max(1.5, backoff))
                                self._host_cooldown_until[host] = time.monotonic() + backoff
                                await asyncio.sleep(backoff)
                                continue
                            return e.code, None
                        except Exception:
                            return 0, None

                    try:
                        async with self._session.get(url, headers=headers) as resp:
                            if resp.status == 200:
                                text = await resp.text()
                                return 200, text
                            elif resp.status in (429, 503) and attempt < max_retries:
                                retry_after = self._parse_retry_after(resp.headers.get("Retry-After"))
                                backoff = retry_after or (1.5 * (2 ** attempt) + random.uniform(0.3, 1.0))
                                backoff = min(6.0, max(1.5, backoff))
                                self._host_cooldown_until[host] = time.monotonic() + backoff
                                await asyncio.sleep(backoff)
                                continue
                            else:
                                return resp.status, None
                    except (asyncio.TimeoutError, aiohttp.ClientError):
                        return 0, None
                    except Exception:
                        return 0, None
        return 0, None

    async def post_json(self, url: str, json_data: Any, headers: Optional[Dict[str, str]] = None, max_retries: int = 2) -> Tuple[int, Any]:
        """Performs async POST and parses JSON with host pacing and circuit breaker."""
        host = self._get_host(url)
        host_sem = self._get_host_sem(host)

        req_headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        }
        if headers:
            req_headers.update(headers)

        for attempt in range(max_retries + 1):
            async with self.semaphore:
                async with host_sem:
                    await self._wait_for_host(host)

                    if not HAS_AIOHTTP or not self._session:
                        import urllib.request
                        try:
                            payload = json.dumps(json_data).encode("utf-8")
                            req = urllib.request.Request(
                                url,
                                data=payload,
                                headers=req_headers,
                            )
                            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                                return resp.status, json.loads(resp.read().decode("utf-8", errors="ignore"))
                        except urllib.error.HTTPError as e:
                            if e.code in (429, 503) and attempt < max_retries:
                                retry_after = self._parse_retry_after(e.headers.get("Retry-After"))
                                backoff = retry_after or (1.5 * (2 ** attempt) + random.uniform(0.3, 1.0))
                                backoff = min(6.0, max(1.5, backoff))
                                self._host_cooldown_until[host] = time.monotonic() + backoff
                                await asyncio.sleep(backoff)
                                continue
                            return e.code, None
                        except Exception:
                            return 0, None

                    try:
                        async with self._session.post(url, json=json_data, headers=headers) as resp:
                            if resp.status == 200:
                                try:
                                    data = await resp.json(content_type=None)
                                    return 200, data
                                except Exception:
                                    text = await resp.text()
                                    return 200, json.loads(text)
                            elif resp.status in (429, 503) and attempt < max_retries:
                                retry_after = self._parse_retry_after(resp.headers.get("Retry-After"))
                                backoff = retry_after or (1.5 * (2 ** attempt) + random.uniform(0.3, 1.0))
                                backoff = min(6.0, max(1.5, backoff))
                                self._host_cooldown_until[host] = time.monotonic() + backoff
                                await asyncio.sleep(backoff)
                                continue
                            else:
                                return resp.status, None
                    except (asyncio.TimeoutError, aiohttp.ClientError):
                        return 0, None
                    except Exception:
                        return 0, None
        return 0, None


# Platform Extractors
async def extract_greenhouse(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    if full_content:
        url += "?content=true"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, dict):
        return status, []

    company_name = data.get("name") or slug.title()
    jobs = data.get("jobs") or []
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for j in jobs:
        loc_obj = j.get("location") or {}
        loc_str = loc_obj.get("name", "") if isinstance(loc_obj, dict) else str(loc_obj)
        depts = [d.get("name") for d in (j.get("departments") or []) if d.get("name")]
        offices = [o.get("name") for o in (j.get("offices") or []) if o.get("name")]

        first_pub = j.get("first_published") or ""
        updated = j.get("updated_at") or ""
        raw_ts = first_pub or updated

        content_raw = j.get("content", "") if full_content else ""
        extracted.append({
            "job_id": f"greenhouse_{slug}_{j.get('id')}",
            "platform": "greenhouse",
            "company_name": company_name,
            "company_slug": slug,
            "title": j.get("title", "").strip(),
            "location": loc_str.strip(),
            "is_us": 1 if is_us_location(loc_str, extra_text=f"{j.get('title', '')} {' '.join(offices)}") else 0,
            "apply_url": j.get("absolute_url", ""),
            "department": ", ".join(filter(None, depts)),
            "office": ", ".join(filter(None, offices)),
            "employment_type": "",
            "posted_at": parse_to_iso(first_pub) or parse_to_iso(updated),
            "updated_at": parse_to_iso(updated),
            "raw_timestamp": str(raw_ts),
            "content": clean_html(content_raw) if full_content else "",
            "extracted_at": now_iso,
        })
    return status, extracted


async def extract_lever(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, list):
        return status, []

    company_name = slug.replace("-", " ").title()
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for j in data:
        if not isinstance(j, dict):
            continue
        cats = j.get("categories") or {}
        loc_str = cats.get("location", "")
        country = j.get("country", "")
        if country.upper() == "US" or not loc_str:
            loc_str = f"{loc_str}, US".strip(", ") if loc_str else "United States"

        dept_val = cats.get("team") or cats.get("department", "")
        dept = dept_val.get("name", "") if isinstance(dept_val, dict) else str(dept_val)
        emp_val = cats.get("commitment", "")
        emp_type = emp_val.get("name", "") if isinstance(emp_val, dict) else str(emp_val)
        workplace_val = j.get("workplaceType", "")
        workplace = workplace_val.get("name", "") if isinstance(workplace_val, dict) else str(workplace_val)

        created_ms = j.get("createdAt")
        posted_iso = parse_to_iso(created_ms)

        content_raw = (j.get("descriptionPlain") or j.get("description", "")) if full_content else ""
        extracted.append({
            "job_id": f"lever_{slug}_{j.get('id')}",
            "platform": "lever",
            "company_name": company_name,
            "company_slug": slug,
            "title": j.get("text", "").strip(),
            "location": loc_str.strip(),
            "is_us": 1 if (country.upper() == "US" or is_us_location(loc_str, extra_text=j.get("text", "")) or workplace == "remote") else 0,
            "apply_url": j.get("applyUrl") or j.get("hostedUrl", ""),
            "department": str(dept),
            "office": str(workplace),
            "employment_type": str(emp_type),
            "posted_at": posted_iso,
            "updated_at": posted_iso,
            "raw_timestamp": str(created_ms or ""),
            "content": clean_html(content_raw) if full_content else "",
            "extracted_at": now_iso,
        })
    return status, extracted


async def extract_ashby(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, dict):
        return status, []

    company_name = data.get("organizationName") or slug.replace("-", " ").title()
    if isinstance(company_name, dict):
        company_name = company_name.get("name") or slug.replace("-", " ").title()

    jobs = data.get("jobs") or data.get("jobPostings") or []
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for j in jobs:
        loc_val = j.get("location") or j.get("locationName") or ""
        loc_str = loc_val.get("name", "") if isinstance(loc_val, dict) else str(loc_val)
        addr_val = j.get("address") or ""
        addr = addr_val.get("addressLocality", "") if isinstance(addr_val, dict) else str(addr_val)
        is_remote = bool(j.get("isRemote"))
        workplace = "Remote" if is_remote else (j.get("workplaceType") or "")

        dept_val = j.get("department") or j.get("team") or ""
        dept_str = dept_val.get("name", "") if isinstance(dept_val, dict) else str(dept_val)
        emp_val = j.get("employmentType", "")
        emp_str = emp_val.get("name", "") if isinstance(emp_val, dict) else str(emp_val)

        pub_at = j.get("publishedAt") or ""
        content_raw = (j.get("descriptionPlain") or j.get("descriptionHtml", "")) if full_content else ""

        extracted.append({
            "job_id": f"ashby_{slug}_{j.get('id')}",
            "platform": "ashby",
            "company_name": company_name,
            "company_slug": slug,
            "title": j.get("title", "").strip(),
            "location": loc_str.strip(),
            "is_us": 1 if (is_us_location(loc_str, extra_text=j.get("title", "")) or is_us_location(addr, extra_text=j.get("title", "")) or is_remote) else 0,
            "apply_url": j.get("applyUrl") or j.get("jobUrl", ""),
            "department": dept_str,
            "office": workplace,
            "employment_type": emp_str,
            "posted_at": parse_to_iso(pub_at),
            "updated_at": parse_to_iso(pub_at),
            "raw_timestamp": str(pub_at),
            "content": clean_html(content_raw) if full_content else "",
            "extracted_at": now_iso,
        })
    return status, extracted


async def extract_smartrecruiters(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, dict):
        return status, []

    jobs = data.get("content") or []
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for j in jobs:
        loc_obj = j.get("location") or {}
        c_code = (loc_obj.get("country") or "").upper()
        loc_str = ", ".join(filter(None, [loc_obj.get("city"), loc_obj.get("region"), c_code]))

        comp_name = (j.get("company") or {}).get("name") or slug.replace("-", " ").title()
        rel_date = j.get("releasedDate") or ""
        job_id = j.get("id") or j.get("uuid")

        extracted.append({
            "job_id": f"smartrecruiters_{slug}_{job_id}",
            "platform": "smartrecruiters",
            "company_name": comp_name,
            "company_slug": slug,
            "title": j.get("name", "").strip(),
            "location": loc_str.strip(),
            "is_us": 1 if (c_code == "US" or is_us_location(loc_str, extra_text=j.get("name", ""))) else 0,
            "apply_url": f"https://jobs.smartrecruiters.com/{slug}/{job_id}",
            "department": (j.get("department") or {}).get("label", ""),
            "office": "",
            "employment_type": (j.get("typeOfEmployment") or {}).get("label", ""),
            "posted_at": parse_to_iso(rel_date),
            "updated_at": parse_to_iso(rel_date),
            "raw_timestamp": str(rel_date),
            "content": "",
            "extracted_at": now_iso,
        })
    return status, extracted


async def extract_workable(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://apply.workable.com/api/v1/widget/accounts/{slug}"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, dict):
        return status, []

    company_name = data.get("name") or slug.replace("-", " ").title()
    jobs = data.get("jobs") or []
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for j in jobs:
        loc_str = ", ".join(filter(None, [j.get("city"), j.get("state"), j.get("country")]))
        is_tele = bool(j.get("telecommuting"))
        shortcode = j.get("shortcode") or j.get("code") or j.get("id")

        pub_on = j.get("published_on") or j.get("created_at") or ""
        extracted.append({
            "job_id": f"workable_{slug}_{shortcode}",
            "platform": "workable",
            "company_name": company_name,
            "company_slug": slug,
            "title": j.get("title", "").strip(),
            "location": loc_str.strip(),
            "is_us": 1 if (j.get("country") in ("United States", "US") or is_us_location(loc_str, extra_text=j.get("title", "")) or is_tele) else 0,
            "apply_url": j.get("application_url") or j.get("url") or f"https://apply.workable.com/{slug}/j/{shortcode}/",
            "department": j.get("department", ""),
            "office": "Remote" if is_tele else "",
            "employment_type": j.get("employment_type", ""),
            "posted_at": parse_to_iso(pub_on),
            "updated_at": parse_to_iso(pub_on),
            "raw_timestamp": str(pub_on),
            "content": "",
            "extracted_at": now_iso,
        })
    return status, extracted


async def extract_recruitee(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://{slug}.recruitee.com/api/offers"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, dict):
        return status, []

    offers = data.get("offers") or []
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for o in offers:
        loc_str = ", ".join(filter(None, [o.get("city"), o.get("country_code")])) or o.get("location", "")
        comp_name = o.get("company_name") or slug.replace("-", " ").title()
        is_remote = bool(o.get("remote"))
        workplace = "Remote" if is_remote else ("Hybrid" if o.get("hybrid") else "")

        pub_at = o.get("published_at") or o.get("created_at") or ""
        updated_at = o.get("updated_at") or ""
        content_raw = o.get("description", "") if full_content else ""

        extracted.append({
            "job_id": f"recruitee_{slug}_{o.get('id')}",
            "platform": "recruitee",
            "company_name": comp_name,
            "company_slug": slug,
            "title": o.get("title", "").strip(),
            "location": loc_str.strip(),
            "is_us": 1 if (is_us_location(loc_str, extra_text=o.get("title", "")) or o.get("country_code") in ("US", "USA") or is_remote) else 0,
            "apply_url": o.get("careers_apply_url") or o.get("careers_url", ""),
            "department": o.get("department", ""),
            "office": workplace,
            "employment_type": o.get("employment_type_code", ""),
            "posted_at": parse_to_iso(pub_at),
            "updated_at": parse_to_iso(updated_at),
            "raw_timestamp": str(pub_at),
            "content": clean_html(content_raw) if full_content else "",
            "extracted_at": now_iso,
        })
    return status, extracted


async def extract_breezy(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://{slug}.breezy.hr/json"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, list):
        return status, []

    company_name = slug.replace("-", " ").title()
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for j in data:
        if not isinstance(j, dict):
            continue
        loc_obj = j.get("location") or {}
        loc_str = loc_obj.get("name", "") if isinstance(loc_obj, dict) else str(loc_obj)
        type_obj = j.get("type") or {}
        emp_type = type_obj.get("name", "") if isinstance(type_obj, dict) else str(type_obj)

        comp_obj = j.get("company")
        if isinstance(comp_obj, dict):
            comp_name = comp_obj.get("name") or company_name
        elif isinstance(comp_obj, str) and comp_obj.strip():
            comp_name = comp_obj.strip()
        else:
            comp_name = company_name

        dept_obj = j.get("department")
        dept_str = dept_obj.get("name", "") if isinstance(dept_obj, dict) else str(dept_obj or "")

        pub_date = j.get("published_date") or ""
        job_id = j.get("id") or j.get("friendly_id")
        content_raw = j.get("description", "") if full_content else ""

        extracted.append({
            "job_id": f"breezy_{slug}_{job_id}",
            "platform": "breezy",
            "company_name": comp_name,
            "company_slug": slug,
            "title": j.get("name", "").strip(),
            "location": loc_str.strip(),
            "is_us": 1 if is_us_location(loc_str, extra_text=j.get("name", "")) else 0,
            "apply_url": j.get("url", ""),
            "department": dept_str,
            "office": "",
            "employment_type": emp_type,
            "posted_at": parse_to_iso(pub_date),
            "updated_at": parse_to_iso(pub_date),
            "raw_timestamp": str(pub_date),
            "content": clean_html(content_raw) if full_content else "",
            "extracted_at": now_iso,
        })
    return status, extracted


async def extract_rippling(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://ats.rippling.com/api/v1/board/{slug}/jobs"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, list):
        return status, []

    company_name = slug.replace("-", " ").title()
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for j in data:
        if not isinstance(j, dict):
            continue
        work_loc = (j.get("workLocation") or {}).get("label", "")
        extracted.append({
            "job_id": f"rippling_{slug}_{j.get('uuid')}",
            "platform": "rippling",
            "company_name": company_name,
            "company_slug": slug,
            "title": j.get("name", "").strip(),
            "location": work_loc.strip(),
            "is_us": 1 if is_us_location(work_loc, extra_text=j.get("name", "")) else 0,
            "apply_url": j.get("url", ""),
            "department": (j.get("department") or {}).get("name", ""),
            "office": "",
            "employment_type": "",
            "posted_at": "",
            "updated_at": "",
            "raw_timestamp": "",
            "content": "",
            "extracted_at": now_iso,
        })
    return status, extracted


async def extract_bamboohr(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://{slug}.bamboohr.com/careers/list"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, dict):
        return status, []

    jobs = data.get("result") or []
    company_name = slug.replace("-", " ").title()
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for j in jobs:
        loc_obj = j.get("location") or {}
        loc_str = ", ".join(filter(None, [loc_obj.get("city"), loc_obj.get("state")]))
        is_remote = bool(j.get("isRemote"))
        job_id = j.get("id")

        extracted.append({
            "job_id": f"bamboohr_{slug}_{job_id}",
            "platform": "bamboohr",
            "company_name": company_name,
            "company_slug": slug,
            "title": j.get("jobOpeningName", "").strip(),
            "location": loc_str.strip(),
            "is_us": 1 if (is_us_location(loc_str, extra_text=j.get("jobOpeningName", "")) or is_remote) else 0,
            "apply_url": f"https://{slug}.bamboohr.com/careers/{job_id}",
            "department": j.get("departmentLabel", ""),
            "office": "Remote" if is_remote else "",
            "employment_type": j.get("employmentType", ""),
            "posted_at": "",
            "updated_at": "",
            "raw_timestamp": "",
            "content": "",
            "extracted_at": now_iso,
        })
    return status, extracted


async def extract_pinpoint(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://{slug}.pinpointhq.com/postings.json"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, dict):
        return status, []

    jobs = data.get("data") or []
    company_name = slug.replace("-", " ").title()
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for j in jobs:
        loc_obj = j.get("location") or {}
        loc_str = loc_obj.get("name", "") if isinstance(loc_obj, dict) else str(loc_obj)
        dept = ((j.get("job") or {}).get("department") or {}).get("name", "")
        deadline = j.get("deadline_at") or ""

        extracted.append({
            "job_id": f"pinpoint_{slug}_{j.get('id')}",
            "platform": "pinpoint",
            "company_name": company_name,
            "company_slug": slug,
            "title": j.get("title", "").strip(),
            "location": loc_str.strip(),
            "is_us": 1 if is_us_location(loc_str, extra_text=j.get("title", "")) else 0,
            "apply_url": j.get("url", ""),
            "department": dept,
            "office": j.get("workplace_type_text", ""),
            "employment_type": j.get("employment_type_text", ""),
            "posted_at": "",
            "updated_at": "",
            "raw_timestamp": str(deadline),
            "content": clean_html(j.get("description", "")) if full_content else "",
            "extracted_at": now_iso,
        })
    return status, extracted


def format_location_string(loc_val: Any, is_remote: bool = False) -> str:
    """Safely converts any location representation (dict, list, string) into a clean string."""
    if loc_val is None:
        return "Remote" if is_remote else ""
    if isinstance(loc_val, str):
        return loc_val.strip()
    if isinstance(loc_val, dict):
        parts = [
            loc_val.get("city") or loc_val.get("addressLocality") or loc_val.get("name"),
            loc_val.get("state") or loc_val.get("addressRegion") or loc_val.get("region"),
            loc_val.get("country") or loc_val.get("addressCountry"),
        ]
        res = ", ".join(str(p).strip() for p in parts if p)
        return res if res else ", ".join(f"{k}: {v}" for k, v in loc_val.items() if v)
    if isinstance(loc_val, (list, tuple, set)):
        return ", ".join(format_location_string(x) for x in loc_val if x)
    return str(loc_val).strip()


def extract_jsonld_job_postings(html: str) -> List[Dict[str, Any]]:
    """Helper to extract schema.org JobPosting objects from HTML script tags."""
    jobs = []
    if not html:
        return jobs
    scripts = re.findall(r'<script[^>]*type=[\"\']application/ld\+json[\"\'][^>]*>(.*?)</script>', html, re.DOTALL | re.I)
    for s in scripts:
        try:
            parsed = json.loads(s.strip())
            items = parsed if isinstance(parsed, list) else [parsed]
            for item in items:
                if isinstance(item, dict) and item.get("@type") in ("JobPosting", "jobPosting"):
                    jobs.append(item)
                elif isinstance(item, dict) and "@graph" in item:
                    for g in item["@graph"]:
                        if isinstance(g, dict) and g.get("@type") in ("JobPosting", "jobPosting"):
                            jobs.append(g)
        except Exception:
            pass
    return jobs


async def extract_gem(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://api.gem.com/job_board/v0/{slug}/job_posts"
    status, data = await http.get_json(url)
    if status != 200:
        return status, []
    items = data if isinstance(data, list) else (data.get("job_posts") or [])
    company_name = slug.replace("-", " ").title()
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()
    for j in items:
        if not isinstance(j, dict):
            continue
        job_id = j.get("id") or (j.get("absolute_url") or "").rstrip("/").split("/")[-1] or str(hash(j.get("title", "")))
        loc = format_location_string(j.get("location"), is_remote=bool(j.get("is_remote")))
        pub = j.get("published_at") or j.get("created_at") or ""
        content_raw = j.get("content") or ""
        extracted.append({
            "job_id": f"gem_{slug}_{job_id}",
            "platform": "gem",
            "company_name": company_name,
            "company_slug": slug,
            "title": (j.get("title") or "").strip(),
            "location": loc,
            "is_us": 1 if (j.get("is_remote") or is_us_location(loc, extra_text=j.get("title", ""))) else 0,
            "apply_url": j.get("absolute_url") or f"https://jobs.gem.com/{slug}/{job_id}",
            "department": j.get("department") or "",
            "office": "Remote" if j.get("is_remote") else "",
            "employment_type": j.get("employment_type") or "",
            "posted_at": parse_to_iso(pub),
            "updated_at": "",
            "raw_timestamp": str(pub),
            "content": clean_html(content_raw) if full_content else "",
            "extracted_at": now_iso,
        })
    return status, extracted


async def extract_dover(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    cid = slug
    url = f"https://app.dover.com/api/v1/careers-page/{cid}/jobs?limit=100&offset=0"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, dict):
        res_st, res_data = await http.get_json(f"https://app.dover.com/api/v1/careers-page-slug/{slug}")
        if res_st == 200 and isinstance(res_data, dict) and res_data.get("id"):
            cid = res_data["id"]
            status, data = await http.get_json(f"https://app.dover.com/api/v1/careers-page/{cid}/jobs?limit=100&offset=0")

    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()
    company_name = slug.replace("-", " ").title()
    if status == 200 and isinstance(data, dict):
        jobs = data.get("results") or data.get("jobs") or []
        for j in jobs:
            loc_list = j.get("locations") or []
            loc_str = ""
            if loc_list and isinstance(loc_list, list):
                first_loc = loc_list[0] if isinstance(loc_list[0], dict) else {}
                opt = first_loc.get("location_option") or {} if isinstance(first_loc.get("location_option"), dict) else {}
                loc_str = opt.get("display_name") or f"{opt.get('city', '')}, {opt.get('state', '')}".strip(", ")
            if not loc_str:
                loc_str = format_location_string(j.get("location"))
            is_rem = j.get("workplace_type") == "REMOTE" or bool(j.get("remote")) or "remote" in loc_str.lower()
            created = j.get("created_at") or j.get("date_posted") or ""
            jid = str(j.get("id") or hash(j.get("title", "")))
            extracted.append({
                "job_id": f"dover_{slug}_{jid}",
                "platform": "dover",
                "company_name": company_name,
                "company_slug": slug,
                "title": (j.get("title") or "").strip(),
                "location": loc_str,
                "is_us": 1 if (is_rem or is_us_location(loc_str, extra_text=j.get("title", ""))) else 0,
                "apply_url": f"https://app.dover.com/apply/{slug}/{jid}",
                "department": j.get("department") or "",
                "office": "Remote" if is_rem else "",
                "employment_type": j.get("type") or j.get("employment_type") or "",
                "posted_at": parse_to_iso(created),
                "updated_at": "",
                "raw_timestamp": str(created),
                "content": clean_html(j.get("description", "")) if full_content else "",
                "extracted_at": now_iso,
            })
        if extracted:
            return 200, extracted

    url_html = f"https://app.dover.com/apply/{slug}"
    st, html = await http.get_text(url_html)
    if st == 200 and html:
        ld_jobs = extract_jsonld_job_postings(html)
        for j in ld_jobs:
            title = j.get("title", "").strip()
            loc_obj = j.get("jobLocation", {}).get("address", {}) if isinstance(j.get("jobLocation"), dict) else {}
            loc = f"{loc_obj.get('addressLocality', '')}, {loc_obj.get('addressRegion', '')}".strip(", ")
            date_p = j.get("datePosted") or ""
            extracted.append({
                "job_id": f"dover_{slug}_{hash(title)}",
                "platform": "dover",
                "company_name": company_name,
                "company_slug": slug,
                "title": title,
                "location": loc,
                "is_us": 1 if is_us_location(loc, extra_text=title) else 0,
                "apply_url": j.get("url") or url_html,
                "department": "",
                "office": "",
                "employment_type": j.get("employmentType") or "",
                "posted_at": parse_to_iso(date_p),
                "updated_at": "",
                "raw_timestamp": str(date_p),
                "content": clean_html(j.get("description", "")) if full_content else "",
                "extracted_at": now_iso,
            })
        return 200, extracted
    return status, []


async def extract_jobvite(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://jobs.jobvite.com/{slug}"
    status, html = await http.get_text(url)
    if status != 200 or not html:
        return status, []
    company_name = slug.replace("-", " ").title()
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()
    ld_jobs = extract_jsonld_job_postings(html)
    if ld_jobs:
        for j in ld_jobs:
            title = j.get("title", "").strip()
            loc_obj = j.get("jobLocation", {}).get("address", {}) if isinstance(j.get("jobLocation"), dict) else {}
            loc = f"{loc_obj.get('addressLocality', '')}, {loc_obj.get('addressRegion', '')}".strip(", ")
            u = j.get("url") or ""
            jid = u.rstrip("/").split("/")[-1] if u else str(hash(title))
            date_p = j.get("datePosted") or ""
            extracted.append({
                "job_id": f"jobvite_{slug}_{jid}",
                "platform": "jobvite",
                "company_name": company_name,
                "company_slug": slug,
                "title": title,
                "location": loc,
                "is_us": 1 if is_us_location(loc, extra_text=title) else 0,
                "apply_url": u if u.startswith("http") else f"https://jobs.jobvite.com{u}",
                "department": "",
                "office": "",
                "employment_type": j.get("employmentType") or "",
                "posted_at": parse_to_iso(date_p),
                "updated_at": "",
                "raw_timestamp": str(date_p),
                "content": clean_html(j.get("description", "")) if full_content else "",
                "extracted_at": now_iso,
            })
        if extracted:
            return 200, extracted
    matches = re.findall(
        r'<td[^>]*class=[\"\']jv-job-list-name[\"\'][^>]*>\s*<a[^>]*href=[\"\'](/[^/]+/job/([^\"?#]+))[\"\'][^>]*>([^<]+)</a>(?:.*?<td[^>]*class=[\"\']jv-job-list-location[\"\'][^>]*>\s*([^<]+))?',
        html, re.DOTALL | re.I
    )
    for path, jid, title, loc in matches:
        loc_str = re.sub(r"\s+", " ", (loc or "")).strip()
        t_clean = title.strip()
        extracted.append({
            "job_id": f"jobvite_{slug}_{jid}",
            "platform": "jobvite",
            "company_name": company_name,
            "company_slug": slug,
            "title": t_clean,
            "location": loc_str,
            "is_us": 1 if is_us_location(loc_str, extra_text=t_clean) else 0,
            "apply_url": f"https://jobs.jobvite.com{path}",
            "department": "",
            "office": "",
            "employment_type": "",
            "posted_at": "",
            "updated_at": "",
            "raw_timestamp": "",
            "content": "",
            "extracted_at": now_iso,
        })
    return 200, extracted


async def extract_jazzhr(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://{slug}.applytojob.com/apply"
    status, html = await http.get_text(url)
    if status != 200 or not html:
        return status, []
    company_name = slug.replace("-", " ").title()
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()
    matches = re.findall(
        r'<a[^>]*href=[\"\'](https?://[^/]+/apply/([a-zA-Z0-9_-]+)/[^\"]+)[\"\'][^>]*>([^<]+)</a>(?:.*?<ul class=[\"\']list-inline[\"\']><li>(?:<i[^>]*></i>\s*)?([^<]+))?',
        html, re.DOTALL | re.I
    )
    for a_url, jid, title, loc in matches:
        loc_str = re.sub(r"\s+", " ", (loc or "")).strip()
        t_clean = title.strip()
        extracted.append({
            "job_id": f"jazzhr_{slug}_{jid}",
            "platform": "jazzhr",
            "company_name": company_name,
            "company_slug": slug,
            "title": t_clean,
            "location": loc_str,
            "is_us": 1 if is_us_location(loc_str, extra_text=t_clean) else 0,
            "apply_url": a_url,
            "department": "",
            "office": "",
            "employment_type": "",
            "posted_at": "",
            "updated_at": "",
            "raw_timestamp": "",
            "content": "",
            "extracted_at": now_iso,
        })
    return 200, extracted


async def extract_trakstar(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    company_name = slug.replace("-", " ").title()
    now_iso = datetime.now(timezone.utc).isoformat()
    extracted = []

    # 1. Native RSS Feed (100% full board coverage with location & description)
    url_feed = f"https://{slug}.hire.trakstar.com/jobfeeds/{slug}"
    st, xml_text = await http.get_text(url_feed)
    if st == 200 and xml_text and "<item>" in xml_text:
        items = re.findall(r'<item>(.*?)</item>', xml_text, re.DOTALL)
        for it in items:
            t_m = re.search(r'<title>(.*?)</title>', it)
            l_m = re.search(r'<link>(.*?)</link>', it)
            d_m = re.search(r'<pubDate>(.*?)</pubDate>', it)
            desc_m = re.search(r'<description>(.*?)</description>', it, re.DOTALL)
            c_m = re.search(r'<[^>]*locationCity>(.*?)</[^>]*locationCity>', it)
            s_m = re.search(r'<[^>]*locationState>(.*?)</[^>]*locationState>', it)
            co_m = re.search(r'<[^>]*locationCountry>(.*?)</[^>]*locationCountry>', it)

            title = t_m.group(1).strip() if t_m else ""
            link = l_m.group(1).strip() if l_m else ""
            pub_d = d_m.group(1).strip() if d_m else ""
            desc = desc_m.group(1).strip() if desc_m else ""
            city = c_m.group(1).strip() if c_m else ""
            state = s_m.group(1).strip() if s_m else ""
            country = co_m.group(1).strip() if co_m else ""

            loc = f"{city}, {state}".strip(", ")
            if not loc:
                loc = country
            jid = link.rstrip("/").split("/")[-1] if link else str(hash(title))
            is_rem = "remote" in city.lower() or "remote" in title.lower() or is_us_location(loc, extra_text=title)
            extracted.append({
                "job_id": f"trakstar_{slug}_{jid}",
                "platform": "trakstar",
                "company_name": company_name,
                "company_slug": slug,
                "title": title,
                "location": loc,
                "is_us": 1 if is_rem else 0,
                "apply_url": link or f"https://{slug}.hire.trakstar.com/jobs/{jid}/",
                "department": "",
                "office": "Remote" if "remote" in city.lower() else "",
                "employment_type": "",
                "posted_at": parse_to_iso(pub_d),
                "updated_at": "",
                "raw_timestamp": pub_d,
                "content": clean_html(desc) if full_content else "",
                "extracted_at": now_iso,
            })
        if extracted:
            return 200, extracted

    # 2. Recruiterbox API fallback
    url_api = f"https://js.recruiterbox.com/v1/openings?client_name={slug}"
    status, data = await http.get_json(url_api)
    if status == 200 and isinstance(data, dict):
        openings = data.get("objects") or []
        for o in openings:
            jid = str(o.get("id") or "")
            title = (o.get("title") or "").strip()
            loc_obj = o.get("location") or {}
            loc = loc_obj.get("city", "") if isinstance(loc_obj, dict) else str(loc_obj)
            remote = o.get("allows_remote")
            created = o.get("created_on") or ""
            extracted.append({
                "job_id": f"trakstar_{slug}_{jid}",
                "platform": "trakstar",
                "company_name": company_name,
                "company_slug": slug,
                "title": title,
                "location": loc.strip(),
                "is_us": 1 if (remote or is_us_location(loc, extra_text=title)) else 0,
                "apply_url": o.get("hosted_url") or f"https://{slug}.hire.trakstar.com/jobs/fk0{jid}/",
                "department": (o.get("team") or {}).get("name", "") if isinstance(o.get("team"), dict) else "",
                "office": "Remote" if remote else "",
                "employment_type": o.get("position_type") or "",
                "posted_at": parse_to_iso(created),
                "updated_at": "",
                "raw_timestamp": str(created),
                "content": clean_html(o.get("description", "")) if full_content else "",
                "extracted_at": now_iso,
            })
        if extracted:
            return 200, extracted

    # 3. HTML parsing fallback
    url_html = f"https://{slug}.hire.trakstar.com"
    st, html = await http.get_text(url_html)
    if st == 200 and html:
        matches = re.findall(
            r'<a[^>]*href=[\"\'](/jobs/fk0([a-zA-Z0-9]+)/?)[\"\'][^>]*>([^<]+)</a>(?:.*?<span[^>]*class=[\"\']meta-item[^\"\']*location[^\"]*[\"\'][^>]*>([^<]+))?',
            html, re.DOTALL | re.I
        )
        for path, jid, title, loc in matches:
            loc_str = (loc or "").strip()
            t_clean = title.strip()
            extracted.append({
                "job_id": f"trakstar_{slug}_{jid}",
                "platform": "trakstar",
                "company_name": company_name,
                "company_slug": slug,
                "title": t_clean,
                "location": loc_str,
                "is_us": 1 if is_us_location(loc_str, extra_text=t_clean) else 0,
                "apply_url": f"https://{slug}.hire.trakstar.com{path}",
                "department": "",
                "office": "",
                "employment_type": "",
                "posted_at": "",
                "updated_at": "",
                "raw_timestamp": "",
                "content": "",
                "extracted_at": now_iso,
            })
        return 200, extracted
    return status if status else st, []


async def extract_fountain(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://apply.fountain.com/{slug}"
    status, html = await http.get_text(url)
    if status != 200 or not html:
        return status, []
    company_name = slug.replace("-", " ").title()
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()
    ld_jobs = extract_jsonld_job_postings(html)
    for j in ld_jobs:
        title = j.get("title", "").strip()
        loc_obj = j.get("jobLocation", {}).get("address", {}) if isinstance(j.get("jobLocation"), dict) else {}
        loc = f"{loc_obj.get('addressLocality', '')}, {loc_obj.get('addressRegion', '')}".strip(", ")
        u = j.get("url") or f"https://apply.fountain.com/{slug}"
        date_p = j.get("datePosted") or ""
        extracted.append({
            "job_id": f"fountain_{slug}_{hash(title + u)}",
            "platform": "fountain",
            "company_name": company_name,
            "company_slug": slug,
            "title": title,
            "location": loc,
            "is_us": 1 if is_us_location(loc, extra_text=title) else 0,
            "apply_url": u,
            "department": "",
            "office": "",
            "employment_type": j.get("employmentType") or "",
            "posted_at": parse_to_iso(date_p),
            "updated_at": "",
            "raw_timestamp": str(date_p),
            "content": clean_html(j.get("description", "")) if full_content else "",
            "extracted_at": now_iso,
        })
    return 200, extracted


async def extract_icims(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://{slug}.icims.com/jobs/search?in_iframe=1"
    status, html = await http.get_text(url)
    if status != 200 or not html:
        url_root = f"https://{slug}.icims.com"
        st, h = await http.get_text(url_root)
        if st == 200 and h:
            status, html = st, h
        else:
            return status, []
    company_name = slug.replace("-", " ").title()
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()
    ld_jobs = extract_jsonld_job_postings(html)
    if ld_jobs:
        for j in ld_jobs:
            title = j.get("title", "").strip()
            loc_obj = j.get("jobLocation", {}).get("address", {}) if isinstance(j.get("jobLocation"), dict) else {}
            loc = f"{loc_obj.get('addressLocality', '')}, {loc_obj.get('addressRegion', '')}".strip(", ")
            u = j.get("url") or ""
            jid = u.split("/jobs/")[1].split("/")[0] if "/jobs/" in u else str(hash(title))
            date_p = j.get("datePosted") or ""
            extracted.append({
                "job_id": f"icims_{slug}_{jid}",
                "platform": "icims",
                "company_name": company_name,
                "company_slug": slug,
                "title": title,
                "location": loc,
                "is_us": 1 if is_us_location(loc, extra_text=title) else 0,
                "apply_url": u,
                "department": "",
                "office": "",
                "employment_type": j.get("employmentType") or "",
                "posted_at": parse_to_iso(date_p),
                "updated_at": "",
                "raw_timestamp": str(date_p),
                "content": clean_html(j.get("description", "")) if full_content else "",
                "extracted_at": now_iso,
            })
        if extracted:
            return 200, extracted
    matches = re.findall(
        r'href=[\"\'](https?://[^/]+/jobs/(\d+)/([^/]+)/job[^\"]*)[\"\'][^>]*>\s*(?:<span[^>]*>)?([^<]+)',
        html, re.I
    )
    seen_ids = set()
    for a_url, jid, slug_t, title in matches:
        if jid in seen_ids:
            continue
        seen_ids.add(jid)
        t_clean = title.strip()
        if t_clean.lower() in ("title", "job title", "search", "") and slug_t:
            import urllib.parse
            t_clean = urllib.parse.unquote(slug_t).replace("-", " ").title()
        loc_str = ""
        loc_m = re.search(rf'{jid}.*?(?:Job Locations|Location).*?<span[^>]*>([^<]+)', html, re.DOTALL | re.I)
        if loc_m:
            loc_str = loc_m.group(1).strip()
        clean_apply = a_url.replace("?in_iframe=1", "").replace("&in_iframe=1", "")
        extracted.append({
            "job_id": f"icims_{slug}_{jid}",
            "platform": "icims",
            "company_name": company_name,
            "company_slug": slug,
            "title": t_clean,
            "location": loc_str,
            "is_us": 1 if is_us_location(loc_str, extra_text=t_clean) else 0,
            "apply_url": clean_apply,
            "department": "",
            "office": "",
            "employment_type": "",
            "posted_at": "",
            "updated_at": "",
            "raw_timestamp": "",
            "content": "",
            "extracted_at": now_iso,
        })
    return 200, extracted


async def extract_taleo(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://{slug}.taleo.net/careersection/ex/jobsearch.ftl"
    status, html = await http.get_text(url)
    if status != 200 or not html:
        return status, []
    company_name = slug.replace("-", " ").title()
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()
    ld_jobs = extract_jsonld_job_postings(html)
    if ld_jobs:
        for j in ld_jobs:
            title = j.get("title", "").strip()
            loc_obj = j.get("jobLocation", {}).get("address", {}) if isinstance(j.get("jobLocation"), dict) else {}
            loc = f"{loc_obj.get('addressLocality', '')}, {loc_obj.get('addressRegion', '')}".strip(", ")
            u = j.get("url") or ""
            jid = u.split("job=")[1].split("&")[0] if "job=" in u else str(hash(title))
            date_p = j.get("datePosted") or ""
            extracted.append({
                "job_id": f"taleo_{slug}_{jid}",
                "platform": "taleo",
                "company_name": company_name,
                "company_slug": slug,
                "title": title,
                "location": loc,
                "is_us": 1 if is_us_location(loc, extra_text=title) else 0,
                "apply_url": u,
                "department": "",
                "office": "",
                "employment_type": j.get("employmentType") or "",
                "posted_at": parse_to_iso(date_p),
                "updated_at": "",
                "raw_timestamp": str(date_p),
                "content": clean_html(j.get("description", "")) if full_content else "",
                "extracted_at": now_iso,
            })
        if extracted:
            return 200, extracted
    matches = re.findall(
        r'<span class=[\"\']titlelink[\"\']>\s*<a[^>]*href=[\"\']([^\"\']*jobdetail\.ftl\?job=([^&\"\']+)[^\"\']*)[\"\']>([^<]+)</a>(?:.*?<span class=[\"\']location[\"\']>([^<]+))?',
        html, re.DOTALL | re.I
    )
    seen = set()
    for href, jid, title, loc in matches:
        if jid in seen:
            continue
        seen.add(jid)
        loc_str = (loc or "").strip()
        t_clean = title.strip()
        apply_u = href if href.startswith("http") else f"https://{slug}.taleo.net/careersection/ex/{href}"
        extracted.append({
            "job_id": f"taleo_{slug}_{jid}",
            "platform": "taleo",
            "company_name": company_name,
            "company_slug": slug,
            "title": t_clean,
            "location": loc_str,
            "is_us": 1 if is_us_location(loc_str, extra_text=t_clean) else 0,
            "apply_url": apply_u,
            "department": "",
            "office": "",
            "employment_type": "",
            "posted_at": "",
            "updated_at": "",
            "raw_timestamp": "",
            "content": "",
            "extracted_at": now_iso,
        })
    return 200, extracted


async def extract_paycom(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    company_name = slug[:8].title()
    now_iso = datetime.now(timezone.utc).isoformat()
    extracted = []

    # 1. SSR Portal shell embedding session JWT & regional Mantle host
    url_portal = f"https://www.paycomonline.net/v4/ats/web.php/portal/{slug}/jobs/1"
    st, html = await http.get_text(url_portal)
    if st == 200 and html and "Job board does not exist" not in html:
        jwt_m = re.search(r'"sessionJWT":"([^"]+)"', html)
        host_m = re.search(r'portal-applicant-tracking\.[a-z0-9-]+\.paycomonline\.net', html)
        if jwt_m and host_m:
            jwt = jwt_m.group(1)
            host = "https://" + host_m.group(0)
            search_url = f"{host}/api/ats/job-posting-previews/search"
            auth_hdrs = {"Authorization": jwt, "Locale": "en-US", "Content-Type": "application/json"}
            search_body = {
                "skip": 0,
                "take": 50,
                "filtersForQuery": {
                    "distanceFrom": 0,
                    "workEnvironments": [],
                    "positionTypes": [],
                    "educationLevels": [],
                    "categories": [],
                    "travelTypes": [],
                    "shiftTypes": [],
                    "otherFilters": [],
                    "keywordSearchText": "",
                    "location": "",
                    "sortOption": ""
                }
            }
            p_st, p_data = await http.post_json(search_url, search_body, headers=auth_hdrs)
            if p_st == 200 and isinstance(p_data, dict):
                previews = p_data.get("jobPostingPreviews") or []
                for p in previews:
                    jid = str(p.get("jobId") or "")
                    title = (p.get("jobTitle") or "").strip()
                    loc = (p.get("location") or "").strip()
                    rem_type = (p.get("remoteType") or "").strip()
                    is_rem = rem_type.lower() == "remote" or is_us_location(loc, extra_text=title)
                    created = p.get("startDate") or ""
                    extracted.append({
                        "job_id": f"paycom_{slug}_{jid}",
                        "platform": "paycom",
                        "company_name": company_name,
                        "company_slug": slug,
                        "title": title,
                        "location": loc,
                        "is_us": 1 if is_rem else 0,
                        "apply_url": f"https://www.paycomonline.net/v4/ats/web.php/portal/{slug}/jobs/{jid}",
                        "department": p.get("department") or "",
                        "office": "Remote" if rem_type.lower() == "remote" else "",
                        "employment_type": p.get("positionType") or "",
                        "posted_at": parse_to_iso(created),
                        "updated_at": "",
                        "raw_timestamp": str(created),
                        "content": clean_html(p.get("description", "")) if full_content else "",
                        "extracted_at": now_iso,
                    })
                if extracted:
                    return 200, extracted

    # 2. Legacy / fallback URL
    url_leg = f"https://www.paycomonline.net/v4/ats/web.php/jobs?clientkey={slug}"
    status, html_leg = await http.get_text(url_leg)
    if status == 200 and html_leg:
        ld_jobs = extract_jsonld_job_postings(html_leg)
        if ld_jobs:
            for j in ld_jobs:
                title = j.get("title", "").strip()
                loc_obj = j.get("jobLocation", {}).get("address", {}) if isinstance(j.get("jobLocation"), dict) else {}
                loc = f"{loc_obj.get('addressLocality', '')}, {loc_obj.get('addressRegion', '')}".strip(", ")
                u = j.get("url") or ""
                jid = u.split("jobs/")[1].split("?")[0] if "jobs/" in u else str(hash(title))
                date_p = j.get("datePosted") or ""
                extracted.append({
                    "job_id": f"paycom_{slug}_{jid}",
                    "platform": "paycom",
                    "company_name": company_name,
                    "company_slug": slug,
                    "title": title,
                    "location": loc,
                    "is_us": 1 if is_us_location(loc, extra_text=title) else 0,
                    "apply_url": u if u.startswith("http") else f"https://www.paycomonline.net{u}",
                    "department": "",
                    "office": "",
                    "employment_type": j.get("employmentType") or "",
                    "posted_at": parse_to_iso(date_p),
                    "updated_at": "",
                    "raw_timestamp": str(date_p),
                    "content": clean_html(j.get("description", "")) if full_content else "",
                    "extracted_at": now_iso,
                })
            if extracted:
                return 200, extracted
        matches = re.findall(
            r'<a[^>]*href=[\"\']([^\"\']*jobs/(\d+)[^\"\']*)[\"\'][^>]*>.*?<span[^>]*class=[\"\'][^\"\']*title[^\"]*[\"\']>([^<]+)</span>(?:.*?<span[^>]*class=[\"\'][^\"\']*location[^\"]*[\"\']>([^<]+))?',
            html_leg, re.DOTALL | re.I
        )
        seen = set()
        for path, jid, title, loc in matches:
            if jid in seen:
                continue
            seen.add(jid)
            loc_str = (loc or "").strip()
            t_clean = title.strip()
            apply_u = path if path.startswith("http") else f"https://www.paycomonline.net{path}"
            extracted.append({
                "job_id": f"paycom_{slug}_{jid}",
                "platform": "paycom",
                "company_name": company_name,
                "company_slug": slug,
                "title": t_clean,
                "location": loc_str,
                "is_us": 1 if is_us_location(loc_str, extra_text=t_clean) else 0,
                "apply_url": apply_u,
                "department": "",
                "office": "",
                "employment_type": "",
                "posted_at": "",
                "updated_at": "",
                "raw_timestamp": "",
                "content": "",
                "extracted_at": now_iso,
            })
        if extracted:
            return 200, extracted
    return 200 if extracted else (st or status), extracted


async def extract_paylocity(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    company_name = slug[:8].title()
    now_iso = datetime.now(timezone.utc).isoformat()
    extracted = []

    # 1. Native All page embedding window.pageData Jobs array
    url_html = f"https://recruiting.paylocity.com/recruiting/jobs/All/{slug}"
    st, html = await http.get_text(url_html)
    if st == 200 and html:
        # Check window.pageData Jobs array
        m = re.search(r'"Jobs":\s*(\[.*?\])\s*,\s*"', html, re.DOTALL)
        if not m:
            m = re.search(r'window\.pageData\s*=\s*(\{.*?\});', html, re.DOTALL)
        if m:
            try:
                blob = json.loads(m.group(1))
                raw_jobs = blob if isinstance(blob, list) else (blob.get("Jobs") or [])
                for j in raw_jobs:
                    jid = str(j.get("JobId") or j.get("id") or "")
                    title = (j.get("JobTitle") or j.get("title") or "").strip()
                    loc_obj = j.get("JobLocation") or {} if isinstance(j.get("JobLocation"), dict) else {}
                    city = loc_obj.get("City") or ""
                    state = loc_obj.get("State") or ""
                    country = loc_obj.get("Country") or ""
                    loc_name = (j.get("LocationName") or "").strip()
                    loc = f"{city}, {state}".strip(", ") or loc_name or country
                    is_rem = bool(j.get("IsRemote")) or (j.get("IndeedRemoteType") in (1, 2)) or "remote" in loc_name.lower() or "remote" in title.lower()
                    pub_d = j.get("PublishedDate") or j.get("postedDate") or ""
                    extracted.append({
                        "job_id": f"paylocity_{slug}_{jid}",
                        "platform": "paylocity",
                        "company_name": company_name,
                        "company_slug": slug,
                        "title": title,
                        "location": loc,
                        "is_us": 1 if (is_rem or country in ("USA", "US", "United States") or is_us_location(loc, extra_text=title)) else 0,
                        "apply_url": f"https://recruiting.paylocity.com/recruiting/jobs/Details/{jid}/{slug}",
                        "department": j.get("Department") or j.get("department") or "",
                        "office": "Remote" if is_rem else "",
                        "employment_type": j.get("EmploymentType") or j.get("employmentType") or "",
                        "posted_at": parse_to_iso(pub_d),
                        "updated_at": "",
                        "raw_timestamp": str(pub_d),
                        "content": clean_html(j.get("Description", "")) if full_content else "",
                        "extracted_at": now_iso,
                    })
                if extracted:
                    return 200, extracted
            except Exception:
                pass

        # Fallback to jsonld if present
        ld_jobs = extract_jsonld_job_postings(html)
        for j in ld_jobs:
            title = j.get("title", "").strip()
            loc_obj = j.get("jobLocation", {}).get("address", {}) if isinstance(j.get("jobLocation"), dict) else {}
            loc = f"{loc_obj.get('addressLocality', '')}, {loc_obj.get('addressRegion', '')}".strip(", ")
            u = j.get("url") or url_html
            date_p = j.get("datePosted") or ""
            extracted.append({
                "job_id": f"paylocity_{slug}_{hash(title + u)}",
                "platform": "paylocity",
                "company_name": company_name,
                "company_slug": slug,
                "title": title,
                "location": loc,
                "is_us": 1 if is_us_location(loc, extra_text=title) else 0,
                "apply_url": u,
                "department": "",
                "office": "",
                "employment_type": j.get("employmentType") or "",
                "posted_at": parse_to_iso(date_p),
                "updated_at": "",
                "raw_timestamp": str(date_p),
                "content": clean_html(j.get("description", "")) if full_content else "",
                "extracted_at": now_iso,
            })
        if extracted:
            return 200, extracted

    return st, []


async def extract_dayforce(http: AsyncHTTPClient, slug_or_url: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    tenant, site, culture = "", "CANDIDATEPORTAL", "en-US"
    if slug_or_url.startswith("http"):
        p = urlparse(slug_or_url).path.strip("/").split("/")
        if len(p) >= 3:
            culture, tenant, site = p[0], p[1], p[2]
        elif len(p) == 2:
            tenant, site = p[0], p[1]
        elif len(p) == 1:
            tenant = p[0]
    elif "/" in slug_or_url:
        parts = slug_or_url.strip("/").split("/")
        if len(parts) >= 2:
            tenant, site = parts[0], parts[1]
        else:
            tenant = parts[0]
    else:
        tenant = slug_or_url.strip()

    if not tenant:
        return 0, []

    company_name = tenant.replace("-", " ").title()
    now_iso = datetime.now(timezone.utc).isoformat()
    extracted = []

    # 1. Dayforce Geo Search API with CSRF token
    csrf_st, csrf_data = await http.get_json("https://jobs.dayforcehcm.com/api/auth/csrf")
    csrf_token = csrf_data.get("csrfToken") if (csrf_st == 200 and isinstance(csrf_data, dict)) else ""

    headers = {"X-CSRF-TOKEN": csrf_token, "Content-Type": "application/json"} if csrf_token else {"Content-Type": "application/json"}
    search_url = f"https://jobs.dayforcehcm.com/api/geo/{tenant}/jobposting/search"
    body = {
        "clientNamespace": tenant,
        "jobBoardCode": site,
        "cultureCode": culture,
        "paginationStart": 0
    }
    st, res = await http.post_json(search_url, body, headers=headers)
    if st == 200 and isinstance(res, dict):
        postings = res.get("jobPostings") or []
        for p in postings:
            jid = str(p.get("jobPostingId") or "")
            title = (p.get("jobTitle") or "").strip()
            loc_list = p.get("postingLocations") or []
            loc_strs = []
            for a in loc_list:
                if isinstance(a, dict):
                    parts = [a.get("cityName", ""), a.get("stateCode", ""), a.get("isoCountryCode", "")]
                    loc_strs.append(", ".join([pt for pt in parts if pt]))
            loc = "; ".join(loc_strs)
            is_rem = bool(p.get("hasVirtualLocation")) or "remote" in title.lower() or is_us_location(loc, extra_text=title)
            created = p.get("postingStartTimestampUTC") or ""
            apply_u = f"https://jobs.dayforcehcm.com/{culture}/{tenant}/{site}/jobs/{jid}"
            extracted.append({
                "job_id": f"dayforce_{tenant}_{jid}",
                "platform": "dayforce",
                "company_name": company_name,
                "company_slug": tenant,
                "title": title,
                "location": loc,
                "is_us": 1 if is_rem else 0,
                "apply_url": apply_u,
                "department": "",
                "office": "Remote" if p.get("hasVirtualLocation") else "",
                "employment_type": "",
                "posted_at": parse_to_iso(created),
                "updated_at": "",
                "raw_timestamp": str(created),
                "content": clean_html(p.get("jobDescription", "")) if full_content else "",
                "extracted_at": now_iso,
            })
        if extracted:
            return 200, extracted

    # 2. HTML scraping fallback
    url_html = f"https://jobs.dayforcehcm.com/{culture}/{tenant}/{site}"
    st_h, html = await http.get_text(url_html)
    if st_h == 200 and html:
        matches = re.findall(
            r'href=[\"\'](/[^/]+/[^/]+/[^/]+/jobs/(\d+))[\"\'][^>]*>([^<]+)</a>',
            html, re.I
        )
        for path, jid, title in matches:
            t_clean = title.strip()
            extracted.append({
                "job_id": f"dayforce_{tenant}_{jid}",
                "platform": "dayforce",
                "company_name": company_name,
                "company_slug": tenant,
                "title": t_clean,
                "location": "",
                "is_us": 1,
                "apply_url": f"https://jobs.dayforcehcm.com{path}",
                "department": "",
                "office": "",
                "employment_type": "",
                "posted_at": "",
                "updated_at": "",
                "raw_timestamp": "",
                "content": "",
                "extracted_at": now_iso,
            })
        return 200, extracted

    return st if st else st_h, []


async def extract_ukg(http: AsyncHTTPClient, slug_or_url: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    host, tenant, guid = "recruiting2.ultipro.com", "", ""
    if slug_or_url.startswith("http"):
        parsed = urlparse(slug_or_url)
        host = parsed.netloc or host
        p = parsed.path.strip("/").split("/")
        if len(p) >= 3 and p[1].lower() == "jobboard":
            tenant, guid = p[0], p[2]
        elif len(p) >= 1:
            tenant = p[0]
    elif "/" in slug_or_url:
        p = slug_or_url.strip("/").split("/")
        if len(p) >= 3 and p[1].lower() == "jobboard":
            tenant, guid = p[0], p[2]
        elif len(p) >= 2:
            tenant, guid = p[0], p[1]
        else:
            tenant = p[0]
    else:
        tenant = slug_or_url.strip()

    if not tenant:
        return 0, []

    company_name = tenant.replace("-", " ").title()
    now_iso = datetime.now(timezone.utc).isoformat()
    extracted = []

    # If guid is missing, try discovering from tenant page
    if not guid:
        for try_host in (host, "recruiting.ultipro.com"):
            st, html = await http.get_text(f"https://{try_host}/{tenant}")
            if st == 200 and html:
                m = re.search(r'/JobBoard/([a-zA-Z0-9-]+)', html)
                if m:
                    guid = m.group(1)
                    host = try_host
                    break

    # If we have guid, use LoadSearchResults JSON API
    if guid:
        search_url = f"https://{host}/{tenant}/JobBoard/{guid}/JobBoardView/LoadSearchResults"
        body = {"opportunitySearch": {"Top": 50, "Skip": 0}}
        st, data = await http.post_json(search_url, body)
        if st == 200 and isinstance(data, dict):
            opps = data.get("opportunities") or []
            for o in opps:
                jid = str(o.get("Id") or "")
                title = (o.get("Title") or "").strip()
                locs = o.get("Locations") or []
                loc_str = ""
                if locs and isinstance(locs, list):
                    l_first = locs[0] if isinstance(locs[0], dict) else {}
                    addr = l_first.get("Address") or {} if isinstance(l_first.get("Address"), dict) else {}
                    city = addr.get("City") or ""
                    state = (addr.get("State") or {}).get("Name", "") if isinstance(addr.get("State"), dict) else ""
                    country = (addr.get("Country") or {}).get("Name", "") if isinstance(addr.get("Country"), dict) else ""
                    parts = [city, state, country]
                    loc_str = ", ".join([p for p in parts if p]) or (l_first.get("LocalizedName") or "")
                created = o.get("PostedDate") or ""
                desc = o.get("BriefDescription") or ""
                apply_u = f"https://{host}/{tenant}/JobBoard/{guid}/OpportunityDetail?opportunityId={jid}"
                is_rem = "remote" in title.lower() or "remote" in loc_str.lower() or is_us_location(loc_str, extra_text=title)
                extracted.append({
                    "job_id": f"ukg_{tenant}_{jid}",
                    "platform": "ukg",
                    "company_name": company_name,
                    "company_slug": tenant,
                    "title": title,
                    "location": loc_str,
                    "is_us": 1 if is_rem else 0,
                    "apply_url": apply_u,
                    "department": "",
                    "office": "Remote" if "remote" in loc_str.lower() else "",
                    "employment_type": "",
                    "posted_at": parse_to_iso(created),
                    "updated_at": "",
                    "raw_timestamp": str(created),
                    "content": clean_html(desc) if full_content else "",
                    "extracted_at": now_iso,
                })
            if extracted:
                return 200, extracted

    # Fallback to HTML scraping
    url_html = f"https://{host}/{tenant}/JobBoard"
    st_h, html = await http.get_text(url_html)
    if st_h == 200 and html:
        matches = re.findall(
            r'href=[\"\'](/[^/]+/JobBoard/[^/]+/OpportunityDetail\?opportunityId=([a-zA-Z0-9_-]+))[\"\'][^>]*>([^<]+)</a>',
            html, re.I
        )
        seen = set()
        for path, jid, title in matches:
            if jid in seen:
                continue
            seen.add(jid)
            t_clean = title.strip()
            extracted.append({
                "job_id": f"ukg_{tenant}_{jid}",
                "platform": "ukg",
                "company_name": company_name,
                "company_slug": tenant,
                "title": t_clean,
                "location": "",
                "is_us": 1,
                "apply_url": f"https://{host}{path}",
                "department": "",
                "office": "",
                "employment_type": "",
                "posted_at": "",
                "updated_at": "",
                "raw_timestamp": "",
                "content": "",
                "extracted_at": now_iso,
            })
        return 200, extracted

    return 0, []


async def extract_successfactors(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://career.successfactors.com/career?company={slug}"
    status, html = await http.get_text(url)
    if status != 200 or not html:
        return status, []
    company_name = slug.replace("-", " ").title()
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()
    ld_jobs = extract_jsonld_job_postings(html)
    for j in ld_jobs:
        title = j.get("title", "").strip()
        loc_obj = j.get("jobLocation", {}).get("address", {}) if isinstance(j.get("jobLocation"), dict) else {}
        loc = f"{loc_obj.get('addressLocality', '')}, {loc_obj.get('addressRegion', '')}".strip(", ")
        u = j.get("url") or url
        jid = u.split("career_job_req_id=")[1].split("&")[0] if "career_job_req_id=" in u else str(hash(title))
        date_p = j.get("datePosted") or ""
        extracted.append({
            "job_id": f"successfactors_{slug}_{jid}",
            "platform": "successfactors",
            "company_name": company_name,
            "company_slug": slug,
            "title": title,
            "location": loc,
            "is_us": 1 if is_us_location(loc, extra_text=title) else 0,
            "apply_url": u,
            "department": "",
            "office": "",
            "employment_type": j.get("employmentType") or "",
            "posted_at": parse_to_iso(date_p),
            "updated_at": "",
            "raw_timestamp": str(date_p),
            "content": clean_html(j.get("description", "")) if full_content else "",
            "extracted_at": now_iso,
        })
    return 200, extracted


async def extract_workday(http: AsyncHTTPClient, slug_or_url: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Extracts live jobs from Workday via CXS REST API (https://{host}/wday/cxs/{company}/{site}/jobs).
    Supports either full board_url or tenant/instance/site slug formats.
    """
    api_url = None
    company_slug = slug_or_url
    host = ""
    site = ""
    company_name = slug_or_url.split("/")[0].title()

    if slug_or_url.startswith("http"):
        parsed = urlparse(slug_or_url)
        host = parsed.netloc
        site = parsed.path.strip("/")
        company = host.split(".")[0]
        company_slug = company
        company_name = company.replace("-", " ").title()
        api_url = f"https://{host}/wday/cxs/{company}/{site}/jobs"
    elif "/" in slug_or_url:
        parts = slug_or_url.strip("/").split("/")
        if len(parts) >= 3:
            company, instance, site = parts[0], parts[1], parts[2]
            host = f"{company}.{instance}.myworkdayjobs.com"
            company_slug = company
            company_name = company.replace("-", " ").title()
            api_url = f"https://{host}/wday/cxs/{company}/{site}/jobs"
        elif len(parts) == 2:
            company, site = parts[0], parts[1]
            host = f"{company}.myworkdayjobs.com"
            company_slug = company
            company_name = company.replace("-", " ").title()
            api_url = f"https://{host}/wday/cxs/{company}/{site}/jobs"
    else:
        company = slug_or_url
        site = "external"
        host = f"{company}.myworkdayjobs.com"
        company_slug = company
        company_name = company.replace("-", " ").title()
        api_url = f"https://{host}/wday/cxs/{company}/{site}/jobs"

    status, data = await http.post_json(api_url, {"appliedFacets": {}, "limit": 20, "offset": 0})
    if status != 200 or not isinstance(data, dict):
        return status, []

    postings = data.get("jobPostings") or []
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for p in postings:
        title = p.get("title", "").strip()
        loc = p.get("locationsText") or ""
        bullets = p.get("bulletFields") or []
        if not loc and len(bullets) > 1:
            loc = bullets[1]
        ext_path = p.get("externalPath") or ""
        job_id = ext_path.split("_")[-1] if "_" in ext_path else str(hash(title))
        apply_url = f"https://{host}/{site}{ext_path}" if host and site else ext_path
        date_p = p.get("postedOn") or ""

        extracted.append({
            "job_id": f"workday_{company_slug}_{job_id}",
            "platform": "workday",
            "company_name": company_name,
            "company_slug": company_slug,
            "title": title,
            "location": loc,
            "is_us": 1 if is_us_location(loc, extra_text=title) else 0,
            "apply_url": apply_url,
            "department": "",
            "office": "",
            "employment_type": "",
            "posted_at": parse_to_iso(date_p),
            "updated_at": "",
            "raw_timestamp": str(date_p),
            "content": "",
            "extracted_at": now_iso,
        })

    return 200, extracted


async def extract_adp(http: AsyncHTTPClient, slug_or_url: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Extracts live jobs from ADP Workforce Now public recruitment API.
    Handles slug as cid, cid/ccId, or full recruitment board URL.
    """
    raw = slug_or_url.strip()
    cid = raw
    ccid = None
    if "cid=" in raw:
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(raw).query)
        cid = qs.get("cid", [raw])[0]
        ccid = qs.get("ccId", [None])[0]
    elif "/" in raw:
        parts = raw.split("/", 1)
        cid = parts[0]
        ccid = parts[1]

    api_url = f"https://workforcenow.adp.com/mascsr/default/careercenter/public/events/staffing/v1/job-requisitions?cid={cid}&lang=en_US&$top=100&$skip=0"
    if ccid:
        api_url += f"&ccId={ccid}"

    status, data = await http.get_json(api_url)
    if status != 200 or not isinstance(data, dict):
        return status, []

    items = data.get("jobRequisitions") or []
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for item in items:
        item_id = str(item.get("itemID") or "")
        title = (item.get("requisitionTitle") or "").strip()
        locations = [
            loc.get("nameCode", {}).get("shortName", "")
            for loc in item.get("requisitionLocations", [])
            if isinstance(loc, dict)
        ]
        loc_str = "; ".join(filter(None, locations))
        post_date = item.get("postDate") or ""

        pay = item.get("payGradeRange") or {}
        min_rate = (pay.get("minimumRate") or {}).get("amountValue")
        max_rate = (pay.get("maximumRate") or {}).get("amountValue")
        curr = (pay.get("minimumRate") or {}).get("currencyCode") or (pay.get("maximumRate") or {}).get("currencyCode") or "USD"

        job_url = f"https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?cid={cid}&jobId={item_id}&lang=en_US"
        if ccid:
            job_url += f"&ccId={ccid}"

        content_raw = ""
        if full_content and item_id:
            detail_url = f"https://workforcenow.adp.com/mascsr/default/careercenter/public/events/staffing/v1/job-requisitions/{item_id}?cid={cid}&lang=en_US"
            d_status, d_data = await http.get_json(detail_url)
            if d_status == 200 and isinstance(d_data, dict):
                content_raw = d_data.get("requisitionDescription") or ""

        job_rec = {
            "job_id": f"adp_{cid}_{item_id}",
            "platform": "adp",
            "company_name": cid,
            "company_slug": cid,
            "title": title,
            "location": loc_str,
            "is_us": 1 if is_us_location(loc_str, extra_text=title) else 0,
            "apply_url": job_url,
            "department": "",
            "office": "",
            "employment_type": "",
            "posted_at": parse_to_iso(post_date),
            "updated_at": parse_to_iso(post_date),
            "raw_timestamp": str(post_date),
            "content": clean_html(content_raw) if full_content else "",
            "extracted_at": now_iso,
        }
        if min_rate is not None or max_rate is not None:
            job_rec["salary_min"] = min_rate
            job_rec["salary_max"] = max_rate
            job_rec["salary_currency"] = curr

        extracted.append(job_rec)

    return status, extracted


PLATFORM_HANDLERS = {
    # Tier 1 & 2 Modern ATS
    "adp": extract_adp,
    "greenhouse": extract_greenhouse,
    "lever": extract_lever,
    "ashby": extract_ashby,
    "smartrecruiters": extract_smartrecruiters,
    "workable": extract_workable,
    "recruitee": extract_recruitee,
    "breezy": extract_breezy,
    "rippling": extract_rippling,
    "bamboohr": extract_bamboohr,
    "pinpoint": extract_pinpoint,
    "workday": extract_workday,
    # High-Volume & Enterprise ATS
    "gem": extract_gem,
    "dover": extract_dover,
    "jobvite": extract_jobvite,
    "jazzhr": extract_jazzhr,
    "trakstar": extract_trakstar,
    "fountain": extract_fountain,
    "icims": extract_icims,
    "taleo": extract_taleo,
    "oracle": extract_taleo,
    "oracletaleo": extract_taleo,
    "oraclecloud": extract_taleo,
    "paycom": extract_paycom,
    "paylocity": extract_paylocity,
    "dayforce": extract_dayforce,
    "ukg": extract_ukg,
    "successfactors": extract_successfactors,
    "sapsuccessfactor": extract_successfactors,
    "sapsuccessfactors": extract_successfactors,
}


COMMON_TECH_SKILLS = [
    (re.compile(r"\bpython\b", re.I), "Python"),
    (re.compile(r"\bjava\b", re.I), "Java"),
    (re.compile(r"\bgolang\b|\bgo\b", re.I), "Go"),
    (re.compile(r"\brust\b", re.I), "Rust"),
    (re.compile(r"\bc\+\+\b", re.I), "C++"),
    (re.compile(r"\bc#\b|\b\.net\b", re.I), ".NET/C#"),
    (re.compile(r"\breact\b", re.I), "React"),
    (re.compile(r"\bnode\.?js\b", re.I), "Node.js"),
    (re.compile(r"\btypescript\b", re.I), "TypeScript"),
    (re.compile(r"\bjavascript\b", re.I), "JavaScript"),
    (re.compile(r"\bangular\b", re.I), "Angular"),
    (re.compile(r"\bvue\.?js\b", re.I), "Vue.js"),
    (re.compile(r"\bnext\.?js\b", re.I), "Next.js"),
    (re.compile(r"\bswift\b", re.I), "Swift"),
    (re.compile(r"\bkotlin\b", re.I), "Kotlin"),
    (re.compile(r"\baws\b", re.I), "AWS"),
    (re.compile(r"\bazure\b", re.I), "Azure"),
    (re.compile(r"\bgcp\b", re.I), "GCP"),
    (re.compile(r"\bdocker\b", re.I), "Docker"),
    (re.compile(r"\bkubernetes\b|\bk8s\b", re.I), "Kubernetes"),
    (re.compile(r"\bterraform\b", re.I), "Terraform"),
    (re.compile(r"\blinux\b", re.I), "Linux"),
    (re.compile(r"\bsql\b", re.I), "SQL"),
    (re.compile(r"\bpostgresql\b|\bpostgres\b", re.I), "PostgreSQL"),
    (re.compile(r"\bsnowflake\b", re.I), "Snowflake"),
    (re.compile(r"\bdatabricks\b", re.I), "Databricks"),
    (re.compile(r"\bspark\b", re.I), "Apache Spark"),
    (re.compile(r"\bkafka\b", re.I), "Kafka"),
    (re.compile(r"\bsalesforce\b", re.I), "Salesforce"),
    (re.compile(r"\bworkday\b", re.I), "Workday"),
    (re.compile(r"\bservicenow\b", re.I), "ServiceNow"),
    (re.compile(r"\bsap\b", re.I), "SAP"),
    (re.compile(r"\bmachine learning\b|\bdeep learning\b|\bai/ml\b", re.I), "Machine Learning"),
    (re.compile(r"\bllm\b|\bgenai\b|\bgenerative ai\b", re.I), "Generative AI"),
    (re.compile(r"\bdata engineer\b", re.I), "Data Engineering"),
    (re.compile(r"\bdevops\b", re.I), "DevOps"),
    (re.compile(r"\bsre\b", re.I), "SRE"),
    (re.compile(r"\bsecurity\b|\bcybersecurity\b", re.I), "Cybersecurity"),
]


def extract_title_skills(title: str) -> List[str]:
    """Fast regex-based skill extraction from job title when JD body is brief."""
    if not title:
        return []
    found = []
    for pat, name in COMMON_TECH_SKILLS:
        if pat.search(title):
            found.append(name)
    return found


def _is_empty_field(val: Any) -> bool:
    """Checks for None, NaN floats, or blank/null sentinel strings."""
    if val is None:
        return True
    if isinstance(val, float):
        import math
        if math.isnan(val):
            return True
    s = str(val).strip()
    return s == "" or s.lower() in ("nan", "none", "null")


async def fetch_jd_content_on_demand(
    http: AsyncHTTPClient, platform: str, url: str
) -> Tuple[str, Dict[str, Any]]:
    """
    On-demand live JD fetcher for ATS boards whose list endpoints omit full descriptions
    (e.g. Workable, UKG, Paycom, ADP, Ashby, or when full_content was skipped).
    Returns (raw_jd_text, extra_meta_dict).
    """
    if not url or not url.startswith("http"):
        return "", {}
    meta: Dict[str, Any] = {}
    try:
        if "greenhouse.io" in url:
            m = re.search(r"greenhouse\.io/([^/]+)/jobs/(\d+)", url)
            if m:
                api = f"https://boards-api.greenhouse.io/v1/boards/{m.group(1)}/jobs/{m.group(2)}?content=true"
                st, data = await http.get_json(api)
                if st == 200 and isinstance(data, dict):
                    if data.get("education"):
                        meta["education"] = data.get("education")
                    ts = parse_to_iso(data.get("first_published") or data.get("updated_at") or "")
                    if ts:
                        meta["posted_at"] = ts
                    c = data.get("content") or ""
                    if len(c) > 50:
                        return html.unescape(c), meta

        if "lever.co" in url:
            m = re.search(r"lever\.co/([^/]+)/([a-f0-9-]+)", url)
            if m:
                api = f"https://api.lever.co/v0/postings/{m.group(1)}/{m.group(2)}"
                st, data = await http.get_json(api)
                if st == 200 and isinstance(data, dict):
                    ts = parse_to_iso(data.get("createdAt") or "")
                    if ts:
                        meta["posted_at"] = ts
                    desc = data.get("descriptionPlain") or data.get("description", "")
                    for lst in data.get("lists", []):
                        desc += "\n" + (lst.get("text", "") or "") + "\n" + (lst.get("content", "") or "")
                    if len(desc) > 50:
                        return html.unescape(desc), meta

        if "smartrecruiters.com" in url:
            m = re.search(r"smartrecruiters\.com/([^/]+)/(\d+)", url)
            if m:
                api = f"https://api.smartrecruiters.com/v1/companies/{m.group(1)}/postings/{m.group(2)}"
                st, data = await http.get_json(api)
                if st == 200 and isinstance(data, dict):
                    ts = parse_to_iso(data.get("releasedDate") or "")
                    if ts:
                        meta["posted_at"] = ts
                    sec = data.get("jobAd", {}).get("sections", {})
                    desc = (sec.get("jobDescription", {}).get("text", "") or "") + "\n" + (sec.get("qualifications", {}).get("text", "") or "")
                    if len(desc) > 50:
                        return html.unescape(desc), meta

        if "workforcenow.adp.com" in url:
            m_cid = re.search(r"cid=([^&]+)", url)
            m_jid = re.search(r"jobId=([^&]+)", url)
            if m_cid and m_jid:
                api = f"https://workforcenow.adp.com/mascsr/default/careercenter/public/events/staffing/v1/job-requisitions/{m_jid.group(1)}?cid={m_cid.group(1)}&lang=en_US"
                st, data = await http.get_json(api)
                if st == 200 and isinstance(data, dict):
                    ts = parse_to_iso(data.get("postDate") or data.get("postedDate") or "")
                    if ts:
                        meta["posted_at"] = ts
                    desc = data.get("requisitionDescription") or ""
                    if len(desc) > 50:
                        return html.unescape(desc), meta

        st, html_text = await http.get_text(url)
        if st != 200 or not html_text:
            return "", meta

        if "CandidateOpportunityDetail" in html_text:
            m_ukg = re.search(r"new\s+US\.Opportunity\.CandidateOpportunityDetail\((.*?)\);", html_text, re.DOTALL)
            if m_ukg:
                try:
                    u_data = json.loads(m_ukg.group(1))
                    desc = u_data.get("Description") or ""
                    ts = parse_to_iso(u_data.get("PostedDate") or u_data.get("UpdatedDate") or "")
                    if ts:
                        meta["posted_at"] = ts
                    if u_data.get("CompensationAnnualMinimum") or u_data.get("CompensationAnnualMaximum"):
                        meta["salary_min"] = u_data.get("CompensationAnnualMinimum")
                        meta["salary_max"] = u_data.get("CompensationAnnualMaximum")
                        meta["salary_currency"] = u_data.get("CompensationCurrencyCode") or "USD"
                    if len(desc) > 50:
                        return html.unescape(desc), meta
                except Exception:
                    pass

        for m in re.finditer(r"<script[^>]*>(.*?)</script>", html_text, re.DOTALL | re.IGNORECASE):
            s_body = m.group(1).strip()
            if "JobPosting" in s_body and ("@context" in s_body or "schema.org" in s_body):
                try:
                    data = json.loads(s_body)
                    if isinstance(data, list):
                        data = data[0]
                    if isinstance(data, dict):
                        if "@graph" in data and isinstance(data["@graph"], list):
                            for node in data["@graph"]:
                                if isinstance(node, dict) and node.get("@type") == "JobPosting":
                                    data = node
                                    break
                        if data.get("@type") == "JobPosting":
                            desc = data.get("description") or ""
                            ts = parse_to_iso(data.get("datePosted") or "")
                            if ts:
                                meta["posted_at"] = ts
                            bs = data.get("baseSalary")
                            if isinstance(bs, dict) and isinstance(bs.get("value"), dict):
                                val = bs["value"]
                                meta["salary_min"] = val.get("minValue")
                                meta["salary_max"] = val.get("maxValue")
                                meta["salary_currency"] = bs.get("currency", "USD")
                            if len(desc) > 80:
                                return html.unescape(desc), meta
                except Exception:
                    pass

        if HAS_BS4:
            soup = BeautifulSoup(html_text[:140000], "html.parser")
            for tag in soup(["script", "style", "nav", "footer", "header", "noscript"]):
                tag.decompose()
            body = soup.get_text(separator="\n", strip=True)
            return (body if len(body) > 200 else ""), meta
        plain = clean_html(html_text[:140000])
        return (plain if len(plain) > 200 else ""), meta
    except Exception:
        return "", meta


def enrich_job_record(raw: Dict[str, Any], enable_enrichment: bool = True) -> Dict[str, Any]:
    """Enriches job dictionary with salary, skills, H-1B, and experience facts."""
    out = dict(raw)
    out.setdefault("job_id", "")
    out.setdefault("platform", "")
    out.setdefault("company_name", "")
    out.setdefault("company_slug", "")
    out.setdefault("title", "")
    out.setdefault("location", "")
    out.setdefault("is_us", 0)
    out.setdefault("apply_url", "")
    out.setdefault("department", "")
    out.setdefault("office", "")
    out.setdefault("employment_type", "")
    out.setdefault("posted_at", "")
    out.setdefault("updated_at", "")
    out.setdefault("raw_timestamp", "")
    out.setdefault("content", "")
    out.setdefault("salary", "")
    out.setdefault("salary_min", None)
    out.setdefault("salary_max", None)
    out.setdefault("salary_currency", "")
    out.setdefault("salary_period", "")
    out.setdefault("skills", "")
    out.setdefault("skills_required", "")
    out.setdefault("skills_preferred", "")
    out.setdefault("work_mode", "")
    out.setdefault("experience_min", None)
    out.setdefault("experience_max", None)
    out.setdefault("education", "")
    out.setdefault("study_qualifications", "")
    out.setdefault("h1b_status", "unknown")
    out.setdefault("visa_sponsorship_status", "unknown")
    out.setdefault("sponsorship_evidence", "")
    out.setdefault("reports_to", "")
    out.setdefault("is_enriched", 0)
    now_iso = datetime.now(timezone.utc).isoformat()
    out.setdefault("extracted_at", now_iso)
    out.setdefault("first_seen_at", now_iso)
    out.setdefault("last_seen_at", now_iso)
    if _is_empty_field(out.get("posted_at")):
        out["posted_at"] = out["first_seen_at"]
        out.setdefault("date_source", "first_seen")
    else:
        out.setdefault("date_source", "ats_api")

    if not enable_enrichment or not HAS_ENRICHMENT:
        return out

    out["is_enriched"] = 1

    desc = raw.get("content") or raw.get("description") or ""
    title = raw.get("title") or ""
    location = raw.get("location") or ""
    company = raw.get("company_name") or ""
    platform = raw.get("platform") or ""

    if desc:
        # 1. Main JD extraction engine
        try:
            eng = extract_job({
                "title": title,
                "location": location,
                "description": desc,
                "company": company,
                "source_board": platform,
            })
            if eng:
                out["salary"] = eng.get("salary") or ""
                out["salary_min"] = eng.get("salary_min")
                out["salary_max"] = eng.get("salary_max")
                out["salary_currency"] = eng.get("salary_currency") or ""
                out["salary_period"] = eng.get("salary_period") or ""
                out["skills"] = eng.get("skills") or ""
                out["skills_required"] = ", ".join(eng.get("skills_required") or [])
                out["skills_preferred"] = ", ".join(eng.get("skills_preferred") or [])
                out["work_mode"] = eng.get("work_mode") or ""
                out["experience_min"] = eng.get("experience_min")
                out["experience_max"] = eng.get("experience_max")
                edu = eng.get("education_requirements") or []
                out["education"] = ", ".join(edu) if isinstance(edu, list) else str(edu)
                if eng.get("study_qualifications"):
                    out["study_qualifications"] = eng.get("study_qualifications") or ""
        except Exception:
            pass

        # 2. Fallback salary parser
        if not out.get("salary_min") and not out.get("salary"):
            try:
                sal = parse_salary(desc)
                if sal.get("salary_min") or sal.get("salary_max"):
                    out["salary_min"] = sal.get("salary_min")
                    out["salary_max"] = sal.get("salary_max")
                    out["salary_currency"] = sal.get("salary_currency") or "USD"
                    out["salary_period"] = sal.get("salary_period") or "year"
                    from scrapers.job_extraction.engine import format_salary
                    out["salary"] = format_salary(
                        out["salary_min"], out["salary_max"], out["salary_currency"], out["salary_period"]
                    )
            except Exception:
                pass

        # 3. JD Facts: H-1B, Visa Sponsorship, and Reporting Line
        try:
            facts = extract_jd_facts(desc, include_industry=False)
            if facts:
                out["h1b_status"] = facts.h1b_status or "unknown"
                out["visa_sponsorship_status"] = facts.visa_sponsorship_status or "unknown"
                out["sponsorship_evidence"] = " | ".join(facts.sponsorship_evidence)
                out["reports_to"] = facts.reports_to or ""
        except Exception:
            pass

    # 4. Work mode fallback + canonical normalization ("On-site" -> "Onsite")
    if _is_empty_field(out.get("work_mode")):
        try:
            wm = detect_work_mode(raw)
            if wm:
                out["work_mode"] = wm.title()
        except Exception:
            pass
    if str(out.get("work_mode") or "").lower().strip() in ("on-site", "on site", "in-office", "in-person"):
        out["work_mode"] = "Onsite"

    # Normalize salary_period
    _sp = str(out.get("salary_period") or "").lower().strip()
    if _sp in ("annual", "annually", "yearly", "yr"):
        out["salary_period"] = "year"
    elif _sp in ("hourly", "hr"):
        out["salary_period"] = "hour"
    elif _sp in ("monthly", "mo"):
        out["salary_period"] = "month"

    # 5. Title-based Skill Extraction fallback
    if not out.get("skills_required") and not out.get("skills_preferred") and not out.get("skills"):
        t_skills = extract_title_skills(title)
        if t_skills:
            out["skills_required"] = ", ".join(t_skills)
            out["skills"] = ", ".join(t_skills)

    # 6. Title-based Seniority / Experience fallback
    if out.get("experience_min") is None:
        t_low = title.lower()
        if any(w in t_low for w in ["intern", "internship", "co-op", "apprentice"]):
            out["experience_min"] = 0.0
        elif any(w in t_low for w in ["junior", "entry level", "associate"]):
            out["experience_min"] = 1.0
        elif any(w in t_low for w in ["senior", "sr.", "sr ", "lead"]):
            out["experience_min"] = 5.0
        elif any(w in t_low for w in ["staff", "principal"]):
            out["experience_min"] = 8.0
        elif any(w in t_low for w in ["director", "vp", "head of", "chief"]):
            out["experience_min"] = 10.0

    # 7. Education & Study Qualifications fallback (handles NaN/empty safely)
    if _is_empty_field(out.get("education")):
        t_low = title.lower()
        if "scientist" in t_low or "research" in t_low:
            out["education"] = "Master's Degree / PhD preferred"
        elif any(w in t_low for w in ["lead", "principal", "staff", "architect", "director"]):
            out["education"] = "Bachelor's Degree in related field"
        elif any(w in t_low for w in ["senior", "sr.", "sr "]):
            out["education"] = "Bachelor's Degree required; Master's preferred"
        else:
            out["education"] = "Bachelor's Degree or equivalent experience"

    if _is_empty_field(out.get("study_qualifications")):
        out["study_qualifications"] = f"Degree: {out['education']}"

    return out


def init_db(db_path: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS live_jobs (
            job_id TEXT PRIMARY KEY,
            platform TEXT NOT NULL,
            company_name TEXT,
            company_slug TEXT NOT NULL,
            title TEXT NOT NULL,
            location TEXT,
            is_us INTEGER DEFAULT 0,
            apply_url TEXT,
            department TEXT,
            office TEXT,
            employment_type TEXT,
            posted_at TEXT,
            updated_at TEXT,
            raw_timestamp TEXT,
            content TEXT,
            salary TEXT,
            salary_min REAL,
            salary_max REAL,
            salary_currency TEXT,
            salary_period TEXT,
            skills TEXT,
            skills_required TEXT,
            skills_preferred TEXT,
            work_mode TEXT,
            experience_min REAL,
            experience_max REAL,
            education TEXT,
            study_qualifications TEXT,
            h1b_status TEXT,
            visa_sponsorship_status TEXT,
            sponsorship_evidence TEXT,
            reports_to TEXT,
            is_enriched INTEGER DEFAULT 0,
            extracted_at TEXT NOT NULL,
            first_seen_at TEXT,
            last_seen_at TEXT,
            date_source TEXT
        );
    """)

    # Auto-migrate table if created under an older schema version
    cur.execute("PRAGMA table_info(live_jobs);")
    existing_cols = {r[1] for r in cur.fetchall()}
    columns_to_add = [
        ("is_india", "INTEGER DEFAULT 0"),
        ("salary", "TEXT"),
        ("salary_min", "REAL"),
        ("salary_max", "REAL"),
        ("salary_currency", "TEXT"),
        ("salary_period", "TEXT"),
        ("skills", "TEXT"),
        ("skills_required", "TEXT"),
        ("skills_preferred", "TEXT"),
        ("work_mode", "TEXT"),
        ("experience_min", "REAL"),
        ("experience_max", "REAL"),
        ("education", "TEXT"),
        ("study_qualifications", "TEXT"),
        ("h1b_status", "TEXT"),
        ("visa_sponsorship_status", "TEXT"),
        ("sponsorship_evidence", "TEXT"),
        ("reports_to", "TEXT"),
        ("is_enriched", "INTEGER DEFAULT 0"),
        ("first_seen_at", "TEXT"),
        ("last_seen_at", "TEXT"),
        ("date_source", "TEXT"),
    ]
    for col_name, col_type in columns_to_add:
        if col_name not in existing_cols:
            cur.execute(f"ALTER TABLE live_jobs ADD COLUMN {col_name} {col_type};")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS scanned_boards (
            platform TEXT NOT NULL,
            company_slug TEXT NOT NULL,
            http_status INTEGER NOT NULL,
            total_jobs INTEGER DEFAULT 0,
            us_jobs INTEGER DEFAULT 0,
            scanned_at TEXT NOT NULL,
            PRIMARY KEY (platform, company_slug)
        );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_platform ON live_jobs(platform);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_company ON live_jobs(company_slug);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_posted ON live_jobs(posted_at);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_first_seen ON live_jobs(first_seen_at);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_us ON live_jobs(is_us);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_india ON live_jobs(is_india);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_sal ON live_jobs(salary_min);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_h1b ON live_jobs(h1b_status);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_boards_status ON scanned_boards(http_status);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_boards_platform ON scanned_boards(platform);")
    conn.commit()
    conn.close()


def save_boards_batch(db_path: str, board_records: List[Tuple[str, str, int, int, int, str]]):
    if not board_records:
        return
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.executemany("""
        INSERT OR REPLACE INTO scanned_boards (
            platform, company_slug, http_status, total_jobs, us_jobs, scanned_at
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, board_records)
    conn.commit()
    conn.close()


def sanitize_sqlite_value(val: Any) -> Any:
    """Ensures values bound to SQLite are primitive types (str, int, float, None) within 64-bit limits."""
    if val is None:
        return None
    if isinstance(val, bool):
        return int(val)
    if isinstance(val, int):
        if val > 9223372036854775807 or val < -9223372036854775808:
            return str(val)
        return val
    if isinstance(val, float):
        import math
        if math.isnan(val) or math.isinf(val):
            return None
        return val
    if isinstance(val, (str, bytes)):
        return val
    if isinstance(val, (list, tuple, set)):
        return ", ".join(str(x) for x in val if x is not None)
    if isinstance(val, dict):
        for k in ("name", "label", "text", "value", "title", "name_text", "addressLocality"):
            if k in val and val[k]:
                return str(val[k])
        try:
            return json.dumps(val, default=str)
        except Exception:
            return str(val)
    return str(val)


def save_jobs_batch(db_path: str, jobs: List[Dict[str, Any]], enable_enrichment: bool = False, keep_content: bool = False):
    if not jobs:
        return
    prepared = []
    for j in jobs:
        enr = enrich_job_record(j, enable_enrichment=enable_enrichment)
        if "is_india" not in enr:
            loc = enr.get("location") or ""
            title = enr.get("title") or ""
            enr["is_india"] = 1 if is_india_location(loc, extra_text=title) else 0
        if not keep_content and "content" in enr:
            enr["content"] = ""
        clean = {k: sanitize_sqlite_value(v) for k, v in enr.items()}
        prepared.append(clean)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("PRAGMA synchronous = OFF;")
    cur.execute("PRAGMA journal_mode = WAL;")
    cur.executemany("""
        INSERT INTO live_jobs (
            job_id, platform, company_name, company_slug, title, location, is_us, is_india,
            apply_url, department, office, employment_type, posted_at, updated_at,
            raw_timestamp, content, salary, salary_min, salary_max, salary_currency,
            salary_period, skills, skills_required, skills_preferred, work_mode,
            experience_min, experience_max, education, study_qualifications, h1b_status,
            visa_sponsorship_status, sponsorship_evidence, reports_to, is_enriched,
            extracted_at, first_seen_at, last_seen_at, date_source
        ) VALUES (
            :job_id, :platform, :company_name, :company_slug, :title, :location, :is_us, :is_india,
            :apply_url, :department, :office, :employment_type, :posted_at, :updated_at,
            :raw_timestamp, :content, :salary, :salary_min, :salary_max, :salary_currency,
            :salary_period, :skills, :skills_required, :skills_preferred, :work_mode,
            :experience_min, :experience_max, :education, :study_qualifications, :h1b_status,
            :visa_sponsorship_status, :sponsorship_evidence, :reports_to, :is_enriched,
            :extracted_at, :first_seen_at, :last_seen_at, :date_source
        )
        ON CONFLICT(job_id) DO UPDATE SET
            platform = excluded.platform,
            company_name = excluded.company_name,
            company_slug = excluded.company_slug,
            title = excluded.title,
            location = excluded.location,
            is_us = excluded.is_us,
            is_india = excluded.is_india,
            apply_url = excluded.apply_url,
            department = excluded.department,
            office = excluded.office,
            employment_type = excluded.employment_type,
            posted_at = CASE
                WHEN live_jobs.date_source IN ('ats_api', 'json_ld') AND live_jobs.posted_at IS NOT NULL AND live_jobs.posted_at != ''
                    THEN live_jobs.posted_at
                WHEN excluded.date_source IN ('ats_api', 'json_ld') AND excluded.posted_at IS NOT NULL AND excluded.posted_at != ''
                    THEN excluded.posted_at
                ELSE COALESCE(NULLIF(live_jobs.posted_at, ''), excluded.posted_at)
            END,
            updated_at = excluded.updated_at,
            raw_timestamp = excluded.raw_timestamp,
            content = CASE WHEN length(excluded.content) > 0 THEN excluded.content ELSE live_jobs.content END,
            salary = COALESCE(NULLIF(excluded.salary, ''), live_jobs.salary),
            salary_min = COALESCE(excluded.salary_min, live_jobs.salary_min),
            salary_max = COALESCE(excluded.salary_max, live_jobs.salary_max),
            salary_currency = COALESCE(NULLIF(excluded.salary_currency, ''), live_jobs.salary_currency),
            salary_period = COALESCE(NULLIF(excluded.salary_period, ''), live_jobs.salary_period),
            skills = COALESCE(NULLIF(excluded.skills, ''), live_jobs.skills),
            skills_required = COALESCE(NULLIF(excluded.skills_required, ''), live_jobs.skills_required),
            skills_preferred = COALESCE(NULLIF(excluded.skills_preferred, ''), live_jobs.skills_preferred),
            work_mode = COALESCE(NULLIF(excluded.work_mode, ''), live_jobs.work_mode),
            experience_min = COALESCE(excluded.experience_min, live_jobs.experience_min),
            experience_max = COALESCE(excluded.experience_max, live_jobs.experience_max),
            education = COALESCE(NULLIF(excluded.education, ''), live_jobs.education),
            study_qualifications = COALESCE(NULLIF(excluded.study_qualifications, ''), live_jobs.study_qualifications),
            h1b_status = CASE WHEN excluded.h1b_status != 'unknown' THEN excluded.h1b_status ELSE live_jobs.h1b_status END,
            visa_sponsorship_status = CASE WHEN excluded.visa_sponsorship_status != 'unknown' THEN excluded.visa_sponsorship_status ELSE live_jobs.visa_sponsorship_status END,
            sponsorship_evidence = COALESCE(NULLIF(excluded.sponsorship_evidence, ''), live_jobs.sponsorship_evidence),
            reports_to = COALESCE(NULLIF(excluded.reports_to, ''), live_jobs.reports_to),
            is_enriched = MAX(live_jobs.is_enriched, excluded.is_enriched),
            extracted_at = excluded.extracted_at,
            first_seen_at = COALESCE(NULLIF(live_jobs.first_seen_at, ''), excluded.first_seen_at),
            last_seen_at = excluded.last_seen_at,
            date_source = CASE
                WHEN live_jobs.date_source IN ('ats_api', 'json_ld') THEN live_jobs.date_source
                ELSE excluded.date_source
            END
    """, prepared)
    conn.commit()
    conn.close()


def export_sqlite_to_csv(db_path: str, csv_path: str, only_us: bool = False, country: str = "us", include_content: bool = False):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(live_jobs);")
    cols = [r[1] for r in cur.fetchall()]
    if not include_content and "content" in cols:
        cols = [c for c in cols if c != "content"]
    col_str = ", ".join(f'"{c}"' for c in cols)
    query = f"SELECT {col_str} FROM live_jobs"
    where_parts = []
    if only_us or country == "us":
        where_parts.append("is_us = 1")
    elif country == "india":
        where_parts.append("is_india = 1")
    elif country == "us_and_india":
        where_parts.append("(is_us = 1 OR is_india = 1)")

    if where_parts:
        query += " WHERE " + " AND ".join(where_parts)
    query += " ORDER BY posted_at DESC"
    cur.execute(query)
    columns = [desc[0] for desc in cur.description]

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        while True:
            rows = cur.fetchmany(5000)
            if not rows:
                break
            writer.writerows(rows)
            
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    cur.execute(f"SELECT COUNT(*) FROM live_jobs {where_sql};")
    count = cur.fetchone()[0]
    conn.close()
    print(f"Exported {count:,} jobs to '{csv_path}' (country={country}, include_content={include_content}).")


def merge_databases(input_patterns: List[str], output_db: str, export_csv: Optional[str] = None, country: str = "us"):
    """Merges multiple shard SQLite databases into a unified database and optional CSV."""
    resolved_paths = []
    for pat in input_patterns:
        for p in pat.split(","):
            p = p.strip()
            if not p:
                continue
            matched = glob.glob(p)
            if matched:
                resolved_paths.extend(matched)
            elif os.path.exists(p):
                resolved_paths.append(p)

    resolved_paths = sorted(list(set(resolved_paths)))
    # Exclude output_db if it was matched in the glob
    out_abs = os.path.abspath(output_db)
    resolved_paths = [p for p in resolved_paths if os.path.abspath(p) != out_abs]

    if not resolved_paths:
        print(f"No valid shard database files found matching: {input_patterns}")
        return

    print(f"\nMerging {len(resolved_paths)} shard databases into '{output_db}':")
    for p in resolved_paths:
        print(f"  • {p}")

    init_db(output_db)
    conn_out = sqlite3.connect(output_db)
    cur_out = conn_out.cursor()

    total_merged_boards = 0
    total_merged_jobs = 0

    for path in resolved_paths:
        try:
            conn_in = sqlite3.connect(path)
            cur_in = conn_in.cursor()

            # Merge scanned_boards
            try:
                cur_in.execute("SELECT platform, company_slug, http_status, total_jobs, us_jobs, scanned_at FROM scanned_boards;")
                boards = cur_in.fetchall()
                if boards:
                    cur_out.executemany("""
                        INSERT OR REPLACE INTO scanned_boards (
                            platform, company_slug, http_status, total_jobs, us_jobs, scanned_at
                        ) VALUES (?, ?, ?, ?, ?, ?)
                    """, boards)
                    total_merged_boards += len(boards)
            except Exception as e:
                print(f"Notice on merging boards from {path}: {e}")

            # Merge live_jobs
            try:
                cur_in.execute("SELECT * FROM live_jobs;")
                cols = [d[0] for d in cur_in.description]
                jobs = cur_in.fetchall()
                if jobs:
                    placeholders = ", ".join(["?"] * len(cols))
                    col_names = ", ".join(cols)
                    cur_out.executemany(f"""
                        INSERT OR REPLACE INTO live_jobs ({col_names})
                        VALUES ({placeholders})
                    """, jobs)
                    total_merged_jobs += len(jobs)
            except Exception as e:
                print(f"Notice on merging jobs from {path}: {e}")

            conn_in.close()
        except Exception as e:
            print(f"Error reading shard database {path}: {e}")

    conn_out.commit()
    conn_out.close()
    print(f"\nMerge Complete! Unified database now contains:")
    print(f"  • Total Scanned Boards: {total_merged_boards:,}")
    print(f"  • Total Live Jobs: {total_merged_jobs:,}")

    if export_csv:
        export_sqlite_to_csv(output_db, export_csv, country=country)


def load_companies(input_path: str, default_platform: str) -> List[Tuple[str, str]]:
    """Loads list of (platform, slug) pairs from CSV or TXT file."""
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    pairs = []
    if input_path.endswith(".csv"):
        with open(input_path, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv.DictReader(f)
            for row in reader:
                slug = (row.get("slug") or row.get("company_slug") or "").strip()
                ats = (row.get("ats_id") or row.get("platform") or default_platform).strip().lower()
                board_url = (row.get("board_url") or "").strip()
                if ats == "workday" and board_url:
                    slug = board_url
                if ats in ("adp", "ukg", "dayforce") and board_url:
                    slug = board_url
                if slug:
                    pairs.append((ats, slug))
    else:
        with open(input_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                s = line.strip().lower()
                if s and not s.startswith("#"):
                    # Check for "platform,slug" format
                    if "," in s:
                        p, sl = s.split(",", 1)
                        pairs.append((p.strip(), sl.strip()))
                    else:
                        pairs.append((default_platform, s))
    return pairs


async def run_extractor(
    targets: List[Tuple[str, str]],
    output_db: str,
    export_csv: Optional[str] = None,
    concurrency: int = 80,
    full_content: bool = False,
    only_us: bool = False,
    country: str = "us",
    days: int = 0,
    keyword: Optional[str] = None,
    batch_size: int = 100,
    enable_enrichment: bool = False,
):
    init_db(output_db)
    total_companies = len(targets)
    total_jobs_saved = 0
    jobs_with_post_time = 0
    boards_with_200 = 0
    boards_with_jobs = 0
    scanned_count = 0

    cutoff_dt = None
    if days > 0:
        cutoff_dt = datetime.now(timezone.utc) - timedelta(days=days)

    kw_clean = keyword.lower().strip() if keyword else ""

    print(f"Starting Hyper-Parallel job extraction across {total_companies:,} companies...")
    print(f"  • Concurrency: {concurrency} (Continuous Worker Pool, Zero-Head-of-Line-Blocking)")
    print(f"  • Database: {output_db}")
    print(f"  • Full Content (HTML): {full_content}")
    print(f"  • JD Enrichment (Salary, Skills, H-1B): {enable_enrichment and HAS_ENRICHMENT}")
    print(f"  • Only US Filter: {only_us}")
    if days > 0:
        print(f"  • Recency Filter: Last {days} days (>= {cutoff_dt.isoformat()})")
    if kw_clean:
        print(f"  • Keyword Filter: '{kw_clean}'")
    print()

    # 1. Round-robin interleave targets across ATS platforms to distribute network load across hosts
    by_plat = defaultdict(list)
    for p, s in targets:
        by_plat[p].append((p, s))

    interleaved_targets = []
    max_plat_len = max((len(lst) for lst in by_plat.values()), default=0)
    for i in range(max_plat_len):
        for p, lst in by_plat.items():
            if i < len(lst):
                interleaved_targets.append(lst[i])

    queue: asyncio.Queue[Tuple[str, str]] = asyncio.Queue()
    for t in interleaved_targets:
        queue.put_nowait(t)

    # 2. In-memory buffer for asynchronous batched SQLite writes
    board_buffer = []
    job_buffer = []
    buffer_lock = asyncio.Lock()
    is_done = False

    async def db_writer():
        nonlocal total_jobs_saved
        while not is_done or board_buffer or job_buffer:
            await asyncio.sleep(1.2)
            async with buffer_lock:
                if board_buffer:
                    to_write_boards = list(board_buffer)
                    board_buffer.clear()
                    save_boards_batch(output_db, to_write_boards)
                if job_buffer:
                    to_write_jobs = list(job_buffer)
                    job_buffer.clear()
                    save_jobs_batch(output_db, to_write_jobs, enable_enrichment=enable_enrichment, keep_content=full_content)
                    total_jobs_saved += len(to_write_jobs)

    # 3. Continuous worker pool with strict per-board timeout
    async with AsyncHTTPClient(concurrency=concurrency, per_host_concurrency=15, min_host_interval=0.03, timeout=12.0) as http:
        db_task = asyncio.create_task(db_writer())

        async def worker():
            nonlocal boards_with_200, boards_with_jobs, jobs_with_post_time, scanned_count
            fetch_content = full_content or enable_enrichment
            while True:
                try:
                    platform, slug = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                handler = PLATFORM_HANDLERS.get(platform)
                status_code, comp_jobs = 0, []
                if handler:
                    try:
                        res = await asyncio.wait_for(handler(http, slug, fetch_content), timeout=16.0)
                        if isinstance(res, tuple):
                            status_code, comp_jobs = res
                        elif isinstance(res, list):
                            status_code, comp_jobs = 200, res
                    except Exception:
                        status_code, comp_jobs = 0, []

                now_iso = datetime.now(timezone.utc).isoformat()
                if status_code == 200:
                    boards_with_200 += 1

                valid_comp_jobs = []
                for j in comp_jobs:
                    # Country location filtering
                    is_us = bool(j.get("is_us"))
                    is_in = bool(j.get("is_india")) or is_india_location(j.get("location", ""), extra_text=j.get("title", ""))
                    if (only_us or country == "us") and not is_us:
                        continue
                    elif country == "india" and not is_in:
                        continue
                    elif country == "us_and_india" and not (is_us or is_in):
                        continue
                    if cutoff_dt and j.get("posted_at"):
                        try:
                            j_dt = datetime.fromisoformat(j["posted_at"].replace("Z", "+00:00"))
                            if j_dt < cutoff_dt:
                                continue
                        except Exception:
                            pass
                    if kw_clean:
                        search_corpus = f"{j.get('title', '')} {j.get('department', '')} {j.get('location', '')}".lower()
                        if kw_clean not in search_corpus:
                            continue
                    valid_comp_jobs.append(j)
                    if j.get("posted_at"):
                        jobs_with_post_time += 1

                if enable_enrichment and valid_comp_jobs:
                    async def _fill_jd(job_item: Dict[str, Any]):
                        if not (job_item.get("content") or job_item.get("description")) or not job_item.get("posted_at"):
                            jd_txt, extra_meta = await fetch_jd_content_on_demand(
                                http, platform, job_item.get("apply_url") or ""
                            )
                            if jd_txt and not (job_item.get("content") or job_item.get("description")):
                                job_item["content"] = clean_html(jd_txt)
                            if extra_meta.get("posted_at") and not job_item.get("posted_at"):
                                job_item["posted_at"] = extra_meta["posted_at"]
                                job_item["date_source"] = "json_ld"
                            if extra_meta.get("education") and not job_item.get("education"):
                                job_item["education"] = extra_meta["education"]
                            if extra_meta.get("salary_min") and not job_item.get("salary_min"):
                                job_item["salary_min"] = extra_meta["salary_min"]
                                job_item["salary_max"] = extra_meta.get("salary_max")
                                job_item["salary_currency"] = extra_meta.get("salary_currency", "USD")
                    await asyncio.gather(*(_fill_jd(j) for j in valid_comp_jobs), return_exceptions=True)

                if len(comp_jobs) > 0:
                    boards_with_jobs += 1

                async with buffer_lock:
                    board_buffer.append((platform, slug, status_code, len(comp_jobs), len(valid_comp_jobs), now_iso))
                    if valid_comp_jobs:
                        job_buffer.extend(valid_comp_jobs)

                queue.task_done()
                scanned_count += 1
                if scanned_count % 100 == 0 or scanned_count == total_companies:
                    pct = (scanned_count / total_companies) * 100
                    rate_200 = (boards_with_200 / max(1, scanned_count)) * 100
                    current_jobs = total_jobs_saved + len(job_buffer)
                    ts_rate = (jobs_with_post_time / max(1, current_jobs)) * 100
                    print(
                        f"[{scanned_count:,}/{total_companies:,} ({pct:.1f}%)] Scanned | Boards 200 OK: {boards_with_200:,}/{scanned_count:,} ({rate_200:.1f}%) | Live Jobs: {current_jobs:,} | Timestamp Rate: {jobs_with_post_time}/{max(1, current_jobs)} ({ts_rate:.1f}%)",
                        flush=True,
                    )

        worker_count = min(concurrency, max(10, total_companies))
        workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
        await asyncio.gather(*workers)

        is_done = True
        await db_task

    print(f"\nExtraction completed successfully!", flush=True)
    print(f"Total Boards Returning HTTP 200: {boards_with_200:,} / {total_companies:,} ({((boards_with_200 / max(1, total_companies)) * 100):.1f}%)", flush=True)
    print(f"Total live jobs in '{output_db}': {total_jobs_saved:,}", flush=True)
    if export_csv:
        export_sqlite_to_csv(output_db, export_csv, only_us=only_us, country=country)


def main():
    parser = argparse.ArgumentParser(description="Universal Multi-Platform ATS Job Extractor")
    parser.add_argument("--platform", default="greenhouse", choices=list(PLATFORM_HANDLERS.keys()) + ["all"],
                        help="ATS platform to target (or 'all' for auto-routing)")
    parser.add_argument("--slug", help="Single company slug to extract immediately")
    parser.add_argument("--input", "-i", help="Input file (CSV or TXT) containing companies/slugs")
    parser.add_argument("--output-db", default="all_ats_jobs.db", help="SQLite database output file")
    parser.add_argument("--export-csv", help="Optional CSV file to export extracted jobs to")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of companies to scan (0 = all)")
    parser.add_argument("--offset", type=int, default=0, help="Offset index into input list")
    parser.add_argument("--shard-index", type=int, default=0, help="Zero-based index of this shard runner")
    parser.add_argument("--total-shards", type=int, default=1, help="Total number of parallel runner shards")
    parser.add_argument("--merge-dbs", nargs="+", help="Merge one or more shard databases into --output-db")
    parser.add_argument("--concurrency", type=int, default=80, help="Max async requests concurrency")
    parser.add_argument("--full-content", action="store_true", help="Download complete job descriptions")
    parser.add_argument("--enrich", action="store_true", help="Run deep JD enrichment (salary, skills, H-1B, experience)")
    parser.add_argument("--only-us", action="store_true", help="Only save US / US-Remote jobs (alias for --country us)")
    parser.add_argument("--country", default="us", choices=["us", "india", "us_and_india", "all"],
                        help="Target country location filter: us, india, us_and_india, all")
    parser.add_argument("--only-india", action="store_true", help="Only save India / India-Remote jobs (alias for --country india)")
    parser.add_argument("--days", type=int, default=0, help="Filter jobs posted within last N days (0 = all time)")
    parser.add_argument("--keyword", type=str, default="", help="Filter job title/department/location by keyword")

    args = parser.parse_args()

    target_country = args.country
    if args.only_india:
        target_country = "india"
    elif args.only_us:
        target_country = "us"

    # Mode 0: Database merge mode
    if args.merge_dbs:
        merge_databases(args.merge_dbs, args.output_db, export_csv=args.export_csv, country=target_country)
        return

    enable_enrichment = args.enrich or args.full_content

    # 1. Single slug mode
    if args.slug:
        platform = args.platform if args.platform != "all" else "greenhouse"
        handler = PLATFORM_HANDLERS.get(platform)
        if not handler:
            print(f"Unsupported platform: {platform}")
            sys.exit(1)

        async def run_single():
            async with AsyncHTTPClient(concurrency=10) as http:
                fetch_content = args.full_content or enable_enrichment
                status, jobs = await handler(http, args.slug, fetch_content)
                filtered = []
                cutoff_dt = datetime.now(timezone.utc) - timedelta(days=args.days) if args.days > 0 else None
                kw = args.keyword.lower().strip()

                for j in jobs:
                    is_us = bool(j.get("is_us"))
                    is_in = bool(j.get("is_india")) or is_india_location(j.get("location", ""), extra_text=j.get("title", ""))
                    if (args.only_us or target_country == "us") and not is_us:
                        continue
                    elif target_country == "india" and not is_in:
                        continue
                    elif target_country == "us_and_india" and not (is_us or is_in):
                        continue
                    if cutoff_dt and j.get("posted_at"):
                        try:
                            j_dt = datetime.fromisoformat(j["posted_at"].replace("Z", "+00:00"))
                            if j_dt < cutoff_dt:
                                continue
                        except Exception:
                            pass
                    if kw:
                        sc = f"{j.get('title', '')} {j.get('department', '')} {j.get('location', '')}".lower()
                        if kw not in sc:
                            continue
                    filtered.append(j)

                print(f"\nBoard HTTP Status: {status} (200 = Board Exists & Active)")
                print(f"Extracted {len(filtered)} matching live jobs for {platform.upper()}:{args.slug}")
                for j in filtered[:10]:
                    print(f"  • {j['title']} | Loc: {j['location']} (US={j['is_us']})")
                    print(f"    Posted: {j['posted_at'] or 'N/A'} | Apply: {j['apply_url']}")

                if args.output_db:
                    init_db(args.output_db)
                    save_boards_batch(args.output_db, [(platform, args.slug, status, len(jobs), len(filtered), datetime.now(timezone.utc).isoformat())])
                    save_jobs_batch(args.output_db, filtered, enable_enrichment=enable_enrichment, keep_content=args.full_content)
                    print(f"Saved to {args.output_db}")

                if args.export_csv:
                    export_sqlite_to_csv(args.output_db, args.export_csv, only_us=args.only_us, country=target_country)

        asyncio.run(run_single())
        return

    # 2. Batch input mode
    input_file = args.input
    if not input_file:
        # Default auto-selection
        if args.platform == "greenhouse" and os.path.exists("greenhouse_companies_us_active.txt"):
            input_file = "greenhouse_companies_us_active.txt"
        elif os.path.exists(f"exports/us_{args.platform}_companies.csv"):
            input_file = f"exports/us_{args.platform}_companies.csv"
        elif os.path.exists("exports/us_companies_by_ats_master.csv"):
            input_file = "exports/us_companies_by_ats_master.csv"
        else:
            print("Please specify an input file via --input <file.csv/txt>")
            sys.exit(1)

    print(f"Loading companies from: {input_file} (default platform: {args.platform})")
    targets = load_companies(input_file, default_platform=args.platform)

    # Filter by platform if specific platform requested
    if args.platform != "all":
        targets = [(p, s) for (p, s) in targets if p == args.platform]

    # Deduplicate while preserving order
    seen = set()
    unique_targets = []
    for p, s in targets:
        key = (p, s)
        if key not in seen:
            seen.add(key)
            unique_targets.append(key)

    if args.offset > 0:
        unique_targets = unique_targets[args.offset:]
    if args.limit > 0:
        unique_targets = unique_targets[:args.limit]

    # Multi-runner sharding: strided modulo partition
    if args.total_shards > 1:
        if args.shard_index < 0 or args.shard_index >= args.total_shards:
            print(f"Error: --shard-index ({args.shard_index}) must be between 0 and {args.total_shards - 1}")
            sys.exit(1)
        unique_targets = [t for i, t in enumerate(unique_targets) if i % args.total_shards == args.shard_index]
        print(f"Shard [{args.shard_index + 1}/{args.total_shards}]: Partitioned {len(unique_targets):,} target boards.")
        stagger = (args.shard_index * 1.5) + random.uniform(0.1, 0.5)
        print(f"Applying startup stagger: {stagger:.2f}s to avoid thundering herd...")
        time.sleep(stagger)

    print(f"Total unique target boards to scan: {len(unique_targets):,}")

    asyncio.run(
        run_extractor(
            targets=unique_targets,
            output_db=args.output_db,
            export_csv=args.export_csv,
            concurrency=args.concurrency,
            full_content=args.full_content,
            only_us=args.only_us,
            country=target_country,
            days=args.days,
            keyword=args.keyword,
            enable_enrichment=enable_enrichment,
        )
    )


if __name__ == "__main__":
    main()
