"""Find ATS companies from the company NAMES in LinkedIn/Indeed results.

LinkedIn's public search shows no employer apply link, so discover_ats_companies.py (apply links) finds little
there. A name is still enough: for each company we do not list yet, ask the hiring platforms' public job
APIs whether a board with that slug exists and has open jobs ("Stripe, Inc." -> stripe, ...). A hit on a
platform we scrape is added to that platform's company list.

  - Names already covered are skipped: any candidate slug already in a list (active, disabled or known dead).
  - A platform that returns the company's name must agree with ours ("Apple" must not match a stranger's
    "apple" board). Platforms that return no name only accept a slug made from the whole name.
  - Every probed name is remembered (probed_companies.json) and not asked again for RECHECK_DAYS.
  - Politeness: bounded concurrency, a request timeout, and a time budget after which the run stops cleanly.

Used by discover_ats_companies.py --by-name; nothing here writes the lists itself.
"""
from __future__ import annotations

import asyncio
import json
import re
import time
import unicodedata
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Awaitable, Callable

ROOT = Path(__file__).resolve().parent
CACHE_FILE = ROOT / "probed_companies.json"
RECHECK_DAYS = 60
SLUG_OK = re.compile(r"^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$")

# Words after the name that are not part of it ("Acme Inc.", "Acme Holdings LLC")
LEGAL = {"inc", "incorporated", "llc", "ltd", "limited", "corp", "corporation", "co", "company", "plc", "gmbh", "ag",
         "sa", "lp", "llp", "pllc", "pc", "bv", "nv", "pty", "srl", "sas", "oy", "ab"}
# Words that may be dropped to get the short form of a name ("Procore Technologies" -> "procore")
GENERIC_TAIL = {"technologies", "technology", "tech", "systems", "solutions", "group", "holdings", "international",
                "global", "services", "software", "labs", "health", "healthcare", "industries", "enterprises",
                "partners", "associates", "consulting", "worldwide", "america", "usa", "us", "the"}
# First words too common to stand for a company on their own
GENERIC_FIRST = GENERIC_TAIL | {"american", "national", "united", "first", "general", "advanced", "applied", "central",
                                "digital", "federal", "premier", "pacific", "atlantic", "northern", "southern",
                                "western", "eastern", "new", "great", "blue", "green", "red", "star", "top", "best"}
SKIP_NAMES = {"confidential", "confidential company", "company confidential", "hiring", "hiring now", "staffing",
              "recruiting", "recruiter", "jobs", "careers", "undisclosed", "anonymous", "stealth"}


@dataclass(frozen=True)
class Candidate:
    slug: str
    kind: str  # "full" (the whole name), "short" (without trailing generic words), "first" (first word only)


@dataclass(frozen=True)
class Hit:
    board: str
    slug: str
    name: str       # the name the platform gave, "" when it gives none
    jobs: int


def _ascii(text: str) -> str:
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")


def words(name: str) -> list[str]:
    """The name as plain lower-case words: no accents, parentheses, punctuation or legal suffixes."""
    text = re.sub(r"\([^)]*\)", " ", _ascii(name).lower())
    text = text.replace("&", " and ").replace("'", "").replace("’", "")
    tokens = re.findall(r"[a-z0-9]+", text)
    while tokens and tokens[-1] in LEGAL:
        tokens.pop()
    return tokens


def cache_key(name: str) -> str:
    return "".join(words(name))


def candidates(name: str) -> list[Candidate]:
    """Slugs a company with this name might use on a hiring platform, most likely first."""
    tokens = words(name)
    if tokens and tokens[0] == "the":
        tokens = tokens[1:]
    if not tokens or " ".join(tokens) in SKIP_NAMES:
        return []
    short = list(tokens)
    while len(short) > 1 and short[-1] in GENERIC_TAIL:
        short.pop()
    if len(short) == 1 and (short[0] in GENERIC_FIRST or len(short[0]) < 4):
        short = list(tokens)  # "Global Partners" must not become "global"
    found: dict[str, str] = {}

    def add(slug: str, kind: str) -> None:
        if SLUG_OK.match(slug) and slug not in found:
            found[slug] = kind

    add("".join(tokens), "full")
    add("-".join(tokens), "full")
    if short != tokens:
        add("".join(short), "short")
        add("-".join(short), "short")
    if len(tokens) > 1 and len(tokens[0]) >= 5 and tokens[0] not in GENERIC_FIRST:
        add(tokens[0], "first")
    return [Candidate(slug, kind) for slug, kind in found.items()]


def _core(name: str) -> list[str]:
    """The words that identify a company: not legal suffixes, "and", or generic words like Technologies."""
    return [word for word in words(name) if word not in GENERIC_TAIL and word != "and"]


def names_match(ours: str, theirs: str) -> bool:
    """Whether a platform's company name is the company we mean: the same identifying words
    ("Procore Technologies" = "Procore", "Wal Mart" = "Walmart"), never just a name inside a longer one
    ("Apple" is not "Apple Valley Dental")."""
    a, b = _core(ours), _core(theirs)
    if not a or not b:
        return False
    return "".join(a) == "".join(b) or set(a) == set(b)


# ---------------------------------------------------------------- the platforms' public job APIs
# Each probe returns a Hit when a board with this slug exists and lists at least one job, else None.

Probe = Callable[["httpx.AsyncClient", str], Awaitable["Hit | None"]]


async def _get(client, url: str):
    try:
        return await client.get(url)
    except Exception:  # noqa: BLE001 - a timeout or reset is "not found this time"
        return None


async def probe_greenhouse(client, slug: str) -> Hit | None:
    board = await _get(client, f"https://boards-api.greenhouse.io/v1/boards/{slug}")
    if board is None or board.status_code != 200:
        return None
    jobs = await _get(client, f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs")
    count = len(jobs.json().get("jobs", [])) if jobs is not None and jobs.status_code == 200 else 0
    return Hit("Greenhouse", slug, str(board.json().get("name") or ""), count) if count else None


async def probe_lever(client, slug: str) -> Hit | None:
    r = await _get(client, f"https://api.lever.co/v0/postings/{slug}?mode=json&limit=1")
    if r is None or r.status_code != 200:
        return None
    data = r.json()
    return Hit("Lever", slug, "", len(data)) if isinstance(data, list) and data else None


async def probe_ashby(client, slug: str) -> Hit | None:
    r = await _get(client, f"https://api.ashbyhq.com/posting-api/job-board/{slug}")
    if r is None or r.status_code != 200:
        return None
    jobs = r.json().get("jobs", [])
    return Hit("Ashby", slug, "", len(jobs)) if jobs else None


async def probe_smartrecruiters(client, slug: str) -> Hit | None:
    r = await _get(client, f"https://api.smartrecruiters.com/v1/companies/{slug}/postings?limit=1")
    if r is None or r.status_code != 200:
        return None
    data = r.json()  # unknown companies answer 200 with totalFound 0
    if not data.get("totalFound"):
        return None
    first = (data.get("content") or [{}])[0]
    return Hit("SmartRecruiters", slug, str((first.get("company") or {}).get("name") or ""), int(data["totalFound"]))


async def probe_workable(client, slug: str) -> Hit | None:
    r = await _get(client, f"https://apply.workable.com/api/v1/widget/accounts/{slug}")
    if r is None or r.status_code != 200:
        return None
    data = r.json()
    jobs = data.get("jobs") or []
    return Hit("Workable", slug, str(data.get("name") or ""), len(jobs)) if jobs else None


async def probe_bamboohr(client, slug: str) -> Hit | None:
    r = await _get(client, f"https://{slug}.bamboohr.com/careers/list")
    if r is None or r.status_code != 200 or "json" not in r.headers.get("content-type", ""):
        return None  # a company that does not exist is redirected to bamboohr.com
    jobs = r.json().get("result") or []
    return Hit("BambooHR", slug, "", len(jobs)) if jobs else None


async def probe_jobvite(client, slug: str) -> Hit | None:
    r = await _get(client, f"https://jobs.jobvite.com/{slug}")
    if r is None or r.status_code != 200:
        return None  # unknown companies are redirected
    count = len(set(re.findall(rf"/{re.escape(slug)}/job/[A-Za-z0-9]+", r.text, re.I)))
    return Hit("Jobvite", slug, "", count) if count else None


async def probe_teamtailor(client, slug: str) -> Hit | None:
    r = await _get(client, f"https://{slug}.teamtailor.com/jobs.rss")
    if r is None or r.status_code != 200 or "<item>" not in r.text:
        return None
    title = re.search(r"<channel>\s*<title>(?:<!\[CDATA\[)?\s*([^<\]]+)", r.text)
    return Hit("Teamtailor", slug, title.group(1).strip() if title else "", r.text.count("<item>"))


PROBES: dict[str, Probe] = {
    "Greenhouse": probe_greenhouse, "Lever": probe_lever, "Ashby": probe_ashby,
    "SmartRecruiters": probe_smartrecruiters, "Workable": probe_workable, "BambooHR": probe_bamboohr,
    "Jobvite": probe_jobvite, "Teamtailor": probe_teamtailor,
}


def accept(candidate: Candidate, hit: Hit, name: str) -> bool:
    """A hit counts when the platform names the company and it is ours; platforms that give no name only count
    for a slug made from the whole name (the one-word shortcut needs a name to confirm it)."""
    if hit.name:
        return names_match(name, hit.name)
    return candidate.kind in ("full", "short")


# ---------------------------------------------------------------- remembering what was asked

def load_cache(path: Path = CACHE_FILE) -> dict[str, dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def save_cache(cache: dict[str, dict], path: Path = CACHE_FILE) -> None:
    path.write_text(json.dumps(cache, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def recently_probed(cache: dict[str, dict], key: str, today: date) -> bool:
    entry = cache.get(key)
    return bool(entry) and date.fromisoformat(entry["on"]) > today - timedelta(days=RECHECK_DAYS)


# ---------------------------------------------------------------- the run

def new_names(names: list[str], known: dict[str, set[str]], cache: dict[str, dict], today: date) -> list[str]:
    """Distinct names worth asking about: not covered by a list, not asked about recently, most frequent first."""
    covered = set().union(*known.values()) if known else set()
    counts: dict[str, list] = {}
    for name in names:
        name = name.strip()
        key = cache_key(name)
        if not name or not key or not candidates(name):
            continue
        entry = counts.setdefault(key, [name, 0])
        entry[1] += 1
    wanted = []
    for key, (name, count) in counts.items():
        if recently_probed(cache, key, today):
            continue
        if any(candidate.slug in covered for candidate in candidates(name)):
            continue  # one of our lists already has this company
        wanted.append((count, name))
    wanted.sort(key=lambda item: (-item[0], item[1].lower()))
    return [name for _, name in wanted]


async def probe_names(names: list[str], *, boards: set[str] | None = None, concurrency: int = 12, timeout: float = 10.0,
                      budget_minutes: float = 0, max_names: int = 0, transport=None,
                      today: date | None = None, cache: dict[str, dict] | None = None) -> tuple[list[tuple[str, Hit]], dict]:
    """Ask the platforms about each name. Returns ([(our name, hit)], stats); the cache is updated in place."""
    import httpx

    today = today or date.today()
    cache = cache if cache is not None else {}
    deadline = time.monotonic() + budget_minutes * 60 if budget_minutes else None
    probes = {board: probe for board, probe in PROBES.items() if boards is None or board in boards}
    todo = names[:max_names] if max_names else list(names)
    gate = asyncio.Semaphore(concurrency)
    found: list[tuple[str, Hit]] = []
    stats = {"asked": 0, "found": 0, "left": 0}

    async def one(client, name: str) -> None:
        hits: list[Hit] = []
        for candidate in candidates(name):
            async def run(board: str, probe: Probe) -> None:
                async with gate:
                    hit = await probe(client, candidate.slug)
                if hit and accept(candidate, hit, name) and not any(h.board == hit.board for h in hits):
                    hits.append(hit)
            await asyncio.gather(*(run(board, probe) for board, probe in probes.items()))
            if hits:
                break  # the most specific slug that exists wins
        cache[cache_key(name)] = {"on": today.isoformat(), "found": [f"{h.board}:{h.slug}" for h in hits]}
        stats["asked"] += 1
        for hit in hits:
            found.append((name, hit))
        stats["found"] += bool(hits)

    async with httpx.AsyncClient(timeout=timeout, transport=transport, follow_redirects=False,
                                 headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/130.0"}) as client:
        for start in range(0, len(todo), 8):
            if deadline and time.monotonic() >= deadline:
                break
            await asyncio.gather(*(one(client, name) for name in todo[start:start + 8]))
    stats["left"] = len(todo) - stats["asked"]
    return found, stats
