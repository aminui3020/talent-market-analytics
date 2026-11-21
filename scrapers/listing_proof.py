"""
Listing proofs: the companies whose whole job list a run read, with every posting on it.

A scraper with a post-time window downloads a company's full list but saves only the new jobs, so a
job that is still open stops being "seen" once it is older than the window. A proof records what the
company still lists, which lets the merge (db/job_lifecycle.py):
  - keep a listed job alive even though this run did not save it again, and
  - retire a stored job the company no longer lists (after two such runs, a day apart).

Only a list read to its natural end counts: a failed page, a capped list or a list cut short by the
post-time window makes the company "partial", and partial companies prove nothing.

Postings are compared by posting_key(), which survives a board moving its job pages to another host.
"""
from __future__ import annotations

import gzip
import json
import re
import threading
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

_ID_PARAMS = ("gh_jid", "jobid", "job_id", "jid", "id")
# .../jobs/12345/some-title/job (iCIMS), .../jobs/12345 (Greenhouse): the number after /job(s)/ names the posting
_NUMBERED_JOB = re.compile(r"/jobs?/(\d+)(?:/|$)")


def posting_key(url: str | None) -> str:
    """The part of a job URL that names the posting: an id parameter, a number after /jobs/, else the last path segment."""
    parts = urlsplit((url or "").strip().lower())
    query = parse_qs(parts.query)
    for name in _ID_PARAMS:
        if query.get(name):
            return query[name][0]
    numbered = _NUMBERED_JOB.search(parts.path)
    if numbered:
        return numbered.group(1)
    path = parts.path.rstrip("/")
    return path.rsplit("/", 1)[-1] if path else parts.netloc


class ListingProof:
    """One board's proofs. Thread- and task-safe; a company with several sites is proven only if all are."""

    def __init__(self, board: str) -> None:
        self.board = board
        self._keys: dict[str, set[str]] = {}
        self._partial: set[str] = set()
        self._lock = threading.Lock()

    def listed(self, company: str, urls) -> None:
        """Every posting URL of one complete company list (an empty list proves the company has no jobs)."""
        company = (company or "").strip()
        if not company:
            return
        with self._lock:
            if company in self._partial:
                return
            self._keys.setdefault(company, set()).update(posting_key(url) for url in urls if url)

    def partial(self, company: str) -> None:
        company = (company or "").strip()
        with self._lock:
            self._partial.add(company)
            self._keys.pop(company, None)

    def companies(self) -> dict[str, set[str]]:
        with self._lock:
            return {company: set(keys) for company, keys in self._keys.items()}

    def write(self, path: str | Path) -> int:
        proven = self.companies()
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            json.dump({"board": self.board, "companies": {c: sorted(k) for c, k in proven.items()}}, handle)
        return len(proven)


_PROOFS: dict[str, ListingProof] = {}


def proof_for(board: str) -> ListingProof:
    """The running process's proof for a board (scrapers record into it; pipeline.py writes it out)."""
    return _PROOFS.setdefault(board, ListingProof(board))


def proof_path(output: str | Path, board: str) -> Path:
    """Where a board's proof goes next to its jobs file: Workday_jobs_weekly.json -> Workday_jobs_weekly.Workday.listed.json.gz"""
    output = Path(output)
    return output.with_name(f"{output.stem}.{board}.listed.json.gz")


def write_all(output: str | Path) -> list[Path]:
    """Writes every board's proof that has companies (the run's scrapers recorded them) next to `output`."""
    written = []
    for board, proof in _PROOFS.items():
        if proof.companies():
            path = proof_path(output, board)
            proof.write(path)
            written.append(path)
    return written


def load(paths) -> dict[tuple[str, str], set[str]]:
    """(lower-cased board, company) -> posting keys, from proof files; unreadable files are skipped."""
    proofs: dict[tuple[str, str], set[str]] = {}
    for path in paths:
        try:
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                data = json.load(handle)
        except (OSError, ValueError):
            continue
        board = str(data.get("board") or "").lower()
        for company, keys in (data.get("companies") or {}).items():
            proofs.setdefault((board, company), set()).update(keys)
    return proofs
