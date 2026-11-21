"""
When a job is closed, and why.

Closing used to rest on one thing: not seen for JOB_EXPIRE_DAYS. A run with a post-time window saves only
the new jobs, so a job that is still open looked "unseen" once it was older than the window. The listing
proofs (listing_proof.py) say what each company still lists, which gives three outcomes for a stored job
of a proven company:

  listed      -> alive: last_seen_at moves up, a job closed by mistake is reopened, strikes reset
  not listed  -> a strike; the second strike (a run at least STRIKE_GAP_HOURS later) closes it as 'delisted'
  not proven  -> untouched here (the unseen-for-N-days rule in neon_upsert.py still applies, reason 'unseen')

Guards: a company whose list came back empty or far smaller than what is stored is not trusted to close
anything (a failing page can look like that), and a job is closed by a proof only on a run that finished
the board cleanly.
"""
from __future__ import annotations

import logging
import os
from collections import defaultdict

from listing_proof import posting_key

log = logging.getLogger(__name__)

STRIKES_TO_CLOSE = int(os.getenv("JOB_STRIKES_TO_CLOSE", "2"))
STRIKE_GAP_HOURS = float(os.getenv("JOB_STRIKE_GAP_HOURS", "12"))
MIN_SHARE_LISTED = 0.5   # a proof listing under this share of the stored jobs closes nothing ...
MIN_STORED_FOR_SHARE = 20  # ... once a company has this many stored jobs

MIGRATION_SQL = """ALTER TABLE public.jobs ADD COLUMN IF NOT EXISTS closed_reason text,
  ADD COLUMN IF NOT EXISTS unlisted_runs integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS unlisted_at timestamptz"""

_STORED_SQL = """
SELECT id, lower(source_board), company, job_url, status, closed_reason, unlisted_runs,
       unlisted_at IS NOT NULL AND unlisted_at > now() - %(gap)s * interval '1 hour'
FROM public.jobs
WHERE lower(source_board) = ANY(%(boards)s::text[])
  AND (status = 'active' OR (status = 'expired' AND closed_reason IN ('unseen', 'delisted')))
"""


def plan(stored: list[tuple], proofs: dict[tuple[str, str], set[str]], close: bool) -> dict[str, list[str]]:
    """What to do with each stored job (rows as _STORED_SQL returns them). Pure, so it is easy to test.

    Returns ids under: alive (listed now), reopen (listed now but closed), strike (first miss),
    close (missed STRIKES_TO_CLOSE runs).
    """
    by_company: dict[tuple[str, str], list[tuple]] = defaultdict(list)
    for row in stored:
        by_company[(row[1], row[2])].append(row)

    out: dict[str, list[str]] = {"alive": [], "reopen": [], "strike": [], "close": []}
    for key, listed in proofs.items():
        rows = by_company.get(key, [])
        active = [r for r in rows if r[4] == "active"]
        missing = [r for r in active if posting_key(r[3]) not in listed]
        trusted = not (not listed and active) and not (
            len(active) >= MIN_STORED_FOR_SHARE and (len(active) - len(missing)) < MIN_SHARE_LISTED * len(active))
        if not trusted and missing:
            log.warning(f"  Lifecycle: {key[0]}/{key[1]}: proof lists {len(listed)} but {len(active)} are stored; "
                        f"nothing closed")
        for row in rows:
            job_id, _, _, url, status, _, strikes, struck_lately = row
            if posting_key(url) in listed:
                out["alive"].append(job_id)
                if status != "active":
                    out["reopen"].append(job_id)
            elif status == "active" and close and trusted:
                if struck_lately:
                    continue  # already counted for this run
                (out["close"] if strikes + 1 >= STRIKES_TO_CLOSE else out["strike"]).append(job_id)
    return out


def _chunks(items: list[str], size: int = 5000):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def apply_listings(conn, proofs: dict[tuple[str, str], set[str]], boards: list[str] | None,
                   close: bool = True) -> dict[str, int]:
    """Applies the proofs of the boards that finished cleanly (boards None = every board proven)."""
    if boards is not None:
        wanted = {b.lower() for b in boards}
        proofs = {key: keys for key, keys in proofs.items() if key[0] in wanted}
    if not proofs:
        return {}
    conn.execute(MIGRATION_SQL)
    stored = conn.execute(_STORED_SQL, {"boards": sorted({b for b, _ in proofs}), "gap": STRIKE_GAP_HOURS}).fetchall()
    todo = plan(stored, proofs, close)
    with conn.cursor() as cur:
        for ids in _chunks(todo["alive"]):
            cur.execute("""UPDATE public.jobs SET last_seen_at = now(), unlisted_runs = 0, unlisted_at = NULL,
                           status = 'active', closed_reason = NULL
                           WHERE id = ANY(%s)""", (ids,))
        for ids in _chunks(todo["strike"]):
            cur.execute("""UPDATE public.jobs SET unlisted_runs = unlisted_runs + 1, unlisted_at = now()
                           WHERE id = ANY(%s)""", (ids,))
        for ids in _chunks(todo["close"]):
            cur.execute("""UPDATE public.jobs SET status = 'expired', closed_reason = 'delisted',
                           unlisted_runs = unlisted_runs + 1, unlisted_at = now(), updated_at = now()
                           WHERE id = ANY(%s)""", (ids,))
    conn.commit()
    counts = {name: len(ids) for name, ids in todo.items()}
    log.info(f"Lifecycle: {len(proofs)} companies proven; {counts['alive']} still listed "
             f"({counts['reopen']} reopened), {counts['strike']} first miss, {counts['close']} closed as delisted")
    return counts
