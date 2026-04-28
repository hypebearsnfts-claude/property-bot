"""
utils/seen_listings.py
----------------------
Tracks property URLs that have already been sent to Telegram so the daily
run never sends the same listing twice.

Storage: seen_listings.json in the project root.
Format:  { "https://rightmove.co.uk/...": "2026-04-17", ... }

Functions
---------
is_duplicate(listing)     — True if this URL was already sent
mark_as_seen(listing)     — Record URL + today's date, save immediately
clean_old_entries()       — Remove entries older than 30 days (run at startup)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

_SEEN_PATH  = Path(__file__).parent.parent / "seen_listings.json"
_MAX_AGE_DAYS = 30


def _load() -> dict[str, str]:
    """Load seen_listings.json, returning {} on any error or unexpected format.

    Guards against the file containing a JSON list (e.g. produced by
    ``echo '[]' > seen_listings.json``) instead of a JSON object.  A non-dict
    value is treated as empty and the file is silently reset to ``{}``.
    """
    if not _SEEN_PATH.exists():
        return {}
    try:
        data = json.loads(_SEEN_PATH.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
        # File contains a list or other non-dict — reset it
        logger.warning(
            "[seen] seen_listings.json contained %s instead of dict — resetting to {}",
            type(data).__name__,
        )
        _save({})
        return {}
    except Exception as exc:
        logger.warning("[seen] Failed to load seen_listings.json: %s", exc)
        return {}


def _save(data: dict[str, str]) -> None:
    """Write seen_listings.json atomically."""
    try:
        _SEEN_PATH.write_text(
            json.dumps(data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception as exc:
        logger.warning("[seen] Failed to save seen_listings.json: %s", exc)


def is_duplicate(listing: dict) -> bool:
    """
    Return True if this listing's URL has already been sent to Telegram.
    Safe to call many times — reads from in-memory dict after first load.
    """
    url = listing.get("url", "").strip()
    if not url:
        return False
    seen = _load()
    return url in seen


def mark_as_seen(listing: dict) -> None:
    """
    Record this listing URL as sent today.
    Saves seen_listings.json immediately so the record persists even if the
    pipeline crashes partway through.
    """
    url = listing.get("url", "").strip()
    if not url:
        return
    seen = _load()
    seen[url] = datetime.now().strftime("%Y-%m-%d")
    _save(seen)
    logger.debug("[seen] Marked as seen: %s", url[:80])


def clean_old_entries() -> None:
    """
    Remove entries older than 30 days and save.
    Call this once at the start of each pipeline run.
    """
    seen = _load()
    if not seen:
        return

    cutoff = datetime.now() - timedelta(days=_MAX_AGE_DAYS)
    before  = len(seen)
    cleaned = {}

    for url, date_str in seen.items():
        try:
            entry_date = datetime.strptime(date_str, "%Y-%m-%d")
            if entry_date >= cutoff:
                cleaned[url] = date_str
        except (ValueError, TypeError):
            pass   # drop malformed entries

    removed = before - len(cleaned)
    if removed:
        logger.info("[seen] Cleaned %d stale entries (>%d days old, %d remaining)",
                    removed, _MAX_AGE_DAYS, len(cleaned))
        _save(cleaned)
    else:
        logger.debug("[seen] No stale entries to clean (%d total)", len(cleaned))
