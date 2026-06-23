"""
utils/seen_listings.py
----------------------
Tracks property URLs that have already been sent to Telegram so the daily
run never sends the same listing twice. Also stores price for price-drop detection.

Storage: seen_listings.json in the project root.
Format:  { "https://rightmove.co.uk/...": {"date": "2026-04-17", "price": "£3,500 pcm"}, ... }
         (old string-only entries like "2026-04-17" are read transparently)

Functions
---------
is_duplicate(listing)     — True if this URL was already sent
mark_as_seen(listing)     — Record URL + today's date + price, save immediately
get_seen_price(url)       — Return stored price string for a URL, or None
clean_old_entries()       — Remove entries older than 30 days (run at startup)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

_SEEN_PATH    = Path(__file__).parent.parent / "seen_listings.json"
_MAX_AGE_DAYS = 30


def _load() -> dict:
    """Load seen_listings.json, returning {} on any error or unexpected format."""
    if not _SEEN_PATH.exists():
        return {}
    try:
        data = json.loads(_SEEN_PATH.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
        logger.warning(
            "[seen] seen_listings.json contained %s instead of dict — resetting to {}",
            type(data).__name__,
        )
        _save({})
        return {}
    except Exception as exc:
        logger.warning("[seen] Failed to load seen_listings.json: %s", exc)
        return {}


def _entry_date(entry) -> str | None:
    """Extract date string from either old string format or new dict format."""
    if isinstance(entry, str):
        return entry
    if isinstance(entry, dict):
        return entry.get("date")
    return None


def _save(data: dict) -> None:
    """Write seen_listings.json atomically."""
    try:
        _SEEN_PATH.write_text(
            json.dumps(data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception as exc:
        logger.warning("[seen] Failed to save seen_listings.json: %s", exc)


def _seen_signatures(seen: dict) -> set[str]:
    """Set of property signatures already sent (for cross-platform/agency dedup)."""
    return {e["sig"] for e in seen.values()
            if isinstance(e, dict) and e.get("sig")}


def is_duplicate(listing: dict) -> bool:
    """Return True if this property was already sent — by exact URL OR by property
    signature (street + postcode + beds), so the same flat is never sent twice
    even if it reappears on a different portal or via a different agency."""
    seen = _load()
    url = listing.get("url", "").strip()
    if url and url in seen:
        return True
    try:
        from utils.dedupe import property_signature
        sig = property_signature(listing)
    except Exception:
        sig = None
    if sig and sig in _seen_signatures(seen):
        return True
    return False


def mark_as_seen(listing: dict) -> None:
    """
    Record this listing as sent today — URL + price (for price-drop detection) +
    property signature (for cross-platform/agency dedup). Saves immediately so the
    record persists on crash.
    """
    url = listing.get("url", "").strip()
    if not url:
        return
    try:
        from utils.dedupe import property_signature
        sig = property_signature(listing)
    except Exception:
        sig = None
    seen = _load()
    seen[url] = {
        "date":  datetime.now().strftime("%Y-%m-%d"),
        "price": listing.get("price", ""),
        "sig":   sig,
    }
    _save(seen)
    logger.debug("[seen] Marked as seen: %s", url[:80])


def get_seen_price(url: str) -> str | None:
    """Return the price stored when this listing was first sent to Telegram."""
    entry = _load().get(url.strip())
    if isinstance(entry, dict):
        return entry.get("price") or None
    return None  # old string-format entry — no price stored


def clean_old_entries() -> None:
    """Remove entries older than 30 days and save."""
    seen = _load()
    if not seen:
        return

    cutoff = datetime.now() - timedelta(days=_MAX_AGE_DAYS)
    before  = len(seen)
    cleaned = {}

    for url, entry in seen.items():
        date_str = _entry_date(entry)
        try:
            if date_str and datetime.strptime(date_str, "%Y-%m-%d") >= cutoff:
                cleaned[url] = entry
        except (ValueError, TypeError):
            pass  # drop malformed entries

    removed = before - len(cleaned)
    if removed:
        logger.info("[seen] Cleaned %d stale entries (>%d days old, %d remaining)",
                    removed, _MAX_AGE_DAYS, len(cleaned))
        _save(cleaned)
    else:
        logger.debug("[seen] No stale entries to clean (%d total)", len(cleaned))
