"""
utils/blocked_listings.py
-------------------------
Persistent memory of listings the detail-page check has BLOCKED (porter /
concierge / unfurnished-only).

Why: the detail check depends on fetching the listing page, and Zoopla
rate-limits those fetches — so the check is a daily dice-roll. Without memory,
a concierge listing that was correctly blocked on Monday gets re-scraped and
re-checked every day until one day the fetch fails and it slips through
(observed live: zoopla 73661757 blocked 19 Jul, sent 2 Aug). Building
attributes don't change, so a positive porter/concierge identification should
be permanent.

Storage: blocked_listings.json in the project root, committed by the workflow.
Format: { url: {"date": "YYYY-MM-DD", "reason": "concierge", "sig": "street|PC|beds"} }

Matching is by URL OR property signature, so the same flat re-listed by another
agency / on another portal stays blocked too.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_PATH = Path(__file__).parent.parent / "blocked_listings.json"


def _load() -> dict:
    if not _PATH.exists():
        return {}
    try:
        data = json.loads(_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception as exc:
        logger.warning("[blocked] Failed to load blocked_listings.json: %s", exc)
        return {}


def _save(data: dict) -> None:
    try:
        _PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:
        logger.warning("[blocked] Failed to save blocked_listings.json: %s", exc)


def _sig(listing: dict) -> Optional[str]:
    try:
        from utils.dedupe import property_signature
        return property_signature(listing)
    except Exception:
        return None


def is_blocked(listing: dict) -> bool:
    """True if this listing (by URL or property signature) was previously
    identified as porter/concierge/unfurnished by the detail check."""
    data = _load()
    url = (listing.get("url") or "").strip()
    if url and url in data:
        return True
    sig = _sig(listing)
    if sig:
        for e in data.values():
            if isinstance(e, dict) and e.get("sig") == sig:
                return True
    return False


def mark_blocked(listing: dict, reason: str = "") -> None:
    """Permanently record a detail-check block for this listing."""
    url = (listing.get("url") or "").strip()
    if not url:
        return
    data = _load()
    data[url] = {
        "date":   datetime.now().strftime("%Y-%m-%d"),
        "reason": reason or "detail-keyword",
        "sig":    _sig(listing),
    }
    _save(data)
    logger.info("[blocked] Remembered block (%s): %s", reason, url[:80])
