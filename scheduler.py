"""
scheduler.py
------------
Orchestrates the full property search pipeline:

  1. Run all three scrapers (Rightmove, OpenRent, Zoopla) concurrently
  2. Deduplicate listings across all sources by URL
  3. For each listing, fetch walking time to configured destinations (optional)
  4. Score and summarise each listing with Claude (optional)
  5. Return/send results

Called by research_bot.py on /run, and by the GitHub Actions daily cron.

Environment variables (all in .env):
  GOOGLE_MAPS_API_KEY   — enables walk time enrichment (optional)
  ANTHROPIC_API_KEY     — enables AI scoring (optional)
  COMMUTE_DESTINATIONS  — pipe-separated list of walk-time targets, e.g.
                          "Oxford Circus Station|Bond Street Station"
                          Defaults to a curated set of central London stations.
"""

import asyncio
import logging
import os
from typing import Optional

from dotenv import load_dotenv

from scrapers import openrent, rightmove, zoopla
from utils.walk_time  import nearest_walk_minutes
from utils.valuation  import score_listing

load_dotenv()

logger = logging.getLogger(__name__)

# Default commute destinations if COMMUTE_DESTINATIONS is not set
_DEFAULT_DESTINATIONS = [
    "Oxford Circus Underground Station, London",
    "Bond Street Underground Station, London",
    "Baker Street Underground Station, London",
    "Marylebone Station, London",
    "Covent Garden Underground Station, London",
    "London Bridge Station, London",
    "Knightsbridge Underground Station, London",
]

_DESTINATIONS: list[str] = [
    d.strip()
    for d in os.getenv("COMMUTE_DESTINATIONS", "").split("|")
    if d.strip()
] or _DEFAULT_DESTINATIONS

_SCORE_MIN = int(os.getenv("SCORE_MIN", "0"))   # filter: only send listings >= this score


async def _run_scrapers() -> list[dict]:
    """Run all scrapers concurrently and return deduplicated listings."""
    logger.info("[scheduler] Starting all scrapers…")

    # Run all three scrapers concurrently
    rm_task = asyncio.create_task(rightmove.scrape())
    zo_task = asyncio.create_task(zoopla.scrape())
    or_task = asyncio.create_task(openrent.scrape())

    rm_listings, zo_listings, or_listings = await asyncio.gather(
        rm_task, zo_task, or_task
    )

    all_listings = rm_listings + zo_listings + or_listings
    logger.info(
        "[scheduler] Raw counts — Rightmove: %d  Zoopla: %d  OpenRent: %d  Total: %d",
        len(rm_listings), len(zo_listings), len(or_listings), len(all_listings),
    )

    # Deduplicate by URL across all sources
    seen: set[str] = set()
    unique: list[dict] = []
    for listing in all_listings:
        url = listing.get("url", "")
        if url and url not in seen:
            seen.add(url)
            unique.append(listing)

    logger.info("[scheduler] After dedup: %d unique listings", len(unique))
    return unique


def _enrich_listing(listing: dict) -> dict:
    """Add walk_time and AI score to a listing dict (in-place, returns it)."""
    address = listing.get("address") or listing.get("area", "")

    # Walk time
    dest, mins = nearest_walk_minutes(address, _DESTINATIONS)
    listing["walk_dest"] = dest
    listing["walk_mins"] = mins

    # AI valuation
    scored = score_listing(listing)
    listing.update(scored)   # adds score, summary, flags

    return listing


def format_listing(listing: dict) -> str:
    """Format a single listing as a readable Telegram message."""
    source  = listing.get("source", "?").capitalize()
    area    = listing.get("area", "")
    price   = listing.get("price", "Price N/A")
    address = listing.get("address", "")
    url     = listing.get("url", "")
    score   = listing.get("score")
    summary = listing.get("summary", "")
    flags   = listing.get("flags", [])
    walk_d  = listing.get("walk_dest")
    walk_m  = listing.get("walk_mins")

    score_str = f"⭐ {score}/10" if score is not None else ""
    walk_str  = f"🚶 {walk_m} min to {walk_d}" if walk_d and walk_m is not None else ""
    flag_str  = "  •  ".join(flags) if flags else ""

    lines = [
        f"*{area}* — {price}  [{source}]",
        address,
    ]
    if score_str or walk_str:
        lines.append("  ".join(filter(None, [score_str, walk_str])))
    if flag_str:
        lines.append(f"_{flag_str}_")
    if summary:
        lines.append(summary)
    lines.append(url)

    return "\n".join(lines)


async def run_search(
    enrich: bool = True,
    score_min: int = _SCORE_MIN,
) -> list[dict]:
    """
    Full pipeline: scrape → enrich → filter → return.

    Parameters
    ----------
    enrich    : bool  Run walk_time + valuation enrichment (requires API keys).
    score_min : int   Only return listings with score >= this value (0 = all).

    Returns
    -------
    List of enriched listing dicts, sorted by score descending.
    """
    listings = await _run_scrapers()

    if enrich:
        logger.info("[scheduler] Enriching %d listings…", len(listings))
        enriched = []
        for i, listing in enumerate(listings, 1):
            try:
                enriched.append(_enrich_listing(listing))
            except Exception as exc:
                logger.warning("[scheduler] Enrichment failed for listing %d: %s", i, exc)
                enriched.append(listing)
        listings = enriched

    # Filter by minimum score
    if score_min > 0:
        before = len(listings)
        listings = [l for l in listings if l.get("score", 0) >= score_min]
        logger.info("[scheduler] Score filter (%d+): %d → %d listings", score_min, before, len(listings))

    # Sort by score descending (unscored listings go to the end)
    listings.sort(key=lambda l: l.get("score", 0), reverse=True)

    logger.info("[scheduler] Search complete — returning %d listings", len(listings))
    return listings


# ── Standalone test ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(message)s",
        level=logging.INFO,
    )
    results = asyncio.run(run_search(enrich=False))
    print(f"\n=== {len(results)} listings ===")
    for r in results[:3]:
        print(format_listing(r))
        print()
