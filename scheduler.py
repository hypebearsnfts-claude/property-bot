"""
scheduler.py
------------
Orchestrates the full property search pipeline:

  1. Run all four scrapers (Rightmove, OpenRent, Zoopla, OnTheMarket) concurrently
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
import re
from typing import Optional

from dotenv import load_dotenv

from scrapers import openrent, onthemarket, rightmove, zoopla
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

# Source priority for cross-platform dedup (lower = preferred)
# OpenRent = direct landlord, usually best price. Rightmove/Zoopla = agent listings.
_SOURCE_PRIORITY: dict[str, int] = {
    "openrent":     0,
    "rightmove":    1,
    "zoopla":       2,
    "onthemarket":  3,
}

# OTM scrapes by postcode district/sub-district slug. Even specific slugs (e.g. nw1)
# can span far more than the target area. These lists define exactly which postcodes
# are acceptable for each area.
#
# Format of each entry:
#   "EC3N"  → outward-code match (listing postcode must equal EC3N exactly)
#   "NW1 5" → sector match  (listing postcode must be NW1 5xx, e.g. NW1 5AB)
#
# Deliberately tight — if a listing can't be confirmed inside the target area it
# is dropped. The walk-time filter still catches anything that slips through.
_OTM_VALID_POSTCODES: dict[str, list[str]] = {
    "Covent Garden":   ["WC2H", "WC2E"],               # Covent Garden & Strand only
    "Soho":            ["W1D", "W1F"],                  # Soho core (no Oxford St sprawl)
    "Knightsbridge":   ["SW1X", "SW3"],                 # Knightsbridge & Beauchamp Place
    "West Kensington": ["W14"],                         # West Kensington only
    "London Bridge":   ["SE1"],                         # SE1 is tightly London Bridge
    "Tower Hill":      ["EC3N", "EC3M"],                # Tower Hill & Monument
    "Baker Street":    ["NW1 5", "NW1 6"],              # Baker St sectors (not Camden/KX)
    "Bond Street":     ["W1K", "W1J"],                  # Bond St & Mayfair
    "Marble Arch":     ["W1H", "W2 1", "W2 2"],         # Marble Arch & edge of W2
    "Oxford Circus":   ["W1B", "W1F"],                  # Oxford Circus & Carnaby St
    "Marylebone":      ["W1U", "W1G"],                  # Marylebone High St & Harley St
    "Regent's Park":   ["NW8", "NW1 4"],                # St John's Wood & Outer Circle
}


def _otm_postcode_ok(listing: dict) -> bool:
    """
    Return True if the listing's postcode falls within the allowed set for its area.

    Supports two matching modes based on the valid-prefix entry:
      - "EC3N"  → exact outward-code match (outward == "EC3N")
      - "NW1 5" → sector match (outward == "NW1" AND sector digit == "5")

    If no postcode is found in the address the listing is kept (don't over-filter).
    """
    area = listing.get("area", "")
    valid_prefixes = _OTM_VALID_POSTCODES.get(area)
    if not valid_prefixes:
        return True
    addr = listing.get("address", "").upper()

    # Try to extract a full UK postcode: OUTWARD SECTOR_DIGIT UNIT (e.g. NW1 5AB)
    m = re.search(r'\b([A-Z]{1,2}\d{1,2}[A-Z]?)\s*(\d)[A-Z]{2}\b', addr)
    if m:
        outward = m.group(1)       # e.g. "NW1"
        sector  = m.group(2)       # e.g. "5"
    else:
        # Fall back to just outward code (no sector available)
        m2 = re.search(r'\b([A-Z]{1,2}\d{1,2}[A-Z]?)\b', addr)
        if not m2:
            return True            # no postcode at all — keep
        outward = m2.group(1)
        sector  = None

    for p in valid_prefixes:
        if " " in p:
            # Sector-level entry e.g. "NW1 5"
            dist, sec = p.split(None, 1)
            if outward == dist and sector == sec:
                return True
        else:
            # Outward-level entry e.g. "EC3N", "W1H"
            if outward == p:
                return True
    return False


def _otm_building_key(listing: dict) -> str | None:
    """
    For OTM listings without a street number, return a normalised building key:
      area + first-meaningful-word-of-street + outward-postcode
    Used to cap duplicates of the same building listed by many agents.
    Returns None if address is too vague to form a key.
    """
    addr = listing.get("address", "").lower()
    area = listing.get("area", "")
    # Only apply to listings without a street number (the problematic ones)
    if re.search(r'\b\d+\b', addr):
        return None   # has a number → standard key will handle it
    pc = re.search(r'\b([a-z]{1,2}\d[a-z\d]?)\b', addr)
    if not pc:
        return None
    outward = pc.group(1)
    _SKIP = {"road", "street", "avenue", "lane", "place", "close", "gardens",
             "way", "drive", "court", "house", "london", "flat", "floor",
             "england", "uk", "the"}
    words = [w for w in re.findall(r'[a-z]{3,}', addr) if w not in _SKIP]
    if not words:
        return None
    return f"{area.lower()}-{words[0]}-{outward}"


def _address_dedup_key(address: str) -> str | None:
    """
    Produce a normalised key for cross-platform dedup of the same physical property.

    Extracts:  street-number  +  first-street-word  +  outward-postcode
    All three must be present for a reliable match. Returns None if uncertain.

    Examples:
      "Flat 3, 12 Greek Street, W1D 4DH"       → "12-greek-w1d"
      "12 Greek Street, Soho, London, W1D 4DH"  → "12-greek-w1d"
      "Apartment 5, 44-46 Baker Street, NW1 5RT" → "44-baker-nw1"
    """
    if not address:
        return None
    a = address.lower()

    # Outward postcode (e.g. "W1D" from "W1D 4DH")
    pc = re.search(r'\b([a-z]{1,2}\d[a-z\d]?)\s*\d[a-z]{2}\b', a)
    outward = pc.group(1) if pc else None

    # Strip unit-level prefixes (flat/apartment/floor etc.) before finding street number
    a_clean = re.sub(
        r'\b(?:flat|apartment|apt|unit|floor|studio|room|suite|ground|first|second|third|basement)'
        r'\s*[\d\w]*[,\s]*',
        ' ', a, flags=re.IGNORECASE,
    ).strip()

    # First street/building number (may be a range like 44-46)
    num = re.search(r'\b(\d+(?:-\d+)?)\b', a_clean)
    number = num.group(1).split('-')[0] if num else None   # use lower of range

    # First meaningful word after the number
    street_word = None
    if num:
        after = a_clean[num.end():].lstrip(', ')
        w = re.search(r'\b([a-z]{3,})\b', after)
        # Skip generic words that appear in many addresses
        _SKIP = {"road", "street", "avenue", "lane", "place", "close", "gardens",
                 "way", "drive", "court", "house", "london", "flat", "floor"}
        if w and w.group(1) not in _SKIP:
            street_word = w.group(1)
        elif w:
            # If first word is generic, try the second meaningful word
            after2 = after[w.end():].lstrip(', ')
            w2 = re.search(r'\b([a-z]{3,})\b', after2)
            if w2 and w2.group(1) not in _SKIP:
                street_word = w2.group(1)

    # Need all three to be confident — two is too ambiguous across a city
    if not (number and street_word and outward):
        return None

    return f"{number}-{street_word}-{outward}"


async def _run_scrapers() -> list[dict]:
    """Run all scrapers concurrently and return deduplicated listings."""
    logger.info("[scheduler] Starting all scrapers…")

    # Run all three scrapers concurrently
    rm_task = asyncio.create_task(rightmove.scrape())
    zo_task = asyncio.create_task(zoopla.scrape())
    or_task = asyncio.create_task(openrent.scrape())
    otm_task = asyncio.create_task(onthemarket.scrape())

    rm_listings, zo_listings, or_listings, otm_listings = await asyncio.gather(
        rm_task, zo_task, or_task, otm_task
    )

    all_listings = rm_listings + zo_listings + or_listings + otm_listings
    logger.info(
        "[scheduler] Raw counts — Rightmove: %d  Zoopla: %d  OpenRent: %d  OTM: %d  Total: %d",
        len(rm_listings), len(zo_listings), len(or_listings), len(otm_listings), len(all_listings),
    )

    # ── Pass 0a: OTM postcode filter ─────────────────────────────────────────
    # OTM district slugs cover huge areas (nw1 = Camden→Kings Cross→Euston).
    # Drop any OTM listing whose address postcode falls outside the target zone.
    otm_before = sum(1 for l in all_listings if l.get("source") == "onthemarket")
    all_listings = [
        l for l in all_listings
        if l.get("source") != "onthemarket" or _otm_postcode_ok(l)
    ]
    otm_after = sum(1 for l in all_listings if l.get("source") == "onthemarket")
    logger.info(
        "[scheduler] OTM postcode filter: %d → %d (%d out-of-area removed)",
        otm_before, otm_after, otm_before - otm_after,
    )

    # ── Pass 0b: OTM building dedup ──────────────────────────────────────────
    # OTM often lists the same building 20-80× (different agents, no street num).
    # For each building key, keep only the single cheapest listing.
    from utils.valuation import _parse_price_pcm as _ppcm
    building_best: dict[str, dict] = {}   # bkey → cheapest listing so far
    otm_no_key:    list[dict]      = []   # OTM listings with no building key (keep all)
    non_otm:       list[dict]      = []   # non-OTM listings (untouched)

    for listing in all_listings:
        if listing.get("source") != "onthemarket":
            non_otm.append(listing)
            continue
        bkey = _otm_building_key(listing)
        if not bkey:
            otm_no_key.append(listing)
            continue
        price = _ppcm(listing.get("price", "")) or 999_999
        existing = building_best.get(bkey)
        if existing is None:
            building_best[bkey] = listing
        else:
            existing_price = _ppcm(existing.get("price", "")) or 999_999
            if price < existing_price:
                building_best[bkey] = listing   # cheaper wins

    otm_kept    = list(building_best.values()) + otm_no_key
    otm_removed = sum(1 for l in all_listings if l.get("source") == "onthemarket") - len(otm_kept)
    all_listings = non_otm + otm_kept
    if otm_removed:
        logger.info(
            "[scheduler] OTM building dedup: kept cheapest of each building group "
            "(%d duplicates removed)",
            otm_removed,
        )

    # ── Pass 1: URL dedup (same listing URL from the same platform) ──────────
    seen_urls: set[str] = set()
    url_unique: list[dict] = []
    for listing in all_listings:
        url = listing.get("url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            url_unique.append(listing)

    before_addr = len(url_unique)

    # ── Pass 2: Cross-platform address dedup ─────────────────────────────────
    # Same physical property listed on Rightmove AND Zoopla gets one entry.
    # We keep the version from the highest-priority source (OpenRent > Rightmove
    # > Zoopla > OTM) since OpenRent is typically the direct-landlord listing.
    addr_best: dict[str, dict] = {}   # dedup_key → best listing so far
    no_key: list[dict] = []           # listings with no reliable address key (keep all)

    for listing in url_unique:
        key = _address_dedup_key(listing.get("address", ""))
        if not key:
            no_key.append(listing)
            continue
        if key not in addr_best:
            addr_best[key] = listing
        else:
            # Keep whichever source has higher priority
            cur_pri = _SOURCE_PRIORITY.get(addr_best[key].get("source", ""), 99)
            new_pri = _SOURCE_PRIORITY.get(listing.get("source", ""), 99)
            if new_pri < cur_pri:
                addr_best[key] = listing

    unique = list(addr_best.values()) + no_key
    cross_removed = before_addr - len(unique)

    logger.info(
        "[scheduler] After dedup: %d unique listings "
        "(%d cross-platform duplicates removed)",
        len(unique), cross_removed,
    )
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


# ── Full automated pipeline entry point ──────────────────────────────────────

async def run_pipeline() -> None:
    """
    End-to-end pipeline:
      scheduler.py → research_bot.run_research_pipeline()
                   → filter_bot.run_filter_pipeline_and_send()
                   → Telegram messages

    All results are sent automatically — no manual commands needed.
    """
    research_token = os.getenv("TELEGRAM_RESEARCH_BOT_TOKEN")
    filter_token   = os.getenv("TELEGRAM_FILTER_BOT_TOKEN")
    chat_id        = os.getenv("TELEGRAM_RESEARCH_CHAT_ID")

    missing = [k for k, v in {
        "TELEGRAM_RESEARCH_BOT_TOKEN": research_token,
        "TELEGRAM_FILTER_BOT_TOKEN":   filter_token,
        "TELEGRAM_RESEARCH_CHAT_ID":   chat_id,
    }.items() if not v]
    if missing:
        raise ValueError(f"Missing required .env variables: {', '.join(missing)}")

    from telegram import Bot
    research_bot = Bot(token=research_token)
    filter_bot   = Bot(token=filter_token)

    # Step 1 — Research: scrape + save listings.json
    from research_bot import run_research_pipeline
    listings_count = await run_research_pipeline(research_bot, chat_id)

    if listings_count == 0:
        logger.warning("[pipeline] No listings found — aborting filter step.")
        return

    # Step 2 — Filter: FMV + walk-time analysis + send results
    from filter_bot import run_filter_pipeline_and_send
    await run_filter_pipeline_and_send(filter_bot, chat_id, listings_count)

    logger.info("[pipeline] Full pipeline complete.")


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        level=logging.INFO,
    )
    # Suppress httpx request logs — they contain the full bot token in the URL
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(run_pipeline())
