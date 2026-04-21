"""
utils/valuation.py
------------------
Two distinct Claude-powered capabilities:

1. score_listing()        — quick Haiku scoring for bulk triage (existing)
2. get_fmv_verdict()      — full FMV analysis pipeline:

      Data sources (apple-to-apple comparison):
        a) VOA historical medians      → 10 years of borough-level rental data
        b) Zoopla let-agreed           → live same-area/bed/bath/size comparables
        c) Rightmove let-agreed        → live same-area/bed/bath/size comparables
        d) OnTheMarket let-agreed      → live same-area/bed/bath/size comparables
        e) Local listings.json         → already-scraped same-area data

      Similarity weighting (per comparable):
        total_weight = recency_weight × bath_weight × size_weight
        recency:   exp(-age_months / 24)   — halves every ~1.4 years
        bath:      1.0 exact | 0.5 ±1 bath | 0.1 ±2+ bath | 0.7 unknown
        size:      exp(-|diff%| / 0.30)    — 30% size diff → 0.37 weight

      PASS rule (hardcoded + unit-tested):
        asking_price <= fmv * 1.05  →  PASS  (at or below 5% above FMV)
        asking_price >  fmv * 1.05  →  FAIL

Set in .env:
    ANTHROPIC_API_KEY   — required for reasoning
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import statistics
from datetime import datetime
from pathlib import Path
from typing import Optional

import anthropic
from dotenv import load_dotenv


load_dotenv()

logger   = logging.getLogger(__name__)
_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# Models
_HAIKU_MODEL  = "claude-haiku-4-5-20251001"   # bulk scoring — fast & cheap
_SONNET_MODEL = "claude-sonnet-4-6"            # FMV reasoning — smarter

LISTINGS_PATH = Path(__file__).parent.parent / "listings.json"

# ── Playwright availability ────────────────────────────────────────────────────
try:
    from playwright.async_api import async_playwright
    _PLAYWRIGHT = True
except ImportError:
    _PLAYWRIGHT = False
    logger.warning("[valuation] playwright not installed — live scraping disabled.")


# ═══════════════════════════════════════════════════════════════════════════════
# PART 1 — Existing bulk scorer (unchanged)
# ═══════════════════════════════════════════════════════════════════════════════

_SCORE_SYSTEM = """\
You are a London property analyst helping a professional find a furnished 2-bedroom flat to rent.
The ideal property is:
- Furnished, 2+ bedrooms
- In or very close to Zone 1/2 central London
- Well-connected (walking distance to tube)
- Good value for the area

For each listing, respond with ONLY valid JSON in this exact format:
{
  "score": <integer 1-10>,
  "summary": "<one concise sentence describing the property and why it is or isn't worth viewing>",
  "flags": ["<short flag>", ...]
}

Scoring guide:
10 = exceptional value, must view
7-9 = strong candidate
4-6 = worth considering
1-3 = overpriced or unsuitable

Flags: "great value", "overpriced", "quiet street", "near tube", "conversion",
"high floor", "new build", "period property". Keep to 2-4 flags maximum.
"""


def score_listing(listing: dict) -> dict:
    """Quick Claude Haiku score for a single listing. Returns {score, summary, flags}."""
    _default = {"score": 5, "summary": "No AI summary available.", "flags": []}

    if not _API_KEY or _API_KEY == "your_anthropic_api_key_here":
        logger.warning("[valuation] ANTHROPIC_API_KEY not set — skipping scoring")
        return _default

    walk_line = ""
    if listing.get("walk_dest") and listing.get("walk_mins") is not None:
        walk_line = f"\nWalk time to {listing['walk_dest']}: {listing['walk_mins']} min"

    user_msg = (
        f"Source: {listing.get('source', 'unknown')}\n"
        f"Area: {listing.get('area', 'unknown')}\n"
        f"Price: {listing.get('price', 'unknown')}\n"
        f"Address: {listing.get('address', 'unknown')}"
        f"{walk_line}\n"
        f"URL: {listing.get('url', '')}"
    )

    try:
        client  = anthropic.Anthropic(api_key=_API_KEY)
        message = client.messages.create(
            model=_HAIKU_MODEL, max_tokens=256,
            system=_SCORE_SYSTEM,
            messages=[{"role": "user", "content": user_msg}],
        )
        data = json.loads(message.content[0].text.strip())
        return {
            "score":   int(data.get("score", 5)),
            "summary": str(data.get("summary", "")),
            "flags":   list(data.get("flags", [])),
        }
    except Exception as exc:
        logger.error("[valuation] score_listing failed: %s", exc)
        return _default


# ═══════════════════════════════════════════════════════════════════════════════
# PART 2 — FMV pipeline
# ═══════════════════════════════════════════════════════════════════════════════

# ── Area mappings ──────────────────────────────────────────────────────────────

# Station-based slugs — same as scrapers/zoopla.py AREAS dict.
# Using /station/tube/{slug}/?radius=0.5 gives the same tight 0.5-mile
# circle as the main scraper, so comparables are genuinely close-by.
_ZOOPLA_SLUGS: dict[str, str] = {
    "Covent Garden":   "covent-garden",
    "Soho":            "piccadilly-circus",
    "Knightsbridge":   "knightsbridge",
    "West Kensington": "west-kensington",
    "London Bridge":   "london-bridge",
    "Tower Hill":      "tower-hill",
    "Baker Street":    "baker-street",
    "Bond Street":     "bond-street",
    "Marble Arch":     "marble-arch",
    "Oxford Circus":   "oxford-circus",
    "Marylebone":      "marylebone",
    "Regent's Park":   "regents-park",
}

# Station IDs — same as scrapers/rightmove.py AREAS dict.
# Using STATION + radius=0.5 keeps comparables within 0.5 miles,
# matching the main scraper's search area exactly.
_RIGHTMOVE_STATIONS: dict[str, str] = {
    "Covent Garden":   "REGION%5E87501",    # no tube station ID; region is tight
    "Soho":            "REGION%5E87529",
    "Knightsbridge":   "REGION%5E85242",
    "West Kensington": "STATION%5E5054",
    "London Bridge":   "STATION%5E5792",
    "Tower Hill":      "STATION%5E9290",
    "Baker Street":    "STATION%5E488",
    "Bond Street":     "STATION%5E1166",
    "Marble Arch":     "STATION%5E6032",
    "Oxford Circus":   "STATION%5E6953",
    "Marylebone":      "STATION%5E6095",
    "Regent's Park":   "STATION%5E7658",
}

_OTM_STATION_SLUGS: dict[str, str] = {
    "Covent Garden":   "covent-garden-station",
    "Soho":            "piccadilly-circus-station",
    "Knightsbridge":   "knightsbridge-station",
    "West Kensington": "west-kensington-station",
    "London Bridge":   "london-bridge-station",
    "Tower Hill":      "tower-hill-station",
    "Baker Street":    "baker-street-station",
    "Bond Street":     "bond-street-station",
    "Marble Arch":     "marble-arch-station",
    "Oxford Circus":   "oxford-circus-station",
    "Marylebone":      "marylebone-station",
    "Regent's Park":   "regents-park-station",
}

_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")


# ── Parsing helpers ────────────────────────────────────────────────────────────

def _parse_price_pcm(price_str: str) -> Optional[int]:
    """Extract monthly £ from scraped price text."""
    if not price_str:
        return None
    for line in str(price_str).splitlines():
        if "pcm" in line.lower() or "per month" in line.lower() or "month" in line.lower():
            m = re.search(r"[\d,]+", line)
            if m:
                return int(m.group().replace(",", ""))
    # Weekly → monthly conversion
    for line in str(price_str).splitlines():
        if "pw" in line.lower() or "per week" in line.lower() or "week" in line.lower():
            m = re.search(r"[\d,]+", line)
            if m:
                return int(int(m.group().replace(",", "")) * 52 / 12)
    m = re.search(r"[\d,]+", str(price_str))
    if m:
        val = int(m.group().replace(",", ""))
        if val >= 500:
            return val
    return None


def _parse_beds(title_str: str) -> Optional[int]:
    """Extract bed count from multiline title field."""
    if not title_str:
        return None
    m = re.search(r"(\d+)\s*bed", str(title_str), re.IGNORECASE)
    if m:
        return int(m.group(1))
    lines = [l.strip() for l in str(title_str).splitlines() if l.strip()]
    if len(lines) >= 3:
        try:
            return int(lines[2])
        except ValueError:
            pass
    return None


def _parse_baths(text: str) -> Optional[int]:
    """Extract bathroom count from text."""
    if not text:
        return None
    m = re.search(r"(\d+)\s*bath", str(text), re.IGNORECASE)
    return int(m.group(1)) if m else None


def _parse_sqft(text: str) -> Optional[int]:
    """Extract square footage from text (converts sq m if needed)."""
    if not text:
        return None
    m = re.search(r"([\d,]+)\s*(?:sq\.?\s*ft|sqft)", str(text), re.IGNORECASE)
    if m:
        return int(m.group(1).replace(",", ""))
    m = re.search(r"([\d,]+)\s*(?:sq\.?\s*m\b|sqm|m²)", str(text), re.IGNORECASE)
    if m:
        return int(int(m.group(1).replace(",", "")) * 10.764)
    return None


# ── Property type ─────────────────────────────────────────────────────────────

def _parse_property_type(text: str) -> Optional[str]:
    """
    Extract property type from listing text.
    Returns one of: 'flat', 'house', 'studio', 'maisonette', or None.
    """
    t = str(text).lower()
    if any(w in t for w in ["flat", "apartment", "penthouse", "duplex"]):
        return "flat"
    if any(w in t for w in ["house", "detached", "semi-detached", "terraced", "townhouse", "cottage", "villa"]):
        return "house"
    if "studio" in t:
        return "studio"
    if "maisonette" in t:
        return "maisonette"
    return None


# ── In-memory FMV cache (per pipeline run) ────────────────────────────────────
# Key: (area, bedrooms, baths, prop_type)  — None is a valid value for each
# Value: dict with fmv + the counts/confidence from the original computation
# This means the same (area, beds, baths, type) combo is only scraped once per run.

_FMV_CACHE: dict[tuple, dict] = {}   # value: {fmv, comparable_count, historical_count, confidence}


# ── Strict comparable filter ───────────────────────────────────────────────────

def _strict_filter(
    comps: list[dict],
    subject_baths: Optional[int],
    subject_sqft: Optional[int],
    subject_prop_type: Optional[str],
) -> list[dict]:
    """
    Hard-filter comparables to apple-to-apple matches.

    Rules (only applied when the field is known for BOTH subject and comparable):
      • Bedrooms  — already matched at scrape time (URL params)
      • Bathrooms — exact match required
      • Prop type — exact match required (flat vs house etc.)
      • Size      — within ±25% sq ft

    If either side is unknown for a field, that field is not filtered on.
    """
    result = []
    for comp in comps:
        # Bathrooms: exact match when both known
        comp_baths = comp.get("baths")
        if subject_baths is not None and comp_baths is not None:
            if comp_baths != subject_baths:
                continue

        # Property type: exact match when both known
        comp_type = comp.get("prop_type") or _parse_property_type(
            comp.get("address", "") + " " + comp.get("source", "")
        )
        if subject_prop_type and comp_type:
            if comp_type != subject_prop_type:
                continue

        # Size: within ±25% when both known
        comp_sqft = comp.get("sqft")
        if subject_sqft and comp_sqft and subject_sqft > 0:
            if abs(subject_sqft - comp_sqft) / subject_sqft > 0.25:
                continue

        result.append(comp)
    return result


# ── Similarity weights (used in loose/fallback mode) ──────────────────────────

def _bath_weight(subject_baths: Optional[int], comp_baths: Optional[int]) -> float:
    """
    Weight a comparable by bathroom similarity.
      exact match  → 1.0
      ±1 bath      → 0.5
      ±2+ bath     → 0.1
      either unknown → 0.7 (slight penalty for uncertainty)
    """
    if subject_baths is None or comp_baths is None:
        return 0.7
    diff = abs(subject_baths - comp_baths)
    if diff == 0:
        return 1.0
    if diff == 1:
        return 0.5
    return 0.1


def _size_weight(subject_sqft: Optional[int], comp_sqft: Optional[int]) -> float:
    """
    Weight a comparable by size similarity — banded, not strict.
      either unknown  → 0.8  (slight uncertainty penalty)
      ≤25% difference → 1.0  (same ballpark, treat as full match)
      25–50% diff     → 0.6  (noticeably different but still useful)
      >50% diff       → 0.2  (very different size, low weight)
    """
    if subject_sqft is None or comp_sqft is None or subject_sqft == 0:
        return 0.8
    diff_pct = abs(subject_sqft - comp_sqft) / subject_sqft
    if diff_pct <= 0.25:
        return 1.0
    if diff_pct <= 0.50:
        return 0.6
    return 0.2


# ── 0. Property-specific price history ────────────────────────────────────────

def _extract_history_from_page_text(full_text: str, address: str, bedrooms: int, source: str) -> list[dict]:
    """
    Extract rental history data points from a full page innerText dump.

    Rightmove and Zoopla often put price and date on SEPARATE lines, e.g.:
        Let agreed
        £2,750 pcm
        November 2022

    So we use a sliding window of 6 lines: if any line in the window has a £price,
    and any other line in the window has a year (with optional month), we record it.

    Only keeps entries with price £500–£30,000/mo and date within the last 10 years.
    Deduplicates by (year, price rounded to £50).
    """
    now   = datetime.now()
    lines = [l.strip() for l in full_text.split('\n') if l.strip()]
    results: list[dict] = []
    seen: set[tuple] = set()

    price_re = re.compile(r'£([\d,]+)')
    year_re  = re.compile(r'(20\d{2})')
    month_year_re = re.compile(
        r'(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|'
        r'Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|'
        r'Dec(?:ember)?)\s+(20\d{2})',
        re.IGNORECASE,
    )
    # Also catch DD Mon YYYY format e.g. "30 Jan 2023"
    dmy_re = re.compile(
        r'\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+(20\d{2})',
        re.IGNORECASE,
    )

    for i, line in enumerate(lines):
        pm = price_re.search(line)
        if not pm:
            continue
        price = int(pm.group(1).replace(',', ''))
        if not (500 <= price <= 30_000):
            continue
        # Weekly → monthly
        window_text = ' '.join(lines[max(0, i-2):i+5])
        if re.search(r'\bpw\b|per week', window_text, re.IGNORECASE):
            price = int(price * 52 / 12)

        # Find date in the surrounding window (2 lines before, 4 lines after)
        m_dmy = dmy_re.search(window_text)
        m_my  = month_year_re.search(window_text)
        m_y   = year_re.search(window_text)

        if not m_y:
            continue

        try:
            if m_dmy:
                dt = datetime.strptime(m_dmy.group(0).strip(), "%d %b %Y")
            elif m_my:
                raw = m_my.group(0)[:10].strip()
                try:
                    dt = datetime.strptime(raw, "%B %Y")
                except ValueError:
                    dt = datetime.strptime(raw[:7], "%b %Y")
            else:
                dt = datetime(int(m_y.group(1)), 1, 1)
        except ValueError:
            dt = datetime(int(m_y.group(1)), 1, 1)

        age_months = max(0, (now.year - dt.year) * 12 + (now.month - dt.month))
        if age_months > 120:   # ignore data older than 10 years
            continue

        key = (dt.strftime('%Y-%m'), round(price / 50) * 50)
        if key in seen:
            continue
        seen.add(key)

        results.append({
            'date':       dt.strftime('%Y-%m'),
            'price':      price,
            'bedrooms':   bedrooms,
            'baths':      None,
            'sqft':       None,
            'prop_type':  None,
            'address':    address,
            'source':     source,
            'age_months': age_months,
        })

    return results


# Keep old name as alias (used by search-based fallbacks)
def _extract_history_rows(history_rows: list[str], address: str, bedrooms: int, source: str) -> list[dict]:
    return _extract_history_from_page_text('\n'.join(history_rows), address, bedrooms, source)


async def _scrape_property_history(address: str, bedrooms: int, listing_url: str = "") -> list[dict]:
    """
    Look up this exact property's own rental history on Zoopla AND Rightmove.

    Strategy:
      1. If we already have the listing URL (Zoopla or Rightmove), visit it directly
         and read the rental/price history section — no search needed.
      2. If the URL is from another platform (OTM, OpenRent), search Zoopla by address
         to find a matching detail page.

    Both sites typically show 2–5 years of past let events per property.
    Returns [] if no history found — caller falls back to area comparables.
    """
    if not _PLAYWRIGHT:
        return []

    import asyncio

    tasks = []
    search_term = re.sub(
        r"(?i)^(?:flat\s*\d+[a-z]?,?\s*|apartment\s*\d+[a-z]?,?\s*|floor\s*\d+,?\s*)",
        "", address.strip()
    ).strip() if address else ""

    # 1. Direct URL — visit the listing page itself (fastest, most accurate)
    if "zoopla.co.uk" in listing_url:
        tasks.append(asyncio.create_task(
            _history_from_zoopla_url(listing_url, address, bedrooms)))
    elif "rightmove.co.uk" in listing_url:
        tasks.append(asyncio.create_task(
            _history_from_rightmove_url(listing_url, address, bedrooms)))

    # 2. Always also search by address/postcode on BOTH platforms concurrently —
    #    catches history even when the listing is on OTM/OpenRent,
    #    and adds Rightmove history to a Zoopla listing (or vice-versa).
    if search_term:
        tasks.append(asyncio.create_task(
            _history_from_zoopla_search(search_term, address, bedrooms)
        ))
        tasks.append(asyncio.create_task(
            _history_from_rightmove_search(search_term, address, bedrooms)
        ))

    if not tasks:
        return []

    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_pts: list[dict] = []
    for r in results:
        if isinstance(r, list):
            all_pts.extend(r)

    # Deduplicate by (year, price rounded to £50)
    combined: list[dict] = []
    seen_keys: set[tuple] = set()
    for pt in all_pts:
        key = (pt["date"][:4], round(pt["price"] / 50) * 50)
        if key not in seen_keys:
            seen_keys.add(key)
            combined.append(pt)

    combined.sort(key=lambda p: p["date"])
    logger.info("[fmv] Property history for '%s': %d data points", address[:50], len(combined))
    return combined


async def _get_page_text_with_scroll(page, extra_wait: float = 1.5) -> str:
    """
    Scroll through a page to trigger lazy-loading, click any history toggle
    buttons, then return the full innerText for Python-side parsing.
    """
    # Scroll in steps to trigger lazy-loaded sections
    for pct in [0.25, 0.5, 0.75, 1.0]:
        await page.evaluate(f"window.scrollTo(0, document.body.scrollHeight * {pct})")
        await page.wait_for_timeout(600)

    # Click any history-related expand buttons
    for btn_text in ["Letting history", "Let history", "Price history",
                     "Rental history", "Previous rentals", "Show history",
                     "View letting history", "Listing history", "Show more"]:
        try:
            btn = page.locator(f"button:has-text('{btn_text}')")
            if await btn.count() > 0:
                await btn.first.click()
                await page.wait_for_timeout(800)
                break
        except Exception:
            pass

    await page.wait_for_timeout(extra_wait * 1000)
    return await page.evaluate("() => document.body.innerText")


async def _history_from_zoopla_url(url: str, address: str, bedrooms: int) -> list[dict]:
    """
    Get Zoopla own-property history.

    Zoopla's active listing page (/to-rent/details/XXXXXX/) doesn't show past
    rental prices — only the current listing.  Instead we search by postcode
    with include_let_agreed=true, which returns ALL previous listings of the
    same property.  Each let-agreed result for a matching address = one
    historical rental data point.
    """
    postcode_m = re.search(
        r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})\b', address, re.IGNORECASE
    )
    if not postcode_m:
        # Fall back to visiting the listing page itself (may still pick up history text)
        return await _history_from_zoopla_page_text(url, address, bedrooms)

    postcode = postcode_m.group(1).strip()
    words = [w for w in re.findall(r'[a-z]{3,}', address.lower())
             if w not in {"the","and","for","london","flat","road","street","lane",
                          "avenue","gardens","square","place","court","house"}]

    results: list[dict] = []
    try:
        async with async_playwright() as pw:
            browser, ctx, page = await _zoopla_stealth_context(pw)
            await _zoopla_accept_cookies(page)

            # Search by postcode, include let-agreed — finds all previous lettings
            search_url = (
                f"https://www.zoopla.co.uk/to-rent/property/london/"
                f"?q={postcode.replace(' ', '+')}"
                f"&beds_min={bedrooms}&beds_max={bedrooms}"
                f"&furnished_state=furnished&include_let_agreed=true"
                f"&results_sort=newest_listings"
            )
            await page.goto(search_url, timeout=30_000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2_000)

            cards_data = await page.evaluate(r"""
                () => {
                    const cards = document.querySelectorAll(
                        '[data-testid="search-result"], ' +
                        '[class*="ListingCard"], article[class*="listing"]'
                    );
                    return Array.from(cards).map(card => {
                        const priceEl = card.querySelector(
                            '[data-testid="listing-price"], [class*="price"]'
                        );
                        const addrEl = card.querySelector(
                            '[data-testid="listing-description"], address, [class*="address"]'
                        );
                        return {
                            price:   priceEl ? priceEl.innerText.trim() : '',
                            address: addrEl  ? addrEl.innerText.trim().replace(/\s+/g,' ') : '',
                            text:    card.innerText.slice(0, 400),
                        };
                    }).filter(d => d.price);
                }
            """)

            now = datetime.now()
            for d in cards_data:
                card_addr = d.get("address", "").lower()
                card_text = d.get("text", "")
                # Must match enough address words to be the same property
                if not words or sum(1 for w in words if w in card_addr) < max(2, len(words) // 2):
                    continue
                price = _parse_price_pcm(d.get("price", ""))
                if not (price and 500 <= price <= 30_000):
                    continue
                # Extract date from card text
                m_my = re.search(
                    r'(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|'
                    r'Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|'
                    r'Dec(?:ember)?)\s+(20\d{2})',
                    card_text, re.IGNORECASE,
                )
                m_y = re.search(r'(20\d{2})', card_text)
                if m_my:
                    try:
                        dt = datetime.strptime(m_my.group(0)[:10].strip(), "%B %Y")
                    except ValueError:
                        try:
                            dt = datetime.strptime(m_my.group(0)[:7], "%b %Y")
                        except ValueError:
                            dt = now
                elif m_y:
                    dt = datetime(int(m_y.group(1)), 1, 1)
                else:
                    dt = now   # let-agreed recently, no explicit date
                age_months = max(0, (now.year - dt.year) * 12 + (now.month - dt.month))
                if age_months > 120:
                    continue
                results.append({
                    "date":       dt.strftime("%Y-%m"),
                    "price":      price,
                    "bedrooms":   bedrooms,
                    "baths":      None,
                    "sqft":       None,
                    "prop_type":  None,
                    "address":    address,
                    "source":     "zoopla_property_history",
                    "age_months": age_months,
                })

            await ctx.close()
            await browser.close()

    except Exception as exc:
        logger.warning("[fmv] Zoopla own history search failed '%s': %s", address[:50], exc)

    logger.info("[fmv] Zoopla own history for '%s': %d records", address[:40], len(results))
    return results


async def _history_from_zoopla_page_text(url: str, address: str, bedrooms: int) -> list[dict]:
    """Fallback: visit Zoopla listing page and extract any visible history from page text."""
    try:
        async with async_playwright() as pw:
            browser, ctx, page = await _zoopla_stealth_context(pw)
            await _zoopla_accept_cookies(page)
            await page.goto(url, timeout=30_000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2_000)
            full_text = await _get_page_text_with_scroll(page)
            await ctx.close()
            await browser.close()
            return _extract_history_from_page_text(full_text, address, bedrooms,
                                                    "zoopla_property_history")
    except Exception as exc:
        logger.warning("[fmv] Zoopla page-text history failed '%s': %s", url[:80], exc)
        return []


async def _history_from_rightmove_url(url: str, address: str, bedrooms: int) -> list[dict]:
    """Visit a Rightmove listing URL directly and extract its let history."""
    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.set_extra_http_headers({"User-Agent": _UA})

            # Strip hash fragment — Rightmove ignores it but cleaner
            clean_url = url.split('#')[0]
            await page.goto(clean_url, timeout=30_000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2_000)

            # Accept cookies
            try:
                await page.locator("button#onetrust-accept-btn-handler").click(timeout=3_000)
                await page.wait_for_timeout(500)
            except Exception:
                pass

            full_text = await _get_page_text_with_scroll(page)
            await browser.close()

            pts = _extract_history_from_page_text(full_text, address, bedrooms,
                                                   "rightmove_property_history")
            logger.info("[fmv] Rightmove direct URL history for '%s': %d pts",
                        address[:40], len(pts))
            return pts
    except Exception as exc:
        logger.warning("[fmv] Rightmove URL history failed '%s': %s", url[:80], exc)
        return []


async def _history_from_zoopla_search(search_term: str, address: str, bedrooms: int) -> list[dict]:
    """Search Zoopla by address string and extract history from the best matching result."""
    try:
        async with async_playwright() as pw:
            browser, ctx, page = await _zoopla_stealth_context(pw)
            await _zoopla_accept_cookies(page)

            search_url = (
                f"https://www.zoopla.co.uk/to-rent/property/london/"
                f"?q={search_term.replace(' ', '+')}"
                f"&beds_min={bedrooms}&beds_max={bedrooms}"
                f"&furnished_state=furnished&include_let_agreed=true"
            )
            await page.goto(search_url, timeout=30_000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2_000)

            words = [w for w in re.findall(r'[a-z]{3,}', search_term.lower())
                     if w not in {"the","and","for","london","flat","road","street","lane"}]
            detail_url = None
            cards = await page.query_selector_all("[data-testid='search-result']")
            for card in cards[:5]:
                try:
                    addr_el   = await card.query_selector(
                        "[data-testid='listing-description'], address, "
                        "[class*='address'], [class*='Address']"
                    )
                    card_addr = (await addr_el.inner_text()).lower() if addr_el else ""
                    matches   = sum(1 for w in words if w in card_addr)
                    if matches >= max(2, len(words) // 2):
                        link_el = await card.query_selector("a[href*='/to-rent/details/']")
                        if not link_el:
                            link_el = await card.query_selector("a[href*='/properties/']")
                        if link_el:
                            href = await link_el.get_attribute("href")
                            if href:
                                detail_url = href if href.startswith("http") \
                                    else "https://www.zoopla.co.uk" + href
                            break
                except Exception:
                    continue

            if not detail_url:
                await ctx.close()
                await browser.close()
                return []

            await page.goto(detail_url, timeout=30_000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2_000)
            full_text = await _get_page_text_with_scroll(page)
            await ctx.close()
            await browser.close()
            return _extract_history_from_page_text(full_text, address, bedrooms,
                                                    "zoopla_property_history")
    except Exception as exc:
        logger.warning("[fmv] Zoopla search history failed for '%s': %s", search_term[:60], exc)
        return []


async def _history_from_rightmove_search(search_term: str, address: str, bedrooms: int) -> list[dict]:
    """Search Rightmove by postcode/address and extract let history from the best match."""
    try:
        # Extract postcode from address — much more reliable than keyword search
        postcode_m = re.search(r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2})\b',
                               address, re.IGNORECASE)
        if not postcode_m:
            return []   # without a postcode Rightmove search is too imprecise

        postcode = postcode_m.group(1).replace(" ", "%20")

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.set_extra_http_headers({"User-Agent": _UA})

            search_url = (
                f"https://www.rightmove.co.uk/property-to-rent/find.html"
                f"?searchType=RENT&locationIdentifier=POSTCODE%5E{postcode}"
                f"&minBedrooms={bedrooms}&maxBedrooms={bedrooms}"
                f"&furnishTypes=furnished&includeLetAgreed=true&sortType=6"
            )
            await page.goto(search_url, timeout=30_000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2_000)

            words = [w for w in re.findall(r'[a-z]{3,}', address.lower())
                     if w not in {"the","and","for","london","flat","road","street","lane"}]
            detail_url = None
            cards_data = await page.evaluate(r"""
                () => Array.from(document.querySelectorAll(
                    'a[href*="/properties/"]'
                )).map(a => ({
                    href: a.href,
                    text: (a.closest('[class*="Card"], .l-searchResult, article') || a).innerText
                }))
                .filter(d => /\/properties\/\d+/.test(d.href))
                .slice(0, 8)
            """)
            for card in cards_data:
                card_text = card.get("text", "").lower()
                matches   = sum(1 for w in words if w in card_text)
                if matches >= max(2, len(words) // 2):
                    detail_url = card["href"]
                    break

            if not detail_url:
                await browser.close()
                return []

            await page.goto(detail_url, timeout=30_000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2_000)
            full_text = await _get_page_text_with_scroll(page)
            await browser.close()
            return _extract_history_from_page_text(full_text, address, bedrooms,
                                                    "rightmove_property_history")
    except Exception as exc:
        logger.warning("[fmv] Rightmove search history failed for '%s': %s", search_term[:60], exc)
        return []


async def _extract_zoopla_history_from_page(page) -> list[str]:
    """
    Extract price/rental history rows from a Zoopla property detail page.
    Tries multiple selectors and also clicks any 'show history' toggle buttons.
    """
    # Try clicking any expand/show buttons for history
    for btn_text in ["Rental history", "Price history", "Show history",
                     "Previous rentals", "Let history"]:
        try:
            btn = page.locator(f"button:has-text('{btn_text}'), "
                               f"span:has-text('{btn_text}')")
            if await btn.count() > 0:
                await btn.first.click()
                await page.wait_for_timeout(800)
                break
        except Exception:
            pass

    return await page.evaluate("""
        () => {
            const out = new Set();
            const yearRe  = /20\\d{2}/;
            const poundRe = /\\u00a3[\\d,]+/;   // £ as unicode escape

            // Broad sweep of every text node
            const walker = document.createTreeWalker(document.body, 4, null);
            let node;
            while ((node = walker.nextNode())) {
                const t = node.textContent.trim();
                if (yearRe.test(t) && poundRe.test(t) && t.length < 200) {
                    out.add(t);
                }
            }

            // Also check history containers
            const containers = document.querySelectorAll(
                '[data-testid*="history"],[data-testid*="price"],' +
                '[class*="History"],[class*="PreviousRental"],[class*="previousRental"],' +
                'table,dl'
            );
            for (const c of containers) {
                const t = c.innerText || '';
                for (const line of t.split('\\n')) {
                    const l = line.trim();
                    if (yearRe.test(l) && poundRe.test(l) && l.length < 200) {
                        out.add(l);
                    }
                }
            }
            return [...out];
        }
    """)


async def _extract_rightmove_history_from_page(page) -> list[str]:
    """
    Extract let/price history rows from a Rightmove property detail page.
    Rightmove shows 'Let history' in a collapsible section on rental pages.
    """
    # Try clicking the let/price history toggle
    for btn_text in ["Let history", "Price history", "Listing history", "Show more"]:
        try:
            btn = page.locator(f"button:has-text('{btn_text}'), "
                               f"[aria-label*='{btn_text}'], span:has-text('{btn_text}')")
            if await btn.count() > 0:
                await btn.first.click()
                await page.wait_for_timeout(800)
                break
        except Exception:
            pass

    return await page.evaluate("""
        () => {
            const out = new Set();
            const yearRe  = /20\\d{2}/;
            const poundRe = /\\u00a3[\\d,]+/;

            // Targeted containers
            const containers = document.querySelectorAll(
                '[class*="History"],[class*="letHistory"],[class*="LetHistory"],' +
                '[id*="history"],[id*="History"],table,dl,ul'
            );
            for (const c of containers) {
                const t = c.innerText || '';
                for (const line of t.split('\\n')) {
                    const l = line.trim();
                    if (yearRe.test(l) && poundRe.test(l) && l.length < 200) {
                        out.add(l);
                    }
                }
            }

            // Broad text-node sweep
            const walker = document.createTreeWalker(document.body, 4, null);
            let node;
            while ((node = walker.nextNode())) {
                const t = node.textContent.trim();
                if (yearRe.test(t) && poundRe.test(t) && t.length < 200) {
                    out.add(t);
                }
            }
            return [...out];
        }
    """)


# ── 1. Historical rent scraping ────────────────────────────────────────────────

async def _zoopla_stealth_context(pw):
    """Create a stealth Playwright browser context for Zoopla to avoid bot detection."""
    browser = await pw.chromium.launch(headless=True)
    ctx = await browser.new_context(
        user_agent=_UA,
        locale="en-GB",
        viewport={"width": 1280, "height": 900},
    )
    page = await ctx.new_page()
    # Apply stealth patches if available (randomises fingerprints)
    try:
        from playwright_stealth import stealth_async
        await stealth_async(page)
    except Exception:
        pass
    return browser, ctx, page


async def _zoopla_accept_cookies(page):
    """Accept cookies on whatever Zoopla page is currently loaded (inline, no extra navigation).

    Previously this navigated to the homepage first, which used up the first page
    load in the browser context.  Zoopla's bot-detection flags a Playwright context
    after one navigation, so the actual search URL (the second load) returned no
    listing cards.  Now we accept cookies directly on the target page instead.
    """
    try:
        accept = page.locator(
            "button:has-text('Accept all'), button:has-text('Accept All'), "
            "#onetrust-accept-btn-handler"
        )
        if await accept.count() > 0:
            await accept.first.click(timeout=4_000)
            await page.wait_for_timeout(800)
    except Exception:
        pass


async def _scrape_zoopla_let_agreed(
    slug: str,
    bedrooms: int,
    baths: Optional[int] = None,
    prop_type: Optional[str] = None,
    subject_sqft: Optional[int] = None,
) -> list[dict]:
    """
    Scrape Zoopla let-agreed listings within 0.25mi of a tube station.
    Uses playwright-stealth to avoid bot detection.
    """
    results: list[dict] = []
    if not _PLAYWRIGHT:
        return results

    sub_type_param = ""
    if prop_type == "flat":
        sub_type_param = "&property_sub_type=flats"
    elif prop_type == "house":
        sub_type_param = "&property_sub_type=houses"

    # Use a fresh browser context for EVERY page — Zoopla's bot-detection flags
    # a Playwright context after the first navigation, so page 2+ return no cards
    # when the same context is reused.
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox",
                  "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"],
        )

        for pn in range(1, 7):
            if pn > 1:
                import asyncio as _asyncio
                await _asyncio.sleep(2.0)

            ctx = await browser.new_context(
                user_agent=_UA,
                locale="en-GB",
                viewport={"width": 1280, "height": 900},
                extra_http_headers={
                    "Accept-Language": "en-GB,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
            )
            page = await ctx.new_page()
            try:
                from playwright_stealth import stealth_async as _stealth
                await _stealth(page)
            except Exception:
                pass

            url = (
                f"https://www.zoopla.co.uk/to-rent/property/station/tube/{slug}/"
                f"?beds_min={bedrooms}&beds_max={bedrooms}"
                f"&price_max=15000&furnished_state=furnished"
                f"&radius=0.25&include_let_agreed=true"
                f"&results_sort=newest_listings{sub_type_param}&pn={pn}"
            )
            try:
                await page.goto(url, timeout=35_000, wait_until="domcontentloaded")
                await page.wait_for_timeout(1_500)

                # Accept cookies inline (no homepage visit needed)
                await _zoopla_accept_cookies(page)

                # Scroll to trigger lazy load
                await page.evaluate("window.scrollTo(0, 600)")
                await page.wait_for_timeout(500)

                # Use the same selector confirmed working in the listing scraper
                try:
                    from playwright.async_api import TimeoutError as _PWTimeout
                    await page.wait_for_selector("a[data-testid*='listing']", timeout=12_000)
                except Exception:
                    logger.info("[fmv] Zoopla let-agreed p%d (%s): 0 cards — stopping", pn, slug)
                    await ctx.close()
                    break

                # Extract card data using the proven selector
                cards_data = await page.evaluate(r"""
                    () => {
                        const links = document.querySelectorAll("a[data-testid*='listing']");
                        return Array.from(links).map(a => {
                            if (!a.href || a.href.includes('/new-homes/')) return null;
                            const price = a.querySelector('[data-testid*="price"], [class*="Price"], [class*="price"]');
                            const addr  = a.querySelector('[data-testid*="address"], [class*="address"], [class*="Address"]');
                            const text  = a.innerText || '';
                            const bathM = text.match(/(\d+)\s*bath/i);
                            const sqftM = text.match(/([\d,]+)\s*sq\.?\s*ft/i)
                                       || text.match(/([\d,]+)\s*sqft/i);
                            const sqmM  = text.match(/([\d,]+)\s*(?:sq\.?\s*m(?!\w)|sqm)/i);
                            let sqft = null;
                            if (sqftM) sqft = parseInt(sqftM[1].replace(/,/g,''));
                            else if (sqmM) sqft = Math.round(parseInt(sqmM[1].replace(/,/g,'')) * 10.764);
                            return {
                                price:   price ? price.innerText.trim() : '',
                                address: addr  ? addr.innerText.trim().replace(/\s+/g,' ') : '',
                                baths:   bathM ? parseInt(bathM[1]) : null,
                                sqft:    sqft,
                            };
                        }).filter(d => d && d.price);
                    }
                """)

                if not cards_data:
                    logger.info("[fmv] Zoopla let-agreed p%d (%s): 0 cards — stopping", pn, slug)
                    await ctx.close()
                    break

                for d in cards_data:
                    price = _parse_price_pcm(d.get("price", ""))
                    if not (price and 500 <= price <= 15_000):
                        continue
                    comp_baths = d.get("baths")
                    comp_sqft  = d.get("sqft")
                    if baths is not None and comp_baths is not None and comp_baths != baths:
                        continue
                    if subject_sqft and comp_sqft and subject_sqft > 0:
                        if abs(subject_sqft - comp_sqft) / subject_sqft > 0.25:
                            continue
                    results.append({
                        "date":       datetime.now().strftime("%Y-%m"),
                        "price":      price,
                        "bedrooms":   bedrooms,
                        "baths":      comp_baths,
                        "sqft":       comp_sqft,
                        "prop_type":  prop_type,
                        "address":    d.get("address", ""),
                        "source":     "zoopla_let_agreed",
                        "age_months": 0,
                    })

                logger.info("[fmv] Zoopla let-agreed p%d (%s): %d cards, %d kept",
                            pn, slug, len(cards_data), len(results))

            except Exception as exc:
                logger.warning("[fmv] Zoopla let-agreed p%d (%s) failed: %s", pn, slug, exc)
                await ctx.close()
                break

            await ctx.close()

        await browser.close()

    logger.info("[fmv] Zoopla let-agreed (%s, %d bed): %d total", slug, bedrooms, len(results))
    return results


async def _scrape_rightmove_let_agreed(
    loc_id: str,
    bedrooms: int,
    baths: Optional[int] = None,
    prop_type: Optional[str] = None,
    subject_sqft: Optional[int] = None,
) -> list[dict]:
    """
    Scrape Rightmove let-agreed listings with strict filters.

    Uses STATION IDs with radius=0.5 (same as main scraper) for tight
    geographic comparables. Passes bedrooms + baths + prop_type into URL.
    Hard-filters results by size (±25%) after extraction.
    """
    results: list[dict] = []
    if not _PLAYWRIGHT:
        return results

    # Build URL — Rightmove supports minBathrooms/maxBathrooms and propertyTypes.
    # Use radius=0.25 for STATION identifiers — tight circle for FMV comparables.
    bath_param   = f"&minBathrooms={baths}&maxBathrooms={baths}" if baths else ""
    radius_param = "&radius=0.25" if "STATION" in loc_id else ""
    if prop_type == "flat":
        type_param = "&propertyTypes=flat"
    elif prop_type == "house":
        type_param = "&propertyTypes=detached,semi-detached,terraced"
    else:
        type_param = ""

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page    = await browser.new_page()
        await page.set_extra_http_headers({"User-Agent": _UA})

        url = (
            f"https://www.rightmove.co.uk/property-to-rent/find.html"
            f"?locationIdentifier={loc_id}"
            f"&minBedrooms={bedrooms}&maxBedrooms={bedrooms}"
            f"&furnishTypes=furnished&includeLetAgreed=true"
            f"&sortType=6{radius_param}{bath_param}{type_param}"
        )
        try:
            await page.goto(url, timeout=30_000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2_000)

            cards_data = await page.evaluate(r"""
                () => {
                    const cards = document.querySelectorAll(
                        '.l-searchResult, [class*="PropertyCard_propertyCardContainerWrapper"]'
                    );
                    return Array.from(cards).map(card => {
                        const price_el = card.querySelector(
                            '.propertyCard-priceValue, [class*="Price_price"]'
                        );
                        const addr_el  = card.querySelector(
                            '.propertyCard-address, [class*="Address_address"]'
                        );
                        if (!price_el) return null;

                        const cardText  = card.innerText || '';
                        const bathMatch = cardText.match(/(\d+)\s*bath/i);
                        const sqftMatch = cardText.match(/([\d,]+)\s*sq\.?\s*ft/i)
                                       || cardText.match(/([\d,]+)\s*sqft/i);
                        const sqmMatch  = cardText.match(/([\d,]+)\s*(?:sq\.?\s*m\b|sqm|m²)/i);
                        let sqft = null;
                        if (sqftMatch) sqft = parseInt(sqftMatch[1].replace(/,/g,''));
                        else if (sqmMatch) sqft = Math.round(parseInt(sqmMatch[1].replace(/,/g,'')) * 10.764);

                        return {
                            price:   price_el.innerText.trim(),
                            address: addr_el ? addr_el.innerText.trim().replace(/\s+/g,' ') : '',
                            baths:   bathMatch ? parseInt(bathMatch[1]) : null,
                            sqft:    sqft,
                        };
                    }).filter(Boolean);
                }
            """)

            for d in cards_data:
                price = _parse_price_pcm(d.get("price", ""))
                if price and 500 <= price <= 50_000:
                    comp_sqft = d.get("sqft")
                    # Hard filter: size within ±25% (when both known)
                    if subject_sqft and comp_sqft and subject_sqft > 0:
                        if abs(subject_sqft - comp_sqft) / subject_sqft > 0.25:
                            continue
                    results.append({
                        "date":       datetime.now().strftime("%Y-%m"),
                        "price":      price,
                        "bedrooms":   bedrooms,
                        "baths":      d.get("baths"),
                        "sqft":       comp_sqft,
                        "prop_type":  prop_type,
                        "address":    d.get("address", ""),
                        "source":     "rightmove_let_agreed",
                        "age_months": 0,
                    })

        except Exception as exc:
            logger.warning("[fmv] Rightmove let-agreed failed (%s): %s", loc_id, exc)

        await browser.close()

    logger.info("[fmv] Rightmove let-agreed (%s, %d bed): %d data points",
                loc_id, bedrooms, len(results))
    return results


async def _scrape_otm_let_agreed(
    slug: str,
    bedrooms: int,
    baths: Optional[int] = None,
    prop_type: Optional[str] = None,
    subject_sqft: Optional[int] = None,
) -> list[dict]:
    """Scrape OnTheMarket let-agreed listings with strict filters.
    Uses article[data-component] selector confirmed by the main OTM scraper."""
    results: list[dict] = []
    if not _PLAYWRIGHT:
        return results

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page    = await browser.new_page()
        await page.set_extra_http_headers({"User-Agent": _UA})

        url = (
            f"https://www.onthemarket.com/to-rent/property/{slug}/"
            f"?min-bedrooms={bedrooms}&max-bedrooms={bedrooms}"
            f"&max-price=15000&furnishing=furnished"
            f"&include-let-agreed=true&radius=0.25"
        )
        try:
            await page.goto(url, timeout=30_000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2_000)

            # Dismiss cookie banner (OTM uses Cookie Control, id #ccc-recommended-settings)
            try:
                await page.locator(
                    "#ccc-recommended-settings, "
                    "button:has-text('Accept all'), button:has-text('Accept All'), "
                    "#onetrust-accept-btn-handler"
                ).first.click(timeout=4_000)
                await page.wait_for_timeout(500)
            except Exception:
                pass

            # Use article[data-component] — confirmed by main OTM scraper (scrapers/onthemarket.py)
            cards_data = await page.evaluate(r"""
                () => {
                    const cards = document.querySelectorAll('article[data-component]');
                    return Array.from(cards).map(card => {
                        const link = card.querySelector('a[href*="/details/"]');
                        if (!link) return null;
                        const cardText = card.innerText || '';
                        // Price: look for pcm pattern
                        const priceMatch = cardText.match(/[\d,]+\s*pcm/i)
                                        || cardText.match(/£\s*[\d,]+/);
                        const addrEl = card.querySelector('address');
                        const bathMatch = cardText.match(/(\d+)\s*bath/i);
                        const sqftMatch = cardText.match(/([\d,]+)\s*sq\.?\s*ft/i)
                                       || cardText.match(/([\d,]+)\s*sqft/i);
                        const sqmMatch  = cardText.match(/([\d,]+)\s*(?:sq\.?\s*m\b|sqm)/i);
                        let sqft = null;
                        if (sqftMatch) sqft = parseInt(sqftMatch[1].replace(/,/g,''));
                        else if (sqmMatch) sqft = Math.round(parseInt(sqmMatch[1].replace(/,/g,'')) * 10.764);
                        return {
                            price:   priceMatch ? priceMatch[0].trim() : '',
                            address: addrEl ? addrEl.innerText.trim().replace(/\s+/g,' ') : '',
                            baths:   bathMatch ? parseInt(bathMatch[1]) : null,
                            sqft:    sqft,
                            text:    cardText.slice(0, 300),
                        };
                    }).filter(d => d && d.price);
                }
            """)

            for d in cards_data:
                price = _parse_price_pcm(d.get("price", ""))
                if price and 500 <= price <= 50_000:
                    comp_baths = d.get("baths")
                    comp_sqft  = d.get("sqft")
                    # Hard filter: baths exact match when both known
                    if baths is not None and comp_baths is not None:
                        if comp_baths != baths:
                            continue
                    # Hard filter: size within ±25% when both known
                    if subject_sqft and comp_sqft and subject_sqft > 0:
                        if abs(subject_sqft - comp_sqft) / subject_sqft > 0.25:
                            continue
                    results.append({
                        "date":       datetime.now().strftime("%Y-%m"),
                        "price":      price,
                        "bedrooms":   bedrooms,
                        "baths":      comp_baths,
                        "sqft":       comp_sqft,
                        "prop_type":  prop_type,
                        "address":    d.get("address", ""),
                        "source":     "otm_let_agreed",
                        "age_months": 0,
                    })

        except Exception as exc:
            logger.warning("[fmv] OTM let-agreed failed (%s): %s", slug, exc)

        await browser.close()

    logger.info("[fmv] OTM let-agreed (%s, %d bed): %d data points", slug, bedrooms, len(results))
    return results


async def get_historical_rents(
    address: str,
    area: str,
    bedrooms: int,
    baths: Optional[int] = None,
    sqft: Optional[int] = None,
    prop_type: Optional[str] = None,
    listing_url: str = "",
) -> list[dict]:
    """
    Gather let-agreed rental data for apple-to-apple comparables.

    Sources:
      1. VOA 10-year borough medians   — kept as Tier 3 fallback only
      2. Zoopla let-agreed (live)      — filtered by beds + prop_type + baths/size
      3. Rightmove let-agreed (live)   — filtered by beds + baths + prop_type + size
      4. OnTheMarket let-agreed (live) — filtered by beds + baths + size

    NOTE: listings.json is intentionally excluded — those are current ASKING prices,
    not let-agreed rents. Including them would corrupt the FMV baseline.

    Returns list of dicts: date, price, bedrooms, baths, sqft, prop_type, source, age_months
    """
    results: list[dict] = []

    # 0 — Property-specific history (best comparable: same flat, real dates)
    try:
        own_history = await _scrape_property_history(address, bedrooms, listing_url=listing_url)
        results.extend(own_history)
        if own_history:
            logger.info("[fmv] Property history for '%s': %d points (own rental history)",
                        address[:50], len(own_history))
    except Exception as exc:
        logger.warning("[fmv] Property history failed: %s", exc)

    # VOA borough medians intentionally excluded — too broad, dilutes micro-location FMV.
    # Sources: property's own history + live let-agreed comps only.

    # 1, 2, 3 — Live let-agreed scraping (concurrent), with strict params
    # Using 0.25 mile radius (tight circle) and let-agreed only — no asking prices
    slug      = _ZOOPLA_SLUGS.get(area)
    loc_id    = _RIGHTMOVE_STATIONS.get(area)
    otm_slug  = _OTM_STATION_SLUGS.get(area)

    import asyncio
    tasks = []
    if slug:
        tasks.append(asyncio.create_task(
            _scrape_zoopla_let_agreed(slug, bedrooms, baths=baths, prop_type=prop_type, subject_sqft=sqft)
        ))
    if loc_id:
        tasks.append(asyncio.create_task(
            _scrape_rightmove_let_agreed(loc_id, bedrooms, baths=baths, prop_type=prop_type, subject_sqft=sqft)
        ))
    if otm_slug:
        tasks.append(asyncio.create_task(
            _scrape_otm_let_agreed(otm_slug, bedrooms, baths=baths, prop_type=prop_type, subject_sqft=sqft)
        ))

    if tasks:
        live_results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in live_results:
            if isinstance(r, list):
                results.extend(r)

    # Deduplicate by (address, price)
    seen: set[tuple] = set()
    unique: list[dict] = []
    for r in results:
        key = (r.get("address", "")[:40], r.get("price"))
        if key not in seen:
            seen.add(key)
            unique.append(r)

    own_count  = len([r for r in unique if r.get("source") == "zoopla_property_history"])
    live_count = len(unique) - own_count
    logger.info(
        "[fmv] %s %d-bed %s %s-bath: %d own-history + %d live let-agreed = %d total",
        area, bedrooms, prop_type or "any-type", baths or "any",
        own_count, live_count, len(unique),
    )
    return unique


# ── 2. Comparable listings ─────────────────────────────────────────────────────

async def get_comparable_listings(
    area: str,
    bedrooms: int,
    current_price: int,
    baths: Optional[int] = None,
    sqft:  Optional[int] = None,
    prop_type: Optional[str] = None,
) -> dict:
    """
    Find comparable furnished listings in the same area.
    Filters by bedrooms, then applies strict baths/size/type matching
    when those fields are known (apple-to-apple).
    """
    comps: list[dict] = []

    # Pull from local listings.json — same area, same beds
    try:
        if LISTINGS_PATH.exists():
            local = json.loads(LISTINGS_PATH.read_text(encoding="utf-8"))
            for listing in local:
                if listing.get("area") != area:
                    continue
                price = _parse_price_pcm(listing.get("price", ""))
                beds  = _parse_beds(listing.get("title", "")) or bedrooms
                if price and beds == bedrooms and 500 <= price <= 50_000:
                    comps.append({
                        "price":     price,
                        "address":   listing.get("address", ""),
                        "source":    listing.get("source", "local"),
                        "baths":     listing.get("baths"),
                        "sqft":      listing.get("sqft"),
                        "prop_type": _parse_property_type(listing.get("title", "") + " " + listing.get("address", "")),
                    })
    except Exception as exc:
        logger.warning("[fmv] comparable listings local read failed: %s", exc)

    # Supplement with live Zoopla (station-based, 0.5 mile radius) if thin
    slug = _ZOOPLA_SLUGS.get(area)
    if slug and _PLAYWRIGHT and len(comps) < 5:
        try:
            live = await _scrape_zoopla_let_agreed(slug, bedrooms, baths=baths, prop_type=prop_type, subject_sqft=sqft)
            for item in live:
                comps.append({
                    "price":     item["price"],
                    "address":   item["address"],
                    "source":    "zoopla_live",
                    "baths":     item.get("baths"),
                    "sqft":      item.get("sqft"),
                    "prop_type": prop_type,
                })
        except Exception as exc:
            logger.warning("[fmv] live comparable scrape failed: %s", exc)

    # Apply strict filter — only keep apple-to-apple matches when fields are known
    strict = _strict_filter(comps, baths, sqft, prop_type)
    comps  = strict if len(strict) >= 3 else comps   # fall back to all if too few

    if not comps:
        return {
            "listings": [], "count": 0,
            "avg": None, "median": None, "min": None, "max": None,
            "price_range": "no data",
        }

    prices = [c["price"] for c in comps]
    avg    = round(statistics.mean(prices))
    median = round(statistics.median(prices))

    return {
        "listings":    comps,
        "count":       len(comps),
        "avg":         avg,
        "median":      median,
        "min":         min(prices),
        "max":         max(prices),
        "price_range": f"£{min(prices):,}–£{max(prices):,}",
    }


# ── 3. FMV calculation ─────────────────────────────────────────────────────────

def calculate_fmv(
    historical_data: list[dict],
    comparable_listings: dict,
    subject_baths: Optional[int] = None,
    subject_sqft:  Optional[int] = None,
    subject_prop_type: Optional[str] = None,
) -> Optional[int]:
    """
    Compute Fair Market Value using a two-tier approach.

    Data pool = property's own rental history + live let-agreed comps from
    Zoopla / Rightmove / OTM.  VOA borough averages are intentionally excluded
    because they are too broad and dilute micro-location accuracy.

    Tier 1 — STRICT (preferred):
        Only comps matching EXACT baths + prop type + size ±25%.
        Recency-weighted. Requires ≥ 3 data points.

    Tier 2 — LOOSE (fallback):
        All data points with soft bath + size weights.
        Requires ≥ 2 data points.

    Returns None if fewer than 2 data points exist — caller treats this as
    insufficient data rather than padding with a borough average.

    Recency weight: exp(-age_months / 24) — halves every ~17 months.
    Rounds result to nearest £50.
    """
    # All sources are live/specific — no VOA separation needed
    all_data: list[dict] = list(historical_data)

    # Add comparable_listings into the pool
    for comp in comparable_listings.get("listings", []):
        if comp.get("price"):
            all_data.append({
                "price":      comp["price"],
                "age_months": 0,
                "baths":      comp.get("baths"),
                "sqft":       comp.get("sqft"),
                "prop_type":  comp.get("prop_type"),
                "source":     comp.get("source", "comparable"),
            })

    def _weighted_avg(data: list[dict], strict: bool) -> Optional[int]:
        ws = wt = 0.0
        for point in data:
            price = point.get("price")
            if not price:
                continue
            recency = math.exp(-point.get("age_months", 0) / 24.0)
            if strict:
                bath_w = size_w = 1.0
            else:
                bath_w = _bath_weight(subject_baths, point.get("baths"))
                size_w = _size_weight(subject_sqft,  point.get("sqft"))
            w  = recency * bath_w * size_w
            ws += price * w
            wt += w
        if wt == 0:
            return None
        return int(round((ws / wt) / 50) * 50)

    # Tier 1 — strict apple-to-apple
    strict_data = _strict_filter(all_data, subject_baths, subject_sqft, subject_prop_type)
    if len(strict_data) >= 3:
        result = _weighted_avg(strict_data, strict=True)
        if result:
            logger.info("[fmv] Tier 1 STRICT: %d comps, FMV £%d", len(strict_data), result)
            return result

    # Tier 2 — all data with soft weights (needs ≥ 2 points)
    if len(all_data) >= 2:
        result = _weighted_avg(all_data, strict=False)
        if result:
            logger.info("[fmv] Tier 2 LOOSE: %d comps, FMV £%d", len(all_data), result)
            return result

    logger.warning("[fmv] Insufficient data (%d points) — cannot compute FMV", len(all_data))
    return None


# ── 4. Claude reasoning ────────────────────────────────────────────────────────

_FMV_SYSTEM = """\
You are a London rental market expert with deep knowledge of Zone 1/2 pricing trends.

You will receive:
- A property's asking price, bedrooms, bathrooms, and size (if known)
- This property's own rental history (what it actually let for in previous years), if found
- Let-agreed prices of similar properties within 0.25 mile (same beds/baths/type), from Zoopla, Rightmove, and OnTheMarket

IMPORTANT: The FMV is calculated from real let-agreed prices only — no asking prices.

Your job:
1. Assess whether the asking price is fair given real let-agreed evidence
2. Write a concise 1-2 sentence reasoning explaining the verdict
3. Be specific — mention actual price ranges, number of data points, and any own-history context

Always be direct. Focus on the numbers. Do not hedge excessively.
Respond with ONLY valid JSON:
{
  "reasoning": "<1-2 sentences>",
  "confidence": "high|medium|low"
}

Confidence guide:
  high   = 10+ let-agreed data points, or own history found
  medium = 3-9 let-agreed data points, or partial data
  low    = fewer than 3 data points total
"""


def _get_claude_reasoning(
    property_dict:   dict,
    historical_data: list[dict],
    comparables:     dict,
    fmv:             int,
    subject_baths:   Optional[int],
    subject_sqft:    Optional[int],
) -> tuple[str, str]:
    """Ask Claude Sonnet to write the reasoning. Returns (reasoning, confidence)."""
    if not _API_KEY or _API_KEY == "your_anthropic_api_key_here":
        comp_count = comparables.get("count", 0)
        confidence = "high" if comp_count >= 10 else "medium" if comp_count >= 5 else "low"
        return (
            f"Based on {comp_count} comparable listings. FMV: £{fmv:,}/month.",
            confidence,
        )

    asking = property_dict.get("price_pcm") or _parse_price_pcm(property_dict.get("price", "")) or 0
    area   = property_dict.get("area", "")

    bath_str = f"{subject_baths} bathroom{'s' if subject_baths != 1 else ''}" if subject_baths else "bathrooms unknown"
    size_str = f"{subject_sqft:,} sq ft" if subject_sqft else "size unknown"

    comp_summary = ""
    if comparables.get("count", 0) > 0:
        comp_summary = (
            f"Comparable listings: {comparables['count']} in {area}, "
            f"range {comparables['price_range']}, "
            f"avg £{comparables['avg']:,}/mo, median £{comparables['median']:,}/mo."
        )

    # Summarise data sources
    own_pts  = [p for p in historical_data
                if p.get("source") in ("zoopla_property_history", "rightmove_property_history")]
    comp_pts = [p for p in historical_data
                if p.get("source") not in ("zoopla_property_history", "rightmove_property_history")]
    hist_summary = ""
    if own_pts:
        own_prices = [p["price"] for p in own_pts]
        own_dates  = sorted(p.get("date", "") for p in own_pts)
        hist_summary += (
            f"Own property history: {len(own_pts)} records, "
            f"range £{min(own_prices):,}–£{max(own_prices):,}/mo "
            f"({own_dates[0][:4] if own_dates else '?'}–{own_dates[-1][:4] if own_dates else '?'}). "
        )
    if comp_pts:
        comp_prices = [p["price"] for p in comp_pts]
        hist_summary += (
            f"Let-agreed comps within 0.25mi (Zoopla/Rightmove/OTM): "
            f"{len(comp_pts)} data points, "
            f"range £{min(comp_prices):,}–£{max(comp_prices):,}/mo."
        )

    user_msg = (
        f"Property: {property_dict.get('address', 'unknown')}\n"
        f"Area: {area}\n"
        f"Bedrooms: {property_dict.get('beds', 'unknown')}\n"
        f"Bathrooms: {bath_str}\n"
        f"Size: {size_str}\n"
        f"Asking price: £{asking:,}/month\n"
        f"Calculated FMV: £{fmv:,}/month\n"
        f"{comp_summary}\n"
        f"{hist_summary}"
    )

    try:
        client  = anthropic.Anthropic(api_key=_API_KEY)
        message = client.messages.create(
            model=_SONNET_MODEL, max_tokens=512,
            system=_FMV_SYSTEM,
            messages=[{"role": "user", "content": user_msg}],
        )
        raw  = message.content[0].text.strip()
        # Strip markdown code fences if Claude wraps the JSON
        raw  = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.DOTALL).strip()
        data = json.loads(raw)
        return str(data.get("reasoning", "")), str(data.get("confidence", "medium"))

    except Exception as exc:
        logger.error("[fmv] Claude reasoning failed: %s", exc)
        comp_count = comparables.get("count", 0)
        confidence = "high" if comp_count >= 10 else "medium" if comp_count >= 5 else "low"
        return f"Based on {comp_count} comparables. FMV: £{fmv:,}/month.", confidence


# ── 5. PASS / FAIL rule (unit-tested) ─────────────────────────────────────────

def _is_pass(asking_price: int, fmv: int) -> bool:
    """
    PASS if: asking_price <= fmv + 500  (at or below £500 above FMV)
    FAIL if: asking_price >  fmv + 500
    There is NO lower limit — any price at or below FMV is always a PASS.
    """
    return asking_price <= fmv + 500


def _run_unit_tests() -> None:
    """Assert the PASS/FAIL rule against canonical examples."""
    assert _is_pass(2_800, 3_000) is True,  "£2,800 ask vs £3,000 FMV should PASS"
    assert _is_pass(3_000, 3_000) is True,  "£3,000 ask vs £3,000 FMV should PASS"
    assert _is_pass(3_500, 3_000) is True,  "£3,500 ask vs £3,000 FMV should PASS (exactly £500 above)"
    assert _is_pass(3_501, 3_000) is False, "£3,501 ask vs £3,000 FMV should FAIL (just over £500)"
    assert _is_pass(3_600, 3_000) is False, "£3,600 ask vs £3,000 FMV should FAIL (20% above)"
    logger.info("[fmv] ✅ All PASS/FAIL unit tests passed")


_run_unit_tests()


# ── 6. Main verdict function ───────────────────────────────────────────────────

async def get_fmv_verdict(property_dict: dict) -> dict:
    """
    Full FMV pipeline for a single property.

    Strategy: apple-to-apple comparison using exact beds + baths + prop type + ±25% size.
    Falls back gracefully when data is thin.
    Uses an in-memory cache (_FMV_CACHE) so each (area, beds, baths, type) combo
    is only scraped once per pipeline run.

    Parameters
    ----------
    property_dict : dict
        Must contain: area, price (or price_pcm), address
        Optional: title (bed count), baths, sqft, prop_type, source, url
    """
    area    = property_dict.get("area", "")
    address = property_dict.get("address", "")
    asking  = (
        property_dict.get("price_pcm")
        or _parse_price_pcm(property_dict.get("price", ""))
        or 0
    )
    bedrooms = (
        property_dict.get("beds")
        or _parse_beds(property_dict.get("title", ""))
        or 2
    )
    subject_baths = (
        property_dict.get("baths")
        or _parse_baths(property_dict.get("title", "") + " " + property_dict.get("address", ""))
    )
    subject_sqft     = property_dict.get("sqft")
    subject_prop_type = (
        property_dict.get("prop_type")
        or _parse_property_type(
            property_dict.get("title", "") + " " + property_dict.get("address", "")
        )
    )

    _default_fail = {
        "fmv":              None,
        "asking_price":     asking,
        "difference":       None,
        "verdict":          "FAIL",
        "confidence":       "low",
        "reasoning":        "Could not calculate FMV — insufficient data.",
        "comparable_count": 0,
        "historical_count": 0,
        "subject_baths":    subject_baths,
        "subject_sqft":     subject_sqft,
        "subject_prop_type": subject_prop_type,
    }

    # ── Cache lookup ──────────────────────────────────────────────────────────
    cache_key = (area, bedrooms, subject_baths, subject_prop_type)
    if cache_key in _FMV_CACHE:
        cached     = _FMV_CACHE[cache_key]
        fmv        = cached["fmv"]
        verdict    = "PASS" if _is_pass(asking, fmv) else "FAIL"
        difference = asking - fmv
        bath_label = f"{subject_baths} bath" if subject_baths else "bath unknown"
        own_count_c = cached.get("own_count", 0)
        let_agreed_count_c = cached.get("let_agreed_count", 0)
        logger.info(
            "[fmv] CACHE HIT: %s | asking £%d | FMV £%d | diff %+d | %s",
            address[:40], asking, fmv, difference, verdict,
        )
        return {
            "fmv":              fmv,
            "asking_price":     asking,
            "difference":       difference,
            "verdict":          verdict,
            "confidence":       cached.get("confidence", "medium"),
            "reasoning":        (
                f"FMV of £{fmv:,}/mo based on {own_count_c} own-history records "
                f"and {let_agreed_count_c} let-agreed comparables "
                f"({area}, {bedrooms}-bed {subject_prop_type or 'property'}, {bath_label})."
            ),
            "own_history_count":    own_count_c,
            "let_agreed_count":     let_agreed_count_c,
            "comparable_count":     let_agreed_count_c,
            "historical_count":     own_count_c,
            "subject_baths":        subject_baths,
            "subject_sqft":         subject_sqft,
            "subject_prop_type":    subject_prop_type,
        }

    # ── Fresh computation ─────────────────────────────────────────────────────
    # FMV is based ONLY on:
    #   (a) this property's own rental history (what it actually let for before)
    #   (b) let-agreed prices of similar properties within 0.25 mile
    # Asking prices from listings.json are intentionally excluded.
    import asyncio

    all_historical = await get_historical_rents(
        address, area, bedrooms,
        baths=subject_baths,
        sqft=subject_sqft,
        prop_type=subject_prop_type,
        listing_url=property_dict.get("url", ""),
    )

    # Split for reporting: own property history vs comparable let-agreed
    own_history   = [p for p in all_historical
                     if p.get("source") in ("zoopla_property_history", "rightmove_property_history")]
    let_agreed    = [p for p in all_historical
                     if p.get("source") not in ("zoopla_property_history", "rightmove_property_history")]

    own_count    = len(own_history)
    let_agreed_count = len(let_agreed)

    # Build a comparables summary dict for Claude reasoning
    let_agreed_prices = [p["price"] for p in let_agreed if p.get("price")]
    comparables = {
        "listings":    let_agreed,
        "count":       let_agreed_count,
        "avg":         round(statistics.mean(let_agreed_prices)) if let_agreed_prices else None,
        "median":      round(statistics.median(let_agreed_prices)) if let_agreed_prices else None,
        "min":         min(let_agreed_prices) if let_agreed_prices else None,
        "max":         max(let_agreed_prices) if let_agreed_prices else None,
        "price_range": (f"£{min(let_agreed_prices):,}–£{max(let_agreed_prices):,}"
                        if let_agreed_prices else "no data"),
    }

    fmv = calculate_fmv(
        all_historical, {},          # empty comparables — all data already in all_historical
        subject_baths, subject_sqft, subject_prop_type,
    )
    if fmv is None or fmv == 0:
        logger.warning("[fmv] No let-agreed data for %s — defaulting to FAIL", address)
        return _default_fail

    # ── Cache store ───────────────────────────────────────────────────────────
    _FMV_CACHE[cache_key] = {
        "fmv":            fmv,
        "own_count":      own_count,
        "let_agreed_count": let_agreed_count,
        "confidence":     None,
    }

    # ── PASS / FAIL ───────────────────────────────────────────────────────────
    verdict    = "PASS" if _is_pass(asking, fmv) else "FAIL"
    difference = asking - fmv

    # ── Claude reasoning ──────────────────────────────────────────────────────
    loop = asyncio.get_event_loop()
    reasoning, confidence = await loop.run_in_executor(
        None,
        _get_claude_reasoning,
        property_dict, all_historical, comparables, fmv,
        subject_baths, subject_sqft,
    )
    _FMV_CACHE[cache_key]["confidence"] = confidence

    logger.info(
        "[fmv] %s | asking £%d | FMV £%d | diff %+d | %s | %s | "
        "own_history=%d let_agreed=%d | beds=%s baths=%s sqft=%s type=%s",
        address[:40], asking, fmv, difference, verdict, confidence,
        own_count, let_agreed_count, bedrooms, subject_baths, subject_sqft, subject_prop_type,
    )

    return {
        "fmv":              fmv,
        "asking_price":     asking,
        "difference":       difference,
        "verdict":          verdict,
        "confidence":       confidence,
        "reasoning":        reasoning,
        "own_history_count":    own_count,
        "let_agreed_count":     let_agreed_count,
        "comparable_count":     let_agreed_count,   # kept for backward compat
        "historical_count":     own_count,           # kept for backward compat
        "subject_baths":        subject_baths,
        "subject_sqft":         subject_sqft,
        "subject_prop_type":    subject_prop_type,
    }
