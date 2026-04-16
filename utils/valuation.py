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
        asking_price <= fmv + 500  →  PASS
        asking_price >  fmv + 500  →  FAIL

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

_ZOOPLA_SLUGS: dict[str, str] = {
    "Covent Garden":  "covent-garden",
    "Soho":           "soho",
    "Knightsbridge":  "sw1x",
    "West Kensington":"west-kensington",
    "London Bridge":  "se1",
    "Tower Hill":     "ec3",
    "Baker Street":   "marylebone",
    "Bond Street":    "mayfair",
    "Marble Arch":    "w1h",
    "Oxford Circus":  "fitzrovia",
    "Marylebone":     "marylebone",
    "Regent's Park":  "regents-park",
}

_RIGHTMOVE_REGIONS: dict[str, str] = {
    "Covent Garden":  "REGION%5E87501",
    "Soho":           "REGION%5E87529",
    "Knightsbridge":  "REGION%5E85242",
    "West Kensington":"REGION%5E85288",
    "London Bridge":  "REGION%5E87516",
    "Tower Hill":     "REGION%5E87535",
    "Baker Street":   "REGION%5E87498",
    "Bond Street":    "REGION%5E87499",
    "Marble Arch":    "REGION%5E87520",
    "Oxford Circus":  "REGION%5E87525",
    "Marylebone":     "REGION%5E85272",
    "Regent's Park":  "REGION%5E87527",
}

_OTM_SLUGS: dict[str, str] = {
    "Covent Garden":   "wc2h",   # updated: specific to Covent Garden/Seven Dials
    "Soho":            "w1d",
    "Knightsbridge":   "sw1x",
    "West Kensington": "w14",
    "London Bridge":   "se1",
    "Tower Hill":      "ec3n",   # updated: specific to Tower Hill/Aldgate
    "Baker Street":    "nw1",
    "Bond Street":     "w1k",
    "Marble Arch":     "w1h",
    "Oxford Circus":   "w1b",
    "Marylebone":      "w1u",
    "Regent's Park":   "nw8",
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

async def _scrape_property_history(address: str, bedrooms: int) -> list[dict]:
    """
    Look up this exact property's own rental history on Zoopla.

    Strategy:
      1. Search Zoopla for the address string — pick the best-matching result.
      2. Visit its detail page and extract the "Price history" / "Rental history" table.
      3. Return each historical entry as a data point with its real date and price.

    This gives the best possible comparable: the same flat, same size, same location,
    across up to 10 years of actual let-agreed prices.

    Returns [] if no history found (no crash — caller falls back to area comparables).
    """
    results: list[dict] = []
    if not _PLAYWRIGHT or not address:
        return results

    # Normalise address for search — strip flat/floor prefixes that confuse search
    search_term = re.sub(
        r"(?i)^(?:flat\s*\d+[a-z]?,?\s*|apartment\s*\d+[a-z]?,?\s*|floor\s*\d+,?\s*)",
        "", address.strip()
    ).strip()
    if not search_term:
        return results

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            page    = await browser.new_page()
            await page.set_extra_http_headers({"User-Agent": _UA})

            # Step 1: Search Zoopla for the address
            search_url = (
                f"https://www.zoopla.co.uk/to-rent/property/london/"
                f"?q={search_term.replace(' ', '+')}"
                f"&beds_min={bedrooms}&beds_max={bedrooms}"
                f"&furnished_state=furnished&include_let_agreed=true"
                f"&results_sort=newest_listings"
            )
            await page.goto(search_url, timeout=30_000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2_000)

            # Find first result whose address closely matches
            cards = await page.query_selector_all("[data-testid='search-result']")
            detail_url = None
            for card in cards[:5]:
                try:
                    addr_el = await card.query_selector("[data-testid='listing-description']")
                    if not addr_el:
                        continue
                    card_addr = (await addr_el.inner_text()).lower()
                    # Check if key words from the address appear in the card
                    words = [w for w in re.findall(r'[a-z]{3,}', search_term.lower()) if w not in {"the","and","for","london","flat","road","street"}]
                    matches = sum(1 for w in words if w in card_addr)
                    if matches >= max(2, len(words) // 2):
                        link_el = await card.query_selector("a[href*='/to-rent/details/']")
                        if link_el:
                            detail_url = await link_el.get_attribute("href")
                            if detail_url and not detail_url.startswith("http"):
                                detail_url = "https://www.zoopla.co.uk" + detail_url
                            break
                except Exception:
                    continue

            if not detail_url:
                await browser.close()
                logger.info("[fmv] Property history: no Zoopla match for '%s'", search_term[:60])
                return results

            # Step 2: Visit the detail page and extract price/rental history
            await page.goto(detail_url, timeout=30_000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2_000)

            # Try to find and click "Show price history" / rental history section
            try:
                history_btn = page.locator(
                    "button:has-text('price history'), button:has-text('Price history'), "
                    "button:has-text('rental history'), button:has-text('Rental history')"
                )
                if await history_btn.count() > 0:
                    await history_btn.first.click()
                    await page.wait_for_timeout(1_000)
            except Exception:
                pass

            # Extract history table rows
            history_rows = await page.evaluate(r"""
                () => {
                    const results = [];
                    // Look for price history tables / lists
                    const rows = document.querySelectorAll(
                        '[data-testid*="price-history"] tr, '
                        '[class*="PriceHistory"] tr, '
                        '[class*="price-history"] tr, '
                        'table tr'
                    );
                    for (const row of rows) {
                        const cells = Array.from(row.querySelectorAll('td, th'));
                        const text  = cells.map(c => c.innerText.trim());
                        // Look for rows that have a date and a price
                        const hasDate  = text.some(t => /20\d{2}/.test(t));
                        const hasPrice = text.some(t => /£[\d,]+/.test(t));
                        if (hasDate && hasPrice) {
                            results.push(text.join(' | '));
                        }
                    }
                    // Also check list-style history items
                    const items = document.querySelectorAll(
                        '[data-testid*="price-history"] li, [class*="PriceHistory"] li'
                    );
                    for (const item of items) {
                        const t = item.innerText.trim();
                        if (/20\d{2}/.test(t) && /£[\d,]+/.test(t)) {
                            results.push(t);
                        }
                    }
                    return results;
                }
            """)

            await browser.close()

            now = datetime.now()
            for row_text in history_rows:
                price_m = re.search(r'£([\d,]+)', row_text)
                year_m  = re.search(r'(20\d{2})', row_text)
                month_m = re.search(
                    r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+(20\d{2})',
                    row_text, re.IGNORECASE
                )
                if not (price_m and year_m):
                    continue
                price = int(price_m.group(1).replace(",", ""))
                if not (500 <= price <= 50_000):
                    continue
                # Weekly → monthly
                if "pw" in row_text.lower() or "per week" in row_text.lower():
                    price = int(price * 52 / 12)

                if month_m:
                    try:
                        dt = datetime.strptime(month_m.group(0), "%b %Y")
                    except ValueError:
                        dt = datetime(int(year_m.group(1)), 1, 1)
                else:
                    dt = datetime(int(year_m.group(1)), 1, 1)

                age_months = max(0, (now.year - dt.year) * 12 + (now.month - dt.month))
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

    except Exception as exc:
        logger.warning("[fmv] Property history scrape failed for '%s': %s", search_term[:60], exc)

    logger.info("[fmv] Property history for '%s': %d data points", address[:50], len(results))
    return results


# ── 1. Historical rent scraping ────────────────────────────────────────────────

async def _scrape_zoopla_let_agreed(
    slug: str,
    bedrooms: int,
    baths: Optional[int] = None,
    prop_type: Optional[str] = None,
    subject_sqft: Optional[int] = None,
) -> list[dict]:
    """
    Scrape Zoopla let-agreed listings with strict filters.

    Passes bedrooms + prop_type into the URL.
    Hard-filters results by baths and size (±25%) after extraction.
    """
    results: list[dict] = []
    if not _PLAYWRIGHT:
        return results

    # Build property_sub_type param
    sub_type_param = ""
    if prop_type == "flat":
        sub_type_param = "&property_sub_type=flats"
    elif prop_type == "house":
        sub_type_param = "&property_sub_type=houses"

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page    = await browser.new_page()
        await page.set_extra_http_headers({"User-Agent": _UA})

        for pn in range(1, 4):   # up to 3 pages
            url = (
                f"https://www.zoopla.co.uk/to-rent/property/{slug}/"
                f"?beds_min={bedrooms}&beds_max={bedrooms}"
                f"&furnished_state=furnished&include_let_agreed=true"
                f"&results_sort=newest_listings{sub_type_param}&pn={pn}"
            )
            try:
                await page.goto(url, timeout=30_000, wait_until="domcontentloaded")
                await page.wait_for_timeout(2_000)

                cards = await page.query_selector_all("[data-testid='search-result']")
                if not cards:
                    break

                for card in cards:
                    try:
                        price_el = await card.query_selector("[data-testid='listing-price']")
                        addr_el  = await card.query_selector("[data-testid='listing-description']")
                        feat_el  = await card.query_selector(
                            "[data-testid='listing-features'], "
                            "[class*='features'], [class*='Features']"
                        )
                        if not (price_el and addr_el):
                            continue

                        price_text = await price_el.inner_text()
                        addr_text  = await addr_el.inner_text()
                        feat_text  = (await feat_el.inner_text()) if feat_el else ""
                        full_text  = price_text + " " + feat_text

                        price = _parse_price_pcm(price_text)
                        if price and 500 <= price <= 50_000:
                            comp_baths = _parse_baths(full_text)
                            comp_sqft  = _parse_sqft(full_text)
                            # Hard filter: baths must match exactly (when both known)
                            if baths is not None and comp_baths is not None:
                                if comp_baths != baths:
                                    continue
                            # Hard filter: size within ±25% (when both known)
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
                                "address":    addr_text.strip(),
                                "source":     "zoopla_let_agreed",
                                "age_months": 0,
                            })
                    except Exception:
                        continue

            except Exception as exc:
                logger.warning("[fmv] Zoopla page %d failed (%s): %s", pn, slug, exc)
                break

        await browser.close()

    logger.info("[fmv] Zoopla let-agreed (%s, %d bed): %d data points", slug, bedrooms, len(results))
    return results


async def _scrape_rightmove_let_agreed(
    region_id: str,
    bedrooms: int,
    baths: Optional[int] = None,
    prop_type: Optional[str] = None,
    subject_sqft: Optional[int] = None,
) -> list[dict]:
    """
    Scrape Rightmove let-agreed listings with strict filters.

    Passes bedrooms + baths + prop_type into the URL directly.
    Hard-filters results by size (±25%) after extraction.
    """
    results: list[dict] = []
    if not _PLAYWRIGHT:
        return results

    # Build URL — Rightmove supports minBathrooms/maxBathrooms and propertyTypes
    bath_param = f"&minBathrooms={baths}&maxBathrooms={baths}" if baths else ""
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
            f"?locationIdentifier={region_id}"
            f"&minBedrooms={bedrooms}&maxBedrooms={bedrooms}"
            f"&furnishTypes=furnished&includeLetAgreed=true"
            f"&sortType=6{bath_param}{type_param}"
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
            logger.warning("[fmv] Rightmove let-agreed failed (%s): %s", region_id, exc)

        await browser.close()

    logger.info("[fmv] Rightmove let-agreed (%s, %d bed): %d data points",
                region_id, bedrooms, len(results))
    return results


async def _scrape_otm_let_agreed(
    slug: str,
    bedrooms: int,
    baths: Optional[int] = None,
    prop_type: Optional[str] = None,
    subject_sqft: Optional[int] = None,
) -> list[dict]:
    """Scrape OnTheMarket let-agreed listings with strict filters."""
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
            f"&furnishing=furnished&include-let-agreed=true"
            # slug is now a postcode district (e.g. "wc2") — old area-name slugs removed
        )
        try:
            await page.goto(url, timeout=30_000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2_000)

            # Dismiss cookie banner
            try:
                await page.locator(
                    "button:has-text('Accept all'), button:has-text('Accept All'), "
                    "#onetrust-accept-btn-handler"
                ).first.click(timeout=4_000)
                await page.wait_for_timeout(500)
            except Exception:
                pass

            cards_data = await page.evaluate(r"""
                () => {
                    const selectors = [
                        'li.otm-PropertyCardListItem',
                        'article[class*="PropertyCard"]',
                        'li[class*="property-result"]',
                    ];
                    let cards = [];
                    for (const sel of selectors) {
                        const found = document.querySelectorAll(sel);
                        if (found.length > 0) { cards = Array.from(found); break; }
                    }
                    return cards.map(card => {
                        const priceEl = card.querySelector('[class*="price"], .price');
                        const addrEl  = card.querySelector('[class*="address"], address');
                        if (!priceEl) return null;
                        const cardText  = card.innerText || '';
                        const bathMatch = cardText.match(/(\d+)\s*bath/i);
                        const sqftMatch = cardText.match(/([\d,]+)\s*sq\.?\s*ft/i)
                                       || cardText.match(/([\d,]+)\s*sqft/i);
                        const sqmMatch  = cardText.match(/([\d,]+)\s*(?:sq\.?\s*m\b|sqm|m²)/i);
                        let sqft = null;
                        if (sqftMatch) sqft = parseInt(sqftMatch[1].replace(/,/g,''));
                        else if (sqmMatch) sqft = Math.round(parseInt(sqmMatch[1].replace(/,/g,'')) * 10.764);
                        return {
                            price:   priceEl.innerText.trim(),
                            address: addrEl ? addrEl.innerText.trim().replace(/\s+/g,' ') : '',
                            baths:   bathMatch ? parseInt(bathMatch[1]) : null,
                            sqft:    sqft,
                        };
                    }).filter(Boolean);
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
        own_history = await _scrape_property_history(address, bedrooms)
        results.extend(own_history)
        if own_history:
            logger.info("[fmv] Property history for '%s': %d points (own rental history)",
                        address[:50], len(own_history))
    except Exception as exc:
        logger.warning("[fmv] Property history failed: %s", exc)

    # VOA borough medians intentionally excluded — too broad, dilutes micro-location FMV.
    # Sources: property's own history + live let-agreed comps only.

    # 1, 2, 3 — Live let-agreed scraping (concurrent), with strict params
    slug      = _ZOOPLA_SLUGS.get(area)
    region_id = _RIGHTMOVE_REGIONS.get(area)
    otm_slug  = _OTM_SLUGS.get(area)

    import asyncio
    tasks = []
    if slug:
        tasks.append(asyncio.create_task(
            _scrape_zoopla_let_agreed(slug, bedrooms, baths=baths, prop_type=prop_type, subject_sqft=sqft)
        ))
    if region_id:
        tasks.append(asyncio.create_task(
            _scrape_rightmove_let_agreed(region_id, bedrooms, baths=baths, prop_type=prop_type, subject_sqft=sqft)
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
) -> dict:
    """
    Find comparable furnished listings in the same area.
    Prefers exact bedroom + bathroom + size match. Falls back gracefully.
    """
    comps: list[dict] = []

    # Pull from local listings.json first (fast)
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
                        "price":   price,
                        "address": listing.get("address", ""),
                        "source":  listing.get("source", "local"),
                        "baths":   listing.get("baths"),
                        "sqft":    listing.get("sqft"),
                    })
    except Exception as exc:
        logger.warning("[fmv] comparable listings local read failed: %s", exc)

    # Supplement with live Zoopla if thin
    slug = _ZOOPLA_SLUGS.get(area)
    if slug and _PLAYWRIGHT and len(comps) < 5:
        try:
            live = await _scrape_zoopla_let_agreed(slug, bedrooms)
            for item in live:
                comps.append({
                    "price":   item["price"],
                    "address": item["address"],
                    "source":  "zoopla_live",
                    "baths":   item.get("baths"),
                    "sqft":    item.get("sqft"),
                })
        except Exception as exc:
            logger.warning("[fmv] live comparable scrape failed: %s", exc)

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
- Current comparable listings (same area, same bedrooms, weighted by bathroom + size match)
- 10 years of VOA historical rental data for the borough
- Live let-agreed data from Zoopla, Rightmove, and OnTheMarket

Your job:
1. Assess whether the calculated Fair Market Value is reasonable given the comparables
2. Write a concise 1-2 sentence reasoning explaining the verdict
3. Be specific — mention actual price ranges, number of comparables, and bathroom/size context where relevant

Always be direct. Focus on the numbers. Do not hedge excessively.
Respond with ONLY valid JSON:
{
  "reasoning": "<1-2 sentences>",
  "confidence": "high|medium|low"
}

Confidence guide:
  high   = 10+ well-matched comparables
  medium = 5-9 comparables or partial bath/size data
  low    = fewer than 5 comparables, or scraping largely failed
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

    # Summarise historical data sources
    voa_points = [p for p in historical_data if "VOA" in p.get("source", "")]
    live_points = [p for p in historical_data if "VOA" not in p.get("source", "")]
    hist_summary = ""
    if voa_points:
        hist_prices = [p["price"] for p in voa_points]
        hist_summary += (
            f"VOA 10-year borough data: {len(voa_points)} annual data points, "
            f"range £{min(hist_prices):,}–£{max(hist_prices):,}/mo. "
        )
    if live_points:
        live_prices = [p["price"] for p in live_points]
        hist_summary += (
            f"Live let-agreed (Zoopla/Rightmove/OTM): {len(live_points)} data points, "
            f"range £{min(live_prices):,}–£{max(live_prices):,}/mo."
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
            model=_SONNET_MODEL, max_tokens=256,
            system=_FMV_SYSTEM,
            messages=[{"role": "user", "content": user_msg}],
        )
        data = json.loads(message.content[0].text.strip())
        return str(data.get("reasoning", "")), str(data.get("confidence", "medium"))

    except Exception as exc:
        logger.error("[fmv] Claude reasoning failed: %s", exc)
        comp_count = comparables.get("count", 0)
        confidence = "high" if comp_count >= 10 else "medium" if comp_count >= 5 else "low"
        return f"Based on {comp_count} comparables. FMV: £{fmv:,}/month.", confidence


# ── 5. PASS / FAIL rule (unit-tested) ─────────────────────────────────────────

def _is_pass(asking_price: int, fmv: int) -> bool:
    """
    PASS if: asking_price <= fmv + 500
    FAIL if: asking_price >  fmv + 500
    There is NO lower limit — any price at or below FMV is always a PASS.
    """
    return asking_price <= fmv + 500


def _run_unit_tests() -> None:
    """Assert the PASS/FAIL rule against canonical examples."""
    assert _is_pass(2_800, 3_000) is True,  "£2,800 ask vs £3,000 FMV should PASS"
    assert _is_pass(3_000, 3_000) is True,  "£3,000 ask vs £3,000 FMV should PASS"
    assert _is_pass(3_400, 3_000) is True,  "£3,400 ask vs £3,000 FMV should PASS (£400 above)"
    assert _is_pass(3_500, 3_000) is True,  "£3,500 ask vs £3,000 FMV should PASS (exactly £500 above)"
    assert _is_pass(3_600, 3_000) is False, "£3,600 ask vs £3,000 FMV should FAIL (£600 above)"
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
        logger.info(
            "[fmv] CACHE HIT: %s | asking £%d | FMV £%d | diff %+d | %s",
            address[:40], asking, fmv, difference, verdict,
        )
        return {
            "fmv":              fmv,
            "asking_price":     asking,
            "difference":       difference,
            "verdict":          verdict,
            "confidence":       cached["confidence"],          # real confidence, not "cached"
            "reasoning":        (
                f"FMV of £{fmv:,}/mo based on {cached['comparable_count']} comparables "
                f"and {cached['historical_count']} historical data points "
                f"({area}, {bedrooms}-bed {subject_prop_type or 'property'}, {bath_label})."
            ),
            "comparable_count": cached["comparable_count"],   # real count from original run
            "historical_count": cached["historical_count"],   # real count from original run
            "subject_baths":    subject_baths,
            "subject_sqft":     subject_sqft,
            "subject_prop_type": subject_prop_type,
        }

    # ── Fresh computation ─────────────────────────────────────────────────────
    import asyncio

    historical_task  = asyncio.create_task(
        get_historical_rents(address, area, bedrooms,
                             baths=subject_baths,
                             sqft=subject_sqft,
                             prop_type=subject_prop_type)
    )
    comparables_task = asyncio.create_task(
        get_comparable_listings(area, bedrooms, asking)
    )
    historical_data, comparables = await asyncio.gather(historical_task, comparables_task)

    fmv = calculate_fmv(
        historical_data, comparables,
        subject_baths, subject_sqft, subject_prop_type,
    )
    if fmv is None or fmv == 0:
        logger.warning("[fmv] No data for %s — defaulting to FAIL", address)
        return _default_fail

    # ── Cache store ───────────────────────────────────────────────────────────
    # Store fmv + the real counts so cache hits can report them accurately
    _FMV_CACHE[cache_key] = {
        "fmv":              fmv,
        "comparable_count": comparables.get("count", 0),
        "historical_count": len(historical_data),
        "confidence":       None,   # filled in after Claude reasoning below
    }

    # ── PASS / FAIL ───────────────────────────────────────────────────────────
    verdict    = "PASS" if _is_pass(asking, fmv) else "FAIL"
    difference = asking - fmv

    # ── Claude reasoning ──────────────────────────────────────────────────────
    loop = asyncio.get_event_loop()
    reasoning, confidence = await loop.run_in_executor(
        None,
        _get_claude_reasoning,
        property_dict, historical_data, comparables, fmv,
        subject_baths, subject_sqft,
    )
    # Now that we have the real confidence, update the cache entry
    _FMV_CACHE[cache_key]["confidence"] = confidence

    logger.info(
        "[fmv] %s | asking £%d | FMV £%d | diff %+d | %s | %s | beds=%s baths=%s sqft=%s type=%s",
        address[:40], asking, fmv, difference, verdict, confidence,
        bedrooms, subject_baths, subject_sqft, subject_prop_type,
    )

    return {
        "fmv":              fmv,
        "asking_price":     asking,
        "difference":       difference,
        "verdict":          verdict,
        "confidence":       confidence,
        "reasoning":        reasoning,
        "comparable_count": comparables.get("count", 0),
        "historical_count": len(historical_data),
        "subject_baths":    subject_baths,
        "subject_sqft":     subject_sqft,
        "subject_prop_type": subject_prop_type,
    }
