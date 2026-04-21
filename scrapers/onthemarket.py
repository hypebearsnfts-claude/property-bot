"""
scrapers/onthemarket.py
-----------------------
Playwright scraper for OnTheMarket furnished rentals in central London.

Extracts per listing: price, address, bedrooms, bathrooms, sqft, url, area, agent.

HTML structure (confirmed 2026-04-16 via live inspection):
  Card:     article[data-component]
  URL:      a[href*="/details/"]  (NOT /property/)
  Address:  <address> element inside the card
  Features: parsed from card innerText via regex
  Agent:    leaf div containing agency name
  Cookie:   #ccc-recommended-settings  (Cookie Control, not OneTrust)

URL format: station slugs with radius — e.g. /baker-street-station/?radius=0.5
  Confirmed 2026-04-17: OTM supports the same station-slug + radius pattern as
  Rightmove, giving the same tight station circles around each tube station.
  Previously used postcode district slugs (nw1, se1) which covered huge areas.

Bot-detection fix 2026-04-20:
  OTM (like Rightmove and Zoopla) detects reused Playwright browser contexts.
  Each page now uses a fresh browser context to avoid the empty-results block.
  Area-level retry (once, after 8 s) covers the case where even page 1 is
  bot-detected (observed for West Kensington under concurrent scraping load).
"""

import asyncio
import logging
import random
import re
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

logger = logging.getLogger(__name__)

# Station slugs — confirmed live on OTM 2026-04-17
# Same 0.5-mile radius as Rightmove STATION searches.
# Soho: no "Soho station" exists; Piccadilly Circus is the central Soho tube stop.
AREAS = {
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

BASE = "https://www.onthemarket.com/to-rent/property/{slug}/"

_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")


def _url(slug: str, page: int = 1) -> str:
    return (
        f"{BASE.format(slug=slug)}"
        f"?min-bedrooms=2&max-price=15000&furnishing=furnished&include-let-agreed=false"
        f"&radius=0.5&page={page}"
    )


def _parse_card_text(text: str) -> tuple[int | None, int | None, int | None, str | None]:
    """
    Parse card innerText for beds, baths, sqft, prop_type.

    OTM card text example:
      "£3,900 pcm (£900 pw) Seven Dials Court, WC2H 2 bedroom flat to rent 2 2 Two Double..."
    The icon counters "2 2" (beds baths) appear right after "bedroom TYPE to rent".
    """
    beds = baths = sqft = prop_type = None

    # Beds from "X bedroom"
    m = re.search(r"(\d+)\s*bedroom", text, re.IGNORECASE)
    if m:
        beds = int(m.group(1))

    # Property type
    m2 = re.search(r"bedroom\s+(flat|apartment|house|studio|maisonette|penthouse|duplex|cottage|bungalow)",
                   text, re.IGNORECASE)
    if m2:
        t = m2.group(1).lower()
        if t in ("flat", "apartment", "penthouse", "duplex"):
            prop_type = "flat"
        elif t in ("house", "cottage", "bungalow"):
            prop_type = "house"
        elif t == "studio":
            prop_type = "studio"
        elif t == "maisonette":
            prop_type = "maisonette"

    # Baths — OTM shows icon counters "BEDS BATHS" right after "to rent"
    # e.g. "2 bedroom flat to rent 2 1 538 sq ft" → beds icon=2, bath icon=1
    m3 = re.search(r"to\s+rent\s+(\d+)\s+(\d+)", text, re.IGNORECASE)
    if m3 and beds and int(m3.group(1)) == beds:
        baths = int(m3.group(2))

    # Sqft
    m4 = re.search(r"([\d,]+)\s*sq\s*ft", text, re.IGNORECASE)
    if m4:
        sqft = int(m4.group(1).replace(",", ""))
    else:
        m4b = re.search(r"([\d,]+)\s*(?:sq\.?\s*m(?!\w)|sqm)", text, re.IGNORECASE)
        if m4b:
            sqft = int(int(m4b.group(1).replace(",", "")) * 10.764)

    return beds, baths, sqft, prop_type


# JavaScript that extracts all property cards from the current OTM page.
# Uses article[data-component] as the card selector (confirmed live).
_EXTRACT_JS = r"""
() => {
    const cards = document.querySelectorAll('article[data-component]');
    const results = [];

    for (const card of cards) {
        const cardText = (card.innerText || '').trim();
        if (!cardText) continue;
        if (/let\s+agreed/i.test(cardText)) continue;

        // URL — find the /details/ link; take first one with a valid href
        const detailLink = Array.from(card.querySelectorAll('a[href*="/details/"]'))
            .find(a => a.getAttribute('href') && a.getAttribute('href').length > 5);
        if (!detailLink) continue;
        const url = detailLink.href;

        // Price — a[href*="/details/"] whose innerText contains "pcm" or "pw"
        const priceLink = Array.from(card.querySelectorAll('a[href*="/details/"]'))
            .find(a => /pcm|pw|per\s+month|per\s+week/i.test(a.innerText || ''));
        const price = priceLink
            ? priceLink.innerText.trim()
            : (cardText.match(/£[\d,]+\s*pcm[^\n]*/i) || [''])[0];

        // Address — <address> element
        const addrEl = card.querySelector('address');
        const address = addrEl
            ? addrEl.innerText.trim().replace(/\s+/g, ' ')
            : '';

        // Agent — leaf div/span with agency-name-like text (has "&" or "-", <60 chars, no £)
        const agentEl = Array.from(card.querySelectorAll('div, span, p'))
            .find(el => {
                if (el.children.length > 0) return false;
                const t = (el.innerText || '').trim();
                return t.length > 3 && t.length < 70
                    && !t.includes('£')
                    && !t.includes('pcm')
                    && !t.includes('Email')
                    && !t.includes('Added')
                    && !t.match(/^\d/)
                    && (t.includes(' - ') || t.includes(' & ') || /[A-Z][a-z]+ [A-Z]/.test(t));
            });
        const agent = agentEl ? agentEl.innerText.trim() : '';

        results.push({ url, price, address, cardText: cardText.substring(0, 400), agent });
    }
    return results;
}
"""


async def _load_page(browser, area: str, slug: str, pn: int) -> list[dict]:
    """
    Load one OTM results page in a fresh browser context.

    A fresh context is used for every page: OTM bot detection flags reused
    Playwright contexts, causing article[data-component] to be absent on page 2+.

    Returns list of raw card dicts (empty on bot-detection or end of results).
    """
    ctx = await browser.new_context(
        user_agent=_UA,
        viewport={"width": 1280, "height": 900},
        locale="en-GB",
    )
    page = await ctx.new_page()
    try:
        try:
            await page.goto(_url(slug, pn), wait_until="domcontentloaded", timeout=35_000)
        except PWTimeout:
            logger.warning("[otm] %s p%d: page load timeout", area, pn)
            return []

        # Give React time to hydrate and render listings
        await page.wait_for_timeout(3_000)

        # Dismiss Cookie Control banner (best-effort)
        try:
            await page.evaluate(
                "const b = document.getElementById('ccc-recommended-settings'); if(b) b.click();"
            )
            await page.wait_for_timeout(1_500)
        except Exception:
            pass

        # Wait for at least one card
        try:
            await page.wait_for_selector("article[data-component]", timeout=10_000)
        except PWTimeout:
            logger.info("[otm] %s p%d: no article[data-component] (bot-detected or end)", area, pn)
            return []

        cards_data = await page.evaluate(_EXTRACT_JS)
        return cards_data or []

    finally:
        await ctx.close()


async def _scrape_area(browser, area: str, slug: str) -> list[dict]:
    """
    Scrape all pages for one area, creating a fresh browser context per page.
    Retries up to 3 times with exponential backoff if page 1 returns 0 listings.
    """
    _RETRY_DELAYS = [8, 20, 35]   # seconds before attempt 2, 3, 4

    for attempt in range(4):
        listings: list[dict] = []
        seen: set[str] = set()

        for pn in range(1, 20):
            if pn > 1:
                await asyncio.sleep(random.uniform(2.5, 4.5))

            try:
                cards_data = await _load_page(browser, area, slug, pn)
            except Exception as exc:
                logger.error("[otm] %s p%d error: %s", area, pn, exc)
                break

            if not cards_data:
                break

            new_on_page = 0
            for d in cards_data:
                url_str = d.get("url", "")
                if not url_str or url_str in seen:
                    continue
                seen.add(url_str)

                card_text = d.get("cardText", "")
                beds, baths, sqft, prop_type = _parse_card_text(card_text)

                listings.append({
                    "source":    "onthemarket",
                    "area":      area,
                    "title":     d.get("address", area),
                    "price":     d.get("price", "Price N/A"),
                    "address":   d.get("address", ""),
                    "url":       url_str,
                    "beds":      beds,
                    "baths":     baths,
                    "sqft":      sqft,
                    "prop_type": prop_type,
                    "agent":     d.get("agent", ""),
                })
                new_on_page += 1

            logger.info("[otm] %s p%d: +%d (total %d)", area, pn, new_on_page, len(listings))

            if new_on_page == 0:
                break

        if listings or attempt == 3:
            break

        delay = _RETRY_DELAYS[attempt]
        logger.info("[otm] %s attempt %d got 0 — retrying in %ds…", area, attempt + 1, delay)
        await asyncio.sleep(delay)

    logger.info("[otm] %s → %d listings", area, len(listings))
    return listings


async def scrape() -> list[dict]:
    """Scrape all configured areas on OnTheMarket. Returns deduplicated listings."""
    sem = asyncio.Semaphore(2)

    async def _guarded(browser, area, slug):
        async with sem:
            return await _scrape_area(browser, area, slug)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox",
                  "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"],
        )
        results = await asyncio.gather(
            *[_guarded(browser, a, s) for a, s in AREAS.items()],
            return_exceptions=True,
        )
        await browser.close()

    all_listings: list[dict] = []
    for r in results:
        if isinstance(r, list):
            all_listings.extend(r)

    seen: set[str] = set()
    unique: list[dict] = []
    for lst in all_listings:
        if lst.get("url") and lst["url"] not in seen:
            seen.add(lst["url"])
            unique.append(lst)

    logger.info("[otm] Total after dedup: %d", len(unique))
    return unique
