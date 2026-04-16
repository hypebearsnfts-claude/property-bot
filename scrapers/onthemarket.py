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

URL format: postcode district slugs — e.g. /wc2/, /sw1x/
  Old area-name slugs (covent-garden-london) are no longer recognised by OTM.
"""

import asyncio
import logging
import random
import re
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

logger = logging.getLogger(__name__)

# Postcode district slugs — confirmed working as of 2026-04
AREAS = {
    "Covent Garden":   "wc2h",   # WC2H = Covent Garden/Seven Dials (not all of WC2)
    "Soho":            "w1d",    # W1D = Soho core (Dean St, Wardour St)
    "Knightsbridge":   "sw1x",   # SW1X = Knightsbridge/Sloane St
    "West Kensington": "w14",    # W14 = West Kensington only
    "London Bridge":   "se1",    # SE1 = London Bridge area
    "Tower Hill":      "ec3n",   # EC3N = Tower Hill/Aldgate (not all of EC3)
    "Baker Street":    "nw1",    # NW1 = filtered to sectors 5-6 post-scrape
    "Bond Street":     "w1k",    # W1K = Bond Street/Mayfair
    "Marble Arch":     "w1h",    # W1H = Marble Arch/Bryanston Sq
    "Oxford Circus":   "w1b",    # W1B = Oxford Circus/Regent St
    "Marylebone":      "w1u",    # W1U = Marylebone High St
    "Regent's Park":   "nw8",    # NW8 = St John's Wood/Regent's Park (filtered post-scrape)
}

BASE = "https://www.onthemarket.com/to-rent/property/{postcode}/"

_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")


def _url(postcode: str, page: int = 1) -> str:
    return (
        f"{BASE.format(postcode=postcode)}"
        f"?min-bedrooms=2&furnishing=furnished&include-let-agreed=false"
        f"&page={page}"
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


async def _scrape_area(browser, area: str, postcode: str) -> list[dict]:
    ctx = await browser.new_context(
        user_agent=_UA,
        viewport={"width": 1280, "height": 900},
        locale="en-GB",
    )
    page = await ctx.new_page()
    listings: list[dict] = []
    seen: set[str] = set()
    cookie_dismissed = False

    try:
        for pn in range(1, 20):
            if pn > 1:
                await asyncio.sleep(random.uniform(2.5, 4.5))

            try:
                await page.goto(_url(postcode, pn), wait_until="domcontentloaded", timeout=35_000)
            except PWTimeout:
                logger.warning("[otm] %s p%d: page load timeout", area, pn)
                break

            # Give React time to hydrate and render listings
            await page.wait_for_timeout(3_000)

            # Dismiss Cookie Control banner (id=ccc) once — must do BEFORE listings render
            if not cookie_dismissed:
                try:
                    await page.evaluate(
                        "const b = document.getElementById('ccc-recommended-settings'); if(b) b.click();"
                    )
                    cookie_dismissed = True
                    await page.wait_for_timeout(1_500)
                except Exception:
                    pass

            # Wait for at least one card
            try:
                await page.wait_for_selector("article[data-component]", timeout=10_000)
            except PWTimeout:
                logger.info("[otm] %s p%d: no article[data-component] found — stopping", area, pn)
                break

            cards_data = await page.evaluate(_EXTRACT_JS)

            if not cards_data:
                logger.info("[otm] %s p%d: extractor returned 0 — stopping", area, pn)
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

    except Exception as exc:
        logger.error("[otm] %s error: %s", area, exc)
    finally:
        await ctx.close()

    logger.info("[otm] %s → %d listings", area, len(listings))
    return listings


async def scrape() -> list[dict]:
    """Scrape all configured areas on OnTheMarket. Returns deduplicated listings."""
    sem = asyncio.Semaphore(2)

    async def _guarded(browser, area, postcode):
        async with sem:
            return await _scrape_area(browser, area, postcode)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        results = await asyncio.gather(
            *[_guarded(browser, a, p) for a, p in AREAS.items()],
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
