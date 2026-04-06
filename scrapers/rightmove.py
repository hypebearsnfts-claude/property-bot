"""
scrapers/rightmove.py
---------------------
Scrapes ALL rental listings from Rightmove using Playwright + playwright-stealth.

Strategy
--------
Navigate directly to Rightmove search results using hardcoded locationIdentifiers.
playwright-stealth patches headless-Chromium fingerprint leaks so Cloudflare/bot-
detection does not block the request.

Pagination: iterates every page (index 0, 24, 48 …) until no cards are returned.
No page cap — fetches the full result set for each area.

locationIdentifiers verified live via los.rightmove.co.uk/typeahead:
  Covent Garden         REGION^87501   (district boundary)
  Soho                  REGION^87529   (district boundary)
  Knightsbridge         REGION^85242   (district boundary)
  Kensington Olympia    STATION^5054   (~1 mile radius)
  London Bridge         STATION^5792   (~1 mile radius)
  Tower Hill            STATION^9290   (~1 mile radius)  ← nearest station to Tower Bridge
  Baker Street          STATION^488    (~1 mile radius)
  Bond Street           STATION^1166   (~1 mile radius)
  Marble Arch           STATION^6032   (~1 mile radius)
  Oxford Circus         STATION^6953   (~1 mile radius)
  Marylebone            STATION^6095   (~1 mile radius)
  Regent's Park         STATION^7658   (~1 mile radius)

STATION searches use radius=1.0 mile (~1.6 km) — the closest Rightmove option to 1 km.
REGION searches use the district boundary polygon (no radius).
"""

import asyncio
import logging
import random
import re

from playwright.async_api import async_playwright, TimeoutError as PWTimeout

logger = logging.getLogger(__name__)

# Inline stealth script — patches the most common headless-Chromium fingerprint leaks
# without relying on playwright-stealth (which has a broken pkg_resources dependency).
_STEALTH_JS = """
() => {
    // Remove webdriver flag
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});

    // Provide a minimal chrome runtime object
    if (!window.chrome) {
        window.chrome = {runtime: {}};
    }

    // Give navigator.plugins a non-zero length
    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});

    // Realistic language list
    Object.defineProperty(navigator, 'languages', {get: () => ['en-GB', 'en']});

    // Patch permissions.query to avoid automation fingerprint
    const _origQuery = window.navigator.permissions.query.bind(navigator.permissions);
    window.navigator.permissions.query = (params) =>
        params.name === 'notifications'
            ? Promise.resolve({state: Notification.permission})
            : _origQuery(params);
}
"""

AREAS: dict[str, str] = {
    # District boundary searches
    "Covent Garden":      "REGION%5E87501",
    "Soho":               "REGION%5E87529",
    "Knightsbridge":      "REGION%5E85242",
    # Station radius searches (~1 mile / ~1.6 km)
    "Kensington Olympia": "STATION%5E5054",
    "London Bridge":      "STATION%5E5792",
    "Tower Hill":         "STATION%5E9290",   # nearest station to Tower Bridge
    "Baker Street":       "STATION%5E488",
    "Bond Street":        "STATION%5E1166",
    "Marble Arch":        "STATION%5E6032",
    "Oxford Circus":      "STATION%5E6953",
    "Marylebone":         "STATION%5E6095",
    "Regent's Park":      "STATION%5E7658",
}

BASE_SEARCH_URL = (
    "https://www.rightmove.co.uk/property-to-rent/find.html"
    "?locationIdentifier={loc_id}"
    "&minBedrooms=2"
    "&furnishTypes=furnished"
    "&includeLetAgreed=false"
    "{radius_param}"
)

# Add ~1 mile radius for STATION searches (closest option to 1 km on Rightmove)
_STATION_RADIUS = "&radius=0.5"

RESULTS_PER_PAGE = 24   # Rightmove paginates in steps of 24
MAX_SAFE_PAGES   = 200  # safety ceiling (~4,800 listings per area)
CARD_SEL = "div.propertyCard-details"


def _build_url(loc_id: str, index: int = 0) -> str:
    radius_param = _STATION_RADIUS if "STATION" in loc_id else ""
    url = BASE_SEARCH_URL.format(loc_id=loc_id, radius_param=radius_param)
    if index > 0:
        url += f"&index={index}"
    return url


async def _scrape_area(browser, area: str, loc_id: str) -> list[dict]:
    ctx = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 900},
        locale="en-GB",
        timezone_id="Europe/London",
        extra_http_headers={
            "Accept-Language": "en-GB,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1",
        },
    )
    page = await ctx.new_page()

    # Inject stealth patches before any page load
    await page.add_init_script(_STEALTH_JS)

    listings: list[dict] = []
    total_reported: int | None = None

    try:
        for page_num in range(MAX_SAFE_PAGES):
            index = page_num * RESULTS_PER_PAGE
            url = _build_url(loc_id, index)

            try:
                await page.goto(url, wait_until="networkidle", timeout=45_000)
            except PWTimeout:
                logger.warning("[rightmove] networkidle timeout on %s p%d, continuing", area, page_num + 1)

            await page.wait_for_timeout(2_000)

            # Accept cookies banner on first page only
            if page_num == 0:
                try:
                    btn = page.locator(
                        "button#onetrust-accept-btn-handler, "
                        "button:has-text('Accept all'), "
                        "button:has-text('Accept')"
                    ).first
                    await btn.click(timeout=3_000)
                    await page.wait_for_timeout(1_000)
                except Exception:
                    pass

                # Read total result count from the page for progress logging
                try:
                    count_el = await page.query_selector(
                        '[class*="searchHeader-resultCount"], '
                        'span[data-bind*="resultCount"], '
                        'h1[class*="result"]'
                    )
                    if count_el:
                        count_text = (await count_el.inner_text()).strip()
                        digits = re.sub(r"[^\d]", "", count_text)
                        if digits:
                            total_reported = int(digits)
                except Exception:
                    pass

                logger.info("[rightmove] %s — landed on: %s | title: %s | site reports: %s results",
                            area, page.url, await page.title(),
                            total_reported if total_reported is not None else "unknown")

            # Wait for property cards
            try:
                await page.wait_for_selector(CARD_SEL, timeout=15_000)
            except PWTimeout:
                logger.info("[rightmove] No cards on page %d for %s — done", page_num + 1, area)
                break

            cards = await page.query_selector_all(CARD_SEL)
            if not cards:
                break

            page_count = 0
            for card in cards:
                try:
                    card_text = (await card.inner_text()).lower()
                    if "let agreed" in card_text:
                        continue

                    link_el = await card.query_selector('a[href*="/properties/"]')
                    if not link_el:
                        continue
                    href = await link_el.get_attribute("href") or ""
                    if not href:
                        continue
                    href = re.sub(r"#.*$", "", href)
                    if not href.startswith("http"):
                        href = "https://www.rightmove.co.uk" + href

                    price_el = await card.query_selector('[class*="price"]')
                    price = (
                        (await price_el.inner_text()).strip().split("\n")[0]
                        if price_el else "Price N/A"
                    )

                    addr_el = await card.query_selector("address")
                    address = (
                        (await addr_el.inner_text()).strip() if addr_el else area
                    )

                    title_el = await card.query_selector('h2, [class*="propertyType"]')
                    title = (
                        (await title_el.inner_text()).strip() if title_el else "Property"
                    )

                    listings.append({
                        "source":  "rightmove",
                        "area":    area,
                        "title":   title,
                        "price":   price,
                        "address": address,
                        "url":     href,
                    })
                    page_count += 1
                except Exception:
                    continue

            logger.info("[rightmove] %s page %d (index=%d): %d cards  [running total: %d]",
                        area, page_num + 1, index, page_count, len(listings))

            if page_count == 0:
                break

            # If we've already collected as many as the site reports, stop early
            if total_reported is not None and len(listings) >= total_reported:
                logger.info("[rightmove] %s — reached reported total (%d), stopping", area, total_reported)
                break

            # Polite delay between pages (1.5 – 3 s) to avoid rate-limiting
            await asyncio.sleep(random.uniform(1.5, 3.0))

        logger.info("[rightmove] %s → %d listings total", area, len(listings))

    except Exception as exc:
        logger.error("[rightmove] Error scraping %s: %s", area, exc)
    finally:
        await ctx.close()

    return listings


async def scrape() -> list[dict]:
    """Run Rightmove scraper across all target areas, return deduplicated listings."""
    all_listings: list[dict] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ],
        )
        for area, loc_id in AREAS.items():
            results = await _scrape_area(browser, area, loc_id)
            all_listings.extend(results)
        await browser.close()

    # Deduplicate by URL
    seen: set[str] = set()
    unique: list[dict] = []
    for listing in all_listings:
        url = listing["url"]
        if url not in seen:
            seen.add(url)
            unique.append(listing)

    logger.info("[rightmove] Total after dedup: %d listings", len(unique))
    return unique


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(message)s",
        level=logging.INFO,
    )
    results = asyncio.run(scrape())
    print(f"\n--- Rightmove: {len(results)} listings found ---")
    for r in results[:5]:
        print(r)
