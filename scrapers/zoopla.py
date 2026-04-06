"""
scrapers/zoopla.py
------------------
Scrapes ALL rental listings from Zoopla using Playwright (Chromium, headless).

Zoopla renders listings with JavaScript so a plain HTTP request won't work.
We load each search page in a headless browser, wait for the property cards
to appear, then extract the data.

Pagination: iterates every page (pn=1, 2, 3 …) until no cards are returned.
No page cap — fetches the full result set for each area.

Search URL format (path-based — area slug in path, NOT ?q= parameter):
  https://www.zoopla.co.uk/to-rent/property/{area-slug}/
    ?beds_min=2
    &furnished_state=furnished
    &pn=<page>

  NOTE: ?q=Area,+London routes to generic London search (12,000+ results).
        The slug-in-path format correctly scopes results to the target area.

Areas mirror the Rightmove scraper (same 12 search zones):
  District boundaries : Covent Garden, Soho, Knightsbridge
  Station areas (~1 mi): Kensington Olympia, London Bridge, Tower Hill,
                         Baker Street, Bond Street, Marble Arch,
                         Oxford Circus, Marylebone, Regent's Park
"""

import asyncio
import logging
import random

from playwright.async_api import async_playwright, TimeoutError as PWTimeout

logger = logging.getLogger(__name__)

# Maps display names → Zoopla area slugs (verified live)
# Station-area names use the closest neighbourhood Zoopla recognises.
AREAS: dict[str, str] = {
    "Covent Garden":      "covent-garden",
    "Soho":               "soho",
    "Knightsbridge":      "london/knightsbridge",   # needs london/ prefix
    "Kensington Olympia": "west-kensington",
    "London Bridge":      "bermondsey",              # london-bridge slug is broken → bermondsey (SE1)
    "Tower Hill":         "aldgate",
    "Baker Street":       "marylebone",
    "Bond Street":        "mayfair",
    "Marble Arch":        "london/marble-arch",      # needs london/ prefix
    "Oxford Circus":      "fitzrovia",
    "Marylebone":         "marylebone",
    "Regent's Park":      "regents-park",
}

MAX_SAFE_PAGES = 100   # safety ceiling (~2,500 listings per area)

BASE_URL = "https://www.zoopla.co.uk/to-rent/property/{slug}/"


def _build_url(slug: str, page_num: int = 1) -> str:
    url = BASE_URL.format(slug=slug)
    url += f"?beds_min=2&furnished_state=furnished&pn={page_num}"
    return url


async def _scrape_area(browser, area: str, slug: str) -> list[dict]:
    ctx = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 900},
    )
    page = await ctx.new_page()
    listings: list[dict] = []

    try:
        for page_num in range(1, MAX_SAFE_PAGES + 1):
            url = _build_url(slug, page_num)
            await page.goto(url, wait_until="domcontentloaded", timeout=30_000)

            # Dismiss cookie banner on first page only
            if page_num == 1:
                try:
                    accept_btn = page.locator(
                        'button[id*="onetrust-accept"], '
                        'button:has-text("Accept all"), '
                        'button:has-text("Accept")'
                    ).first
                    await accept_btn.click(timeout=3_000)
                    await page.wait_for_timeout(500)
                except Exception:
                    pass

            # Wait for listing cards
            # Zoopla renders each listing as div[id^="listing_"] (verified April 2026)
            card_sel = 'div[id^="listing_"]'
            try:
                await page.wait_for_selector(card_sel, timeout=12_000)
            except PWTimeout:
                logger.info("[zoopla] No cards on page %d for %s — done", page_num, area)
                break

            await page.wait_for_timeout(1_000)  # let lazy images settle

            cards = await page.query_selector_all(card_sel)
            if not cards:
                break

            page_count = 0
            for card in cards:
                try:
                    # Skip let agreed properties
                    card_text = (await card.inner_text()).lower()
                    if "let agreed" in card_text:
                        continue

                    # Link
                    link_el = await card.query_selector(
                        'a[href*="/to-rent/details/"]'
                    )
                    href = await link_el.get_attribute("href") if link_el else ""
                    if href and not href.startswith("http"):
                        href = "https://www.zoopla.co.uk" + href

                    # Price  (class*="priceText" verified April 2026)
                    price_el = await card.query_selector(
                        'p[class*="priceText"], '
                        'p[class*="price_price"], '
                        '[class*="Price"]'
                    )
                    price = (await price_el.inner_text()).strip() if price_el else "Price N/A"

                    # Address
                    addr_el = await card.query_selector('address')
                    addr = (await addr_el.inner_text()).strip() if addr_el else area

                    # Title — use address as title (Zoopla has no separate title field)
                    title = addr if addr != area else "Property"

                    if not href:
                        continue

                    listings.append(
                        {
                            "source":  "zoopla",
                            "area":    area,
                            "title":   title,
                            "price":   price,
                            "address": addr,
                            "url":     href,
                        }
                    )
                    page_count += 1
                except Exception:
                    continue

            logger.info(
                "[zoopla] %s page %d: %d cards  [running total: %d]",
                area, page_num, page_count, len(listings)
            )
            if page_count == 0:
                break

            # Polite delay between pages to avoid rate-limiting
            await asyncio.sleep(random.uniform(1.5, 3.0))

        logger.info("[zoopla] %s → %d listings total", area, len(listings))

    except Exception as exc:
        logger.error("[zoopla] Error scraping %s: %s", area, exc)
    finally:
        await ctx.close()

    return listings


async def scrape() -> list[dict]:
    """Run Zoopla scraper across all target areas and return deduplicated listings."""
    all_listings: list[dict] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        for area, slug in AREAS.items():
            results = await _scrape_area(browser, area, slug)
            all_listings.extend(results)
        await browser.close()

    # Deduplicate by URL
    seen: set[str] = set()
    unique: list[dict] = []
    for listing in all_listings:
        if listing["url"] not in seen:
            seen.add(listing["url"])
            unique.append(listing)

    logger.info("[zoopla] Total after dedup: %d listings", len(unique))
    return unique


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(message)s",
        level=logging.INFO,
    )
    results = asyncio.run(scrape())
    print(f"\n--- Zoopla: {len(results)} listings found ---")
    for r in results[:5]:
        print(r)
