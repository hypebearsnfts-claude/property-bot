"""
scrapers/openrent.py  --  Playwright version
---------------------------------------------
OpenRent loads listings via JavaScript infinite scroll.
A plain HTTP request only returns ~20 cards; all others require
the browser to scroll down and trigger additional loads.

We use headless Chromium (same as rightmove.py / zoopla.py) to:
  1. Navigate to the search URL with area=1 (1km radius) and isLive=true.
  2. Scroll the page repeatedly until no new a.pli cards appear.
  3. Extract title, price, and URL from each card.
  4. Apply filter reinforcement (beds >=2, furnished, not let-agreed).

Search URL format:
  https://www.openrent.co.uk/properties-to-rent/{area}-london
    ?term={area}+London
    &bedrooms_min=2
    &furnishedType=1
    &area=1
    &isLive=true
"""

import asyncio
import logging
from urllib.parse import quote_plus

from playwright.async_api import async_playwright, TimeoutError as PWTimeout

logger = logging.getLogger(__name__)

AREAS = [
    "Covent Garden",
    "Soho",
    "Mayfair",
    "Marylebone",
    "Kensington",
    "West Kensington",
    "Knightsbridge",
    "London Bridge",
    "Tower Bridge",
]

BASE_URL = "https://www.openrent.co.uk/properties-to-rent/{slug}"

MAX_SCROLL_ATTEMPTS = 30
SCROLL_WAIT_MS = 2000


def _build_url(area: str) -> str:
    slug = area.lower().replace(" ", "-") + "-london"
    term = quote_plus(f"{area} London")
    return (
        BASE_URL.format(slug=slug)
        + f"?term={term}&bedrooms_min=2&furnishedType=1&area=1&isLive=true"
    )


def _card_passes_filters(card_text: str, li_texts: list) -> bool:
    if "let agreed" in card_text.lower():
        return False
    beds_ok = False
    for text in li_texts:
        if "bed" not in text:
            continue
        for word in text.split():
            try:
                if int(word) >= 2:
                    beds_ok = True
                    break
            except ValueError:
                pass
        if beds_ok:
            break
    furnished_ok = any(
        "furnished" in t and "unfurnished" not in t
        for t in li_texts
    )
    return beds_ok and furnished_ok


async def _scrape_area(browser, area: str) -> list:
    ctx = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 900},
    )
    page = await ctx.new_page()
    listings = []

    try:
        url = _build_url(area)
        await page.goto(url, wait_until="domcontentloaded", timeout=30_000)

        try:
            accept_btn = page.locator(
                "button[id*=onetrust-accept], "
                "button:has-text(Accept all), "
                "button:has-text(Accept), "
                "button:has-text(I agree)"
            ).first
            await accept_btn.click(timeout=3_000)
            await page.wait_for_timeout(500)
        except Exception:
            pass

        card_sel = "a.pli"
        try:
            await page.wait_for_selector(card_sel, timeout=15_000)
        except PWTimeout:
            logger.info("[openrent] No cards found for %s", area)
            return listings

        prev_count = 0
        for attempt in range(MAX_SCROLL_ATTEMPTS):
            current_count = await page.eval_on_selector_all(
                card_sel, "els => els.length"
            )
            if current_count == prev_count and attempt > 0:
                logger.info(
                    "[openrent] %s scroll stopped at %d cards (attempt %d)",
                    area, current_count, attempt,
                )
                break
            prev_count = current_count
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(SCROLL_WAIT_MS)

        logger.info("[openrent] %s: %d total cards after scrolling", area, prev_count)

        cards = await page.query_selector_all(card_sel)
        for card in cards:
            try:
                card_text = await card.inner_text()
                li_elements = await card.query_selector_all("li")
                li_texts = []
                for li in li_elements:
                    t = await li.inner_text()
                    li_texts.append(t.strip().lower())
                if not _card_passes_filters(card_text, li_texts):
                    continue
                href = await card.get_attribute("href") or ""
                if not href:
                    continue
                if not href.startswith("http"):
                    href = "https://www.openrent.co.uk" + href
                price_el = await card.query_selector("div.pim span.fs-4")
                price = (
                    (await price_el.inner_text()).strip() + " /month"
                    if price_el
                    else "Price N/A"
                )
                title_el = await card.query_selector("div.fw-medium")
                title = (await title_el.inner_text()).strip() if title_el else area
                listings.append({
                    "source": "openrent",
                    "area": area,
                    "title": title,
                    "price": price,
                    "address": title,
                    "url": href,
                })
            except Exception:
                continue

        logger.info("[openrent] %s -> %d listings after filtering", area, len(listings))

    except Exception as exc:
        logger.error("[openrent] Error scraping %s: %s", area, exc)
    finally:
        await ctx.close()

    return listings


async def scrape() -> list:
    all_listings = []
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        for area in AREAS:
            results = await _scrape_area(browser, area)
            all_listings.extend(results)
        await browser.close()
    seen = set()
    unique = []
    for listing in all_listings:
        if listing["url"] not in seen:
            seen.add(listing["url"])
            unique.append(listing)
    logger.info("[openrent] Total after dedup: %d listings", len(unique))
    return unique


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(message)s",
        level=logging.INFO,
    )
    results = asyncio.run(scrape())
    print(f"\n--- OpenRent: {len(results)} listings found ---")
    for r in results[:5]:
        print(r)
