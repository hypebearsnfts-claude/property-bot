import asyncio, logging, random
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

logger = logging.getLogger(__name__)

AREAS = {
    "Covent Garden":      "covent-garden",
    "Soho":               "soho",
    "Knightsbridge":      "london/knightsbridge",
    "Kensington Olympia": "west-kensington",
    "London Bridge":      "bermondsey",
    "Tower Hill":         "aldgate",
    "Baker Street":       "marylebone",
    "Bond Street":        "mayfair",
    "Marble Arch":        "london/marble-arch",
    "Oxford Circus":      "fitzrovia",
    "Marylebone":         "marylebone",
    "Regent\'s Park":     "regents-park",
}

BASE = "https://www.zoopla.co.uk/to-rent/property/{slug}/"
LISTING_SEL = "a[data-testid*='listing']"

async def _scrape_area(browser, area, slug):
    ctx = await browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        viewport={"width": 1280, "height": 900},
    )
    page = await ctx.new_page()
    listings, accepted = [], False
    try:
        for pn in range(1, 101):
            url = f"{BASE.format(slug=slug)}?beds_min=2&furnished_state=furnished&pn={pn}"
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            except PWTimeout:
                logger.warning("[zoopla] %s p%d timeout", area, pn); break

            if not accepted:
                try:
                    await page.locator("button:has-text('Accept all'), button:has-text('Accept All')").first.click(timeout=3_000)
                    accepted = True
                    await page.wait_for_timeout(500)
                except Exception:
                    accepted = True

            # Wait for listings to render (client-side)
            try:
                await page.wait_for_selector(LISTING_SEL, timeout=12_000)
            except PWTimeout:
                logger.info("[zoopla] %s p%d: no listings", area, pn); break

            await asyncio.sleep(random.uniform(0.3, 0.6))

            # Extract all listing data via JS
            cards_data = await page.evaluate(r"""
                (sel) => {
                    const links = document.querySelectorAll(sel);
                    return Array.from(links).map(a => {
                        if (!a.href || a.href.includes('/new-homes/')) return null;
                        // Skip let agreed
                        const text = a.innerText.toLowerCase();
                        if (text.includes('let agreed')) return null;
                        const price = a.querySelector('[class*="Price"], [class*="price"], [data-testid*="price"]');
                        const addr = a.querySelector('[class*="address"], [class*="Address"], [data-testid*="address"]');
                        const title = a.querySelector('[class*="listing-results-attr"], [class*="PropertyType"], [data-testid*="title"], h2, [class*="title"]');
                        return {
                            url: a.href,
                            price: price ? price.innerText.trim() : 'Price N/A',
                            address: addr ? addr.innerText.trim().replace(/\s+/g,' ') : '',
                            title: title ? title.innerText.trim() : 'Property',
                        };
                    }).filter(Boolean);
                }
            """, LISTING_SEL)

            cnt = 0
            for d in cards_data:
                listings.append({"source":"zoopla","area":area,
                                  "title":d["title"],"price":d["price"],
                                  "address":d["address"],"url":d["url"]})
                cnt += 1

            logger.info("[zoopla] %s p%d: +%d (total %d)", area, pn, cnt, len(listings))
            if cnt == 0:
                break
    except Exception as exc:
        logger.error("[zoopla] %s error: %s", area, exc)
    finally:
        await ctx.close()
    logger.info("[zoopla] %s -> %d listings", area, len(listings))
    return listings

async def scrape():
    sem = asyncio.Semaphore(4)
    async def _s(browser, area, slug):
        async with sem: return await _scrape_area(browser, area, slug)
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        results = await asyncio.gather(
            *[_s(browser, a, s) for a, s in AREAS.items()],
            return_exceptions=True,
        )
        await browser.close()
    all_listings = []
    for r in results:
        if isinstance(r, list): all_listings.extend(r)
    seen, unique = set(), []
    for lst in all_listings:
        if lst.get("url") and lst["url"] not in seen:
            seen.add(lst["url"]); unique.append(lst)
    logger.info("[zoopla] Total after dedup: %d", len(unique))
    return unique
