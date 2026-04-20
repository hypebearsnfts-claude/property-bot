import asyncio, logging, random
from playwright.async_api import async_playwright, TimeoutError as PWTimeout
try:
    from playwright_stealth import stealth_async as _stealth_async
    _STEALTH_AVAILABLE = True
except ImportError:
    _STEALTH_AVAILABLE = False

logger = logging.getLogger(__name__)

# Station slugs for Zoopla's /station/tube/ URL format — confirmed live 2026-04-17.
# All use radius=0.5 miles, matching Rightmove STATION and OTM station searches.
# Soho has no tube station; Piccadilly Circus is the central Soho stop.
# Tower Hill is the correct tube station (previously used broad "ec3" or "tower-bridge").
AREAS = {
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

LISTING_SEL = "a[data-testid*='listing']"

_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

_EXTRA_HEADERS = {
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}


def _url(slug, pn=1):
    return (f"https://www.zoopla.co.uk/to-rent/property/station/tube/{slug}/"
            f"?beds_min=2&price_max=15000&furnished_state=furnished&radius=0.5&results_sort=newest_listings&pn={pn}")


async def _load_page(browser, area, slug, pn):
    """
    Load one Zoopla results page in a brand-new browser context.

    A fresh context is essential: Zoopla bot detection flags the Playwright
    session after the first page load, so page 2+ return no listing cards
    when the same context is reused.

    Returns list[dict] of raw card data (empty on bot-detection/timeout).
    """
    ctx = await browser.new_context(
        user_agent=_UA,
        viewport={"width": 1280, "height": 900},
        locale="en-GB",
        extra_http_headers=_EXTRA_HEADERS,
    )
    page = await ctx.new_page()
    if _STEALTH_AVAILABLE:
        await _stealth_async(page)
    try:
        url = _url(slug, pn)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=35_000)
        except PWTimeout:
            logger.warning("[zoopla] %s p%d: goto timeout", area, pn)
            return []

        # Dismiss cookie banner (best-effort)
        try:
            await page.locator(
                "button:has-text('Accept all'), button:has-text('Accept All')"
            ).first.click(timeout=4_000)
            await page.wait_for_timeout(800)
        except Exception:
            pass

        # Scroll to trigger lazy-loaded cards
        await page.evaluate("window.scrollTo(0, 400)")
        await asyncio.sleep(random.uniform(0.4, 0.8))
        await page.evaluate("window.scrollTo(0, 800)")

        # Wait for listing cards
        try:
            await page.wait_for_selector(LISTING_SEL, timeout=18_000)
        except PWTimeout:
            logger.info("[zoopla] %s p%d: selector timeout (bot-detected or end)", area, pn)
            return []

        await asyncio.sleep(random.uniform(0.5, 1.0))

        # Extract card data via JS
        cards_data = await page.evaluate(r"""
            (sel) => {
                const links = document.querySelectorAll(sel);
                return Array.from(links).map(a => {
                    if (!a.href || a.href.includes('/new-homes/')) return null;
                    const text = a.innerText.toLowerCase();
                    if (text.includes('let agreed')) return null;
                    const price = a.querySelector('[data-testid*="price"], [class*="Price"], [class*="price"]');
                    const addr  = a.querySelector('[data-testid*="address"], [class*="address"], [class*="Address"]');
                    const title = a.querySelector('[data-testid*="title"], h2, [class*="title"], [class*="Title"]');

                    const featText = a.innerText || '';
                    const bathMatch = featText.match(/(\d+)\s*bath/i);
                    const sqftMatch = featText.match(/([\d,]+)\s*sq\.?\s*ft/i)
                                   || featText.match(/([\d,]+)\s*sqft/i);
                    const sqmMatch  = featText.match(/([\d,]+)\s*(?:sq\.?\s*m(?!\w)|sqm)/i);
                    let sqft = null;
                    if (sqftMatch) sqft = parseInt(sqftMatch[1].replace(/,/g,''));
                    else if (sqmMatch) sqft = Math.round(parseInt(sqmMatch[1].replace(/,/g,'')) * 10.764);

                    const agentEl = a.querySelector('[data-testid="listing-agent-name"], [class*="AgentName"], [class*="agent-name"], [class*="BranchName"]');
                    const agent = agentEl ? agentEl.innerText.trim() : '';

                    return {
                        url:     a.href,
                        price:   price ? price.innerText.trim() : 'Price N/A',
                        address: addr  ? addr.innerText.trim().replace(/\s+/g,' ') : '',
                        title:   title ? title.innerText.trim() : 'Property',
                        baths:   bathMatch ? parseInt(bathMatch[1]) : null,
                        sqft:    sqft,
                        agent:   agent,
                    };
                }).filter(Boolean);
            }
        """, LISTING_SEL)

        return cards_data

    finally:
        await ctx.close()


async def _scrape_area(browser, area, slug):
    """
    Scrape all pages for one area, creating a fresh browser context per page.
    Retries once (after a delay) if page 1 returns 0 listings.
    """
    for attempt in range(2):
        listings       = []
        seen_this_area = set()

        for pn in range(1, 51):
            if pn > 1:
                await asyncio.sleep(random.uniform(3.5, 6.0))

            try:
                cards_data = await _load_page(browser, area, slug, pn)
            except Exception as exc:
                logger.error("[zoopla] %s p%d error: %s", area, pn, exc)
                break

            if not cards_data:
                # Empty page — bot-detected or end of results
                break

            page_urls = {d['url'] for d in cards_data if d.get('url')}
            if pn > 1 and page_urls and page_urls.issubset(seen_this_area):
                logger.info("[zoopla] %s p%d: all dupes — end of results", area, pn)
                break

            cnt = 0
            for d in cards_data:
                u = d.get('url', '')
                if u and u not in seen_this_area:
                    seen_this_area.add(u)
                    listings.append({
                        "source":  "zoopla",
                        "area":    area,
                        "title":   d["title"],
                        "price":   d["price"],
                        "address": d["address"],
                        "url":     u,
                        "baths":   d.get("baths"),
                        "sqft":    d.get("sqft"),
                        "agent":   d.get("agent", ""),
                    })
                    cnt += 1

            logger.info("[zoopla] %s p%d: +%d (total %d)", area, pn, cnt, len(listings))

            if cnt == 0:
                break

        if listings or attempt == 1:
            break

        logger.info("[zoopla] %s attempt 1 got 0 — retrying in 10s…", area)
        await asyncio.sleep(10.0)

    logger.info("[zoopla] %s -> %d listings", area, len(listings))
    return listings


async def scrape():
    sem = asyncio.Semaphore(2)

    async def _s(browser, area, term):
        async with sem:
            return await _scrape_area(browser, area, term)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        results = await asyncio.gather(
            *[_s(browser, a, t) for a, t in AREAS.items()],
            return_exceptions=True,
        )
        await browser.close()

    all_listings = []
    for r in results:
        if isinstance(r, list):
            all_listings.extend(r)

    seen, unique = set(), []
    for lst in all_listings:
        if lst.get("url") and lst["url"] not in seen:
            seen.add(lst["url"])
            unique.append(lst)

    logger.info("[zoopla] Total after dedup: %d", len(unique))
    return unique
