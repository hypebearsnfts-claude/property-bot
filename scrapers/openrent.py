import asyncio, logging, re
from playwright.async_api import async_playwright, TimeoutError as PWTimeout
try:
    from playwright_stealth import stealth_async as _stealth_async
    _STEALTH_AVAILABLE = True
except ImportError:
    _STEALTH_AVAILABLE = False

logger = logging.getLogger(__name__)

# OpenRent search URL — uses their /properties-to-rent search with a term + radius
# radius=1 = 1 mile; bedrooms_min=2; furnishedType=1 = furnished only
# We search by area name (their autocomplete accepts station/area names fine)
AREAS = {
    "Covent Garden":   ("covent-garden-london",   "Covent Garden, London"),
    "Soho":            ("soho-london",             "Soho, London"),
    "Knightsbridge":   ("knightsbridge-london",    "Knightsbridge, London"),
    "West Kensington": ("west-kensington-london",  "West Kensington, London"),
    "London Bridge":   ("london-bridge-london",    "London Bridge, London"),
    "Tower Hill":      ("tower-hill-london",       "Tower Hill, London"),
    "Baker Street":    ("baker-street-london",     "Baker Street, London"),
    "Bond Street":     ("bond-street-london",      "Bond Street, London"),
    "Marble Arch":     ("marble-arch-london",      "Marble Arch, London"),
    "Oxford Circus":   ("oxford-circus-london",    "Oxford Circus, London"),
    "Marylebone":      ("marylebone-london",       "Marylebone, London"),
    "Regent's Park":   ("regents-park-london",     "Regent's Park, London"),
}

def _search_url(slug: str, term: str) -> str:
    from urllib.parse import quote
    return (
        f"https://www.openrent.co.uk/properties-to-rent/{slug}"
        f"?term={quote(term)}&bedrooms_min=2&max_rent=15000"
        f"&furnishedType=1&isLive=true&radius=2"
    )


async def _scrape_area(browser, area, slug, term):
    ctx = await browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        viewport={"width": 1280, "height": 900},
        locale="en-GB",
        extra_http_headers={
            "Accept-Language": "en-GB,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
        },
    )
    page = await ctx.new_page()
    if _STEALTH_AVAILABLE:
        await _stealth_async(page)
    listings = []
    try:
        url = _search_url(slug, term)
        await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        # Cookie banner
        try:
            await page.locator(
                "button#onetrust-accept-btn-handler, button:has-text('Accept all'), button:has-text('I agree')"
            ).first.click(timeout=3_000)
            await page.wait_for_timeout(500)
        except Exception:
            pass

        # OpenRent uses a.pli for listing cards — wait for them
        # If they don't appear within 15s the area likely has no results
        try:
            await page.wait_for_selector("a.pli, div.property-result", timeout=15_000)
        except PWTimeout:
            logger.info("[openrent] %s -> no listings (selector timeout)", area)
            return listings

        # Scroll to load all lazy cards
        prev = 0
        for _ in range(10):
            cur = await page.eval_on_selector_all("a.pli, div.property-result", "els => els.length")
            if cur == prev and _ > 0:
                break
            prev = cur
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1000)

        cards = await page.query_selector_all("a.pli, div.property-result a")
        for card in cards:
            try:
                text = (await card.inner_text()).lower()
                if "let agreed" in text:
                    continue
                # Require at least 2 beds
                beds_ok = any(int(w) >= 2 for w in re.findall(r'\d+', text))
                if not beds_ok:
                    continue
                href = await card.get_attribute("href") or ""
                if not href:
                    continue
                if not href.startswith("http"):
                    href = "https://www.openrent.co.uk" + href
                # Price — multiple possible selectors
                price_el = await card.query_selector(
                    "div.pim span.fs-4, span.price, [class*='price'], [class*='Price']"
                )
                price_raw = ((await price_el.inner_text()).strip()) if price_el else ""
                price = (price_raw + " pcm") if price_raw and "pcm" not in price_raw.lower() else price_raw or "Price N/A"

                title_el = await card.query_selector("div.fw-medium, p.fw-medium, h2, [class*='title']")
                title = (await title_el.inner_text()).strip() if title_el else area

                card_text = await card.inner_text()
                bath_m = re.search(r"(\d+)\s*bath", card_text, re.IGNORECASE)
                sqft_m = re.search(r"([\d,]+)\s*(?:sq\.?\s*ft|sqft)", card_text, re.IGNORECASE)
                sqm_m  = re.search(r"([\d,]+)\s*(?:sq\.?\s*m\b|sqm|m²)", card_text, re.IGNORECASE)
                sqft = None
                if sqft_m:
                    sqft = int(sqft_m.group(1).replace(",", ""))
                elif sqm_m:
                    sqft = int(int(sqm_m.group(1).replace(",", "")) * 10.764)

                listings.append({
                    "source":  "openrent",
                    "area":    area,
                    "title":   title,
                    "price":   price,
                    "address": title,
                    "url":     href,
                    "baths":   int(bath_m.group(1)) if bath_m else None,
                    "sqft":    sqft,
                })
            except Exception:
                continue
    except Exception as exc:
        logger.error("[openrent] %s error: %s", area, exc)
    finally:
        await ctx.close()
    logger.info("[openrent] %s -> %d listings", area, len(listings))
    return listings


async def scrape():
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
            *[_scrape_area(browser, a, slug, term) for a, (slug, term) in AREAS.items()],
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
    logger.info("[openrent] Total after dedup: %d", len(unique))
    return unique
