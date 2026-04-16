import asyncio, logging, random, re
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

logger = logging.getLogger(__name__)

AREAS = {
    "Covent Garden":      "REGION%5E87501",
    "Soho":               "REGION%5E87529",
    "Knightsbridge":      "REGION%5E85242",
    "West Kensington":    "STATION%5E5054",
    "London Bridge":      "STATION%5E5792",
    "Tower Hill":         "STATION%5E9290",
    "Baker Street":       "STATION%5E488",
    "Bond Street":        "STATION%5E1166",
    "Marble Arch":        "STATION%5E6032",
    "Oxford Circus":      "STATION%5E6953",
    "Marylebone":         "STATION%5E6095",
    "Regent\'s Park":     "STATION%5E7658",
}

BASE = "https://www.rightmove.co.uk/property-to-rent/find.html"
CARD_SEL = "div[class*='PropertyCard_propertyCardContainerWrapper']"

def _url(loc_id, index=0):
    r = "&radius=0.25" if "STATION" in loc_id else ""
    return (f"{BASE}?locationIdentifier={loc_id}{r}"
            f"&minBedrooms=2&furnishTypes=furnished"
            f"&includeLetAgreed=false&sortType=6&index={index}&channel=RENT")

async def _scrape_area(browser, area, loc_id):
    ctx = await browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        viewport={"width": 1280, "height": 900},
    )
    page = await ctx.new_page()
    listings, index, total, accepted = [], 0, 0, False
    try:
        while True:
            try:
                await page.goto(_url(loc_id, index), wait_until="domcontentloaded", timeout=30_000)
            except PWTimeout:
                logger.warning("[rightmove] %s timeout idx=%d", area, index); break

            if not accepted:
                try:
                    await page.locator("button#onetrust-accept-btn-handler").click(timeout=3_000)
                    accepted = True
                except Exception:
                    accepted = True

            await asyncio.sleep(random.uniform(0.5, 1.0))

            # Get total result count once
            if index == 0:
                try:
                    txt = await page.locator('[data-test="results-count"]').inner_text(timeout=5_000)
                    total = int(re.sub(r"[^\d]", "", txt) or "0")
                except Exception:
                    total = 0

            # Extract all card data via JS in one call
            try:
                await page.wait_for_selector(CARD_SEL, timeout=10_000)
            except PWTimeout:
                logger.info("[rightmove] %s idx=%d: no cards", area, index); break

            cards_data = await page.evaluate(r"""
                (sel) => {
                    const cards = document.querySelectorAll(sel);
                    return Array.from(cards).map(card => {
                        // Skip let agreed
                        const badge = card.querySelector('[class*="LetAgreed"], [class*="let-agreed"], [class*="letAgreed"]');
                        if (badge) return null;
                        const link = card.querySelector('a[href*="/properties/"]');
                        if (!link) return null;
                        const price = card.querySelector('[class*="Price_price"], [class*="propertyCard-priceValue"], [class*="price__"]');
                        const addr = card.querySelector('address, [class*="Address_address"], [class*="propertyCard-address"]');
                        const title = card.querySelector('[class*="propertyCard-title"], h2, [class*="Title"]');

                        // Beds/baths/sqft from features text
                        const featText = card.innerText || '';
                        const bathMatch = featText.match(/(\d+)\s*bath/i);
                        const sqftMatch = featText.match(/([\d,]+)\s*sq\.?\s*ft/i)
                                       || featText.match(/([\d,]+)\s*sqft/i);
                        const sqmMatch  = featText.match(/([\d,]+)\s*(?:sq\.?\s*m(?!\w)|sqm)/i);
                        let sqft = null;
                        if (sqftMatch) sqft = parseInt(sqftMatch[1].replace(/,/g,''));
                        else if (sqmMatch) sqft = Math.round(parseInt(sqmMatch[1].replace(/,/g,'')) * 10.764);

                        // Agent name
                        const agentEl = card.querySelector('[class*="ContactBlock_contactTitle"], [class*="agentLogo"], .propertyCard-contactsItem span, [data-test="agent-title"]');
                        const agent = agentEl ? agentEl.innerText.trim() : '';

                        return {
                            url:     link.href || '',
                            price:   price ? price.innerText.trim() : 'Price N/A',
                            address: addr  ? addr.innerText.trim().replace(/\s+/g,' ') : '',
                            title:   title ? title.innerText.trim() : 'Property',
                            baths:   bathMatch ? parseInt(bathMatch[1]) : null,
                            sqft:    sqft,
                            agent:   agent,
                        };
                    }).filter(Boolean);
                }
            """, CARD_SEL)

            page_count = 0
            for d in cards_data:
                listings.append({
                    "source":  "rightmove",
                    "area":    area,
                    "title":   d["title"],
                    "price":   d["price"],
                    "address": d["address"],
                    "url":     d["url"],
                    "baths":   d.get("baths"),
                    "sqft":    d.get("sqft"),
                    "agent":   d.get("agent", ""),
                })
                page_count += 1

            logger.info("[rightmove] %s idx=%d +%d (total %d/%d)", area, index, page_count, len(listings), total)
            index += 24
            if (total and index >= total) or page_count == 0:
                break
    except Exception as exc:
        logger.error("[rightmove] %s error: %s", area, exc)
    finally:
        await ctx.close()
    logger.info("[rightmove] %s -> %d listings", area, len(listings))
    return listings

async def scrape():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        results = await asyncio.gather(
            *[_scrape_area(browser, a, l) for a, l in AREAS.items()],
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
    logger.info("[rightmove] Total after dedup: %d", len(unique))
    return unique
