import asyncio, logging, random, re
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

logger = logging.getLogger(__name__)

# Limit concurrent areas. Each area now creates one fresh context per page
# (serially), so we never have more than _SEM contexts open simultaneously.
_SEM: asyncio.Semaphore | None = None

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
    "Regent's Park":      "STATION%5E7658",
}

BASE     = "https://www.rightmove.co.uk/property-to-rent/find.html"
CARD_SEL = "div[class*='PropertyCard_propertyCardContainerWrapper']"
_UA      = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")


def _url(loc_id, index=0):
    r = "&radius=0.5" if "STATION" in loc_id else ""
    return (f"{BASE}?locationIdentifier={loc_id}{r}"
            f"&minBedrooms=2&maxPrice=15000&furnishTypes=furnished"
            f"&includeLetAgreed=false&sortType=6&index={index}&channel=RENT")


async def _load_page(browser, area, loc_id, index):
    """
    Load one Rightmove results page in a brand-new browser context.

    A fresh context is required for every page request: Rightmove's bot
    detection flags Playwright after the first navigation in a context, so
    page 2+ return an empty page when the same context/page object is reused.

    Returns (cards_data: list[dict], total: int).
    cards_data is empty list on bot-detection / selector timeout.
    total is 0 on pages other than page 0.
    """
    ctx = await browser.new_context(
        user_agent=_UA,
        viewport={"width": 1280, "height": 900},
    )
    page = await ctx.new_page()
    try:
        try:
            await page.goto(_url(loc_id, index), wait_until="domcontentloaded", timeout=30_000)
        except PWTimeout:
            logger.warning("[rightmove] %s idx=%d: goto timeout", area, index)
            return [], 0

        # Dismiss OneTrust cookie banner (best-effort)
        try:
            await page.locator("button#onetrust-accept-btn-handler").click(timeout=3_000)
        except Exception:
            pass

        await asyncio.sleep(random.uniform(0.5, 1.0))

        # ── Result count (first page only) ──────────────────────────────────
        total = 0
        if index == 0:
            # Try the dedicated element first
            try:
                txt = await page.locator('[data-test="results-count"]').inner_text(timeout=5_000)
                total = int(re.sub(r"[^\d]", "", txt) or "0")
            except Exception:
                pass
            # Regex fallback: look for "185 results" / "1,240 results" anywhere
            if not total:
                try:
                    body = await page.inner_text("body")
                    m = re.search(r'(\d[\d,]*)\s+result', body, re.IGNORECASE)
                    if m:
                        total = int(m.group(1).replace(",", ""))
                except Exception:
                    pass

        # ── Wait for property cards ──────────────────────────────────────────
        try:
            await page.wait_for_selector(CARD_SEL, timeout=10_000)
        except PWTimeout:
            logger.info("[rightmove] %s idx=%d: no cards (bot-detected or end)", area, index)
            return [], total

        # ── Extract card data via JS ─────────────────────────────────────────
        cards_data = await page.evaluate(r"""
            (sel) => {
                const cards = document.querySelectorAll(sel);
                return Array.from(cards).map(card => {
                    const badge = card.querySelector('[class*="LetAgreed"], [class*="let-agreed"], [class*="letAgreed"]');
                    if (badge) return null;
                    const link = card.querySelector('a[href*="/properties/"]');
                    if (!link) return null;
                    const price = card.querySelector('[class*="Price_price"], [class*="propertyCard-priceValue"], [class*="price__"]');
                    const addr  = card.querySelector('address, [class*="Address_address"], [class*="propertyCard-address"]');
                    const title = card.querySelector('[class*="propertyCard-title"], h2, [class*="Title"]');
                    const featText = card.innerText || '';
                    const bathMatch = featText.match(/(\d+)\s*bath/i);
                    const sqftMatch = featText.match(/([\d,]+)\s*sq\.?\s*ft/i)
                                   || featText.match(/([\d,]+)\s*sqft/i);
                    const sqmMatch  = featText.match(/([\d,]+)\s*(?:sq\.?\s*m(?!\w)|sqm)/i);
                    let sqft = null;
                    if (sqftMatch) sqft = parseInt(sqftMatch[1].replace(/,/g,''));
                    else if (sqmMatch) sqft = Math.round(parseInt(sqmMatch[1].replace(/,/g,'')) * 10.764);
                    const agentEl = card.querySelector('[class*="ContactBlock_contactTitle"], [class*="agentLogo"], .propertyCard-contactsItem span, [data-test="agent-title"]');
                    const agent = agentEl ? agentEl.innerText.trim() : '';
                    return {
                        url:         link.href || '',
                        price:       price ? price.innerText.trim() : 'Price N/A',
                        address:     addr  ? addr.innerText.trim().replace(/\s+/g,' ') : '',
                        title:       title ? title.innerText.trim() : 'Property',
                        baths:       bathMatch ? parseInt(bathMatch[1]) : null,
                        sqft:        sqft,
                        agent:       agent,
                        description: featText.slice(0, 600),
                    };
                }).filter(Boolean);
            }
        """, CARD_SEL)

        return cards_data, total

    finally:
        await ctx.close()


async def _scrape_area(browser, area, loc_id):
    async with _SEM:
        return await _scrape_area_inner(browser, area, loc_id)


async def _scrape_area_inner(browser, area, loc_id):
    # Stagger area starts to spread load across RM's servers
    await asyncio.sleep(random.uniform(0.5, 3.0))

    _RETRY_DELAYS  = [6, 15, 30]   # seconds to wait before attempt 2, 3, 4
    _RM_PAGE_SIZE  = 24            # Rightmove always returns exactly 24 per page

    for attempt in range(4):   # up to 4 attempts — handles brief network drops + bot detection
        listings      = []
        seen_urls     = set()
        index         = 0
        total         = 0
        pages_fetched = 0          # pages that returned at least 1 new listing

        for page_num in range(50):   # max ~1 200 listings (50 × 24)
            if page_num > 0:
                # Delay between consecutive page fetches for the same area
                await asyncio.sleep(random.uniform(3.0, 5.5))

            try:
                cards_data, page_total = await _load_page(browser, area, loc_id, index)
            except Exception as exc:
                logger.error("[rightmove] %s idx=%d unexpected error: %s", area, index, exc)
                break

            if page_total:
                total = page_total

            page_count = 0
            for d in cards_data:
                u = d.get("url", "")
                if u and u not in seen_urls:
                    seen_urls.add(u)
                    listings.append({
                        "source":      "rightmove",
                        "area":        area,
                        "title":       d["title"],
                        "price":       d["price"],
                        "address":     d["address"],
                        "url":         u,
                        "baths":       d.get("baths"),
                        "sqft":        d.get("sqft"),
                        "agent":       d.get("agent", ""),
                        "description": d.get("description", ""),
                    })
                    page_count += 1

            if page_count > 0:
                pages_fetched += 1
            logger.info("[rightmove] %s idx=%d +%d (total %d/%d)",
                        area, index, page_count, len(listings), total)

            if page_count == 0:
                break

            index += 24
            if total and index >= total:
                break

        # Retry if: got 0, OR stopped after 1 full page when total says there's more
        # (page 2 bot-detected — same symptom as Tower Hill / Covent Garden getting exactly 24)
        stopped_after_one_full_page = (
            pages_fetched == 1
            and len(listings) >= _RM_PAGE_SIZE
            and (total == 0 or total > _RM_PAGE_SIZE)
        )
        if (listings and not stopped_after_one_full_page) or attempt == 3:
            break

        delay = _RETRY_DELAYS[attempt]
        reason = "0 listings" if not listings else f"only 1 page ({len(listings)}/{total}) — possible early bot-stop"
        logger.info("[rightmove] %s attempt %d got %s — retrying in %ds…", area, attempt + 1, reason, delay)
        await asyncio.sleep(delay)

    logger.info("[rightmove] %s -> %d listings", area, len(listings))
    return listings


async def scrape():
    global _SEM
    _SEM = asyncio.Semaphore(4)   # max 4 areas processed concurrently
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox",
                  "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"],
        )
        results = await asyncio.gather(
            *[_scrape_area(browser, a, l) for a, l in AREAS.items()],
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

    logger.info("[rightmove] Total after dedup: %d", len(unique))
    return unique
