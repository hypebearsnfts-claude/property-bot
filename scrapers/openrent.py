import asyncio, logging, re
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

logger = logging.getLogger(__name__)

AREAS = {
    "Covent Garden":      ("covent-garden-london",      "Covent Garden London"),
    "Soho":               ("soho-london",               "Soho London"),
    "Knightsbridge":      ("knightsbridge-london",       "Knightsbridge London"),
    "West Kensington":    ("west-kensington-london",     "West Kensington London"),
    "London Bridge":      ("london-bridge-london",       "London Bridge London"),
    "Tower Hill":         ("tower-hill-london",          "Tower Hill London"),
    "Baker Street":       ("baker-street-london",        "Baker Street London"),
    "Bond Street":        ("bond-street-london",         "Bond Street London"),
    "Marble Arch":        ("marble-arch-london",         "Marble Arch London"),
    "Oxford Circus":      ("oxford-circus-london",       "Oxford Circus London"),
    "Marylebone":         ("marylebone-london",          "Marylebone London"),
    "Regent\'s Park":     ("regents-park-london",        "Regents Park London"),
}

BASE = "https://www.openrent.co.uk/properties-to-rent/{slug}"

async def _scrape_area(browser, area, slug, term):
    ctx = await browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        viewport={"width": 1280, "height": 900},
    )
    page = await ctx.new_page()
    listings = []
    try:
        url = f"{BASE.format(slug=slug)}?term={term.replace(' ','+')}+&bedrooms_min=2&furnishedType=1&area=1&isLive=true"
        await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        try:
            await page.locator("button#onetrust-accept-btn-handler, button:has-text('Accept all'), button:has-text('I agree')").first.click(timeout=3_000)
            await page.wait_for_timeout(500)
        except Exception:
            pass
        try:
            await page.wait_for_selector("a.pli", timeout=15_000)
        except PWTimeout:
            return listings
        prev = 0
        for _ in range(10):
            cur = await page.eval_on_selector_all("a.pli", "els => els.length")
            if cur == prev and _ > 0: break
            prev = cur
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1000)
        cards = await page.query_selector_all("a.pli")
        for card in cards:
            try:
                text = (await card.inner_text()).lower()
                if "let agreed" in text: continue
                beds_ok = any(int(w) >= 2 for w in text.split() if w.isdigit())
                if not beds_ok: continue
                if "furnished" not in text or "unfurnished" in text: continue
                href = await card.get_attribute("href") or ""
                if not href: continue
                if not href.startswith("http"): href = "https://www.openrent.co.uk" + href
                price_el = await card.query_selector("div.pim span.fs-4")
                price = ((await price_el.inner_text()).strip() + " /month") if price_el else "Price N/A"
                title_el = await card.query_selector("div.fw-medium, p.fw-medium")
                title = (await title_el.inner_text()).strip() if title_el else area

                # Extract baths + sqft from full card text
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
        browser = await pw.chromium.launch(headless=True)
        results = await asyncio.gather(
            *[_scrape_area(browser, a, s, t) for a, (s, t) in AREAS.items()],
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
