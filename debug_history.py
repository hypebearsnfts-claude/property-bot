"""
debug_history.py
----------------
Dumps the full page text from a Zoopla and Rightmove listing
so we can see exactly what's on the page and how history is structured.

Usage:  python3 debug_history.py
"""
import asyncio
import re
from pathlib import Path
from playwright.async_api import async_playwright

_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

ZOOPLA_URL    = "https://www.zoopla.co.uk/to-rent/details/72897158/"
RIGHTMOVE_URL = "https://www.rightmove.co.uk/properties/174102632#/?channel=RES_LET"

OUT = Path(__file__).parent / "debug_history.txt"

async def dump(url: str, label: str, page):
    print(f"\n>>> Loading {label}: {url}")
    await page.goto(url, timeout=30_000, wait_until="domcontentloaded")
    await page.wait_for_timeout(2_000)

    # Accept cookies
    for sel in ["button#onetrust-accept-btn-handler",
                "button:has-text('Accept all')",
                "button:has-text('Accept All')"]:
        try:
            btn = page.locator(sel)
            if await btn.count() > 0:
                await btn.first.click()
                await page.wait_for_timeout(800)
                break
        except Exception:
            pass

    # Scroll slowly to trigger lazy loading
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.3)")
    await page.wait_for_timeout(800)
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.6)")
    await page.wait_for_timeout(800)
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    await page.wait_for_timeout(1_500)

    # Try clicking any history-related button
    for btn_text in ["Letting history", "Let history", "Price history",
                     "Rental history", "Previous rentals", "Show history",
                     "View letting history", "Listing history"]:
        try:
            btn = page.locator(f"button:has-text('{btn_text}')")
            if await btn.count() > 0:
                print(f"    → Clicking '{btn_text}' button")
                await btn.first.click()
                await page.wait_for_timeout(1_000)
                break
        except Exception:
            pass

    full_text = await page.evaluate("() => document.body.innerText")
    return full_text


async def main():
    lines_out = []

    async with async_playwright() as pw:
        # ── Zoopla ──────────────────────────────────────────────────────────
        browser = await pw.chromium.launch(headless=True)
        ctx  = await browser.new_context(user_agent=_UA, locale="en-GB",
                                         viewport={"width": 1280, "height": 900})
        page = await ctx.new_page()

        # Accept cookies on homepage first
        await page.goto("https://www.zoopla.co.uk/", timeout=20_000,
                        wait_until="domcontentloaded")
        await page.wait_for_timeout(1_000)
        try:
            accept = page.locator("button:has-text('Accept all'), #onetrust-accept-btn-handler")
            if await accept.count() > 0:
                await accept.first.click()
                await page.wait_for_timeout(600)
        except Exception:
            pass

        zoopla_text = await dump(ZOOPLA_URL, "Zoopla", page)
        await ctx.close()
        await browser.close()

        # ── Rightmove ───────────────────────────────────────────────────────
        browser = await pw.chromium.launch(headless=True)
        page2   = await browser.new_page()
        await page2.set_extra_http_headers({"User-Agent": _UA})
        rm_text = await dump(RIGHTMOVE_URL, "Rightmove", page2)
        await browser.close()

    # ── Extract and write ────────────────────────────────────────────────────
    year_re  = re.compile(r'20\d{2}')
    price_re = re.compile(r'£[\d,]+')

    with open(OUT, "w", encoding="utf-8") as f:
        for label, text in [("ZOOPLA", zoopla_text), ("RIGHTMOVE", rm_text)]:
            f.write(f"\n{'='*60}\n{label} — ALL LINES CONTAINING YEAR + £ PRICE\n{'='*60}\n")
            lines = [l.strip() for l in text.split('\n') if l.strip()]
            hits = []
            for i, line in enumerate(lines):
                if year_re.search(line) and price_re.search(line):
                    # Show 2 lines of context
                    ctx_lines = lines[max(0,i-1):i+3]
                    hits.append(f"  [{i}] " + " | ".join(ctx_lines))
            f.write('\n'.join(hits) if hits else "  (none found)\n")

            f.write(f"\n\n{'='*60}\n{label} — FULL PAGE TEXT (first 300 lines)\n{'='*60}\n")
            for i, line in enumerate(lines[:300]):
                f.write(f"  {i:3d}: {line}\n")

    print(f"\n✅ Written to {OUT}")


if __name__ == "__main__":
    asyncio.run(main())
