"""
test_scrapers.py — Quick single-area test for the pagination fix.

Tests the three areas that were completely broken before:
  • Rightmove  Bond Street       (185 live listings)
  • Zoopla     Knightsbridge     (157 live listings)
  • OTM        West Kensington   (186 live listings)

All three should now return >50 listings with the fresh-context-per-page fix.

Usage:
    cd ~/path/to/property-bot
    python test_scrapers.py
"""

import asyncio
import logging
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


async def test_rightmove(browser):
    import scrapers.rightmove as rm
    rm._SEM = asyncio.Semaphore(4)
    t0 = time.time()
    result = await rm._scrape_area_inner(browser, "Bond Street", "STATION%5E1166")
    elapsed = time.time() - t0
    print(f"\n{'='*55}")
    print(f"  RIGHTMOVE  Bond Street:   {len(result):4d} listings  ({elapsed:.0f}s)")
    print(f"  Expected:  ~185 listings")
    print(f"  Status:    {'✅ PASS' if len(result) > 50 else '❌ FAIL — still bot-detected'}")
    print(f"{'='*55}\n")
    return len(result)


async def test_zoopla(browser):
    from scrapers.zoopla import _scrape_area
    t0 = time.time()
    result = await _scrape_area(browser, "Knightsbridge", "knightsbridge")
    elapsed = time.time() - t0
    print(f"\n{'='*55}")
    print(f"  ZOOPLA     Knightsbridge: {len(result):4d} listings  ({elapsed:.0f}s)")
    print(f"  Expected:  ~157 listings")
    print(f"  Status:    {'✅ PASS' if len(result) > 50 else '❌ FAIL — still bot-detected'}")
    print(f"{'='*55}\n")
    return len(result)


async def test_otm(browser):
    from scrapers.onthemarket import _scrape_area
    t0 = time.time()
    result = await _scrape_area(browser, "West Kensington", "west-kensington-station")
    elapsed = time.time() - t0
    print(f"\n{'='*55}")
    print(f"  OTM        West Kensington:{len(result):3d} listings  ({elapsed:.0f}s)")
    print(f"  Expected:  ~186 listings")
    print(f"  Status:    {'✅ PASS' if len(result) > 50 else '❌ FAIL — still bot-detected'}")
    print(f"{'='*55}\n")
    return len(result)


async def main():
    from playwright.async_api import async_playwright

    print("\n🔍 Starting scraper pagination test (3 areas concurrently)…\n")

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

        rm_count, zp_count, otm_count = await asyncio.gather(
            test_rightmove(browser),
            test_zoopla(browser),
            test_otm(browser),
        )

        await browser.close()

    print(f"\n{'='*55}")
    print(f"  FINAL SUMMARY")
    print(f"  Rightmove Bond Street:       {rm_count:4d}  (live ~185)")
    print(f"  Zoopla Knightsbridge:        {zp_count:4d}  (live ~157)")
    print(f"  OTM West Kensington:         {otm_count:4d}  (live ~186)")
    print(f"{'='*55}")

    all_pass = rm_count > 50 and zp_count > 50 and otm_count > 50
    if all_pass:
        print("\n✅  ALL SCRAPERS PASSING — pagination fix confirmed working!\n")
    else:
        fails = []
        if rm_count <= 50:  fails.append(f"Rightmove ({rm_count})")
        if zp_count <= 50:  fails.append(f"Zoopla ({zp_count})")
        if otm_count <= 50: fails.append(f"OTM ({otm_count})")
        print(f"\n❌  Still failing: {', '.join(fails)}\n")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
