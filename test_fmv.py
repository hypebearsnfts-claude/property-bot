"""
test_fmv.py
-----------
Quick FMV test — runs get_fmv_verdict on one listing from each source
(Zoopla, Rightmove, OnTheMarket) and prints a clear summary.

Usage:
    python3 test_fmv.py
"""

import asyncio
import logging
import sys
from pathlib import Path

# Write all output to fmv_test.log in the same folder as this script
LOG_PATH = Path(__file__).parent / "fmv_test.log"
log_file = open(LOG_PATH, "w", encoding="utf-8")

class Tee:
    def __init__(self, *streams): self.streams = streams
    def write(self, data):
        for s in self.streams: s.write(data); s.flush()
    def flush(self):
        for s in self.streams: s.flush()

tee = Tee(sys.stdout, log_file)
sys.stdout = tee
sys.stderr = tee

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)
# Suppress httpx request logs — they contain the full bot token in the URL
logging.getLogger("httpx").setLevel(logging.WARNING)

print(f"Logging to: {LOG_PATH}\n")

from utils.valuation import get_fmv_verdict, _FMV_CACHE

TEST_LISTINGS = [
    {
        "label":   "Zoopla (direct URL)",
        "source":  "zoopla",
        "area":    "Covent Garden",
        "address": "Temple House, Arundel Street WC2R",
        "price":   "£7,367 pcm",
        "title":   "2 bedroom flat",
        "url":     "https://www.zoopla.co.uk/to-rent/details/72897158/",
    },
    {
        "label":   "Rightmove (direct URL)",
        "source":  "rightmove",
        "area":    "Tower Hill",
        "address": "16 Christian Street, London, E1 1AW",
        "price":   "£3,250 pcm",
        "title":   "2 bedroom flat",
        "url":     "https://www.rightmove.co.uk/properties/174102632#/?channel=RES_LET",
    },
    {
        "label":   "OnTheMarket (search fallback)",
        "source":  "onthemarket",
        "area":    "London Bridge",
        "address": "151 Tower Bridge Road, London, SE1 3JE",
        "price":   "£3,497 pcm",
        "title":   "2 bedroom flat",
        "url":     "https://www.onthemarket.com/details/19272996/",
    },
]


def _banner(label: str):
    print("\n" + "=" * 60)
    print(f"  {label}")
    print("=" * 60)


async def test_one(listing: dict):
    _banner(listing["label"])
    print(f"  Address : {listing['address']}")
    print(f"  Asking  : {listing['price']}")
    print(f"  URL     : {listing['url']}\n")

    # Clear cache so each test does a fresh scrape
    _FMV_CACHE.clear()

    verdict = await get_fmv_verdict(listing)

    fmv       = verdict.get("fmv")
    asking    = verdict.get("asking_price")
    diff      = verdict.get("difference")
    result    = verdict.get("verdict")
    conf      = verdict.get("confidence")
    own_hist  = verdict.get("own_history_count", 0)
    let_agr   = verdict.get("let_agreed_count", 0)
    reasoning = verdict.get("reasoning", "")

    print(f"  FMV              : £{fmv:,}/mo" if fmv else "  FMV              : N/A")
    print(f"  Asking           : £{asking:,}/mo" if asking else "  Asking           : N/A")
    if fmv and asking:
        sign = "+" if diff >= 0 else ""
        print(f"  Difference       : {sign}£{diff:,}")
    print(f"  VERDICT          : {result}")
    print(f"  Confidence       : {conf}")
    print(f"  Own history      : {own_hist} record(s)")
    print(f"  Let-agreed comps : {let_agr} within 0.25mi")
    if reasoning:
        print(f"\n  Reasoning: {reasoning}")


async def main():
    print("\n🔍 FMV Test — own history + let-agreed comps only (no asking prices)")
    for listing in TEST_LISTINGS:
        await test_one(listing)
    print("\n✅ Done\n")


if __name__ == "__main__":
    asyncio.run(main())
