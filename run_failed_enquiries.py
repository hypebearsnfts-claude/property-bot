"""
run_failed_enquiries.py
-----------------------
One-shot script to re-submit all previously-failed enquiries from enquiry_log.json.

Usage (from the property-bot directory with .venv active):
    python run_failed_enquiries.py

Requires the same .env / environment variables as the main bot:
    RIGHTMOVE_COOKIES   — JSON cookie array for Rightmove session
"""

import asyncio
import json
import logging
import os
import sys
from pathlib import Path

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Verbose logging so we can see what's happening
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    stream=sys.stdout,
)

from enquiry_bot import submit_enquiries

LOG_PATH = Path(__file__).parent / "enquiry_log.json"


def get_failed_listings() -> list[dict]:
    """Read enquiry_log.json and return listing dicts for every failed entry."""
    if not LOG_PATH.exists():
        print("enquiry_log.json not found.")
        return []

    data = json.loads(LOG_PATH.read_text(encoding="utf-8"))
    failed = []
    for url, entry in data.items():
        if entry.get("status") != "failed":
            continue
        if "rightmove.co.uk" in url:
            src = "rightmove"
        elif "onthemarket.com" in url:
            src = "onthemarket"
        elif "zoopla.co.uk" in url:
            src = "zoopla"
        else:
            continue  # Skip unknown / non-automated portals
        failed.append({
            "url":     url,
            "source":  src,
            "address": entry.get("address", ""),
            "area":    "",
            "price":   "",
        })
    return failed


async def main():
    listings = get_failed_listings()
    if not listings:
        print("✅ No failed enquiries found in enquiry_log.json — nothing to retry.")
        return

    print(f"\n🔁 Retrying {len(listings)} failed enquiries...\n")
    for lst in listings:
        print(f"   [{lst['source']:12}]  {lst['address'][:55]}  {lst['url'][:60]}")

    print()
    results = await submit_enquiries(listings)

    # Summary
    sent    = [(u, v) for u, v in results.items() if v.get("status") == "sent"]
    failed  = [(u, v) for u, v in results.items() if v.get("status") == "failed"]
    skipped = [(u, v) for u, v in results.items() if v.get("status") == "skipped"]
    other   = [(u, v) for u, v in results.items()
               if v.get("status") not in ("sent", "failed", "skipped")]

    print("\n" + "=" * 60)
    print(f"RESULTS: {len(sent)} sent  |  {len(failed)} failed  |  {len(skipped)} skipped  |  {len(other)} other")
    print("=" * 60)

    if sent:
        print(f"\n✅  Sent ({len(sent)}):")
        for url, v in sent:
            addr = v.get("address") or v.get("area") or ""
            print(f"   {addr[:55]}  {url[:70]}")

    if failed:
        print(f"\n❌  Still failed ({len(failed)}):")
        for url, v in failed:
            addr = v.get("address") or v.get("area") or ""
            print(f"   {addr[:55]}  {url[:70]}")

    if other:
        print(f"\n⚠️   Other ({len(other)}):")
        for url, v in other:
            print(f"   [{v.get('status')}]  {url[:70]}")

    print()


if __name__ == "__main__":
    asyncio.run(main())
