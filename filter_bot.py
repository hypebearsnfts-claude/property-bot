"""
filter_bot.py
-------------
Pipeline:
  1. Read listings.json
  2. Walk-time filter  →  keep listings ≤ MAX_WALK_MINS from a tube/rail station
  3. FMV verdict       →  keep listings where asking_price <= FMV + £500
  4. Send each passing property to Telegram in a rich formatted message
  5. Send a summary when done

Commands:
  /start   — confirm bot is online
  /status  — show settings and listings.json count
  /run     — full pipeline: walk filter → FMV check → send results

Environment variables (.env):
  TELEGRAM_FILTER_BOT_TOKEN   — bot token from @BotFather
  MAX_WALK_MINS               — max walk to station in minutes (default 10)
  MAX_LISTINGS_SEND           — safety cap on messages per /run (default 50)
  GOOGLE_MAPS_API_KEY         — required for walk times
  ANTHROPIC_API_KEY           — required for FMV reasoning
"""

import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from utils.walk_time import nearest_walk_minutes
from utils.valuation import get_fmv_verdict, _parse_price_pcm
from utils.seen_listings import is_duplicate, mark_as_seen, clean_old_entries

# ── Config ────────────────────────────────────────────────────────────────────

load_dotenv()

TOKEN         = os.getenv("TELEGRAM_FILTER_BOT_TOKEN")
MAX_WALK_MINS = int(os.getenv("MAX_WALK_MINS", "10"))
MAX_SEND      = int(os.getenv("MAX_LISTINGS_SEND", "0"))   # 0 = no limit

LISTINGS_PATH = Path(__file__).parent / "listings.json"

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
# Suppress httpx request logs — they contain the full bot token in the URL
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ── Agent blacklist ───────────────────────────────────────────────────────────
# Listings from these agents are silently dropped before any other processing.
# Case-insensitive. Add/remove names here as needed.

BLACKLISTED_AGENTS = [
    "greater london properties",
    "foxtons",
    "dexters",
    "ila",
    "219baker",
    "219 baker",
    "blueground",
    "cbre",
    "glp",
]


def _is_blacklisted(listing: dict) -> bool:
    """Return True if the listing is from a blacklisted agent."""
    agent = (listing.get("agent") or "").lower()
    if agent:
        return any(blocked in agent for blocked in BLACKLISTED_AGENTS)
    # Fallback: check title + address for agent name leakage
    haystack = " ".join([
        listing.get("title", ""),
        listing.get("address", ""),
        listing.get("url", ""),
    ]).lower()
    return any(blocked in haystack for blocked in BLACKLISTED_AGENTS)


# ── Keyword blacklist ─────────────────────────────────────────────────────────
# Listings whose title, address, or description contain any of these phrases
# are silently dropped. Case-insensitive. Add/remove phrases here as needed.

BLACKLISTED_KEYWORDS = [
    "24/7 concierge",
    "24 hour concierge",
    "24hr concierge",
    "round-the-clock concierge",
    "round the clock concierge",
]


def _has_blacklisted_keyword(listing: dict) -> bool:
    """Return True if any blacklisted keyword appears anywhere in the listing text."""
    haystack = " ".join([
        listing.get("title", ""),
        listing.get("address", ""),
        listing.get("description", ""),
        listing.get("summary", ""),
    ]).lower()
    return any(kw in haystack for kw in BLACKLISTED_KEYWORDS)


# Walk filter uses dynamic nearest-station lookup (any tube/rail station)
# via Google Maps Geocoding + Places Nearby — no hardcoded list needed.


# ── Message formatting ────────────────────────────────────────────────────────

# MarkdownV2 special characters that must be escaped in plain text
_MDV2_SPECIAL = r'\_*[]()~`>#+=|{}.!'

def _esc(text: str) -> str:
    """Escape all MarkdownV2 special characters in a plain-text string."""
    result = ""
    for ch in str(text):
        if ch in _MDV2_SPECIAL or ch == '-':
            result += '\\' + ch
        else:
            result += ch
    return result


def _format_station(station: str) -> str:
    return (
        station
        .replace(", London", "")
        .replace(" Underground Station", " ⬤")
        .replace(" Station", " 🚉")
    )


def _format_property_message(listing: dict, verdict: dict) -> str:
    """Format the rich Telegram MarkdownV2 message for a passing property."""
    area    = listing.get("area", "")
    address = listing.get("address", "")
    url     = listing.get("url", "")
    source  = listing.get("source", "").capitalize()

    # Price
    asking     = verdict.get("asking_price") or listing.get("price_pcm", 0)
    fmv        = verdict.get("fmv", 0)
    difference = verdict.get("difference", 0)   # asking - fmv

    # Walk
    station     = listing.get("walk_station", "nearest station")
    walk_mins   = listing.get("walk_mins")
    walk_mins   = walk_mins if walk_mins is not None else "?"
    station_fmt = _format_station(station) if station else "nearest station"

    # Beds / prop type
    beds      = listing.get("beds")
    prop_type = listing.get("prop_type") or "property"
    bed_str   = f"{beds} bed " if beds else ""

    # FMV label (plain text — will be escaped)
    if difference < 0:
        diff_label = f"£{abs(difference):,} below FMV — great deal"
    elif difference == 0:
        diff_label = "exactly at FMV"
    else:
        diff_label = f"£{difference:,} above — within tolerance"

    # Data sources context
    own_history_count = verdict.get("own_history_count", verdict.get("historical_count", 0))
    let_agreed_count  = verdict.get("let_agreed_count",  verdict.get("comparable_count", 0))
    confidence        = verdict.get("confidence", "low").capitalize()
    reasoning         = verdict.get("reasoning", "")

    own_str = (f"{own_history_count} own\\-history record{'s' if own_history_count != 1 else ''}"
               if own_history_count else "no own history found")
    let_str = (f"{let_agreed_count} let\\-agreed comp{'s' if let_agreed_count != 1 else ''} \\(0\\.25mi\\)"
               if let_agreed_count else "no let\\-agreed comps found")

    # Build message — escape all dynamic plain-text content
    lines = [
        f"🏠 *{_esc(bed_str)}{_esc(prop_type)} \\- {_esc(area)}*",
        f"📍 {_esc(address)}",
        f"💰 Asking: £{asking:,}/month",
        f"📊 FMV: £{fmv:,}/month \\({_esc(diff_label)}\\)",
        f"🚶 {walk_mins} min walk to {_esc(station_fmt)}",
        f"✅ VERDICT: PASS  \\[{_esc(source)}\\]",
        f"🔗 [View listing]({url})",
        "\\-\\-\\-",
        f"Own history: {own_str}",
        f"Nearby let\\-agreed: {let_str}",
        f"Confidence: {_esc(confidence)}",
    ]
    if reasoning:
        # Escape reasoning for italic display
        lines.append(f"_{_esc(reasoning)}_")

    return "\n".join(lines)


# ── Walk time check ───────────────────────────────────────────────────────────

def _check_walk(listing: dict) -> tuple[Optional[str], Optional[int]]:
    address = listing.get("address") or listing.get("area", "")
    # Pass full listing so nearest_walk_minutes can use the free area-based
    # fallback if Google Maps API is unavailable or returns an error.
    return nearest_walk_minutes(address, listing=listing)


# ── Pipeline ──────────────────────────────────────────────────────────────────

async def run_pipeline(
    max_walk: int = MAX_WALK_MINS,
) -> tuple[list[dict], int, int, int]:
    """
    Full pipeline: load → walk filter → dedup → FMV verdict.

    Returns (passing_listings, total_scraped, new_after_dedup, walk_count)
    Each passing listing has walk_station, walk_mins, and verdict dict attached.
    """
    if not LISTINGS_PATH.exists():
        logger.error("[filter] listings.json not found at %s", LISTINGS_PATH)
        return [], 0, 0, 0

    # Clean seen_listings.json of entries older than 30 days
    clean_old_entries()

    raw: list[dict] = json.loads(LISTINGS_PATH.read_text(encoding="utf-8"))
    logger.info("[filter] Loaded %d listings", len(raw))

    # Step 1 — agent blacklist filter
    before = len(raw)
    raw = [l for l in raw if not _is_blacklisted(l)]
    blocked = before - len(raw)
    if blocked:
        logger.info("[filter] Agent blacklist: removed %d listings (%s)",
                    blocked, ", ".join(BLACKLISTED_AGENTS))

    # Step 2 — keyword blacklist filter (e.g. 24/7 concierge)
    before = len(raw)
    raw = [l for l in raw if not _has_blacklisted_keyword(l)]
    kw_blocked = before - len(raw)
    if kw_blocked:
        logger.info("[filter] Keyword blacklist: removed %d listings", kw_blocked)

    # Step 3 — walk time filter
    loop = asyncio.get_event_loop()
    walk_passed: list[dict] = []

    for listing in raw:
        try:
            station, mins = await loop.run_in_executor(None, _check_walk, listing)
        except Exception as exc:
            logger.warning("[filter] Walk check failed for %s: %s",
                           listing.get("address"), exc)
            continue

        if mins is None or mins <= max_walk:
            listing = dict(listing)
            listing["walk_station"] = station or "nearest station"
            listing["walk_mins"]    = mins  # None → displays as "?" in message
            # Attach parsed price for convenience
            pcm = _parse_price_pcm(listing.get("price", ""))
            if pcm:
                listing["price_pcm"] = pcm
            walk_passed.append(listing)

    walk_count = len(walk_passed)
    logger.info("[filter] Walk filter (≤%d min): %d/%d passed", max_walk, walk_count, len(raw))

    # Step 4 — Duplicate filter (skip listings already sent in a previous run)
    new_listings = [l for l in walk_passed if not is_duplicate(l)]
    dupes_skipped = walk_count - len(new_listings)
    if dupes_skipped:
        logger.info("[filter] Duplicate filter: skipped %d already-sent listings (%d new)",
                    dupes_skipped, len(new_listings))
    walk_passed = new_listings

    # Step 5 — FMV verdict
    fmv_passed: list[dict] = []

    for i, listing in enumerate(walk_passed, 1):
        logger.info("[filter] FMV check %d/%d: %s", i, len(walk_passed), listing.get("address", "")[:50])
        try:
            verdict = await get_fmv_verdict(listing)
            if verdict.get("verdict") == "PASS":
                listing["_verdict"] = verdict
                fmv_passed.append(listing)
            else:
                logger.debug("[filter] FAIL: %s (asking £%s, FMV £%s)",
                             listing.get("address", "")[:40],
                             verdict.get("asking_price"), verdict.get("fmv"))
        except Exception as exc:
            logger.warning("[filter] FMV verdict failed for %s: %s",
                           listing.get("address"), exc)

    # Sort by walk time (closest first)
    fmv_passed.sort(key=lambda l: l.get("walk_mins") or 999)

    total_scraped  = len(json.loads(LISTINGS_PATH.read_text(encoding="utf-8"))) if LISTINGS_PATH.exists() else 0
    new_after_dedup = len(walk_passed)   # walk_passed was reassigned to new_listings after dedup
    logger.info("[filter] FMV filter: %d/%d passed", len(fmv_passed), new_after_dedup)
    return fmv_passed, total_scraped, new_after_dedup, walk_count


# ── Telegram handlers ─────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Filter Bot is online 🔎")


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    listings_info = "❌ not found"
    if LISTINGS_PATH.exists():
        try:
            raw = json.loads(LISTINGS_PATH.read_text(encoding="utf-8"))
            listings_info = f"✅ {len(raw):,} listings"
        except Exception:
            listings_info = "⚠️ found but unreadable"

    google_key    = "✅ set" if os.getenv("GOOGLE_MAPS_API_KEY")  else "❌ missing"
    anthropic_key = "✅ set" if os.getenv("ANTHROPIC_API_KEY")     else "❌ missing"

    await update.message.reply_text(
        f"⚙️ *Filter Bot settings*\n"
        f"• Max walk to station: {MAX_WALK_MINS} min\n"
        f"• listings\\.json: {listings_info}\n"
        f"• Google Maps API: {google_key}\n"
        f"• Anthropic API: {anthropic_key}\n\n"
        "Use /run to filter and send results\\.",
        parse_mode="MarkdownV2",
    )


async def run(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        f"🔍 Starting filter pipeline…\n"
        f"Step 1: Agent & keyword blacklist\n"
        f"Step 2: Walk time check \\(≤{MAX_WALK_MINS} min to station\\)\n"
        f"Step 3: Duplicate filter \\(skip already\\-sent listings\\)\n"
        f"Step 4: FMV verdict \\(asking ≤ FMV \\+ £500\\)\n"
        "This will take several minutes — results sent as they pass\\.",
        parse_mode="MarkdownV2",
    )

    try:
        passing, total_scraped, new_count, walk_count = await run_pipeline(MAX_WALK_MINS)
    except Exception as exc:
        logger.error("[filter] Pipeline failed: %s", exc, exc_info=True)
        await update.message.reply_text(f"❌ Pipeline failed: {exc}")
        return

    dupes_skipped = walk_count - new_count
    if not passing:
        await update.message.reply_text(
            f"No listings passed all filters\\.\n"
            f"• {total_scraped:,} scraped today\n"
            f"• {dupes_skipped:,} already sent \\(skipped\\)\n"
            f"• {new_count} new listings checked for FMV\n"
            f"• 0 passed FMV\n\n"
            "Try raising MAX\\_WALK\\_MINS or checking that ANTHROPIC\\_API\\_KEY is set\\.",
            parse_mode="MarkdownV2",
        )
        return

    to_send = passing[:MAX_SEND] if MAX_SEND > 0 else passing
    await update.message.reply_text(
        f"✅ *{len(passing)}* listings passed — sending {len(to_send)}…",
        parse_mode="Markdown",
    )

    sent = 0
    for listing in to_send:
        verdict = listing.get("_verdict", {})
        try:
            msg = _format_property_message(listing, verdict)
            await update.message.reply_text(
                msg,
                parse_mode="MarkdownV2",
                disable_web_page_preview=True,
            )
            mark_as_seen(listing)   # record so tomorrow's run skips it
            sent += 1
            await asyncio.sleep(0.5)   # Telegram rate limit
        except Exception as exc:
            logger.warning("[filter] Failed to send listing: %s", exc)
            # Fallback: plain text
            try:
                plain = (
                    f"{listing.get('area')} — £{verdict.get('asking_price', '?')}/mo\n"
                    f"{listing.get('address')}\n"
                    f"Walk: {listing.get('walk_mins')} min | FMV: £{verdict.get('fmv', '?')}/mo\n"
                    f"{listing.get('url')}"
                )
                await update.message.reply_text(plain, disable_web_page_preview=True)
                mark_as_seen(listing)
                sent += 1
            except Exception:
                pass

    # Summary message
    await update.message.reply_text(
        f"🏁 *Filter complete\\.*\n"
        f"• Scraped today: {total_scraped:,}\n"
        f"• Already sent \\(skipped\\): {dupes_skipped:,}\n"
        f"• New listings checked for FMV: {new_count}\n"
        f"• Passed FMV: {len(passing)}\n"
        f"• Sent: {sent}",
        parse_mode="MarkdownV2",
    )


# ── Automated pipeline (called by scheduler.py) ───────────────────────────────

async def run_filter_pipeline_and_send(
    bot,
    chat_id: str,
    total_scraped: int = 0,
) -> None:
    """
    Run the full filter pipeline and push results to Telegram.
    Called directly by scheduler.py — no manual command needed.

    Steps:
      1. Send "Filter Bot started" message
      2. Run agent blacklist + walk filter + FMV pipeline
      3. Send each PASS listing as a formatted message
      4. Send final summary
    """
    logger.info("[filter] Automated pipeline triggered (total_scraped=%d)", total_scraped)
    chat_id = int(chat_id)

    count_str = f"{total_scraped:,}" if total_scraped else "?"
    await bot.send_message(
        chat_id=chat_id,
        text=f"⚙️ Filter Bot started. Analysing {count_str} properties...",
    )

    try:
        passing, total_scraped_now, new_count, walk_count = await run_pipeline(MAX_WALK_MINS)
    except Exception as exc:
        logger.error("[filter] Pipeline failed: %s", exc)
        await bot.send_message(chat_id=chat_id, text=f"❌ Filter pipeline failed: {exc}")
        return

    dupes_skipped = walk_count - new_count

    if not passing:
        await bot.send_message(
            chat_id=chat_id,
            text=(
                f"✅ Done. 0 properties passed.\n"
                f"• Scraped today: {total_scraped_now:,}\n"
                f"• Already sent (skipped): {dupes_skipped:,}\n"
                f"• New listings checked for FMV: {new_count:,}"
            ),
        )
        return

    to_send = passing[:MAX_SEND] if MAX_SEND > 0 else passing
    await bot.send_message(
        chat_id=chat_id,
        text=f"✅ *{len(passing)}* listings passed — sending {len(to_send)}…",
        parse_mode="Markdown",
    )

    sent = 0
    for listing in to_send:
        verdict = listing.get("_verdict", {})
        for attempt in range(3):   # up to 3 tries per listing
            try:
                msg = _format_property_message(listing, verdict)
                await bot.send_message(
                    chat_id=chat_id,
                    text=msg,
                    parse_mode="MarkdownV2",
                    disable_web_page_preview=True,
                )
                mark_as_seen(listing)
                sent += 1
                await asyncio.sleep(2.5)   # ~24 msg/min — safely under Telegram's 30/min limit
                break
            except Exception as exc:
                exc_str = str(exc)
                # Telegram flood control — wait the requested time then retry
                import re as _re
                retry_m = _re.search(r"Retry in (\d+)", exc_str)
                if retry_m:
                    wait = int(retry_m.group(1)) + 2
                    logger.info("[filter] Flood control — waiting %ds before retry", wait)
                    await asyncio.sleep(wait)
                    continue
                # MarkdownV2 parse error — fall back to plain text
                logger.warning("[filter] Send failed (attempt %d): %s", attempt + 1, exc)
                try:
                    plain = (
                        f"{listing.get('area')} — £{verdict.get('asking_price', '?')}/mo\n"
                        f"{listing.get('address')}\n"
                        f"Walk: {listing.get('walk_mins')} min | FMV: £{verdict.get('fmv', '?')}/mo\n"
                        f"{listing.get('url')}"
                    )
                    await bot.send_message(chat_id=chat_id, text=plain, disable_web_page_preview=True)
                    mark_as_seen(listing)
                    sent += 1
                    await asyncio.sleep(2.5)
                except Exception:
                    pass
                break

    await bot.send_message(
        chat_id=chat_id,
        text=(
            f"✅ Done.\n"
            f"• Scraped today: {total_scraped_now:,}\n"
            f"• Already sent (skipped): {dupes_skipped:,}\n"
            f"• New listings checked for FMV: {new_count:,}\n"
            f"• Passed FMV & sent: {sent}"
        ),
    )
    logger.info("[filter] Automated pipeline complete — %d/%d new passed, %d sent (dupes skipped: %d)",
                len(passing), new_count, sent, dupes_skipped)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    if not TOKEN:
        raise ValueError("TELEGRAM_FILTER_BOT_TOKEN is not set in .env")

    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start",  start))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("run",    run))

    logger.info("Filter Bot starting (walk≤%d min, FMV+£500 rule) — polling…", MAX_WALK_MINS)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
