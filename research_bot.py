"""
research_bot.py
---------------
Telegram bot that triggers property scraping and sends listings.
Commands:
  /start  - confirm bot is online
  /status - confirm scrapers are ready
  /run    - kick off a full property search (scrape + enrich + send results)

Also exports:
  run_research_pipeline(bot, chat_id)  — called by scheduler.py in automated mode.
    Runs all scrapers, saves listings.json, sends a Telegram summary, returns count.
"""

import asyncio
import json
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

import scheduler
from scheduler import format_listing

# ── Config ───────────────────────────────────────────────────────────────────
load_dotenv()

TOKEN   = os.getenv("TELEGRAM_RESEARCH_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_RESEARCH_CHAT_ID")

# Max listings to send per run (Telegram rate-limits at ~30 msg/sec)
MAX_SEND = int(os.getenv("MAX_LISTINGS_SEND", "50"))

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ── Command handlers ─────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a greeting when /start is issued."""
    await update.message.reply_text("Research Bot is online 🏠")


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Report scraper readiness when /status is issued."""
    await update.message.reply_text(
        "All scrapers ready ✅\n"
        "• Rightmove\n"
        "• OpenRent\n"
        "• Zoopla\n\n"
        "Use /run to start a property search."
    )


async def run(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Run the full pipeline and send results to Telegram."""
    await update.message.reply_text(
        "🔍 Starting property search across Rightmove, OpenRent & Zoopla…\n"
        "This takes a few minutes — I'll send results as they're ready."
    )
    logger.info("Property search triggered by user %s", update.effective_user.id)

    try:
        listings = await scheduler.run_search(enrich=True)
    except Exception as exc:
        logger.error("run_search failed: %s", exc)
        await update.message.reply_text(f"❌ Search failed: {exc}")
        return

    if not listings:
        await update.message.reply_text("No listings found. Try again later.")
        return

    total    = len(listings)
    to_send  = listings[:MAX_SEND]

    await update.message.reply_text(
        f"✅ Found *{total}* listings — sending top {len(to_send)}…",
        parse_mode="Markdown",
    )

    for listing in to_send:
        try:
            msg = format_listing(listing)
            await update.message.reply_text(msg, parse_mode="Markdown",
                                            disable_web_page_preview=True)
            await asyncio.sleep(0.4)   # stay well under Telegram rate limit
        except Exception as exc:
            logger.warning("Failed to send listing: %s", exc)

    await update.message.reply_text(
        f"🏁 Done! Sent {len(to_send)} of {total} listings."
    )


# ── Automated pipeline entry point (called by scheduler.py) ──────────────────

LISTINGS_PATH = Path(__file__).parent / "listings.json"


async def run_research_pipeline(bot, chat_id) -> int:
    """
    Scrape all sources, save listings.json, notify Telegram, return count.

    Called by scheduler.py as step 1 of the automated daily pipeline.

    Parameters
    ----------
    bot      : telegram.Bot instance
    chat_id  : Telegram chat ID (str or int)

    Returns
    -------
    int  Number of unique listings saved to listings.json (0 if none found).
    """
    chat_id = int(chat_id)
    logger.info("[research] Automated pipeline started")

    await bot.send_message(
        chat_id=chat_id,
        text="🔍 Research Bot: scraping Rightmove, Zoopla, OpenRent & OnTheMarket…",
    )

    try:
        # Run all 4 scrapers with full deduplication (no enrichment here —
        # that happens in filter_bot so we get accurate walk times per listing)
        listings = await scheduler._run_scrapers()
    except Exception as exc:
        logger.error("[research] Scraping failed: %s", exc)
        await bot.send_message(
            chat_id=chat_id,
            text=f"❌ Research scraping failed: {exc}",
        )
        return 0

    if not listings:
        await bot.send_message(
            chat_id=chat_id,
            text="⚠️ Research Bot: no listings found. Filter step skipped.",
        )
        return 0

    # Save to listings.json for filter_bot to read
    try:
        LISTINGS_PATH.write_text(
            json.dumps(listings, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info("[research] Saved %d listings to %s", len(listings), LISTINGS_PATH)
    except Exception as exc:
        logger.error("[research] Failed to write listings.json: %s", exc)
        await bot.send_message(
            chat_id=chat_id,
            text=f"❌ Research Bot: failed to save listings — {exc}",
        )
        return 0

    await bot.send_message(
        chat_id=chat_id,
        text=f"✅ Research complete — *{len(listings):,}* unique listings saved.",
        parse_mode="Markdown",
    )
    return len(listings)


# ── Entry point ──────────────────────────────────────────────────────────────

def main() -> None:
    if not TOKEN:
        raise ValueError("TELEGRAM_RESEARCH_BOT_TOKEN is not set in .env")

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start",  start))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("run",    run))

    logger.info("Research Bot starting — polling for updates…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
