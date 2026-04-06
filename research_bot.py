"""
research_bot.py
---------------
Telegram bot that triggers property scraping and sends listings.
Commands:
  /start  - confirm bot is online
  /status - confirm scrapers are ready
  /run    - kick off a full property search (scrape + enrich + send results)
"""

import asyncio
import logging
import os

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
