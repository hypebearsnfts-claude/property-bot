"""
enquiry_bot.py
--------------
Submits viewing enquiries for listings that passed all filters.

  Rightmove / Zoopla / OnTheMarket  →  auto-fills and submits the enquiry form
  OpenRent                           →  returns a note so Telegram prompts manual contact

Contact details used in all forms:
  Name    : Ernest Siow
  Email   : ernest.slh@hotmail.com
  Phone   : +6590673996
  Message : see ENQUIRY_MESSAGE below

Dedup: enquiry_log.json tracks submitted URLs so the same property is
       never enquired twice (even across multiple daily runs).
"""

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path

from playwright.async_api import async_playwright, TimeoutError as PWTimeout

try:
    from playwright_stealth import stealth_async as _stealth_async
    _STEALTH = True
except ImportError:
    _STEALTH = False

logger = logging.getLogger(__name__)

# ── Contact details ───────────────────────────────────────────────────────────

CONTACT_FIRST = "Ernest"
CONTACT_LAST  = "Siow"
CONTACT_NAME  = "Ernest Siow"
CONTACT_EMAIL = "ernest.slh@hotmail.com"
CONTACT_PHONE = "+6590673996"
ENQUIRY_MESSAGE = (
    "Hi there, I am very much interested in this property. "
    "If you may, could you whatsapp me at +6590673996 so I can respond quickly? "
    "Thank you in advance."
)

# ── Enquiry log ───────────────────────────────────────────────────────────────
# Tracks which URLs have already been enquired so we never double-contact.
# Committed to GitHub (like seen_listings.json) so state persists across runs.

_LOG_PATH = Path(__file__).parent / "enquiry_log.json"


def _load_log() -> dict:
    if not _LOG_PATH.exists():
        return {}
    try:
        data = json.loads(_LOG_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_log(data: dict) -> None:
    try:
        _LOG_PATH.write_text(
            json.dumps(data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception as exc:
        logger.warning("[enquiry] Failed to save enquiry_log.json: %s", exc)


def already_enquired(listing: dict) -> bool:
    """Return True if an enquiry was already submitted for this listing URL."""
    url = listing.get("url", "").strip()
    return bool(url) and url in _load_log()


def mark_enquired(listing: dict, status: str = "sent") -> None:
    """Record this listing URL with today's date and enquiry status."""
    url = listing.get("url", "").strip()
    if not url:
        return
    log = _load_log()
    log[url] = {
        "date":    datetime.now().strftime("%Y-%m-%d"),
        "status":  status,
        "address": listing.get("address", ""),
    }
    _save_log(log)


# ── Browser context helper ────────────────────────────────────────────────────

async def _new_ctx(browser):
    """Open a fresh browser context with a realistic user-agent."""
    ctx = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 900},
        locale="en-GB",
        extra_http_headers={"Accept-Language": "en-GB,en;q=0.9"},
    )
    page = await ctx.new_page()
    if _STEALTH:
        await _stealth_async(page)
    return ctx, page


async def _dismiss_cookies(page):
    """Try to accept/dismiss cookie banners."""
    for sel in [
        "button#onetrust-accept-btn-handler",
        "button:has-text('Accept all')",
        "button:has-text('Accept cookies')",
        "button:has-text('I agree')",
        "button:has-text('OK')",
    ]:
        try:
            await page.locator(sel).first.click(timeout=2_500)
            await page.wait_for_timeout(400)
            return
        except Exception:
            continue


async def _safe_fill(page, selectors: list[str], value: str, timeout: int = 4_000) -> bool:
    """Try each selector in order; return True on first success."""
    for sel in selectors:
        try:
            await page.fill(sel, value, timeout=timeout)
            return True
        except Exception:
            continue
    return False


# ── Rightmove ─────────────────────────────────────────────────────────────────

async def _enquire_rightmove(browser, listing: dict) -> bool:
    ctx, page = await _new_ctx(browser)
    url = listing["url"]
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        await _dismiss_cookies(page)

        # Click "Email agent" / "Contact agent" button
        btn = page.locator(
            "button:has-text('Email agent'), "
            "button:has-text('Contact agent'), "
            "a:has-text('Email agent'), "
            "[data-test='contact-agent-button'], "
            "button:has-text('Request details')"
        ).first
        await btn.click(timeout=10_000)
        await page.wait_for_timeout(1_200)

        # Fill first + last name (Rightmove splits them)
        await _safe_fill(page, [
            "input[name='firstName']", "input[id*='firstName' i]",
            "input[placeholder*='first name' i]",
        ], CONTACT_FIRST)

        await _safe_fill(page, [
            "input[name='lastName']", "input[id*='lastName' i]",
            "input[placeholder*='last name' i]",
        ], CONTACT_LAST)

        # Some versions use a single name field
        await _safe_fill(page, [
            "input[name='name']", "input[placeholder*='full name' i]",
        ], CONTACT_NAME)

        await _safe_fill(page, [
            "input[name='email']", "input[type='email']",
        ], CONTACT_EMAIL)

        await _safe_fill(page, [
            "input[name='phone']", "input[type='tel']",
            "input[name='telephone']",
        ], CONTACT_PHONE)

        await _safe_fill(page, [
            "textarea[name='message']", "textarea[id*='message' i]",
            "textarea",
        ], ENQUIRY_MESSAGE)

        # Submit
        submit = page.locator(
            "button[type='submit']:has-text('Send'), "
            "button:has-text('Send enquiry'), "
            "button:has-text('Submit'), "
            "button[type='submit']"
        ).first
        await submit.click(timeout=10_000)
        await page.wait_for_timeout(2_500)

        logger.info("[enquiry] ✅ Rightmove submitted: %s", url[:80])
        return True

    except Exception as exc:
        logger.warning("[enquiry] ❌ Rightmove failed (%s): %s", url[:60], exc)
        return False
    finally:
        await ctx.close()


# ── Zoopla ────────────────────────────────────────────────────────────────────

async def _enquire_zoopla(browser, listing: dict) -> bool:
    ctx, page = await _new_ctx(browser)
    url = listing["url"]
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        await _dismiss_cookies(page)

        btn = page.locator(
            "button:has-text('Enquire'), "
            "button:has-text('Get in touch'), "
            "a:has-text('Email agent'), "
            "[data-testid='enquiry-button'], "
            "button:has-text('Contact agent')"
        ).first
        await btn.click(timeout=10_000)
        await page.wait_for_timeout(1_200)

        await _safe_fill(page, [
            "input[name='firstName']", "input[id*='firstName' i]",
        ], CONTACT_FIRST)

        await _safe_fill(page, [
            "input[name='lastName']", "input[id*='lastName' i]",
        ], CONTACT_LAST)

        await _safe_fill(page, [
            "input[name='name']", "input[placeholder*='name' i]",
        ], CONTACT_NAME)

        await _safe_fill(page, [
            "input[type='email']", "input[name='email']",
        ], CONTACT_EMAIL)

        await _safe_fill(page, [
            "input[type='tel']", "input[name='phone']",
        ], CONTACT_PHONE)

        await _safe_fill(page, ["textarea"], ENQUIRY_MESSAGE)

        submit = page.locator(
            "button[type='submit'], button:has-text('Send message'), "
            "button:has-text('Send enquiry')"
        ).first
        await submit.click(timeout=10_000)
        await page.wait_for_timeout(2_500)

        logger.info("[enquiry] ✅ Zoopla submitted: %s", url[:80])
        return True

    except Exception as exc:
        logger.warning("[enquiry] ❌ Zoopla failed (%s): %s", url[:60], exc)
        return False
    finally:
        await ctx.close()


# ── OnTheMarket ───────────────────────────────────────────────────────────────

async def _enquire_otm(browser, listing: dict) -> bool:
    ctx, page = await _new_ctx(browser)
    url = listing["url"]
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        await _dismiss_cookies(page)

        btn = page.locator(
            "button:has-text('Email agent'), "
            "button:has-text('Contact agent'), "
            "a:has-text('Email agent'), "
            "[data-testid='contact-button'], "
            "button:has-text('Request viewing')"
        ).first
        await btn.click(timeout=10_000)
        await page.wait_for_timeout(1_200)

        await _safe_fill(page, [
            "input[name='first_name']", "input[name='firstName']",
            "input[id*='first' i]",
        ], CONTACT_FIRST)

        await _safe_fill(page, [
            "input[name='last_name']", "input[name='lastName']",
            "input[id*='last' i]",
        ], CONTACT_LAST)

        await _safe_fill(page, [
            "input[name='name']", "input[placeholder*='name' i]",
        ], CONTACT_NAME)

        await _safe_fill(page, [
            "input[type='email']", "input[name='email']",
        ], CONTACT_EMAIL)

        await _safe_fill(page, [
            "input[type='tel']", "input[name='phone']",
            "input[name='telephone']",
        ], CONTACT_PHONE)

        await _safe_fill(page, ["textarea"], ENQUIRY_MESSAGE)

        submit = page.locator(
            "button[type='submit'], button:has-text('Send enquiry'), "
            "button:has-text('Send message'), button:has-text('Submit')"
        ).first
        await submit.click(timeout=10_000)
        await page.wait_for_timeout(2_500)

        logger.info("[enquiry] ✅ OTM submitted: %s", url[:80])
        return True

    except Exception as exc:
        logger.warning("[enquiry] ❌ OTM failed (%s): %s", url[:60], exc)
        return False
    finally:
        await ctx.close()


# ── Main dispatcher ───────────────────────────────────────────────────────────

_AUTO_SOURCES   = {"rightmove", "zoopla", "onthemarket"}
_MANUAL_SOURCES = {"openrent"}


async def submit_enquiries(listings: list[dict]) -> dict[str, str]:
    """
    Submit enquiries for all given listings.

    Returns a dict mapping URL → status:
      "sent"    — enquiry form submitted successfully
      "failed"  — form submission failed (logged, won't retry today)
      "manual"  — OpenRent listing; user must contact manually
      "skipped" — already enquired previously
    """
    results: dict[str, str] = {}

    to_process = []
    for lst in listings:
        url = lst.get("url", "")
        if already_enquired(lst):
            results[url] = "skipped"
            logger.debug("[enquiry] Already enquired: %s", url[:60])
        else:
            to_process.append(lst)

    if not to_process:
        logger.info("[enquiry] All listings already enquired — nothing to do")
        return results

    # Mark OpenRent as manual immediately (no browser needed)
    auto_listings = []
    for lst in to_process:
        source = lst.get("source", "").lower()
        url    = lst.get("url", "")
        if source in _MANUAL_SOURCES:
            mark_enquired(lst, status="manual")
            results[url] = "manual"
        elif source in _AUTO_SOURCES:
            auto_listings.append(lst)
        else:
            results[url] = "skipped"

    if not auto_listings:
        return results

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

        for lst in auto_listings:
            source = lst.get("source", "").lower()
            url    = lst.get("url", "")

            try:
                if source == "rightmove":
                    ok = await _enquire_rightmove(browser, lst)
                elif source == "zoopla":
                    ok = await _enquire_zoopla(browser, lst)
                elif source == "onthemarket":
                    ok = await _enquire_otm(browser, lst)
                else:
                    ok = False

                status = "sent" if ok else "failed"
                mark_enquired(lst, status=status)
                results[url] = status

            except Exception as exc:
                logger.error("[enquiry] Unexpected error for %s: %s", url[:60], exc)
                mark_enquired(lst, status="failed")
                results[url] = "failed"

            await asyncio.sleep(3)   # polite gap between requests

        await browser.close()

    sent   = sum(1 for v in results.values() if v == "sent")
    failed = sum(1 for v in results.values() if v == "failed")
    manual = sum(1 for v in results.values() if v == "manual")
    skiped = sum(1 for v in results.values() if v == "skipped")
    logger.info(
        "[enquiry] Complete — sent: %d, failed: %d, manual: %d, skipped: %d",
        sent, failed, manual, skiped,
    )
    return results


def enquiry_summary(results: dict[str, str], listings: list[dict]) -> str:
    """
    Build a short Telegram summary message for the enquiry run.
    Also lists each OpenRent listing separately so Ernest can contact manually.
    """
    sent   = sum(1 for v in results.values() if v == "sent")
    failed = sum(1 for v in results.values() if v == "failed")
    manual = [
        lst for lst in listings
        if results.get(lst.get("url", "")) == "manual"
    ]

    lines = [
        "📨 *Enquiries*",
        f"• Auto\\-submitted: {sent}",
    ]
    if failed:
        lines.append(f"• Failed \\(portal blocked\\): {failed}")
    if manual:
        lines.append(f"• OpenRent \\(contact manually\\): {len(manual)}")
        lines.append("")
        for lst in manual:
            addr = lst.get("address") or lst.get("title") or "OpenRent listing"
            url  = lst.get("url", "")
            asking = lst.get("price_pcm") or lst.get("price", "")
            lines.append(
                f"📱 [{addr} — £{asking}/mo]({url})\n"
                f"_WhatsApp \\+6590673996 or call_"
            )

    return "\n".join(lines)
