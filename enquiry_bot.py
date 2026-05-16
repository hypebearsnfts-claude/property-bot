"""
enquiry_bot.py
--------------
Submits viewing enquiries for listings that passed all filters.

Login strategy per portal
─────────────────────────
  Rightmove    →  standard email / password login
  Zoopla       →  "Sign in with Google" (Gmail OAuth)
  OnTheMarket  →  "Sign in with Google" (Gmail OAuth)
  OpenRent     →  no automation; flagged in Telegram for manual contact

A single authenticated browser context is created per portal at the start
of the run and reused for every listing on that portal — one login, many
enquiries, minimal Google bot-detection risk.

Contact details
───────────────
  Name    : Ernest Siow
  Email   : ernest.slh@hotmail.com
  Phone   : +6590673996
  Message : see ENQUIRY_MESSAGE below

Credentials (from .env / GitHub Secrets)
─────────────────────────────────────────
  RIGHTMOVE_EMAIL / RIGHTMOVE_PASSWORD  — standard email login
  ZOOPLA_EMAIL    / ZOOPLA_PASSWORD     — Google account (gmail)
  OTM_EMAIL       / OTM_PASSWORD        — Google account (gmail)

Dedup
─────
  enquiry_log.json records every processed URL so the same listing is
  never enquired twice across daily runs.
"""

import asyncio
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path

from playwright.async_api import async_playwright, BrowserContext, Page, TimeoutError as PWTimeout

try:
    from playwright_stealth import stealth_async as _stealth_async
    _STEALTH = True
except ImportError:
    _STEALTH = False

logger = logging.getLogger(__name__)

# ── Contact details ───────────────────────────────────────────────────────────

CONTACT_FIRST   = "Ernest"
CONTACT_LAST    = "Siow"
CONTACT_NAME    = "Ernest Siow"
CONTACT_EMAIL   = "ernest.slh@hotmail.com"
CONTACT_PHONE   = "+6590673996"
ENQUIRY_MESSAGE = (
    "Hi there, I am very much interested in this property. "
    "If you may, could you whatsapp me at +6590673996 so I can respond quickly? "
    "Thank you in advance."
)

# ── Portal credentials (from .env / GitHub Secrets) ──────────────────────────

_RM_EMAIL   = os.getenv("RIGHTMOVE_EMAIL",    "")
_RM_PASS    = os.getenv("RIGHTMOVE_PASSWORD", "")
_ZO_EMAIL   = os.getenv("ZOOPLA_EMAIL",       "")
_ZO_PASS    = os.getenv("ZOOPLA_PASSWORD",    "")
_OTM_EMAIL  = os.getenv("OTM_EMAIL",          "")
_OTM_PASS   = os.getenv("OTM_PASSWORD",       "")

# ── Enquiry log ───────────────────────────────────────────────────────────────

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
            json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
        )
    except Exception as exc:
        logger.warning("[enquiry] Failed to save enquiry_log.json: %s", exc)


def already_enquired(listing: dict) -> bool:
    url = listing.get("url", "").strip()
    return bool(url) and url in _load_log()


def mark_enquired(listing: dict, status: str = "sent") -> None:
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


# ── Browser helpers ───────────────────────────────────────────────────────────

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


async def _new_page(ctx: BrowserContext) -> Page:
    page = await ctx.new_page()
    if _STEALTH:
        await _stealth_async(page)
    return page


async def _new_ctx(browser) -> BrowserContext:
    return await browser.new_context(
        user_agent=_UA,
        viewport={"width": 1280, "height": 900},
        locale="en-GB",
        extra_http_headers={"Accept-Language": "en-GB,en;q=0.9"},
    )


async def _dismiss_cookies(page: Page) -> None:
    for sel in [
        "button#onetrust-accept-btn-handler",
        "button:has-text('Accept all')",
        "button:has-text('Accept cookies')",
        "button#ccc-recommended-settings",
        "button:has-text('I agree')",
        "button:has-text('OK')",
        "[aria-label='Accept all']",
    ]:
        try:
            await page.locator(sel).first.click(timeout=2_500)
            await page.wait_for_timeout(400)
            return
        except Exception:
            continue


async def _safe_fill(page: Page, selectors: list[str], value: str,
                     timeout: int = 5_000) -> bool:
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            await loc.wait_for(state="visible", timeout=timeout)
            await loc.fill(value)
            return True
        except Exception:
            continue
    return False


async def _safe_click(page: Page, selectors: list[str],
                      timeout: int = 8_000) -> bool:
    for sel in selectors:
        try:
            await page.locator(sel).first.click(timeout=timeout)
            return True
        except Exception:
            continue
    return False


# ── Google OAuth helper ───────────────────────────────────────────────────────
# Called on the accounts.google.com popup page after clicking
# "Continue with Google" / "Sign in with Google" on a portal.

async def _google_oauth(google_page: Page, email: str, password: str) -> bool:
    """
    Complete a Google sign-in on the given popup/redirect page.
    Returns True if login appeared successful.
    """
    try:
        # Step 1: email
        await google_page.wait_for_load_state("domcontentloaded", timeout=15_000)
        await google_page.wait_for_timeout(1_500)

        email_filled = await _safe_fill(google_page, [
            "input[type='email']",
            "input[name='identifier']",
            "#identifierId",
        ], email, timeout=10_000)

        if not email_filled:
            logger.warning("[enquiry] Google OAuth: email field not found")
            return False

        await _safe_click(google_page, [
            "#identifierNext",
            "button:has-text('Next')",
            "[data-idom-class*='next' i]",
        ])
        await google_page.wait_for_timeout(2_500)

        # Step 2: password
        pwd_filled = await _safe_fill(google_page, [
            "input[type='password']",
            "input[name='Passwd']",
        ], password, timeout=10_000)

        if not pwd_filled:
            logger.warning("[enquiry] Google OAuth: password field not found")
            return False

        await _safe_click(google_page, [
            "#passwordNext",
            "button:has-text('Next')",
        ])
        await google_page.wait_for_timeout(3_000)

        # Step 3: handle "Allow" / permissions screen if shown
        try:
            await _safe_click(google_page, [
                "button:has-text('Allow')",
                "button:has-text('Continue')",
                "[data-action='consent']",
            ], timeout=4_000)
            await google_page.wait_for_timeout(2_000)
        except Exception:
            pass  # Not always shown

        # Check for errors (wrong password, verification required, etc.)
        content = (await google_page.content()).lower()
        if any(m in content for m in [
            "wrong password", "couldn't find your google account",
            "verify it's you", "confirm your recovery",
            "unusual activity", "this browser or app may not be secure",
        ]):
            logger.warning("[enquiry] Google OAuth: blocked or wrong credentials")
            return False

        logger.info("[enquiry] Google OAuth: completed successfully")
        return True

    except Exception as exc:
        logger.warning("[enquiry] Google OAuth exception: %s", exc)
        return False


# ── Rightmove login ───────────────────────────────────────────────────────────

async def _build_rightmove_ctx(browser) -> tuple[BrowserContext | None, bool]:
    """
    Create an authenticated Rightmove browser context.
    Returns (ctx, True) on success, (None, False) on failure.
    """
    if not (_RM_EMAIL and _RM_PASS):
        logger.info("[enquiry] Rightmove: no credentials in env")
        return None, False

    ctx = await _new_ctx(browser)
    page = await _new_page(ctx)
    try:
        await page.goto(
            "https://www.rightmove.co.uk/user/login.html",
            wait_until="domcontentloaded", timeout=30_000,
        )
        await page.wait_for_timeout(2_000)
        await _dismiss_cookies(page)

        await _safe_fill(page, [
            "input[name='email']", "input[type='email']",
            "input[id*='email' i]",
        ], _RM_EMAIL)
        await _safe_fill(page, [
            "input[name='password']", "input[type='password']",
        ], _RM_PASS)

        await _safe_click(page, [
            "button[type='submit']",
            "button:has-text('Log in')",
            "button:has-text('Sign in')",
        ])
        await page.wait_for_timeout(3_500)

        content = (await page.content()).lower()
        if "log out" in content or "my rightmove" in content or "saved properties" in content:
            logger.info("[enquiry] Rightmove: logged in ✓")
            await page.close()
            return ctx, True

        logger.warning("[enquiry] Rightmove: login may have failed (no logout link found)")
        await page.close()
        return ctx, False  # keep ctx, attempt anyway

    except Exception as exc:
        logger.warning("[enquiry] Rightmove login exception: %s", exc)
        try:
            await page.close()
        except Exception:
            pass
        return ctx, False


# ── Zoopla login (Google OAuth) ───────────────────────────────────────────────

async def _build_zoopla_ctx(browser) -> tuple[BrowserContext | None, bool]:
    if not (_ZO_EMAIL and _ZO_PASS):
        logger.info("[enquiry] Zoopla: no credentials in env")
        return None, False

    ctx = await _new_ctx(browser)
    page = await _new_page(ctx)
    try:
        await page.goto(
            "https://www.zoopla.co.uk/login/",
            wait_until="domcontentloaded", timeout=30_000,
        )
        await page.wait_for_timeout(2_000)
        await _dismiss_cookies(page)

        # Click "Continue with Google"
        google_btn_clicked = False
        async with page.expect_popup(timeout=12_000) as popup_info:
            clicked = await _safe_click(page, [
                "button:has-text('Continue with Google')",
                "button:has-text('Sign in with Google')",
                "a:has-text('Continue with Google')",
                "[data-testid*='google' i]",
            ], timeout=10_000)
            if not clicked:
                logger.warning("[enquiry] Zoopla: Google button not found")
                await page.close()
                return None, False
            google_btn_clicked = True

        if google_btn_clicked:
            google_page = await popup_info.value
            oauth_ok = await _google_oauth(google_page, _ZO_EMAIL, _ZO_PASS)
            # Popup closes automatically after successful login
            try:
                await google_page.wait_for_close(timeout=15_000)
            except Exception:
                pass
            await page.wait_for_timeout(3_000)

            if not oauth_ok:
                await page.close()
                return None, False

        content = (await page.content()).lower()
        logged_in = (
            "sign out" in content or "log out" in content
            or "my profile" in content or "saved properties" in content
        )
        logger.info("[enquiry] Zoopla: login %s", "✓" if logged_in else "uncertain")
        await page.close()
        return ctx, logged_in

    except Exception as exc:
        logger.warning("[enquiry] Zoopla login exception: %s", exc)
        try:
            await page.close()
        except Exception:
            pass
        return None, False


# ── OnTheMarket login (Google OAuth) ─────────────────────────────────────────

async def _build_otm_ctx(browser) -> tuple[BrowserContext | None, bool]:
    if not (_OTM_EMAIL and _OTM_PASS):
        logger.info("[enquiry] OTM: no credentials in env")
        return None, False

    ctx = await _new_ctx(browser)
    page = await _new_page(ctx)
    try:
        await page.goto(
            "https://www.onthemarket.com/accounts/login/",
            wait_until="domcontentloaded", timeout=30_000,
        )
        await page.wait_for_timeout(2_000)
        await _dismiss_cookies(page)

        # Click "Sign in with Google"
        google_btn_found = False
        try:
            async with page.expect_popup(timeout=12_000) as popup_info:
                clicked = await _safe_click(page, [
                    "button:has-text('Sign in with Google')",
                    "button:has-text('Continue with Google')",
                    "a:has-text('Sign in with Google')",
                    "[data-provider='google']",
                    "[class*='google' i]",
                ], timeout=10_000)
                if not clicked:
                    raise Exception("Google button not found")
                google_btn_found = True

            google_page = await popup_info.value
            oauth_ok = await _google_oauth(google_page, _OTM_EMAIL, _OTM_PASS)
            try:
                await google_page.wait_for_close(timeout=15_000)
            except Exception:
                pass
            await page.wait_for_timeout(3_000)

            if not oauth_ok:
                await page.close()
                return None, False

        except Exception:
            if not google_btn_found:
                # Fallback: try standard email/password on OTM (some accounts use this)
                logger.info("[enquiry] OTM: trying standard email login")
                await _safe_fill(page, [
                    "input[type='email']", "input[name='email']",
                    "input[name='username']",
                ], _OTM_EMAIL)
                await _safe_fill(page, [
                    "input[type='password']", "input[name='password']",
                ], _OTM_PASS)
                await _safe_click(page, [
                    "button[type='submit']",
                    "button:has-text('Sign in')",
                    "button:has-text('Log in')",
                ])
                await page.wait_for_timeout(3_000)

        content = (await page.content()).lower()
        logged_in = (
            "sign out" in content or "log out" in content
            or "my account" in content or "saved searches" in content
        )
        logger.info("[enquiry] OTM: login %s", "✓" if logged_in else "uncertain")
        await page.close()
        return ctx, logged_in

    except Exception as exc:
        logger.warning("[enquiry] OTM login exception: %s", exc)
        try:
            await page.close()
        except Exception:
            pass
        return None, False


# ── Per-listing enquiry submitters ────────────────────────────────────────────

async def _submit_rightmove(ctx: BrowserContext, listing: dict) -> str:
    """
    Navigate directly to Rightmove's contact form page and submit.

    When the shared context is authenticated (login succeeded), Rightmove
    pre-fills the user's name / email / phone — we just add a message and
    click 'Send email'.  If the context is a guest session the same form
    shows plain text inputs which we fill manually.

    The contact form lives at a separate URL:
      /property-to-rent/contactBranch.html?propertyId=<id>
    We construct that URL from the listing URL so we never need to navigate
    to the listing page itself (avoids the extra round-trip + click).
    """
    page = await _new_page(ctx)
    url  = listing["url"]
    try:
        # Extract numeric property ID from URL  (e.g. /properties/88493235)
        m = re.search(r'/properties/(\d+)', url)
        if not m:
            logger.warning("[enquiry] Rightmove: cannot extract property ID from %s", url[:60])
            return "failed"
        prop_id     = m.group(1)
        contact_url = (
            "https://www.rightmove.co.uk/property-to-rent/contactBranch.html"
            f"?propertyId={prop_id}&backToPropertyURL=%2Fproperties%2F{prop_id}"
        )

        await page.goto(contact_url, wait_until="domcontentloaded", timeout=30_000)
        await page.wait_for_timeout(2_500)
        await _dismiss_cookies(page)

        # Confirm we're on the contact form (not a 404 / redirect to login)
        page_url = page.url
        if "contactBranch" not in page_url and "contact" not in page_url:
            logger.warning("[enquiry] Rightmove: redirected away from contact form for %s → %s",
                           prop_id, page_url[:80])
            return "failed"

        # ── Personal details (guest form only; pre-filled when logged in) ──
        await _safe_fill(page, [
            "input[name='firstName']", "input[id*='firstName' i]",
            "input[placeholder*='first name' i]",
        ], CONTACT_FIRST)
        await _safe_fill(page, [
            "input[name='lastName']", "input[id*='lastName' i]",
            "input[placeholder*='last name' i]",
        ], CONTACT_LAST)
        await _safe_fill(page, [
            "input[name='email']", "input[type='email']", "input[id*='email' i]",
        ], CONTACT_EMAIL)
        await _safe_fill(page, [
            "input[name='phone']", "input[type='tel']",
            "input[name='telephone']", "input[id*='phone' i]",
        ], CONTACT_PHONE)

        # ── Message textarea (optional on RM, but we include our WhatsApp ask) ──
        await _safe_fill(page, [
            "textarea[placeholder*='work' i]",
            "textarea[placeholder*='viewing' i]",
            "textarea",
        ], ENQUIRY_MESSAGE)

        # ── Submit — Rightmove's button says "Send email" ──
        clicked = await _safe_click(page, [
            "button:has-text('Send email')",
            "button:has-text('Send enquiry')",
            "button:has-text('Send message')",
            "button[type='submit']",
        ])
        if not clicked:
            logger.warning("[enquiry] Rightmove: submit button not found for property %s", prop_id)
            return "failed"

        await page.wait_for_timeout(4_000)

        # ── Confirm success ──
        final = (await page.content()).lower()
        if any(s in final for s in [
            "thank you", "enquiry sent", "email has been sent",
            "message sent", "successfully sent", "we'll be in touch",
        ]):
            logger.info("[enquiry] ✅ Rightmove submitted: %s", url[:80])
            return "sent"

        # If page redirected away from the form, treat as sent
        if "contactBranch" not in page.url:
            logger.info("[enquiry] ✅ Rightmove submitted (redirect): %s", url[:80])
            return "sent"

        # Explicit error on page → failed
        if any(e in final for e in ["error", "please fill", "required field", "captcha problem"]):
            logger.warning("[enquiry] Rightmove: submission error detected for %s", prop_id)
            return "failed"

        # No confirmation but no obvious error — assume sent
        logger.info("[enquiry] ✅ Rightmove submitted (assumed): %s", url[:80])
        return "sent"

    except Exception as exc:
        logger.warning("[enquiry] Rightmove submit failed (%s): %s", url[:60], exc)
        return "failed"
    finally:
        await page.close()


async def _submit_zoopla(ctx: BrowserContext, listing: dict) -> str:
    page = await _new_page(ctx)
    url = listing["url"]
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        await page.wait_for_timeout(3_000)
        await _dismiss_cookies(page)

        # Open enquiry form
        await _safe_click(page, [
            "[data-testid='enquiry-button']",
            "button:has-text('Enquire')",
            "button:has-text('Get in touch')",
            "button:has-text('Email agent')",
            "button:has-text('Contact agent')",
            "a:has-text('Enquire')",
        ])
        await page.wait_for_timeout(1_500)

        await _safe_fill(page, [
            "input[name='firstName']", "input[id*='firstName' i]",
            "input[placeholder*='first name' i]",
        ], CONTACT_FIRST)
        await _safe_fill(page, [
            "input[name='lastName']", "input[id*='lastName' i]",
            "input[placeholder*='last name' i]",
        ], CONTACT_LAST)
        await _safe_fill(page, [
            "input[type='email']", "input[name='email']",
        ], CONTACT_EMAIL)
        await _safe_fill(page, [
            "input[type='tel']", "input[name='phone']",
            "input[name='telephone']",
        ], CONTACT_PHONE)
        await _safe_fill(page, ["textarea"], ENQUIRY_MESSAGE)

        clicked = await _safe_click(page, [
            "button:has-text('Send message')",
            "button:has-text('Send enquiry')",
            "button[type='submit']",
        ])
        if not clicked:
            return "failed"

        await page.wait_for_timeout(3_000)
        logger.info("[enquiry] ✅ Zoopla submitted: %s", url[:80])
        return "sent"

    except Exception as exc:
        logger.warning("[enquiry] Zoopla submit failed (%s): %s", url[:60], exc)
        return "failed"
    finally:
        await page.close()


async def _submit_otm(ctx: BrowserContext, listing: dict) -> str:
    """
    OTM contact form — no login required.

    The contact form lives at a separate URL:
      /agents/contact/<property_id>/?form-name=details-contact

    Form fields (confirmed from live page inspection):
      • radio  "to view a property" / "more details"
      • input  type=text   — full name
      • input  type=email  — email address
      • input  type=tel    — phone number
      • select             — country (defaults to UK, leave as-is)
      • textarea           — message (optional, placeholder contains "useful details")
      • button type=submit text="Submit"
    """
    page = await _new_page(ctx)
    url  = listing["url"]
    try:
        # Extract numeric property ID from OTM URL  (e.g. /details/15716375/)
        m = re.search(r'/details/(\d+)', url)
        if not m:
            logger.warning("[enquiry] OTM: cannot extract property ID from %s", url[:60])
            return "failed"
        prop_id     = m.group(1)
        contact_url = (
            f"https://www.onthemarket.com/agents/contact/{prop_id}/"
            "?form-name=details-contact"
        )

        await page.goto(contact_url, wait_until="domcontentloaded", timeout=35_000)
        await page.wait_for_timeout(2_500)
        await _dismiss_cookies(page)

        # Confirm we landed on the form (not a redirect or error page)
        if "agents/contact" not in page.url:
            logger.warning("[enquiry] OTM: redirected away from contact form for %s → %s",
                           prop_id, page.url[:80])
            return "failed"

        # ── Intent radio button (prefer "more details") ──
        await _safe_click(page, [
            "input[type='radio'][value*='more' i]",
            "label:has-text('more details') input",
            "label:has-text('more details')",
        ], timeout=3_000)

        # ── Full name (single text input — not split first/last on OTM) ──
        name_ok = await _safe_fill(page, [
            "input[type='text']",
            "input[name='name']", "input[name='full_name']",
            "input[id*='name' i]", "input[placeholder*='name' i]",
        ], CONTACT_NAME)
        if not name_ok:
            logger.warning("[enquiry] OTM: name field not found at %s", url[:60])

        # ── Email ──
        email_ok = await _safe_fill(page, [
            "input[type='email']", "input[name='email']", "input[id*='email' i]",
        ], CONTACT_EMAIL)
        if not email_ok:
            logger.warning("[enquiry] OTM: email field not found at %s", url[:60])
            return "failed"

        # ── Phone ──
        await _safe_fill(page, [
            "input[type='tel']", "input[name='phone']",
            "input[name='telephone']", "input[id*='phone' i]",
        ], CONTACT_PHONE)

        # ── Message (optional on OTM, placeholder contains "useful details") ──
        await _safe_fill(page, [
            "textarea[placeholder*='useful' i]",
            "textarea[placeholder*='detail' i]",
            "textarea",
        ], ENQUIRY_MESSAGE)

        # ── Submit — OTM's button text is "Submit" ──
        clicked = await _safe_click(page, [
            "button:has-text('Submit')",
            "button[type='submit']",
            "input[type='submit']",
        ])
        if not clicked:
            logger.warning("[enquiry] OTM: submit button not found for property %s", prop_id)
            return "failed"

        await page.wait_for_timeout(4_000)

        # ── Confirm success ──
        final = (await page.content()).lower()
        if any(s in final for s in [
            "thank you", "enquiry sent", "message sent",
            "successfully sent", "we've received", "we will be in touch",
        ]):
            logger.info("[enquiry] ✅ OTM submitted: %s", url[:80])
            return "sent"

        # Redirect away from form → likely success
        if "agents/contact" not in page.url:
            logger.info("[enquiry] ✅ OTM submitted (redirect): %s", url[:80])
            return "sent"

        if any(e in final for e in ["error", "please fill", "required", "invalid"]):
            logger.warning("[enquiry] OTM: submission error detected for %s", prop_id)
            return "failed"

        # No clear confirmation but no error either
        logger.info("[enquiry] ✅ OTM submitted (assumed): %s", url[:80])
        return "sent"

    except Exception as exc:
        logger.warning("[enquiry] OTM submit failed (%s): %s", url[:60], exc)
        return "failed"
    finally:
        await page.close()


# ── Main dispatcher ───────────────────────────────────────────────────────────

# Sources handled manually (no automated enquiry attempted)
_MANUAL_SOURCES = {"openrent"}

# Zoopla requires Google OAuth which is blocked in headless CI — treat as manual
_LOGIN_REQUIRED_SOURCES = {"zoopla"}


async def submit_enquiries(listings: list[dict]) -> dict:
    """
    Submit enquiries for all new listings.

    Portal strategy (as of 2026):
      • Rightmove    — navigate directly to contactBranch.html; login attempted
                       (pre-fills details); guest form also works.
      • OnTheMarket  — navigate directly to /agents/contact/{id}/; no login needed.
      • Zoopla       — Google OAuth only; blocked in headless CI → manual.
      • OpenRent     — direct landlord contact; flagged for manual follow-up.

    Returns dict mapping URL → result dict:
      {
        "status":  "sent" | "failed" | "login_required" | "manual" | "skipped",
        "area":    str,
        "price":   str,
        "address": str,
      }
    """
    results: dict[str, dict] = {}

    # Split into already-done vs to-process
    to_process = []
    for lst in listings:
        url = lst.get("url", "")
        if already_enquired(lst):
            results[url] = {"status": "skipped"}
        else:
            to_process.append(lst)

    if not to_process:
        logger.info("[enquiry] All listings already processed")
        return results

    # Handle manual / login-required sources immediately (no browser needed)
    auto = []
    for lst in to_process:
        source = lst.get("source", "").lower()
        url    = lst.get("url", "")
        if source in _MANUAL_SOURCES:
            mark_enquired(lst, status="manual")
            results[url] = {
                "status":  "manual",
                "area":    lst.get("area", ""),
                "price":   lst.get("price", ""),
                "address": lst.get("address", ""),
            }
        elif source in _LOGIN_REQUIRED_SOURCES:
            mark_enquired(lst, status="login_required")
            results[url] = {
                "status":  "login_required",
                "area":    lst.get("area", ""),
                "price":   lst.get("price", ""),
                "address": lst.get("address", ""),
            }
        else:
            auto.append(lst)

    if not auto:
        return results

    # Group by source
    by_source: dict[str, list[dict]] = {}
    for lst in auto:
        src = lst.get("source", "").lower()
        by_source.setdefault(src, []).append(lst)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox", "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )

        # Rightmove: build one authenticated context (pre-fills contact details)
        # OTM: uses a fresh anonymous context per listing (guest form, no login)
        rm_ctx: BrowserContext | None = None
        if "rightmove" in by_source:
            rm_ctx, _ = await _build_rightmove_ctx(browser)

        # Shared anonymous context for OTM (avoids re-launching browser per listing)
        otm_ctx: BrowserContext | None = None
        if "onthemarket" in by_source:
            otm_ctx = await _new_ctx(browser)

        # Submit per listing
        for source, listings_grp in by_source.items():
            if source == "rightmove":
                ctx = rm_ctx
            elif source == "onthemarket":
                ctx = otm_ctx
            else:
                ctx = None

            if ctx is None:
                for lst in listings_grp:
                    url = lst.get("url", "")
                    mark_enquired(lst, status="login_required")
                    results[url] = {
                        "status":  "login_required",
                        "area":    lst.get("area", ""),
                        "price":   lst.get("price", ""),
                        "address": lst.get("address", ""),
                    }
                continue

            for lst in listings_grp:
                url = lst.get("url", "")
                try:
                    if source == "rightmove":
                        status = await _submit_rightmove(ctx, lst)
                    elif source == "onthemarket":
                        status = await _submit_otm(ctx, lst)
                    else:
                        status = "failed"

                    mark_enquired(lst, status=status)
                    results[url] = {
                        "status":  status,
                        "area":    lst.get("area", ""),
                        "price":   lst.get("price", ""),
                        "address": lst.get("address", ""),
                    }
                except Exception as exc:
                    logger.error("[enquiry] Unexpected error for %s: %s", url[:60], exc)
                    mark_enquired(lst, status="failed")
                    results[url] = {
                        "status":  "failed",
                        "area":    lst.get("area", ""),
                        "price":   lst.get("price", ""),
                        "address": lst.get("address", ""),
                    }

                await asyncio.sleep(2)

        # Clean up contexts
        for ctx in [rm_ctx, otm_ctx]:
            if ctx:
                try:
                    await ctx.close()
                except Exception:
                    pass

        await browser.close()

    sent  = sum(1 for v in results.values() if v.get("status") == "sent")
    fail  = sum(1 for v in results.values() if v.get("status") == "failed")
    login = sum(1 for v in results.values() if v.get("status") == "login_required")
    man   = sum(1 for v in results.values() if v.get("status") == "manual")
    logger.info(
        "[enquiry] Done — sent: %d, failed: %d, login_required: %d, manual: %d",
        sent, fail, login, man,
    )
    return results


# ── Telegram summary builder ──────────────────────────────────────────────────

def _esc(text: str) -> str:
    special = r'\_*[]()~`>#+=|{}.!-'
    return "".join(f"\\{c}" if c in special else c for c in str(text))


def enquiry_summary(results: dict, listings: list[dict]) -> str:
    """Build a compact MarkdownV2 Telegram message summarising enquiry results."""
    sent      = [(u, v) for u, v in results.items() if v.get("status") == "sent"]
    failed    = [(u, v) for u, v in results.items() if v.get("status") == "failed"]
    login_req = [(u, v) for u, v in results.items() if v.get("status") == "login_required"]
    manual    = [(u, v) for u, v in results.items() if v.get("status") == "manual"]

    lines = ["📨 *Enquiries*"]

    if sent:
        lines.append(f"\n✅ *Submitted \\({len(sent)}\\)*")
        for url, v in sent:
            lines.append(f"  • [{_esc(v.get('area',''))} — {_esc(v.get('price',''))}]({url})")

    if failed:
        lines.append(f"\n❌ *Failed \\({len(failed)}\\)*")
        for url, v in failed:
            lines.append(f"  • [{_esc(v.get('area',''))} — {_esc(v.get('price',''))}]({url})")

    if login_req:
        lines.append(f"\n🔐 *Zoopla — enquire manually \\({len(login_req)}\\)*")
        for url, v in login_req:
            lines.append(f"  • [{_esc(v.get('area',''))} — {_esc(v.get('price',''))}]({url})")

    if manual:
        lines.append(f"\n📱 *OpenRent — send your own message \\({len(manual)}\\)*")
        for url, v in manual:
            lines.append(f"  • [{_esc(v.get('area',''))} — {_esc(v.get('price',''))}]({url})")

    if not (sent or failed or login_req or manual):
        return "📨 *No new enquiries to process\\.*"

    return "\n".join(lines)
