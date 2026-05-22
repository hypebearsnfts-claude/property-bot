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

_RM_EMAIL    = os.getenv("RIGHTMOVE_EMAIL",    "")
_RM_PASS     = os.getenv("RIGHTMOVE_PASSWORD", "")
_RM_COOKIES  = os.getenv("RIGHTMOVE_COOKIES",  "")   # JSON cookie array — preferred auth method
_OTM_EMAIL   = os.getenv("OTM_EMAIL",          "")
_OTM_PASS    = os.getenv("OTM_PASSWORD",       "")

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
    if not url:
        return False
    entry = _load_log().get(url)
    if not entry:
        return False
    # Only skip listings that were successfully sent or manually handled.
    # Re-attempt anything that previously failed or required login.
    return entry.get("status") in ("sent", "manual")


def mark_enquired(listing: dict, status: str = "sent") -> None:
    url = listing.get("url", "").strip()
    if not url:
        return
    log = _load_log()
    log[url] = {
        "date":    datetime.now().strftime("%Y-%m-%d"),
        "status":  status,
        "address": listing.get("address", ""),
        "source":  listing.get("source", ""),
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

    Strategy (in order of preference):
      1. Cookie-based auth  — load saved session cookies from RIGHTMOVE_COOKIES env var.
                              These come from a real Chrome login and bypass bot detection entirely.
      2. Email/password     — fallback headless login (may be blocked by Rightmove bot detection).
      3. Guest ctx          — no auth, attempt form as guest (limited functionality).

    Returns (ctx, True) on confirmed login, (ctx, False) if uncertain but worth trying,
    (None, False) only when there is no context at all.
    """
    ctx = await _new_ctx(browser)

    # ── Strategy 1: Cookie-based auth ────────────────────────────────────────
    if _RM_COOKIES:
        try:
            cookies = json.loads(_RM_COOKIES)
            await ctx.add_cookies(cookies)
            logger.info("[enquiry] Rightmove: loaded %d saved cookies", len(cookies))

            # Quick verification — visit account page and check for logout link
            page = await _new_page(ctx)
            try:
                await page.goto(
                    "https://www.rightmove.co.uk/user/login.html",
                    wait_until="load", timeout=25_000,
                )
                await page.wait_for_timeout(2_000)
                content = (await page.content()).lower()
                logged_in = any(kw in content for kw in [
                    "log out", "logout", "sign out", "my rightmove", "saved properties",
                ])
                await page.close()
                if logged_in:
                    logger.info("[enquiry] Rightmove: cookie auth ✓")
                    return ctx, True
                logger.warning("[enquiry] Rightmove: cookies loaded but session may have expired")
                return ctx, False  # Still worth trying — contact form may work
            except Exception:
                await page.close()
                return ctx, False
        except Exception as exc:
            logger.warning("[enquiry] Rightmove: failed to load cookies: %s", exc)
            # Fall through to password login

    # ── Strategy 2: Email/password login ─────────────────────────────────────
    if not (_RM_EMAIL and _RM_PASS):
        logger.info("[enquiry] Rightmove: no credentials — will attempt as guest")
        return ctx, False

    page = await _new_page(ctx)
    try:
        try:
            await page.goto(
                "https://www.rightmove.co.uk/user/login.html",
                wait_until="networkidle", timeout=35_000,
            )
        except Exception:
            await page.goto(
                "https://www.rightmove.co.uk/user/login.html",
                wait_until="load", timeout=30_000,
            )
        await page.wait_for_timeout(2_000)
        await _dismiss_cookies(page)
        await page.wait_for_timeout(500)

        email_ok = await _safe_fill(page, [
            "input[name='email']", "input[type='email']", "input[id*='email' i]",
        ], _RM_EMAIL)
        await _safe_fill(page, [
            "input[name='password']", "input[type='password']",
        ], _RM_PASS)

        if not email_ok:
            logger.warning("[enquiry] Rightmove: email input not found — bot detection likely")
            await page.close()
            return ctx, False

        await _safe_click(page, [
            "button[type='submit']",
            "button:has-text('Log in')",
            "button:has-text('Sign in')",
        ])
        await page.wait_for_timeout(4_000)
        try:
            await page.wait_for_load_state("networkidle", timeout=10_000)
        except Exception:
            pass

        content = (await page.content()).lower()
        logged_in = any(kw in content for kw in [
            "log out", "logout", "sign out", "my rightmove", "saved properties", "my account",
        ])
        if logged_in:
            logger.info("[enquiry] Rightmove: password login ✓")
        else:
            logger.warning("[enquiry] Rightmove: password login failed — will attempt as guest")
        await page.close()
        return ctx, logged_in

    except Exception as exc:
        logger.warning("[enquiry] Rightmove login exception: %s", exc)
        try:
            await page.close()
        except Exception:
            pass
        return ctx, False


# ── Zoopla login (Google OAuth) ───────────────────────────────────────────────

async def _build_zoopla_ctx(browser) -> tuple[BrowserContext, bool]:
    """
    Zoopla does not require login — 'Email agent' form is available to guests.
    Just return a plain browser context ready to go.
    """
    ctx = await _new_ctx(browser)
    logger.info("[enquiry] Zoopla: using guest context (no login needed)")
    return ctx, False


# ── OnTheMarket login (Google OAuth) ─────────────────────────────────────────

async def _build_otm_ctx(browser) -> tuple[BrowserContext | None, bool]:
    # OTM's /agents/contact/ form works without login — always return a valid ctx.
    # We attempt login only if credentials are present; failure is non-fatal.
    ctx = await _new_ctx(browser)

    if not (_OTM_EMAIL and _OTM_PASS):
        logger.info("[enquiry] OTM: no credentials — using guest context")
        return ctx, False  # ctx is valid; guest form will be used

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
                # OAuth failed but ctx is still valid — fall through to guest form
                logger.warning("[enquiry] OTM: OAuth failed — will use guest form")
                await page.close()
                return ctx, False

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
        return ctx, False  # ctx is valid; guest form will be used


# ── Per-listing enquiry submitters ────────────────────────────────────────────

async def _fill_and_submit_rm_form(page: Page, url: str) -> str:
    """
    Fill and submit the Rightmove contact form on whatever page is currently loaded.
    Handles both the guest (unauthenticated) and logged-in form variants.
    Returns "sent" on confirmed success, "failed" otherwise.
    """
    current_url = page.url
    logger.info("[enquiry] Rightmove: filling form at %s", current_url[:80])

    # Abort if on login page
    if "login" in current_url.lower() or "sign-in" in current_url.lower():
        logger.warning("[enquiry] Rightmove: on login page — aborting for %s", url[:60])
        return "failed"

    # Abort if the page body indicates login is required (wall without URL change)
    await page.wait_for_timeout(2_500)
    try:
        body_text = (await page.inner_text("body")).lower()
        login_wall_phrases = [
            "log in to contact", "sign in to contact", "login to contact",
            "please log in", "please sign in", "you need to be logged in",
            "create a free account", "register to contact",
        ]
        if any(p in body_text for p in login_wall_phrases):
            logger.info("[enquiry] Rightmove: login wall detected for %s", url[:60])
            return "failed"
        # Also abort if the property is no longer available
        unavail_phrases = [
            "no longer available", "property has been removed",
            "listing has been removed", "let agreed",
        ]
        if any(p in body_text for p in unavail_phrases):
            logger.info("[enquiry] Rightmove: property unavailable for %s", url[:60])
            return "failed"
    except Exception:
        pass

    # ── Guest fields (short timeout — skipped silently when logged in) ──────────
    await _safe_fill(page, [
        "input[name='firstName']", "input[id='firstName']",
        "input[placeholder*='first name' i]",
    ], CONTACT_FIRST, timeout=2_000)
    await _safe_fill(page, [
        "input[name='lastName']", "input[id='lastName']",
        "input[placeholder*='last name' i]",
    ], CONTACT_LAST, timeout=2_000)
    await _safe_fill(page, [
        "input[name='email']", "input[type='email']", "input[id='email']",
    ], CONTACT_EMAIL, timeout=2_000)
    await _safe_fill(page, [
        "input[name='telephone']", "input[name='phone']",
        "input[type='tel']", "input[id='telephone']",
    ], CONTACT_PHONE, timeout=2_000)

    # ── Required radio buttons ──────────────────────────────────────────────────
    await _safe_click(page, ["input[name='incomeSatisfactory'][value='YES']"],           timeout=5_000)
    await _safe_click(page, ["input[name='adverseCredit'][value='NO_ADVERSE_CREDIT']"],  timeout=5_000)

    # ── Message textarea ────────────────────────────────────────────────────────
    filled = await _safe_fill(page, [
        "#comments", "textarea[name='comments']", "textarea[id='comments']",
        "textarea[placeholder*='message' i]", "textarea[placeholder*='viewing' i]",
        "textarea[placeholder*='work' i]", "textarea",
    ], ENQUIRY_MESSAGE, timeout=7_000)
    if not filled:
        logger.warning("[enquiry] Rightmove: message textarea not found at %s", url[:60])
        return "failed"

    # ── Submit ──────────────────────────────────────────────────────────────────
    clicked = await _safe_click(page, [
        "button:has-text('Send email')",
        "button.dsrm_primary[type='submit']",
        "input[type='submit'][value*='Send' i]",
        "button[type='submit']",
    ])
    if not clicked:
        logger.warning("[enquiry] Rightmove: submit button not found at %s", url[:60])
        return "failed"

    # ── Verify success ──────────────────────────────────────────────────────────
    await page.wait_for_timeout(4_000)
    try:
        confirm_text = (await page.inner_text("body")).lower()
        success_phrases = [
            "thank you", "message sent", "enquiry sent", "email sent",
            "we'll be in touch", "agent has been notified", "your message has been sent",
            "message has been received", "confirmation",
        ]
        if any(p in confirm_text for p in success_phrases):
            logger.info("[enquiry] ✅ Rightmove confirmed sent: %s", url[:80])
            return "sent"
        # Check if form is gone (also indicates success on some Rightmove variants)
        form_still_present = await page.locator("textarea, input[name='firstName']").count()
        if form_still_present == 0:
            logger.info("[enquiry] ✅ Rightmove sent (form gone): %s", url[:80])
            return "sent"
        # Form still there with no success message — likely an error
        logger.warning("[enquiry] Rightmove: no success confirmation at %s", url[:60])
        return "failed"
    except Exception:
        # Can't read page — treat as sent (submit was clicked)
        logger.info("[enquiry] ✅ Rightmove submitted (unverified): %s", url[:80])
        return "sent"


async def _submit_rightmove(ctx: BrowserContext, listing: dict) -> str:
    page = await _new_page(ctx)
    url = listing["url"]
    try:
        m = re.search(r'/properties/(\d+)', url)
        if not m:
            logger.warning("[enquiry] Rightmove: cannot extract property ID from %s", url)
            return "failed"
        prop_id = m.group(1)

        contact_url = (
            "https://www.rightmove.co.uk/property-to-rent/contactBranch.html"
            f"?propertyId={prop_id}&backToPropertyURL=%2Fproperties%2F{prop_id}"
        )

        # ── Step 1: Navigate to listing page to set Referer header ───────────────
        # Rightmove requires a Referer from the listing page; going direct to
        # contactBranch.html without it redirects many agents to the login wall.
        listing_url = f"https://www.rightmove.co.uk/properties/{prop_id}"
        try:
            await page.goto(listing_url, wait_until="domcontentloaded", timeout=30_000)
        except Exception:
            try:
                await page.goto(listing_url, wait_until="load", timeout=25_000)
            except Exception:
                pass
        await page.wait_for_timeout(2_000)
        await _dismiss_cookies(page)
        await page.wait_for_timeout(500)

        # If the listing is gone (redirected away from /properties/ID), skip
        if prop_id not in page.url:
            logger.info("[enquiry] Rightmove: listing no longer available — %s", url[:60])
            return "failed"

        # ── Step 2: Click "Request details" / "Email agent" ──────────────────────
        # Simple click — no expect_navigation wrapper (causes 10s timeout on modals).
        # After clicking we inspect page.url to see what happened.
        contact_selectors = [
            "a[href*='contactBranch']",
            "button:has-text('Request details')",
            "a:has-text('Request details')",
            "button:has-text('Email agent')",
            "a:has-text('Email agent')",
            "button:has-text('Contact agent')",
            "a:has-text('Contact agent')",
            "[data-testid*='email-agent' i]",
            "[data-testid*='contact-agent' i]",
        ]
        # 2 s per selector — button is either visible right away or not there at all.
        # (8 s default × 9 selectors = 72 s wasted on expired/unavailable listings)
        await _safe_click(page, contact_selectors, timeout=2_000)

        # Give the page time to navigate or open a modal
        await page.wait_for_timeout(3_000)

        # ── Step 3: Decide where we ended up ─────────────────────────────────────
        cur = page.url
        if "login" in cur or "sign-in" in cur:
            # Still needs auth — try direct contact URL (may work as guest)
            logger.info("[enquiry] Rightmove: click led to login — trying direct URL")
            try:
                await page.goto(contact_url, wait_until="networkidle", timeout=35_000)
            except Exception:
                await page.goto(contact_url, wait_until="load", timeout=30_000)
            await page.wait_for_timeout(2_000)

        elif "contactBranch" not in cur:
            # Still on the listing page — either a modal opened or button wasn't found.
            # Check for visible form fields; if absent, go direct.
            has_form = False
            try:
                await page.locator("textarea, input[name='firstName']").first.wait_for(
                    state="visible", timeout=3_000
                )
                has_form = True
            except Exception:
                pass

            if not has_form:
                logger.info("[enquiry] Rightmove: no modal — navigating direct to contact form")
                try:
                    await page.goto(contact_url, wait_until="networkidle", timeout=35_000)
                except Exception:
                    try:
                        await page.goto(contact_url, wait_until="load", timeout=30_000)
                    except Exception:
                        await page.goto(contact_url, wait_until="domcontentloaded", timeout=25_000)
                await page.wait_for_timeout(2_000)

        # ── Step 4: Fill and submit ───────────────────────────────────────────────
        return await _fill_and_submit_rm_form(page, url)

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

        # Click "Email agent" / enquiry button to open the guest form
        clicked = await _safe_click(page, [
            "button:has-text('Email agent')",
            "a:has-text('Email agent')",
            "[data-testid='enquiry-button']",
            "button:has-text('Enquire')",
            "button:has-text('Get in touch')",
            "button:has-text('Contact agent')",
            "a:has-text('Enquire')",
        ])
        if not clicked:
            logger.warning("[enquiry] Zoopla: email agent button not found — %s", url[:60])
            return "failed"
        await page.wait_for_timeout(2_000)

        # Fill guest form fields
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

        msg_filled = await _safe_fill(page, ["textarea"], ENQUIRY_MESSAGE)
        if not msg_filled:
            logger.warning("[enquiry] Zoopla: message field not found — %s", url[:60])
            return "failed"

        clicked = await _safe_click(page, [
            "button:has-text('Send message')",
            "button:has-text('Send enquiry')",
            "button:has-text('Submit')",
            "button[type='submit']",
        ])
        if not clicked:
            logger.warning("[enquiry] Zoopla: submit button not found — %s", url[:60])
            return "failed"

        await page.wait_for_timeout(3_000)

        # Verify success
        confirm_text = (await page.inner_text("body")).lower()
        success_phrases = [
            "thank you", "message sent", "enquiry sent", "we'll be in touch",
            "agent has been notified", "your message has been sent",
        ]
        if any(p in confirm_text for p in success_phrases):
            logger.info("[enquiry] ✅ Zoopla confirmed sent: %s", url[:80])
            return "sent"

        # Form gone = also a success indicator
        if await page.locator("textarea").count() == 0:
            logger.info("[enquiry] ✅ Zoopla sent (form gone): %s", url[:80])
            return "sent"

        logger.warning("[enquiry] Zoopla: no success confirmation — %s", url[:60])
        return "failed"

    except Exception as exc:
        logger.warning("[enquiry] Zoopla submit failed (%s): %s", url[:60], exc)
        return "failed"
    finally:
        await page.close()


async def _submit_otm(ctx: BrowserContext, listing: dict) -> str:
    page = await _new_page(ctx)
    url = listing["url"]
    try:
        # Navigate directly to the contact form URL (no login required for OTM).
        m = re.search(r'/details/(\d+)', url)
        if not m:
            logger.warning("[enquiry] OTM: cannot extract property ID from %s", url)
            return "failed"
        prop_id = m.group(1)
        contact_url = f"https://www.onthemarket.com/agents/contact/{prop_id}/?form-name=details-contact"

        try:
            await page.goto(contact_url, wait_until="networkidle", timeout=40_000)
        except Exception:
            try:
                await page.goto(contact_url, wait_until="load", timeout=35_000)
            except Exception:
                await page.goto(contact_url, wait_until="domcontentloaded", timeout=30_000)

        await page.wait_for_timeout(2_500)
        await _dismiss_cookies(page)
        await page.wait_for_timeout(500)

        logger.info("[enquiry] OTM: contact page loaded — %s", page.url[:80])

        # OTM uses a single "Full name" field (id="name"), not split first/last.
        await _safe_fill(page, ["#name", "input[name='name']", "input[placeholder*='name' i]"], CONTACT_NAME)

        email_ok = await _safe_fill(page, [
            "#email", "input[type='email']", "input[name='email']",
        ], CONTACT_EMAIL)
        if not email_ok:
            logger.warning("[enquiry] OTM: email field not found at %s", url[:60])
            return "failed"

        await _safe_fill(page, [
            "#telephone", "input[type='tel']", "input[name='telephone']",
        ], CONTACT_PHONE)

        msg_filled = await _safe_fill(page, [
            "#message", "textarea[name='message']", "textarea",
        ], ENQUIRY_MESSAGE)
        if not msg_filled:
            logger.warning("[enquiry] OTM: message textarea not found at %s", url[:60])
            return "failed"

        # The submit button text is "Submit"
        clicked = await _safe_click(page, [
            "button:has-text('Submit')",
            "button[type='submit']",
        ])
        if not clicked:
            logger.warning("[enquiry] OTM: submit button not found at %s", url[:60])
            return "failed"

        await page.wait_for_timeout(3_000)
        logger.info("[enquiry] ✅ OTM submitted: %s", url[:80])
        return "sent"

    except Exception as exc:
        logger.warning("[enquiry] OTM submit failed (%s): %s", url[:60], exc)
        return "failed"
    finally:
        await page.close()


# ── Retry helper ─────────────────────────────────────────────────────────────

def get_failed_enquiry_listings() -> list[dict]:
    """
    Return minimal listing dicts for every previously-failed enquiry so they
    can be passed back to submit_enquiries() for retry.

    Only includes portals that support automated submission (Rightmove, OTM).
    Zoopla and OpenRent are excluded — they require manual contact.
    """
    log = _load_log()
    retry = []
    for url, entry in log.items():
        if entry.get("status") != "failed":
            continue
        if "rightmove.co.uk" in url:
            src = "rightmove"
        elif "onthemarket.com" in url:
            src = "onthemarket"
        else:
            continue  # Zoopla / OpenRent — cannot be automated
        retry.append({
            "url":     url,
            "source":  src,
            "address": entry.get("address", ""),
            "area":    "",
            "price":   "",
        })
    return retry


# ── Main dispatcher ───────────────────────────────────────────────────────────

_MANUAL_SOURCES = {"openrent"}


async def submit_enquiries(listings: list[dict]) -> dict:
    """
    Submit enquiries for all new listings.

    Returns dict mapping URL → result dict:
      {
        "status":  "sent" | "failed" | "login_required" | "manual" | "skipped",
        "area":    str,
        "price":   str,
        "address": str,
      }
    """
    results: dict[str, dict] = {}

    # Split listings
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

    # Mark OpenRent as manual immediately
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

        # Build one authenticated context per portal
        ctx_map: dict[str, BrowserContext | None] = {}

        if "rightmove" in by_source:
            ctx_map["rightmove"], _ = await _build_rightmove_ctx(browser)

        if "zoopla" in by_source:
            ctx_map["zoopla"], _ = await _build_zoopla_ctx(browser)

        if "onthemarket" in by_source:
            ctx_map["onthemarket"], _ = await _build_otm_ctx(browser)

        # Submit per listing using the authenticated context
        for source, listings_grp in by_source.items():
            ctx = ctx_map.get(source)

            if ctx is None:
                # No credentials / login failed — mark all as login_required
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
                    elif source == "zoopla":
                        status = await _submit_zoopla(ctx, lst)
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
                    results[url] = {"status": "failed", "area": lst.get("area", ""),
                                    "price": lst.get("price", ""), "address": ""}

                await asyncio.sleep(2)

        # Close all authenticated contexts
        for ctx in ctx_map.values():
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


def _listing_label(v: dict) -> str:
    """One-line display label for a listing result — falls back to address for retries."""
    area  = v.get("area", "").strip()
    price = v.get("price", "").strip()
    addr  = v.get("address", "").strip()
    if area and price:
        return f"{_esc(area)} — {_esc(price)}"
    if addr:
        return _esc(addr[:60])
    return "unknown"


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
            lines.append(f"  • [{_listing_label(v)}]({url})")

    if failed:
        lines.append(f"\n❌ *Failed \\({len(failed)}\\)*")
        for url, v in failed:
            lines.append(f"  • [{_listing_label(v)}]({url})")

    if login_req:
        lines.append(f"\n🔐 *Login failed — enquire manually \\({len(login_req)}\\)*")
        for url, v in login_req:
            lines.append(f"  • [{_listing_label(v)}]({url})")

    if manual:
        lines.append(f"\n📱 *OpenRent — send your own message \\({len(manual)}\\)*")
        for url, v in manual:
            lines.append(f"  • [{_listing_label(v)}]({url})")

    if not (sent or failed or login_req or manual):
        return "📨 *No new enquiries to process\\.*"

    return "\n".join(lines)
