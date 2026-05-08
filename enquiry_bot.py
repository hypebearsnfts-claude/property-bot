"""
enquiry_bot.py
--------------
Extracts agent phone numbers from passing listings and sends a compact
contact list to Telegram so Ernest can call / WhatsApp each agent directly.

Why not auto-submit portal forms?
  Rightmove, Zoopla, and OnTheMarket now all require a logged-in account
  before the enquiry form is shown. Without login the form is never rendered,
  so 100 % of automated form submissions fail. Extracting the phone number
  from the listing page does NOT require login and works reliably.

Flow per listing:
  1. Visit listing URL (fresh Playwright context, stealth mode)
  2. Extract agent phone via <a href="tel:..."> link or text pattern
  3. Extract agent name if possible
  4. OpenRent  →  mark as manual (phone in card, contact via WhatsApp)
  5. All others →  add to contact list

Output:
  enquiry_summary()  builds a Telegram MarkdownV2 message with one line
  per listing:  Area — Agent — 📱 phone number (tappable)

Dedup:
  enquiry_log.json  records which URLs have already been extracted so the
  same listing is not processed again across daily runs.
"""

import asyncio
import json
import logging
import re
from datetime import datetime
from pathlib import Path

from playwright.async_api import async_playwright, TimeoutError as PWTimeout

try:
    from playwright_stealth import stealth_async as _stealth_async
    _STEALTH = True
except ImportError:
    _STEALTH = False

logger = logging.getLogger(__name__)

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


def mark_enquired(listing: dict, status: str = "contact_sent",
                   phone: str = "") -> None:
    url = listing.get("url", "").strip()
    if not url:
        return
    log = _load_log()
    log[url] = {
        "date":    datetime.now().strftime("%Y-%m-%d"),
        "status":  status,
        "phone":   phone,
        "address": listing.get("address", ""),
    }
    _save_log(log)


# ── Browser helpers ───────────────────────────────────────────────────────────

async def _new_ctx(browser):
    ctx = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
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
    for sel in [
        "button#onetrust-accept-btn-handler",
        "button:has-text('Accept all')",
        "button:has-text('Accept cookies')",
        "button#ccc-recommended-settings",
        "button:has-text('I agree')",
    ]:
        try:
            await page.locator(sel).first.click(timeout=2_500)
            await page.wait_for_timeout(400)
            return
        except Exception:
            continue


# ── Phone extraction ──────────────────────────────────────────────────────────

_PHONE_JS = """
() => {
    // 1. Prefer explicit tel: links
    const telLinks = Array.from(document.querySelectorAll('a[href^="tel:"]'))
        .map(a => a.href.replace('tel:', '').replace(/\s/g, '').trim())
        .filter(t => t.length >= 7);
    if (telLinks.length > 0) return telLinks[0];

    // 2. Fallback: scan visible text for UK / SG phone patterns
    const text = document.body.innerText || '';
    const m = text.match(
        /(?:\+44[\s\-]?(?:\d[\s\-]?){9,10}|0\d{3,4}[\s\-]?\d{3,4}[\s\-]?\d{3,4})/
    );
    return m ? m[0].replace(/\s+/g, ' ').trim() : null;
}
"""

_AGENT_JS = """
() => {
    const selectors = [
        '[data-testid="agent-name"]', '[data-testid="brandName"]',
        '.agent-name', '.agency-name', '[class*="agentName"]',
        '[class*="AgentName"]', '[class*="brandName"]',
        '.propertyAgency', '.agency', '.branch-name',
    ];
    for (const sel of selectors) {
        const el = document.querySelector(sel);
        if (el) {
            const t = el.innerText.trim();
            if (t.length > 2 && t.length < 80) return t;
        }
    }
    return null;
}
"""


async def _extract_contact(browser, listing: dict) -> dict:
    """
    Visit listing page and return {phone, agent_name, url}.
    Returns phone=None if not found.
    """
    ctx, page = await _new_ctx(browser)
    url = listing["url"]
    result = {"phone": None, "agent_name": listing.get("agent", ""), "url": url}
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        await page.wait_for_timeout(2_500)
        await _dismiss_cookies(page)
        await page.wait_for_timeout(800)

        phone = await page.evaluate(_PHONE_JS)
        if phone:
            # Normalise spacing
            phone = re.sub(r"\s+", " ", phone).strip()
            result["phone"] = phone

        agent = await page.evaluate(_AGENT_JS)
        if agent:
            result["agent_name"] = agent

        logger.info(
            "[enquiry] %s → phone=%s agent=%s",
            url[:60], result["phone"] or "none", result["agent_name"] or "none",
        )
    except Exception as exc:
        logger.warning("[enquiry] Contact extraction failed for %s: %s", url[:60], exc)
    finally:
        await ctx.close()
    return result


# ── Main dispatcher ───────────────────────────────────────────────────────────

_MANUAL_SOURCES = {"openrent"}


async def submit_enquiries(listings: list[dict]) -> dict:
    """
    Extract agent contact details for all new listings.

    Returns dict mapping URL → contact info dict:
      {
        "status":     "contact_sent" | "no_phone" | "manual" | "skipped",
        "phone":      "020 XXXX XXXX" or None,
        "agent_name": "Knight Frank" or "",
        "address":    "...",
        "area":       "...",
        "price":      "...",
      }
    """
    results: dict[str, dict] = {}

    to_process = []
    for lst in listings:
        url = lst.get("url", "")
        if already_enquired(lst):
            results[url] = {"status": "skipped"}
            logger.debug("[enquiry] Already processed: %s", url[:60])
        else:
            to_process.append(lst)

    if not to_process:
        logger.info("[enquiry] All listings already processed")
        return results

    # Separate manual (OpenRent) from auto
    manual, auto = [], []
    for lst in to_process:
        if lst.get("source", "").lower() in _MANUAL_SOURCES:
            manual.append(lst)
        else:
            auto.append(lst)

    # Mark OpenRent as manual immediately
    for lst in manual:
        url = lst.get("url", "")
        mark_enquired(lst, status="manual", phone="")
        results[url] = {
            "status":     "manual",
            "phone":      None,
            "agent_name": lst.get("agent", ""),
            "address":    lst.get("address", ""),
            "area":       lst.get("area", ""),
            "price":      lst.get("price", ""),
        }

    if not auto:
        return results

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox", "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )

        for lst in auto:
            url = lst.get("url", "")
            try:
                info = await _extract_contact(browser, lst)
                status = "contact_sent" if info["phone"] else "no_phone"
                mark_enquired(lst, status=status, phone=info["phone"] or "")
                results[url] = {
                    "status":     status,
                    "phone":      info["phone"],
                    "agent_name": info["agent_name"],
                    "address":    lst.get("address", ""),
                    "area":       lst.get("area", ""),
                    "price":      lst.get("price", ""),
                }
            except Exception as exc:
                logger.error("[enquiry] Unexpected error for %s: %s", url[:60], exc)
                mark_enquired(lst, status="failed", phone="")
                results[url] = {"status": "failed"}

            await asyncio.sleep(2)

        await browser.close()

    found  = sum(1 for v in results.values() if v.get("status") == "contact_sent")
    no_ph  = sum(1 for v in results.values() if v.get("status") == "no_phone")
    manual_c = sum(1 for v in results.values() if v.get("status") == "manual")
    logger.info(
        "[enquiry] Done — phone found: %d, no phone: %d, manual: %d",
        found, no_ph, manual_c,
    )
    return results


# ── Telegram summary builder ──────────────────────────────────────────────────

def _esc(text: str) -> str:
    """Escape MarkdownV2 special characters."""
    special = r'\_*[]()~`>#+=|{}.!-'
    return "".join(f"\\{c}" if c in special else c for c in str(text))


def enquiry_summary(results: dict, listings: list[dict]) -> str:
    """
    Build a compact MarkdownV2 Telegram message listing agent phone numbers.
    Grouped into: phones found / no phone / OpenRent manual.
    """
    with_phone  = [(url, v) for url, v in results.items()
                   if v.get("status") == "contact_sent" and v.get("phone")]
    no_phone    = [(url, v) for url, v in results.items()
                   if v.get("status") == "no_phone"]
    manual      = [(url, v) for url, v in results.items()
                   if v.get("status") == "manual"]

    lines = ["📞 *Agent contacts*"]

    if with_phone:
        lines.append("")
        for url, v in with_phone:
            area    = _esc(v.get("area", ""))
            agent   = _esc(v.get("agent_name", "Agent") or "Agent")
            phone   = v.get("phone", "")
            price   = _esc(v.get("price", ""))
            # Format phone as tappable tel: link
            phone_clean = re.sub(r"[^\d+]", "", phone)
            lines.append(
                f"🏠 *{area}* — {price}\n"
                f"  {agent}\n"
                f"  📱 [{_esc(phone)}](tel:{phone_clean})"
            )

    if no_phone:
        lines.append("")
        lines.append("*No phone found — view listing directly:*")
        for url, v in no_phone:
            area  = _esc(v.get("area", ""))
            price = _esc(v.get("price", ""))
            lines.append(f"  • [{area} — {price}]({url})")

    if manual:
        lines.append("")
        lines.append("*OpenRent — contact manually:*")
        for url, v in manual:
            area  = _esc(v.get("area", ""))
            price = _esc(v.get("price", ""))
            lines.append(f"  • [{area} — {price}]({url})")

    if not (with_phone or no_phone or manual):
        return "📞 *No new agent contacts to process\\.*"

    return "\n".join(lines)
