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
import csv
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters as tg_filters

from utils.walk_time import nearest_walk_minutes
from utils.valuation import get_fmv_verdict, _parse_price_pcm
from utils.seen_listings import is_duplicate, mark_as_seen, clean_old_entries
from enquiry_bot import submit_enquiries, enquiry_summary, already_enquired, check_price_changes

# ── Config ────────────────────────────────────────────────────────────────────

load_dotenv()

TOKEN          = os.getenv("TELEGRAM_FILTER_BOT_TOKEN")
MAX_WALK_MINS  = int(os.getenv("MAX_WALK_MINS", "10"))
MAX_SEND       = int(os.getenv("MAX_LISTINGS_SEND", "0"))   # 0 = no limit
MAX_PRICE_PCM  = int(os.getenv("MAX_PRICE_PCM", "0"))       # 0 = no cap for 3+ bed listings
MAX_PRICE_2BED = int(os.getenv("MAX_PRICE_2BED", "5500"))   # hard ceiling for 2-bed listings

LISTINGS_PATH     = Path(__file__).parent / "listings.json"
AIRDNA_RATES_PATH = Path(__file__).parent / "airdna_rates.json"
# One CSV per run of the properties sent to Telegram (opens in Excel). The file is
# named passed_<date>_<HHMM>.csv (unique per run) and committed by the workflow.
# Columns: link, listed_by (agent/landlord — blank where the portal doesn't expose
# it), price, bedrooms, nearest_station (the station the AirDNA ADR is based on),
# required_nightly (the £/night we need to clear), airdna_adr (AirDNA average
# £/night for that station + bed count), margin (required − ADR, in £; negative =
# the nightly we need is BELOW the AirDNA average = more headroom). The three
# AirDNA columns are blank for the >£7,500 comparables route, which doesn't run
# the AirDNA check.
_PASSED_CSV_COLS  = ["link", "listed_by", "price", "bedrooms", "nearest_station",
                     "required_nightly", "airdna_adr", "margin"]
_RUN_STAMP        = None   # set lazily on first write → one file for the whole run


def _run_csv_path() -> Path:
    global _RUN_STAMP
    if _RUN_STAMP is None:
        _RUN_STAMP = datetime.now().strftime("%Y-%m-%d_%H%M")
    out_dir = Path(__file__).parent / "passed_runs"
    out_dir.mkdir(exist_ok=True)
    return out_dir / f"passed_{_RUN_STAMP}.csv"


def _log_passed_listing(listing: dict, verdict: dict) -> None:
    """Append one sent property to this run's passed_<timestamp>.csv (best-effort)."""
    try:
        req = verdict.get("str_required_nightly")
        adr = verdict.get("str_airdna_avg")
        margin = round(req - adr) if (req is not None and adr is not None) else ""
        row = {
            "link":             listing.get("url", ""),
            "listed_by":        listing.get("agent") or listing.get("landlord") or "",
            "price":            listing.get("price", ""),
            "bedrooms":         listing.get("beds", ""),
            "nearest_station":  listing.get("area") or listing.get("walk_station") or "",
            "required_nightly": round(req) if req is not None else "",
            "airdna_adr":       round(adr) if adr is not None else "",
            "margin":           margin,
        }
        path = _run_csv_path()
        is_new = not path.exists()
        with path.open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=_PASSED_CSV_COLS)
            if is_new:
                w.writeheader()
            w.writerow(row)
    except Exception as exc:
        logger.warning("[filter] Failed to write passed CSV: %s", exc)
STR_MAX_ABOVE_ADR = int(os.getenv("STR_MAX_ABOVE_ADR", "50"))  # £ tolerance above AirDNA ADR
# FMV routing by asking rent: at or below this, decide with the AirDNA STR check
# only (fast, no LLM). Above this, use the old comparables/LLM FMV method, where
# AirDNA's per-station ADR caps make STR viability unreliable.
FMV_OLD_METHOD_THRESHOLD = int(os.getenv("FMV_OLD_METHOD_THRESHOLD", "7500"))

# Sources that should normally return listings every run. If one of these hits
# zero it's almost certainly bot-blocked or a broken selector — worth an alert.
# (zoopla / openrent are chronically bot-blocked, so they're intentionally
# excluded here to avoid daily false alarms — they're still counted/reported.)
CORE_SOURCES = {s.strip() for s in os.getenv(
    "CORE_SOURCES", "rightmove,onthemarket").split(",") if s.strip()}
# Alert if the detail-page check blocks at least this fraction (and this many).
DETAIL_BLOCK_ALERT_FRACTION = float(os.getenv("DETAIL_BLOCK_ALERT_FRACTION", "0.8"))
DETAIL_BLOCK_ALERT_MIN      = int(os.getenv("DETAIL_BLOCK_ALERT_MIN", "10"))

# Populated by run_pipeline() each run; read by run_filter_pipeline_and_send()
# to emit health alerts without changing run_pipeline's return signature.
_LAST_RUN_DIAG: dict = {}


def _load_airdna_rates() -> dict:
    """Load AirDNA nightly rates by bedroom count from airdna_rates.json."""
    try:
        if AIRDNA_RATES_PATH.exists():
            return json.loads(AIRDNA_RATES_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("[str] Failed to load airdna_rates.json: %s", exc)
    return {}


def _get_airdna_avg(area: str, beds: int) -> Optional[int]:
    """
    Look up AirDNA average nightly rate (GBP, Entire Place) for a given station
    area and bedroom count.

    Lookup priority:
      1. by_station[area][beds]   — hyper-local 0.5-mile radius per station
      2. by_bedrooms[beds]        — London-wide fallback

    The daily Cowork task populates by_station with real per-station data.
    """
    rates    = _load_airdna_rates()
    beds_key = str(beds)

    # 1. Station-specific (0.5-mile radius — apple-to-apple comparison)
    station_data = rates.get("by_station", {}).get(area, {})
    val = station_data.get(beds_key)
    if val:
        return int(val)

    # 2. London-wide fallback
    val = rates.get("by_bedrooms", {}).get(beds_key)
    if val:
        return int(val)

    return None


# Property-type tokens that precede the bedroom count on portal cards.
_PROP_TYPES = (
    r"Flat|Apartment|Maisonette|Penthouse|House|Studio|Duplex|Mews|Cottage|"
    r"Bungalow|Town\s?house|Triplex|Terraced?|End of Terrace|Detached|"
    r"Semi-Detached|Link Detached|Bedsit|Serviced Apartment|Barn Conversion|"
    r"Not Specified|Ground Floor|Character Property"
)


def _infer_beds(listing: dict) -> Optional[int]:
    """Best-effort bedroom count from a listing's title/description.

    Rightmove (and similar) cards render as
        <address>\\n<property type>\\n<bedrooms>\\n<bathrooms>[\\n<distance>]
    — they do NOT contain the literal text 'N bed'. So the reliable signal is
    the first standalone 1–2 digit line in the title (the bedroom count), with a
    '<type> <N>' / 'N bed' regex on the description as a fallback. Returns None
    only when nothing usable is found.
    """
    title = listing.get("title") or ""
    desc  = listing.get("description") or ""
    # 1) First standalone integer line in the structured title = bedrooms.
    for part in title.split("\n"):
        p = part.strip()
        if re.fullmatch(r"\d{1,2}", p):
            return int(p)
    if re.search(r"\bstudio\b", title, re.I):
        return 0
    # 2) Description fallback: "<TYPE> <beds>" then explicit "N bed(room)".
    m = re.search(r"(?:%s)s?[\s\n]+(\d{1,2})\b" % _PROP_TYPES, desc, re.I)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d{1,2})\s*bed", desc, re.I)
    if m:
        return int(m.group(1))
    if re.search(r"\bstudio\b", desc, re.I):
        return 0
    return None


def _infer_agent(listing: dict) -> str:
    """Best-effort agency/landlord name from a listing's description.

    Rightmove cards don't expose the agent in a selectable element — it sits in
    the card text as 'Added/Reduced on DD/MM/YYYY by <AGENCY>, <branch>'. Pull
    that out so 'listed_by' isn't blank. Returns '' if nothing is found.
    """
    desc = listing.get("description") or ""
    if not desc:
        return ""
    m = re.search(r"(?:Added|Reduced)\s+on\s+\d{2}/\d{2}/\d{4}\s+by\s+([^\n]+)", desc, re.I)
    if m:
        return m.group(1).strip()
    # Fallback: "<agent>\n0xx xxx ..." (agent line immediately above a phone number)
    m = re.search(r"\bby\s+([^\n]+?)\s*\n\s*0\d[\d ]{6,}", desc)
    if m:
        return m.group(1).strip()
    return ""


def _str_not_viable(listing: dict) -> bool:
    """
    Return True if the listing's STR (Airbnb) potential doesn't cover the
    long-term rental cost — i.e. it would need to charge too much per night.

    Formula (Ernest's rule):
      required_nightly = asking_pcm × 1.5 ÷ 21
      airdna_avg       = AirDNA ADR for same station area + bed count
                         (Entire Place, 0.5-mile radius, GBP)
      Remove if: required_nightly > airdna_avg + £50

    AirDNA guest ranges: 1-bed=1-4 guests, 2-bed=1-6, 3-bed=1-8,
                         4-bed=1-10, 5-bed=1-12, 6-bed=1-14, 7-bed=1-16

    Returns False (keep listing) if AirDNA data is unavailable.
    """
    asking_pcm = _parse_price_pcm(listing.get("price", ""))
    beds       = listing.get("beds")
    area       = listing.get("area", "")
    if not asking_pcm or not beds:
        return False   # can't compute — keep listing

    airdna_avg = _get_airdna_avg(area, beds)
    if not airdna_avg:
        return False   # no AirDNA data — don't reject

    required_nightly = (asking_pcm * 1.5) / 21
    margin = required_nightly - airdna_avg

    if margin > STR_MAX_ABOVE_ADR:
        logger.info(
            "[str] STR not viable: %s (%s) | £%d/mo → £%.0f/night needed, "
            "AirDNA %d-bed avg £%d/night (over by £%.0f)",
            listing.get("address", "")[:35], area, asking_pcm,
            required_nightly, beds, airdna_avg, margin,
        )
        return True

    return False


def _airdna_str_verdict(listing: dict) -> Optional[dict]:
    """AirDNA STR-viability verdict with the numbers (for gating + messaging).

    Returns None if it can't be computed (missing price/beds or no AirDNA data),
    so the caller can fall back to the comparables FMV method.
    """
    asking_pcm = _parse_price_pcm(listing.get("price", "")) or listing.get("price_pcm")
    beds       = listing.get("beds")
    area       = listing.get("area", "")
    if not asking_pcm or not beds:
        return None
    airdna_avg = _get_airdna_avg(area, beds)
    if not airdna_avg:
        return None
    required_nightly = round((asking_pcm * 1.5) / 21)
    return {
        "viable":           (required_nightly - airdna_avg) <= STR_MAX_ABOVE_ADR,
        "asking_price":     asking_pcm,
        "required_nightly": required_nightly,
        "airdna_avg":       airdna_avg,
        "beds":             beds,
    }

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
# Suppress httpx request logs — they contain the full bot token in the URL
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ── Portal preference (cross-portal dedup) ───────────────────────────────────
# When the same physical property appears on multiple portals we keep only the
# highest-priority version so we don't end up with a Rightmove enquiry for a
# property that is already covered by OTM or Zoopla (whose forms are simpler).
#
# Priority: OnTheMarket > Zoopla > Rightmove > everything else.

_PORTAL_PRIORITY: list[str] = ["onthemarket", "zoopla", "rightmove"]


def _addr_tokens(addr: str) -> list[str]:
    """Normalise an address string into a list of meaningful lowercase tokens."""
    a = addr.lower()
    # Remove full UK postcodes  (e.g. "SW1A 2AA", "E1 6JG")
    a = re.sub(r'\b[a-z]{1,2}\d{1,2}[a-z]?\s+\d[a-z]{2}\b', '', a)
    # Remove "flat N" / "apartment 3B" etc.
    a = re.sub(r'\b(flat|apartment|apt|unit)\s*[\d\w]+\b', '', a)
    # Remove punctuation
    a = re.sub(r'[^\w\s]', ' ', a)
    _NOISE = {'the', 'a', 'of', 'and', 'in', 'at', 'london'}
    _NUM_RE = re.compile(r'^\d+[a-z]?$')
    # Keep numeric tokens regardless of length; drop short non-numeric noise words
    return [
        t for t in a.split()
        if t not in _NOISE and (_NUM_RE.match(t) or len(t) > 2)
    ]


def _address_match(addr1: str, addr2: str) -> bool:
    """
    Return True if two address strings almost certainly describe the same
    physical property.  Conservative — prefers false negatives over false
    positives so we never silently drop a valid listing.

    Rules
    ─────
    • If both addresses start with a street number → numbers must match, then
      at least one non-generic street token (not "street", "road", etc.) must
      match.
    • Otherwise (building-name addresses) → the first two significant tokens
      must match exactly (e.g. "sapphire court" == "sapphire court").
    """
    t1 = _addr_tokens(addr1)
    t2 = _addr_tokens(addr2)
    if not t1 or not t2:
        return False

    _NUM_RE = re.compile(r'^\d+[a-z]?$')
    # Generic suffixes that can't distinguish two different streets on their own
    _GENERIC = {
        'street', 'road', 'avenue', 'lane', 'place', 'close', 'court',
        'gardens', 'garden', 'square', 'terrace', 'way', 'drive', 'grove',
        'crescent', 'mews', 'walk', 'row', 'hill', 'park',
    }

    def get_num(tokens: list[str]) -> Optional[str]:
        for t in tokens:
            if _NUM_RE.match(t):
                return t
        return None

    num1 = get_num(t1)
    num2 = get_num(t2)

    if num1 and num2:
        # Both have street numbers — numbers must agree, then at least one
        # non-generic token (the actual street name) must also match.
        if num1 != num2:
            return False
        rest1 = {t for t in t1 if t != num1 and t not in _GENERIC}
        rest2 = {t for t in t2 if t != num2 and t not in _GENERIC}
        return bool(rest1 & rest2)

    else:
        # Building-name addresses — first two tokens must match
        # (e.g. "sapphire court" vs "kings court" — "court" alone is not enough)
        if len(t1) < 2 or len(t2) < 2:
            return False
        return t1[0] == t2[0] and t1[1] == t2[1]


def _dedupe_cross_portal(listings: list[dict]) -> list[dict]:
    """
    Group listings that appear to be the same physical property across portals
    and keep only the highest-priority portal version.

    Example: Rightmove + OTM versions of the same flat → keep OTM only.
    """
    def portal_rank(lst: dict) -> int:
        src = lst.get("source", "").lower()
        try:
            return _PORTAL_PRIORITY.index(src)
        except ValueError:
            return len(_PORTAL_PRIORITY)   # unknown portals ranked last

    kept: list[dict] = []
    used = [False] * len(listings)

    for i, l1 in enumerate(listings):
        if used[i]:
            continue
        group = [i]
        addr1 = l1.get("address", "")

        for j in range(i + 1, len(listings)):
            if used[j]:
                continue
            l2 = listings[j]
            # Only merge across portals — same portal/URL is handled by seen_listings
            if l1.get("source", "") == l2.get("source", ""):
                continue
            if _address_match(addr1, l2.get("address", "")):
                group.append(j)
                used[j] = True

        # Pick the highest-priority portal in the group
        best = min((listings[k] for k in group), key=portal_rank)
        kept.append(best)
        used[i] = True

        if len(group) > 1:
            others = [
                f"{listings[k].get('source', '?')} — {listings[k].get('address', '')[:40]}"
                for k in group if listings[k] is not best
            ]
            logger.info(
                "[filter] Cross-portal dedup: keeping %s (%s), dropping: %s",
                best.get("source"), best.get("address", "")[:40], " | ".join(others),
            )

    return kept


# ── Detail-page keyword checker (Playwright) ─────────────────────────────────
# Key features like "24 Hour Porter" / "24 Hr Concierge" only appear on the
# listing detail page — NOT on search results cards. Plain HTTP requests are
# blocked by OTM and Rightmove from GitHub Actions IPs. We use Playwright
# instead — a real browser context that renders the page exactly as a user
# would see it, making bot detection much harder.

_DETAIL_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def _has_unfurnished(t: str) -> bool:
    """True only if the text indicates an UNFURNISHED-only let.

    "furnished or unfurnished" (and slash/part variants) means a furnished
    option IS available, so those must NOT be blocked. `t` must be lowercase.
    """
    if "unfurnished" not in t:
        return False
    masked = (
        t.replace("furnished or unfurnished", " ")
         .replace("furnished/unfurnished", " ")
         .replace("furnished / unfurnished", " ")
         .replace("furnished or part furnished", " ")
         .replace("furnished or part-furnished", " ")
    )
    return "unfurnished" in masked


def _blocked_keyword(text: str) -> Optional[str]:
    """Detail-page deep check on the FULL rendered page text.

    Returns the matched term (for logging) or None.

    Deliberately NARROW: this scans the entire page (header nav, footer, and
    "similar properties" rails included), so it must only use terms that won't
    appear in site chrome. It exists to catch attributes hidden in the listing
    body that the card didn't expose: concierge, porter, and unfurnished-only.

    Do NOT add nav/category terms here (e.g. "student accommodation",
    "student property", agent names) — those appear in every portal's menu and
    would false-block 100% of listings. They are already handled at the card
    level in _has_blacklisted_keyword (which scans only the listing's own fields).
    """
    if not text:
        return None
    t = text.lower()
    if "concierge" in t:
        return "concierge"
    m = _WHOLE_WORD_RE.search(t)           # whole-word "porter"
    if m:
        return m.group(0)
    if _has_unfurnished(t):
        return "unfurnished"
    return None


def _text_has_blocked_keyword(text: str) -> bool:
    """Bool wrapper around _blocked_keyword (kept for callers/tests)."""
    return _blocked_keyword(text) is not None


async def _check_detail_pages_playwright(listings: list[dict]) -> list[bool]:
    """
    Visit each listing's detail page with Playwright and return a boolean list
    indicating which listings contain a blacklisted keyword.

    Uses 4 concurrent browser contexts for speed.  Falls back to False (don't
    block) on any individual fetch failure so a network blip never silently
    drops a good listing.

    Returns list[bool] — True = blocked, False = keep.
    """
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout

    results = [False] * len(listings)

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox",
                      "--disable-dev-shm-usage",
                      "--disable-blink-features=AutomationControlled"],
            )
            sem = asyncio.Semaphore(4)   # 4 concurrent pages

            async def _check_one(idx: int, url: str) -> None:
                async with sem:
                    ctx = await browser.new_context(
                        user_agent=_DETAIL_UA,
                        viewport={"width": 1280, "height": 900},
                        locale="en-GB",
                    )
                    page = await ctx.new_page()
                    try:
                        clean_url = url.split('#')[0]
                        await page.goto(clean_url, wait_until="domcontentloaded",
                                        timeout=20_000)
                        await page.wait_for_timeout(1_500)
                        # Get visible text — first 25k chars to skip footer nav
                        text = await page.evaluate(
                            "() => document.body.innerText.slice(0, 25000)"
                        )
                        kw = _blocked_keyword(text)
                        if kw:
                            results[idx] = True
                            logger.info("[filter] Detail check blocked (%r): %s", kw, url[:70])
                    except PWTimeout:
                        logger.debug("[filter] Detail page timeout: %s", url[:70])
                    except Exception as exc:
                        logger.debug("[filter] Detail page error (%s): %s", url[:60], exc)
                    finally:
                        await ctx.close()

            await asyncio.gather(
                *[_check_one(i, l.get("url", "")) for i, l in enumerate(listings)]
            )
            await browser.close()

    except Exception as exc:
        logger.warning("[filter] Detail-page Playwright check failed: %s", exc)
        # On total failure, return all False (don't block anything)

    return results


# ── Agent blacklist ───────────────────────────────────────────────────────────
# Listings from these agents are silently dropped before any other processing.
# Case-insensitive. Add/remove names here as needed.

BLACKLISTED_AGENTS = [
    "greater london properties",
    "foxtons",
    "savills",
    "chestertons",
    "dexters",
    "tavistock bow",
    "ila",
    "219baker",
    "219 baker",
    "blueground",
    "cbre",
    "glp",
]


def _is_blacklisted(listing: dict) -> bool:
    """Return True if the listing is from a blacklisted agent.
    Checks agent field + title + address + url + description so the agent
    name is caught even when the scraper fails to extract it cleanly.
    """
    haystack = " ".join([
        listing.get("agent", ""),
        listing.get("title", ""),
        listing.get("address", ""),
        listing.get("url", ""),
        listing.get("description", ""),
    ]).lower()
    return any(blocked in haystack for blocked in BLACKLISTED_AGENTS)


# ── Keyword blacklist ─────────────────────────────────────────────────────────
# Listings whose title, address, or description contain any of these phrases
# are silently dropped. Case-insensitive. Add/remove phrases here as needed.

BLACKLISTED_KEYWORDS = [
    # Any concierge service — "concierge" is safe as substring (no common false positives)
    "concierge",
    # NOTE: "unfurnished" is intentionally NOT a naive substring here — it would
    # wrongly match "furnished or unfurnished" (furnished IS available). It is
    # handled by _has_unfurnished(), called below, which excludes those variants.
    # Student-only lettings — Rightmove STU_LET listings sometimes appear in regular rental searches
    "student property",
    "student accommodation",
    # Agent names — belt-and-suspenders in case agent field isn't populated
    "greater london properties",
    "foxtons",
    "dexters",
    "blueground",
    "219 baker",
    "219baker",
]

# Keywords that must match as whole words only (not substrings).
# "porter" must be whole-word: "reporter", "supporter", "transporter" all contain
# "porter" as a substring and would cause false positives if matched naively.
_BLACKLISTED_WHOLE_WORDS = [
    "porter",   # catches "24 hour porter", "porter service", "portered", "porterage"
                # but NOT "reporter", "supporter", "transporter"
    # Note: "unfurnished" is already in BLACKLISTED_KEYWORDS as a safe substring match
    # (no English word ends in "unfurnished") so whole-word matching is not needed.
]

_WHOLE_WORD_RE = re.compile(
    r'\b(?:' + '|'.join(re.escape(w) for w in _BLACKLISTED_WHOLE_WORDS) + r')',
    re.IGNORECASE,
)


def _has_blacklisted_keyword(listing: dict) -> bool:
    """Return True if any blacklisted keyword appears anywhere in the listing text."""
    features = listing.get("features", [])
    features_str = " ".join(features) if isinstance(features, list) else str(features)

    haystack = " ".join([
        listing.get("title", ""),
        listing.get("address", ""),
        listing.get("description", ""),
        listing.get("summary", ""),
        features_str,
        listing.get("amenities") or "",
        listing.get("key_features") or "",
    ]).lower()

    # Substring check for most keywords
    if any(kw in haystack for kw in BLACKLISTED_KEYWORDS):
        return True

    # Whole-word check for porter (avoids false positives like reporter/supporter)
    if _WHOLE_WORD_RE.search(haystack):
        return True

    # Unfurnished-only — but keep "furnished or unfurnished" (furnished available)
    if _has_unfurnished(haystack):
        return True

    return False


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
    _PLATFORM_NAMES = {"rightmove": "Rightmove", "onthemarket": "OnTheMarket",
                       "zoopla": "Zoopla", "openrent": "OpenRent"}
    _src_raw = (listing.get("source") or "").lower()
    source   = _PLATFORM_NAMES.get(_src_raw, _src_raw.capitalize() or "Unknown")

    # Price
    asking     = verdict.get("asking_price") or listing.get("price_pcm", 0) or 0
    fmv        = verdict.get("fmv")          # may be None for low-confidence passes
    difference = verdict.get("difference")   # may be None when fmv is None

    # Walk
    station     = listing.get("walk_station", "nearest station")
    walk_mins   = listing.get("walk_mins")
    walk_mins   = walk_mins if walk_mins is not None else "?"
    station_fmt = _format_station(station) if station else "nearest station"

    # Beds / prop type
    beds      = listing.get("beds")
    prop_type = listing.get("prop_type") or "property"
    bed_str   = f"{beds} bed " if beds else ""

    # FMV line — handle the AirDNA STR method, the comparables FMV, and the None case
    if verdict.get("method") == "airdna_str":
        req = verdict.get("str_required_nightly")
        adr = verdict.get("str_airdna_avg")
        fmv_line = (f"📊 STR viable: needs £{req:,}/night vs AirDNA avg £{adr:,}/night ✅"
                    if req and adr else "📊 STR viable \\(AirDNA\\)")
    elif fmv is not None and difference is not None:
        if difference < 0:
            diff_label = f"£{abs(difference):,} below FMV — great deal"
        elif difference == 0:
            diff_label = "exactly at FMV"
        else:
            diff_label = f"£{difference:,} above — within tolerance"
        fmv_line = f"📊 FMV: £{fmv:,}/month \\({_esc(diff_label)}\\)"
    else:
        fmv_line = "📊 FMV: unverified \\(no comparables — check value manually\\)"

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
        fmv_line,
        f"🚇 Nearest station: {_esc(station_fmt)}",
        f"🏢 {_esc(source)}",
        f"🔗 [View listing]({url})",
        *(["📱 *OpenRent — please contact manually*"] if listing.get("source") == "openrent" else []),
    ]
    # Comparables context only applies to the old FMV method (>£7,500). Hide it for
    # AirDNA STR passes, where no sale/let comparables are pulled.
    if verdict.get("method") != "airdna_str":
        lines += [
            "\\-\\-\\-",
            f"Own history: {own_str}",
            f"Nearby let\\-agreed: {let_str}",
            f"Confidence: {_esc(confidence)}",
        ]
        if reasoning:
            lines.append(f"_{_esc(reasoning)}_")

    return "\n".join(lines)


# ── Walk time check ───────────────────────────────────────────────────────────

def _check_walk(listing: dict) -> tuple[Optional[str], Optional[int]]:
    address = listing.get("address") or listing.get("area", "")
    # Pass full listing so nearest_walk_minutes can use the free area-based
    # fallback if Google Maps API is unavailable or returns an error.
    return nearest_walk_minutes(address, listing=listing)


# ── Coverage helpers ────────────────────────────────────────────────────────

# A zero-listing station is only a real gap if NO other station that DID return
# listings lies within this distance. Stations closer than this share most of
# their 0.5mi catchment, so the same properties get scraped once and deduped to
# whichever neighbour was processed first (e.g. Charing Cross ⊂ Covent Garden).
OVERLAP_MI = 0.55


def _haversine_mi(a: float, b: float, c: float, d: float) -> float:
    import math
    R = 3958.7613
    p = math.pi / 180
    x = 0.5 - math.cos((c - a) * p) / 2 + math.cos(a * p) * math.cos(c * p) * (1 - math.cos((d - b) * p)) / 2
    return 2 * R * math.asin(math.sqrt(x))


def _uncovered_zero_stations(raw_zero: list[str], per_station: dict[str, int]) -> list[str]:
    """From the stations that returned 0 listings, keep only those with NO
    non-zero station within OVERLAP_MI — i.e. genuine lost coverage, not a
    dedup/labelling artifact of overlapping central stations."""
    coords = _load_airdna_rates().get("station_coords", {})
    uncovered: list[str] = []
    for s in raw_zero:
        sc = coords.get(s)
        if not sc:
            uncovered.append(s)   # no coords to reason about → don't silence it
            continue
        covered = any(
            other != s
            and per_station.get(other, 0) > 0
            and oc
            and _haversine_mi(sc[0], sc[1], oc[0], oc[1]) <= OVERLAP_MI
            for other, oc in coords.items()
        )
        if not covered:
            uncovered.append(s)
    return uncovered


# ── Pipeline ──────────────────────────────────────────────────────────────────

async def run_pipeline(
    max_walk: int = MAX_WALK_MINS,
) -> tuple[list[dict], int, int, int, list[dict]]:
    """
    Full pipeline: load → walk filter → dedup → FMV verdict.

    Returns (passing_listings, total_scraped, new_after_dedup, walk_count)
    Each passing listing has walk_station, walk_mins, and verdict dict attached.
    """
    _LAST_RUN_DIAG.clear()
    if not LISTINGS_PATH.exists():
        logger.error("[filter] listings.json not found at %s", LISTINGS_PATH)
        _LAST_RUN_DIAG.update(per_source={}, detail_checked=0, detail_blocked=0)
        return [], 0, 0, 0, []

    # Clean seen_listings.json of entries older than 30 days
    clean_old_entries()

    raw: list[dict] = json.loads(LISTINGS_PATH.read_text(encoding="utf-8"))
    logger.info("[filter] Loaded %d listings", len(raw))

    # Backfill bedroom count where the scraper didn't capture it (portal cards
    # show "<type> <beds> <baths>", not the literal "N bed"). Without beds the
    # FMV router can't run the AirDNA STR check and wrongly falls back to the
    # comparables FMV. Infer from title/description so every source has beds.
    beds_filled = agent_filled = 0
    for _l in raw:
        if _l.get("beds") is None:
            b = _infer_beds(_l)
            if b is not None:
                _l["beds"] = b
                beds_filled += 1
        if not (_l.get("agent") or _l.get("landlord")):
            a = _infer_agent(_l)
            if a:
                _l["agent"] = a
                agent_filled += 1
    if beds_filled or agent_filled:
        logger.info("[filter] Backfilled from title/desc: %d beds, %d agents", beds_filled, agent_filled)

    # Per-source counts of the full scraped set (for observability + alerts)
    per_source: dict[str, int] = {}
    # Per-station counts across ALL sources combined (coverage monitoring)
    per_station: dict[str, int] = {}
    for _l in raw:
        src = (_l.get("source") or "unknown").lower()
        per_source[src] = per_source.get(src, 0) + 1
        area = _l.get("area")
        if area:
            per_station[area] = per_station.get(area, 0) + 1
    logger.info("[filter] Scraped by source: %s",
                ", ".join(f"{k}={v}" for k, v in sorted(per_source.items())) or "none")
    # Expected stations = the canonical AirDNA station list (matches scraper AREAS)
    expected_stations = sorted(_load_airdna_rates().get("by_station", {}).keys())
    raw_zero = [s for s in expected_stations if per_station.get(s, 0) == 0]
    # Overlap-aware: a station whose 0.5mi catchment overlaps a neighbour that DID
    # return listings isn't actually uncovered — its properties were scraped under
    # the neighbour and dropped by URL dedup. Only flag genuinely-isolated zeros.
    zero_stations = _uncovered_zero_stations(raw_zero, per_station)
    overlap_covered = [s for s in raw_zero if s not in zero_stations]
    if raw_zero:
        logger.warning("[filter] Stations with 0 listings (all sources): %s",
                       ", ".join(raw_zero))
        if overlap_covered:
            logger.info("[filter] …of which covered by an overlapping neighbour "
                        "(not a real gap): %s", ", ".join(overlap_covered))
    _LAST_RUN_DIAG.update(per_source=per_source, per_station=per_station,
                          zero_stations=zero_stations, overlap_covered=overlap_covered,
                          detail_checked=0, detail_blocked=0)

    # Step 0 — price change detection (runs on full scraped set before any filtering)
    price_drops = check_price_changes(raw)
    if price_drops:
        logger.info("[filter] Price drops detected: %d", len(price_drops))
    else:
        price_drops = []

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

    # Step 4.5 — Cross-portal dedup: when the same property appears on Rightmove
    # AND on OTM/Zoopla, keep only the higher-priority portal version so we avoid
    # Rightmove's finicky enquiry form whenever a better alternative exists.
    before_xp = len(walk_passed)
    walk_passed = _dedupe_cross_portal(walk_passed)
    xp_removed = before_xp - len(walk_passed)
    if xp_removed:
        logger.info(
            "[filter] Cross-portal dedup: dropped %d Rightmove duplicate(s) covered by OTM/Zoopla",
            xp_removed,
        )

    # Step 4.7 — Detail-page keyword check via Playwright
    # Plain HTTP requests to OTM/Rightmove are blocked from GitHub Actions IPs.
    # Playwright renders the full page (JavaScript + CSS) like a real browser,
    # bypassing bot detection and reliably exposing Key Features sections that
    # contain "24 Hour Porter", "24 Hr Concierge", etc.
    if walk_passed:
        detail_flags  = await _check_detail_pages_playwright(walk_passed)
        before_detail = len(walk_passed)
        walk_passed   = [l for l, blocked in zip(walk_passed, detail_flags) if not blocked]
        detail_removed = before_detail - len(walk_passed)
        _LAST_RUN_DIAG.update(detail_checked=before_detail, detail_blocked=detail_removed)
        if detail_removed:
            logger.info("[filter] Detail-page Playwright check: blocked %d/%d listings "
                        "(porter/concierge/unfurnished hidden in key features)",
                        detail_removed, before_detail)

    # Step 5 — Hard price ceiling by bed count (skip FMV for overpriced listings)
    def _over_price_cap(listing: dict) -> bool:
        pcm = _parse_price_pcm(listing.get("price", ""))
        if not pcm:
            return False
        beds = listing.get("beds")
        if beds == 2:
            return pcm > MAX_PRICE_2BED
        # 3+ bed: only cap if MAX_PRICE_PCM > 0 (0 = no cap)
        return MAX_PRICE_PCM > 0 and pcm > MAX_PRICE_PCM

    before_price = len(walk_passed)
    walk_passed  = [l for l in walk_passed if not _over_price_cap(l)]
    price_removed = before_price - len(walk_passed)
    if price_removed:
        logger.info("[filter] Price ceiling (2-bed ≤£%d, 3+bed ≤£%d): removed %d listings",
                    MAX_PRICE_2BED, MAX_PRICE_PCM, price_removed)

    # Step 6 — FMV verdict, routed by asking rent:
    #   • <= FMV_OLD_METHOD_THRESHOLD (£7,500): decide with the AirDNA STR check only
    #     (fast, no LLM). Falls back to the comparables FMV if STR can't be computed.
    #   • >  threshold (or unknown price): use the old comparables/LLM FMV method.
    fmv_passed: list[dict] = []
    n_airdna = n_oldfmv = 0

    async def _old_fmv(listing: dict) -> bool:
        try:
            verdict = await get_fmv_verdict(listing)
        except Exception as exc:
            logger.warning("[filter] FMV verdict failed for %s: %s", listing.get("address"), exc)
            return False
        if verdict.get("verdict") == "PASS":
            listing["_verdict"] = verdict
            return True
        logger.debug("[filter] FMV FAIL: %s (asking £%s, FMV £%s)",
                     listing.get("address", "")[:40], verdict.get("asking_price"), verdict.get("fmv"))
        return False

    for i, listing in enumerate(walk_passed, 1):
        pcm = _parse_price_pcm(listing.get("price", "")) or listing.get("price_pcm")
        use_airdna = pcm is not None and pcm <= FMV_OLD_METHOD_THRESHOLD
        if use_airdna:
            sv = _airdna_str_verdict(listing)
            if sv is None:
                # Can't compute STR (missing beds / no AirDNA) → fall back to old FMV
                n_oldfmv += 1
                if await _old_fmv(listing):
                    fmv_passed.append(listing)
                continue
            n_airdna += 1
            if sv["viable"]:
                listing["_verdict"] = {
                    "verdict": "PASS", "method": "airdna_str",
                    "asking_price": sv["asking_price"], "fmv": None, "difference": None,
                    "str_required_nightly": sv["required_nightly"], "str_airdna_avg": sv["airdna_avg"],
                    "confidence": "AirDNA STR",
                }
                fmv_passed.append(listing)
            else:
                logger.info("[filter] STR FAIL: %s | needs £%d/night vs AirDNA £%d",
                            listing.get("address", "")[:40], sv["required_nightly"], sv["airdna_avg"])
        else:
            logger.info("[filter] FMV (>£%d) check %d/%d: %s", FMV_OLD_METHOD_THRESHOLD, i,
                        len(walk_passed), listing.get("address", "")[:50])
            n_oldfmv += 1
            if await _old_fmv(listing):
                fmv_passed.append(listing)

    logger.info("[filter] FMV routing: %d via AirDNA STR (≤£%d), %d via comparables FMV (>£%d/unknown)",
                n_airdna, FMV_OLD_METHOD_THRESHOLD, n_oldfmv, FMV_OLD_METHOD_THRESHOLD)

    # Sort by walk time (closest first)
    fmv_passed.sort(key=lambda l: l.get("walk_mins") or 999)

    total_scraped  = len(json.loads(LISTINGS_PATH.read_text(encoding="utf-8"))) if LISTINGS_PATH.exists() else 0
    new_after_dedup = len(walk_passed)   # walk_passed was reassigned to new_listings after dedup
    logger.info("[filter] FMV filter: %d/%d passed", len(fmv_passed), new_after_dedup)
    return fmv_passed, total_scraped, new_after_dedup, walk_count, price_drops


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
        passing, total_scraped, new_count, walk_count, price_drops = await run_pipeline(MAX_WALK_MINS)
    except Exception as exc:
        logger.error("[filter] Pipeline failed: %s", exc, exc_info=True)
        await update.message.reply_text(f"❌ Pipeline failed: {exc}")
        return

    dupes_skipped = walk_count - new_count

    # ── Price drop alerts ─────────────────────────────────────────────────────
    for drop in (price_drops or []):
        try:
            await update.message.reply_text(
                f"💰 *Price drop!*\n"
                f"{drop['address']}\n"
                f"~~{drop['old_price']}~~ → *{drop['new_price']}*\n"
                f"{drop['url']}",
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
        except Exception as exc:
            logger.warning("[filter] Failed to send price drop alert: %s", exc)

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
            _log_passed_listing(listing, verdict)
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
                _log_passed_listing(listing, verdict)
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

def _source_breakdown_str() -> str:
    """One-line 'rightmove=74, onthemarket=89, zoopla=0, openrent=0' summary."""
    per = _LAST_RUN_DIAG.get("per_source", {}) or {}
    if not per:
        return "no scrape data"
    return ", ".join(f"{k}={v}" for k, v in sorted(per.items()))


def _build_health_warnings(sent: int, new_count: int, total_scraped: int) -> list[str]:
    """Detect 'looks broken' conditions worth a Telegram heads-up.

    Returns human-readable warning lines (empty list = all healthy).
    """
    warnings: list[str] = []
    per = _LAST_RUN_DIAG.get("per_source", {}) or {}

    # 1. Nothing scraped at all → every source blocked / network issue
    if total_scraped == 0:
        warnings.append("0 listings scraped from ALL sources — likely network/IP block.")
    else:
        # 2. A normally-reliable source returned zero → bot-blocked / selector broke
        for src in sorted(CORE_SOURCES):
            if per.get(src, 0) == 0:
                warnings.append(f"source '{src}' returned 0 listings — likely bot-blocked or selector broke.")

    # 3. Detail-page check blocked an abnormally high share → possible false positives
    checked = _LAST_RUN_DIAG.get("detail_checked", 0) or 0
    blocked = _LAST_RUN_DIAG.get("detail_blocked", 0) or 0
    if checked >= DETAIL_BLOCK_ALERT_MIN and blocked / checked >= DETAIL_BLOCK_ALERT_FRACTION:
        pct = round(100 * blocked / checked)
        warnings.append(
            f"detail-page check blocked {pct}% ({blocked}/{checked}) — "
            f"possible false-positive keyword match (see 'Detail check blocked' log lines)."
        )

    # 4. Scraped real listings but sent nothing → something downstream is over-filtering
    if total_scraped > 0 and new_count > 0 and sent == 0:
        warnings.append(
            f"{new_count} new listing(s) checked but 0 sent — every one was filtered out."
        )

    # 5. A station returned 0 listings across ALL sources → lost coverage there
    zero_stations = _LAST_RUN_DIAG.get("zero_stations", []) or []
    if total_scraped > 0 and zero_stations:
        warnings.append(
            f"{len(zero_stations)} station(s) had 0 listings from every source "
            f"and no nearby station to cover them: " + ", ".join(zero_stations)
        )

    return warnings


async def _send_health_alert(bot, chat_id: int, sent: int, new_count: int, total_scraped: int) -> None:
    """Send a Telegram heads-up if the run looks unhealthy. No-op if healthy."""
    try:
        warnings = _build_health_warnings(sent, new_count, total_scraped)
        if not warnings:
            return
        body = "⚠️ Property bot health check\n" + "\n".join(f"• {w}" for w in warnings)
        body += f"\n\nScraped by source: {_source_breakdown_str()}"
        await bot.send_message(chat_id=chat_id, text=body, disable_web_page_preview=True)
    except Exception as exc:
        logger.warning("[filter] Failed to send health alert: %s", exc)


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
        passing, total_scraped_now, new_count, walk_count, price_drops = await run_pipeline(MAX_WALK_MINS)
    except Exception as exc:
        logger.error("[filter] Pipeline failed: %s", exc)
        await bot.send_message(chat_id=chat_id, text=f"❌ Filter pipeline failed: {exc}")
        return

    dupes_skipped = walk_count - new_count

    # ── Price drop alerts — listings you've already seen that got cheaper ─────
    for drop in (price_drops or []):
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=(
                    f"💰 *Price drop!*\n"
                    f"{drop['address']}\n"
                    f"~~{drop['old_price']}~~ → *{drop['new_price']}*\n"
                    f"{drop['url']}"
                ),
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
        except Exception as exc:
            logger.warning("[filter] Failed to send price drop alert: %s", exc)

    if not passing:
        await bot.send_message(
            chat_id=chat_id,
            text=(
                f"✅ Done. 0 properties passed.\n"
                f"• Scraped today: {total_scraped_now:,}\n"
                f"• By source: {_source_breakdown_str()}\n"
                f"• Already sent (skipped): {dupes_skipped:,}\n"
                f"• New listings checked for FMV: {new_count:,}"
            ),
        )
        await _send_health_alert(bot, chat_id, sent=0, new_count=new_count,
                                 total_scraped=total_scraped_now)
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
                _log_passed_listing(listing, verdict)
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
                    fmv_val = verdict.get('fmv')
                    fmv_str = f"£{fmv_val:,}" if fmv_val else "unverified"
                    plain = (
                        f"{listing.get('area')} — £{verdict.get('asking_price', '?'):,}/mo\n"
                        f"{listing.get('address')}\n"
                        f"Walk: {listing.get('walk_mins')} min | FMV: {fmv_str}/mo\n"
                        f"{listing.get('url')}"
                    )
                    await bot.send_message(chat_id=chat_id, text=plain, disable_web_page_preview=True)
                    mark_as_seen(listing)
                    _log_passed_listing(listing, verdict)
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
            f"• By source: {_source_breakdown_str()}\n"
            f"• Already sent (skipped): {dupes_skipped:,}\n"
            f"• New listings checked for FMV: {new_count:,}\n"
            f"• Passed FMV & sent: {sent}"
        ),
    )
    await _send_health_alert(bot, chat_id, sent=sent, new_count=new_count,
                             total_scraped=total_scraped_now)
    logger.info("[filter] Automated pipeline complete — %d/%d new passed, %d sent (dupes skipped: %d)",
                len(passing), new_count, sent, dupes_skipped)

    # Auto-enquiry removed — send enquiries by pasting links into Telegram chat.


# ── Manual enquiry handler — paste links to trigger enquiries ────────────────

_PORTAL_PATTERNS = [
    (re.compile(r"https?://(?:www\.)?rightmove\.co\.uk/properties/(\d+)"),    "rightmove"),
    (re.compile(r"https?://(?:www\.)?zoopla\.co\.uk/to-rent/details/(\d+)"),  "zoopla"),
    (re.compile(r"https?://(?:www\.)?onthemarket\.com/details/(\d+)"),         "onthemarket"),
    (re.compile(r"https?://(?:www\.)?openrent\.co\.uk/property-to-rent/\S+"), "openrent"),
]

def _extract_listings_from_text(text: str) -> list[dict]:
    """Parse property URLs out of a free-form text message."""
    listings = []
    seen = set()
    for pattern, source in _PORTAL_PATTERNS:
        for m in pattern.finditer(text):
            url = m.group(0).split("?")[0].split("#")[0].strip()
            if url in seen:
                continue
            seen.add(url)
            listings.append({"url": url, "source": source, "address": "", "area": "", "price": ""})
    return listings


async def enquire_links(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Triggered when the user sends a message containing property URLs.
    Submits enquiries for every valid Rightmove / Zoopla / OTM link found.
    OpenRent links are flagged as manual-only.
    """
    msg  = update.message
    text = msg.text or ""
    chat_id = msg.chat_id

    listings = _extract_listings_from_text(text)
    if not listings:
        return  # no property URLs found — ignore message

    manual  = [l for l in listings if l["source"] == "openrent"]
    to_send = [l for l in listings if l["source"] != "openrent"]

    reply_parts = []
    if manual:
        reply_parts.append(
            f"ℹ️ {len(manual)} OpenRent link(s) — contact landlord directly (no auto-enquiry)."
        )

    if not to_send:
        if reply_parts:
            await msg.reply_text("\n".join(reply_parts))
        return

    if manual:
        await msg.reply_text("\n".join(reply_parts))

    await msg.reply_text(
        f"📞 Submitting enquiries for {len(to_send)} listing(s)…\n"
        f"_(I'll confirm each one as it goes — allow ~1 min per listing)_",
        parse_mode="Markdown",
    )

    sent_count = failed_count = 0
    try:
        results = await submit_enquiries(to_send)
        for url, result in results.items():
            status  = result.get("status", "unknown")
            address = result.get("address") or url.split("/")[-1]
            if status == "sent":
                sent_count += 1
                await msg.reply_text(f"✅ Sent: {address}\n{url}", disable_web_page_preview=True)
            elif status == "manual":
                await msg.reply_text(f"📋 Manual needed: {address}\n{url}", disable_web_page_preview=True)
            elif status == "skipped":
                await msg.reply_text(f"⏭️ Already enquired: {address}", disable_web_page_preview=True)
            else:
                failed_count += 1
                await msg.reply_text(f"❌ Failed: {address}\n{url}", disable_web_page_preview=True)

        await msg.reply_text(
            f"🏁 Done — {sent_count} sent, {failed_count} failed out of {len(to_send)} listings."
        )
    except Exception as exc:
        logger.error("[enquire_links] Submission error: %s", exc)
        await msg.reply_text(f"⚠️ Enquiry error: {exc}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    if not TOKEN:
        raise ValueError("TELEGRAM_FILTER_BOT_TOKEN is not set in .env")

    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start",  start))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("run",    run))
    # Paste property links → auto-submit enquiries
    app.add_handler(MessageHandler(tg_filters.TEXT & ~tg_filters.COMMAND, enquire_links))

    logger.info("Filter Bot starting (walk≤%d min, FMV+£500 rule) — polling…", MAX_WALK_MINS)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
