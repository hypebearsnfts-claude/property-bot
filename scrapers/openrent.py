import asyncio, logging, os, re
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# OpenRent search URL — uses their /properties-to-rent search with a term + radius.
# IMPORTANT: OpenRent server-renders the full results list in the page HTML. The
# fancy `a.pli` cards you see in a browser are built CLIENT-SIDE by JavaScript
# (and personalised to the logged-in user). That JS/AJAX does not complete on the
# GitHub Actions runner, which is why the old Playwright scraper (which waited for
# `a.pli`) timed out and returned 0. We instead fetch the server HTML with a plain
# HTTP request and parse the property links directly — no JS, no browser needed.
#
# The server returns a wider set than the on-screen filters (bedrooms_min / radius
# are applied client-side), so we re-apply them here: >=2 beds, and <= 0.5 miles
# using the per-card distance text.
AREAS = {
    "Covent Garden":      ("covent-garden-london",      "Covent Garden, London"),
    "Soho":               ("soho-london",               "Soho, London"),
    "Baker Street":       ("baker-street-london",       "Baker Street, London"),
    "Bond Street":        ("bond-street-london",        "Bond Street, London"),
    "Marble Arch":        ("marble-arch-london",        "Marble Arch, London"),
    "Oxford Circus":      ("oxford-circus-london",      "Oxford Circus, London"),
    "Marylebone":         ("marylebone-london",         "Marylebone, London"),
    "Regent's Park":      ("regents-park-london",       "Regent's Park, London"),
    "Kensington Olympia": ("kensington-olympia-london", "Kensington Olympia, London"),
    "Holborn":            ("holborn-london",            "Holborn, London"),
    "Chancery Lane":      ("chancery-lane-london",      "Chancery Lane, London"),
    "Farringdon":         ("farringdon-london",         "Farringdon, London"),
    "Angel":              ("angel-london",              "Angel, London"),
    "Old Street":         ("old-street-london",         "Old Street, London"),
    "Charing Cross":      ("charing-cross-london",      "Charing Cross, London"),
    "Victoria":           ("victoria-london",           "Victoria, London"),
    "King's Cross St Pancras": ("kings-cross-london",   "King's Cross, London"),
    "Goodge Street":      ("goodge-street-london",      "Goodge Street, London"),
    "Russell Square":     ("russell-square-london",     "Russell Square, London"),
    "Gloucester Road":    ("gloucester-road-london",    "Gloucester Road, London"),
    "Lancaster Gate":     ("lancaster-gate-london",     "Lancaster Gate, London"),
}

_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Upgrade-Insecure-Requests": "1",
}

# OpenRent's WAF returns HTTP 405 to a plain `requests` fingerprint from the cloud,
# but serves the page fine to a real Chrome TLS handshake. curl_cffi impersonates
# Chrome (free) and is tried first; plain requests is the fallback. Optional proxy
# via env if OpenRent ever IP-blocks too (kept cloud-side, no PC, no login).
_PROXY = os.getenv("OPENRENT_PROXY") or os.getenv("PROXY_URL") or ""


def _fetch(url: str):
    """Return HTML for an OpenRent search page, or None if blocked."""
    proxies = {"http": _PROXY, "https": _PROXY} if _PROXY else None
    try:
        from curl_cffi import requests as creq
        r = creq.get(url, headers=_HEADERS, impersonate="chrome124",
                     proxies=proxies, timeout=30)
        if r.status_code == 200 and "/property-to-rent/london/" in r.text:
            return r.text
        logger.info("[openrent] curl_cffi status=%s", r.status_code)
    except ImportError:
        logger.warning("[openrent] curl_cffi not installed — add it to requirements.txt")
    except Exception as exc:
        logger.info("[openrent] curl_cffi error: %s", exc)
    try:
        import requests
        r = requests.get(url, headers=_HEADERS, proxies=proxies, timeout=30)
        if r.status_code == 200 and "/property-to-rent/london/" in r.text:
            return r.text
        logger.info("[openrent] requests status=%s", r.status_code)
    except Exception as exc:
        logger.info("[openrent] requests error: %s", exc)
    return None


_MAX_MILES   = 0.5
_MILE_IN_KM  = 1.609344
_MAX_KM      = _MAX_MILES * _MILE_IN_KM       # 0.5 mi -> ~0.805 km
_MAX_PAGES   = 5                              # 20 results/page; stop early on distance
_PROP_RE     = re.compile(r"/property-to-rent/london/[^/\s\"']+/(\d+)")


def _search_url(slug: str, term: str, skip: int = 0) -> str:
    from urllib.parse import quote
    base = (
        f"https://www.openrent.co.uk/properties-to-rent/{slug}"
        f"?term={quote(term)}&bedrooms_min=2&max_rent=15000"
        f"&furnishedType=1&isLive=true&radius={_MAX_MILES}"
    )
    return base + (f"&skip={skip}" if skip else "")


def _parse_card(area: str, href: str, text: str):
    """Build a listing dict from a property anchor's href + surrounding text.
    Returns None if it should be skipped (let-agreed, <2 beds, or out of radius)."""
    tl = text.lower()
    if "let agreed" in tl:
        return None

    # Distance — enforce the 0.5-mile radius (server returns a wider set)
    dist_km = None
    dm = re.search(r"([\d.]+)\s*km", tl)
    if dm:
        try:
            dist_km = float(dm.group(1))
        except ValueError:
            dist_km = None
    mile_m = re.search(r"([\d.]+)\s*mile", tl)
    if dist_km is None and mile_m:
        try:
            dist_km = float(mile_m.group(1)) * _MILE_IN_KM
        except ValueError:
            dist_km = None
    if dist_km is not None and dist_km > _MAX_KM + 1e-6:
        return None

    # Bedrooms — require >= 2 (the URL's bedrooms_min is only applied client-side).
    # "Studio" / "Room in a Shared Flat" have no "N bed" and are skipped.
    beds_m = re.search(r"(\d+)\s*bed", tl)
    if not beds_m or int(beds_m.group(1)) < 2:
        return None
    beds = int(beds_m.group(1))

    price_m = (re.search(r"£\s*([\d,]+)\s*(?:per month|/\s*month|pcm|per calendar month)", text, re.I)
               or re.search(r"£\s*([\d,]+)", text))
    price = (f"£{price_m.group(1)} pcm") if price_m else "Price N/A"

    bath_m = re.search(r"(\d+)\s*bath", tl)
    sqft_m = re.search(r"([\d,]+)\s*(?:sq\.?\s*ft|sqft)", tl)
    sqm_m  = re.search(r"([\d,]+)\s*(?:sq\.?\s*m\b|sqm|m²)", tl)
    sqft = None
    if sqft_m:
        sqft = int(sqft_m.group(1).replace(",", ""))
    elif sqm_m:
        sqft = int(int(sqm_m.group(1).replace(",", "")) * 10.764)

    full = href if href.startswith("http") else "https://www.openrent.co.uk" + href
    # Title: first chunk that looks like "2 Bed Flat, Newport House, WC2H"
    tm = re.search(r"(\d+\s*Bed[^£\n]{0,80}?[A-Z]{1,2}\d[A-Z\d]?)", text)
    title = tm.group(1).strip() if tm else text.strip()[:120]

    return {
        "source":      "openrent",
        "area":        area,
        "title":       title,
        "price":       price,
        "address":     title,
        "url":         full.split("?")[0],
        "beds":        beds,
        "baths":       int(bath_m.group(1)) if bath_m else None,
        "sqft":        sqft,
        "description": text[:600],
    }


def _scrape_area_sync(area: str, slug: str, term: str):
    listings, seen = [], set()
    for page_i in range(_MAX_PAGES):
        url = _search_url(slug, term, skip=page_i * 20)
        html = _fetch(url)
        if not html:
            if page_i == 0:
                logger.info("[openrent] %s -> blocked / no HTML", area)
            break
        soup = BeautifulSoup(html, "lxml")
        anchors = [a for a in soup.find_all("a", href=True) if _PROP_RE.search(a["href"])]
        if not anchors:
            # No property links in server HTML — nothing more to page through.
            break
        page_added, page_seen = 0, 0
        for a in anchors:
            m = _PROP_RE.search(a["href"])
            pid = m.group(1)
            if pid in seen:
                continue
            seen.add(pid)
            page_seen += 1
            text = a.get_text(" ", strip=True)
            if len(text) < 30 and a.parent is not None:
                text = a.parent.get_text(" ", strip=True)
            rec = _parse_card(area, a["href"], text)
            if rec:
                listings.append(rec)
                page_added += 1
        # Distance-sorted results: once a full page yields nothing new, stop.
        if page_seen == 0:
            break
    logger.info("[openrent] %s -> %d listings", area, len(listings))
    return listings


async def scrape():
    results = await asyncio.gather(
        *[asyncio.to_thread(_scrape_area_sync, a, slug, term)
          for a, (slug, term) in AREAS.items()],
        return_exceptions=True,
    )
    all_listings = []
    for r in results:
        if isinstance(r, list):
            all_listings.extend(r)
        else:
            logger.warning("[openrent] area task failed: %s", r)
    seen, unique = set(), []
    for lst in all_listings:
        if lst.get("url") and lst["url"] not in seen:
            seen.add(lst["url"]); unique.append(lst)
    logger.info("[openrent] Total after dedup: %d", len(unique))
    return unique
