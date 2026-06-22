import asyncio, logging, os, re

logger = logging.getLogger(__name__)

# Station slugs for Zoopla's /station/tube/ URL format. All use radius=0.5 miles.
# Zoopla applies beds_min / furnished_state / radius SERVER-SIDE, so the returned
# results are already filtered — we just parse the cards.
AREAS = {
    "Covent Garden":      "covent-garden",
    "Soho":               "piccadilly-circus",
    "Baker Street":       "baker-street",
    "Bond Street":        "bond-street",
    "Marble Arch":        "marble-arch",
    "Oxford Circus":      "oxford-circus",
    "Marylebone":         "marylebone",
    "Regent's Park":      "regents-park",
    "Kensington Olympia": "kensington-olympia",
    "Holborn":            "holborn",
    "Chancery Lane":      "chancery-lane",
    "Farringdon":         "farringdon",
    "Angel":              "angel",
    "Old Street":         "old-street",
    "Charing Cross":      "charing-cross",
    "Victoria":           "victoria",
    "King's Cross St Pancras": "kings-cross-st-pancras",
    "Goodge Street":      "goodge-street",
    "Russell Square":     "russell-square",
    "Gloucester Road":    "gloucester-road",
    "Lancaster Gate":     "lancaster-gate",
}

_MAX_PAGES = 4
_DETAIL_RE = re.compile(r"/to-rent/details/(\d+)")

# Optional proxy / unlocker. Zoopla sits behind Cloudflare, which blocks datacenter
# IPs (GitHub Actions). curl_cffi (Chrome TLS impersonation) is tried first and is
# free; if it's still blocked, set ZOOPLA_PROXY (a residential/unlocker proxy URL,
# e.g. http://user:pass@host:port) as a GitHub secret and requests route through it
# — still fully cloud-side, no PC and no login required.
_PROXY = os.getenv("ZOOPLA_PROXY") or os.getenv("PROXY_URL") or ""

_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"),
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Referer": "https://www.google.com/",
    "Upgrade-Insecure-Requests": "1",
}

# Modern browser fingerprints to rotate through (curl_cffi >= 0.15). A site that
# fingerprint-blocks one may let another through; we try each until one returns a
# real results page. This defeats TLS/JA3 detection — NOT datacenter-IP bans.
_IMPERSONATE = ["chrome136", "chrome131", "safari180", "chrome131_android", "chrome124"]


def _url(slug, pn=1):
    return (f"https://www.zoopla.co.uk/to-rent/property/station/tube/{slug}/"
            f"?beds_min=2&price_max=15000&furnished_state=furnished&radius=0.5"
            f"&results_sort=newest_listings&pn={pn}")


def _fetch_html(url: str):
    """Fetch a Zoopla page from the cloud, best-effort past Cloudflare.

    1) curl_cffi impersonating Chrome (free; defeats TLS/JA3 fingerprinting).
    2) plain requests (last resort).
    Both routed through ZOOPLA_PROXY if set. Returns HTML str or None if blocked.
    """
    proxies = {"http": _PROXY, "https": _PROXY} if _PROXY else None

    # Attempt 1 — curl_cffi, rotating through modern browser fingerprints
    try:
        from curl_cffi import requests as creq
        for imp in _IMPERSONATE:
            try:
                r = creq.get(url, headers=_HEADERS, impersonate=imp,
                             proxies=proxies, timeout=30)
                if r.status_code == 200 and "/to-rent/details/" in r.text:
                    return r.text
                logger.info("[zoopla] curl_cffi imp=%s status=%s len=%s (blocked?)",
                            imp, r.status_code, len(r.text or ""))
            except Exception as exc:
                logger.info("[zoopla] curl_cffi imp=%s error: %s", imp, exc)
    except ImportError:
        logger.warning("[zoopla] curl_cffi not installed — add it to requirements.txt")

    # Attempt 2 — plain requests
    try:
        import requests
        r = requests.get(url, headers=_HEADERS, proxies=proxies, timeout=30)
        if r.status_code == 200 and "/to-rent/details/" in r.text:
            return r.text
        logger.info("[zoopla] requests status=%s len=%s (blocked?)",
                    r.status_code, len(r.text or ""))
    except Exception as exc:
        logger.info("[zoopla] requests error: %s", exc)

    return None


def _parse_card(area: str, href: str, text: str):
    tl = text.lower()
    if "let agreed" in tl:
        return None
    m = _DETAIL_RE.search(href)
    if not m:
        return None

    beds_m = re.search(r"(\d+)\s*beds?\b", tl)
    if not beds_m or int(beds_m.group(1)) < 2:
        return None
    beds = int(beds_m.group(1))

    price_m = re.search(r"£\s*([\d,]+)\s*pcm", text, re.I) or re.search(r"£\s*([\d,]+)\s*pw", text, re.I)
    price = (f"£{price_m.group(1)} pcm") if price_m else "Price N/A"

    bath_m = re.search(r"(\d+)\s*baths?\b", tl)
    sqft_m = re.search(r"([\d,]+)\s*sq\.?\s*ft", tl)
    sqft = int(sqft_m.group(1).replace(",", "")) if sqft_m else None

    addr_m = re.search(r"([A-Z][A-Za-z0-9'’.\- ]+,\s*London\s+[A-Z]{1,2}\d[A-Z\d]?)", text)
    address = addr_m.group(1).strip() if addr_m else area

    full = href if href.startswith("http") else "https://www.zoopla.co.uk" + href
    return {
        "source":      "zoopla",
        "area":        area,
        "title":       address,
        "price":       price,
        "address":     address,
        "url":         full.split("?")[0],
        "beds":        beds,
        "baths":       int(bath_m.group(1)) if bath_m else None,
        "sqft":        sqft,
        "description": text[:600],
    }


def _parse_html(area: str, html: str):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")
    # Prefer the text-bearing listing card anchor; fall back to any detail link.
    anchors = soup.select("a[data-testid='listing-card-content']")
    if not anchors:
        anchors = [a for a in soup.find_all("a", href=True) if _DETAIL_RE.search(a.get("href", ""))]
    out, seen = [], set()
    for a in anchors:
        href = a.get("href", "")
        m = _DETAIL_RE.search(href)
        if not m or m.group(1) in seen:
            continue
        text = a.get_text(" ", strip=True)
        if len(text) < 20 and a.parent is not None:
            text = a.parent.get_text(" ", strip=True)
        rec = _parse_card(area, href, text)
        if rec:
            seen.add(m.group(1))
            out.append(rec)
    return out


def _scrape_area_sync(area: str, slug: str):
    listings, seen = [], set()
    for pn in range(1, _MAX_PAGES + 1):
        html = _fetch_html(_url(slug, pn))
        if not html:
            if pn == 1:
                logger.info("[zoopla] %s -> blocked / no HTML", area)
            break
        page = _parse_html(area, html)
        new = [r for r in page if r["url"] not in seen]
        for r in new:
            seen.add(r["url"])
        listings.extend(new)
        if not new:
            break
    logger.info("[zoopla] %s -> %d listings", area, len(listings))
    return listings


async def scrape():
    results = await asyncio.gather(
        *[asyncio.to_thread(_scrape_area_sync, a, slug) for a, slug in AREAS.items()],
        return_exceptions=True,
    )
    all_listings = []
    for r in results:
        if isinstance(r, list):
            all_listings.extend(r)
        else:
            logger.warning("[zoopla] area task failed: %s", r)
    seen, unique = set(), []
    for lst in all_listings:
        if lst.get("url") and lst["url"] not in seen:
            seen.add(lst["url"]); unique.append(lst)
    logger.info("[zoopla] Total after dedup: %d", len(unique))
    return unique
