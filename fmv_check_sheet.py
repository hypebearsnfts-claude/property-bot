#!/usr/bin/env python3
"""
fmv_check_sheet.py — run a list of Rightmove property links through the same
AirDNA STR / FMV check the property bot uses, and write a results CSV.

WHY A LOCAL SCRIPT: Rightmove blocks datacenter IPs (GitHub Actions), so this is
meant to be run on your own machine (residential IP), where curl_cffi can fetch
the listing pages. It needs `curl_cffi` (already in requirements.txt) and reads
airdna_rates.json from the same folder.

USAGE
    python fmv_check_sheet.py INPUT [-o OUTPUT.csv]

  INPUT can be:
    • a local CSV exported from your sheet (File ▸ Download ▸ CSV), or
    • a Google Sheets URL / file-id (must be link-shared "anyone with link"), or
    • a published-CSV URL.
  The script auto-detects the link, price and address columns (it doesn't rely on
  the header names, which are in Russian in your sheet).

WHAT IT DOES per Rightmove link
    1. Fetches the page and reads exact bedrooms + latitude/longitude from the
       embedded __PAGE_MODEL data.
    2. Finds the nearest of your 21 AirDNA stations (haversine).
    3. STR check (same formula as the bot): required_nightly = asking × 1.5 ÷ 21;
       PASS if required_nightly <= AirDNA ADR(station, beds) + £50.
    4. Listings over £7,500/mo are flagged ">£7500 — use comparables FMV" (the bot
       routes those to the LLM FMV; this script doesn't run the LLM).

Output columns: address, agency, price_pcm, beds, nearest_station, dist_mi,
                airdna_adr, required_nightly, margin, verdict, link
"""
import argparse, csv, json, math, re, sys, time
from pathlib import Path

HERE = Path(__file__).parent
RATES = json.loads((HERE / "airdna_rates.json").read_text(encoding="utf-8"))
STATION_COORDS = RATES.get("station_coords", {})
BY_STATION = RATES.get("by_station", {})
BY_BEDROOMS = RATES.get("by_bedrooms", {})

STR_TOLERANCE = 50      # £ above AirDNA ADR still counts as viable
FMV_THRESHOLD = 7500    # above this, the bot uses the comparables/LLM FMV instead

_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def _fetch(url: str) -> str | None:
    try:
        from curl_cffi import requests as creq
        r = creq.get(url, headers=_HEADERS, impersonate="chrome124", timeout=30)
        if r.status_code == 200:
            return r.text
        print(f"  ! HTTP {r.status_code} for {url}", file=sys.stderr)
    except ImportError:
        print("  ! curl_cffi not installed — `pip install curl_cffi`", file=sys.stderr)
    except Exception as exc:
        print(f"  ! fetch error: {exc}", file=sys.stderr)
    # fallback: plain requests
    try:
        import requests
        r = requests.get(url, headers=_HEADERS, timeout=30)
        if r.status_code == 200:
            return r.text
    except Exception:
        pass
    return None


def _haversine_mi(a, b, c, d):
    R = 3958.7613
    p = math.pi / 180
    x = 0.5 - math.cos((c - a) * p) / 2 + math.cos(a * p) * math.cos(c * p) * (1 - math.cos((d - b) * p)) / 2
    return 2 * R * math.asin(math.sqrt(x))


def _nearest_station(lat, lng):
    best, best_d = None, 1e9
    for name, (slat, slng) in STATION_COORDS.items():
        d = _haversine_mi(lat, lng, slat, slng)
        if d < best_d:
            best, best_d = name, d
    return best, round(best_d, 2)


def _extract_rightmove(html: str):
    """Return (beds, lat, lng, page_price) from a Rightmove detail page, or Nones.

    Rightmove embeds a 'flatted' array in window.__PAGE_MODEL.data where object
    values are stored as integer indices into the array. We resolve those.
    """
    idx = html.find("window.__PAGE_MODEL")
    if idx == -1:
        return None, None, None, None
    start = html.find("{", idx)
    try:
        obj, _ = json.JSONDecoder().raw_decode(html, start)
        arr = json.loads(obj["data"]) if isinstance(obj.get("data"), str) else obj.get("data")
        if not isinstance(arr, list):
            return None, None, None, None
    except Exception:
        return None, None, None, None

    def resolve(v):
        return arr[v] if isinstance(v, int) and 0 <= v < len(arr) else v

    # Bedrooms: the <title> describes the MAIN property unambiguously, so prefer it
    # over the encoded blob (which can also hold "similar properties" nodes).
    lat = lng = beds = price = None
    m = re.search(r"<title>[^<]*?(\d+)\s*bedroom", html, re.I)
    if m:
        beds = int(m.group(1))
    elif re.search(r"<title>[^<]*?studio", html, re.I):
        beds = 0
    for node in arr:
        if isinstance(node, dict):
            if lat is None and "latitude" in node and "longitude" in node:
                la, lo = resolve(node["latitude"]), resolve(node["longitude"])
                if isinstance(la, (int, float)) and isinstance(lo, (int, float)):
                    lat, lng = la, lo
            if beds is None and "bedrooms" in node:
                b = resolve(node["bedrooms"])
                if isinstance(b, int):
                    beds = b
    return beds, lat, lng, price


def _airdna_adr(station, beds):
    bk = str(beds)
    if station and station in BY_STATION and bk in BY_STATION[station]:
        return BY_STATION[station][bk], f"{station}"
    if bk in BY_BEDROOMS:
        return BY_BEDROOMS[bk], "London-wide"
    return None, None


def _parse_price(s):
    m = re.search(r"([\d,]+)", str(s or ""))
    return int(m.group(1).replace(",", "")) if m else None


def _load_rows(source: str):
    """Yield CSV rows (list of cells) from a local path, sheets URL/id, or CSV url."""
    text = None
    if Path(source).exists():
        text = Path(source).read_text(encoding="utf-8")
    else:
        # turn a Google Sheets URL or bare id into an export-CSV URL
        m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", source)
        sheet_id = m.group(1) if m else (source if re.fullmatch(r"[a-zA-Z0-9-_]{30,}", source) else None)
        url = (f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
               if sheet_id else source)
        text = _fetch(url)
        if not text:
            sys.exit(f"Could not read input from {source}")
    return list(csv.reader(text.splitlines()))


def _detect_columns(rows):
    """Find the link, price and address column indices by scanning cell content."""
    link_c = price_c = addr_c = None
    for row in rows[:8]:
        for i, cell in enumerate(row):
            c = (cell or "").strip()
            if link_c is None and re.search(r"https?://\S*(rightmove|zoopla|onthemarket|openrent)", c):
                link_c = i
            if price_c is None and re.search(r"£\s*[\d,]+", c):
                price_c = i
        if link_c is not None and price_c is not None:
            break
    addr_c = 0  # the sheet's first column is the address
    return link_c, price_c, addr_c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input", help="CSV path, Google Sheets URL/id, or CSV url")
    ap.add_argument("-o", "--output", default=str(HERE / "fmv_sheet_results.csv"))
    ap.add_argument("--sleep", type=float, default=1.0, help="seconds between fetches")
    args = ap.parse_args()

    rows = _load_rows(args.input)
    link_c, price_c, addr_c = _detect_columns(rows)
    if link_c is None:
        sys.exit("No property links found in the input.")

    out_cols = ["address", "agency", "price_pcm", "beds", "nearest_station",
                "dist_mi", "airdna_adr", "required_nightly", "margin", "verdict", "link"]
    results = []
    seen = set()
    for row in rows:
        if len(row) <= link_c:
            continue
        link = (row[link_c] or "").strip()
        if "rightmove.co.uk/properties/" not in link:
            continue
        key = re.search(r"/properties/(\d+)", link)
        key = key.group(1) if key else link
        if key in seen:
            continue
        seen.add(key)

        address = row[addr_c].strip() if len(row) > addr_c else ""
        agency  = row[2].strip() if len(row) > 2 else ""
        price   = _parse_price(row[price_c]) if (price_c is not None and len(row) > price_c) else None

        print(f"· {address[:45]:45}  {link}")
        html = _fetch(link.split("#")[0])
        beds = lat = lng = None
        if html:
            beds, lat, lng, page_price = _extract_rightmove(html)
            price = price or _parse_price(page_price)

        station = dist = adr = required = margin = None
        if lat is not None and lng is not None:
            station, dist = _nearest_station(lat, lng)
        if price and price > FMV_THRESHOLD:
            verdict = ">£7500 — use comparables FMV (manual)"
        elif not price or not beds:
            verdict = "SKIP — missing price/beds"
        else:
            adr, src = _airdna_adr(station, beds)
            if not adr:
                verdict = "SKIP — no AirDNA data"
            else:
                required = round(price * 1.5 / 21)
                margin = required - adr
                verdict = "PASS" if margin <= STR_TOLERANCE else "FAIL"
                if station and src == "London-wide":
                    station = f"{station}? (London-wide ADR)"
        results.append([address, agency, price or "", beds if beds is not None else "",
                        station or "", dist if dist is not None else "", adr or "",
                        required if required is not None else "",
                        margin if margin is not None else "", verdict, link])
        time.sleep(args.sleep)

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(out_cols)
        w.writerows(results)
    npass = sum(1 for r in results if r[9] == "PASS")
    print(f"\nDone — {len(results)} listings, {npass} PASS. Written to {args.output}")


if __name__ == "__main__":
    main()
