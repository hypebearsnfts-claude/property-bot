"""
utils/dedupe.py
---------------
Normalised property "signature" for cross-platform / cross-agency de-duplication.

The same physical flat listed on Rightmove, Zoopla and OpenRent — or by two
different agencies on the same portal — has different URLs but the same street,
postcode and bedroom count. URL-based dedup can't see that. We build a signature
from street + outward postcode + beds and dedup on it instead.

Deliberately conservative: if we can't extract a street AND a postcode AND beds,
we return None so the caller keeps the listing (better to risk a rare duplicate
than to silently drop a genuinely distinct property).
"""
from __future__ import annotations

import re
from typing import Optional

# UK outward postcode, e.g. W1H, SW1A, EC1N, NW1. (The inward part like "2AB" is
# optional and ignored — outward + beds + street is enough to identify a flat.)
_OUTWARD_RE = re.compile(r"\b([A-Z]{1,2}\d[A-Z\d]?)\b", re.I)
_BED_SEG_RE = re.compile(r"\b\d+\s*bed", re.I)
_FLAT_RE    = re.compile(r"\b(flat|apartment|apt|unit|studio)\s*[\w\d]+\b", re.I)

# Segments that are a property type word only (not a street/building name).
_TYPE_ONLY = {
    "flat", "apartment", "apt", "studio", "maisonette", "house", "penthouse",
    "duplex", "mews", "cottage", "bungalow", "room", "unit", "london", "",
}


def _outward_postcode(addr: str) -> str:
    """Return the outward postcode (last match — postcodes sit at the address end)."""
    cands = _OUTWARD_RE.findall(addr or "")
    return cands[-1].upper() if cands else ""


def _street_segment(addr: str) -> str:
    """First comma-segment that looks like a street/building name (not a bed count,
    flat number, postcode or bare type word)."""
    for raw in (addr or "").split(","):
        seg = _FLAT_RE.sub("", raw)
        if _BED_SEG_RE.search(seg):          # "2 Bed Flat" descriptor
            continue
        seg = re.sub(r"[^\w\s]", " ", seg).lower()
        toks = [t for t in seg.split() if t]
        toks = [t for t in toks if not re.fullmatch(r"[a-z]{1,2}\d[a-z\d]?", t)]  # drop postcode tokens
        toks = [t for t in toks if t != "london"]
        if not toks:
            continue
        if len(toks) == 1 and toks[0] in _TYPE_ONLY:
            continue
        return " ".join(toks)
    return ""


def property_signature(listing: dict) -> Optional[str]:
    """Return 'street|outward_pc|beds' or None if it can't be determined safely."""
    beds = listing.get("beds")
    addr = (listing.get("address") or listing.get("title") or "").strip()
    if beds is None or not addr:
        return None
    street = _street_segment(addr)
    pc = _outward_postcode(addr)
    if not street or not pc:
        return None
    return f"{street}|{pc}|{beds}"
