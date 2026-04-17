"""
utils/walk_time.py
------------------
Returns the walk time from a property to the nearest tube/rail station.

FREE APPROACH (no Google Maps API needed):
  Every property is scraped within 0.25 miles (≤5 min walk) of its tube
  station by design — the scrapers all use radius=0.25 miles.  We simply
  return the station the property was scraped from and a walk time of 5 min.
  This is accurate and costs nothing.

  GOOGLE_MAPS_API_KEY is still read from .env.  If it is set AND the Geocoding
  API works, the full 3-step Google Maps lookup is used instead, giving exact
  walking times to the nearest ANY station.  If it fails for any reason the
  code falls back silently to the free method.

Walk-time mapping (area → tube station):
  Uses the same 12 areas as the scrapers.  Any area not in the table gets
  a generic "Nearest tube station" label with 5 min assumed.
"""

import logging
import os
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logger   = logging.getLogger(__name__)
_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")

_GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
_PLACES_URL  = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
_MATRIX_URL  = "https://maps.googleapis.com/maps/api/distancematrix/json"

# Known tube stations for each scraper area.
# walk_mins is a conservative estimate assuming 0.25 mi radius ≈ 5 min walk.
_STATION_MAP: dict[str, str] = {
    "Covent Garden":   "Covent Garden",
    "Soho":            "Piccadilly Circus",
    "Knightsbridge":   "Knightsbridge",
    "West Kensington": "West Kensington",
    "London Bridge":   "London Bridge",
    "Tower Hill":      "Tower Hill",
    "Baker Street":    "Baker Street",
    "Bond Street":     "Bond Street",
    "Marble Arch":     "Marble Arch",
    "Oxford Circus":   "Oxford Circus",
    "Marylebone":      "Marylebone",
    "Regent's Park":   "Regent's Park",
}

_DEFAULT_WALK_MINS = 5   # conservative for 0.25 mi radius


def _free_walk(listing: dict) -> tuple[str, int]:
    """
    Free fallback: look up the scraped station from the listing's 'area' field.
    Every property was scraped within 0.25 mi of this station (≈ 5 min walk).
    """
    area    = listing.get("area", "")
    station = _STATION_MAP.get(area, "Nearest tube station")
    return station, _DEFAULT_WALK_MINS


def nearest_walk_minutes(
    origin: str,
    destinations: list[str] = None,   # ignored — kept for backwards compat
    listing: dict = None,
) -> tuple[Optional[str], Optional[int]]:
    """
    Return (station_name, walk_minutes) for the property.

    Tries Google Maps first if GOOGLE_MAPS_API_KEY is configured and working.
    Falls back silently to the free station-map method on any failure.

    Args:
        origin:   property address string (used for Google Maps lookup)
        listing:  full listing dict — provides 'area' for the free fallback
    """
    # ── Free path: no API key configured ─────────────────────────────────────
    if not _API_KEY or _API_KEY == "your_google_maps_api_key_here":
        if listing:
            station, mins = _free_walk(listing)
            logger.debug("[walk_time] Free walk: %s → %s (%d min)", origin[:40], station, mins)
            return station, mins
        return None, None

    address = (origin or "").strip()
    if not address:
        if listing:
            return _free_walk(listing)
        return None, None

    # ── Step 1: Geocode ───────────────────────────────────────────────────────
    try:
        geo = requests.get(
            _GEOCODE_URL,
            params={"address": address + ", London, UK", "key": _API_KEY},
            timeout=10,
        ).json()

        if geo.get("status") != "OK" or not geo.get("results"):
            logger.warning("[walk_time] Geocoding failed for '%s': %s — using free fallback",
                           address[:60], geo.get("status"))
            if listing:
                return _free_walk(listing)
            return None, None

        loc = geo["results"][0]["geometry"]["location"]
        lat, lng = loc["lat"], loc["lng"]

    except Exception as exc:
        logger.warning("[walk_time] Geocoding error: %s — using free fallback", exc)
        if listing:
            return _free_walk(listing)
        return None, None

    # ── Step 2: Find nearest transit stations ─────────────────────────────────
    try:
        places = requests.get(
            _PLACES_URL,
            params={
                "location": f"{lat},{lng}",
                "rankby":   "distance",
                "type":     "transit_station",
                "key":      _API_KEY,
            },
            timeout=10,
        ).json()

        if places.get("status") not in ("OK", "ZERO_RESULTS"):
            logger.warning("[walk_time] Places Nearby status: %s — using free fallback",
                           places.get("status"))
            if listing:
                return _free_walk(listing)
            return None, None

        station_names = [
            p["name"] + ", London"
            for p in places.get("results", [])[:5]
        ]

        if not station_names:
            if listing:
                return _free_walk(listing)
            return None, None

    except Exception as exc:
        logger.warning("[walk_time] Places error: %s — using free fallback", exc)
        if listing:
            return _free_walk(listing)
        return None, None

    # ── Step 3: Distance Matrix ───────────────────────────────────────────────
    try:
        resp = requests.get(
            _MATRIX_URL,
            params={
                "origins":      address,
                "destinations": "|".join(station_names),
                "mode":         "walking",
                "units":        "metric",
                "key":          _API_KEY,
            },
            timeout=10,
        ).json()

        if resp.get("status") != "OK":
            logger.warning("[walk_time] Distance Matrix status: %s — using free fallback",
                           resp.get("status"))
            if listing:
                return _free_walk(listing)
            return None, None

        elements = resp.get("rows", [{}])[0].get("elements", [])
        times: dict[str, int] = {}
        for dest, elem in zip(station_names, elements):
            if elem.get("status") == "OK":
                times[dest] = round(elem["duration"]["value"] / 60)

        if not times:
            if listing:
                return _free_walk(listing)
            return None, None

        best = min(times, key=times.__getitem__)
        return best, times[best]

    except Exception as exc:
        logger.warning("[walk_time] Distance Matrix error: %s — using free fallback", exc)
        if listing:
            return _free_walk(listing)
        return None, None


def get_walk_minutes(
    origin: str,
    destinations: list[str],
) -> dict[str, Optional[int]]:
    """
    Return walking minutes from *origin* to each destination via Distance Matrix.
    Kept for backwards compatibility.
    """
    result: dict[str, Optional[int]] = {d: None for d in destinations}

    if not _API_KEY or _API_KEY == "your_google_maps_api_key_here":
        return result
    if not destinations:
        return result

    try:
        resp = requests.get(
            _MATRIX_URL,
            params={
                "origins":      origin,
                "destinations": "|".join(destinations),
                "mode":         "walking",
                "units":        "metric",
                "key":          _API_KEY,
            },
            timeout=10,
        ).json()

        if resp.get("status") != "OK":
            return result

        elements = resp.get("rows", [{}])[0].get("elements", [])
        for dest, elem in zip(destinations, elements):
            if elem.get("status") == "OK":
                result[dest] = round(elem["duration"]["value"] / 60)

    except Exception as exc:
        logger.error("[walk_time] Distance Matrix request failed: %s", exc)

    return result
