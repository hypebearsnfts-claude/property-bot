"""
utils/walk_time.py
------------------
Returns the walk time from a property address to the nearest tube or rail
station using the Google Maps APIs.

Strategy (3 calls per property):
  1. Geocoding API          — convert address text → lat/lng
  2. Places Nearby Search   — find closest transit stations by distance
  3. Distance Matrix API    — get actual walking time to the nearest 5

This finds ANY station, not just a hardcoded list, so a property near
Holborn, Waterloo, or any other station will still pass the filter.

Set GOOGLE_MAPS_API_KEY in .env.
If any call fails, returns (None, None) — the pipeline treats None as a
pass-through (listing is kept with '?' walk time shown in Telegram).
"""

import logging
import os
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logger    = logging.getLogger(__name__)
_API_KEY  = os.getenv("GOOGLE_MAPS_API_KEY", "")

_GEOCODE_URL  = "https://maps.googleapis.com/maps/api/geocode/json"
_PLACES_URL   = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
_MATRIX_URL   = "https://maps.googleapis.com/maps/api/distancematrix/json"


def get_walk_minutes(
    origin: str,
    destinations: list[str],
) -> dict[str, Optional[int]]:
    """
    Return walking minutes from *origin* to each destination via Distance Matrix.
    Used internally and available for direct calls with known station names.
    """
    result: dict[str, Optional[int]] = {d: None for d in destinations}

    if not _API_KEY or _API_KEY == "your_google_maps_api_key_here":
        logger.warning("[walk_time] GOOGLE_MAPS_API_KEY not set — skipping walk times")
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
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") != "OK":
            logger.warning("[walk_time] Distance Matrix status: %s", data.get("status"))
            return result

        elements = data.get("rows", [{}])[0].get("elements", [])
        for dest, elem in zip(destinations, elements):
            if elem.get("status") == "OK":
                result[dest] = round(elem["duration"]["value"] / 60)
            else:
                logger.debug("[walk_time] No result for %s: %s", dest, elem.get("status"))

    except Exception as exc:
        logger.error("[walk_time] Distance Matrix request failed: %s", exc)

    return result


def nearest_walk_minutes(
    origin: str,
    destinations: list[str] = None,   # ignored — kept for backwards compat only
) -> tuple[Optional[str], Optional[int]]:
    """
    Find the nearest tube/rail station to *origin* and return
    (station_name, walk_minutes).

    Ignores the *destinations* argument — dynamically finds the nearest
    station using the Google Maps Places Nearby API so ANY station qualifies,
    not just a hardcoded list.

    Returns (None, None) on any failure — the pipeline treats None as a
    pass-through with '?' displayed in the Telegram message.
    """
    if not _API_KEY or _API_KEY == "your_google_maps_api_key_here":
        logger.warning("[walk_time] GOOGLE_MAPS_API_KEY not set")
        return None, None

    address = origin.strip()
    if not address:
        return None, None

    # ── Step 1: Geocode the property address ──────────────────────────────────
    try:
        geo = requests.get(
            _GEOCODE_URL,
            params={"address": address + ", London, UK", "key": _API_KEY},
            timeout=10,
        ).json()

        if geo.get("status") != "OK" or not geo.get("results"):
            logger.warning("[walk_time] Geocoding failed for '%s': %s",
                           address[:60], geo.get("status"))
            return None, None

        loc = geo["results"][0]["geometry"]["location"]
        lat, lng = loc["lat"], loc["lng"]

    except Exception as exc:
        logger.error("[walk_time] Geocoding request failed: %s", exc)
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
            logger.warning("[walk_time] Places Nearby status: %s", places.get("status"))
            return None, None

        station_names = [
            p["name"] + ", London"
            for p in places.get("results", [])[:5]   # check 5 nearest
        ]

        if not station_names:
            logger.warning("[walk_time] No transit stations found near '%s'", address[:60])
            return None, None

    except Exception as exc:
        logger.error("[walk_time] Places Nearby request failed: %s", exc)
        return None, None

    # ── Step 3: Walk times to the nearest stations ────────────────────────────
    times = get_walk_minutes(address, station_names)
    valid = {k: v for k, v in times.items() if v is not None}

    if not valid:
        return None, None

    best = min(valid, key=valid.__getitem__)
    return best, valid[best]
