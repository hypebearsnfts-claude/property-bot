"""
utils/walk_time.py
------------------
Returns walking times from a property address to one or more destinations
using the Google Maps Distance Matrix API.

Usage:
    from utils.walk_time import get_walk_minutes, nearest_walk_minutes

    times = get_walk_minutes(
        origin="42 Baker Street, London W1U 6AH",
        destinations=["Oxford Circus Station", "Marylebone Station"],
    )
    # {"Oxford Circus Station": 9, "Marylebone Station": 4}

Set GOOGLE_MAPS_API_KEY in .env.
If the key is missing or any call fails, returns None for that destination.
"""

import logging
import os
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logger    = logging.getLogger(__name__)
_API_KEY  = os.getenv("GOOGLE_MAPS_API_KEY", "")
_ENDPOINT = "https://maps.googleapis.com/maps/api/distancematrix/json"


def get_walk_minutes(
    origin: str,
    destinations: list[str],
) -> dict[str, Optional[int]]:
    """
    Return walking minutes from *origin* to each destination.

    Parameters
    ----------
    origin : str
        Full address of the property, e.g. "5 Marylebone Lane, London W1U 2PG".
    destinations : list[str]
        Place names or addresses to measure walk time to.

    Returns
    -------
    dict mapping each destination to walk minutes (int), or None on failure.
    """
    result: dict[str, Optional[int]] = {d: None for d in destinations}

    if not _API_KEY or _API_KEY == "your_google_maps_api_key_here":
        logger.warning("[walk_time] GOOGLE_MAPS_API_KEY not set — skipping walk times")
        return result

    if not destinations:
        return result

    try:
        resp = requests.get(
            _ENDPOINT,
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
            logger.warning("[walk_time] API status: %s", data.get("status"))
            return result

        elements = data.get("rows", [{}])[0].get("elements", [])
        for dest, elem in zip(destinations, elements):
            if elem.get("status") == "OK":
                result[dest] = round(elem["duration"]["value"] / 60)
            else:
                logger.debug("[walk_time] No result for %s: %s", dest, elem.get("status"))

    except Exception as exc:
        logger.error("[walk_time] Request failed: %s", exc)

    return result


def nearest_walk_minutes(
    origin: str,
    destinations: list[str],
) -> tuple[Optional[str], Optional[int]]:
    """
    Return (closest_destination, walk_minutes), or (None, None) if unavailable.
    """
    times = get_walk_minutes(origin, destinations)
    valid = {k: v for k, v in times.items() if v is not None}
    if not valid:
        return None, None
    best = min(valid, key=valid.__getitem__)
    return best, valid[best]
