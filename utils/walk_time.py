"""
utils/walk_time.py
------------------
Returns the nearest station for a property.

NO external API is used. Every property is scraped within a 0.5-mile radius
(≈10 min walk) of its station by design — all scrapers search with radius=0.5
miles. So the nearest station is simply the area the listing was scraped from,
and the walk time is a conservative 10 minutes. This is accurate for our use
and costs nothing, so there is no geocoding, no network call, and nothing that
can hang or fail.

(Google Maps was removed: the scrapers already enforce the radius, and the
Geocoding API key kept getting REQUEST_DENIED and falling back to a slow free
geocoder anyway — pure overhead.)
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

_DEFAULT_WALK_MINS = 10  # conservative for a 0.5-mile radius


def nearest_walk_minutes(
    origin: str = "",
    destinations: list[str] = None,   # ignored — kept for backwards compat
    listing: dict = None,
) -> tuple[Optional[str], Optional[int]]:
    """
    Return (station_name, walk_minutes) for the property.

    The station is the area the listing was scraped from (every listing is
    within 0.5 mi of it). No API calls — fast and can't fail.
    """
    if listing:
        station = listing.get("area") or "Nearest station"
        return station, _DEFAULT_WALK_MINS
    return None, None


def get_walk_minutes(
    origin: str,
    destinations: list[str],
) -> dict[str, Optional[int]]:
    """Backwards-compat shim — no external API, returns the default for each."""
    return {d: _DEFAULT_WALK_MINS for d in (destinations or [])}
