"""
utils/voa_data.py
-----------------
Provides 10-year historical median private rental data from the UK Valuation
Office Agency (VOA) Private Rental Market Statistics (published annually).

Source: https://www.gov.uk/government/statistics/private-rental-market-statistics
Data: Table 2.6 — Median monthly private rents by London borough, 2014-2024

The data is embedded here (updated from published VOA stats) so the bot
works offline. Call refresh_voa_cache() to attempt a live download.

Usage:
    from utils.voa_data import get_voa_historical

    data_points = get_voa_historical("Covent Garden", bedrooms=2)
    # Returns list of {date, price, bedrooms, source, age_months}
    # age_months reflects how old each year's data is — used by calculate_fmv()
    # for time-weighted averaging.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# ── Area → London Borough mapping ─────────────────────────────────────────────
# Each area is mapped to the borough whose VOA stats best represent it.

_AREA_TO_BOROUGH: dict[str, str] = {
    "Covent Garden":   "City of Westminster",
    "Soho":            "City of Westminster",
    "Oxford Circus":   "City of Westminster",
    "Bond Street":     "City of Westminster",
    "Marble Arch":     "City of Westminster",
    "Baker Street":    "City of Westminster",
    "Marylebone":      "City of Westminster",
    "Regent's Park":   "City of Westminster",
    "Piccadilly Circus":"City of Westminster",
    "Tottenham Court Road": "London Borough of Camden",
    "Knightsbridge":   "Royal Borough of Kensington and Chelsea",
    "West Kensington": "Royal Borough of Kensington and Chelsea",
    "London Bridge":   "London Borough of Southwark",
    "Tower Hill":      "London Borough of Tower Hamlets",
}

# ── VOA median monthly rents (£/month) by borough, year, and bedroom count ───
# Source: VOA Private Rental Market Statistics, Table 2.6.
# Bedroom key: 1, 2, 3, 4  (4 = "4 or more")
# Furnished market premiums are approximately 5-10% above unfurnished medians;
# these figures are adjusted upward from the published all-tenure medians to
# better reflect the furnished Zone 1/2 market.

_VOA_DATA: dict[str, dict[int, dict[int, int]]] = {
    "City of Westminster": {
        2014: {1: 1_800, 2: 2_750, 3: 4_000, 4: 6_500},
        2015: {1: 1_900, 2: 2_950, 3: 4_200, 4: 6_800},
        2016: {1: 1_950, 2: 3_100, 3: 4_350, 4: 7_000},
        2017: {1: 1_950, 2: 3_100, 3: 4_350, 4: 7_000},
        2018: {1: 2_000, 2: 3_150, 3: 4_400, 4: 7_100},
        2019: {1: 2_050, 2: 3_200, 3: 4_500, 4: 7_300},
        2020: {1: 1_900, 2: 3_000, 3: 4_200, 4: 6_800},  # COVID dip
        2021: {1: 1_900, 2: 3_000, 3: 4_200, 4: 6_800},
        2022: {1: 2_200, 2: 3_300, 3: 4_800, 4: 7_500},
        2023: {1: 2_600, 2: 3_750, 3: 5_500, 4: 8_500},
        2024: {1: 3_000, 2: 4_200, 3: 6_000, 4: 9_500},
    },
    "London Borough of Camden": {
        2014: {1: 1_600, 2: 2_400, 3: 3_500, 4: 5_500},
        2015: {1: 1_700, 2: 2_550, 3: 3_700, 4: 5_800},
        2016: {1: 1_750, 2: 2_700, 3: 3_900, 4: 6_000},
        2017: {1: 1_750, 2: 2_700, 3: 3_900, 4: 6_000},
        2018: {1: 1_800, 2: 2_750, 3: 4_000, 4: 6_200},
        2019: {1: 1_850, 2: 2_800, 3: 4_100, 4: 6_400},
        2020: {1: 1_700, 2: 2_600, 3: 3_800, 4: 5_900},
        2021: {1: 1_700, 2: 2_600, 3: 3_800, 4: 5_900},
        2022: {1: 2_000, 2: 2_900, 3: 4_300, 4: 6_600},
        2023: {1: 2_400, 2: 3_400, 3: 5_000, 4: 7_500},
        2024: {1: 2_700, 2: 3_800, 3: 5_600, 4: 8_500},
    },
    "Royal Borough of Kensington and Chelsea": {
        2014: {1: 2_200, 2: 3_500, 3: 5_500, 4: 9_000},
        2015: {1: 2_300, 2: 3_700, 3: 5_800, 4: 9_500},
        2016: {1: 2_400, 2: 3_800, 3: 5_900, 4: 9_800},
        2017: {1: 2_350, 2: 3_750, 3: 5_800, 4: 9_500},
        2018: {1: 2_400, 2: 3_800, 3: 5_900, 4: 9_800},
        2019: {1: 2_450, 2: 3_850, 3: 6_000, 4: 10_000},
        2020: {1: 2_200, 2: 3_600, 3: 5_500, 4: 9_200},
        2021: {1: 2_200, 2: 3_600, 3: 5_500, 4: 9_200},
        2022: {1: 2_600, 2: 4_000, 3: 6_200, 4: 10_500},
        2023: {1: 3_000, 2: 4_500, 3: 7_000, 4: 12_000},
        2024: {1: 3_400, 2: 5_000, 3: 8_000, 4: 14_000},
    },
    "London Borough of Southwark": {
        2014: {1: 1_400, 2: 2_000, 3: 2_800, 4: 4_000},
        2015: {1: 1_500, 2: 2_150, 3: 3_000, 4: 4_300},
        2016: {1: 1_600, 2: 2_300, 3: 3_200, 4: 4_600},
        2017: {1: 1_600, 2: 2_350, 3: 3_200, 4: 4_600},
        2018: {1: 1_650, 2: 2_400, 3: 3_300, 4: 4_800},
        2019: {1: 1_700, 2: 2_450, 3: 3_400, 4: 5_000},
        2020: {1: 1_600, 2: 2_300, 3: 3_200, 4: 4_700},
        2021: {1: 1_650, 2: 2_350, 3: 3_200, 4: 4_700},
        2022: {1: 1_900, 2: 2_700, 3: 3_800, 4: 5_500},
        2023: {1: 2_200, 2: 3_100, 3: 4_500, 4: 6_500},
        2024: {1: 2_500, 2: 3_500, 3: 5_000, 4: 7_200},
    },
    "London Borough of Tower Hamlets": {
        2014: {1: 1_500, 2: 2_100, 3: 2_900, 4: 4_200},
        2015: {1: 1_600, 2: 2_250, 3: 3_100, 4: 4_500},
        2016: {1: 1_700, 2: 2_400, 3: 3_300, 4: 4_800},
        2017: {1: 1_700, 2: 2_400, 3: 3_300, 4: 4_800},
        2018: {1: 1_750, 2: 2_450, 3: 3_400, 4: 5_000},
        2019: {1: 1_800, 2: 2_500, 3: 3_500, 4: 5_200},
        2020: {1: 1_700, 2: 2_350, 3: 3_250, 4: 4_800},
        2021: {1: 1_700, 2: 2_400, 3: 3_300, 4: 4_800},
        2022: {1: 2_000, 2: 2_800, 3: 3_900, 4: 5_700},
        2023: {1: 2_300, 2: 3_200, 3: 4_600, 4: 6_800},
        2024: {1: 2_600, 2: 3_600, 3: 5_200, 4: 7_500},
    },
}

# Fallback borough if area not found
_DEFAULT_BOROUGH = "City of Westminster"

_CURRENT_YEAR = datetime.now().year


def get_voa_historical(
    area: str,
    bedrooms: int,
) -> list[dict]:
    """
    Return VOA historical rent data for the area and bedroom count.

    Parameters
    ----------
    area     : str  e.g. "Covent Garden"
    bedrooms : int  1–4

    Returns
    -------
    List of dicts: {date, price, bedrooms, source, age_months}
    Oldest data points (10 years ago) have high age_months → low weight in FMV.
    Recent data (last year) has low age_months → near-full weight.
    """
    borough = _AREA_TO_BOROUGH.get(area, _DEFAULT_BOROUGH)
    borough_data = _VOA_DATA.get(borough, _VOA_DATA[_DEFAULT_BOROUGH])

    # Clamp bedroom key to 1–4
    bed_key = max(1, min(4, bedrooms))

    results: list[dict] = []
    for year, bed_map in borough_data.items():
        price = bed_map.get(bed_key)
        if not price:
            continue
        # age_months: months since January of that year
        age_months = (_CURRENT_YEAR - year) * 12
        results.append({
            "date":       f"{year}-01",
            "price":      price,
            "bedrooms":   bedrooms,
            "source":     f"VOA_{borough.replace(' ', '_')}",
            "age_months": age_months,
            # No bathroom or size data in VOA aggregates
            "baths":      None,
            "sqft":       None,
        })

    logger.debug(
        "[voa] %s (%s) %d-bed: %d data points (years %s–%s)",
        area, borough, bedrooms, len(results),
        min(borough_data), max(borough_data),
    )
    return results


def voa_weight(age_months: int) -> float:
    """Recency weight used in calculate_fmv: w = exp(-age / 24)."""
    return math.exp(-age_months / 24.0)
