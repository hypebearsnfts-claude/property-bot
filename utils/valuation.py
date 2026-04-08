"""
utils/valuation.py
------------------
Uses the Anthropic Claude API to score and summarise a property listing.

Usage:
    from utils.valuation import score_listing

    result = score_listing({
        "source":  "zoopla",
        "area":    "Marylebone",
        "title":   "Weymouth Street, London W1W",
        "price":   "£3,500 pcm",
        "address": "Weymouth Street, London W1W",
        "url":     "https://www.zoopla.co.uk/to-rent/details/12345/",
    })
    # {
    #   "score": 8,
    #   "summary": "Spacious 2-bed flat in prime Marylebone. Good value at £3,500 pcm.",
    #   "flags": ["close to tube", "quiet street"],
    # }

Set ANTHROPIC_API_KEY in .env.
If the key is missing the function returns a default neutral result.
"""

import logging
import os
from typing import Optional

import anthropic
from dotenv import load_dotenv

load_dotenv()

logger   = logging.getLogger(__name__)
_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
_MODEL   = "claude-haiku-4-5-20251001"   # fast + cheap for bulk scoring

_SYSTEM_PROMPT = """\
You are a London property analyst helping a professional find a furnished 2-bedroom flat to rent.
The ideal property is:
- Furnished, 2+ bedrooms
- In or very close to Zone 1/2 central London
- Well-connected (walking distance to tube)
- Good value for the area

For each listing, respond with ONLY valid JSON in this exact format:
{
  "score": <integer 1-10>,
  "summary": "<one concise sentence describing the property and why it is or isn't worth viewing>",
  "flags": ["<short flag>", ...]
}

Scoring guide:
10 = exceptional value, must view
7-9 = strong candidate
4-6 = worth considering
1-3 = overpriced or unsuitable

Flags are short labels like: "great value", "overpriced", "quiet street",
"near tube", "conversion", "high floor", "new build", "period property".
Keep to 2-4 flags maximum.
"""


def score_listing(listing: dict) -> dict:
    """
    Score and summarise a property listing using Claude.

    Parameters
    ----------
    listing : dict
        Keys: source, area, title, price, address, url
        Optional extra keys (e.g. walk_mins) are included if present.

    Returns
    -------
    dict with keys: score (int), summary (str), flags (list[str])
    """
    _default = {"score": 5, "summary": "No AI summary available.", "flags": []}

    if not _API_KEY or _API_KEY == "your_anthropic_api_key_here":
        logger.warning("[valuation] ANTHROPIC_API_KEY not set — skipping scoring")
        return _default

    # Build the user message
    walk_line = ""
    if listing.get("walk_dest") and listing.get("walk_mins") is not None:
        walk_line = f"\nWalk time to {listing['walk_dest']}: {listing['walk_mins']} min"

    user_msg = (
        f"Source: {listing.get('source', 'unknown')}\n"
        f"Area: {listing.get('area', 'unknown')}\n"
        f"Price: {listing.get('price', 'unknown')}\n"
        f"Address: {listing.get('address', 'unknown')}"
        f"{walk_line}\n"
        f"URL: {listing.get('url', '')}"
    )

    try:
        client = anthropic.Anthropic(api_key=_API_KEY)
        message = client.messages.create(
            model=_MODEL,
            max_tokens=256,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": (__import__("json").dumps(user_msg, indent=2) if isinstance(user_msg, dict) else str(user_msg))}],
        )
        raw = message.content[0].text.strip() if message.content else ""
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"): raw = raw[4:]
            raw = raw.strip()
        if not raw:
            return _default
        import json
        data = json.loads(raw)
        return {
            "score":   int(data.get("score", 5)),
            "summary": str(data.get("summary", "")),
            "flags":   list(data.get("flags", [])),
        }

    except Exception as exc:
        logger.error("[valuation] Failed to score listing %s: %s", listing.get("url"), exc)
        return _default
