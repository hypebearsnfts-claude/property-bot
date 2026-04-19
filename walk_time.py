import os, requests

AREA_STATIONS = {
    "Covent Garden":   ["Covent Garden Station, London", "Holborn Station, London"],
    "Soho":            ["Tottenham Court Road Station, London", "Piccadilly Circus Station, London"],
    "Knightsbridge":   ["Knightsbridge Station, London", "South Kensington Station, London"],
    "West Kensington": ["West Kensington Station, London", "Kensington Olympia Station, London"],
    "London Bridge":   ["London Bridge Station, London", "Borough Station, London"],
    "Tower Hill":      ["Tower Hill Station, London", "Aldgate Station, London"],
    "Baker Street":    ["Baker Street Station, London", "Marylebone Station, London"],
    "Bond Street":     ["Bond Street Station, London", "Oxford Circus Station, London"],
    "Marble Arch":     ["Marble Arch Station, London", "Bond Street Station, London"],
    "Oxford Circus":   ["Oxford Circus Station, London", "Tottenham Court Road Station, London"],
    "Marylebone":      ["Marylebone Station, London", "Baker Street Station, London"],
    "Regent's Park": ["Regent's Park Station, London", "Baker Street Station, London"],
}
_ALL = list(dict.fromkeys(s for v in AREA_STATIONS.values() for s in v))

def get_walk_time(address, area=None, api_key=None, max_minutes=10):
    key = api_key or os.getenv("GOOGLE_MAPS_API_KEY")
    if not key:
        return {"within_10_min": None, "station": None, "minutes": None, "error": "No API key"}
    stations = AREA_STATIONS.get(area, _ALL) if area else _ALL
    try:
        r = requests.get("https://maps.googleapis.com/maps/api/distancematrix/json",
            params={"origins": address, "destinations": "|".join(stations),
                    "mode": "walking", "units": "metric", "key": key}, timeout=10)
        data = r.json()
    except Exception as e:
        return {"within_10_min": None, "station": None, "minutes": None, "error": str(e)}
    if data.get("status") != "OK":
        return {"within_10_min": None, "station": None, "minutes": None, "error": data.get("status")}
    best, best_s = None, None
    for i, el in enumerate((data.get("rows") or [{}])[0].get("elements", [])):
        if el.get("status") == "OK":
            m = round(el["duration"]["value"] / 60)
            if best is None or m < best:
                best, best_s = m, stations[i].replace(" Station, London","").replace(", London","")
    if best is None:
        return {"within_10_min": None, "station": None, "minutes": None, "error": "No results"}
    return {"within_10_min": best <= max_minutes, "station": best_s, "minutes": best, "error": None}

if __name__ == "__main__":
    import sys
    addr = sys.argv[1] if len(sys.argv) > 1 else "Tavistock Street, London, WC2E"
    area = sys.argv[2] if len(sys.argv) > 2 else "Covent Garden"
    r = get_walk_time(addr, area)
    print(r)
    if r["within_10_min"]: print(f"PASS: {r['minutes']} min to {r['station']}")
    elif r["within_10_min"] is False: print(f"FAIL: {r['minutes']} min to {r['station']}")
    else: print("ERROR:", r["error"])
