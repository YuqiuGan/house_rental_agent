from urllib.parse import quote_plus
from zoneinfo import ZoneInfo
import requests
import datetime as dt
import os
import re
from langsmith import Client
from langchain.tools import BaseTool, StructuredTool, tool
from src.settings import GOOGLE_MAP_API_KEY, GOOGLE_PIC_SAVING_DIR


def get_departure_time(timeZone_str="America/New_York", departure_time_hour=8, departure_time_minute=0):

    departure_time = dt.time(departure_time_hour, departure_time_minute)
    timeZone = ZoneInfo(timeZone_str)
    now = dt.datetime.now(timeZone)
    days_ahead = (0 - now.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7

    target_date = (now.date() + dt.timedelta(days=days_ahead))
    aware_dt = dt.datetime.combine(target_date, departure_time, tzinfo=timeZone)

    unix_seconds = int(aware_dt.timestamp())
    rfc3339 = aware_dt.isoformat()    # e.g. '2025-09-01T08:00:00-04:00'
    return aware_dt, unix_seconds, rfc3339


def best_transit_route(origin, destination, mode: str = "transit", tz_name="America/New_York", general_departure_time_hour=8, general_departure_time_minute=0):

    _, unix_ts, _ = get_departure_time(tz_name, general_departure_time_hour, general_departure_time_minute)

    params = {
        "origin": origin,
        "destination": destination,
        "mode": mode,
        "alternatives": "true",
        "departure_time": unix_ts,
        "key": GOOGLE_MAP_API_KEY,
    }

    r = requests.get("https://maps.googleapis.com/maps/api/directions/json", params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "OK" or not data.get("routes"):
        raise RuntimeError(data.get("status"), data.get("error_message"))

    def route_key(route):
        leg = route["legs"][0]
        # Prefer earliest arrival_time if present (transit); fallback to total duration
        arr = leg.get("arrival_time", {}).get("value")  # unix seconds
        dur = leg["duration"]["value"]
        return (arr if arr is not None else 10**15, dur)

    best_route = min(data["routes"], key=route_key)
    return best_route

def static_map_with_route(route, origin_name='Default', path_color="0x0066FFE6", path_weight=6, maptype="roadmap", w=1000, h=700):
    base = "https://maps.googleapis.com/maps/api/staticmap"

    # create the path first

    poly = route["overview_polyline"]["points"]
    path = f"path=weight:{path_weight}|color:{path_color}|enc:{poly}"
    parts = [f"size={w}x{h}", "scale=2", f"maptype={maptype}",
             quote_plus(path, safe=":=|,")]

    # find the origin and destination
    first_leg = route["legs"][0]
    last_leg = route["legs"][-1]

    origin = first_leg["start_location"]
    destination = last_leg["end_location"]

    parts.append(f"markers=color:green|label:O|{origin['lat']},{origin['lng']}")
    parts.append(f"markers=color:red|label:D|{destination['lat']},{destination['lng']}")

    # add markers of each step

    walk_coords = []
    transit_coords = []

    for leg in route["legs"]:
        for step in leg["steps"]:
            sl = step["start_location"]
            lat, lng = sl["lat"], sl["lng"]

            if step["travel_mode"] == "WALKING":
                walk_coords.append((lat, lng))
            elif step["travel_mode"] == "TRANSIT":
                transit_coords.append((lat, lng))
            else:
                # optional: handle DRIVING, BICYCLING, etc.
                transit_coords.append((lat, lng))

    BATCH = 50
    for i in range(0, len(walk_coords), BATCH):
        batch = walk_coords[i:i+BATCH]
        coord_str = "|".join(f"{lat},{lng}" for lat, lng in batch)
        # parts.append(f"markers=size:mid|color:orange|label:W|{coord_str}")
        parts.append(
        f"markers=icon:https://maps.gstatic.com/mapfiles/ms2/micons/man.png|{coord_str}"
    )

    for i in range(0, len(transit_coords), BATCH):
        batch = transit_coords[i:i+BATCH]
        coord_str = "|".join(f"{lat},{lng}" for lat, lng in batch)
        # parts.append(f"markers=size:mid|color:green|label:T|{coord_str}")
        parts.append(
        f"markers=icon:https://maps.gstatic.com/mapfiles/ms2/micons/bus.png|{coord_str}"
    )

    # use google's autofit feature
    parts.append(f"visible={origin['lat']},{origin['lng']}|{destination['lat']},{destination['lng']}")

    parts.append(f"key={GOOGLE_MAP_API_KEY}")
    url = base + "?" + "&".join(parts)
    
    file_name = f"{origin_name}.png"
    output_fileName = os.path.join(GOOGLE_PIC_SAVING_DIR, file_name)
    img = requests.get(url, timeout=30)

    if img.status_code == 200:
        with open(output_fileName, "wb") as f:
            f.write(img.content)
        map_result = {"status": "ok", "path": output_fileName}
    else:
        map_result = {
            "status": "error",
            "code": img.status_code,
            "message": img.text[:200]
        }
    return map_result

@tool
def query_google_maps(origin, destination, origin_name, generate_route_map=False):

    """
    Find the best public transit route between two locations using Google Maps.

    Args:
        origin (str): Starting location (address or place name).
        destination (str): Destination location (address or place name).
        origin_name (str): Label used to name the saved map image (if generated).
        generate_route_map (bool, optional): Default value is False. If True, saves a static map PNG of the route.

    Returns:
        dict: {
            "duration_text": Trip duration in readable text (e.g. "25 mins"),
            "distance_text": Distance in readable text (e.g. "3.1 km"),
            "steps": List of steps (travel_mode, instructions, transit details),
            "map_result": Either a saved map path or "No route map generated"
        }

    """

    best_route = best_transit_route(origin, destination)
    leg = best_route["legs"][0] # if there is no waypoint
    duration_text = leg["duration"]["text"]
    duration_seconds = leg["duration"]["value"]
    distance_text = leg["distance"]["text"]
    steps = [(s["travel_mode"], s.get("html_instructions", ""), s.get("transit_details", {})) for s in leg["steps"]]

    if generate_route_map:
        safe_name = re.sub(r'[^A-Za-z0-9._-]+', '_', origin_name).strip('_')
        map_result = static_map_with_route(best_route, origin_name=safe_name)
        # map_result = static_map_with_route(best_route, origin_name=origin_name)
    else:
        map_result = "No route map generated"

    return {
        "duration_text": duration_text,
        "distance_text": distance_text,
        "steps": steps,
        "map_result": map_result
    }

