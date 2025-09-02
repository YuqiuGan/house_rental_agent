from urllib.parse import quote_plus
from zoneinfo import ZoneInfo
import requests
import datetime as dt
import os
import re
import time
from langsmith import Client
from langchain.tools import BaseTool, StructuredTool, tool
from src.settings import ZILLOW_DATASET_ID, BRIGHTDATA_API_KEY, BRIGHTDATA_LISTING_SNAPSHOT_KEYS, BRIGHTDATA_LISTING_SNAPSHOT_DESCP

@tool
def submit_zillow_query_via_brightData(location: str, listingCategory: str = "House for rent", 
                                       HomeType: str = "Apartments", days_on_zillow: str = "7 days",
                                       limit_multiple_results: int = 10):

    """
    Calls Bright Data trigger endpoint and returns the API's JSON response verbatim.

    Parameters
    ----------
    location : str
        Target search location, e.g. "Hoboken, NJ".
    listingCategory : str, optional
        Listing category, default "House for rent".
    HomeType : str, optional
        Type of home, one of ["Apartments", "Houses", "Condos", "Townhomes"]. Default "Apartments".
    days_on_zillow : str, optional
        Time window for listing recency. One of:
        ["", "1 day", "7 days", "14 days", "30 days", "60 days", "90 days"]. "" means anytime.
        Default "7 days".
    limit_multiple_results : int, optional
        Maximum number of results Bright Data should return for this query.
        Default 10.

    Returns
    ----------
    On success: {"snapshot_id": "<id>"}
    On error:   {"error": "...", "code": "...", "type": "...", "line": "...", "index": N, "errors": [...]}
    If response is not JSON: {"error": "Non-JSON response", "status_code": <int>, "text": "<raw text>"}

    """

    url = "https://api.brightdata.com/datasets/v3/trigger"

    headers = {
        "Authorization": f"Bearer {BRIGHTDATA_API_KEY}",
        "Content-Type": "application/json",
    }

    params = {
        "dataset_id": ZILLOW_DATASET_ID,
        "include_errors": "true",
        "type": "discover_new",
        "discover_by": "input_filters",
        "limit_multiple_results": limit_multiple_results
    }

    data_tmp = {}
    data_tmp['location'] = location
    data_tmp['listingCategory'] = listingCategory
    data_tmp['HomeType'] = HomeType
    data_tmp['days_on_zillow'] = days_on_zillow
    data_tmp['exact_address'] = False

    data = [data_tmp]

    response = requests.post(url, headers=headers, params=params, json=data)

    if "application/json" in response.headers.get("content-type", ""):

        return response.json()
    
    return {"error": "Non-JSON response", "status_code": response.status_code, "text": response.text}

def get_listing_info(listing_item: dict,
    selected_keys: list,
    *,
    max_photos: int = 5,
    max_price_history: int = 5,
    max_desc_chars: int = 800):
    
    
    out = {k: listing_item.get(k, None) for k in selected_keys}

    photos = out.get("photos")
    if isinstance(photos, list):
        out["photos"] = photos[:max_photos]

    ph = out.get("priceHistory")
    if isinstance(ph, list):
        out["priceHistory"] = ph[-max_price_history:]  # most recent entries

    for text_key in ("description", "overview"):
        val = out.get(text_key)
        if isinstance(val, str) and len(val) > max_desc_chars:
            out[text_key] = val[:max_desc_chars].rstrip() + "…"

    return out


def process_brightdata_snapshot(snapshot_output, selected_keys, key_description, **kwargs):

    if isinstance(snapshot_output, dict) and snapshot_output.get("error"):
        return {"success": False, "error_payload": snapshot_output}

    if isinstance(snapshot_output, list):
        listings = [get_listing_info(item, selected_keys, **kwargs)
                    for item in snapshot_output if isinstance(item, dict)]
        return {"success": True, "count": len(listings), "listings": listings, "schema": key_description}

    return {"success": False, "error_payload": {"error": "Unexpected snapshot output type"}}


@tool
def retrieve_snapshot_from_brightData(snapshot_id: str, fmt="json", interval=60, max_attempts=3):

    """
    Retrieve Zillow listing data from a Bright Data snapshot.

    Summary
    -------
    Polls Bright Data's snapshot endpoint until the snapshot is ready (HTTP 200)
    or max attempts are reached.

    Parameters
    ----------
    snapshot_id : str
        The Bright Data snapshot ID returned from the trigger/submit call.
    fmt : str, default "json"
        Response format requested from Bright Data (kept for completeness).
    interval : int, default 60
        Seconds to wait between attempts when the snapshot is still processing (HTTP 202).
    max_attempts : int, default 3
        Maximum number of polling attempts before returning a timeout object.

    Returns
    -------
    dict
        On success (HTTP 200 → processed):
          {
            "success": True,
            "count": <int>,
            "listings": [ {<filtered listing dict>}, ... ],
            "schema": { <key -> description> }
          }

        If still processing after all attempts (timeout):
          {
            "error": True,
            "status_code": 408,
            "message": "Snapshot <id> not ready after <seconds> seconds"
          }

        On immediate HTTP error (non-200/202):
          {
            "error": True,
            "status_code": <int>,
            "message": <response text>
          }

    Agent Usage Notes
    -----------------
    - Call this tool with a known `snapshot_id`.
    - If the result has `"success": True`, proceed to reason over `listings`.
    - If the result has `"error": True` with `status_code == 408`, the snapshot
      is likely still processing; the agent may retry this tool later.
    - Any other `"error": True` typically indicates a permanent issue; decide
      whether to re-submit the query or report the error upstream.

    """

    url = f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}"
    params = {"format": fmt}

    RETRIEVE_HEADERS = {"Authorization": f"Bearer {BRIGHTDATA_API_KEY}"}

    for attempt in range(max_attempts):
        resp = requests.get(url, headers=RETRIEVE_HEADERS, params=params)

        if resp.status_code == 200:
            
            return process_brightdata_snapshot(resp.json(), BRIGHTDATA_LISTING_SNAPSHOT_KEYS, BRIGHTDATA_LISTING_SNAPSHOT_DESCP)

        elif resp.status_code == 202:
            print(f"[{attempt+1}/{max_attempts}] Snapshot not ready, retrying in {interval}s…")
            time.sleep(interval)

        else:
            print("Unable to retrieve the Snapshot right now, please try it later...")
            return {
                "error": True,
                "status_code": resp.status_code,
                "message": resp.text
            }

    return {
        "error": True,
        "status_code": 408,
        "message": f"Snapshot {snapshot_id} not ready after {interval*max_attempts} seconds"
    }