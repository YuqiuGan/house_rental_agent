import os
import json
from typing import Dict, Iterable, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from langsmith import Client
from langchain.tools import BaseTool, StructuredTool, tool
from src.settings import DATABASE_URL
import datetime as dt
from src.init_db_engine import get_engine

JSON_COLUMNS = {
    "price_history",
    "nearby_homes",
    "interior_description",
    "overview",
    "property_description",
    "getting_around_scores"
}

ALL_COLUMNS = [
    # identity
    "external_id",
    "listing_data_source",
    # address
    "address_unit", "address_street", "address_city", "address_state",
    # geo raw (optional; keep if you store original strings)
    "longitude_text", "latitude_text",
    # essentials
    "bedrooms", "bathrooms", "listing_price", "year_built",
    "longitude", "latitude", "home_type", "living_area",
    "rent_zestimate", "photo_count", "hdp_url",
    "has_approved_third_party_virtual_tour_url",
    "street_view_tile_image_url_medium_lat_long",
    "general_description", "price_history", "nearby_homes",
    "is_instant_offer_enabled", "is_off_market", "days_on_zillow",
    "virtual_tour_url", "photos", "utilities", "interior_description",
    "overview", "is_listed_by_management_company",
    "property_description", "tags", "unit_amenities",
    "availability_date", "getting_around_scores",
    # timestamps
    "updated_at"
]

UPSERT_SQL = text("""
INSERT INTO listing (
    external_id, listing_data_source,
    address_unit, address_street, address_city, address_state,
    longitude_text, latitude_text,
    bedrooms, bathrooms, listing_price, year_built,
    longitude, latitude, home_type, living_area,
    rent_zestimate, photo_count, hdp_url,
    has_approved_third_party_virtual_tour_url,
    street_view_tile_image_url_medium_lat_long,
    general_description, price_history, nearby_homes,
    is_instant_offer_enabled, is_off_market, days_on_zillow,
    virtual_tour_url, photos, utilities, interior_description,
    overview, is_listed_by_management_company, property_description,
    tags, unit_amenities, availability_date, getting_around_scores,
    updated_at
) VALUES (
    :external_id, :listing_data_source,
    :address_unit, :address_street, :address_city, :address_state,
    :longitude_text, :latitude_text,
    :bedrooms, :bathrooms, :listing_price, :year_built,
    :longitude, :latitude, :home_type, :living_area,
    :rent_zestimate, :photo_count, :hdp_url,
    :has_approved_third_party_virtual_tour_url,
    :street_view_tile_image_url_medium_lat_long,
    :general_description, CAST(:price_history AS jsonb), CAST(:nearby_homes AS jsonb),
    :is_instant_offer_enabled, :is_off_market, :days_on_zillow,
    :virtual_tour_url, :photos, :utilities, CAST(:interior_description AS jsonb),
    CAST(:overview AS jsonb), :is_listed_by_management_company, CAST(:property_description AS jsonb),
    :tags, :unit_amenities, :availability_date, CAST(:getting_around_scores AS jsonb),
    :updated_at
)
ON CONFLICT (listing_data_source, external_id)
DO UPDATE SET
    address_unit   = EXCLUDED.address_unit,
    address_street = EXCLUDED.address_street,
    address_city   = EXCLUDED.address_city,
    address_state  = EXCLUDED.address_state,
    longitude_text = EXCLUDED.longitude_text,
    latitude_text  = EXCLUDED.latitude_text,
    bedrooms       = EXCLUDED.bedrooms,
    bathrooms      = EXCLUDED.bathrooms,
    listing_price  = EXCLUDED.listing_price,
    year_built     = EXCLUDED.year_built,
    longitude      = EXCLUDED.longitude,
    latitude       = EXCLUDED.latitude,
    home_type      = EXCLUDED.home_type,
    living_area    = EXCLUDED.living_area,
    rent_zestimate = EXCLUDED.rent_zestimate,
    photo_count    = EXCLUDED.photo_count,
    hdp_url        = EXCLUDED.hdp_url,
    has_approved_third_party_virtual_tour_url = EXCLUDED.has_approved_third_party_virtual_tour_url,
    street_view_tile_image_url_medium_lat_long = EXCLUDED.street_view_tile_image_url_medium_lat_long,
    general_description = EXCLUDED.general_description,
    price_history  = COALESCE(listing.price_history, '{}'::jsonb) || COALESCE(EXCLUDED.price_history, '{}'::jsonb),
    nearby_homes   = EXCLUDED.nearby_homes,
    is_instant_offer_enabled = EXCLUDED.is_instant_offer_enabled,
    is_off_market  = EXCLUDED.is_off_market,
    days_on_zillow = EXCLUDED.days_on_zillow,
    virtual_tour_url = EXCLUDED.virtual_tour_url,
    photos         = EXCLUDED.photos,
    utilities      = EXCLUDED.utilities,
    interior_description = EXCLUDED.interior_description,
    overview       = EXCLUDED.overview,
    is_listed_by_management_company = EXCLUDED.is_listed_by_management_company,
    property_description = EXCLUDED.property_description,
    tags           = EXCLUDED.tags,
    unit_amenities = EXCLUDED.unit_amenities,
    availability_date = EXCLUDED.availability_date,
    getting_around_scores = EXCLUDED.getting_around_scores,
    updated_at     = COALESCE(EXCLUDED.updated_at, listing.updated_at);
""")

# _engine: Optional[Engine] = None

def _engine_cached():
    return get_engine()


def _coerce_payload(record: Dict):
    """Ensure all expected keys exist; JSON fields are serialized; nulls allowed."""
    payload = {}
    for col in ALL_COLUMNS:
        val = record.get(col, None)
        if col in JSON_COLUMNS:
            # Accept dict/list/str; store as JSON string for CAST(:param AS jsonb)
            if val is None:
                payload[col] = None
            elif isinstance(val, (dict, list)):
                payload[col] = json.dumps(val)
            else:
                # assume already a JSON string
                payload[col] = str(val)
        else:
            payload[col] = val
    return payload

def upsert_listing(record: Dict):
    """
    Insert or update a single listing.
    Required keys: external_id (str), listing_data_source (str).
    Other keys are optional.
    """
    if not record.get("external_id") or not record.get("listing_data_source"):
        # raise ValueError("Both 'external_id' and 'listing_data_source' are required.")
        return {"error": True, "message": "Missing 'external_id' and 'listing_data_source', failed to upsert"}
    payload = _coerce_payload(record)
    with _engine_cached().begin() as conn:
        conn.execute(UPSERT_SQL, payload)
    return {"error": False, "message": "Successfully upsert the listing"}

def _get_listing_photos(photos: list):
    if photos:
        listing_photo_list = []
        for item in photos:
            listing_photo_list.append(item.get('mixedSources').get('jpeg')[-1].get('url'))
        return listing_photo_list
    else:
        return

def _get_nearby_homes(nearbyHome: list):
    if nearbyHome:
        nh_count = 0
        nearby_home_list = []
        for home_json_str in nearbyHome:
            if nh_count < 10:
                data = json.loads(home_json_str)
                nearby_home = {}
                nearby_home['external_id'] = data.get('zpid')
                nearby_home['address'] = data.get('address')
                nearby_home_list.append(nearby_home)
                nh_count += 1
        return nearby_home_list
    else:
        return []

def _get_price_history(price_history: list, now):
    if price_history:
        ph_count = 0
        history_list = []
        for his in price_history:
            if ph_count < 5:
                if his.get('date') > now.strftime('%Y-%m-%d'):
                    history = {}
                    history['date'] = his.get('date')
                    history['event'] = his.get('event')
                    history['price'] = his.get('price')
                    history_list.append(history)
                    ph_count += 1
        return history_list
    else:
        return []

def load_zillow_listing(file_path: str):

    with open(file_path, 'r') as file:
        data = json.load(file)

    listings = []

    for lst in data:
        listing = {}
        # system time
        now = dt.datetime.today()

        # required fields
        listing['external_id'] = lst.get('zpid')
        listing['listing_data_source'] = 'zillow'

        if lst.get('zpid'):
            # optional fields
            listing['address_unit']         = lst.get('address').get('streetAddress')
            listing['address_street']       = lst.get('address').get('streetAddress')
            listing['address_city']         = lst.get('address').get('city')
            listing['address_state']        = lst.get('address').get('state')
            listing['longitude_text']       = str(lst.get('longitude'))
            listing['latitude_text']        = str(lst.get('latitude'))
            listing['bedrooms']             = lst.get('bedrooms')
            listing['bathrooms']            = lst.get('bathrooms')
            listing['listing_price']        = lst.get('price')
            listing['year_built']           = lst.get('yearBuilt')
            listing['longitude']            = lst.get('longitude')
            listing['latitude']             = lst.get('latitude')
            listing['home_type']            = lst.get('homeType')
            listing['living_area']          = lst.get('livingAreaValue') if not lst.get('livingArea') else lst.get('livingArea')
            listing['rent_zestimate']       = lst.get('rentZestimate')
            listing['photo_count']          = lst.get('photoCount')
            listing['hdp_url']              = lst.get('hdpUrl')
            listing['has_approved_third_party_virtual_tour_url'] = lst.get('hasApprovedThirdPartyVirtualTourUrl')
            listing['street_view_tile_image_url_medium_lat_long'] = lst.get('streetViewTileImageUrlMediumLatLong')
            listing['general_description']  = lst.get('description')
            listing['price_history']        = _get_price_history(lst.get('priceHistory'), now)
            listing['nearby_homes']         = _get_nearby_homes(lst.get('nearbyHomes'))
            listing['is_instant_offer_enabled'] = True if lst.get('isInstantOfferEnabled') == 'Yes' else False
            listing['is_off_market']        = lst.get('isOffMarket')
            listing['days_on_zillow']       = lst.get('days_on_zillow') if not lst.get('daysOnZillow') else lst.get('daysOnZillow')
            listing['virtual_tour_url']     = lst.get('virtualTourUrl')
            listing['photos']               = _get_listing_photos(lst.get('photos'))
            listing['utilities']            = lst.get('utilities')
            listing['interior_description'] = lst.get('interior_full')
            listing['overview']             = lst.get('overview')
            listing['is_listed_by_management_company'] = lst.get('is_listed_by_management_company')
            listing['property_description'] = lst.get('property')
            listing['tags']                 = lst.get('tags')
            listing['unit_amenities']       = lst.get('unit_amenities')
            listing['availability_date']    = lst.get('availability_date')
            listing['getting_around_scores'] = lst.get('getting_around')
            listing['updated_at']           = now.strftime('%Y-%m-%d')

            listings.append(listing)

    return listings

@tool
def bulk_upsert_listings(snapshot_file_path: str):
    
    """
    Summary
    -------
    Bulk insert/update many listings to the listing table in the database.

    Input Parameters
    -------
    snapshot_file_path: the file path of saved snapshot json file.

    Returns
    -------
    dict

    when success: {"error": False, "message": "<text>"}

    when failed: {"error": True, "message": "<text>"}

    """

    records = load_zillow_listing(snapshot_file_path)
    rows = []
    failed = 0
    print("Calling bulk_upsert_listings function...")
    for r in records:
        if not r.get("external_id") or not r.get("listing_data_source"):
            # Skip or raise depending on your policy
            failed += 1
            continue
        rows.append(_coerce_payload(r))
    if not rows:
        return {"error": True, "message": "Missing 'external_id' and 'listing_data_source', failed to upsert"}
    with _engine_cached().begin() as conn:
        conn.execute(UPSERT_SQL, rows)   # SQLAlchemy will executemany()
    return {"error": False, "message": f"Successfully upsert the {len(rows)} listings, failed {failed} listings"}