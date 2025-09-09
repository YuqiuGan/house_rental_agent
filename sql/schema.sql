-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm;   -- for fuzzy text search

DROP TABLE IF EXISTS listing CASCADE;
CREATE TABLE listing (

    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Unique external id
    external_id text UNIQUE NOT NULL,
    listing_data_source text NOT NULL,

    UNIQUE (listing_data_source, external_id),

    -- Address info
    address_unit        text,
    address_street      text,
    address_city        text,
    address_state       text,
    longitude_text      text,
    latitude_text       text,

    -- Essential info
    bedrooms            numeric(3,1),
    bathrooms           numeric(3,1),
    listing_price       numeric,
    year_built          smallint,
    longitude           double precision,
    latitude            double precision,
    home_type           text,
    living_area         numeric,
    rent_zestimate      numeric,
    photo_count         integer,
    hdp_url             text,
    has_approved_third_party_virtual_tour_url boolean,
    street_view_tile_image_url_medium_lat_long text,
    general_description         text,
    price_history       jsonb,
    nearby_homes        jsonb,
    is_instant_offer_enabled boolean,
    is_off_market       boolean,
    days_on_zillow      integer,
    virtual_tour_url    text,
    photos              text[],
    utilities           text[],
    interior_description jsonb,
    overview            jsonb,
    is_listed_by_management_company boolean,
    property_description jsonb,
    tags                text[],
    unit_amenities      text[],
    availability_date   date,
    getting_around_scores jsonb,

    -- Timestamp
    updated_at          timestamptz NOT NULL DEFAULT now(),
    created_at          timestamptz NOT NULL DEFAULT now()
);