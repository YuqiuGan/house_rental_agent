import os
from typing import Dict, Iterable, Optional, List, Dict, Any, Tuple, Union
from pydantic import BaseModel, field_validator
import json, re
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from langsmith import Client
from langchain.tools import BaseTool, StructuredTool, tool
from src.settings import DATABASE_URL
import datetime as dt
from sqlalchemy import create_engine, select, asc, desc, and_, or_
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy import String, Text, Integer, Float, Boolean, Date, Double, MetaData, Table
from src.init_db_engine import get_engine


class AgentQueryDatabaseInput(BaseModel):
    # Prefer dict, but accept str and parse it.
    spec: Union[Dict[str, Any], str]

    @field_validator("spec", mode="before")
    @classmethod
    def parse_spec(cls, v):
        if isinstance(v, str):
            s = v.strip()
            if s.startswith("```"):
                s = re.sub(r"^```[a-zA-Z0-9]*\n", "", s)
                s = re.sub(r"\n```$", "", s)
            return json.loads(s)
        return v


ALLOWED_SELECT = {"external_id","listing_data_source","address_unit","address_street","address_city","address_state","longitude_text","latitude_text","bedrooms","bathrooms",
    "listing_price","year_built","longitude","latitude","home_type","living_area","rent_zestimate","photo_count","hdp_url","has_approved_third_party_virtual_tour_url",
    "street_view_tile_image_url_medium_lat_long","general_description","price_history","nearby_homes","is_instant_offer_enabled","is_off_market","days_on_zillow",
    "virtual_tour_url","photos","utilities","interior_description","overview","is_listed_by_management_company","property_description","tags","unit_amenities",
    "availability_date","getting_around_scores","updated_at","created_at"}

ALLOWED_FIELDS = {'external_id', 'listing_data_source', 'address_city', 'address_state', 'bedrooms', 'bathrooms', 'listing_price',
     'year_built', 'home_type', 'living_area', 'days_on_zillow', 'availability_date', 'updated_at', 'created_at'}

ALLOWED_OPS = {"=", "!=", ">", ">=", "<", "<=", "ilike", "in", "between"}

MAX_LIMIT = 50

DEFAULT_SELECT = ["external_id","listing_data_source","address_unit","address_street","address_city","address_state","longitude_text","latitude_text","bedrooms","bathrooms",
    "listing_price","year_built","longitude","latitude","home_type","living_area","rent_zestimate","photo_count","hdp_url","has_approved_third_party_virtual_tour_url",
    "street_view_tile_image_url_medium_lat_long","general_description","price_history","nearby_homes","is_instant_offer_enabled","is_off_market","days_on_zillow",
    "virtual_tour_url","photos","utilities","interior_description","overview","is_listed_by_management_company","property_description","tags","unit_amenities",
    "availability_date","getting_around_scores","updated_at","created_at"]

# _engine: Optional[Engine] = None

# def _engine_cached() -> Engine:
#     global _engine
#     if _engine is None:
#         _engine = create_engine(DATABASE_URL, pool_pre_ping=True)
#     return _engine

def _engine_cached():
    return get_engine()


_metadata = MetaData()
_LISTING = None

def _listing_table(engine: Engine) -> Table:
    global _LISTING
    if _LISTING is None:
        _LISTING = Table("listing", _metadata, autoload_with=engine)
    return _LISTING

def col(name: str):
    return _listing_table(_engine_cached()).c[name]

# def col(name: str):
    
#     return getattr(Listing, name)

def sanitize_agent_spec(spec: Dict[str, Any]):
    errors, warnings = [], []

    # select
    req_select = spec.get("select") or DEFAULT_SELECT
    good_select = [c for c in req_select if c in ALLOWED_SELECT]
    bad_select  = [c for c in req_select if c not in ALLOWED_SELECT]
    if bad_select:
        warnings.append({"select_dropped": bad_select})
    if not good_select:
        good_select = DEFAULT_SELECT
        warnings.append({"select_defaulted": DEFAULT_SELECT})

    # where
    where_and = []
    for i, cond in enumerate(spec.get("where") or []):
        field, op, val = cond.get("field"), cond.get("op"), cond.get("value")
        if field not in ALLOWED_FIELDS:
            errors.append({"where_invalid_field": cond})
            continue
        if op not in ALLOWED_OPS:
            errors.append({"where_invalid_op": cond})
            continue
        # normalize shapes
        if op == "in":
            if not isinstance(val, (list, tuple)) or not val:
                errors.append({"where_invalid_in_value": cond})
                continue
        if op == "between":
            if not (isinstance(val, (list, tuple)) and len(val) == 2):
                errors.append({"where_invalid_between_value": cond})
                continue
        if op == "ilike" and not isinstance(val, str):
            errors.append({"where_ilike_needs_string": cond})
            continue
        where_and.append({"field": field, "op": op, "value": val})

    # or
    where_or = []
    for cond in (spec.get("where_any") or []):
        field, op, val = cond.get("field"), cond.get("op"), cond.get("value")
        if field not in ALLOWED_FIELDS:
            errors.append({"where_any_invalid_field": cond}); continue
        if op not in ALLOWED_OPS:
            errors.append({"where_any_invalid_op": cond}); continue
        if op == "in":
            if not isinstance(val, (list, tuple)) or not val:
                errors.append({"where_any_invalid_in_value": cond}); continue
        if op == "between":
            if not (isinstance(val, (list, tuple)) and len(val) == 2):
                errors.append({"where_any_invalid_between_value": cond}); continue
        if op == "ilike" and not isinstance(val, str):
            errors.append({"where_any_ilike_needs_string": cond}); continue
        where_or.append({"field": field, "op": op, "value": val})

    # # tags helpers
    # tags_any = spec.get("tags_any") or []
    # if tags_any and not all(isinstance(x, str) for x in tags_any):
    #     errors.append({"tags_any_invalid": tags_any})
    #     tags_any = []

    # tags_all = spec.get("tags_all") or []
    # if tags_all and not all(isinstance(x, str) for x in tags_all):
    #     errors.append({"tags_all_invalid": tags_all})
    #     tags_all = []

    # order_by
    order_spec = []
    for ob in (spec.get("order_by") or []):
        f = ob.get("field")
        d = (ob.get("direction") or "asc").lower()
        if f not in ALLOWED_FIELDS:
            errors.append({"order_by_invalid_field": ob})
            continue
        if d not in {"asc","desc"}:
            warnings.append({"order_by_defaulted_direction": ob})
            d = "asc"
        order_spec.append({"field": f, "direction": d})

    # pagination
    limit  = spec.get("limit", 50)
    offset = spec.get("offset", 0)
    try:
        limit = min(int(limit), MAX_LIMIT)
        if limit < 1:
            raise ValueError
    except Exception:
        warnings.append({"limit_defaulted": limit})
        limit = 50
    try:
        offset = max(int(offset), 0)
    except Exception:
        warnings.append({"offset_defaulted": offset})
        offset = 0

    normalized = {
        "select": good_select,
        "where": where_and,
        "where_any": where_or, 
        "order_by": order_spec,
        "limit": limit,
        "offset": offset
    }
    # Never interrupt; ok == True even with non-fatal issues
    # You can flip to False only if you want to block empty queries.
    ok = True
    return {"ok": ok, "errors": errors, "warnings": warnings, "normalized_spec": normalized}


def build_stmt_from_spec(normalized: Dict[str, Any]):
    sel_cols = [col(c) for c in normalized["select"]]
    stmt = select(*sel_cols)

    # where
    and_clauses = []
    for cond in normalized["where"]:
        c, op, v = col(cond["field"]), cond["op"], cond["value"]
        if op == "=":  and_clauses.append(c == v)
        elif op == "!=": and_clauses.append(c != v)
        elif op == ">":  and_clauses.append(c >  v)
        elif op == ">=": and_clauses.append(c >= v)
        elif op == "<":  and_clauses.append(c <  v)
        elif op == "<=": and_clauses.append(c <= v)
        elif op == "ilike": and_clauses.append(c.ilike(v))
        elif op == "in":    and_clauses.append(c.in_(v))
        elif op == "between": and_clauses.append(c.between(v[0], v[1]))

    # or
    or_clauses = []
    for cond in normalized.get("where_any", []):
        c, op, v = col(cond["field"]), cond["op"], cond["value"]
        if op == "=": or_clauses.append(c == v)
        elif op == "!=": or_clauses.append(c != v)
        elif op == ">": or_clauses.append(c > v)
        elif op == ">=": or_clauses.append(c >= v)
        elif op == "<": or_clauses.append(c < v)
        elif op == "<=": or_clauses.append(c <= v)
        elif op == "ilike": or_clauses.append(c.ilike(v))
        elif op == "in": or_clauses.append(c.in_(v))
        elif op == "between": or_clauses.append(c.between(v[0], v[1]))

    if and_clauses and or_clauses:
        stmt = stmt.where(and_(*and_clauses, or_(*or_clauses)))
    elif and_clauses:
        stmt = stmt.where(and_(*and_clauses))
    elif or_clauses:
        stmt = stmt.where(or_(*or_clauses))

    # if clauses:
    #     stmt = stmt.where(and_(*clauses))

    # # array helpers
    # if normalized.get("tags_any"):
    #     stmt = stmt.where(Listing.tags.overlap(normalized["tags_any"]))   # &&
    # if normalized.get("tags_all"):
    #     stmt = stmt.where(Listing.tags.contains(normalized["tags_all"]))  # @>

    # order_by
    if normalized.get("order_by"):
        orders = []
        for ob in normalized["order_by"]:
            orders.append(asc(col(ob["field"])) if ob["direction"] == "asc" else desc(col(ob["field"])))
        stmt = stmt.order_by(*orders)

    # pagination
    stmt = stmt.limit(normalized["limit"]).offset(normalized["offset"])
    return stmt

QUERY_LISTING_TABLE_DESC = """
    Summary
    -------
    Query the listings' information from database "listing" table.

    Input Parameters
    -------
    spec: structured JSON describing columns, filters, sort, and pagination.

    ## Schema of spec: 
    Data type: Dictionary
    {
      "select":   [str],                          // optional; If not specified, pass an empty list [] to use default setting to select all columns
      "where":    [ {"field": str, "op": str, "value": any}, ... ] And clause
      "where_any": [ {"field": str, "op": str, "value": any}, ... ] Or clause
      "order_by": [ {"field": str, "direction": "asc"|"desc"} ],
      "limit":    int,                            // default 50, capped at MAX_LIMIT
      "offset":   int,                            // default 0
    }

    ## Allowed columns in "select"

    ["external_id","listing_data_source","address_unit","address_street","address_city","address_state","longitude_text","latitude_text","bedrooms","bathrooms",
    "listing_price","year_built","longitude","latitude","home_type","living_area","rent_zestimate","photo_count","hdp_url","has_approved_third_party_virtual_tour_url",
    "street_view_tile_image_url_medium_lat_long","general_description","price_history","nearby_homes","is_instant_offer_enabled","is_off_market","days_on_zillow",
    "virtual_tour_url","photos","utilities","interior_description","overview","is_listed_by_management_company","property_description","tags","unit_amenities",
    "availability_date","getting_around_scores","updated_at","created_at"]


    ## Allowed columns in "where":
    ['external_id', 'listing_data_source', 'address_city', 'address_state', 'bedrooms', 'bathrooms', 'listing_price',
     'year_built', 'home_type', 'living_area', 'days_on_zillow', 'availability_date', 'updated_at', 'created_at']

    ## Allowed operators: 
    
    [=, !=, >, >=, <, <=, ilike, in, between]

    ## Example of usage:

    1. spec for “Bayonne, ≥2 beds, price ≤ 2600, sort by price asc, 25 rows”:
      {
        "select": ["external_id","address_street","address_city",
                   "bedrooms","bathrooms","listing_price","home_type","hdp_url"],
        "where": [
          {"field":"address_city","op":"ilike","value":"%Bayonne%"},
          {"field":"bedrooms","op":">=","value":2},
          {"field":"listing_price","op":"<=","value":2600}
        ],
        "order_by": [{"field":"listing_price","direction":"asc"}],
        "limit": 25, "offset": 0
      }

    2. spec for “Bayonne or Jersey City, price ≤ 2600, sort by price asc, 50 rows”:
      {
        "select": ["external_id","address_city","bedrooms","listing_price","hdp_url"],
        "where": [ {"field":"listing_price","op":"<=","value":2600} ],
        "where_any": [
            {"field":"address_city","op":"ilike","value":"%Bayonne%"},
            {"field":"address_city","op":"ilike","value":"%Jersey City%"}
        ],
        "order_by": [{"field":"listing_price","direction":"asc"}],
        "limit": 50
      }

    Returns
    -------
    dict

    {
      "ok": bool,                 // False only if execution failed
      "errors":   [ ... ],        // blocking issues (e.g., invalid where fields/ops/values)
      "warnings": [ ... ],        // non-blocking sanitizations (e.g., dropped select cols, defaulted limit)
      "meta": { "limit": int, "offset": int, "returned": int },
      "data": [ {column: value, ...}, ... ]  // one dict per row, listing infomation
    }

    Note for Agent
    -------
    - Boolean logic is limited to a single top level: AND(where...) AND OR(where_any...).
    In other words, the final predicate is:
        AND( <all conditions in `where`>, OR( <all conditions in `where_any`> ) )
    - Nested boolean groups aren't supported. If you need complex logic,
    try rewriting with `IN`/`BETWEEN` or make multiple queries.
    - If "select" is [], the service returns ALL columns (be mindful of payload size).
    For lighter responses, specify only the columns you need.
    - Fields and ops are validated; invalid entries are dropped and reported in `warnings`/`errors`.
    - Field home_type only have the following options: ['APARTMENT', 'CONDO', 'SINGLE_FAMILY'], if not sure, use ilike rather than exact match.
    """

def query_listing_table(spec: Dict[str, Any]):
    
    """
    Summary
    -------
    Query the listings' information from database "listing" table.

    Input Parameters
    -------
    spec: structured JSON describing columns, filters, sort, and pagination.

    ## Schema of spec: 
    Data type: Dictionary
    {
      "select":   [str],                          // optional; If not specified, pass an empty list [] to use default setting to select all columns
      "where":    [ {"field": str, "op": str, "value": any}, ... ] And clause
      "where_any": [ {"field": str, "op": str, "value": any}, ... ] Or clause
      "order_by": [ {"field": str, "direction": "asc"|"desc"} ],
      "limit":    int,                            // default 50, capped at MAX_LIMIT
      "offset":   int,                            // default 0
    }

    Returns
    -------
    dict

    {
      "ok": bool,                 // False only if execution failed
      "errors":   [ ... ],        // blocking issues (e.g., invalid where fields/ops/values)
      "warnings": [ ... ],        // non-blocking sanitizations (e.g., dropped select cols, defaulted limit)
      "meta": { "limit": int, "offset": int, "returned": int },
      "data": [ {column: value, ...}, ... ]  // one dict per row, listing infomation
    }
    """

    check = sanitize_agent_spec(spec)
    normalized = check["normalized_spec"]
    engine = _engine_cached()

    try:
        # Use a transaction so SET LOCAL applies
        with engine.begin() as conn:
            conn.exec_driver_sql("SET LOCAL statement_timeout = '2000ms'")
            conn.exec_driver_sql("SET LOCAL search_path = public")

            stmt = build_stmt_from_spec(normalized)
            rows = conn.execute(stmt).mappings().all()
            data = [dict(r) for r in rows]

        return {
            "ok": True,
            "errors": check["errors"],
            "warnings": check["warnings"],
            "meta": {
                "limit": normalized["limit"],
                "offset": normalized["offset"],
                "returned": len(data)
            },
            "data": data
        }

    except Exception as e:
        return {
            "ok": False,
            "errors": check["errors"] + [{"execution_error": str(e)}],
            "warnings": check["warnings"],
            "meta": {"limit": normalized["limit"], "offset": normalized["offset"]},
            "data": []
        }

def agent_query_listing_table(spec: Dict[str, Any]) -> str:
    result = query_listing_table(spec)
    # return json.dumps(result, ensure_ascii=False)
    return result

agent_query_listing_table_tool = StructuredTool.from_function(
    name="agent_query_listing_table",
    description = QUERY_LISTING_TABLE_DESC,
    func=agent_query_listing_table,
    args_schema=AgentQueryDatabaseInput,
    return_direct=False,
)