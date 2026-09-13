"""
parser/bronze_to_silver_core.py

Component 3 (ETL Bronze -> Silver) — pure logic to parse alonhadat.com.vn
listing HTML into ParsedListing or ParseError. No I/O imports, reused by
Spark's mapPartitions.

Data conventions:
- STRING fields (title, orientation, legal_status, address_*): "" when
  missing, never NULL — so the UNIQUE constraint (gold.dim_location) can
  upsert idempotently.
- NUMERIC/DATE fields: kept as NULL when missing, never coerced to 0.
- Nullable BOOLEAN fields: NULL means "undetermined" (tri-state).

Note: several string literals below (HTML field labels, category names)
are intentionally kept in Vietnamese — they must match the source site's
actual text, and translating them would break parsing.
"""


from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Optional, Union

from bs4 import BeautifulSoup, Tag

from bronze_paths import DATASET_PREFIX as _DATASET_BRONZE_PREFIX
from bronze_paths import WEB_PREFIX as _WEB_BRONZE_PREFIX

# ---------------------------------------------------------------------------
# Parse result: one of two types, distinguished so the caller (Spark) can
# route to staging or quarantine.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ParsedListing:
    listing_id: int
    listing_url: str
    source_part: str
    source_bronze_key: str
    crawl_date: datetime

    title: str
    listing_type: str  # 'Cần bán' (for sale) | 'Cho thuê' (for rent)
    property_type: str
    posted_date: date

    price_vnd: Optional[Decimal]  # None when price_is_negotiable=True
    price_raw: str
    price_is_negotiable: bool
    price_is_outlier: bool  # price/m2 above threshold — flagged only, price_vnd kept as-is

    area_m2: Optional[Decimal]  # None when undetermined/outlier
    area_raw: str
    area_is_undetermined: bool
    area_is_outlier: bool  # raw area_m2 outside the plausible range, nulled out

    length_m: Optional[Decimal]
    width_m: Optional[Decimal]
    street_width_m: Optional[Decimal]
    floors: Optional[int]
    bedrooms: Optional[int]

    orientation: str
    legal_status: str

    has_dining_room: Optional[bool]
    has_kitchen: Optional[bool]
    has_rooftop: Optional[bool]
    has_car_parking: Optional[bool]
    owner_direct: Optional[bool]

    is_expired: bool
    has_warning: bool

    address_street_new: str
    address_ward_new: str
    address_province_new: str
    # address_old_raw keeps the raw text for audit; the 3 fields below are
    # split out via parse_old_address(). province_old may DIFFER from
    # address_province_new (a listing straddling old/new boundaries due to
    # the administrative merger).
    address_old_raw: str
    address_ward_old: str
    address_district_old: str
    address_province_old: str


@dataclass(frozen=True)
class ParseError:
    listing_url: str
    crawl_date: datetime
    source_bronze_key: str
    error_reason: str
    raw_html: bytes


# ---------------------------------------------------------------------------
# parse_vn_number — auto-detects whether '.' is a thousands separator or
# ',' is the decimal separator
# ---------------------------------------------------------------------------

_MISSING_MARKERS = {"", "-", "--", "---", "_", "n/a", "na"}


def _is_missing(text: str) -> bool:
    return text.strip().lower() in _MISSING_MARKERS


def parse_vn_number(text: Optional[str]) -> Optional[Decimal]:
    """Parse a Vietnamese-formatted number, not a naive replace(',', '.').

    Rules:
    - Contains ',' -> ',' is the decimal separator; '.' is a thousands
      separator if it also appears.
    - No ',' -> '.' grouped in 3-digit blocks is a thousands separator.
    - No separators -> parse the number directly.
    - Missing-data markers or negative numbers -> None.

    Returns None (not "") — this function is only for NUMERIC fields.
    """
    if text is None:
        return None
    cleaned = text.strip()
    if _is_missing(cleaned):
        return None

    cleaned = re.sub(r"\s*m(?:2|²)?\s*$", "", cleaned, flags=re.IGNORECASE).strip()
    if cleaned == "" or _is_missing(cleaned):
        return None

    if "," in cleaned:
        cleaned = cleaned.replace(".", "")  # '.' is a thousands separator, strip it first
        cleaned = cleaned.replace(",", ".")  # ',' is the decimal separator
    elif "." in cleaned and re.fullmatch(r"-?\d{1,3}(\.\d{3})+", cleaned):
        cleaned = cleaned.replace(".", "")

    try:
        value = Decimal(cleaned)
        if value < 0:
            return None
        return value
    except InvalidOperation:
        return None


# ---------------------------------------------------------------------------
# Sanitization thresholds for physically implausible values (e.g. width_m=99999999.00).
# Chosen from the real data distribution (wide enough to avoid cutting
# valid cases) + this project's scope (land plots excluded).
# ---------------------------------------------------------------------------

_MAX_WIDTH_LENGTH_M = Decimal("500")
_MAX_STREET_WIDTH_M = Decimal("200")
_MAX_AREA_M2 = Decimal("10000")   # project scope excludes land plots (1 hectare)
_MIN_AREA_M2 = Decimal("3")       # below this, likely a different field leaked into the area cell

# Benchmark: high-end HCMC apartments ~55-85M VND/m2, prime District 1
# frontage land ~1-2B VND/m2 -> 5B VND/m2 is wide enough to not cut valid cases.
_MAX_PRICE_PER_M2_VND = Decimal("5000000000")


def _sanitize_dimension(value: Optional[Decimal], max_valid: Decimal) -> Optional[Decimal]:
    """Null out width_m/length_m/street_width_m if above threshold or <=0.
    No separate flag — we accept losing the ability to distinguish
    "outlier" from "missing" for these fields."""
    if value is None or value <= 0 or value > max_valid:
        return None
    return value


def _sanitize_area(area_m2: Optional[Decimal]) -> tuple[Optional[Decimal], bool]:
    """Null out area_m2 outside [MIN, MAX] and set area_is_outlier=True.
    Needs its own flag because area_m2 feeds directly into the GENERATED
    price_per_m2_vnd column."""
    if area_m2 is not None and (area_m2 > _MAX_AREA_M2 or area_m2 < _MIN_AREA_M2):
        return None, True
    return area_m2, False


def _detect_price_outlier(
    price_vnd: Optional[Decimal], area_m2: Optional[Decimal]
) -> bool:
    """Detect price/m2 above threshold — flags only, does NOT null out
    price_vnd (the price genuinely exists on the site, only its
    reliability is in question). Uses the already-sanitized area_m2
    (call this after _sanitize_area())."""
    if price_vnd is None or area_m2 is None or area_m2 == 0:
        return False
    return (price_vnd / area_m2) > _MAX_PRICE_PER_M2_VND


def extract_listing_id_from_url(url: str) -> Optional[int]:
    """Extract listing_id from a URL like '...-12345678.html'."""
    match = re.search(r"-(\d+)\.html?\s*$", url.strip())
    if not match:
        return None
    return int(match.group(1))


def infer_source_from_bronze_key(source_bronze_key: str) -> str:
    """Infer 'source' ('dataset'|'web') from the source_bronze_key prefix
    — the single source of truth shared by pipeline.bronze_file_state.source
    and gold.dim_source.source_name (keeps the two in sync)."""
    if source_bronze_key.startswith(_DATASET_BRONZE_PREFIX):
        return "dataset"
    if source_bronze_key.startswith(_WEB_BRONZE_PREFIX):
        return "web"
    raise ValueError(
        f"Could not infer source (dataset/web) from source_bronze_key: {source_bronze_key!r}"
    )


def _parse_check_icon(cell: Tag) -> Optional[bool]:
    """True if a check icon is present. None if the "missing" marker is
    shown instead — tri-state boolean."""
    if cell.find("img", alt="check") is not None:
        return True
    return None


def _get_text(tag: Optional[Tag]) -> str:
    if tag is None:
        return ""
    return tag.get_text(" ", strip=True)


def remove_special_characters(text: str) -> str:
    """Strip symbol characters like emoji, keep letters, digits, spaces and punctuation."""
    return "".join(char for char in text if not unicodedata.category(char).startswith("S")).strip()


def _parse_moreinfor_table(section: Tag) -> dict[str, Tag]:
    """Parse the section.moreinfor1 table -> {label: value} dict. Column
    counts are uneven (colspan), so pair cells by appearance order rather
    than a fixed position."""
    result: dict[str, Tag] = {}
    table = section.find("table")
    if table is None:
        return result
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        for i in range(0, len(cells) - 1, 2):
            label = _get_text(cells[i])
            if label:
                result[label] = cells[i + 1]
    return result


def parse_old_address(raw: str) -> tuple[str, str, str]:
    """Split (ward_old, district_old, province_old) out of address_old_raw,
    formatted like 'Street X, Ward/Commune Y, District Z, Province (old)'
    — takes the last 3 comma-separated parts. Returns ("", "", "") if raw
    is empty or has fewer than 3 parts."""
    if not raw:
        return "", "", ""
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) < 3:
        return "", "", ""
    ward_old = parts[-3]
    district_old = parts[-2]
    province_old = parts[-1]
    return ward_old, district_old, province_old


# Sourced from listing_taxonomy.py — the single mapping shared with
# crawler/web_crawler_core.py's crawl-scope slugs, so the two can never
# drift out of sync (see the module docstring there for the failure mode
# this prevents).
from listing_taxonomy import (
    IN_SCOPE_PROVINCE_NEW as IN_SCOPE_PROVINCE,
    in_scope_listing_type_labels,
    in_scope_property_type_labels,
)

IN_SCOPE_LISTING_TYPES = in_scope_listing_type_labels()
IN_SCOPE_PROPERTY_TYPES = in_scope_property_type_labels()

def is_in_scope(listing: ParsedListing) -> bool:
    """Out-of-scope listings are silently dropped in parse_partition()
    (Step 4). Filters on address_province_new (current address), not
    address_province_old (which may differ from province_new after the
    administrative merger)."""
    return (
        listing.address_province_new == IN_SCOPE_PROVINCE
        and listing.listing_type in IN_SCOPE_LISTING_TYPES
        and listing.property_type in IN_SCOPE_PROPERTY_TYPES
    )

def parse_listing_html(
    html: bytes,
    listing_url: str,
    crawl_date: datetime,
    source_part: str,
    source_bronze_key: str,
) -> Union[ParsedListing, ParseError]:
    """Step 3 — parse one Bronze record into ParsedListing or ParseError
    (routed to silver.parse_quarantine)."""

    def _fail(reason: str) -> ParseError:
        return ParseError(
            listing_url=listing_url,
            crawl_date=crawl_date,
            source_bronze_key=source_bronze_key,
            error_reason=reason,
            raw_html=html,
        )

    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception as exc:  # noqa: BLE001 - intentionally catches any HTML parse error
        return _fail(f"HTML parse error: {exc}")

    # Safe container: class="property" (distinct from the sidebar's "property-item").
    article = soup.find("article", class_="property")
    if article is None:
        return _fail("could not find <article class='property'>")

    listing_id = extract_listing_id_from_url(listing_url)
    if listing_id is None:
        return _fail(f"could not extract listing_id from url: {listing_url}")

    title_tag = article.find(attrs={"itemprop": "name"})
    title = remove_special_characters(_get_text(title_tag))
    if not title:
        return _fail("missing title (itemprop=name)")

    # posted_date: must read the datetime attribute (the display text may be "Today").
    time_tag = article.find("time", attrs={"itemprop": "datePosted"})
    if time_tag is None or not time_tag.get("datetime"):
        return _fail("missing <time itemprop=datePosted datetime=...>")
    try:
        posted_date = datetime.strptime(time_tag["datetime"].strip(), "%Y-%m-%d").date()
    except ValueError:
        return _fail(f"posted_date has invalid format: {time_tag.get('datetime')!r}")

    price_tag = article.find(attrs={"itemprop": "price"})
    if price_tag is None or price_tag.get("value") is None:
        return _fail("missing <data itemprop=price value=...>")
    price_raw = _get_text(price_tag)
    try:
        price_value = Decimal(price_tag["value"].strip())
    except InvalidOperation:
        return _fail(f"price value is not numeric: {price_tag.get('value')!r}")
    price_is_negotiable = price_value == 0
    price_vnd = None if price_is_negotiable else price_value

    area_span = article.find(attrs={"itemprop": "floorSize"})
    if area_span is None:
        return _fail("missing itemprop=floorSize")
    area_value_tag = area_span.find(attrs={"itemprop": "value"})
    area_raw = _get_text(area_value_tag)
    area_is_undetermined = area_raw.strip().upper() == "KXĐ"  # site's own "undetermined" marker
    area_m2 = None if area_is_undetermined else parse_vn_number(area_raw)
    if not area_is_undetermined and area_m2 is None:
        return _fail(f"area_m2 could not be parsed: {area_raw!r}")
    area_m2, area_is_outlier = _sanitize_area(area_m2)

    # Must be computed AFTER area_m2 has been sanitized above.
    price_is_outlier = _detect_price_outlier(price_vnd, area_m2)

    # is_expired/has_warning don't have a fixed position -> search the whole subtree.
    is_expired = article.find(class_="expired") is not None
    has_warning = article.find(class_="warning") is not None

    address_tag = article.find(attrs={"itemprop": "address"})
    address_street_new = (
        _get_text(address_tag.find(attrs={"itemprop": "streetAddress"})) if address_tag else ""
    )
    address_ward_new = (
        _get_text(address_tag.find(attrs={"itemprop": "addressLocality"})) if address_tag else ""
    )
    address_province_new = (
        _get_text(address_tag.find(attrs={"itemprop": "addressRegion"})) if address_tag else ""
    )

    old_address_tag = article.find("p", class_="old-address")
    address_old_raw = _get_text(old_address_tag)
    address_ward_old, address_district_old, address_province_old = parse_old_address(address_old_raw)

    moreinfor_section = article.find("section", class_="moreinfor1")
    if moreinfor_section is None:
        return _fail("missing section.moreinfor1")
    fields = _parse_moreinfor_table(moreinfor_section)

    # Field labels below ("Loại tin", "Loại BDS", etc.) are the exact
    # Vietnamese text used as table keys on the source site — must not be translated.
    listing_type = _get_text(fields.get("Loại tin"))
    if not listing_type:
        return _fail("missing 'Loại tin' in the moreinfor1 table")

    property_type = _get_text(fields.get("Loại BDS"))
    if not property_type:
        return _fail("missing 'Loại BDS' in the moreinfor1 table")

    def _num(label: str) -> Optional[Decimal]:
        cell = fields.get(label)
        return parse_vn_number(_get_text(cell)) if cell is not None else None

    def _int(label: str) -> Optional[int]:
        value = _num(label)
        return int(value) if value is not None else None

    def _text_or_empty(label: str) -> str:
        """"" when the field is absent or is a missing-data marker."""
        cell = fields.get(label)
        if cell is None:
            return ""
        text = _get_text(cell)
        return "" if _is_missing(text) else text

    def _check(label: str) -> Optional[bool]:
        cell = fields.get(label)
        return _parse_check_icon(cell) if cell is not None else None

    return ParsedListing(
        listing_id=listing_id,
        listing_url=listing_url,
        source_part=source_part,
        source_bronze_key=source_bronze_key,
        crawl_date=crawl_date,
        title=title,
        listing_type=listing_type,
        property_type=property_type,
        posted_date=posted_date,
        price_vnd=price_vnd,
        price_raw=price_raw,
        price_is_negotiable=price_is_negotiable,
        price_is_outlier=price_is_outlier,
        area_m2=area_m2,
        area_raw=area_raw,
        area_is_undetermined=area_is_undetermined,
        area_is_outlier=area_is_outlier,
        length_m=_sanitize_dimension(_num("Chiều dài"), _MAX_WIDTH_LENGTH_M),
        width_m=_sanitize_dimension(_num("Chiều ngang"), _MAX_WIDTH_LENGTH_M),
        street_width_m=_sanitize_dimension(_num("Đường trước nhà"), _MAX_STREET_WIDTH_M),
        floors=_int("Số lầu"),
        bedrooms=_int("Số phòng ngủ"),
        orientation=_text_or_empty("Hướng"),
        legal_status=_text_or_empty("Pháp lý"),
        has_dining_room=_check("Phòng ăn"),
        has_kitchen=_check("Nhà bếp"),
        has_rooftop=_check("Sân thượng"),
        has_car_parking=_check("Chổ để xe hơi"),  # typo is intentional, matches the site
        owner_direct=_check("Chính chủ"),
        is_expired=is_expired,
        has_warning=has_warning,
        address_street_new=address_street_new,
        address_ward_new=address_ward_new,
        address_province_new=address_province_new,
        address_old_raw=address_old_raw,
        address_ward_old=address_ward_old,
        address_district_old=address_district_old,
        address_province_old=address_province_old,
    )
