"""
listing_taxonomy.py (project root)

Single source of truth for the site's listing_type/property_type taxonomy,
shared by the crawler (needs URL slugs to build crawl targets) and the
parser (needs the exact Vietnamese labels the site renders, to filter
parsed listings into scope).

Deliberately dependency-free so both pure core modules can import it —
including parser/bronze_to_silver_core.py, which runs inside Spark closures
and must stay free of config/I/O imports.

Each dict maps a URL slug (used to build alonhadat.com.vn URLs) to the
exact label the site displays in listing HTML (used to filter
ParsedListing.listing_type / .property_type into scope). Keeping both
representations in one place avoids the failure mode already logged once
in error_log.md, where the crawl-side slug list and the parse-side scope
filter drifted out of sync after only one of the two was updated.
"""

LISTING_TYPE_SLUG_TO_LABEL: dict[str, str] = {
    "can-ban": "Cần bán",
    "cho-thue": "Cho thuê",
}

PROPERTY_TYPE_SLUG_TO_LABEL: dict[str, str] = {
    "nha-mat-tien": "Nhà mặt tiền",
    "nha-trong-hem": "Nhà trong hẻm",
    "biet-thu-nha-lien-ke": "Biệt thự, nhà liền kề",
    "can-ho-chung-cu": "Căn hộ chung cư",
    "phong-tro-nha-tro": "Phòng trọ, nhà trọ",
}

# Old administrative boundaries used to build crawl URLs — the site has not
# been updated to the new post-merger boundaries.
PROVINCES_OLD: tuple[str, ...] = ("ho-chi-minh", "binh-duong", "ba-ria-vung-tau")

# The single new-boundary province value every in-scope listing must have,
# regardless of which of the 3 old provinces it was crawled under (all 3
# were merged into this one after the administrative merger).
IN_SCOPE_PROVINCE_NEW = "Hồ Chí Minh"


def listing_type_slugs() -> tuple[str, ...]:
    return tuple(LISTING_TYPE_SLUG_TO_LABEL.keys())


def property_type_slugs() -> tuple[str, ...]:
    return tuple(PROPERTY_TYPE_SLUG_TO_LABEL.keys())


def in_scope_listing_type_labels() -> set[str]:
    return set(LISTING_TYPE_SLUG_TO_LABEL.values())


def in_scope_property_type_labels() -> set[str]:
    return set(PROPERTY_TYPE_SLUG_TO_LABEL.values())
