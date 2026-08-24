"""
parser/bronze_to_silver_core.py

Logic thuần parse HTML tin đăng alonhadat.com.vn -> ParsedListing hoặc ParseError.
Không import I/O libs, chỉ nhận bytes HTML + tham số ngữ cảnh (url, crawl_date,
source_bronze_key, source_part). Có thể test độc lập, dùng lại cho Spark mapPartitions
hoặc script debug.
"""


from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Optional, Union

from bs4 import BeautifulSoup, Tag

# ---------------------------------------------------------------------------
# Kết quả parse: 1 trong 2 loại, phân biệt bằng type để caller (Spark) dễ
# route sang staging hay quarantine.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ParsedListing:
    listing_id: int
    listing_url: str
    source_part: str
    source_bronze_key: str
    crawl_date: datetime

    title: str
    listing_type: str  # 'sale' | 'rent'
    property_type: str
    posted_date: date

    price_vnd: Optional[Decimal]  # None khi price_is_negotiable=True
    price_raw: str
    price_is_negotiable: bool

    area_m2: Optional[Decimal]  # None khi area_is_undetermined=True
    area_raw: str
    area_is_undetermined: bool

    length_m: Optional[Decimal]
    width_m: Optional[Decimal]
    street_width_m: Optional[Decimal]
    floors: Optional[int]
    bedrooms: Optional[int]

    orientation: Optional[str]
    legal_status: Optional[str]

    has_dining_room: Optional[bool]
    has_kitchen: Optional[bool]
    has_rooftop: Optional[bool]
    has_car_parking: Optional[bool]
    owner_direct: Optional[bool]

    is_expired: bool
    has_warning: bool

    address_street_new: Optional[str]
    address_ward_new: Optional[str]
    address_province_new: Optional[str]
    # address_old_raw giữ nguyên text thô để audit; ward_old/district_old
    # tách qua parse_old_address() theo quy luật 4-phần-cách-dấu-phẩy.
    address_old_raw: Optional[str]
    address_ward_old: Optional[str]
    address_district_old: Optional[str]


@dataclass(frozen=True)
class ParseError:
    listing_url: str
    crawl_date: datetime
    source_bronze_key: str
    error_reason: str
    raw_html: bytes


# ---------------------------------------------------------------------------
# parse_vn_number — tự nhận diện '.' là hàng nghìn hay ',' là thập phân
# ---------------------------------------------------------------------------

_MISSING_MARKERS = {"", "-", "--", "---", "_", "n/a", "na"}


def _is_missing(text: str) -> bool:
    return text.strip().lower() in _MISSING_MARKERS


def parse_vn_number(text: Optional[str]) -> Optional[Decimal]:
    """Parse số kiểu VN, không naive replace(',', '.').

    Quy tắc:
    - Có ',' -> ',' là thập phân; '.' là hàng nghìn nếu cùng xuất hiện.
    - Không ',' -> '.' theo nhóm 3 chữ số là hàng nghìn.
    - Không dấu -> parse thẳng số.
    - Ký hiệu thiếu dữ liệu ('---','-','_','') -> None.
    """
    if text is None:
        return None
    cleaned = text.strip()
    if _is_missing(cleaned):
        return None

    # Bỏ đơn vị hay gặp ở cuối chuỗi: "m", "m2", "m²"
    cleaned = re.sub(r"\s*m(?:2|²)?\s*$", "", cleaned, flags=re.IGNORECASE).strip()
    if cleaned == "" or _is_missing(cleaned):
        return None

    if "," in cleaned:
        cleaned = cleaned.replace(".", "")  # '.' (nếu có) là hàng nghìn, bỏ trước
        cleaned = cleaned.replace(",", ".")  # ',' là thập phân
    elif "." in cleaned and re.fullmatch(r"\d{1,3}(\.\d{3})+", cleaned):
        cleaned = cleaned.replace(".", "")
    # còn lại (có '.' nhưng không đúng dạng nhóm-3-chữ-số, hoặc không dấu
    # nào) -> giữ nguyên, Decimal() tự parse.

    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def extract_listing_id_from_url(url: str) -> Optional[int]:
    """Trích listing_id từ URL dạng '...-12345678.html'.
    Nguồn chính cho listing_id, 'Mã tin' chỉ QC đối chiếu."""
    match = re.search(r"-(\d+)\.html?\s*$", url.strip())
    if not match:
        return None
    return int(match.group(1))


def _parse_check_icon(cell: Tag) -> Optional[bool]:
    """True nếu có icon check (<img alt="check">).
    None nếu ký hiệu thiếu ('_','-','--','---',''...). 
    """
    if cell.find("img", alt="check") is not None:
        return True
    return None


def _get_text(tag: Optional[Tag]) -> str:
    if tag is None:
        return ""
    return tag.get_text(" ", strip=True)


# ---------------------------------------------------------------------------
# Parse bảng section.moreinfor1 -> dict {label: value}.
# Mỗi <tr> gồm các cặp (label, value) tuần tự, số cột không đều (colspan),
# nên không dựa vào vị trí cố định mà ghép theo thứ tự xuất hiện.
# ---------------------------------------------------------------------------


def parse_old_address(raw: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Tách (ward_old, district_old) từ address_old_raw dạng 
    'Đường X, Phường/Xã/Thị Trấn Y, Quận/Huyện/Thành phố Z, Tỉnh/Thành (cũ)'.
    Lấy 3 phần cuối theo dấu ',' từ phải, bỏ tỉnh/thành (không lưu vì cấp tỉnh không đổi).
    Giữ nguyên tiền tố (Phường/Xã/Quận/Huyện...) trong giá trị trả về."""
    if not raw:
        return None, None
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) < 3:
        return None, None
    ward_old = parts[-3] or None
    district_old = parts[-2] or None
    return ward_old, district_old


def _parse_moreinfor_table(section: Tag) -> dict[str, Tag]:
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


_LISTING_TYPE_MAP = {"Cần bán": "sale", "Cho thuê": "rent"}


def parse_listing_html(
    html: bytes,
    listing_url: str,
    crawl_date: datetime,
    source_part: str,
    source_bronze_key: str,
) -> Union[ParsedListing, ParseError]:
    """Parse 1 bản ghi Bronze (raw HTML) thành ParsedListing hoặc ParseError
    để ghi vào silver.parse_quarantine."""

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
    except Exception as exc:  # noqa: BLE001 - cố tình bắt mọi lỗi parse HTML
        return _fail(f"loi parse html: {exc}")

    # Container an toàn: class="property" (khác "property-item" sidebar).
    # BeautifulSoup so khớp theo token class riêng nên không nhầm lẫn.
    article = soup.find("article", class_="property")
    if article is None:
        return _fail("khong tim thay <article class='property'>")

    listing_id = extract_listing_id_from_url(listing_url)
    if listing_id is None:
        return _fail(f"khong trich duoc listing_id tu url: {listing_url}")

    title_tag = article.find(attrs={"itemprop": "name"})
    title = _get_text(title_tag)
    if not title:
        return _fail("thieu title (itemprop=name)")

    # posted_date: BẮT BUỘC lấy attribute datetime, KHÔNG lấy text hiển thị
    # (text có thể là "Hôm nay"/"Hôm qua", không parse được thành ngày).
    time_tag = article.find("time", attrs={"itemprop": "datePosted"})
    if time_tag is None or not time_tag.get("datetime"):
        return _fail("thieu <time itemprop=datePosted datetime=...>")
    try:
        posted_date = datetime.strptime(time_tag["datetime"].strip(), "%Y-%m-%d").date()
    except ValueError:
        return _fail(f"posted_date sai dinh dang: {time_tag.get('datetime')!r}")

    price_tag = article.find(attrs={"itemprop": "price"})
    if price_tag is None or price_tag.get("value") is None:
        return _fail("thieu <data itemprop=price value=...>")
    price_raw = _get_text(price_tag)
    try:
        price_value = Decimal(price_tag["value"].strip())
    except InvalidOperation:
        return _fail(f"price value khong phai so: {price_tag.get('value')!r}")
    price_is_negotiable = price_value == 0
    price_vnd = None if price_is_negotiable else price_value

    area_span = article.find(attrs={"itemprop": "floorSize"})
    if area_span is None:
        return _fail("thieu itemprop=floorSize")
    area_value_tag = area_span.find(attrs={"itemprop": "value"})
    area_raw = _get_text(area_value_tag)
    area_is_undetermined = area_raw.strip().upper() == "KXĐ"
    area_m2 = None if area_is_undetermined else parse_vn_number(area_raw)
    if not area_is_undetermined and area_m2 is None:
        return _fail(f"area_m2 khong parse duoc: {area_raw!r}")

    # is_expired / has_warning: KHÔNG cố định vị trí trong article -> tìm
    # toàn bộ subtree thay vì chỉ children trực tiếp.
    is_expired = article.find(class_="expired") is not None
    has_warning = article.find(class_="warning") is not None

    address_tag = article.find(attrs={"itemprop": "address"})
    address_street_new = (
        _get_text(address_tag.find(attrs={"itemprop": "streetAddress"})) or None if address_tag else None
    )
    address_ward_new = (
        _get_text(address_tag.find(attrs={"itemprop": "addressLocality"})) or None if address_tag else None
    )
    address_province_new = (
        _get_text(address_tag.find(attrs={"itemprop": "addressRegion"})) or None if address_tag else None
    )

    old_address_tag = article.find("p", class_="old-address")
    address_old_raw = _get_text(old_address_tag) or None
    address_ward_old, address_district_old = parse_old_address(address_old_raw)

    moreinfor_section = article.find("section", class_="moreinfor1")
    if moreinfor_section is None:
        return _fail("thieu section.moreinfor1")
    fields = _parse_moreinfor_table(moreinfor_section)

    listing_type_raw = _get_text(fields.get("Loại tin"))
    listing_type = _LISTING_TYPE_MAP.get(listing_type_raw)
    if listing_type is None:
        # Đúng nguyên tắc đã chốt: chỉ giữ "Cần bán"/"Cho thuê", loại bỏ
        # "Cần mua"/"Cần thuê" (demand-side) và mọi giá trị lạ khác.
        return _fail(f"listing_type khong hop le (demand-side hoac loi parse): {listing_type_raw!r}")

    property_type = _get_text(fields.get("Loại BDS"))
    if not property_type:
        return _fail("thieu 'Loai BDS' trong bang moreinfor1")

    def _num(label: str) -> Optional[Decimal]:
        cell = fields.get(label)
        return parse_vn_number(_get_text(cell)) if cell is not None else None

    def _int(label: str) -> Optional[int]:
        value = _num(label)
        return int(value) if value is not None else None

    def _text_or_none(label: str) -> Optional[str]:
        cell = fields.get(label)
        if cell is None:
            return None
        text = _get_text(cell)
        return None if _is_missing(text) else text

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
        area_m2=area_m2,
        area_raw=area_raw,
        area_is_undetermined=area_is_undetermined,
        length_m=_num("Chiều dài"),
        width_m=_num("Chiều ngang"),
        street_width_m=_num("Đường trước nhà"),
        floors=_int("Số lầu"),
        bedrooms=_int("Số phòng ngủ"),
        orientation=_text_or_none("Hướng"),
        legal_status=_text_or_none("Pháp lý"),
        has_dining_room=_check("Phòng ăn"),
        has_kitchen=_check("Nhà bếp"),
        has_rooftop=_check("Sân thượng"),
        has_car_parking=_check("Chổ để xe hơi"),  # typo cố tình, đúng theo site
        owner_direct=_check("Chính chủ"),
        is_expired=is_expired,
        has_warning=has_warning,
        address_street_new=address_street_new,
        address_ward_new=address_ward_new,
        address_province_new=address_province_new,
        address_old_raw=address_old_raw,
        address_ward_old=address_ward_old,
        address_district_old=address_district_old,
    )
