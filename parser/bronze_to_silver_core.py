"""
parser/bronze_to_silver_core.py

Logic thuần parse HTML tin đăng alonhadat.com.vn -> ParsedListing hoặc ParseError.
Không import I/O libs — chỉ nhận bytes HTML + ngữ cảnh (url, crawl_date,
source_bronze_key, source_part). Test độc lập được, dùng lại cho Spark mapPartitions.

QUY ƯỚC DỰ ÁN:
- Trường CHUỖI (title, orientation, legal_status, address_*): dùng "" khi thiếu,
  KHÔNG dùng NULL — để UNIQUE constraint (gold.dim_location) upsert idempotent đúng.
- Trường SỐ/DATE (price_vnd, area_m2, floors...): giữ NULL khi thiếu, không đổi sang 0.
- Trường BOOLEAN nullable (has_dining_room...): giữ NULL = "không xác định" (tri-state).
"""


from __future__ import annotations

import re
import unicodedata
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
    listing_type: str  # 'Cần bán' | 'Cho thuê'
    property_type: str
    posted_date: date

    price_vnd: Optional[Decimal]  # None khi price_is_negotiable=True (SỐ — vẫn NULL)
    price_raw: str
    price_is_negotiable: bool
    price_is_outlier: bool  # True khi price_vnd/area_m2 > _MAX_PRICE_PER_M2_VND — CHỈ gắn cờ, price_vnd giữ nguyên

    area_m2: Optional[Decimal]  # None khi area_is_undetermined/area_is_outlier=True (SỐ — vẫn NULL)
    area_raw: str
    area_is_undetermined: bool
    area_is_outlier: bool  # True khi area_m2 gốc > _MAX_AREA_M2 hoặc < _MIN_AREA_M2 (đã bị null hóa)

    length_m: Optional[Decimal]
    width_m: Optional[Decimal]
    street_width_m: Optional[Decimal]
    floors: Optional[int]
    bedrooms: Optional[int]

    # --- CHUỖI: dùng "" khi thiếu, KHÔNG dùng None (xem quy ước đầu file) ---
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
    # address_old_raw giữ nguyên text thô để audit; ward_old/district_old/
    # province_old tách qua parse_old_address() theo quy luật 4-phần-cách-dấu-phẩy.
    # province_old có thể KHÁC address_province_new (xem parse_old_address()).
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
    - Là số âm -> None.

    LƯU Ý: trả None (không phải "") vì đây là hàm phục vụ trường KIỂU SỐ —
    quy ước "" chỉ áp dụng cho trường kiểu chuỗi (xem module docstring).
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
    elif "." in cleaned and re.fullmatch(r"-?\d{1,3}(\.\d{3})+", cleaned):
        cleaned = cleaned.replace(".", "")
    # còn lại (có '.' nhưng không đúng dạng nhóm-3-chữ-số, hoặc không dấu
    # nào) -> giữ nguyên, Decimal() tự parse.

    try:
        value = Decimal(cleaned)
        if value < 0:
            return None
        return value
    except InvalidOperation:
        return None


# ---------------------------------------------------------------------------
# Sanitize ngưỡng hợp lý vật lý — phát hiện qua review thủ công Phase 5
# (VD thực tế: width_m=99999999.00, area_m2=989593.00 với price_per_m2_vnd
# thấp bất thường). parse_vn_number() đã loại số âm nhưng CHƯA loại 0 hoặc
# số dương phi lý. Ngưỡng chọn theo p99 thực tế (rộng hơn hẳn để không cắt
# nhầm case hợp lệ) + domain knowledge (phạm vi đồ án không gồm đất nền).
# ---------------------------------------------------------------------------

_MAX_WIDTH_LENGTH_M = Decimal("500")   # p99 thực tế: width=27, length=55
_MAX_STREET_WIDTH_M = Decimal("200")   # p99 thực tế: 40
_MAX_AREA_M2 = Decimal("10000")        # phạm vi đồ án không gồm đất nền (1ha)
_MIN_AREA_M2 = Decimal("3")            # SỬA (Phase 5): p1 area thực tế toàn Silver = 21m2,
                                        # p5 = 34m2 -> <3m2 nằm sâu ngoài phân phối tự nhiên,
                                        # nghi field khác bị nhầm vào ô diện tích (VD thực tế:
                                        # area_raw='01' -> area_m2=1.00, 4 dòng cùng chuỗi thô
                                        # giống hệt nhau, không phải diện tích thật)

# Ngưỡng phát hiện price_vnd/area_m2 phi lý (Phase 5, phát hiện qua
# validate_gold_load.sql check price_per_m2_no_extreme_outlier). Benchmark
# đối chiếu thực tế: căn hộ cao cấp HCMC ~55-85 triệu/m2 (2024), đất mặt
# tiền đắt nhất trung tâm Q1 theo báo chí ~1-2 tỷ/m2 -> 5 tỷ/m2 rộng rãi
# hơn hẳn mức đắt nhất thực tế ghi nhận, tránh cắt nhầm case hợp lệ.
_MAX_PRICE_PER_M2_VND = Decimal("5000000000")


def _sanitize_dimension(value: Optional[Decimal], max_valid: Decimal) -> Optional[Decimal]:
    """Null hóa width_m/length_m/street_width_m vượt ngưỡng hoặc <=0.
    Không gắn cờ riêng — 3 trường này không phải measure chính, chấp nhận
    mất khả năng phân biệt "null vì outlier" vs "null vì thiếu" để đơn giản."""
    if value is None or value <= 0 or value > max_valid:
        return None
    return value


def _sanitize_area(area_m2: Optional[Decimal]) -> tuple[Optional[Decimal], bool]:
    """area_m2 vượt _MAX_AREA_M2 hoặc dưới _MIN_AREA_M2 -> null hóa + gắn
    area_is_outlier=True. Bắt buộc có cờ riêng (khác 3 trường kích thước trên)
    vì area_m2 feed trực tiếp vào price_per_m2_vnd (GENERATED) — cần audit,
    không lẫn với area_is_undetermined (site tự ghi "KXĐ")."""
    if area_m2 is not None and (area_m2 > _MAX_AREA_M2 or area_m2 < _MIN_AREA_M2):
        return None, True
    return area_m2, False


def _detect_price_outlier(
    price_vnd: Optional[Decimal], area_m2: Optional[Decimal]
) -> bool:
    """Phát hiện giá/m2 vượt _MAX_PRICE_PER_M2_VND — chỉ gắn cờ, KHÔNG null hóa
    price_vnd (khác area_is_outlier): giá vẫn tồn tại thật trên site, chỉ đáng
    ngờ độ tin cậy (khác "Thỏa thuận" thật = price_value=0), lọc ở Gold/dashboard
    qua cờ này thay vì xóa dữ liệu. Dùng area_m2 ĐÃ sanitize (gọi sau _sanitize_area()).

    3 nguyên nhân thực tế: (1) price_raw "X tỷ/m²" nhưng site tự nhân thành
    tổng giá, (2) area_m2 quá nhỏ (đã xử lý riêng), (3) sai dấu phẩy thập phân
    kiểu VN (VD "6,9 tỷ" đọc nhầm) khiến giá gấp ~100 lần thật."""
    if price_vnd is None or area_m2 is None or area_m2 == 0:
        return False
    return (price_vnd / area_m2) > _MAX_PRICE_PER_M2_VND


def extract_listing_id_from_url(url: str) -> Optional[int]:
    """Trích listing_id từ URL dạng '...-12345678.html'.
    Nguồn chính cho listing_id, 'Mã tin' chỉ QC đối chiếu."""
    match = re.search(r"-(\d+)\.html?\s*$", url.strip())
    if not match:
        return None
    return int(match.group(1))


_DATASET_BRONZE_PREFIX = "bronze/dataset/"
_WEB_BRONZE_PREFIX = "bronze/web/"


def infer_source_from_bronze_key(source_bronze_key: str) -> str:
    """Suy 'source' ('dataset'|'web') từ prefix của source_bronze_key.

    Đặt ở core (không phải cột riêng trong Silver) vì suy được 100% từ
    source_bronze_key — thêm cột sẽ là dữ liệu derived/redundant, vi phạm
    ranh giới Medallion (Silver = clean raw grain).

    1 nguồn sự thật duy nhất, dùng chung cho:
    - control-plane: pipeline.bronze_file_state.source (bronze_file_state_io.py)
    - Gold ETL: gold.dim_source.source_name (silver_to_gold_io.py)
    Tránh lặp lại logic ở 2 nơi rồi lệch nhau, cùng pattern với
    silver.compute_row_hash()/gold.compute_feature_key() (1 nguồn sự thật)."""
    if source_bronze_key.startswith(_DATASET_BRONZE_PREFIX):
        return "dataset"
    if source_bronze_key.startswith(_WEB_BRONZE_PREFIX):
        return "web"
    raise ValueError(
        f"Không suy được source (dataset/web) từ source_bronze_key: {source_bronze_key!r}"
    )


def _parse_check_icon(cell: Tag) -> Optional[bool]:
    """True nếu có icon check (<img alt="check">).
    None nếu ký hiệu thiếu ('_','-','--','---',''...) — GIỮ NGUYÊN None,
    vì đây là boolean tri-state (unknown), không thuộc quy ước "" cho chuỗi.
    """
    if cell.find("img", alt="check") is not None:
        return True
    return None


def _get_text(tag: Optional[Tag]) -> str:
    if tag is None:
        return ""
    return tag.get_text(" ", strip=True)


def remove_special_characters(text: str) -> str:
    """Loại ký tự symbol như emoji, giữ chữ, số, khoảng trắng và dấu câu."""
    return "".join(char for char in text if not unicodedata.category(char).startswith("S")).strip()


# ---------------------------------------------------------------------------
# Parse bảng section.moreinfor1 -> dict {label: value}.
# Mỗi <tr> gồm các cặp (label, value) tuần tự, số cột không đều (colspan),
# nên không dựa vào vị trí cố định mà ghép theo thứ tự xuất hiện.
# ---------------------------------------------------------------------------

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


def parse_old_address(raw: str) -> tuple[str, str, str]:
    """Tách (ward_old, district_old, province_old) từ address_old_raw dạng
    'Đường X, Phường/Xã Y, Quận/Huyện Z, Tỉnh/Thành (cũ)' — lấy 3 phần cuối
    theo dấu ',' từ phải, giữ nguyên tiền tố. PHẢI lưu riêng province_old vì
    có tin lệch tỉnh cũ/mới do sáp nhập địa giới (VD: Long Điền cũ thuộc
    'Bà Rịa Vũng Tàu', mới thuộc 'Hồ Chí Minh') — không coi là trùng province_new.
    Trả về ("", "", "") khi raw rỗng hoặc không đủ 3 phần."""
    if not raw:
        return "", "", ""
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) < 3:
        return "", "", ""
    ward_old = parts[-3]
    district_old = parts[-2]
    province_old = parts[-1]
    return ward_old, district_old, province_old


IN_SCOPE_PROVINCE = "Hồ Chí Minh"
IN_SCOPE_LISTING_TYPES = {"Cần bán", "Cho thuê"}
IN_SCOPE_PROPERTY_TYPES = {
    "Biệt thự, nhà liền kề",
    "Căn hộ chung cư",
    "Nhà mặt tiền",
    "Nhà trong hẻm",
    "Phòng trọ, nhà trọ",
}

def is_in_scope(listing: ParsedListing) -> bool:
    """Kiểm tra tin có thuộc phạm vi đồ án không — ngoài phạm vi bị bỏ qua lặng lẽ
    ở parse_partition(), không ghi staging lẫn quarantine.
    Lọc theo address_province_new (địa chỉ HIỆN TẠI) — KHÔNG dùng
    address_province_old (tỉnh trước sáp nhập, có thể khác province_new)."""
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
    title = remove_special_characters(_get_text(title_tag))
    if not title:
        return _fail("thieu title (itemprop=name)")

    # posted_date: BẮT BUỘC lấy attribute datetime, (text có thể là "Hôm nay"/"Hôm qua").
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
    # Null hóa + gắn cờ nếu area_m2 vượt ngưỡng hợp lý (đất >10.000m2 không
    # thuộc phạm vi đồ án, hoặc <3m2 nghi field khác bị nhầm vào ô diện tích
    # — gần như chắc chắn lỗi parse thập phân/nhập liệu).
    area_m2, area_is_outlier = _sanitize_area(area_m2)

    # Phát hiện giá/m2 phi lý (Phase 5) — PHẢI tính SAU khi area_m2 đã
    # sanitize ở trên (dùng area_m2 cuối cùng, không dùng giá trị thô).
    price_is_outlier = _detect_price_outlier(price_vnd, area_m2)

    # is_expired / has_warning: KHÔNG cố định vị trí trong article -> tìm
    # toàn bộ subtree thay vì chỉ children trực tiếp.
    is_expired = article.find(class_="expired") is not None
    has_warning = article.find(class_="warning") is not None

    # --- Địa chỉ: CHUỖI, dùng "" khi thiếu (không dùng "or None" nữa) ---
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
        return _fail("thieu section.moreinfor1")
    fields = _parse_moreinfor_table(moreinfor_section)

    listing_type = _get_text(fields.get("Loại tin"))
    if not listing_type:
        return _fail("thieu 'Loai tin' trong bang moreinfor1")

    property_type = _get_text(fields.get("Loại BDS"))
    if not property_type:
        return _fail("thieu 'Loai BDS' trong bang moreinfor1")

    def _num(label: str) -> Optional[Decimal]:
        cell = fields.get(label)
        return parse_vn_number(_get_text(cell)) if cell is not None else None

    def _int(label: str) -> Optional[int]:
        value = _num(label)
        return int(value) if value is not None else None

    def _text_or_empty(label: str) -> str:
        """Trả về "" khi field không tồn tại hoặc là marker thiếu dữ liệu
        ('-','--','---','_',...) — theo quy ước dự án (xem module docstring)."""
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
        address_province_old=address_province_old,
    )