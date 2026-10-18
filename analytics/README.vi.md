# analytics/ — Analytics layer cho RealEstateDW

🇻🇳 Tiếng Việt (file này) · 🇬🇧 [English](README.md)

Về README chính của project (pipeline Data Engineering): [../README.vi.md](../README.vi.md)

> Snapshot dữ liệu: crawl đến 2026-10-04; số liệu đo ngày 2026-10-12 trên 245.000 listing. Mọi con số lấy từ [`docs/metrics.md`](docs/metrics.md) mục 5.
> Hệ thống crawl chỉ bật khi cần dữ liệu (không chạy 24/7), nên analytics là một bản duy nhất theo snapshot này, không đánh phiên bản.

## 1. Tổng quan

RealEstateDW crawl tin đăng từ alonhadat.com.vn vào kho dữ liệu Medallion (Bronze, Silver, Gold). Gold đã sạch nhưng chưa trả lời được các câu hỏi BI: giá/m² theo khu vực, nhà mặt tiền đắt hơn nhà hẻm bao nhiêu, yield cho thuê, và dữ liệu đáng tin đến đâu.

`analytics/` thêm một **lớp phân tích chỉ đọc Gold** (3 view PostgreSQL) và một **dashboard Metabase 7 tab**. Mỗi tab trả lời một câu hỏi.

- Không sửa Gold, Silver, pipeline và 13 card Metabase cũ. Mọi thứ nằm trong schema `analytics`.
- Báo cáo bằng median, không dùng mean; ẩn nhóm có n < 30; nêu rõ hạn chế.

## 2. Kiến trúc

```
alonhadat.com.vn
      |
      v
  Bronze (S3, Parquet) --> Silver (SCD2) --> Gold: gold.vw_fact_report
                                                      |
  pipeline.run_state (lịch sử chạy DAG 2)             |  (chỉ đọc)
            |                                         |
            v                                         v
        +---------------------------------------------------+
        |  schema analytics (3 view)                        |
        |  - vw_listing_latest   (1 dòng mỗi listing)       |
        |  - vw_listing_versions (version đã loại trùng)    |
        |  - vw_crawl_coverage   (1 dòng mỗi ngày crawl)    |
        +---------------------------------------------------+
                              |
                              v
                  Metabase: dashboard 7 tab (Q1-Q7)
```

| View | Grain | Vai trò |
|---|---|---|
| `vw_listing_latest` | 1 dòng mỗi listing (version có `MAX(valid_from)`) | Mọi cờ và quy tắc lọc nằm một chỗ; `is_analysis_ready` là cột lọc duy nhất cho giá/m² |
| `vw_listing_versions` | 1 dòng mỗi version thật của listing | Đã loại version liên tiếp trùng hash; dùng cho Q5 |
| `vw_crawl_coverage` | 1 dòng mỗi ngày, kể cả ngày không chạy | Mặt nạ "ngày thiếu dữ liệu" cho Q6, Q7 |

Chi tiết cột: [`docs/data_dictionary.md`](docs/data_dictionary.md).

## 3. Dashboard 7 tab

Metabase collection "Analytics". SQL từng card nằm ở `sql/cards/`, đúng bản đang dùng trên Metabase.

| Tab | Câu hỏi | Kết quả chính | SQL |
|---|---|---|---|
| Q1 Mặt bằng giá | Giá/m² theo khu vực, loại hình, bán/thuê | Nhà trong hẻm, Cần bán, hcm_old: median 119,63 triệu/m² (p25 88,08; p75 163,27; n 120.046) | `q1_price_level.sql` |
| Q2 Mặt tiền vs Hẻm | Nhà mặt tiền đắt hơn nhà hẻm bao nhiêu % cùng phường | hcm_old: premium median 50,81% (258 ô, 89 phường đủ mẫu) | `q2_frontage_premium.sql` |
| Q3 Gross yield | Yield cho thuê theo quận | hcm_old: nhà trong hẻm 1,99%/năm (14 quận); căn hộ 3,43% (17); nhà mặt tiền 1,69% (17) | `q3_gross_yield.sql` |
| Q4 Diện tích | Chiết khấu giá/m² theo diện tích | Nhà trong hẻm so với nhóm 40–60 m²: +11,33% (<40 m²), −10,34% (60–100), −23,25% (≥100) | `q4_area_discount.sql` |
| Q5 Đổi giá | Listing được web gặp lại có đổi giá không | Cả hai loại tin: 996 cặp đổi giá; 84,54% là giảm; p25 / median / p75 = −9,09 / −5,06 / −2,11 (%) | `q5_price_change.sql` |
| Q6 Độ tin cậy | Dữ liệu đáng tin đến đâu | 17 ngày không có lần chạy; 12 ngày chạy thiếu (dưới 20 lần); 4.350 version trùng hash đã loại | `q6_reliability.sql` |
| Q7 Ổn định theo tuần | Giá/m² theo tuần có ổn định không | Dataset median 118,75 → 120,19; web 119,23 / 117,00 / 131,91; không kết luận xu hướng thị trường | `q7_stability.sql` |

Cách đọc đúng:
- Q5 là phân bố mức đổi giá **trong số cặp dataset → web có hash khác nhau** (cách nhau 14–98 ngày). Không đọc thành "x% listing đổi giá".
- Q7 không nối chuỗi dataset với chuỗi web; hai nguồn khác cơ cấu và độ phủ.
- Q4: căn hộ ≥100 m² và biệt thự chỉ có 4 ô mỗi nhóm, không kết luận.

## 4. Độ tin cậy dữ liệu: lỗi đã phát hiện và cách xử lý

| Vấn đề | Bằng chứng | Xử lý |
|---|---|---|
| Merge SCD2 đảo thứ tự version ở Silver | 4.850 listing sai `is_current` / `valid_to`; 4.350 version trùng hash | Bản mới nhất = `MAX(valid_from)`; loại bản trùng trong `vw_listing_versions`; không sửa pipeline (ngoài phạm vi) |
| Sai đơn vị giá, tin gắn nhãn sai | Bán < 1 triệu/m²: 31 listing; thuê ≥ 10 triệu/m²/tháng: 12; thuê < 10.000 đ/m²/tháng: 4 | Loại bằng quy tắc R1, R4, R6 |
| Sai diện tích (mất dấu thập phân, hoặc tổng diện tích sàn nhà nhiều tầng) | 282 listing có diện tích gấp khoảng 10 hoặc 100 lần dài × ngang | Gắn cờ `area_suspect`, loại khỏi giá/m² (R7) |
| Crawler tắt khi chưa cần dữ liệu | 17 trên 40 ngày không chạy, 12 ngày chạy thiếu | Mặt nạ `vw_crawl_coverage` ở Q6, Q7; không kết luận xu hướng |
| Hai nguồn không so sánh được | Web không crawl lại URL cũ: hết hạn 28,33% (dataset) so với 0,02% (web) | Tách hai nguồn thành hai chuỗi; không so tỷ lệ hết hạn |

## 5. Quy tắc phân tích

- Median (`percentile_cont`) kèm p25, p75; n luôn hiển thị; nhóm n < 30 bị ẩn.
- Không trộn tin bán và tin thuê trong cùng một phép tính.
- Điều kiện luôn đúng thì ghi cứng trong SQL; điều kiện người xem đổi thì dùng Field Filter + bộ lọc dashboard (giá trị mặc định đặt ở dashboard). Card dùng Field Filter không đặt alias cho `analytics.vw_listing_latest`.
- Mọi số hiển thị `ROUND` 2 chữ số.
- Định nghĩa chỉ số, quy tắc R1–R9, cửa sổ thời gian: [`docs/metrics.md`](docs/metrics.md).

## 6. Cách chạy lại

Yêu cầu: PostgreSQL 16 đã có `gold.vw_fact_report` và `pipeline.run_state`; Metabase kết nối vào database này.

1. **Tạo 3 view** (PowerShell, từ thư mục gốc project; chạy lại nhiều lần được):
   ```powershell
   $env:PGCLIENTENCODING = 'UTF8'
   psql "$env:POSTGRES_DW_DSN_LOCAL" -v ON_ERROR_STOP=1 -f analytics/sql/01_create_base_views.sql
   ```
2. **Kiểm bất biến:** chạy `qa/02_invariants.sql`; kỳ vọng 4 truy vấn đều trả 0 dòng.
3. **Metabase:** sync schema; với mỗi card, tạo Native query từ file trong `sql/cards/`, map Field Filter theo chú thích đầu file, nối bộ lọc dashboard và đặt giá trị mặc định theo [`docs/metrics.md`](docs/metrics.md) mục 4. Q1 CARD 3 cần bản đồ phường (GeoJSON) đã đăng ký trong Metabase.
4. **Kiểm chéo bằng pandas** (tùy chọn): xuất `latest_ready.csv` và `median_by_group.csv` từ DataGrip vào `analytics/qa/_out/`, rồi chạy `python analytics\qa\recompute_median.py`; dòng cuối phải là "KẾT QUẢ: KHỚP".

Đã kiểm gì và kiểm thế nào: [`docs/qa.md`](docs/qa.md).

## 7. Hạn chế đã biết

Danh sách đầy đủ: [`docs/metrics.md`](docs/metrics.md) mục 7.

1. Giá rao, không phải giá giao dịch.
2. Hai nguồn khác độ phủ và cơ cấu; web bắt đầu 2026-08-26, có khoảng trống 08-14 → 08-25 và các ngày tắt hệ thống. `valid_from` là ngày crawl, không phải ngày nạp Gold.
3. Q5 chỉ gồm cặp dataset → web có hash khác nhau, không đại diện thị trường; 13 cặp nghi lỗi (1,3%); tab Cho thuê chỉ có 89 cặp đổi giá.
4. `posted_date` nhiều khả năng là ngày đăng hoặc làm mới gần nhất; số tin theo tuần phản ánh lịch crawl.
5. Lỗi đơn vị giá nằm giữa phân phối không bị bắt; `area_m2` của nhà nhiều tầng thường là tổng diện tích sàn.
6. Tỷ trọng `region_group` chưa đo lại sau lần chạy pipeline thứ hai.

## 8. Cấu trúc thư mục

```
analytics/
  README.md / README.vi.md
  docs/    metrics.md, data_dictionary.md, qa.md
  sql/     01_create_base_views.sql
    cards/ q1 ... q7 (SQL từng card Metabase)
  qa/      01_ ... 06_*.sql, q5_sizing.sql, q5_qa.sql, recheck_flags_q1.sql,
           recompute_median.py, recompute_q2_cell.py
  assets/dashboard/   7 ảnh chốt dashboard
```

## 9. Ảnh dashboard

![Q1 Mặt bằng giá](assets/dashboard/01_mat_bang_gia.png)
![Q2 Mặt tiền vs Hẻm](assets/dashboard/02_mat_tien_vs_hem.png)
![Q3 Gross yield](assets/dashboard/03_gross_yield.png)
![Q4 Diện tích](assets/dashboard/04_dien_tich.png)
![Q5 Đổi giá](assets/dashboard/05_doi_gia.png)
![Q6 Độ tin cậy](assets/dashboard/06_do_tin_cay.png)
![Q7 Ổn định theo tuần](assets/dashboard/07_on_dinh_theo_tuan.png)
