"""
crawler/queue_reset.py

Cơ chế "reset định kỳ" cho crawl.crawl_queue — bổ sung 2026/08/13 (lần 3).

VẤN ĐỀ: alonhadat.com.vn KHÔNG sort tin theo thời gian đăng (đã xác nhận
thực nghiệm, xem tong_hop_boi_canh_crawler_alonhadat.md mục 8). Cơ chế
enqueue_next_page() hiện tại chỉ SINH THÊM trang phía sau (page_num tăng
dần) mỗi khi trang hiện tại crawl thành công — nghĩa là 1 khi trang 1, 2,
3... đã ở status='success', chúng KHÔNG BAO GIỜ được crawl lại nữa. Nếu
tin VIP/được đẩy liên tục chiếm giữ các trang đầu, tin MỚI bị chôn ở đó
sẽ bị bỏ sót VĨNH VIỄN.

GIẢI PHÁP: mỗi category có 1 mốc last_full_reset_at (bảng
crawl.category_reset_state). Nếu đã quá config.QUEUE_FULL_RESET_INTERVAL_HOURS
giờ kể từ lần reset gần nhất (hoặc chưa từng reset) -> đưa TOÀN BỘ
[URL-DS] của category đó (mọi trang, mọi trạng thái TRỪ 'in_progress'
đang chạy dở) về lại 'pending', reset attempt_count=0,
next_retry_after=NULL — để claim_batch() ưu tiên crawl lại từ trang 1
(FIFO theo created_at cũ, các row này có created_at cũ hơn nên được ưu
tiên claim trước các trang mới hơn).

Đây là chi phí ĐÃ ĐƯỢC CHẤP NHẬN từ trước (mục 8.2 #6, mục 9.5
tong_hop_boi_canh_crawler_alonhadat.md): full re-crawl định kỳ là cách
an toàn duy nhất để không bỏ sót tin mới khi site không sort theo thời
gian. Việc reset chỉ ĐÁNH DẤU pending, không crawl ngay lập tức — các
run_batch() tiếp theo sẽ dần dần xử lý lại theo đúng nhịp
MAX_PAGES_PER_RUN/delay bình thường, không có burst request nào phát
sinh từ việc reset.

An toàn khi nhiều tiến trình gọi gần nhau: dùng UPDATE ... RETURNING
trên category_reset_state để "claim" quyền reset 1 cách nguyên tử —
chỉ đúng 1 lần gọi thực sự thực hiện reset dù nhiều run_batch() có thể
trigger gần nhau.
"""
import logging
from typing import List, Optional

from crawler import config

logger = logging.getLogger(__name__)


def reset_stale_categories(conn, interval_hours: Optional[int] = None) -> List[str]:
    """
    Kiểm tra từng category trong config.CATEGORIES; category nào đã quá
    hạn (xem docstring module) thì reset toàn bộ [URL-DS] của nó về
    'pending'. Trả về danh sách category vừa được reset trong lần gọi này
    (rỗng ở đa số lần gọi — bình thường, vì hầu hết category chưa đến hạn).
    """
    if interval_hours is None:
        interval_hours = config.QUEUE_FULL_RESET_INTERVAL_HOURS

    reset_categories: List[str] = []

    with conn.cursor() as cur:
        for category in config.CATEGORIES:
            # Đảm bảo luôn có 1 row cho category (lần đầu tiên chưa từng reset).
            cur.execute(
                """
                INSERT INTO crawl.category_reset_state (category, last_full_reset_at)
                VALUES (%s, NULL)
                ON CONFLICT (category) DO NOTHING
                """,
                (category,),
            )

            # "Claim" quyền reset 1 cách nguyên tử: chỉ UPDATE thành công
            # (trả về 1 row) nếu thực sự đã quá hạn hoặc chưa từng reset.
            cur.execute(
                """
                UPDATE crawl.category_reset_state
                SET last_full_reset_at = now()
                WHERE category = %s
                  AND (last_full_reset_at IS NULL
                       OR last_full_reset_at < now() - (%s * INTERVAL '1 hour'))
                RETURNING category
                """,
                (category, interval_hours),
            )
            claimed = cur.fetchone()
            if not claimed:
                continue

            cur.execute(
                """
                UPDATE crawl.crawl_queue
                SET status = 'pending',
                    attempt_count = 0,
                    next_retry_after = NULL,
                    updated_at = now()
                WHERE category = %s AND status != 'in_progress'
                """,
                (category,),
            )
            reset_categories.append(category)
            logger.warning(
                "RESET ĐỊNH KỲ: đưa toàn bộ [URL-DS] của category=%s về 'pending' "
                "(đã quá %d giờ kể từ lần reset gần nhất, hoặc lần đầu tiên) — "
                "sẽ crawl lại từ trang 1 dần dần ở các run_batch() tiếp theo.",
                category, interval_hours,
            )

    conn.commit()
    return reset_categories
