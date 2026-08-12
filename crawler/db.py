"""
Kết nối Postgres DW.

Bài học đã ghi trong error_log.md (2026/08/11): script chạy trên máy host
(ngoài Docker network) không resolve được hostname 'postgres-dw', PHẢI
dùng 'localhost:5433'. Module này tự chọn đúng DSN theo nơi code đang
chạy, thay vì bắt người dùng tự export biến môi trường bằng tay
(nguồn lỗi shell bash/PowerShell không tương thích đã gặp trước đó).
"""
import psycopg2
import psycopg2.extras

from crawler import config


def _resolve_dsn() -> str:
    if config.RUNNING_IN_CONTAINER:
        dsn = config.POSTGRES_DW_DSN
        if not dsn:
            raise RuntimeError(
                "Đang chạy trong container nhưng thiếu POSTGRES_DW_DSN trong .env"
            )
        return dsn

    dsn = config.POSTGRES_DW_DSN_LOCAL
    if not dsn:
        raise RuntimeError(
            "Đang chạy trên máy host nhưng thiếu POSTGRES_DW_DSN_LOCAL trong .env "
            "(nhắc lại: hostname 'postgres-dw' KHÔNG resolve được từ host, "
            "phải dùng 'localhost:5433')"
        )
    return dsn


def get_conn():
    """Trả về 1 connection psycopg2 mới, autocommit=False (tự quản lý transaction)."""
    return psycopg2.connect(_resolve_dsn())


def dict_cursor(conn):
    """Cursor trả kết quả dạng dict (RealDictRow) — tiện cho các hàm queue_manager/parser."""
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
