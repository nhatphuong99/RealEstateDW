"""Kết nối postgres-dw, dùng psycopg2 với context manager an toàn (tự commit/rollback)."""
import contextlib

import psycopg2
import psycopg2.extras

from . import config


@contextlib.contextmanager
def get_conn():
    conn = psycopg2.connect(config.DB_DSN)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_dict_cursor(conn):
    """Trả về cursor dạng dict (RealDictCursor) để thao tác dữ liệu thuận tiện hơn."""
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
