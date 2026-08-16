"""
Script: demo_reset_tool.py

CONG CU CLI de RESET / MO PHONG cac trang thai loi trong crawl.dataset_part_queue
(Postgres) va S3 - phuc vu chay lai DAG NHIEU LAN trong qua trinh test/demo do
an, ma khong can go SQL tay moi lan.

CHI DUNG CHO MOI TRUONG TEST/DEMO - khong dung tren du lieu production that.

Cac lenh (subcommand):

  status                          Xem tong quan trang thai hien tai cua queue
  list --status <status>          Liet ke chi tiet cac part theo 1 trang thai

  full-reset [--yes]              XOA SACH crawl.dataset_part_queue + crawl.crawl_run
                                   (RESTART IDENTITY) - de chay lai tu dau.
                                   KHONG dong toi du lieu tren S3 (dung
                                   --wipe-s3 neu muon xoa luon).
  full-reset --wipe-s3 [--yes]    Nhu tren, VA xoa toan bo object duoi
                                   prefix "bronze/" tren S3 - CAN THAN, khong
                                   the hoan tac.

  simulate-failed --parts 3,5,9 [--message TEXT]
                                   Danh dau cac part nay la status='failed'
                                   (mo phong: "cac part nay da tung tai LOI
                                   o lan crawl truoc"). Lan chay DAG tiep
                                   theo se TU DONG retry lai (hanh vi mac
                                   dinh cua list_processable_parts()).

  simulate-missing-row --parts 12,15
                                   XOA HAN dong cua cac part nay khoi
                                   Postgres (mo phong: "dong bi xoa nham
                                   khoi DB", hoac "chua tung duoc dua vao
                                   queue do loi quet truoc do"). Lan chay
                                   DAG tiep theo, scan_and_fill_gaps() se
                                   tu phat hien va bo sung lai (NEU part do
                                   nam trong pham vi tam nhin hien tai).

  simulate-deleted-s3 --parts 7   XOA THAT file tren S3 cua cac part dang
                                   status='success' (dong Postgres GIU
                                   NGUYEN 'success' - mo phong dung kich ban
                                   "ai do xoa file S3 truc tiep"). Lan chay
                                   DAG tiep theo, reconcile_missing_storage_
                                   objects() se tu phat hien va tai lai.

Vi du:
    python demo_reset_tool.py status
    python demo_reset_tool.py full-reset --yes
    python demo_reset_tool.py simulate-failed --parts 23,24 --message "gia lap loi mang"
    python demo_reset_tool.py simulate-missing-row --parts 27,28,29
    python demo_reset_tool.py simulate-deleted-s3 --parts 15
"""

from __future__ import annotations

import argparse
import sys

import boto3
import psycopg2

from bronze_crawler_postgres_store import DW_DSN
from bronze_crawler_io import S3_BUCKET, AWS_REGION


def _connect():
    return psycopg2.connect(DW_DSN)


def _confirm(prompt: str, auto_yes: bool) -> bool:
    if auto_yes:
        return True
    answer = input(f"{prompt} (go 'yes' de xac nhan): ").strip().lower()
    return answer == "yes"


def _parse_parts(parts_str: str) -> list:
    try:
        return [int(p.strip()) for p in parts_str.split(",") if p.strip()]
    except ValueError:
        sys.exit(f"[LOI] Danh sach part khong hop le: '{parts_str}' (vi du hop le: '3,5,9')")


# -----------------------------------------------------------------------------
# status / list
# -----------------------------------------------------------------------------
def cmd_status(args):
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status, COUNT(*) FROM crawl.dataset_part_queue GROUP BY status ORDER BY status;"
        )
        rows = cur.fetchall()
        cur.execute("SELECT COALESCE(MAX(part_number), 0) FROM crawl.dataset_part_queue;")
        max_part = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM crawl.crawl_run;")
        total_runs = cur.fetchone()[0]

    print(f"\n=== Tong quan crawl.dataset_part_queue (max_part_number = {max_part}) ===")
    if not rows:
        print("  (queue dang RONG)")
    for status, count in rows:
        print(f"  {status:<12} : {count}")
    print(f"\nTong so lan crawl_run da ghi nhan: {total_runs}")
    print()


def cmd_list(args):
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT part_number, status, attempts, last_error, s3_key, updated_at
            FROM crawl.dataset_part_queue
            WHERE status = %s
            ORDER BY part_number ASC;
            """,
            (args.status,),
        )
        rows = cur.fetchall()

    if not rows:
        print(f"Khong co part nao o trang thai '{args.status}'.")
        return

    print(f"\n=== {len(rows)} part o trang thai '{args.status}' ===")
    for part_number, status, attempts, last_error, s3_key, updated_at in rows:
        print(f"  part{part_number:<4} attempts={attempts} updated_at={updated_at}")
        if last_error:
            print(f"      last_error: {last_error[:150]}")
        if s3_key:
            print(f"      s3_key: {s3_key}")
    print()


# -----------------------------------------------------------------------------
# full-reset
# -----------------------------------------------------------------------------
def cmd_full_reset(args):
    if not _confirm(
        "Sap XOA SACH crawl.dataset_part_queue + crawl.crawl_run"
        + (" VA TOAN BO du lieu tren S3 (prefix 'bronze/')" if args.wipe_s3 else ""),
        args.yes,
    ):
        print("Da huy.")
        return

    with _connect() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE crawl.dataset_part_queue RESTART IDENTITY CASCADE;")
        cur.execute("TRUNCATE TABLE crawl.crawl_run RESTART IDENTITY CASCADE;")
        conn.commit()
    print("[OK] Da xoa sach crawl.dataset_part_queue va crawl.crawl_run.")

    if args.wipe_s3:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        paginator = s3.get_paginator("list_objects_v2")
        deleted = 0
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="bronze/"):
            keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if keys:
                s3.delete_objects(Bucket=S3_BUCKET, Delete={"Objects": keys})
                deleted += len(keys)
        print(f"[OK] Da xoa {deleted} object tren S3 duoi prefix 'bronze/'.")

    print(
        "\n[NHAC] Neu dang dung che do mo phong (simulation), nho reset lai "
        "Airflow Variable 've moc ban dau:\n"
        "  docker compose exec airflow-scheduler airflow variables set "
        "alonhadat_sim_max_visible_part 50\n"
        "(hoac xoa han Variable nay de dung gia tri mac dinh 50 - xem lenh "
        "'airflow variables delete alonhadat_sim_max_visible_part')."
    )


# -----------------------------------------------------------------------------
# simulate-failed
# -----------------------------------------------------------------------------
def cmd_simulate_failed(args):
    parts = _parse_parts(args.parts)
    message = args.message or "gia_lap_loi_thu_cong_qua_demo_reset_tool"

    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE crawl.dataset_part_queue
            SET status = 'failed', last_error = %s, updated_at = now()
            WHERE part_number = ANY(%s);
            """,
            (message, parts),
        )
        affected = cur.rowcount
        conn.commit()

    print(f"[OK] Da danh dau {affected}/{len(parts)} part la 'failed': {parts}")
    if affected < len(parts):
        print(
            "[CANH BAO] Mot so part trong danh sach CHUA TUNG co trong queue "
            "(khong the danh dau 'failed' cho part chua ton tai) - dung lenh "
            "'status' hoac 'list --status pending' de kiem tra truoc."
        )


# -----------------------------------------------------------------------------
# simulate-missing-row
# -----------------------------------------------------------------------------
def cmd_simulate_missing_row(args):
    parts = _parse_parts(args.parts)

    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM crawl.dataset_part_queue WHERE part_number = ANY(%s);",
            (parts,),
        )
        affected = cur.rowcount
        conn.commit()

    print(f"[OK] Da XOA {affected}/{len(parts)} dong khoi Postgres: {parts}")
    print(
        "[LUU Y] Neu cac part nay TRUOC DO da 'success', file tren S3 VAN CON "
        "NGUYEN (lenh nay khong dong toi S3) - lan crawl tiep theo se tai lai "
        "va GHI DE len file cu (idempotent, khong sao)."
    )


# -----------------------------------------------------------------------------
# simulate-deleted-s3
# -----------------------------------------------------------------------------
def cmd_simulate_deleted_s3(args):
    parts = _parse_parts(args.parts)

    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT part_number, s3_key FROM crawl.dataset_part_queue "
            "WHERE part_number = ANY(%s) AND status = 'success';",
            (parts,),
        )
        rows = cur.fetchall()

    found_parts = {pn for pn, _ in rows}
    missing_parts = set(parts) - found_parts
    if missing_parts:
        print(
            f"[CANH BAO] Cac part sau KHONG o trang thai 'success' (bo qua): "
            f"{sorted(missing_parts)}"
        )

    if not rows:
        print("[LOI] Khong co part nao hop le de mo phong xoa S3.")
        return

    s3 = boto3.client("s3", region_name=AWS_REGION)
    deleted = []
    for part_number, s3_key in rows:
        if not s3_key:
            print(f"[BO QUA] part{part_number} khong co s3_key ghi nhan.")
            continue
        s3.delete_object(Bucket=S3_BUCKET, Key=s3_key)
        deleted.append(part_number)
        print(f"[OK] Da xoa file S3 cua part{part_number}: {s3_key}")

    print(
        f"\n[LUU Y QUAN TRONG] Dong Postgres cua {deleted} VAN CON status='success' "
        f"(CO Y - day chinh la kich ban can mo phong: DB noi 'con', S3 thi 'mat'). "
        f"Lan chay DAG tiep theo, reconcile_missing_storage_objects() se phat "
        f"hien va tu dong tai lai."
    )


def main():
    parser = argparse.ArgumentParser(
        description="Cong cu reset/mo phong trang thai crawler (CHI DUNG CHO TEST/DEMO)"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_status = sub.add_parser("status", help="Xem tong quan trang thai queue")
    p_status.set_defaults(func=cmd_status)

    p_list = sub.add_parser("list", help="Liet ke chi tiet part theo trang thai")
    p_list.add_argument(
        "--status", required=True, choices=["pending", "downloading", "success", "failed"]
    )
    p_list.set_defaults(func=cmd_list)

    p_reset = sub.add_parser("full-reset", help="Xoa sach queue (va tuy chon S3) de chay lai tu dau")
    p_reset.add_argument("--wipe-s3", action="store_true", help="Xoa luon toan bo object tren S3")
    p_reset.add_argument("--yes", action="store_true", help="Bo qua buoc xac nhan")
    p_reset.set_defaults(func=cmd_full_reset)

    p_fail = sub.add_parser("simulate-failed", help="Gia lap cac part bi loi o lan crawl truoc")
    p_fail.add_argument("--parts", required=True, help="Danh sach part_number, cach nhau boi dau phay")
    p_fail.add_argument("--message", default=None, help="Noi dung last_error tuy chinh")
    p_fail.set_defaults(func=cmd_simulate_failed)

    p_miss = sub.add_parser(
        "simulate-missing-row", help="Gia lap dong Postgres bi xoa (lo hong o giua)"
    )
    p_miss.add_argument("--parts", required=True, help="Danh sach part_number, cach nhau boi dau phay")
    p_miss.set_defaults(func=cmd_simulate_missing_row)

    p_delS3 = sub.add_parser(
        "simulate-deleted-s3", help="Gia lap file S3 bi xoa thu cong (DB van ghi success)"
    )
    p_delS3.add_argument("--parts", required=True, help="Danh sach part_number, cach nhau boi dau phay")
    p_delS3.set_defaults(func=cmd_simulate_deleted_s3)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
