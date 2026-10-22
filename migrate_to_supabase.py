"""
Salin data user dari SQLite lokal (geosense.db) ke database di DATABASE_URL
(mis. Supabase Postgres).

Jalankan: python migrate_to_supabase.py
Aman diulang: user yang email-nya sudah ada di target akan dilewati.
"""

import datetime
import os
import sqlite3
import sys

from sqlalchemy import inspect

import database
import models

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

SQLITE_PATH = os.getenv("SQLITE_SOURCE", "geosense.db")
COLUMNS = [
    "email",
    "fullname",
    "phone_number",
    "google_sub",
    "picture",
    "role",
    "is_active",
    "created_at",
    "last_login",
]


def parse_dt(value):
    if not value:
        return None
    if isinstance(value, datetime.datetime):
        return value
    try:
        return datetime.datetime.fromisoformat(str(value))
    except ValueError:
        return None


def main() -> int:
    info = database.database_info()
    print(f"Target : {info['dialect']} -> {info['target']}")

    if info["dialect"] == "sqlite":
        print("Target masih SQLite. Isi DATABASE_URL dulu di .env.")
        return 1

    if not os.path.exists(SQLITE_PATH):
        print(f"Sumber {SQLITE_PATH} tidak ditemukan. Tidak ada yang disalin.")
        return 1

    # Pastikan tabel ada di target
    models.Base.metadata.create_all(bind=database.engine)
    database.ensure_schema()

    source = sqlite3.connect(SQLITE_PATH)
    source.row_factory = sqlite3.Row
    source_columns = {row[1] for row in source.execute("PRAGMA table_info(users)")}
    selected = [c for c in COLUMNS if c in source_columns]
    rows = list(source.execute(f"SELECT {', '.join(selected)} FROM users"))
    source.close()

    print(f"Sumber : {SQLITE_PATH} ({len(rows)} user)")

    # Cek kolom yang benar-benar ada di target (jaga-jaga kalau skema beda)
    target_columns = {col["name"] for col in inspect(database.engine).get_columns("users")}

    db = database.SessionLocal()
    inserted = skipped = 0
    try:
        for row in rows:
            email = (row["email"] or "").strip().lower()
            if not email:
                continue

            if db.query(models.User).filter(models.User.email == email).first():
                skipped += 1
                continue

            data = {"email": email}
            for column in COLUMNS[1:]:
                if column not in row.keys() or column not in target_columns:
                    continue
                value = row[column]
                if column in ("created_at", "last_login"):
                    value = parse_dt(value)
                if column == "is_active":
                    value = bool(value) if value is not None else True
                data[column] = value

            db.add(models.User(**data))
            inserted += 1

        db.commit()
    except Exception as exc:
        db.rollback()
        print(f"Gagal migrasi: {type(exc).__name__}: {exc}")
        return 1
    finally:
        db.close()

    print(f"Selesai: {inserted} user disalin, {skipped} dilewati (sudah ada).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
