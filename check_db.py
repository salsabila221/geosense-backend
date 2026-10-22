"""Cek cepat koneksi database. Jalankan: python check_db.py"""

import sys

from sqlalchemy import text

import database

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def main() -> int:
    info = database.database_info()
    print(f"Target : {info['dialect']} -> {info['target']}")
    print(f"Pooler : {info['pooler']}")

    try:
        with database.engine.connect() as conn:
            print("Ping   :", conn.execute(text("select 1")).scalar())
    except Exception as exc:
        print("\nâŒ Gagal konek database:")
        print(f"{type(exc).__name__}: {exc}")
        print(
            "\nTips:\n"
            "  - Password salah? Reset di Supabase -> Project Settings -> Database.\n"
            "  - Karakter spesial di password harus di-URL-encode (@ -> %40, # -> %23).\n"
            "  - Username pooler harus 'postgres.<project-ref>', bukan 'postgres'.\n"
            "  - Kalau tidak mau pakai Supabase, kosongkan DATABASE_URL -> pakai SQLite."
        )
        return 1

    print("\nâœ… Koneksi database OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())

