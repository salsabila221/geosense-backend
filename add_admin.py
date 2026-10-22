"""
Tambah / perbarui admin langsung di database (tanpa lewat login Google).

Contoh:
    python add_admin.py luthfi24002@mail.unpad.ac.id "M.Luthfi Firdaus" 081311960981
    python add_admin.py email@x.com "Nama Lengkap" 08123456789
"""

import datetime
import re
import sys

import database
import models

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

PHONE_PATTERN = re.compile(r"^\+628[1-9]\d{6,11}$")


def normalize_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw or "")
    if digits.startswith("0"):
        digits = "62" + digits[1:]
    elif digits.startswith("8"):
        digits = "62" + digits
    return f"+{digits}" if digits else ""


def upsert_admin(email: str, fullname: str, phone_raw: str) -> str:
    email = email.strip().lower()
    phone = normalize_phone(phone_raw)

    if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email):
        raise SystemExit(f"Email tidak valid: {email}")
    if not PHONE_PATTERN.match(phone):
        raise SystemExit(f"Nomor tidak valid: {phone_raw} (contoh: 08123456789)")

    db = database.SessionLocal()
    try:
        user = db.query(models.User).filter(models.User.email == email).first()
        if user:
            user.fullname = fullname.strip() or user.fullname
            user.phone_number = phone
            user.role = "admin"
            user.is_active = True
            result = "diperbarui"
        else:
            db.add(
                models.User(
                    email=email,
                    fullname=fullname.strip(),
                    phone_number=phone,
                    role="admin",
                    is_active=True,
                    created_at=datetime.datetime.utcnow(),
                )
            )
            result = "ditambahkan"

        db.commit()
        return f"[OK] {email} {result} | {fullname} | {phone} | role=admin"
    except Exception as exc:
        db.rollback()
        raise SystemExit(f"Gagal menyimpan {email}: {type(exc).__name__}: {exc}")
    finally:
        db.close()


def main() -> int:
    if len(sys.argv) != 4:
        print(__doc__)
        return 1

    print(upsert_admin(sys.argv[1], sys.argv[2], sys.argv[3]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
