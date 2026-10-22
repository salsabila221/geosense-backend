import datetime
import os

import jwt
from dotenv import load_dotenv
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.orm import Session

import database
import models

load_dotenv()

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-geosense-ganti-di-produksi")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_HOURS = int(os.getenv("JWT_EXPIRE_HOURS", "168"))  # default 7 hari

# auto_error=False -> endpoint publik tetap bisa dipanggil tanpa token
bearer_scheme = HTTPBearer(auto_error=False)


def _admin_emails() -> set[str]:
    raw = os.getenv("ADMIN_EMAILS", "")
    return {email.strip().lower() for email in raw.split(",") if email.strip()}


def is_admin_email(email: str | None) -> bool:
    return bool(email) and email.lower() in _admin_emails()


def create_access_token(user: models.User) -> str:
    now = datetime.datetime.now(datetime.timezone.utc)
    payload = {
        "sub": user.email,
        "email": user.email,
        "name": user.fullname,
        "role": user.role,
        "iat": now,
        "exp": now + datetime.timedelta(hours=JWT_EXPIRE_HOURS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_access_token(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Sesi login sudah kadaluwarsa, silakan login ulang.",
        )
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token login tidak valid.",
        )


def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
    db: Session = Depends(database.get_db),
) -> models.User:
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Butuh login (header Authorization: Bearer <token>).",
        )

    payload = decode_access_token(credentials.credentials)
    email = payload.get("sub")
    user = db.query(models.User).filter(models.User.email == email).first()

    if not user:
        raise HTTPException(status_code=401, detail="User tidak ditemukan.")
    if not user.is_active:
        raise HTTPException(status_code=403, detail="Akun ini sedang dinonaktifkan.")
    return user


def get_optional_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
    db: Session = Depends(database.get_db),
) -> models.User | None:
    """Seperti get_current_user, tapi tidak mewajibkan login.

    Dipakai endpoint publik yang ingin tetap tahu siapa user-nya kalau token
    dikirim (mis. pendaftaran Web Push).
    """
    if credentials is None:
        return None

    try:
        payload = decode_access_token(credentials.credentials)
    except HTTPException:
        return None

    email = payload.get("sub")
    if not email:
        return None
    return db.query(models.User).filter(models.User.email == email).first()


def require_admin(user: models.User = Depends(get_current_user)) -> models.User:
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="Akses khusus admin.")
    return user
