import asyncio
import datetime
import json
import os
import random
import re
import ssl
import sys
import time
from contextlib import asynccontextmanager
from typing import Optional

import paho.mqtt.client as mqtt
import socketio
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

load_dotenv()

# Windows kadang memakai code page cp1252 -> karakter emoji di print() bikin
# UnicodeEncodeError dan server gagal start ("Application startup failed").
# Paksa stdout/stderr ke UTF-8 supaya aman.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Import modul database & model
import auth
import database
import models
import notifier

# Web Push bersifat opsional: kalau pywebpush belum terpasang / VAPID belum
# dikonfigurasi, backend tetap jalan normal (WhatsApp & dashboard tidak terganggu).
try:
    import push_notifier
except Exception as _push_exc:  # noqa: BLE001
    push_notifier = None
    print("Web Push tidak aktif:", _push_exc)

# Otomatis buat tabel database jika belum ada + tambah kolom baru (migrasi ringan)
# Dibungkus try/except supaya server tetap hidup dan error DB terlihat jelas di /health.
DB_INIT_ERROR: str | None = None
try:
    models.Base.metadata.create_all(bind=database.engine)
    database.ensure_schema()
except Exception as exc:  # mis. password Supabase salah / host tidak bisa dijangkau
    DB_INIT_ERROR = f"{type(exc).__name__}: {exc}"
    print("=" * 70)
    print("❌ GAGAL INISIALISASI DATABASE")
    print(DB_INIT_ERROR)
    print("Cek DATABASE_URL di .env (password Supabase, host pooler, sslmode).")
    print("Server tetap dijalankan, tapi endpoint yang butuh DB akan error.")
    print("=" * 70)

# ---------------------------------------------------------
# KONFIGURASI CONSTANT
# ---------------------------------------------------------
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "").strip()
CORS_ORIGINS = [
    origin.strip()
    for origin in os.getenv("CORS_ORIGINS", "http://localhost:5173,http://localhost:4173").split(",")
    if origin.strip()
]
# Saat dev, Vite kadang pindah ke 5174/5175 dst. Regex ini bikin origin localhost
# (port apa pun) tetap diizinkan tanpa perlu edit .env terus.
CORS_ORIGIN_REGEX = os.getenv("CORS_ORIGIN_REGEX", r"http://(localhost|127\.0\.0\.1)(:\d+)?")

MQTT_BROKER = os.getenv("MQTT_BROKER", "3cd0f777a8ec4a20af722dd1214f7eb3.s1.eu.hivemq.cloud")
MQTT_PORT = int(os.getenv("MQTT_PORT", "8883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "geosense/data")
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "Kelom2")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "TCPRule1")

# Global State Variables
loop = None
mqtt_client = None


# ---------------------------------------------------------
# 1. LIFESPAN HANDLER
# ---------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global loop
    loop = asyncio.get_running_loop()
    seed_status_history()
    setup_mqtt()
    print("🚀 Backend GeoSense & MQTT Broker siap berjalan!")
    print(f"🗄️  Database: {database.database_info()['dialect']} -> {database.database_info()['target']}")

    yield

    global mqtt_client
    if mqtt_client:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        print("🔌 Koneksi MQTT Backend dilepas.")


# ---------------------------------------------------------
# 2. INISIALISASI FASTAPI & SOCKET.IO
# ---------------------------------------------------------
fastapi_app = FastAPI(
    title="GeoSense Backend API",
    version="1.1.0",
    lifespan=lifespan,
)

fastapi_app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_origin_regex=CORS_ORIGIN_REGEX or None,
    # Kita pakai Bearer token (bukan cookie), jadi credentials tidak perlu.
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")


# ---------------------------------------------------------
# 3. INTEGRASI HIVEMQ CLOUD (MQTT SUBSCRIBER)
# ---------------------------------------------------------
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("✅ FastAPI terhubung ke HiveMQ Cloud Broker!")
        client.subscribe(MQTT_TOPIC)
        print(f"📥 FastAPI Subscribed ke topik ML: '{MQTT_TOPIC}'")
    else:
        print(f"⚠️ Gagal koneksi MQTT, response code: {rc}")


def normalize_status(value) -> str:
    return str(value or "").strip().upper()


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def record_status_change(
    db,
    *,
    status: str,
    source: str,
    changed_by: str | None = None,
    kondisi_tanah=None,
    aktivitas=None,
    p2p_amplitude=None,
    rms=None,
    note=None,
):
    """Simpan satu baris riwayat. Tidak pernah update baris lama."""
    db.add(
        models.StatusHistory(
            status=normalize_status(status) or "UNKNOWN",
            source=source,
            changed_by=changed_by,
            kondisi_tanah=kondisi_tanah,
            aktivitas=aktivitas,
            p2p_amplitude=to_float(p2p_amplitude),
            rms=to_float(rms),
            note=note,
            created_at=datetime.datetime.utcnow(),
        )
    )


def seed_status_history():
    """Kalau riwayat masih kosong, catat status terkini sebagai titik awal."""
    db = database.SessionLocal()
    try:
        if db.query(models.StatusHistory).count() > 0:
            return
        current = db.query(models.SystemStatus).filter(models.SystemStatus.id == 1).first()
        if current:
            db.add(
                models.StatusHistory(
                    status=normalize_status(current.status),
                    source="system",
                    note="Data awal sebelum riwayat perubahan diaktifkan",
                    created_at=current.last_updated or datetime.datetime.utcnow(),
                )
            )
            db.commit()
    except Exception as exc:
        print("⚠️ Gagal menyiapkan riwayat status:", exc)
        db.rollback()
    finally:
        db.close()


def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode("utf-8")
        data_payload = json.loads(payload_str)
        if not isinstance(data_payload, dict):
            raise ValueError("Payload MQTT harus berupa objek JSON")

        # 1. Update status terkini + catat ke riwayat kalau statusnya berubah
        status_val = normalize_status(data_payload.get("status", "AMAN")) or "AMAN"
        status_changed = False
        db = database.SessionLocal()
        try:
            sys_status = db.query(models.SystemStatus).filter(models.SystemStatus.id == 1).first()
            previous_status = normalize_status(sys_status.status) if sys_status else None

            if not sys_status:
                sys_status = models.SystemStatus(
                    id=1,
                    status=status_val,
                    last_updated=datetime.datetime.utcnow(),
                )
                db.add(sys_status)
            else:
                sys_status.status = status_val
                # Telemetri tetap masuk walau status tidak berubah. Tanpa ini,
                # dashboard bisa menampilkan "last updated" yang sudah basi.
                sys_status.last_updated = datetime.datetime.utcnow()

            if previous_status != status_val:
                status_changed = True
                record_status_change(
                    db,
                    status=status_val,
                    source="ml",
                    kondisi_tanah=data_payload.get("kondisi_tanah"),
                    aktivitas=data_payload.get("aktivitas"),
                    p2p_amplitude=data_payload.get("p2p_amplitude"),
                    rms=data_payload.get("rms"),
                    note="Perubahan otomatis dari model ML",
                )

            db.commit()
        except Exception as db_err:
            print("❌ DB Update Error:", db_err)
            db.rollback()
            status_changed = False
        finally:
            db.close()

        # 2. Broadcast notifikasi (WhatsApp + Web Push) kalau status berubah
        #    ke SIAGA/WASPADA. Keduanya jalan di background thread.
        if status_changed:
            notifier.notify_status_change(
                status_val,
                source="ml",
                kondisi_tanah=data_payload.get("kondisi_tanah"),
                aktivitas=data_payload.get("aktivitas"),
                p2p_amplitude=data_payload.get("p2p_amplitude"),
                rms=data_payload.get("rms"),
            )
            if push_notifier:
                push_notifier.notify_status_change(
                    status_val,
                    source="ml",
                    kondisi_tanah=data_payload.get("kondisi_tanah"),
                    aktivitas=data_payload.get("aktivitas"),
                    p2p_amplitude=data_payload.get("p2p_amplitude"),
                    rms=data_payload.get("rms"),
                )

        # 3. Kirim data real-time ke Frontend via Socket.IO
        if loop and loop.is_running():
            asyncio.run_coroutine_threadsafe(sio.emit("geosense_update", data_payload), loop)
    except Exception as e:
        print("⚠️ Gagal membaca data JSON MQTT di Backend:", e)


def setup_mqtt():
    global mqtt_client
    random_id = hex(random.getrandbits(24))[2:]
    mqtt_client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"fastapi_geosense_{random_id}",
    )
    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    mqtt_client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    try:
        mqtt_client.connect_async(MQTT_BROKER, MQTT_PORT, keepalive=60)
        mqtt_client.loop_start()
    except Exception as e:
        print("❌ Gagal inisialisasi koneksi MQTT:", e)


# ---------------------------------------------------------
# 4. SCHEMA REQUEST
# ---------------------------------------------------------
class GoogleAuthRequest(BaseModel):
    # Frontend @react-oauth/google mengirim { credential: "<id_token>" }
    # Field "token" tetap diterima supaya kompatibel dengan kode lama.
    credential: Optional[str] = None
    token: Optional[str] = None

    def id_token_value(self) -> str:
        value = (self.credential or self.token or "").strip()
        if not value:
            raise HTTPException(
                status_code=422,
                detail="Field 'credential' (Google ID token) wajib diisi.",
            )
        return value


class GoogleRegisterRequest(BaseModel):
    credential: Optional[str] = None
    token: Optional[str] = None
    fullname: str = Field(min_length=2, max_length=120)
    phone_number: Optional[str] = None
    telegram: Optional[str] = None  # alias lama dari frontend

    def id_token_value(self) -> str:
        value = (self.credential or self.token or "").strip()
        if not value:
            raise HTTPException(
                status_code=422,
                detail="Field 'credential' (Google ID token) wajib diisi.",
            )
        return value

    def contact_value(self) -> Optional[str]:
        return (self.phone_number or self.telegram or "").strip() or None


class PushSubscribeRequest(BaseModel):
    endpoint: str = Field(min_length=10, max_length=4096)
    keys: dict[str, str] = Field(default_factory=dict)
    user_agent: Optional[str] = Field(default=None, max_length=512)


class PushUnsubscribeRequest(BaseModel):
    endpoint: str = Field(min_length=10, max_length=4096)


# Nomor WhatsApp Indonesia: +628xxxxxxxxx (setelah dinormalisasi)
PHONE_PATTERN = re.compile(r"^\+628[1-9]\d{6,11}$")


def normalize_phone(raw: str) -> str:
    """08xx / 8xx / +628xx / 628xx -> +628xx (format konsisten buat WhatsApp)."""
    digits = re.sub(r"\D", "", raw or "")
    if digits.startswith("0"):
        digits = "62" + digits[1:]
    elif digits.startswith("8"):
        digits = "62" + digits
    return f"+{digits}" if digits else ""


def validate_phone(raw: str | None) -> str:
    value = (raw or "").strip()
    if not value:
        raise HTTPException(status_code=400, detail="Nomor WhatsApp wajib diisi.")
    phone = normalize_phone(value)
    if not PHONE_PATTERN.match(phone):
        raise HTTPException(
            status_code=400,
            detail="Format nomor WhatsApp tidak valid. Contoh: 08123456789 atau +628123456789.",
        )
    return phone


def verify_google_token(raw_token: str) -> dict:
    """Verifikasi ID token Google dan kembalikan payload-nya."""
    if not GOOGLE_CLIENT_ID:
        raise HTTPException(
            status_code=500,
            detail="GOOGLE_CLIENT_ID belum diset di .env backend.",
        )
    try:
        return id_token.verify_oauth2_token(raw_token, google_requests.Request(), GOOGLE_CLIENT_ID)
    except ValueError as exc:
        # Pesan asli Google sangat membantu saat debug (audience mismatch, expired, dll.)
        raise HTTPException(status_code=401, detail=f"Token Google tidak valid: {exc}")


def user_payload(user: models.User) -> dict:
    return {
        **user.to_public_dict(),
        "is_admin": user.role == "admin",
    }


# ---------------------------------------------------------
# 5. API ENDPOINTS
# ---------------------------------------------------------
@fastapi_app.get("/")
def root():
    return {"status": "Server Backend GeoSense FastAPI Aktif 🚀"}


@fastapi_app.get("/health")
def health(db: Session = Depends(database.get_db)):
    db_ok = False
    db_error = DB_INIT_ERROR
    try:
        db.execute(models.User.__table__.select().limit(1))
        db_ok = True
        db_error = None
    except Exception as exc:
        db_error = db_error or f"{type(exc).__name__}: {exc}"
    return {
        "status": "ok" if db_ok else "degraded",
        "database": {**database.database_info(), "connected": db_ok, "error": db_error},
    }


# ---- Login Google ----
@fastapi_app.post("/api/auth/google")
async def google_auth(
    auth_data: GoogleAuthRequest,
    db: Session = Depends(database.get_db),
):
    id_info = verify_google_token(auth_data.id_token_value())

    email = (id_info.get("email") or "").lower()
    if not email:
        raise HTTPException(status_code=400, detail="Token tidak valid (email tidak ditemukan).")

    if id_info.get("email_verified") is False:
        raise HTTPException(status_code=401, detail="Email Google belum terverifikasi.")

    fullname = id_info.get("name") or email.split("@")[0]
    picture = id_info.get("picture")
    google_sub = id_info.get("sub")

    user = db.query(models.User).filter(models.User.email == email).first()

    if user and not user.is_active:
        raise HTTPException(status_code=403, detail="Akun ini sedang dinonaktifkan.")

    # User baru ATAU user lama yang belum punya nomor WhatsApp -> onboarding.
    if not user or not (user.phone_number or "").strip():
        return {
            "success": True,
            "is_new_user": True,
            "email": email,
            "name": fullname,
            "picture": picture,
        }

    # Sinkronkan data terbaru dari Google + promosikan admin via ADMIN_EMAILS
    user.google_sub = google_sub or user.google_sub
    user.picture = picture or user.picture
    user.fullname = user.fullname or fullname
    if auth.is_admin_email(email):
        user.role = "admin"
    user.last_login = datetime.datetime.utcnow()
    db.commit()
    db.refresh(user)

    return {
        "success": True,
        "is_new_user": False,
        "email": user.email,
        "name": user.fullname,
        "is_admin": user.role == "admin",
        "role": user.role,
        "picture": user.picture,
        "access_token": auth.create_access_token(user),
        "token_type": "bearer",
        "user": user_payload(user),
    }


# ---- Onboarding user baru (simpan ke database) ----
@fastapi_app.post("/api/auth/google/register")
async def google_register(
    data: GoogleRegisterRequest,
    db: Session = Depends(database.get_db),
):
    # Email diambil dari token Google, bukan dari body (biar tidak bisa dipalsukan).
    id_info = verify_google_token(data.id_token_value())
    email = (id_info.get("email") or "").lower()
    if not email:
        raise HTTPException(status_code=400, detail="Token tidak valid (email tidak ditemukan).")
    if id_info.get("email_verified") is False:
        raise HTTPException(status_code=401, detail="Email Google belum terverifikasi.")

    fullname = (data.fullname or "").strip()
    if len(fullname) < 2:
        raise HTTPException(status_code=400, detail="Nama lengkap wajib diisi (minimal 2 karakter).")

    phone_number = validate_phone(data.contact_value())

    user = db.query(models.User).filter(models.User.email == email).first()

    if user:
        # Sudah terdaftar: lengkapi data kalau kosong, lalu login.
        if not user.is_active:
            raise HTTPException(status_code=403, detail="Akun ini sedang dinonaktifkan.")
        user.fullname = fullname or user.fullname
        user.phone_number = phone_number
        if auth.is_admin_email(email):
            user.role = "admin"
        user.last_login = datetime.datetime.utcnow()
        db.commit()
        db.refresh(user)
        return {
            "success": True,
            "message": "User sudah terdaftar, data diperbarui.",
            "access_token": auth.create_access_token(user),
            "token_type": "bearer",
            "user": user_payload(user),
        }

    new_user = models.User(
        email=email,
        fullname=fullname,
        phone_number=phone_number,
        google_sub=id_info.get("sub"),
        picture=id_info.get("picture"),
        role="admin" if auth.is_admin_email(email) else "user",
        is_active=True,
        created_at=datetime.datetime.utcnow(),
        last_login=datetime.datetime.utcnow(),
    )

    try:
        db.add(new_user)
        db.commit()
        db.refresh(new_user)
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Gagal menyimpan data user: {e}")

    return {
        "success": True,
        "message": "Pendaftaran berhasil",
        "access_token": auth.create_access_token(new_user),
        "token_type": "bearer",
        "user": user_payload(new_user),
    }


# ---- Profil user yang sedang login ----
@fastapi_app.get("/api/auth/me")
def get_me(current_user: models.User = Depends(auth.get_current_user)):
    return {"success": True, "user": user_payload(current_user)}


# ---- Daftar user (admin only) ----
@fastapi_app.get("/api/admin/users")
def list_users(
    _: models.User = Depends(auth.require_admin),
    db: Session = Depends(database.get_db),
):
    users = db.query(models.User).order_by(models.User.created_at.desc()).all()
    return {"success": True, "total": len(users), "users": [u.to_public_dict() for u in users]}


# ---- Status sistem ----
@fastapi_app.get("/api/system/status")
def system_status(db: Session = Depends(database.get_db)):
    sys_status = db.query(models.SystemStatus).filter(models.SystemStatus.id == 1).first()
    last_updated = sys_status.last_updated if sys_status else None
    if last_updated and last_updated.tzinfo is None:
        last_updated = last_updated.replace(tzinfo=datetime.timezone.utc)
    return {
        "status": sys_status.status if sys_status else "Normal",
        "last_updated": last_updated.isoformat() if last_updated else None,
    }


@fastapi_app.get("/api/system/history")
def system_status_history(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    source: Optional[str] = Query(None, description="Filter: ml | admin | system"),
    _: models.User = Depends(auth.require_admin),
    db: Session = Depends(database.get_db),
):
    """Riwayat perubahan status, terbaru dulu. Khusus admin."""
    query = db.query(models.StatusHistory)
    if source:
        query = query.filter(models.StatusHistory.source == source)

    total = query.count()
    rows = (
        query.order_by(models.StatusHistory.created_at.desc(), models.StatusHistory.id.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )
    return {
        "success": True,
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": [row.to_public_dict() for row in rows],
    }


# ---- WhatsApp / Fonnte (admin) ----
@fastapi_app.get("/api/admin/wa/config")
def wa_config(
    _: models.User = Depends(auth.require_admin),
    db: Session = Depends(database.get_db),
):
    recipients = (
        db.query(models.User)
        .filter(models.User.is_active.is_(True), models.User.phone_number.isnot(None))
        .count()
    )
    return {
        "success": True,
        "enabled": notifier.enabled(),
        "token_configured": bool(notifier.FONNTE_TOKEN),
        "notify_statuses": sorted(notifier.NOTIFY_STATUSES),
        "cooldown_seconds": notifier.COOLDOWN_SECONDS,
        "recipients": recipients,
        "app_url": notifier.APP_URL,
    }


@fastapi_app.post("/api/admin/wa/test")
def wa_test(
    phone: str = Query(..., description="Nomor tujuan, contoh: 08123456789"),
    _: models.User = Depends(auth.require_admin),
):
    ok, response = notifier.send_test(phone)
    if not ok:
        raise HTTPException(status_code=502, detail=f"Fonnte gagal mengirim: {response}")
    return {"success": True, "response": response}


@fastapi_app.get("/api/admin/wa/logs")
def wa_logs(
    limit: int = Query(20, ge=1, le=200),
    _: models.User = Depends(auth.require_admin),
    db: Session = Depends(database.get_db),
):
    rows = (
        db.query(models.NotificationLog)
        .order_by(models.NotificationLog.id.desc())
        .limit(limit)
        .all()
    )
    return {"success": True, "items": [row.to_public_dict() for row in rows]}


# ---- Web Push / PWA (notifikasi browser) ----
@fastapi_app.get("/api/push/public-key")
def push_public_key():
    """Kunci publik VAPID untuk PushManager.subscribe() di frontend."""
    if not push_notifier:
        return {
            "success": False,
            "enabled": False,
            "publicKey": None,
            "detail": "Modul Web Push belum terpasang di server.",
        }
    return {
        "success": True,
        "enabled": push_notifier.enabled(),
        "publicKey": push_notifier.VAPID_PUBLIC_KEY or None,
    }


@fastapi_app.post("/api/push/subscribe")
def push_subscribe(
    payload: PushSubscribeRequest,
    current_user: Optional[models.User] = Depends(auth.get_optional_user),
    db: Session = Depends(database.get_db),
):
    """Simpan / perbarui langganan push dari satu browser/perangkat."""
    if not push_notifier:
        raise HTTPException(status_code=503, detail="Web Push belum tersedia di server.")

    endpoint = payload.endpoint.strip()
    keys = payload.keys or {}
    p256dh = str(keys.get("p256dh") or "").strip()
    auth_key = str(keys.get("auth") or "").strip()

    if not endpoint.startswith(("https://", "http://")):
        raise HTTPException(status_code=400, detail="Endpoint push tidak valid.")
    if not p256dh or not auth_key:
        raise HTTPException(status_code=400, detail="Kunci langganan push tidak lengkap.")

    user_agent = (payload.user_agent or "")[:255] or None
    now = datetime.datetime.utcnow()

    existing = (
        db.query(models.PushSubscription)
        .filter(models.PushSubscription.endpoint == endpoint)
        .first()
    )
    if existing:
        existing.p256dh = p256dh
        existing.auth = auth_key
        existing.last_seen = now
        existing.user_agent = user_agent or existing.user_agent
        if current_user:
            existing.user_email = current_user.email
    else:
        db.add(
            models.PushSubscription(
                endpoint=endpoint,
                p256dh=p256dh,
                auth=auth_key,
                user_email=current_user.email if current_user else None,
                user_agent=user_agent,
                created_at=now,
                last_seen=now,
            )
        )

    try:
        db.commit()
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Gagal menyimpan langganan push: {exc}")

    total = db.query(models.PushSubscription).count()
    return {
        "success": True,
        "total": total,
        "enabled": push_notifier.enabled(),
        "user_email": current_user.email if current_user else None,
    }


@fastapi_app.post("/api/push/unsubscribe")
def push_unsubscribe(
    payload: PushUnsubscribeRequest,
    db: Session = Depends(database.get_db),
):
    """Hapus langganan push (dipanggil saat user menekan 'Matikan')."""
    endpoint = payload.endpoint.strip()
    deleted = (
        db.query(models.PushSubscription)
        .filter(models.PushSubscription.endpoint == endpoint)
        .delete()
    )
    db.commit()
    return {"success": True, "deleted": deleted}


@fastapi_app.get("/api/admin/push/stats")
def push_stats(
    _: models.User = Depends(auth.require_admin),
    db: Session = Depends(database.get_db),
):
    rows = (
        db.query(models.PushSubscription)
        .order_by(models.PushSubscription.created_at.desc())
        .all()
    )
    return {
        "success": True,
        "enabled": bool(push_notifier and push_notifier.enabled()),
        "total": len(rows),
        "last_sent_at": push_notifier.last_sent_at() if push_notifier else None,
        "items": [row.to_public_dict() for row in rows[:20]],
    }


@fastapi_app.post("/api/admin/push/test")
def push_test(_: models.User = Depends(auth.require_admin)):
    """Kirim notifikasi uji ke semua perangkat yang berlangganan."""
    if not push_notifier:
        raise HTTPException(status_code=503, detail="Web Push belum tersedia di server.")
    result = push_notifier.send_test()
    return {"success": True, **result}


# ---- Emergency Admin Override (admin only) ----
@fastapi_app.post("/api/admin/override")
async def admin_override(
    status: str = Query(...),
    admin: models.User = Depends(auth.require_admin),
    db: Session = Depends(database.get_db),
):
    print(f"⚠️ Admin Emergency Override oleh {admin.email} ke status: {status}")

    status_str = "WASPADA" if status.lower() == "warning" else normalize_status(status)

    sys_status = db.query(models.SystemStatus).filter(models.SystemStatus.id == 1).first()
    previous_status = normalize_status(sys_status.status) if sys_status else None

    if not sys_status:
        sys_status = models.SystemStatus(id=1, status=status_str)
        db.add(sys_status)
    else:
        sys_status.status = status_str

    record_status_change(
        db,
        status=status_str,
        source="admin",
        changed_by=admin.email,
        aktivitas="ADMIN OVERRIDE",
        note=(
            "Override manual admin"
            if previous_status != status_str
            else f"Override admin (status tetap {status_str})"
        ),
    )
    db.commit()

    # Notifikasi hanya kalau statusnya benar-benar berubah
    wa_result = {"queued": False, "reason": "status tidak berubah"}
    push_result = {"queued": False, "reason": "status tidak berubah"}
    if previous_status != status_str:
        wa_result = notifier.notify_status_change(
            status_str,
            source="admin",
            changed_by=admin.email,
            aktivitas="ADMIN OVERRIDE",
        )
        if push_notifier:
            push_result = push_notifier.notify_status_change(
                status_str,
                source="admin",
                changed_by=admin.email,
                aktivitas="ADMIN OVERRIDE",
            )

    override_payload = {
        "timestamp": int(time.time()),
        "kondisi_tanah": "Override Admin",
        "status": status_str,
        "aktivitas": "ADMIN OVERRIDE",
        "p2p_amplitude": 0,
        "rms": 0,
    }

    await sio.emit("geosense_update", override_payload)
    return {
        "success": True,
        "message": f"Status diubah ke {status_str}",
        "whatsapp": wa_result,
        "push": push_result,
    }


# ---------------------------------------------------------
# 6. BINDING FASTAPI + SOCKET.IO UNTUK UVICORN
# ---------------------------------------------------------
app = socketio.ASGIApp(sio, other_asgi_app=fastapi_app)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
