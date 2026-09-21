import time
import json
import ssl
import asyncio
import random
from contextlib import asynccontextmanager
from typing import Optional
from pydantic import BaseModel
from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
import socketio
import paho.mqtt.client as mqtt
from sqlalchemy.orm import Session
from google.oauth2 import id_token
from google.auth.transport import requests

# Import modul database & model SQLite lokal[cite: 5, 9]
import database
import models

# Otomatis buat tabel database jika belum ada[cite: 5, 9]
models.Base.metadata.create_all(bind=database.engine)

# ---------------------------------------------------------
# KONFIGURASI CONSTANT
# ---------------------------------------------------------
GOOGLE_CLIENT_ID = "154325619553-16skq2jomkno70n87nnkpptkgipakq9f.apps.googleusercontent.com"
MQTT_BROKER = "3cd0f777a8ec4a20af722dd1214f7eb3.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_TOPIC = "geosense/data"

# Global State Variables
loop = None
mqtt_client = None

# ---------------------------------------------------------
# 1. LIFESPAN HANDLER (MODERN REPLACEMENT FOR ON_EVENT)
# ---------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- STARTUP PHASE ---
    global loop
    loop = asyncio.get_running_loop()
    setup_mqtt()
    print("🚀 Backend GeoSense & MQTT Broker siap berjalan!")
    
    yield  # Aplikasi berjalan menerima request
    
    # --- SHUTDOWN PHASE ---
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
    lifespan=lifespan
)

fastapi_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

sio = socketio.AsyncServer(async_mode='asgi', cors_allowed_origins='*')

# ---------------------------------------------------------
# 3. INTEGRASI HIVEMQ CLOUD (MQTT SUBSCRIBER)[cite: 8]
# ---------------------------------------------------------
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("✅ FastAPI terhubung ke HiveMQ Cloud Broker!")
        client.subscribe(MQTT_TOPIC)
        print(f"📥 FastAPI Subscribed ke topik ML: '{MQTT_TOPIC}'")
    else:
        print(f"⚠️ Gagal koneksi MQTT, response code: {rc}")

def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode('utf-8')
        data_payload = json.loads(payload_str)
        
        # 1. Sync & Update status sistem terbaru di SQLite
        status_val = data_payload.get("status", "AMAN")
        db = database.SessionLocal()
        try:
            sys_status = db.query(models.SystemStatus).filter(models.SystemStatus.id == 1).first()
            if not sys_status:
                sys_status = models.SystemStatus(id=1, status=status_val)
                db.add(sys_status)
            else:
                sys_status.status = status_val
            db.commit()
        except Exception as db_err:
            print("❌ DB Update Error:", db_err)
            db.rollback()
        finally:
            db.close()

        # 2. Kirim data real-time ke Frontend via Socket.IO
        if loop and loop.is_running():
            asyncio.run_coroutine_threadsafe(
                sio.emit('geosense_update', data_payload), loop
            )
    except Exception as e:
        print("⚠️ Gagal membaca data JSON MQTT di Backend:", e)

def setup_mqtt():
    global mqtt_client
    random_id = hex(random.getrandbits(24))[2:]
    mqtt_client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2, 
        client_id=f"fastapi_geosense_{random_id}"
    )
    mqtt_client.username_pw_set("Kelom2", "TCPRule1")
    mqtt_client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    
    try:
        mqtt_client.connect_async(MQTT_BROKER, MQTT_PORT, keepalive=60)
        mqtt_client.loop_start()
    except Exception as e:
        print("❌ Gagal inisialisasi koneksi MQTT:", e)

# ---------------------------------------------------------
# 4. SCHEMA REQUEST & API ENDPOINTS
# ---------------------------------------------------------
class GoogleAuthRequest(BaseModel):
    credential: str  # Sesuaikan dengan payload dari @react-oauth/google

class GoogleRegisterRequest(BaseModel):
    email: str
    fullname: str
    telegram: str

@fastapi_app.get("/")
def root():
    return {"status": "Server Backend GeoSense FastAPI Aktif 🚀"}

# Endpoint Verifikasi Token Google OAuth
@fastapi_app.post("/api/auth/google")
async def google_auth(auth_data: GoogleAuthRequest, db: Session = Depends(database.get_db)):
    try:
        # Verifikasi token ke server Google
        id_info = id_token.verify_oauth2_token(
            auth_data.credential, 
            requests.Request(), 
            GOOGLE_CLIENT_ID
        )

        email = id_info.get("email")
        fullname = id_info.get("name")

        if not email:
            raise HTTPException(status_code=400, detail="Token tidak valid (Email tidak ditemukan)")

        # Cek ketersediaan user di SQLite database[cite: 9]
        user = db.query(models.User).filter(models.User.email == email).first()
        
        # Jika user belum pernah terdaftar, beri sinyal ke FE untuk Onboarding
        if not user:
            return {
                "is_new_user": True,
                "email": email,
                "name": fullname
            }

        # Jika user sudah terdaftar
        return {
            "is_new_user": False,
            "is_admin": user.role == "admin",
            "name": user.fullname
        }

    except ValueError:
        raise HTTPException(status_code=401, detail="Token Google tidak valid atau kadaluwarsa")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Terjadi kesalahan server: {str(e)}")

# Endpoint Pendaftaran User Baru dari Google Onboarding
@fastapi_app.post("/api/auth/google/register")
async def google_register(data: GoogleRegisterRequest, db: Session = Depends(database.get_db)):
    try:
        user_exist = db.query(models.User).filter(models.User.email == data.email).first()
        if user_exist:
            return {"success": True, "message": "User sudah terdaftar"}

        new_user = models.User(
            email=data.email,
            fullname=data.fullname,
            telegram=data.telegram,
            role="user",
            is_active=True
        )
        db.add(new_user)
        db.commit()
        return {"success": True, "message": "Pendaftaran berhasil"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Gagal menyimpan data user: {str(e)}")

# Emergency Admin Override Endpoint[cite: 8]
@fastapi_app.post("/api/admin/override")
async def admin_override(status: str = Query(...), db: Session = Depends(database.get_db)):
    print(f"⚠️ Admin Emergency Override dipicu ke status: {status}")
    
    status_str = "WASPADA" if status == "Warning" else status.upper()

    # Update status di DB SQLite
    sys_status = db.query(models.SystemStatus).filter(models.SystemStatus.id == 1).first()
    if not sys_status:
        sys_status = models.SystemStatus(id=1, status=status_str)
        db.add(sys_status)
    else:
        sys_status.status = status_str
    db.commit()

    override_payload = {
        "timestamp": int(time.time()),
        "kondisi_tanah": "Override Admin",
        "status": status_str,
        "aktivitas": "ADMIN OVERRIDE",
        "p2p_amplitude": 0,
        "rms": 0
    }
    
    await sio.emit('geosense_update', override_payload)
    return {"success": True, "message": f"Status diubah ke {status_str}"}

# ---------------------------------------------------------
# 5. BINDING FASTAPI + SOCKET.IO UNTUK UVICORN[cite: 8]
# ---------------------------------------------------------
app = socketio.ASGIApp(sio, other_asgi_app=fastapi_app)