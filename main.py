import time
import json
import ssl
import asyncio
import base64
from pydantic import BaseModel
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import socketio
import paho.mqtt.client as mqtt

# ---------------------------------------------------------
# 1. INISIALISASI FASTAPI & SOCKET.IO
# ---------------------------------------------------------
fastapi_app = FastAPI(title="GeoSense Backend API")

fastapi_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

sio = socketio.AsyncServer(async_mode='asgi', cors_allowed_origins='*')
loop = None
mqtt_client = None


# ---------------------------------------------------------
# 2. INTEGRASI HIVEMQ CLOUD (MQTT CLIENT)
# ---------------------------------------------------------
MQTT_BROKER = "3cd0f777a8ec4a20af722dd1214f7eb3.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_TOPIC = "geosense/data"

# Menyesuaikan Signature Callback Paho-MQTT v2
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("✅ FastAPI terhubung ke HiveMQ Cloud Broker!")
        client.subscribe(MQTT_TOPIC)
        print(f"📥 FastAPI Subscribed ke topik: '{MQTT_TOPIC}'")
    else:
        print(f"⚠️ Gagal koneksi MQTT, response code: {rc}")

def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode('utf-8')
        data_payload = json.loads(payload_str)
        
        # Kirim data secara async ke Socket.IO (Frontend React)
        if loop and loop.is_running():
            asyncio.run_coroutine_threadsafe(
                sio.emit('geosense_update', data_payload), loop
            )
    except Exception as e:
        print("Gagal membaca data JSON MQTT di Backend:", e)

def setup_mqtt():
    global mqtt_client
    mqtt_client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2, 
        client_id="fastapi_geosense_backend"
    )
    mqtt_client.username_pw_set("Kelom2", "TCPRule1")
    mqtt_client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    
    try:
        mqtt_client.connect_async(MQTT_BROKER, MQTT_PORT, keepalive=60)
        mqtt_client.loop_start()
    except Exception as e:
        print("Gagal inisialisasi koneksi MQTT:", e)

# ---------------------------------------------------------
# LIFECYCLE EVENTS
# ---------------------------------------------------------
@fastapi_app.on_event("startup")
async def startup_event():
    global loop
    loop = asyncio.get_event_loop()
    setup_mqtt()

@fastapi_app.on_event("shutdown")
async def shutdown_event():
    global mqtt_client
    if mqtt_client:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        print("🔌 Koneksi MQTT Backend dilepas.")

# ---------------------------------------------------------
# 3. ROUTE API & SOCKET EVENTS
# ---------------------------------------------------------
@sio.event
async def connect(sid, environ):
    print(f"🔌 Client Frontend terhubung! (ID Socket: {sid})")

@sio.event
async def disconnect(sid):
    print(f"❌ Client Frontend terputus: {sid}")

@fastapi_app.get("/")
def root():
    return {"status": "Server Backend GeoSense FastAPI Aktif 🚀"}

@fastapi_app.post("/api/admin/override")
async def admin_override(status: str):
    print(f"⚠️ Admin Emergency Override dipicu ke status: {status}")
    
    override_payload = {
        "timestamp": int(time.time()),
        "kondisi_tanah": "Override Admin",
        "status": "WASPADA" if status == "Warning" else status.upper(),
        "aktivitas": "ADMIN OVERRIDE",
        "p2p_amplitude": 0,
        "rms": 0
    }
    
    await sio.emit('geosense_update', override_payload)
    return {"success": True, "message": f"Status diubah ke {status}"}

# =========================================================
# 🔑 GOOGLE AUTHENTICATION & USER ROLE MANAGEMENT
# =========================================================

ADMIN_EMAILS = [
    "salsabila@gmail.com",
    "admin@geosense.com"
]

class GoogleAuthRequest(BaseModel):
    credential: str

class GoogleRegisterRequest(BaseModel):
    email: str
    fullname: str
    telegram: str

def decode_google_jwt(token: str) -> dict:
    try:
        payload_b64 = token.split('.')[1]
        payload_b64 += '=' * (-len(payload_b64) % 4)
        payload_json = base64.b64decode(payload_b64).decode('utf-8')
        return json.loads(payload_json)
    except Exception as e:
        print("⚠️ Gagal decode JWT:", e)
        return {}

@fastapi_app.post("/api/auth/google")
async def google_auth(data: GoogleAuthRequest):
    user_info = decode_google_jwt(data.credential)
    user_email = user_info.get("email", "").lower()
    user_name = user_info.get("name", "User")
    
    if not user_email:
        return {"error": "Token Google tidak valid atau gagal didecode"}

    is_admin = any(user_email == admin_email.lower() for admin_email in ADMIN_EMAILS)
    assigned_role = "admin" if is_admin else "user"
    
    print(f"🔑 Auth Login: {user_email} -> Role: {assigned_role}")
    
    return {
        "success": True,
        "message": "Authentication berhasil",
        "email": user_email,
        "name": user_name,
        "is_admin": is_admin,
        "role": assigned_role,
        "is_new_user": False
    }

@fastapi_app.post("/api/auth/google/register")
async def google_register(data: GoogleRegisterRequest):
    print(f"📝 Registrasi User Baru: {data.fullname} ({data.email}) - Telegram: {data.telegram}")
    
    return {
        "success": True,
        "message": "Data registrasi berhasil disimpan"
    }

# ---------------------------------------------------------
# 4. BINDING FASTAPI + SOCKET.IO UNTUK UVICORN
# ---------------------------------------------------------
app = socketio.ASGIApp(sio, other_asgi_app=fastapi_app)