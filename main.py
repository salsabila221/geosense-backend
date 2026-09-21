import time
import json
import ssl
import asyncio
import base64
from typing import Optional
from pydantic import BaseModel
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import socketio
import paho.mqtt.client as mqtt
import random

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
        
        # Kirim data secara async ke Socket.IO
        if loop and loop.is_running():
            asyncio.run_coroutine_threadsafe(
                sio.emit('geosense_update', data_payload), loop
            )
    except Exception as e:
        print("Gagal membaca data JSON MQTT di Backend:", e)

def setup_mqtt():
    global mqtt_client
    # Gunakan Client ID dinamis seperti di Node.js agar tidak tumbukan saat restart
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
        print("Gagal inisialisasi koneksi MQTT:", e)

# ---------------------------------------------------------
# LIFECYCLE EVENTS
# ---------------------------------------------------------
@fastapi_app.on_event("startup")
async def startup_event():
    global loop
    loop = asyncio.get_running_loop() # Ambil loop yang sedang aktif
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

# Samakan dengan Express: menerima status melalui Query Parameter (?status=...)
@fastapi_app.post("/api/admin/override")
async def admin_override(status: str = Query(...)):
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

# ---------------------------------------------------------
# 4. BINDING FASTAPI + SOCKET.IO UNTUK UVICORN
# ---------------------------------------------------------
app = socketio.ASGIApp(sio, other_asgi_app=fastapi_app)