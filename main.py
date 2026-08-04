import ssl
import json
import asyncio
from typing import List
import paho.mqtt.client as mqtt
from fastapi import FastAPI, Depends, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
from pydantic import BaseModel
from sqlalchemy.orm import Session
import models
from database import engine, get_db, SessionLocal

# --- SETUP DATABASE ---
models.Base.metadata.create_all(bind=engine)

# --- FLAG KONTROL OVERRIDE ---
IS_OVERRIDDEN = False
FORCED_STATUS = "Aman"

# --- WEBSOCKET CONNECTION MANAGER ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print(f"[WS] Client terhubung. Total client aktif: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            print(f"[WS] Client terputus. Total client tersisa: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        """Kirim data telemetry ke seluruh Frontend yang sedang buka web"""
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                print(f"[WS ERROR] Gagal kirim pesan ke client: {e}")

manager = ConnectionManager()
main_loop = None  # Event loop untuk jembatan Asyncio & MQTT Thread

# --- FUNGSI HELPER DATABASE & MQTT ---
def update_status_in_db(new_status):
    db = SessionLocal()
    try:
        status_entry = db.query(models.SystemStatus).filter(models.SystemStatus.id == 1).first()
        if not status_entry:
            status_entry = models.SystemStatus(id=1, status=new_status)
            db.add(status_entry)
        else:
            status_entry.status = new_status
        db.commit()
    except Exception as e:
        print(f"[DB ERROR] Gagal update status: {e}")
    finally:
        db.close()

def on_message(client, userdata, msg):
    global IS_OVERRIDDEN, FORCED_STATUS, main_loop
    
    try:
        payload = json.loads(msg.payload.decode())
        raw_status = payload.get("status", "AMAN")
        vibration_val = payload.get("nilai_mm_s", 0.0)
        humidity_val = payload.get("kelembapan_pct", payload.get("kelembapan", 0.0))

        # Mapping status
        if raw_status in ["BAHAYA", "Warning"]:
            mapped_status = "Warning"
        elif raw_status in ["SIAGA", "Siaga"]:
            mapped_status = "Siaga"
        else:
            mapped_status = "Aman"

        # Jika sedang di-override Admin, paksakan status sesuai kunci Admin
        final_status = FORCED_STATUS if IS_OVERRIDDEN else mapped_status

        # Update DB jika tidak di-override
        if not IS_OVERRIDDEN:
            update_status_in_db(final_status)

        # Buat payload data yang siap dibroadcast ke Frontend WebSocket
        ws_payload = {
            "vibration": round(float(vibration_val), 3),
            "humidity": round(float(humidity_val), 1),
            "status": final_status,
            "is_overridden": IS_OVERRIDDEN,
            "timestamp": payload.get("timestamp")
        }

        # Broadcast data ke seluruh client WebSocket melalui event loop
        if main_loop and main_loop.is_running():
            asyncio.run_coroutine_threadsafe(
                manager.broadcast(ws_payload), main_loop
            )

        print(f"[MQTT->WS] Vibration: {vibration_val} mm/s | Status: {final_status}")

    except Exception as e:
        print(f"[MQTT ERROR] Gagal dekode payload: {e}")

# --- KONEKSI HIVEMQ CLOUD ---
MQTT_BROKER = "130e9cfdf74f4c538f0dc340a76fac23.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_USER = "gemastik"
MQTT_PASSWORD = "12345678"

mqtt_client = mqtt.Client()
mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
mqtt_client.tls_set(cert_reqs=ssl.CERT_NONE)
mqtt_client.tls_insecure_set(True)
mqtt_client.on_message = on_message

try:
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.subscribe("sensor/gemastik/data")
    mqtt_client.loop_start()
    print("[MQTT] Berhasil terhubung ke HiveMQ Cloud!")
except Exception as e:
    print(f"[MQTT ERROR] Gagal konek ke HiveMQ Cloud: {e}")

# --- APP SETUP ---
app = FastAPI()

# Menyimpan event loop saat FastAPI baru menyala
@app.on_event("startup")
async def startup_event():
    global main_loop
    main_loop = asyncio.get_running_loop()

app.add_middleware(
    CORSMiddleware, 
    allow_origins=["*"], 
    allow_credentials=True, 
    allow_methods=["*"], 
    allow_headers=["*"]
)

GOOGLE_CLIENT_ID = "154325619553-16skq2jomkno70n87nnkpptkgipakq9f.apps.googleusercontent.com"

# --- WEBSOCKET ENDPOINT ---
@app.websocket("/ws/telemetry")
async def websocket_telemetry(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Jaga koneksi tetap terbuka menerima ping/pong
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        manager.disconnect(websocket)

# --- REST API ENDPOINTS ---
class GoogleAuthRequest(BaseModel):
    token: str

@app.post("/api/auth/google")
async def auth_google(data: GoogleAuthRequest, db: Session = Depends(get_db)):
    try:
        id_info = id_token.verify_oauth2_token(data.token, google_requests.Request(), GOOGLE_CLIENT_ID)
        email_user = id_info.get("email")
        nama_user = id_info.get("name")
        
        user_in_db = db.query(models.User).filter(models.User.email == email_user).first()
        
        ADMIN_MAPS = {
            "salsabilawiryawan7@gmail.com": "@chocomunn",
            "s4yed.sult4n@gmail.com": "@username_tele_1",
            "reyhanfachrurozzi7@gmail.com": "@ryhnfch"
        }
        
        if not user_in_db and email_user in ADMIN_MAPS:
            user_in_db = models.User(email=email_user, fullname=nama_user, telegram=ADMIN_MAPS[email_user], role="admin")
            db.add(user_in_db)
            db.commit()
            db.refresh(user_in_db)
        
        return {
            "is_new_user": user_in_db is None,
            "is_admin": user_in_db.role == "admin" if user_in_db else False,
            "email": email_user,
            "name": nama_user
        }
    except ValueError:
        return {"success": False, "error": "Invalid token"}

@app.get("/api/status")
async def get_status(db: Session = Depends(get_db)):
    status_entry = db.query(models.SystemStatus).filter(models.SystemStatus.id == 1).first()
    return {
        "status": status_entry.status if status_entry else "Normal",
        "lastUpdated": status_entry.last_updated.strftime("%Y-%m-%d %H:%M:%S") if status_entry else "N/A"
    }

@app.post("/api/admin/override")
async def override_status(status: str, db: Session = Depends(get_db)):
    """Endpoint khusus Admin untuk mengunci & memaksa ubah status EWS"""
    global IS_OVERRIDDEN, FORCED_STATUS
    IS_OVERRIDDEN = True
    FORCED_STATUS = status
    
    status_entry = db.query(models.SystemStatus).filter(models.SystemStatus.id == 1).first()
    if not status_entry:
        status_entry = models.SystemStatus(id=1, status=status)
        db.add(status_entry)
    else:
        status_entry.status = status
    db.commit()

    # Kirim perintah override ke MQTT untuk hardware
    command_payload = json.dumps({
        "override_active": True,
        "forced_status": status
    })
    mqtt_client.publish("sensor/gemastik/command", command_payload)

    return {
        "success": True, 
        "message": f"Status berhasil di-override menjadi {status}."
    }

@app.post("/api/admin/reset-override")
async def reset_override():
    """Endpoint untuk melepas kuncian Admin dan kembali ke mode otomatis"""
    global IS_OVERRIDDEN
    IS_OVERRIDDEN = False
    return {"success": True, "message": "Sistem kembali ke pemantauan otomatis."}

class UserRegisterRequest(BaseModel):
    email: str
    fullname: str
    telegram: str

@app.post("/api/auth/google/register")
async def register_user(data: UserRegisterRequest, db: Session = Depends(get_db)):
    new_user = models.User(email=data.email, fullname=data.fullname, telegram=data.telegram, role="user")
    db.add(new_user)
    db.commit()
    return {"success": True}