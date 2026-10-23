import datetime

from sqlalchemy import JSON, Boolean, Column, DateTime, Float, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB

from database import Base

# JSONB di Supabase/Postgres, JSON biasa di SQLite (untuk testing lokal).
JSONPayload = JSON().with_variant(JSONB, "postgresql")


class User(Base):
    __tablename__ = "users"

    # Email tetap jadi primary key supaya kompatibel dengan tabel lama
    email = Column(String, primary_key=True, index=True)
    fullname = Column(String, nullable=True)
    phone_number = Column(String, nullable=True)

    # Data tambahan dari Google
    google_sub = Column(String, nullable=True, index=True)
    picture = Column(String, nullable=True)

    role = Column(String, default="user", nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)

    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    last_login = Column(DateTime, nullable=True)

    def to_public_dict(self) -> dict:
        return {
            "email": self.email,
            "fullname": self.fullname,
            "phone_number": self.phone_number,
            "picture": self.picture,
            "role": self.role,
            "is_active": self.is_active,
        }


class SystemStatus(Base):
    """Status sistem TERKINI (satu baris, id=1). Untuk riwayat lihat StatusHistory."""

    __tablename__ = "system_status"

    id = Column(Integer, primary_key=True, index=True, default=1)
    status = Column(String, default="Normal")
    last_updated = Column(
        DateTime,
        default=datetime.datetime.utcnow,
        onupdate=datetime.datetime.utcnow,
    )


class StatusHistory(Base):
    """
    Catatan SETIAP perubahan status (append-only, tidak pernah di-update).
    Sumber: 'ml' (MQTT), 'admin' (override), 'system' (migrasi/awal).
    """

    __tablename__ = "status_history"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    status = Column(String, nullable=False, index=True)
    source = Column(String, nullable=False, default="ml", index=True)  # ml | admin | system
    changed_by = Column(String, nullable=True)  # email admin, null kalau dari ML

    kondisi_tanah = Column(String, nullable=True)
    aktivitas = Column(String, nullable=True)
    p2p_amplitude = Column(Float, nullable=True)
    rms = Column(Float, nullable=True)
    note = Column(String, nullable=True)

    created_at = Column(DateTime, default=datetime.datetime.utcnow, index=True)

    def to_public_dict(self) -> dict:
        created = self.created_at
        if created and created.tzinfo is None:
            created = created.replace(tzinfo=datetime.timezone.utc)
        return {
            "id": self.id,
            "status": self.status,
            "source": self.source,
            "changed_by": self.changed_by,
            "kondisi_tanah": self.kondisi_tanah,
            "aktivitas": self.aktivitas,
            "p2p_amplitude": self.p2p_amplitude,
            "rms": self.rms,
            "note": self.note,
            "created_at": created.isoformat() if created else None,
        }


class PushSubscription(Base):
    """Langganan Web Push (PWA) per perangkat/browser."""

    __tablename__ = "push_subscriptions"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    endpoint = Column(Text, nullable=False, unique=True)
    p256dh = Column(Text, nullable=False)
    auth = Column(Text, nullable=False)

    user_email = Column(String(255), nullable=True, index=True)
    user_agent = Column(String(255), nullable=True)

    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    last_seen = Column(DateTime, default=datetime.datetime.utcnow)

    def to_public_dict(self) -> dict:
        created = self.created_at
        if created and created.tzinfo is None:
            created = created.replace(tzinfo=datetime.timezone.utc)
        return {
            "id": self.id,
            "user_email": self.user_email,
            "user_agent": self.user_agent,
            "created_at": created.isoformat() if created else None,
        }


class NotificationLog(Base):
    """Riwayat pengiriman WhatsApp (Fonnte) — buat audit & debugging."""

    __tablename__ = "notification_log"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    phone = Column(String, nullable=False, index=True)
    status = Column(String, nullable=False)  # SIAGA / WASPADA / TEST
    source = Column(String, nullable=False, default="ml")  # ml | admin
    success = Column(Boolean, default=False, nullable=False)
    detail = Column(Text, nullable=True)  # response mentah dari Fonnte
    created_at = Column(DateTime, default=datetime.datetime.utcnow, index=True)

    def to_public_dict(self) -> dict:
        created = self.created_at
        if created and created.tzinfo is None:
            created = created.replace(tzinfo=datetime.timezone.utc)
        return {
            "id": self.id,
            "phone": self.phone,
            "status": self.status,
            "source": self.source,
            "success": self.success,
            "detail": self.detail,
            "created_at": created.isoformat() if created else None,
        }


class MqttMessage(Base):
    """
    Arsip mentah SETIAP pesan MQTT (rawdata / gpsdata).

    Tabel ini sengaja menyimpan payload asli (text + JSON kalau valid) supaya
    tidak ada data yang hilang walaupun format dari device berubah.
    """

    __tablename__ = "mqtt_messages"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    topic = Column(String(255), nullable=False, index=True)
    message_type = Column(String(32), nullable=False, default="unknown", index=True)
    node_id = Column(String(64), nullable=True, index=True)

    received_at = Column(DateTime, default=datetime.datetime.utcnow, index=True)
    source_timestamp = Column(DateTime, nullable=True, index=True)

    payload_text = Column(Text, nullable=False)
    payload_json = Column(JSONPayload, nullable=True)
    error = Column(Text, nullable=True)

    def to_public_dict(self, include_payload: bool = True) -> dict:
        received = self.received_at
        if received and received.tzinfo is None:
            received = received.replace(tzinfo=datetime.timezone.utc)
        source = self.source_timestamp
        if source and source.tzinfo is None:
            source = source.replace(tzinfo=datetime.timezone.utc)
        data = {
            "id": self.id,
            "topic": self.topic,
            "message_type": self.message_type,
            "node_id": self.node_id,
            "received_at": received.isoformat() if received else None,
            "source_timestamp": source.isoformat() if source else None,
            "error": self.error,
        }
        if include_payload:
            data["payload"] = self.payload_json
            data["payload_text"] = self.payload_text
        return data


class NodeTelemetry(Base):
    """Satu baris per node per pesan MQTT (data geofon dan/atau GPS)."""

    __tablename__ = "node_telemetry"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    mqtt_message_id = Column(Integer, nullable=True, index=True)
    node_id = Column(String(64), nullable=False, index=True)
    topic = Column(String(255), nullable=False, index=True)
    message_type = Column(String(32), nullable=False, default="unknown", index=True)

    received_at = Column(DateTime, default=datetime.datetime.utcnow, index=True)
    source_timestamp = Column(DateTime, nullable=True, index=True)

    online = Column(Boolean, nullable=True)
    status = Column(String(32), nullable=True)
    aktivitas = Column(String(64), nullable=True)

    p2p = Column(Float, nullable=True)
    rms = Column(Float, nullable=True)
    peak = Column(Float, nullable=True)
    signal = Column(JSONPayload, nullable=True)

    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    altitude = Column(Float, nullable=True)
    satellites = Column(Integer, nullable=True)
    hdop = Column(Float, nullable=True)
    gps_fix = Column(Boolean, nullable=True)

    raw_data = Column(JSONPayload, nullable=True)

    def to_public_dict(self) -> dict:
        received = self.received_at
        if received and received.tzinfo is None:
            received = received.replace(tzinfo=datetime.timezone.utc)
        return {
            "id": self.id,
            "mqtt_message_id": self.mqtt_message_id,
            "node_id": self.node_id,
            "topic": self.topic,
            "message_type": self.message_type,
            "received_at": received.isoformat() if received else None,
            "online": self.online,
            "status": self.status,
            "aktivitas": self.aktivitas,
            "p2p": self.p2p,
            "rms": self.rms,
            "peak": self.peak,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "altitude": self.altitude,
            "satellites": self.satellites,
            "hdop": self.hdop,
            "gps_fix": self.gps_fix,
        }


class EventLocation(Base):
    """Hasil triangulasi/TDOA dan arah longsor dari pesan MQTT."""

    __tablename__ = "event_locations"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    mqtt_message_id = Column(Integer, nullable=True, index=True)
    received_at = Column(DateTime, default=datetime.datetime.utcnow, index=True)
    source_timestamp = Column(DateTime, nullable=True, index=True)

    x = Column(Float, nullable=True)
    y = Column(Float, nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    estimated_latitude = Column(Float, nullable=True)
    estimated_longitude = Column(Float, nullable=True)
    bearing_degrees = Column(Float, nullable=True)
    compass = Column(String(32), nullable=True)
    direction_source = Column(String(64), nullable=True)

    raw_data = Column(JSONPayload, nullable=True)

    def to_public_dict(self) -> dict:
        received = self.received_at
        if received and received.tzinfo is None:
            received = received.replace(tzinfo=datetime.timezone.utc)
        return {
            "id": self.id,
            "mqtt_message_id": self.mqtt_message_id,
            "received_at": received.isoformat() if received else None,
            "x": self.x,
            "y": self.y,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "estimated_latitude": self.estimated_latitude,
            "estimated_longitude": self.estimated_longitude,
            "bearing_degrees": self.bearing_degrees,
            "compass": self.compass,
            "direction_source": self.direction_source,
        }
