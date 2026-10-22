import datetime

from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String, Text

from database import Base


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
