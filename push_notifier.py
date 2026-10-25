"""
Web Push (PWA) untuk GeoSense.

Mengirim notifikasi ke browser/perangkat yang sudah berlangganan lewat
Push API + VAPID. Dipakai berdampingan dengan WhatsApp (Fonnte):

    import push_notifier
    push_notifier.notify_status_change("WASPADA", source="ml")

Konfigurasi di .env:
    PUSH_ENABLED=1
    VAPID_PUBLIC_KEY=<base64url public key>
    VAPID_PRIVATE_KEY_PATH=vapid_private.pem
    VAPID_SUBJECT=mailto:admin@example.com
"""

import datetime
import json
import os
import sys
import threading
import time

from dotenv import load_dotenv
from pywebpush import WebPushException, webpush

import database
import models

load_dotenv()

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PUSH_ENABLED = os.getenv("PUSH_ENABLED", "1").strip() == "1"
VAPID_PUBLIC_KEY = os.getenv("VAPID_PUBLIC_KEY", "").strip()
VAPID_SUBJECT = os.getenv("VAPID_SUBJECT", "mailto:admin@geosense.local").strip()

_private_key_raw = os.getenv("VAPID_PRIVATE_KEY_PATH", "vapid_private.pem").strip()
VAPID_PRIVATE_KEY_PATH = (
    _private_key_raw
    if os.path.isabs(_private_key_raw)
    else os.path.join(BASE_DIR, _private_key_raw)
)

try:
    COOLDOWN_SECONDS = int(os.getenv("PUSH_COOLDOWN_SECONDS", "300"))
except ValueError:
    COOLDOWN_SECONDS = 300

NOTIFY_STATUSES = {
    status.strip().upper()
    for status in os.getenv("PUSH_NOTIFY_STATUSES", "SIAGA,WASPADA,WARNING,BAHAYA").split(",")
    if status.strip()
}

STATUS_META = {
    "SIAGA": {
        "title": "GeoSense: Status SIAGA",
        "desc": "Getaran tanah meningkat. Siapkan tindakan evakuasi dan pantau dashboard.",
    },
    "WASPADA": {
        "title": "GeoSense: Status WASPADA",
        "desc": "BAHAYA! Potensi pergeseran tanah terdeteksi. Segera jauhi area rawan.",
    },
    "WARNING": {
        "title": "GeoSense: Status WASPADA",
        "desc": "BAHAYA! Potensi pergeseran tanah terdeteksi. Segera jauhi area rawan.",
    },
    "BAHAYA": {
        "title": "GeoSense: Status BAHAYA",
        "desc": "BAHAYA! Potensi pergeseran tanah terdeteksi. Segera jauhi area rawan.",
    },
}

_send_lock = threading.Lock()
_last_sent = {"status": None, "at": 0.0}


def enabled() -> bool:
    return (
        PUSH_ENABLED
        and bool(VAPID_PUBLIC_KEY)
        and bool(VAPID_SUBJECT)
        and os.path.exists(VAPID_PRIVATE_KEY_PATH)
    )


def normalize_status(value) -> str:
    return str(value or "").strip().upper()


def should_notify(status: str) -> bool:
    return normalize_status(status) in NOTIFY_STATUSES


def _load_subscriptions() -> list[dict]:
    db = database.SessionLocal()
    try:
        rows = db.query(models.PushSubscription).all()
        return [
            {
                "id": row.id,
                "endpoint": row.endpoint,
                "p256dh": row.p256dh,
                "auth": row.auth,
            }
            for row in rows
        ]
    finally:
        db.close()


def _delete_subscription(subscription_id: int) -> None:
    db = database.SessionLocal()
    try:
        db.query(models.PushSubscription).filter(
            models.PushSubscription.id == subscription_id
        ).delete()
        db.commit()
    except Exception as exc:
        db.rollback()
        print("Gagal menghapus langganan push:", exc)
    finally:
        db.close()


def _build_payload(status: str, source: str, context: dict) -> dict:
    meta = STATUS_META.get(status, {
        "title": f"GeoSense: Status {status}",
        "desc": "Ada pembaruan status dari sistem GeoSense.",
    })

    lines = [meta["desc"]]
    if context.get("kondisi_tanah"):
        lines.append(f"Kondisi: {context['kondisi_tanah']}")
    if context.get("aktivitas"):
        lines.append(f"Aktivitas: {context['aktivitas']}")

    sumber = "Model ML" if source == "ml" else "Override admin"
    lines.append(f"Sumber: {sumber}")

    return {
        "title": meta["title"],
        "body": " | ".join(lines),
        # URL relatif supaya klik notifikasi tetap membuka origin yang dipakai
        # user (localhost, LAN, atau domain produksi).
        "url": "/",
        "status": status,
        "tag": "geosense-status",
        "requireInteraction": status in {"WASPADA", "WARNING", "BAHAYA"},
    }


def send_to_all(payload: dict) -> dict:
    """Kirim payload ke semua langganan. Return ringkasan hasil."""
    if not enabled():
        return {"sent": 0, "failed": 0, "removed": 0, "reason": "Web Push belum dikonfigurasi"}

    subscriptions = _load_subscriptions()
    if not subscriptions:
        return {"sent": 0, "failed": 0, "removed": 0, "reason": "belum ada perangkat berlangganan"}

    data = json.dumps(payload, ensure_ascii=False)
    sent = 0
    failed = 0
    removed = 0

    for item in subscriptions:
        subscription_info = {
            "endpoint": item["endpoint"],
            "keys": {"p256dh": item["p256dh"], "auth": item["auth"]},
        }
        try:
            webpush(
                subscription_info=subscription_info,
                data=data,
                vapid_private_key=VAPID_PRIVATE_KEY_PATH,
                vapid_claims={"sub": VAPID_SUBJECT},
                ttl=3600,
                timeout=15,
            )
            sent += 1
        except WebPushException as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            failed += 1
            # 404/410 = subscription mati. 400/401/403 biasanya subscription
            # dibuat dengan VAPID key lama / malformed; hapus supaya perangkat
            # bisa subscribe ulang dengan key yang benar.
            if status_code in (400, 401, 403, 404, 410):
                _delete_subscription(item["id"])
                removed += 1
            print(f"Push gagal ({status_code}) untuk {item['endpoint'][:48]}...: {exc}")
        except Exception as exc:
            failed += 1
            print(f"Push error: {type(exc).__name__}: {exc}")

    return {"sent": sent, "failed": failed, "removed": removed, "total": len(subscriptions)}


def _broadcast(status: str, source: str, context: dict) -> None:
    payload = _build_payload(status, source, context)
    result = send_to_all(payload)
    print(
        f"Push '{status}': terkirim {result.get('sent', 0)}, "
        f"gagal {result.get('failed', 0)}, dihapus {result.get('removed', 0)}"
    )


def notify_status_change(
    status: str,
    *,
    source: str = "ml",
    changed_by: str | None = None,
    kondisi_tanah: str | None = None,
    aktivitas: str | None = None,
    p2p_amplitude=None,
    rms=None,
    force: bool = False,
) -> dict:
    """Dipanggil setiap perubahan status. Non-blocking (background thread)."""
    status = normalize_status(status)

    if not should_notify(status):
        return {"queued": False, "reason": "status tidak termasuk PUSH_NOTIFY_STATUSES"}

    if not enabled():
        return {"queued": False, "reason": "Web Push belum dikonfigurasi"}

    now = time.time()
    with _send_lock:
        if not force:
            if _last_sent["status"] == status and now - _last_sent["at"] < COOLDOWN_SECONDS:
                return {"queued": False, "reason": "cooldown"}
        _last_sent["status"] = status
        _last_sent["at"] = now

    context = {
        "changed_by": changed_by,
        "kondisi_tanah": kondisi_tanah,
        "aktivitas": aktivitas,
        "p2p_amplitude": p2p_amplitude,
        "rms": rms,
    }

    thread = threading.Thread(
        target=_broadcast,
        args=(status, source, context),
        daemon=True,
        name=f"push-notify-{status}",
    )
    thread.start()
    return {"queued": True}


def send_test() -> dict:
    return send_to_all(
        {
            "title": "Tes Notifikasi GeoSense",
            "body": "Notifikasi browser berhasil diaktifkan. Peringatan status akan muncul di sini.",
            "url": "/",
            "status": "TEST",
            "tag": "geosense-test",
        }
    )


def subscription_count() -> int:
    db = database.SessionLocal()
    try:
        return db.query(models.PushSubscription).count()
    finally:
        db.close()


def last_sent_at() -> str | None:
    if not _last_sent["at"]:
        return None
    when = datetime.datetime.fromtimestamp(_last_sent["at"], datetime.timezone.utc)
    return when.isoformat()
