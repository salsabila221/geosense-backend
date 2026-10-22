"""
Integrasi WhatsApp via Fonnte (https://fonnte.com).

Kenapa di backend? Token Fonnte bersifat rahasia (siapa pun yang punya token bisa
mengirim pesan dari nomor kamu). Jadi semua pengiriman dilakukan server, bukan frontend.

Cara pakai dari kode lain:
    import notifier
    notifier.notify_status_change("WASPADA", source="ml", kondisi_tanah="Tanah retak")

Kalau FONNTE_TOKEN kosong, semua pengiriman otomatis dilewati (log saja), jadi
aplikasi tetap jalan normal tanpa Fonnte.
"""

import datetime
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from dotenv import load_dotenv

import database
import models

load_dotenv()

# Windows console bisa cp1252 -> emoji di print() bikin thread notifikasi error.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

FONNTE_URL = os.getenv("FONNTE_URL", "https://api.fonnte.com/send").strip()
FONNTE_TOKEN = os.getenv("FONNTE_TOKEN", "").strip()
WA_ENABLED = os.getenv("WA_ENABLED", "1").strip() == "1"
APP_URL = os.getenv("APP_URL", "http://localhost:5173").strip()

try:
    COOLDOWN_SECONDS = int(os.getenv("WA_COOLDOWN_SECONDS", "300"))
except ValueError:
    COOLDOWN_SECONDS = 300

NOTIFY_STATUSES = {
    status.strip().upper()
    for status in os.getenv("WA_NOTIFY_STATUSES", "SIAGA,WASPADA,WARNING,BAHAYA").split(",")
    if status.strip()
}

STATUS_META = {
    "SIAGA": {
        "emoji": "⚠️",
        "desc": "Getaran tanah meningkat. Mohon siapkan tindakan evakuasi.",
    },
    "WASPADA": {
        "emoji": "🚨",
        "desc": "BAHAYA! Potensi pergeseran tanah terdeteksi. Segera jauhi area rawan.",
    },
    "WARNING": {
        "emoji": "🚨",
        "desc": "BAHAYA! Potensi pergeseran tanah terdeteksi. Segera jauhi area rawan.",
    },
    "BAHAYA": {
        "emoji": "🚨",
        "desc": "BAHAYA! Potensi pergeseran tanah terdeteksi. Segera jauhi area rawan.",
    },
}

# Anti-spam: status yang sama tidak dikirim ulang dalam rentang cooldown.
_send_lock = threading.Lock()
_last_sent = {"status": None, "at": 0.0}

BULAN = [
    "Jan", "Feb", "Mar", "Apr", "Mei", "Jun",
    "Jul", "Agu", "Sep", "Okt", "Nov", "Des",
]


def enabled() -> bool:
    return WA_ENABLED and bool(FONNTE_TOKEN)


def normalize_status(value) -> str:
    return str(value or "").strip().upper()


def should_notify(status: str) -> bool:
    return normalize_status(status) in NOTIFY_STATUSES


def _now_wib() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=7)))


def _format_time(dt: datetime.datetime) -> str:
    return f"{dt.day} {BULAN[dt.month - 1]} {dt.year}, {dt:%H.%M} WIB"


def _target(phone: str) -> str:
    """08123456789 / +628123456789 / 628123456789 -> 628123456789 (format Fonnte)."""
    digits = re.sub(r"\D", "", phone or "")
    if digits.startswith("0"):
        digits = "62" + digits[1:]
    elif digits.startswith("8"):
        digits = "62" + digits
    return digits


def build_message(
    status: str,
    *,
    fullname: str | None = None,
    source: str = "ml",
    changed_by: str | None = None,
    kondisi_tanah: str | None = None,
    aktivitas: str | None = None,
    p2p_amplitude=None,
    rms=None,
    when: datetime.datetime | None = None,
) -> str:
    status = normalize_status(status)
    meta = STATUS_META.get(status, {"emoji": "ℹ️", "desc": ""})
    when = when or _now_wib()

    sumber = "Model ML (otomatis)" if source == "ml" else f"Override Admin ({changed_by or 'admin'})"

    lines = [
        f"{meta['emoji']} *PERINGATAN DINI GEOSENSE*",
        "",
        f"Halo {fullname or 'Pengguna GeoSense'},",
        f"Status sistem berubah menjadi *{status}*.",
    ]

    if meta["desc"]:
        lines += ["", meta["desc"]]

    lines += [
        "",
        f"• Waktu     : {_format_time(when)}",
        f"• Sumber    : {sumber}",
    ]

    if kondisi_tanah:
        lines.append(f"• Kondisi   : {kondisi_tanah}")
    if aktivitas:
        lines.append(f"• Aktivitas : {aktivitas}")

    getaran = []
    if p2p_amplitude is not None:
        getaran.append(f"p2p {p2p_amplitude}")
    if rms is not None:
        getaran.append(f"rms {rms}")
    if getaran:
        lines.append(f"• Getaran   : {' | '.join(getaran)}")

    lines += [
        "",
        f"Cek dashboard: {APP_URL}",
    ]
    return "\n".join(lines)


def _post_fonnte(target: str, message: str) -> tuple[bool, dict]:
    """Kirim satu pesan. Return (sukses, response_json)."""
    payload = urllib.parse.urlencode(
        {
            "target": target,
            "message": message,
            "countryCode": "62",
        }
    ).encode("utf-8")

    request = urllib.request.Request(
        FONNTE_URL,
        data=payload,
        headers={
            "Authorization": FONNTE_TOKEN,
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=25) as response:
            raw = response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", "replace") or str(exc)
        return False, {"http_status": exc.code, "raw": raw}
    except Exception as exc:  # timeout, DNS, dll.
        return False, {"error": f"{type(exc).__name__}: {exc}"}

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return False, {"raw": raw}

    return bool(data.get("status")), data


def _log(phone: str, status: str, source: str, success: bool, detail: str) -> None:
    db = database.SessionLocal()
    try:
        db.add(
            models.NotificationLog(
                phone=phone,
                status=normalize_status(status),
                source=source,
                success=success,
                detail=(detail or "")[:2000],
                created_at=datetime.datetime.utcnow(),
            )
        )
        db.commit()
    except Exception as exc:
        print("⚠️ Gagal menyimpan log notifikasi:", exc)
        db.rollback()
    finally:
        db.close()


def _recipients() -> list[tuple[str, str | None]]:
    """Semua user aktif yang punya nomor WhatsApp. Return [(phone, fullname)]."""
    db = database.SessionLocal()
    try:
        users = (
            db.query(models.User)
            .filter(models.User.is_active.is_(True), models.User.phone_number.isnot(None))
            .all()
        )
        result = []
        seen = set()
        for user in users:
            phone = _target(user.phone_number)
            if not phone or phone in seen:
                continue
            seen.add(phone)
            result.append((phone, user.fullname))
        return result
    finally:
        db.close()


def _broadcast(status: str, source: str, context: dict) -> None:
    recipients = _recipients()
    if not recipients:
        print("ℹ️ Notifikasi WA: tidak ada user dengan nomor WhatsApp.")
        return

    print(f"📤 Notifikasi WA '{status}' ke {len(recipients)} nomor...")
    for phone, fullname in recipients:
        message = build_message(
            status,
            fullname=fullname,
            source=source,
            changed_by=context.get("changed_by"),
            kondisi_tanah=context.get("kondisi_tanah"),
            aktivitas=context.get("aktivitas"),
            p2p_amplitude=context.get("p2p_amplitude"),
            rms=context.get("rms"),
        )
        ok, response = _post_fonnte(phone, message)
        _log(phone, status, source, ok, json.dumps(response, ensure_ascii=False))
        print(f"   {'✅' if ok else '❌'} {phone}: {response}")
        time.sleep(0.5)  # jeda kecil biar tidak kena rate limit Fonnte


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
    """
    Dipanggil setiap ada perubahan status. Non-blocking: pengiriman jalan di
    background thread supaya MQTT/HTTP tidak ikut menunggu.
    """
    status = normalize_status(status)

    if not should_notify(status):
        return {"queued": False, "reason": "status tidak termasuk WA_NOTIFY_STATUSES"}

    if not enabled():
        print(f"ℹ️ Notifikasi WA dilewati (FONNTE_TOKEN belum diset) untuk status {status}.")
        return {"queued": False, "reason": "Fonnte belum dikonfigurasi"}

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
        name=f"wa-notify-{status}",
    )
    thread.start()
    return {"queued": True}


def send_test(phone: str) -> tuple[bool, dict]:
    """Kirim pesan uji ke satu nomor (dipakai endpoint admin)."""
    if not enabled():
        return False, {"error": "FONNTE_TOKEN belum diset di .env"}

    target = _target(phone)
    if not re.fullmatch(r"628[1-9]\d{6,11}", target):
        return False, {"error": "Nomor tidak valid. Contoh: 08123456789"}

    message = "\n".join(
        [
            "✅ *TES NOTIFIKASI GEOSENSE*",
            "",
            "Integrasi WhatsApp via Fonnte berhasil.",
            f"Waktu: {_format_time(_now_wib())}",
            "",
            f"Cek dashboard: {APP_URL}",
        ]
    )
    ok, response = _post_fonnte(target, message)
    _log(target, "TEST", "admin", ok, json.dumps(response, ensure_ascii=False))
    return ok, response
