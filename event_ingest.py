"""
Ingest event waveform GeoSense (protokol LoRa event-based).

Node STM32 mengirim satu event dalam beberapa frame:

- ``EVENT_META`` : identitas node, ID event, UTC GPS + validitas, jumlah
  sampel, indeks trigger, periode sampel, dan info wavelet.
- ``DATA``       : potongan sampel waveform (hasil denoising Haar level 5).
- ``EVENT_END``  : penutup event.

Receiver LoRa (Jetson/ESP32) menyusun ulang frame biner, lalu meneruskannya
ke MQTT. Modul ini menerima dua bentuk:

1. **Event lengkap** dalam satu pesan JSON (paling sederhana, disarankan):

   .. code-block:: json

      {
        "type": "event",
        "n": "NODE02",
        "event_id": 17,
        "utc": "2026-10-23T10:15:30.250Z",
        "utc_valid": true,
        "sample_count": 900,
        "trigger_index": 180,
        "sample_period_us": 11111,
        "wavelet": {"name": "haar", "level": 5},
        "samples": [12015, 11984, 12021]
      }

2. **Frame terpisah** (EVENT_META / DATA / EVENT_END) yang dirangkai di sini
   berdasarkan ``(node_id, event_id)`` dan ``first_index``.

Nilai sampel masih **ADC counts**, bukan volt terkalibrasi. Statistik
``p2p``/``rms``/``peak`` dihitung dari nilai tersebut dan diberi unit
``adc_counts`` supaya dashboard tidak salah label mV.

Dokumentasi kontrak lengkap ada di ``EVENT_WAVEFORM.md``.
"""

from __future__ import annotations

import datetime
import math
import threading
import time
from typing import Any

from sqlalchemy.orm import Session

import models
import telemetry
from mqtt_ingest import parse_timestamp

MAX_SAMPLES = 200_000
DEFAULT_UNIT = "adc_counts"

_FRAME_TYPE_KEYS = ("frame_type", "packet_type", "kind", "type", "frame")
_NODE_KEYS = ("n", "node_id", "node", "sensor_id", "device_id")
_EVENT_ID_KEYS = ("event_id", "event", "eid", "id_event", "eventid", "event_index")
_SAMPLE_KEYS = ("samples", "d", "waveform", "values", "data")
_FIRST_INDEX_KEYS = (
    "first_index",
    "start_index",
    "sample_start",
    "sample_index",
    "idx",
    "offset",
)
_SEQ_KEYS = ("seq", "sequence", "packet", "packet_seq", "frame_index", "p")
_TOTAL_KEYS = (
    "sample_count",
    "total_samples",
    "samples_count",
    "jumlah_sampel",
    "n_samples",
)
_TRIGGER_KEYS = ("trigger_index", "trigger", "trigger_pos", "trig_index")
_UTC_KEYS = ("utc", "gps_utc", "event_utc", "time_utc", "utc_time")
_UTC_VALID_KEYS = ("utc_valid", "gps_valid", "time_valid", "valid_utc")
_PERIOD_KEYS = (
    "sample_period_us",
    "sample_period",
    "period_us",
    "dt_us",
    "sampling_period_us",
)
_END_TOTAL_KEYS = ("total_packets", "packets", "total_frames", "frame_count", "total")
_UNIT_KEYS = ("unit", "sample_unit", "amplitude_unit")

_META_MARKERS = {
    "event_meta",
    "eventmeta",
    "meta",
    "metadata",
    "meta_event",
    "start",
    "event_start",
}
_DATA_MARKERS = {
    "data",
    "event_data",
    "eventdata",
    "waveform",
    "chunk",
    "samples",
    "sample",
}
_END_MARKERS = {"event_end", "eventend", "end", "finish", "done", "stop", "event_stop"}
_EVENT_MARKERS = {"event", "event_waveform", "waveform_event", "complete", "full", "record"}


def _first(payload: Any, keys: tuple[str, ...]) -> Any:
    """Ambil field pertama yang ada di payload atau di kontainer meta/header."""
    if not isinstance(payload, dict):
        return None
    for key in keys:
        value = payload.get(key)
        if value is not None and value != "":
            return value
    for container in ("meta", "metadata", "header", "info"):
        nested = payload.get(container)
        if not isinstance(nested, dict):
            continue
        for key in keys:
            value = nested.get(key)
            if value is not None and value != "":
                return value
    return None


def _text(payload: Any, keys: tuple[str, ...]) -> str | None:
    value = _first(payload, keys)
    if value is None or isinstance(value, (dict, list, tuple, bool)):
        return None
    text = str(value).strip()
    return text or None


def _number(payload: Any, keys: tuple[str, ...]) -> float | None:
    value = _first(payload, keys)
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _int(payload: Any, keys: tuple[str, ...]) -> int | None:
    number = _number(payload, keys)
    return int(number) if number is not None else None


def _bool(payload: Any, keys: tuple[str, ...]) -> bool | None:
    value = _first(payload, keys)
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in ("1", "true", "yes", "y", "valid", "ok", "fix", "fixed"):
        return True
    if text in ("0", "false", "no", "n", "invalid", "none"):
        return False
    return None


def frame_kind(payload: dict) -> str | None:
    """Kembalikan ``meta`` | ``data`` | ``end`` | ``event`` | None."""
    marker = _text(payload, _FRAME_TYPE_KEYS)
    if not marker:
        return None
    marker = marker.strip().lower().replace("-", "_").replace(" ", "_")
    if marker in _META_MARKERS:
        return "meta"
    if marker in _DATA_MARKERS:
        return "data"
    if marker in _END_MARKERS:
        return "end"
    if marker in _EVENT_MARKERS:
        return "event"
    return None
def _node_id(payload: dict) -> str | None:
    # Firmware memakai node index 0-based; dukung payload yang hanya
    # membawa node_index (tanpa ``n``).
    if "node_index" in payload:
        try:
            index = int(payload["node_index"])
            if 0 <= index <= 2:
                return f"geo_{index + 1}"
        except (TypeError, ValueError):
            pass
    value = _first(payload, _NODE_KEYS)
    token = telemetry.node_id_from_token(value)
    if token:
        return token
    if _text(payload, ("node_id", "node", "sensor_id")):
        return telemetry.canonical_node_id(_first(payload, ("node_id", "node", "sensor_id")))
    return None


def _event_id(payload: dict) -> str | None:
    value = _first(payload, _EVENT_ID_KEYS)
    if value is None or isinstance(value, (dict, list, tuple, bool)):
        return None
    text = str(value).strip()
    return text or None


def _sample_values(payload: dict) -> list[Any] | None:
    for key in _SAMPLE_KEYS:
        values = payload.get(key)
        if isinstance(values, (list, tuple)):
            if not values:
                return []
            if len(values) > MAX_SAMPLES:
                values = values[:MAX_SAMPLES]
            cleaned: list[Any] = []
            for item in values:
                if item is None or isinstance(item, bool):
                    cleaned.append(None)
                    continue
                try:
                    number = float(item)
                except (TypeError, ValueError):
                    cleaned.append(None)
                    continue
                if not math.isfinite(number):
                    cleaned.append(None)
                    continue
                cleaned.append(int(number) if number.is_integer() else round(number, 4))
            return cleaned
    return None


def _wavelet(payload: dict) -> dict:
    raw = _first(payload, ("wavelet", "denoise", "denoising"))
    info: dict[str, Any] = {}
    if isinstance(raw, dict):
        info.update(
            {
                "name": raw.get("name") or raw.get("type") or raw.get("wavelet"),
                "level": raw.get("level") or raw.get("decomposition_level"),
            }
        )
        if "denoised" in raw:
            info["denoised"] = bool(raw.get("denoised"))
    elif isinstance(raw, str) and raw.strip():
        info["name"] = raw.strip()

    if not info.get("name"):
        info["name"] = _text(payload, ("wavelet_name",))
    if info.get("level") is None:
        info["level"] = _int(payload, ("wavelet_level", "decomposition_level", "level"))
    if "denoised" not in info:
        denoised = _bool(payload, ("denoised", "is_denoised"))
        info["denoised"] = True if denoised is None else denoised
    return {key: value for key, value in info.items() if value not in (None, "")}


def _sample_period_us(payload: dict) -> float | None:
    value = _number(payload, _PERIOD_KEYS)
    if value is not None and value > 0:
        return value
    rate = _number(payload, ("sampling_rate", "sample_rate", "fs", "sps"))
    if rate is not None and rate > 0:
        return round(1_000_000.0 / rate, 6)
    return None


def parse_message(payload: Any) -> tuple[str, dict] | None:
    """
    Klasifikasikan payload event.

    Kembalian: ``("complete", payload)`` untuk event utuh,
    ``("frame", payload)`` untuk EVENT_META/DATA/EVENT_END, atau ``None``
    kalau bukan pesan event.
    """
    if not isinstance(payload, dict):
        return None

    kind = frame_kind(payload)
    node_id = _node_id(payload)
    event_id = _event_id(payload)
    if node_id is None or event_id is None:
        return None

    samples = _sample_values(payload)
    has_first_index = _first(payload, _FIRST_INDEX_KEYS) is not None
    has_seq = _first(payload, _SEQ_KEYS) is not None
    total = _int(payload, _TOTAL_KEYS)
    utc = _first(payload, _UTC_KEYS)
    trigger = _int(payload, _TRIGGER_KEYS)

    if kind == "event" and samples is not None:
        return "complete", payload

    if kind in ("meta", "end"):
        return "frame", _frame(payload)

    if kind == "data":
        # Frame DATA biasanya punya first_index/seq. Kalau tidak ada dan
        # membawa metadata total/UTC/trigger, anggap event utuh satu pesan.
        if has_first_index or has_seq:
            return "frame", _frame(payload)
        if samples is not None and (total is not None or utc or trigger is not None):
            return "complete", payload
        return "frame", _frame(payload)

    # Tanpa penanda tipe: hanya dianggap event kalau ada metadata event.
    if samples is not None and (total is not None or trigger is not None or utc):
        return "complete", payload
    return None


def _statistics(values: list[Any]) -> dict:
    numeric = [
        float(value)
        for value in values
        if value is not None and not isinstance(value, bool)
    ]
    if not numeric:
        return {"p2p": None, "rms": None, "peak": None, "mean": None}
    return {
        "p2p": round(max(numeric) - min(numeric), 4),
        "rms": round(math.sqrt(sum(v * v for v in numeric) / len(numeric)), 4),
        "peak": round(max(abs(v) for v in numeric), 4),
        "mean": round(sum(numeric) / len(numeric), 4),
    }


def _event_record(
    *,
    node_id: str,
    event_id: str,
    meta: dict,
    samples: list[Any],
    complete: bool,
    frames: int,
    missing: int,
) -> dict:
    utc_value = meta.get("utc")
    event_time = parse_timestamp(utc_value)
    utc_valid = meta.get("utc_valid")
    if utc_valid is None:
        utc_valid = event_time is not None

    stats = _statistics(samples)
    trigger_index = meta.get("trigger_index")
    trigger_value = None
    if (
        trigger_index is not None
        and 0 <= int(trigger_index) < len(samples)
        and samples[int(trigger_index)] is not None
    ):
        trigger_value = samples[int(trigger_index)]

    expected = meta.get("sample_count")
    unit = meta.get("unit") or DEFAULT_UNIT
    period = meta.get("sample_period_us")
    sampling_rate = round(1_000_000.0 / period, 4) if period else None

    record: dict[str, Any] = {
        "node_id": node_id,
        "event_id": event_id,
        "event_utc": event_time.isoformat() if event_time else (
            str(utc_value) if utc_value else None
        ),
        "utc_valid": bool(utc_valid),
        "unit": unit,
        "denoised": bool(meta.get("denoised", True)),
        "sample_count": sum(1 for value in samples if value is not None),
        "expected_samples": expected,
        "trigger_index": int(trigger_index) if trigger_index is not None else None,
        "trigger_value": trigger_value,
        "sample_period_us": period,
        "sampling_rate": sampling_rate,
        "wavelet": meta.get("wavelet") or {},
        "signal": list(samples),
        "frames_received": frames,
        "missing_samples": missing,
        "complete": complete,
        **stats,
    }
    return record


def complete_event(payload: dict) -> dict | None:
    """Bangun record event dari satu pesan JSON yang sudah lengkap."""
    node_id = _node_id(payload)
    event_id = _event_id(payload)
    samples = _sample_values(payload)
    if node_id is None or event_id is None or samples is None:
        return None

    expected = _int(payload, _TOTAL_KEYS)
    if expected is not None and expected > len(samples):
        samples = list(samples) + [None] * (expected - len(samples))
    missing = sum(1 for value in samples if value is None)

    meta = {
        "utc": _first(payload, _UTC_KEYS),
        "utc_valid": _bool(payload, _UTC_VALID_KEYS),
        "sample_count": expected or len(samples),
        "trigger_index": _int(payload, _TRIGGER_KEYS),
        "sample_period_us": _sample_period_us(payload),
        "unit": _text(payload, _UNIT_KEYS) or DEFAULT_UNIT,
        "wavelet": _wavelet(payload),
    }
    return _event_record(
        node_id=node_id,
        event_id=event_id,
        meta=meta,
        samples=samples,
        complete=True,
        frames=1,
        missing=missing,
    )


class EventAssembler:
    """Rangkai frame EVENT_META/DATA/EVENT_END menjadi satu event."""

    def __init__(self, ttl_seconds: float = 600.0) -> None:
        self._ttl = float(ttl_seconds)
        self._lock = threading.Lock()
        self._buffers: dict[tuple[str, str], dict] = {}
        self._finished: dict[tuple, float] = {}

    def _fresh(self, bucket: dict, now: float) -> bool:
        return (now - bucket.get("updated_at", now)) <= self._ttl

    def _cleanup(self, now: float) -> None:
        for key in [key for key, value in self._buffers.items() if not self._fresh(value, now)]:
            self._buffers.pop(key, None)
        for key in [key for key, seen in self._finished.items() if (now - seen) > self._ttl]:
            self._finished.pop(key, None)

    def mark_finalized(self, record: dict) -> bool:
        """Return False kalau event yang sama sudah pernah diproses."""
        key = (
            record.get("node_id"),
            record.get("event_id"),
            record.get("event_utc") or record.get("sample_count"),
        )
        now = time.time()
        with self._lock:
            self._cleanup(now)
            if key in self._finished:
                return False
            self._finished[key] = now
            return True

    def feed(self, frame: dict) -> dict | None:
        """Masukkan satu frame; return record event kalau sudah lengkap."""
        node_id = frame.get("node_id")
        event_id = frame.get("event_id")
        kind = frame.get("kind")
        if not node_id or not event_id or kind not in ("meta", "data", "end"):
            return None

        now = time.time()
        key = (node_id, event_id)
        with self._lock:
            self._cleanup(now)
            bucket = self._buffers.get(key)

            # EVENT_END untuk event yang sudah final (atau belum pernah ada)
            # tidak boleh membuat buffer kosong.
            if kind == "end" and bucket is None:
                return None

            if bucket is None:
                bucket = {
                    "node_id": node_id,
                    "event_id": event_id,
                    "meta": {},
                    "samples": {},
                    "frames": 0,
                    "started_at": now,
                }
                self._buffers[key] = bucket
            bucket["updated_at"] = now

            if kind == "meta":
                bucket["meta"].update(frame.get("meta") or {})
                return None

            if kind == "data":
                samples = frame.get("samples") or []
                if samples:
                    first_index = frame.get("first_index")
                    if first_index is None:
                        first_index = (max(bucket["samples"]) + 1) if bucket["samples"] else 0
                    for offset, value in enumerate(samples):
                        index = int(first_index) + offset
                        if 0 <= index < MAX_SAMPLES:
                            bucket["samples"][index] = value
                    bucket["frames"] += 1
                self._maybe_fill_meta(bucket, frame)
                if self._is_complete(bucket):
                    return self._finalize(key, bucket, complete=True)
                return None

            # kind == "end"
            bucket["meta"].update(frame.get("meta") or {})
            end_total = frame.get("total_packets")
            if end_total is not None:
                bucket["meta"].setdefault("total_packets", end_total)
            return self._finalize(key, bucket, complete=True)

    def _maybe_fill_meta(self, bucket: dict, frame: dict) -> None:
        meta = bucket["meta"]
        for field in ("utc", "utc_valid", "trigger_index", "sample_period_us", "unit"):
            if meta.get(field) is None and frame.get(field) is not None:
                meta[field] = frame[field]
        if not meta.get("wavelet") and frame.get("wavelet"):
            meta["wavelet"] = frame["wavelet"]

    def _is_complete(self, bucket: dict) -> bool:
        expected = bucket["meta"].get("sample_count")
        if not expected:
            return False
        try:
            expected = int(expected)
        except (TypeError, ValueError):
            return False
        received = sum(1 for value in bucket["samples"].values() if value is not None)
        indices = bucket["samples"]
        if not indices:
            return False
        return (
            received >= expected
            and min(indices) <= 0
            and max(indices) >= expected - 1
        )

    def _finalize(self, key: tuple[str, str], bucket: dict, *, complete: bool) -> dict:
        self._buffers.pop(key, None)
        meta = bucket["meta"]
        indices = sorted(bucket["samples"])
        expected = meta.get("sample_count")
        try:
            expected = int(expected) if expected is not None else None
        except (TypeError, ValueError):
            expected = None

        if indices:
            last = max(indices[-1], (expected - 1) if expected else indices[-1])
        else:
            last = -1
        if expected is not None:
            last = max(last, expected - 1)

        samples = [bucket["samples"].get(index) for index in range(last + 1)]
        missing = sum(1 for value in samples if value is None)
        complete_flag = bool(complete and (expected is None or missing == 0))
        return _event_record(
            node_id=bucket["node_id"],
            event_id=bucket["event_id"],
            meta=meta,
            samples=samples,
            complete=complete_flag,
            frames=bucket["frames"],
            missing=missing,
        )


def _frame(payload: dict) -> dict:
    kind = frame_kind(payload)
    samples = _sample_values(payload) or []
    first_index = _int(payload, _FIRST_INDEX_KEYS)

    total = _int(payload, _TOTAL_KEYS)
    if kind == "data" and _first(payload, ("total_samples", "samples_count", "jumlah_sampel")) is None:
        # Pada frame DATA, "sample_count"/"n_samples" biasanya berarti jumlah
        # sampel di frame ini, bukan total event.
        total = None

    meta = {
        "utc": _first(payload, _UTC_KEYS),
        "utc_valid": _bool(payload, _UTC_VALID_KEYS),
        "sample_count": total,
        "trigger_index": _int(payload, _TRIGGER_KEYS),
        "sample_period_us": _sample_period_us(payload),
        "unit": _text(payload, _UNIT_KEYS) or DEFAULT_UNIT,
        "wavelet": _wavelet(payload),
        "total_packets": _int(payload, _END_TOTAL_KEYS),
    }
    meta = {key: value for key, value in meta.items() if value is not None}

    return {
        "kind": kind,
        "node_id": _node_id(payload),
        "event_id": _event_id(payload),
        "samples": samples,
        "first_index": first_index,
        "seq": _int(payload, _SEQ_KEYS),
        "meta": meta,
        "total_packets": _int(payload, _END_TOTAL_KEYS),
    }


def frame_from_payload(payload: Any) -> dict | None:
    if not isinstance(payload, dict) or frame_kind(payload) not in ("meta", "data", "end"):
        return None
    if _node_id(payload) is None or _event_id(payload) is None:
        return None
    return _frame(payload)


def persist_event(
    db: Session,
    *,
    topic: str,
    record: dict,
    mqtt_message_id: int | None = None,
    received_at: datetime.datetime | None = None,
) -> int:
    """Simpan event waveform sebagai satu baris ``node_telemetry``."""
    raw = {
        key: value
        for key, value in record.items()
        if key not in ("signal",)
    }
    row = models.NodeTelemetry(
        mqtt_message_id=mqtt_message_id,
        node_id=telemetry.canonical_node_id(record.get("node_id")),
        topic=topic or "event",
        message_type="event",
        received_at=received_at or datetime.datetime.utcnow(),
        source_timestamp=parse_timestamp(record.get("event_utc")),
        p2p=record.get("p2p"),
        rms=record.get("rms"),
        peak=record.get("peak"),
        signal=record.get("signal"),
        raw_data=raw,
    )
    db.add(row)
    db.commit()
    return row.id
