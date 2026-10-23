"""
Ingest pesan MQTT GeoSense.

Topik yang dipakai:
- ``rawdata``  : data geofon (statistik/waveform/lokasi TDOA)
- ``gpsdata``  : data GPS per node

Setiap pesan disimpan mentah ke tabel ``mqtt_messages``. Data yang bisa
diparse juga dipecah per node ke ``node_telemetry`` dan ``event_locations``.
"""

from __future__ import annotations

import copy
import datetime
import json
import math
from typing import Any

from sqlalchemy.orm import Session

import models
import telemetry

# Field level atas yang bukan milik satu node.
_TOP_LEVEL_NON_NODE_KEYS = {
    "nodes",
    "location",
    "estimated_location",
    "event_location",
    "tdoa_location",
    "direction",
    "timestamp",
    "time",
    "ts",
    "datetime",
    "received_at",
    "created_at",
    "source",
    "type",
    "tdoa_ready",
    "origin",
}


def topic_kind(topic: str) -> str:
    """Klasifikasikan topik menjadi gps | geophone | processed | unknown."""
    parts = [part for part in (topic or "").strip().lower().split("/") if part]
    for part in reversed(parts):
        if "gps" in part:
            return "gps"
        if "raw" in part or part in {"geophone", "geofon", "getaran", "sensor"}:
            return "geophone"
        if part in {"data", "geosense_data", "processed", "result", "ml"}:
            return "processed"
    return "unknown"


def message_type_from_topic(topic: str) -> str:
    return {
        "gps": "gpsdata",
        "geophone": "rawdata",
        "processed": "processed",
        "unknown": "unknown",
    }[topic_kind(topic)]


def parse_payload(raw_text: str) -> Any | None:
    """Parse JSON; kembalikan None kalau payload bukan JSON."""
    try:
        return json.loads(raw_text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None


def topic_node_id(topic: str) -> str | None:
    """Ambil node dari topik, mis. ``rawdata/geo_1`` -> ``geo_1``."""
    if not topic:
        return None
    last = topic.rstrip("/").split("/")[-1]
    return telemetry.node_id_from_token(last)


def node_id_from_mapping(mapping: dict) -> str | None:
    """Cari ID node dari field-field yang umum dipakai device."""
    if not isinstance(mapping, dict):
        return None

    for key in ("node_id", "sensor_id", "node", "geophone", "geofon", "sensor", "device_id"):
        value = mapping.get(key)
        token = telemetry.node_id_from_token(value)
        if token:
            return token

    # Nama node non-standar tetap dipertahankan.
    for key in ("node_id", "sensor_id", "node", "geophone", "geofon"):
        value = mapping.get(key)
        if isinstance(value, str) and value.strip():
            return telemetry.canonical_node_id(value)

    # "id" hanya dipakai kalau memang berbentuk ID node (1/2/3/geo_1/...).
    return telemetry.node_id_from_token(mapping.get("id"))


def _add_record(records: dict[str, dict], node_id: str | None, value: Any) -> None:
    if not node_id or not isinstance(value, dict):
        return
    if node_id in records:
        records[node_id].update(copy.deepcopy(value))
    else:
        records[node_id] = copy.deepcopy(value)


def _records_from_list(values: list, topic: str) -> dict[str, dict]:
    records: dict[str, dict] = {}
    if not values:
        return records

    if all(isinstance(item, (int, float)) and not isinstance(item, bool) for item in values):
        node_id = topic_node_id(topic)
        if node_id:
            records[node_id] = {"signal": list(values)}
        return records

    for index, value in enumerate(values):
        if not isinstance(value, dict):
            continue
        node_id = node_id_from_mapping(value) or telemetry.canonical_node_id(
            value.get("name") or value.get("id"), index
        )
        _add_record(records, node_id, value)
    return records


def extract_records(payload: Any, topic: str = "") -> dict[str, dict]:
    """Pecah payload menjadi ``{node_id: record}``."""
    if isinstance(payload, list):
        return _records_from_list(payload, topic)
    if not isinstance(payload, dict):
        return {}

    records: dict[str, dict] = {}

    raw_nodes = payload.get("nodes")
    if isinstance(raw_nodes, dict):
        for index, (key, value) in enumerate(raw_nodes.items()):
            node_id = telemetry.node_id_from_token(key) or telemetry.canonical_node_id(
                key, index
            )
            _add_record(records, node_id, value)
    elif isinstance(raw_nodes, list):
        for index, value in enumerate(raw_nodes):
            if not isinstance(value, dict):
                continue
            node_id = node_id_from_mapping(value) or telemetry.canonical_node_id(
                value.get("name") or value.get("id"), index
            )
            _add_record(records, node_id, value)

    # Format {"geo_1": {...}, "geo_2": {...}}.
    for key, value in payload.items():
        if key == "nodes" or not isinstance(value, dict):
            continue
        node_id = telemetry.node_id_from_token(key)
        if node_id:
            _add_record(records, node_id, value)

    # Format satu node: {"node": "geo_1", "p2p": ...} / {"sensor_id": ...}.
    single_id = node_id_from_mapping(payload) or topic_node_id(topic)
    if single_id:
        record = records.setdefault(single_id, {})
        for key, value in payload.items():
            if key in _TOP_LEVEL_NON_NODE_KEYS or key == "nodes":
                continue
            if telemetry.node_id_from_token(key) and isinstance(value, dict):
                continue
            if key in telemetry._TOP_LEVEL_GPS_MAPS and isinstance(value, dict):
                # Peta GPS per node ditangani di bawah, bukan sebagai GPS node ini.
                if any(telemetry.node_id_from_token(map_key) for map_key in value):
                    continue
            record.setdefault(key, copy.deepcopy(value))

    # Peta GPS di level atas: {"gps": {"geo_1": {...}, ...}}.
    for map_key in telemetry._TOP_LEVEL_GPS_MAPS:
        gps_map = payload.get(map_key)
        if isinstance(gps_map, dict):
            for key, gps_value in gps_map.items():
                node_id = telemetry.node_id_from_token(key)
                if not node_id:
                    continue
                record = records.setdefault(node_id, {})
                if not isinstance(record.get("gps"), dict):
                    record["gps"] = copy.deepcopy(gps_value)
        elif isinstance(gps_map, list):
            for index, gps_value in enumerate(gps_map):
                if not isinstance(gps_value, dict):
                    continue
                node_id = node_id_from_mapping(gps_value) or telemetry.canonical_node_id(
                    gps_value.get("name") or gps_value.get("id"), index
                )
                record = records.setdefault(node_id, {})
                if not isinstance(record.get("gps"), dict):
                    record["gps"] = copy.deepcopy(gps_value)

    # Format yang dibungkus: {"data": {"geo_1": {...}}} / {"payload": ...}.
    if not records:
        for key in ("data", "payload", "message", "telemetry", "readings"):
            nested = payload.get(key)
            if isinstance(nested, (dict, list)):
                nested_records = extract_records(nested, topic)
                if nested_records:
                    records = nested_records
                    break

    # Payload hanya berisi GPS tanpa ID node, tetapi topik membawa nama node.
    if not records:
        node_id = topic_node_id(topic) or node_id_from_mapping(payload)
        if node_id and (telemetry.extract_gps_record(payload) or _has_metric(payload)):
            records[node_id] = copy.deepcopy(payload)

    return records


def enrich_records(records: dict[str, dict]) -> dict[str, dict]:
    """Tambahkan GPS kanonik + statistik sinyal dari data mentah."""
    for record in records.values():
        telemetry.compute_signal_stats(record)
        gps = telemetry.extract_gps_record(record)
        if gps:
            record["gps"] = gps
        elif "gps" in record:
            record.pop("gps", None)
    return records


def extract_location(payload: Any) -> dict | None:
    """Ambil lokasi event/TDOA dari payload."""
    if not isinstance(payload, dict):
        return None

    for key in ("location", "estimated_location", "event_location", "tdoa_location"):
        value = payload.get(key)
        if isinstance(value, dict):
            location = copy.deepcopy(value)
            if "origin" not in location and "origin" in payload:
                location["origin"] = copy.deepcopy(payload.get("origin"))
            return location
        if isinstance(value, (list, tuple)) and len(value) >= 2:
            return {"x": value[0], "y": value[1]}

    # Sebagian publisher menaruh x/y langsung di root, biasanya bersama tdoa_ready.
    if payload.get("tdoa_ready"):
        x = _number(payload, "x", "east", "dx")
        y = _number(payload, "y", "north", "dy")
        if x is not None and y is not None:
            return {"x": x, "y": y}

    for key in ("data", "payload", "message", "telemetry"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            location = extract_location(nested)
            if location:
                return location

    return None


def extract_meta(payload: Any) -> dict:
    """Ambil metadata status/aktivitas/statistik level atas."""
    if not isinstance(payload, dict):
        return {}

    meta = {
        "status": _text(payload, "status", "status_bencana", "state"),
        "kondisi_tanah": _text(payload, "kondisi_tanah", "soil_condition", "soil"),
        "aktivitas": _text(
            payload,
            "aktivitas",
            "activity",
            "predicted_activity",
            "prediction",
            "class",
        ),
        "p2p_amplitude": _number(payload, "p2p_amplitude", "p2p", "p2p_mv"),
        "rms": _number(payload, "rms", "rms_amplitude"),
        "timestamp": extract_timestamp(payload),
    }

    for key in ("data", "payload", "message", "telemetry"):
        nested = payload.get(key)
        if not isinstance(nested, dict):
            continue
        nested_meta = extract_meta(nested)
        for field, value in nested_meta.items():
            if meta.get(field) is None and value is not None:
                meta[field] = value
    return meta


def extract_timestamp(payload: Any) -> datetime.datetime | None:
    if not isinstance(payload, dict):
        return None
    for key in ("timestamp", "time", "ts", "datetime", "received_at", "created_at"):
        if key in payload:
            parsed = parse_timestamp(payload.get(key))
            if parsed:
                return parsed
    for key in ("data", "payload", "message", "telemetry"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            parsed = extract_timestamp(nested)
            if parsed:
                return parsed
    return None


def parse_timestamp(value: Any) -> datetime.datetime | None:
    if value is None or isinstance(value, bool):
        return None

    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return value

    if isinstance(value, (int, float)):
        number = float(value)
        if not math.isfinite(number) or number <= 0:
            return None
        if number > 1e11:  # epoch milidetik
            number /= 1000.0
        try:
            return datetime.datetime.fromtimestamp(number, tz=datetime.timezone.utc).replace(
                tzinfo=None
            )
        except (OverflowError, OSError, ValueError):
            return None

    text = str(value).strip()
    if not text:
        return None

    try:
        number = float(text)
    except ValueError:
        number = None
    if number is not None:
        return parse_timestamp(number)

    try:
        parsed = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return parsed


def persist_message(
    db: Session,
    *,
    topic: str,
    raw_text: str,
    payload: Any,
    message_type: str,
    records: dict[str, dict],
    location: dict | None,
    error: str | None = None,
    received_at: datetime.datetime | None = None,
) -> int | None:
    """
    Simpan satu pesan MQTT + data terstruktur per node/lokasi.

    Commit dilakukan di sini supaya setiap pesan langsung masuk ke Supabase.
    Caller cukup menangani exception kalau database sedang down.
    """
    received_at = received_at or datetime.datetime.utcnow()
    source_timestamp = extract_timestamp(payload)

    message = models.MqttMessage(
        topic=topic,
        message_type=message_type,
        node_id=next(iter(records)) if len(records) == 1 else None,
        received_at=received_at,
        source_timestamp=source_timestamp,
        payload_text=raw_text,
        payload_json=payload if isinstance(payload, (dict, list)) else None,
        error=error,
    )
    db.add(message)
    db.flush()  # butuh message.id untuk relasi

    for node_id, record in records.items():
        gps = record.get("gps") if isinstance(record.get("gps"), dict) else {}
        signal = record.get("signal")
        db.add(
            models.NodeTelemetry(
                mqtt_message_id=message.id,
                node_id=telemetry.canonical_node_id(node_id),
                topic=topic,
                message_type=message_type,
                received_at=received_at,
                source_timestamp=source_timestamp,
                online=_bool(record, "online", "connected"),
                status=_text(record, "status", "state"),
                aktivitas=_text(record, "aktivitas", "activity", "prediction"),
                p2p=_number(
                    record,
                    "p2p",
                    "p2p_amplitude",
                    "peak_to_peak",
                    "amplitude",
                    "pp",
                ),
                rms=_number(record, "rms", "rms_amplitude"),
                peak=_number(record, "peak", "max", "amplitude_max"),
                signal=signal if isinstance(signal, (list, dict)) else None,
                latitude=_number(gps, "latitude", "lat"),
                longitude=_number(gps, "longitude", "lon", "lng"),
                altitude=_number(gps, "altitude", "alt", "elevation"),
                satellites=_int(gps, "satellites", "sats", "num_sats"),
                hdop=_number(gps, "hdop", "accuracy"),
                gps_fix=_bool(gps, "fix", "valid", "has_fix"),
                raw_data=record,
            )
        )

    if isinstance(location, dict):
        direction = location.get("direction") if isinstance(location.get("direction"), dict) else {}
        bearing = _number(direction, "bearing_degrees", "bearing", "heading", "azimuth")
        if bearing is None:
            bearing = _number(location, "bearing_degrees", "bearing", "heading", "azimuth")
        db.add(
            models.EventLocation(
                mqtt_message_id=message.id,
                received_at=received_at,
                source_timestamp=source_timestamp,
                x=_number(location, "x", "east", "dx"),
                y=_number(location, "y", "north", "dy"),
                latitude=_number(location, "latitude", "lat"),
                longitude=_number(location, "longitude", "lon", "lng"),
                estimated_latitude=_number(
                    location, "estimated_latitude", "event_latitude"
                ),
                estimated_longitude=_number(
                    location, "estimated_longitude", "event_longitude"
                ),
                bearing_degrees=bearing,
                compass=_text(direction, "compass") or _text(location, "compass"),
                direction_source=_text(direction, "source"),
                raw_data=location,
            )
        )

    db.commit()
    return message.id


def _has_metric(mapping: dict) -> bool:
    for key in ("signal", "d", "data", "samples", "values", "waveform", "raw"):
        value = mapping.get(key)
        if isinstance(value, (list, tuple)) and value:
            return True
    return _number(
        mapping,
        "p2p",
        "p2p_amplitude",
        "rms",
        "peak",
        "voltage",
        "mv",
        "val",
    ) is not None


def _number(mapping: Any, *keys: str) -> float | None:
    if not isinstance(mapping, dict):
        return None
    for key in keys:
        value = mapping.get(key)
        if value is None or isinstance(value, bool):
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            return number
    return None


def _int(mapping: Any, *keys: str) -> int | None:
    number = _number(mapping, *keys)
    return int(number) if number is not None else None


def _text(mapping: Any, *keys: str) -> str | None:
    if not isinstance(mapping, dict):
        return None
    for key in keys:
        value = mapping.get(key)
        if value is None or isinstance(value, (dict, list, tuple)):
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _bool(mapping: Any, *keys: str) -> bool | None:
    if not isinstance(mapping, dict):
        return None
    for key in keys:
        if key not in mapping:
            continue
        value = mapping.get(key)
        if value is None or value == "":
            continue
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value).strip().lower()
        if text in ("1", "true", "yes", "y", "fixed", "valid", "ok", "online", "connect"):
            return True
        if text in ("0", "false", "no", "n", "invalid", "none", "offline", "disconnect"):
            return False
    return None
