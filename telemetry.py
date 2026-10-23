"""
Normalisasi payload telemetri GeoSense.

GPS dapat dikirim di dalam objek node (payload yang sama dengan geofon) atau
lewat topik MQTT terpisah ``gpsdata``. Backend menggabungkan keduanya per node
sebelum mengirim payload dashboard.

Format gabungan yang direkomendasikan:

    {
      "status": "AMAN",
      "timestamp": 1760000000,
      "nodes": {
        "geo_1": {
          "online": true,
          "p2p": 1.2,
          "rms": 0.4,
          "peak": 2.1,
          "signal": [0.1, 0.2, 0.3],
          "gps": {
            "latitude": -6.9024,
            "longitude": 107.6188,
            "altitude": 850.0,
            "satellites": 8,
            "hdop": 0.9,
            "fix": true
          }
        },
        "geo_2": { "...": "..." },
        "geo_3": { "...": "..." }
      },
      "location": {"x": 12.3, "y": -4.5}
    }

Untuk kompatibilitas, normalizer juga menerima GPS di field node langsung
(`latitude`/`longitude`, `lat`/`lon`), di dalam `gps`, `gnss`, `position`, atau
sebagai peta terpisah `gps`/`node_gps` di level atas payload.

Arah longsor dihitung dari vektor hasil triangulasi `location.x` (meter, timur)
dan `location.y` (meter, utara). Arah 0 derajat = Utara, 90 = Timur. Kalau
Processing memakai origin koordinat selain pusat array, kirim `location.origin`
berisi `{latitude, longitude}` atau `"geo_1"` agar arah dan estimasi posisi
dihitung dari origin yang benar.
"""

from __future__ import annotations

import copy
import math
import re
from typing import Any

NODE_KEYS = ("geo_1", "geo_2", "geo_3")

_COMPASS_8 = (
    "Utara",
    "Timur Laut",
    "Timur",
    "Tenggara",
    "Selatan",
    "Barat Daya",
    "Barat",
    "Barat Laut",
)
_COMPASS_8_SHORT = ("U", "TL", "T", "TG", "S", "BD", "B", "BL")

_LAT_KEYS = ("latitude", "lat", "lintang", "latitude_deg", "lat_deg")
_LON_KEYS = (
    "longitude",
    "lon",
    "lng",
    "long",
    "bujur",
    "longitude_deg",
    "lon_deg",
    "lng_deg",
)
_ALT_KEYS = ("altitude", "alt", "elevation", "elevasi")
_SAT_KEYS = ("satellites", "sats", "num_sats", "sat_count", "satelit")
_HDOP_KEYS = ("hdop", "horizontal_dilution", "accuracy")
_FIX_KEYS = ("fix", "valid", "has_fix", "gps_fix", "fixed")

_GPS_CONTAINER_KEYS = ("gps", "gnss", "position", "coords", "coordinates", "location")
_TOP_LEVEL_GPS_MAPS = ("gps", "node_gps", "gps_data", "gps_nodes")


def _to_float(value: Any) -> float | None:
    """Konversi angka/string angka ke float. String koma desimal didukung."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    # "107,6" -> "107.6" (tetapi jangan rusak pemisah ribuan "1,234.5").
    if "," in text and "." not in text:
        text = text.replace(",", ".")

    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _parse_coordinate(value: Any, *, latitude: bool) -> float | None:
    """Parse desimal derajat, termasuk format '6.9 S' / '107.6 E'."""
    numeric = _to_float(value)
    if numeric is not None:
        result = numeric
    else:
        text = str(value or "").strip().upper()
        match = re.fullmatch(r"([+-]?\d+(?:[.,]\d+)?)\s*°?\s*([NSEW])?", text)
        if not match:
            return None
        result = _to_float(match.group(1))
        if result is None:
            return None
        hemisphere = match.group(2)
        if hemisphere in ("S", "W"):
            result = -abs(result)

    limit = 90 if latitude else 180
    if not math.isfinite(result) or abs(result) > limit:
        return None
    return result


def _first_value(container: dict, keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in container and container[key] not in (None, ""):
            return container[key]
    return None


def _as_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in ("1", "true", "yes", "y", "fixed", "valid", "ok", "connect"):
        return True
    if text in ("0", "false", "no", "n", "invalid", "none", "disconnected"):
        return False
    return None


def _coords_from_pair(first: Any, second: Any) -> tuple[float, float] | None:
    """Ambil pasangan [lat, lon] atau [lon, lat] berdasarkan rentang nilai."""
    a = _parse_coordinate(first, latitude=True) if abs(_to_float(first) or 0) <= 90 else None
    b = _parse_coordinate(second, latitude=False)
    if a is not None and b is not None:
        return a, b

    lon = _parse_coordinate(first, latitude=False)
    lat = _parse_coordinate(second, latitude=True)
    if lon is not None and lat is not None:
        return lat, lon
    return None


def _extract_gps(node: dict) -> dict | None:
    """Cari GPS di dalam satu objek node, apa pun nama field dari device."""
    candidates: list[Any] = []
    for key in _GPS_CONTAINER_KEYS:
        if key in node:
            candidates.append(node[key])
    candidates.append(node)

    # Field datar seperti gps_latitude / gps_lon.
    flat_lat = _first_value(node, ("gps_latitude", "gps_lat"))
    flat_lon = _first_value(node, ("gps_longitude", "gps_lon", "gps_lng"))
    if flat_lat is not None and flat_lon is not None:
        candidates.append({"latitude": flat_lat, "longitude": flat_lon})

    for candidate in candidates:
        if isinstance(candidate, str):
            pair = re.split(r"[,;\s]+", candidate.strip())
            if len(pair) >= 2:
                pair_coords = _coords_from_pair(pair[0], pair[1])
                if pair_coords:
                    return {"latitude": pair_coords[0], "longitude": pair_coords[1]}
            continue

        if isinstance(candidate, (list, tuple)) and len(candidate) >= 2:
            pair_coords = _coords_from_pair(candidate[0], candidate[1])
            if pair_coords:
                return {"latitude": pair_coords[0], "longitude": pair_coords[1]}
            continue

        if not isinstance(candidate, dict):
            continue

        lat_raw = _first_value(candidate, _LAT_KEYS)
        lon_raw = _first_value(candidate, _LON_KEYS)
        if lat_raw is None or lon_raw is None:
            # Dukung {"coordinates": [lat, lon]}.
            nested = _first_value(candidate, ("coordinates", "coord", "position"))
            if isinstance(nested, (list, tuple)) and len(nested) >= 2:
                pair_coords = _coords_from_pair(nested[0], nested[1])
                if pair_coords:
                    lat_raw, lon_raw = pair_coords
            elif isinstance(nested, dict):
                lat_raw = _first_value(nested, _LAT_KEYS)
                lon_raw = _first_value(nested, _LON_KEYS)

        if lat_raw is None or lon_raw is None:
            continue

        latitude = _parse_coordinate(lat_raw, latitude=True)
        longitude = _parse_coordinate(lon_raw, latitude=False)
        if latitude is None or longitude is None:
            continue

        gps = {
            "latitude": round(latitude, 7),
            "longitude": round(longitude, 7),
        }
        altitude = _to_float(_first_value(candidate, _ALT_KEYS))
        satellites = _to_float(_first_value(candidate, _SAT_KEYS))
        hdop = _to_float(_first_value(candidate, _HDOP_KEYS))
        fix = _as_bool(_first_value(candidate, _FIX_KEYS))

        if altitude is not None:
            gps["altitude"] = round(altitude, 2)
        if satellites is not None:
            gps["satellites"] = int(satellites)
        if hdop is not None:
            gps["hdop"] = round(hdop, 2)
        if fix is not None:
            gps["fix"] = fix
        return gps

    return None


def _canonical_node_key(key: str, index: int) -> str:
    text = str(key or "").strip().lower()
    if text in NODE_KEYS:
        return text
    match = re.search(r"([1-9]\d*)$", text)
    if match and 1 <= int(match.group(1)) <= 3:
        return f"geo_{int(match.group(1))}"
    return f"geo_{index + 1}"


_NODE_TOKEN_RE = re.compile(
    r"^(?:(?:geo|geofon|node|sensor)[-_ ]?)?([1-9]\d*)$",
    re.IGNORECASE,
)


def node_id_from_token(value: Any) -> str | None:
    """Ubah 'geo_1', 'geofon-1', 'node 1', atau '1' menjadi 'geo_1'."""
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip().lower()
    match = _NODE_TOKEN_RE.fullmatch(text)
    if not match:
        return None
    number = int(match.group(1))
    if 1 <= number <= 99:
        return f"geo_{number}"
    return None


def canonical_node_id(value: Any, index: int = 0) -> str:
    """ID node yang aman untuk database; nama tak dikenal tetap dipertahankan."""
    token = node_id_from_token(value)
    if token:
        return token
    text = re.sub(r"[^a-z0-9_-]+", "_", str(value or "").strip().lower()).strip("_")
    return text or f"node_{index + 1}"


def extract_gps_record(record: dict) -> dict | None:
    """Ambil GPS kanonik dari satu objek node."""
    if not isinstance(record, dict):
        return None
    return _extract_gps(record)


def compute_signal_stats(record: dict) -> dict:
    """
    Hitung p2p/rms/peak dari array sinyal mentah kalau field itu belum ada.

    Nilai kecil (< 10) dianggap Volt lalu dikonversi ke mV, sama seperti
    konversi yang dipakai aplikasi Processing.
    """
    if not isinstance(record, dict):
        return record

    signal = None
    for key in ("signal", "d", "data", "samples", "values", "waveform", "raw", "readings"):
        value = record.get(key)
        if isinstance(value, (list, tuple)) and value:
            signal = value
            break

    if signal is None:
        return record

    flat: list[float] = []

    def _flatten(value):
        if isinstance(value, (list, tuple)):
            for item in value:
                _flatten(item)
            return
        number = _to_float(value)
        if number is not None:
            flat.append(number)

    _flatten(signal)
    if not flat:
        return record

    # Jangan mengubah array asli yang akan disimpan; hanya skala untuk statistik.
    max_abs = max(abs(item) for item in flat)
    scaled = [item * 1000.0 for item in flat] if max_abs < 10.0 else flat

    if _to_float(record.get("p2p")) is None and _to_float(record.get("p2p_amplitude")) is None:
        record["p2p"] = round(max(scaled) - min(scaled), 4)
    if _to_float(record.get("rms")) is None:
        record["rms"] = round(
            math.sqrt(sum(item * item for item in scaled) / len(scaled)), 4
        )
    if _to_float(record.get("peak")) is None:
        record["peak"] = round(max(abs(item) for item in scaled), 4)

    if not isinstance(record.get("signal"), (list, tuple)):
        record["signal"] = list(signal) if isinstance(signal, list) else flat
    return record


def _normalize_nodes(payload: dict) -> dict | None:
    raw_nodes = payload.get("nodes")
    nodes: dict[str, dict] = {}

    if isinstance(raw_nodes, dict):
        for index, (key, value) in enumerate(raw_nodes.items()):
            if not isinstance(value, dict):
                continue
            nodes[_canonical_node_key(key, index)] = copy.deepcopy(value)
    elif isinstance(raw_nodes, list):
        for index, value in enumerate(raw_nodes):
            if not isinstance(value, dict):
                continue
            source_key = value.get("id") or value.get("node_id") or value.get("name") or index + 1
            nodes[_canonical_node_key(str(source_key), index)] = copy.deepcopy(value)

    if not nodes:
        return None

    # GPS kadang dikirim sebagai peta terpisah di level atas payload.
    for map_key in _TOP_LEVEL_GPS_MAPS:
        gps_map = payload.get(map_key)
        if not isinstance(gps_map, dict):
            continue
        for key, gps_value in gps_map.items():
            node_key = _canonical_node_key(str(key), 0)
            node = nodes.setdefault(node_key, {})
            if not isinstance(node.get("gps"), dict) and isinstance(gps_value, dict):
                # Bungkus agar _extract_gps bisa membaca latitude/longitude.
                node["gps"] = copy.deepcopy(gps_value)

    for node in nodes.values():
        gps = _extract_gps(node)
        if gps:
            node["gps"] = gps
        else:
            node.pop("gps", None)

    return nodes


def compass_label(bearing: float) -> tuple[str, str]:
    """Kembalikan (label 8 arah, singkatan) dari bearing 0-360."""
    normalized = bearing % 360
    index = int((normalized + 22.5) // 45) % 8
    return _COMPASS_8[index], _COMPASS_8_SHORT[index]


def _node_center(nodes: dict[str, dict]) -> tuple[float, float] | None:
    coords = []
    for node in nodes.values():
        gps = node.get("gps")
        if isinstance(gps, dict):
            lat = _parse_coordinate(gps.get("latitude"), latitude=True)
            lon = _parse_coordinate(gps.get("longitude"), latitude=False)
            if lat is not None and lon is not None:
                coords.append((lat, lon))
    if not coords:
        return None
    return (
        sum(item[0] for item in coords) / len(coords),
        sum(item[1] for item in coords) / len(coords),
    )


def _gps_coords_for_node(nodes: dict[str, dict], node_key: str) -> tuple[float, float] | None:
    node = nodes.get(node_key)
    gps = node.get("gps") if isinstance(node, dict) else None
    if not isinstance(gps, dict):
        return None
    lat = _parse_coordinate(gps.get("latitude"), latitude=True)
    lon = _parse_coordinate(gps.get("longitude"), latitude=False)
    if lat is None or lon is None:
        return None
    return lat, lon


def _origin_from_location(
    location: dict,
    nodes: dict[str, dict],
) -> tuple[float, float] | None:
    origin = location.get("origin") or location.get("reference")
    if isinstance(origin, dict):
        lat = _parse_coordinate(_first_value(origin, _LAT_KEYS), latitude=True)
        lon = _parse_coordinate(_first_value(origin, _LON_KEYS), latitude=False)
        if lat is not None and lon is not None:
            return lat, lon
    if isinstance(origin, str):
        coords = _gps_coords_for_node(nodes, _canonical_node_key(origin, 0))
        if coords:
            return coords

    # Processing menghitung location.x/y relatif terhadap geo_1 (0,0), bukan
    # centroid. Jadi default-kan ke GPS geo_1 kalau tersedia.
    geo1 = _gps_coords_for_node(nodes, "geo_1")
    if geo1:
        return geo1

    return _node_center(nodes)


def _offset_to_lat_lon(
    origin: tuple[float, float],
    east_meters: float,
    north_meters: float,
) -> tuple[float, float]:
    lat, lon = origin
    latitude = lat + north_meters / 111_320.0
    longitude = lon + east_meters / (111_320.0 * max(0.01, math.cos(math.radians(lat))))
    return latitude, longitude


def _normalize_location(
    payload: dict,
    nodes: dict[str, dict] | None,
) -> dict | None:
    raw_location = payload.get("location")
    if isinstance(raw_location, (list, tuple)) and len(raw_location) >= 2:
        location = {"x": raw_location[0], "y": raw_location[1]}
    elif isinstance(raw_location, dict):
        location = copy.deepcopy(raw_location)
    else:
        return None

    explicit_lat = _parse_coordinate(_first_value(location, _LAT_KEYS), latitude=True)
    explicit_lon = _parse_coordinate(_first_value(location, _LON_KEYS), latitude=False)

    if explicit_lat is not None and explicit_lon is not None:
        location["latitude"] = round(explicit_lat, 7)
        location["longitude"] = round(explicit_lon, 7)

    x = _to_float(_first_value(location, ("x", "east", "dx")))
    y = _to_float(_first_value(location, ("y", "north", "dy")))
    if x is not None:
        location["x"] = round(x, 3)
    if y is not None:
        location["y"] = round(y, 3)

    # Kalau ML sudah mengirim bearing, pakai itu. Kalau tidak, hitung dari vektor.
    bearing = None
    direction_source = None
    existing_direction = location.get("direction")
    if isinstance(existing_direction, dict):
        candidate = _to_float(_first_value(existing_direction, ("bearing_degrees", "bearing", "heading", "azimuth")))
        if candidate is not None:
            bearing = candidate
            source_value = existing_direction.get("source")
            direction_source = (
                str(source_value).strip() if source_value else "location.direction"
            )

    for key in ("bearing_degrees", "bearing", "heading", "azimuth", "direction_deg"):
        if bearing is not None:
            break
        candidate = _to_float(location.get(key))
        if candidate is not None:
            bearing = candidate
            direction_source = f"location.{key}"
            break

    if bearing is None and x is not None and y is not None and (abs(x) > 1e-9 or abs(y) > 1e-9):
        # x = timur, y = utara. atan2(east, north) menghasilkan bearing dari Utara.
        bearing = math.degrees(math.atan2(x, y))
        direction_source = "location.x_y"

    # Estimasi koordinat event tetap dihitung dari x/y + origin, walaupun
    # bearing sudah dikirim eksplisit oleh Processing.
    if (
        x is not None
        and y is not None
        and nodes
        and explicit_lat is None
        and explicit_lon is None
        and (abs(x) > 1e-9 or abs(y) > 1e-9)
    ):
        origin = _origin_from_location(location, nodes)
        if origin:
            est_lat, est_lon = _offset_to_lat_lon(origin, x, y)
            location["estimated_latitude"] = round(est_lat, 7)
            location["estimated_longitude"] = round(est_lon, 7)

    if bearing is not None:
        bearing = bearing % 360
        compass, short = compass_label(bearing)
        location["direction"] = {
            "bearing_degrees": round(bearing, 1),
            "compass": compass,
            "compass_short": short,
            "source": direction_source,
        }

    # Kalau titik event sudah punya koordinat sendiri, arahkan dari pusat node.
    if explicit_lat is not None and explicit_lon is not None and nodes:
        center = _node_center(nodes)
        if center:
            dx = (explicit_lon - center[1]) * 111_320.0 * math.cos(math.radians(center[0]))
            dy = (explicit_lat - center[0]) * 111_320.0
            if abs(dx) > 1e-9 or abs(dy) > 1e-9:
                bearing = (math.degrees(math.atan2(dx, dy)) + 360) % 360
                compass, short = compass_label(bearing)
                location["direction"] = {
                    "bearing_degrees": round(bearing, 1),
                    "compass": compass,
                    "compass_short": short,
                    "source": "location.lat_lon",
                }

    return location


def normalize_payload(payload: dict) -> dict:
    """Salin payload MQTT dan tambahkan field GPS/arah yang sudah kanonik."""
    if not isinstance(payload, dict):
        return payload

    normalized = copy.deepcopy(payload)
    nodes = _normalize_nodes(normalized)
    location = _normalize_location(normalized, nodes)

    if nodes is not None:
        normalized["nodes"] = nodes
    if location is not None:
        normalized["location"] = location
        if "direction" in location:
            normalized["direction"] = location["direction"]

    return normalized
