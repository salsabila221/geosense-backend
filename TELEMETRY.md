# Format Telemetri GPS + Geofon

> **Protokol baru (event waveform):** node STM32 sekarang mengirim satu event
> (`EVENT_META` → `DATA` → `EVENT_END`) berisi waveform denoised dalam ADC
> counts, bukan ringkasan RMS per paket. Backend mendukung event utuh maupun
> frame terpisah. Lihat [`EVENT_WAVEFORM.md`](EVENT_WAVEFORM.md) dan
> `C:\geosense-processing\LORA_PROTOCOL.md`.

## Format asli firmware (dipakai sekarang)

Firmware mengirim dua payload terpisah, masing-masing satu node per pesan.
Kunci `n` berisi nama node (`NODE01`, `NODE02`, `NODE03`) dan otomatis
dinormalisasi menjadi `geo_1`, `geo_2`, `geo_3`.

Geofon (`rawdata`):

```json
{"n":"NODE01","p":27,"t":50843,"d":[0.00012,0.00143,37.97,0.00034,0.00000,0.00096,0.00000]}
```

| Field | Arti |
|---|---|
| `n` | ID node (`NODE01` → `geo_1`) |
| `p` | Nomor urut paket dari device |
| `t` | Uptime device dalam milidetik (bukan epoch) |
| `d` | Sampel getaran; backend menghitung `p2p`, `rms`, `peak`, dan `signal` |

GPS (`gpsdata`):

```json
{"n":"NODE01","type":"gps","lat":-6.900000,"lon":107.600000,"sat":8,"hdop":1.2,"utc":"2026-10-23T04:30:00Z","uptime_ms":60000}
```

| Field | Arti |
|---|---|
| `n` | ID node (`NODE01` → `geo_1`) |
| `type` | `gps`, dipakai sebagai penentu tipe kalau topik tidak dikenali |
| `lat` / `lon` | Koordinat desimal derajat |
| `sat` | Jumlah satelit |
| `hdop` | Akurasi horizontal |
| `utc` | Waktu GPS; dipakai sebagai `source_timestamp` |
| `uptime_ms` | Uptime device dalam milidetik |

Backend menggabungkan keduanya per node: nilai `p2p`/`rms`/`signal` dari
`rawdata` dan `gps` dari `gpsdata` akan tampil bersama di dashboard.

## Topik MQTT

Backend subscribe tiga topik:

| Topik | Isi |
|---|---|
| `rawdata` | Event waveform (`EVENT_META`/`DATA`/`EVENT_END`) atau data geofon lama: `p2p`, `rms`, `signal` |
| `gpsdata` | GPS per node: `latitude`, `longitude`, `altitude`, `satellites`, `hdop`, `fix` |
| `geosense/data` | Hasil processing: TDOA (`location.x/y`), statistik node, aktivitas |

```env
MQTT_TOPIC=rawdata,gpsdata,geosense/data
```

Pisahkan topik dengan koma. `rawdata`, `gpsdata`, dan hasil processing boleh
datang kapan saja; backend menyimpan state terakhir per node lalu
menggabungkannya sebelum mengirim `geosense_update` ke dashboard. Jadi GPS dan
hasil TDOA tidak harus berada dalam pesan yang sama dengan waveform.

## Contoh pesan `gpsdata`

```json
{
  "node": "geo_1",
  "gps": {
    "latitude": -6.9024,
    "longitude": 107.6188,
    "altitude": 850.0,
    "satellites": 8,
    "hdop": 0.9,
    "fix": true
  }
}
```

Format peta per node juga didukung:

```json
{
  "gps": {
    "geo_1": { "latitude": -6.9024, "longitude": 107.6188 },
    "geo_2": { "latitude": -6.9020, "longitude": 107.6192 },
    "geo_3": { "latitude": -6.9028, "longitude": 107.6190 }
  }
}
```

## Contoh pesan `rawdata`

```json
{
  "status": "AMAN",
  "timestamp": 1760000000,
  "kondisi_tanah": "Tanah stabil",
  "aktivitas": "Tidak ada getaran signifikan",
  "p2p_amplitude": 1.1,
  "rms": 0.39,
  "nodes": {
    "geo_1": { "online": true, "p2p": 1.1, "rms": 0.4, "signal": [0.1, 0.2, 0.3] },
    "geo_2": { "online": true, "p2p": 0.8, "rms": 0.3 },
    "geo_3": { "online": true, "p2p": 0.9, "rms": 0.35 }
  },
  "location": { "x": 120.0, "y": 80.0 }
}
```

Payload satu node juga diterima, misalnya
`{"sensor_id": "geo_2", "d": [0.0123, 0.0125, 0.0121]}`. Backend menghitung
`p2p`, `rms`, dan `peak` dari array `d`/`signal` bila field itu belum dikirim.

## Penyimpanan Supabase

Semua pesan MQTT disimpan, termasuk payload yang bukan JSON:

| Tabel | Isi |
|---|---|
| `mqtt_messages` | Payload mentah (`payload_text`) + JSON hasil parse |
| `node_telemetry` | Satu baris per node: geofon, GPS, sinyal, status |
| `event_locations` | `location.x/y`, bearing, arah, estimasi koordinat |

Tabel dibuat otomatis saat backend start (`Base.metadata.create_all`).
Pesan tetap disimpan walau status tidak berubah atau format JSON berubah.

## Format payload gabungan (kompatibilitas)

Backend juga masih menerima GPS di dalam payload yang sama dengan geofon. GPS
melekat pada masing-masing node, jadi tempat paling natural adalah di dalam
objek node:

```json
{
  "status": "AMAN",
  "timestamp": 1760000000,
  "kondisi_tanah": "Tanah stabil",
  "aktivitas": "Tidak ada getaran signifikan",
  "p2p_amplitude": 1.1,
  "rms": 0.39,
  "sampling_rate": 90,
  "nodes": {
    "geo_1": {
      "online": true,
      "p2p": 1.1,
      "rms": 0.4,
      "peak": 1.8,
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
  "location": {
    "x": 120.0,
    "y": 80.0
  }
}
```

## Kenapa topik dipisah?

- Node bisa mengirim GPS lebih jarang daripada data getaran.
- Payload geofon tidak perlu diubah hanya untuk menambahkan GPS.
- Backend tetap menggabungkan keduanya per node sebelum diteruskan ke dashboard.
- Data mentah dari kedua topik tetap tersimpan di Supabase.

## Field yang diterima backend

Backend menormalkan beberapa variasi nama field:

- GPS di dalam `node.gps` / `node.gnss` / `node.position`
- GPS langsung di node: `latitude`/`longitude`, `lat`/`lon`/`lng`
- Jumlah satelit: `satellites`, `sats`, `sat`, `num_sats`
- Timestamp GPS: `utc` (ISO 8601) atau epoch (`timestamp`/`ts`)
- ID node singkat: `n` (`NODE01` → `geo_1`)
- `gps_latitude`/`gps_longitude`
- Peta GPS terpisah di level atas: `gps`, `node_gps`, `gps_data`, `gps_nodes`
- Koordinat berupa angka, string desimal, atau format `6.9 S` / `107.6 E`

Setelah dinormalisasi, frontend selalu menerima:

```json
"gps": {
  "latitude": -6.9024,
  "longitude": 107.6188,
  "satellites": 8,
  "hdop": 0.9,
  "fix": true
}
```

## Arah longsor

Arah dihitung dari hasil triangulasi ML:

```json
"location": { "x": 120.0, "y": 80.0 }
```

Konvensi yang dipakai: `x` = meter ke timur, `y` = meter ke utara. Bearing
dihitung dengan `atan2(x, y)`, sehingga:

- `0°` = Utara
- `90°` = Timur
- `180°` = Selatan
- `270°` = Barat

Backend menambahkan:

```json
"direction": {
  "bearing_degrees": 56.3,
  "compass": "Timur Laut",
  "compass_short": "TL",
  "source": "location.x_y"
}
```

`location.x` dan `location.y` dari Processing dihitung relatif terhadap
`geo_1` (0,0). Backend memakai GPS `geo_1` sebagai origin default. Kalau ingin
origin lain, kirim salah satu field berikut di `location`:

```json
"location": {
  "x": 120.0,
  "y": 80.0,
  "origin": { "latitude": -6.9207, "longitude": 107.6151 }
}
```

atau:

```json
"location": {
  "x": 120.0,
  "y": 80.0,
  "origin": "geo_1"
}
```

Kalau ML sudah punya bearing sendiri, kirim `location.bearing_degrees` dan
backend/frontend akan memakainya langsung.
