# Event Waveform (protokol LoRa baru)

Dokumen ini menjelaskan cara backend menerima **satu event getaran** dari node
STM32. Node tidak lagi mengirim RMS/peak frequency sebagai data utama, tetapi
merekam cuplikan waveform, menyimpannya ke SD, lalu mengirim salinan yang sudah
didahului denoising wavelet Haar level 5.

Alur data:

```text
Node STM32
  └─ LoRa: EVENT_META → DATA (beberapa frame) → EVENT_END   (biner + CRC-16)
       └─ EBYTE radio di Jetson (USB-UART), ACK per frame
            └─ MQTT HiveMQ (JSON)
                 └─ Backend FastAPI: rangkai event, hitung statistik, simpan
                      └─ Socket.IO geosense_update → dashboard
```

## Link node → Jetson (biner)

Yang diterima Jetson langsung dari LoRa adalah **frame biner v2** (header 25
byte + payload + CRC-16), bukan JSON. Receiver di Jetson sudah diimplementasikan
di `C:\geosense-processing\lora_receiver.py`:

1. membaca byte dari radio EBYTE lewat USB-UART (9600 8N1),
2. memvalidasi header/versi dan CRC-16 setiap frame,
3. mengirim **ACK** untuk `EVENT_META`, setiap `DATA`, dan `EVENT_END`,
4. menyusun ulang sampel berdasarkan `first_sample_index`,
5. publish JSON event ke MQTT `rawdata` dan GPS JSON ke `gpsdata`.

Layout frame, CRC, ACK, dan encoding sampel didokumentasikan di
`C:\geosense-processing\LORA_PROTOCOL.md`.

```powershell
cd C:\geosense-processing
python lora_receiver.py --port COM5 --baud 9600
```

Backend tetap menerima **JSON** di MQTT, jadi tidak perlu tahu detail radio.

> Firmware node **V2** memakai wire protocol yang sama dengan V1 (header 25
> byte, CRC, META/DATA/END/ACK). Perubahan V2 ada di sisi radio (TX UART
> langsung + AUX, timeout ACK 5 s) dan threshold trigger, bukan format frame.
> Untuk ESP32 receiver mode bridge, lihat `LORA_PROTOCOL.md`.

## TDOA event mode

`C:\geosense-processing\event_tdoa.py` berlangganan event dari `rawdata`,
memasangkan event 3 node berdasarkan UTC (toleransi dapat diatur), menghitung
TDOA dengan cross-correlation, lalu publish hasil ke `geosense/data`:

- `location.x` / `location.y` (meter, relatif geo_1)
- `location.origin` (GPS geo_1 bila tersedia)
- `location.tdoa` (delay, quality, residual, event IDs)
- statistik node (`p2p`, `rms`, `peak`, `unit`)

Backend subscribe `geosense/data` sehingga arah longsor dan estimasi koordinat
masuk ke dashboard. Jalankan:

```powershell
cd C:\geosense-processing
python event_tdoa.py
```

Konfigurasi penting (env): `WAVE_SPEED`, `GEOPHONE_POS_JSON`,
`TDOA_PAIR_TOLERANCE_S`, `TDOA_WINDOW_SAMPLES`, `TDOA_MIN_QUALITY`.

## Bentuk pesan MQTT yang diterima backend

Receiver boleh mengirim **event utuh** (disarankan, paling sederhana) atau
**frame terpisah** yang dirangkai backend.

### 1. Event utuh (satu pesan)

```json
{
  "type": "event",
  "n": "NODE02",
  "event_id": 17,
  "utc": "2026-10-23T10:15:30.250Z",
  "utc_valid": true,
  "sample_count": 900,
  "trigger_index": 180,
  "sample_period_us": 11111,
  "wavelet": { "name": "haar", "level": 5, "denoised": true },
  "samples": [12015, 11984, 12021, 11990, 12008]
}
```

### 2. Frame terpisah

```json
{ "type": "EVENT_META", "n": "NODE02", "event_id": 17,
  "utc": "2026-10-23T10:15:30.250Z", "utc_valid": true,
  "sample_count": 900, "trigger_index": 180,
  "sample_period_us": 11111,
  "wavelet": { "name": "haar", "level": 5 } }
```

```json
{ "type": "DATA", "n": "NODE02", "event_id": 17,
  "seq": 0, "first_index": 0,
  "samples": [12015, 11984, 12021, 11990] }
```

```json
{ "type": "EVENT_END", "n": "NODE02", "event_id": 17, "total_packets": 12 }
```

Backend merangkai frame berdasarkan `(node, event_id)` dan `first_index`
(atau urutan frame kalau `first_index` tidak ada). Event diselesaikan saat
`EVENT_END` diterima, atau saat jumlah sampel sudah mencapai `sample_count`.

### Alias field yang diterima

| Data | Nama field yang diterima |
|---|---|
| Tipe frame | `frame_type`, `packet_type`, `kind`, `type`, `frame` |
| Node | `n`, `node`, `node_id`, `sensor_id`, `device_id` |
| ID event | `event_id`, `event`, `eid`, `id_event`, `eventid` |
| Sampel | `samples`, `d`, `waveform`, `values`, `data` |
| Indeks awal | `first_index`, `start_index`, `sample_start`, `sample_index`, `idx`, `offset` |
| Nomor urut | `seq`, `sequence`, `packet`, `packet_seq`, `frame_index`, `p` |
| Total sampel | `sample_count`, `total_samples`, `samples_count`, `n_samples` |
| Trigger | `trigger_index`, `trigger`, `trigger_pos` |
| UTC | `utc`, `gps_utc`, `event_utc`, `time_utc` |
| Validitas UTC | `utc_valid`, `gps_valid`, `time_valid` |
| Periode sampel | `sample_period_us`, `period_us`, `dt_us`, `sampling_period_us` |
| Sampling rate | `sampling_rate`, `sample_rate`, `fs`, `sps` |
| Wavelet | `wavelet` (`{name, level}`), `wavelet_name`, `wavelet_level` |
| Unit | `unit`, `sample_unit` |

Nama node `NODE01`/`NODE02`/`NODE03` otomatis menjadi `geo_1`/`geo_2`/`geo_3`.

## Yang dilakukan backend

1. Menyimpan **setiap pesan MQTT mentah** di `mqtt_messages`
   (`message_type = "event"`), termasuk frame yang belum lengkap.
2. Merangkai frame menjadi satu waveform.
3. Menghitung `p2p`, `rms`, `peak`, `mean` dari nilai sampel.
4. Menyimpan satu baris `node_telemetry` dengan `message_type = "event"`:
   `signal` berisi waveform, `raw_data` berisi metadata event.
5. Mengirim `geosense_update` ke dashboard dengan:
   - `update_type`: `"event"`
   - `nodes.<node>.signal`: waveform (denoised)
   - `nodes.<node>.unit`: `"adc_counts"`
   - `nodes.<node>.event_id`, `event_utc`, `utc_valid`, `trigger_index`,
     `sample_count`, `expected_samples`, `missing_samples`, `complete`,
     `sample_period_us`, `wavelet`
   - `sampling_rate` di level atas (dihitung dari `sample_period_us`)

Endpoint verifikasi (admin):

```text
GET /api/admin/events?limit=20&include_signal=false
GET /api/admin/telemetry/messages?message_type=event&include_payload=true
```

## Penting: satuan ADC counts

Nilai waveform masih **ADC counts**, bukan volt. Backend **tidak** mengalikan
dengan 1000 seperti data lama. Dashboard harus membaca `unit` dan menampilkan
`ADC counts` selama belum ada kalibrasi.

Supaya bisa kembali ke satuan mV/volt, perlu diketahui minimal:

- resolusi ADC (mis. 12-bit),
- referensi tegangan ADC (mis. 3.3 V),
- penguatan/atenuasi rangkaian geophone,
- sensitivitas geophone (V/m/s) bila ingin satuan fisik.

Tanpa itu, threshold status tidak boleh memakai angka mV lama
(`THRESHOLD_SIAGA = 150 mV`, `THRESHOLD_WASPADA = 400 mV`).

## Frame biner langsung ke MQTT?

Backend tetap menerima **JSON**. Kalau suatu saat frame LoRa diteruskan apa
adanya ke MQTT, backend belum men-decode-nya; lakukan decode + CRC + ACK di
receiver (seperti `lora_receiver.py`) lalu publish JSON seperti di atas.

## Dampak ke `geosense-processing`

Processing lama mengasumsikan:

- stream kontinu 90 SPS,
- window 256 sampel,
- nilai dalam volt/mV,
- RMS/peak frequency sebagai fitur.

Dengan protokol event, processing perlu diubah menjadi:

1. Mengumpulkan event per node (`event_id`, `utc`, `trigger_index`, waveform).
2. Memasangkan event dari 3 node berdasarkan UTC/trigger (UTC hanya perkiraan,
   bukan PPS, jadi perlu toleransi waktu).
3. Menjalankan TDOA pada waveform event yang sudah dipasangkan, bukan pada
   buffer kontinu.
4. Melatih ulang model ML dengan data ADC counts + denoising Haar level 5,
   atau menonaktifkan prediksi ML sampai dataset baru tersedia.

Langkah 1–3 sudah diimplementasikan di `event_tdoa.py`. Langkah 4 (ML) belum;
`main_processor.py` masih memakai model lama dan mengabaikan payload event.
