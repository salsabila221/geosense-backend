# Login Google + Database (Supabase) — GeoSense Backend

## Ringkasan alur

```
Frontend (React)                 Backend (FastAPI)                Database
─────────────────                ─────────────────                ────────
<GoogleLogin/>  ──credential──►  POST /api/auth/google
                                 verifikasi ID token ke Google
                                 cari user by email  ──────────►  SELECT users
   user baru?  ◄──is_new_user──  (belum ada)
   form nomor HP ──credential──► POST /api/auth/google/register
                                 INSERT user + JWT  ───────────►  INSERT users
   user lama   ◄──access_token─ (JWT)  ────────────────────────►  UPDATE last_login
```

Backend memegang `GOOGLE_CLIENT_ID` dan memverifikasi token langsung ke Google
(`google.oauth2.id_token.verify_oauth2_token`), jadi email user tidak bisa dipalsukan
dari sisi frontend.

## 1. Siapkan Google Cloud Console

1. Buka https://console.cloud.google.com/apis/credentials
2. Pilih/pakai OAuth 2.0 Client ID tipe **Web application** (yang dipakai frontend).
3. Di **Authorized JavaScript origins** tambahkan:
   - `http://localhost:5173`
   - `http://localhost:4173`
   - domain produksi frontend (mis. `https://geosense.vercel.app`)
4. **Authorized redirect URIs** tidak perlu diisi (kita pakai flow credential/ID token).
5. Pastikan Client ID di frontend (`VITE_GOOGLE_CLIENT_ID`) **sama persis** dengan
   `GOOGLE_CLIENT_ID` di backend. Kalau beda, error-nya `audience mismatch`.

## 2. Siapkan Supabase (Postgres)

1. Buat project baru di https://supabase.com
2. Buka **Project Settings → Database → Connection string → URI**
3. **Jangan pakai "Direct connection"** kalau backend-nya di Hugging Face Spaces /
   Railway / Vercel — direct connection hanya IPv6. Pakai:
   - **Session pooler** (port `5432`) → paling aman untuk SQLAlchemy, atau
   - **Transaction pooler** (port `6543`) → kode sudah otomatis pakai `NullPool`.
4. Ganti `[YOUR-PASSWORD]` dengan password database.
5. Isi di `.env` backend:

```env
DATABASE_URL=postgresql://postgres.xxxxxxxxxxxx:PASSWORD@aws-0-ap-southeast-1.pooler.supabase.com:5432/postgres
PGSSLMODE=require
```

Tabel `users` dan `system_status` dibuat otomatis saat backend start
(`Base.metadata.create_all`), tidak perlu bikin tabel manual. Kalau mau lihat isinya:
Supabase → **Table Editor → users**.

> Kalau `DATABASE_URL` dikosongkan, backend otomatis pakai SQLite lokal
> (`geosense.db`). Jadi develop di laptop tetap gampang tanpa internet.

### Tes koneksi & pindahkan data lama

```bash
python check_db.py            # cek DATABASE_URL bisa connect atau tidak
python migrate_to_supabase.py # salin user dari geosense.db -> Supabase (aman diulang)
```

`migrate_to_supabase.py` hanya menyalin user yang belum ada di target, jadi tidak
akan membuat duplikat. Untuk pakai SQLite sebagai sumber dengan nama lain:
`set SQLITE_SOURCE=namafile.db` sebelum menjalankan.

## 3. Jalankan

Backend:

```bash
cd geosense-backend
pip install -r requirements.txt
cp .env.example .env      # lalu isi nilainya
uvicorn main:app --reload --port 8000
```

Cek: http://localhost:8000/health → `database.connected: true`

Frontend:

```bash
cd geosense/Geosense-early-warning-system/frontend/geosense
npm install
cp .env.example .env
npm run dev
```

## 4. Endpoint yang dipakai

| Method | Endpoint | Auth | Keterangan |
|---|---|---|---|
| POST | `/api/auth/google` | – | Verifikasi ID token Google, login / deteksi user baru |
| POST | `/api/auth/google/register` | – | Onboarding user baru (verifikasi token ulang, simpan `fullname` + `phone_number` WhatsApp) |
| GET | `/api/auth/me` | Bearer | Profil user dari token JWT |
| GET | `/api/admin/users` | Bearer admin | Daftar semua user di database |
| GET | `/api/system/status` | – | Status sistem terakhir |
| GET | `/api/system/history?limit=50&offset=0&source=ml` | Bearer admin | Riwayat perubahan status (terbaru dulu) |
| POST | `/api/admin/override?status=...` | Bearer admin | Emergency override (butuh role admin) |
| GET | `/api/admin/wa/config` | Bearer admin | Status integrasi Fonnte + jumlah penerima |
| POST | `/api/admin/wa/test?phone=08...` | Bearer admin | Kirim pesan WhatsApp uji |
| GET | `/api/admin/wa/logs?limit=20` | Bearer admin | Log pengiriman WhatsApp |
| GET | `/api/push/public-key` | – | Kunci publik VAPID untuk Web Push |
| POST | `/api/push/subscribe` | opsional Bearer | Simpan/perbarui langganan push satu perangkat |
| POST | `/api/push/unsubscribe` | – | Hapus langganan push |
| GET | `/api/admin/push/stats` | Bearer admin | Jumlah perangkat berlangganan push |
| POST | `/api/admin/push/test` | Bearer admin | Kirim notifikasi browser uji ke semua perangkat |

## 5. Riwayat perubahan status

Ada dua tabel yang berbeda perannya:

| Tabel | Isi |
|---|---|
| `system_status` | **Status terkini** saja (1 baris, `id = 1`). Cepat dibaca untuk dashboard. |
| `status_history` | **Log append-only**. Setiap perubahan status menambah baris baru, tidak pernah menimpa yang lama. |

Kapan baris baru dibuat:

- **Dari ML (MQTT)** → hanya kalau status hasil model **berbeda** dari status terakhir.
  Payload `kondisi_tanah`, `aktivitas`, `p2p_amplitude`, `rms` ikut disimpan.
  Pesan MQTT yang statusnya sama tidak menambah baris (biar tidak spam).
- **Override admin** → selalu dicatat sebagai aksi admin (`changed_by` = email admin),
  termasuk kalau statusnya sama dengan sebelumnya.
- **Saat pertama kali** → satu baris `source = "system"` sebagai titik awal.

Nilai `source`: `ml` | `admin` | `system`.

Cek isinya:

```sql
select id, status, source, changed_by, note, created_at
from status_history
order by created_at desc
limit 20;
```

Atau lewat API (butuh token admin): `GET /api/system/history?limit=20`.

## 6. Notifikasi WhatsApp (Fonnte)

**Kenapa di backend, bukan frontend?** Token Fonnte itu seperti password nomor WA kamu —
siapa pun yang punya token bisa mengirim pesan dari nomor tersebut. Kalau ditaruh di
frontend (`VITE_...`), token ikut terkirim ke browser dan bisa dicuri. Jadi semua
pengiriman dilakukan server.

### 6.1 Ambil token Fonnte

1. Daftar / login di https://fonnte.com
2. Menu **Device → Add Device**, lalu scan QR dengan WhatsApp nomor pengirim
3. Pastikan status device **Connected**
4. Copy **Token** di menu Device

### 6.2 Taruh di `.env` backend

```env
WA_ENABLED=1
FONNTE_TOKEN=token-dari-fonnte
WA_NOTIFY_STATUSES=SIAGA,WASPADA,WARNING,BAHAYA
WA_COOLDOWN_SECONDS=300
APP_URL=https://dashboard-kamu.vercel.app
```

Restart backend. Kalau `FONNTE_TOKEN` kosong, fitur WA otomatis mati dan aplikasi tetap
jalan normal (hanya mencatat di log bahwa notifikasi dilewati).

### 6.3 Kapan pesan dikirim

| Kejadian | Kirim WA? |
|---|---|
| ML mengubah status → `SIAGA` / `WASPADA` / `WARNING` | ✅ ke semua user aktif yang punya nomor |
| Admin override ke `SIAGA` / `WASPADA` | ✅ |
| Admin override ke `AMAN` | ❌ tidak perlu |
| Status sama berulang dalam `WA_COOLDOWN_SECONDS` | ❌ dilewati (anti-spam) |
| Status berubah tapi bukan status waspada | ❌ |

Pengiriman berjalan di **background thread**, jadi MQTT dan API tetap responsif.
Setiap percobaan dicatat di tabel `notification_log` (berhasil/gagal + respons Fonnte).

### 6.4 Tes

Paling gampang: login sebagai admin → section **Notifikasi WhatsApp (Fonnte)** di
dashboard → isi nomor → **Kirim Tes**.

Atau lewat Swagger http://localhost:8000/docs → `POST /api/admin/wa/test?phone=08123456789`
(klik Authorize dulu, masukkan token JWT dari login Google).

### 6.5 Troubleshooting Fonnte

| Respons / gejala | Arti | Solusi |
|---|---|---|
| `device disconnected` | HP pengirim offline / logout | Buka WhatsApp di HP, scan ulang QR di Fonnte |
| `token invalid` | Token salah/terhapus | Copy ulang token dari menu Device |
| `insufficient quota` | Kuota pesan habis | Top up di dashboard Fonnte |
| Pesan tidak masuk | Nomor tujuan tidak aktif di WA | Cek nomor, format `08xx` atau `628xx` |
| Log "Diterima" tapi WA tidak masuk | **WhatsApp reach-out time-lock**: nomor device masih baru, penerima belum pernah chat ke device itu. Fonnte tetap balas `message in queue`, tapi WhatsApp menahan pesannya (state `0`) | Minta penerima kirim **satu pesan apa saja** ke nomor device Fonnte. Setelah itu notifikasi akan masuk normal. Nomor device baru selalu kena ini. |
| `502 Fonnte gagal mengirim` | Ditolak API | Lihat isi respons di tabel `notification_log` |

> **Catatan penting:** kolom `success` di `notification_log` artinya **Fonnte sudah menerima**
> pesan (status `message in queue`), **bukan** jaminan sudah sampai ke WhatsApp. Status
> pengiriman sebenarnya ada di dashboard Fonnte menu **Pesan** (Sent / Pending / state 0),
> atau perlu webhook status. Kalau `success = true` tapi pesan tidak masuk, hampir selalu
> karena time-lock WhatsApp di atas.

## 7. PWA & Notifikasi Browser (Web Push)

Frontend sekarang bisa dipasang sebagai aplikasi (PWA) dan menerima notifikasi
langsung di perangkat. Notifikasi berjalan **dua jalur**:

| Jalur | Kapan | Butuh aplikasi terbuka? |
|---|---|---|
| WhatsApp (Fonnte) | Status berubah ke SIAGA/WASPADA | Tidak |
| Web Push (PWA) | Status berubah ke SIAGA/WASPADA | Tidak (selama browser mengizinkan) |

### 7.1 Bagian frontend

| File | Fungsi |
|---|---|
| `public/manifest.webmanifest` | Nama aplikasi, ikon, warna tema, shortcut Panduan/Darurat |
| `public/sw.js` | Service worker: cache app shell + terima event `push` + klik notifikasi |
| `public/icons/` | Ikon 192/512, maskable, Apple Touch, badge notifikasi |
| `src/lib/push.js` | Minta izin, `PushManager.subscribe()`, kirim langganan ke backend |
| `src/hooks/usePWA.js` | Deteksi `beforeinstallprompt`, status installed, iOS |
| `src/components/pwa/InstallPrompt.jsx` | Banner "Pasang GeoSense" + petunjuk iOS |
| `src/components/pwa/BrowserNotifications.jsx` | Kartu aktifkan/matikan/tes notifikasi |

Service worker didaftarkan di `src/main.jsx` saat halaman selesai dimuat.

Cara pasang:

- **Android / Chrome / Edge desktop**: tombol **Install** muncul di banner, atau
  menu browser → **Install app / Add to Home screen**.
- **iPhone / iPad (Safari)**: menu **Bagikan** → **Tambahkan ke Layar Utama**.
  Web Push di iOS baru aktif setelah PWA dipasang dan memakai iOS 16.4+.

### 7.2 Konfigurasi backend

Tambahkan di `.env` backend:

```env
PUSH_ENABLED=1
VAPID_PUBLIC_KEY=<public-key-base64url>
VAPID_PRIVATE_KEY_PATH=vapid_private.pem
VAPID_SUBJECT=mailto:emailkamu@gmail.com
PUSH_NOTIFY_STATUSES=SIAGA,WASPADA,WARNING,BAHAYA
PUSH_COOLDOWN_SECONDS=300
```

- `VAPID_PRIVATE_KEY_PATH` boleh relatif terhadap folder backend atau absolut.
- File privat **tidak boleh di-commit** (sudah masuk `.gitignore`).
- Kalau file privat hilang / mau bikin kunci baru:

```bash
cd geosense-backend
python generate_vapid.py
```

Script akan membuat `vapid_private.pem` dan mencetak `VAPID_PUBLIC_KEY` untuk
ditempel ke `.env`. Semua perangkat harus berlangganan ulang setelah kunci
diganti (buka dashboard → **Matikan** → **Aktifkan notifikasi**).

### 7.3 Kapan notifikasi browser dikirim

Sama seperti WhatsApp: hanya saat status berubah ke `SIAGA`, `WASPADA`,
`WARNING`, atau `BAHAYA`. Status `AMAN` tidak mengirim notifikasi. Cooldown
mencegah spam kalau status yang sama berulang.

Setiap perangkat yang menekan **Aktifkan notifikasi** disimpan di tabel
`push_subscriptions` (endpoint + kunci enkripsi + email kalau sedang login).
Endpoint yang sudah kedaluwarsa (HTTP 404/410) otomatis dihapus.

### 7.4 Tes

1. Buka dashboard frontend.
2. Pada kartu **Notifikasi Browser**, klik **Aktifkan notifikasi** lalu izinkan.
3. Klik **Tes notifikasi** → notifikasi harus muncul di pojok perangkat.
4. Admin juga bisa memakai Swagger:
   `POST /api/admin/push/test` (Authorize dengan token JWT admin).

### 7.5 Troubleshooting Web Push

| Gejala | Penyebab | Solusi |
|---|---|---|
| Tombol aktif tidak muncul / disabled | Browser tidak mendukung, atau izin diblokir | Pakai Chrome/Edge/Safari terbaru; buka pengaturan situs → izinkan notifikasi |
| Notifikasi tidak muncul di iPhone | Belum dipasang ke Home Screen atau iOS < 16.4 | Pasang lewat menu Bagikan Safari |
| `Web Push belum tersedia` | `pywebpush` belum terpasang | `pip install -r requirements.txt` lalu restart backend |
| `enabled: false` di `/api/push/public-key` | VAPID belum lengkap / file PEM tidak ada | Cek `PUSH_ENABLED`, `VAPID_PUBLIC_KEY`, dan `VAPID_PRIVATE_KEY_PATH` |
| `total` bertambah tapi notifikasi tidak masuk | Izin notifikasi perangkat mati / subscription kedaluwarsa | Matikan lalu aktifkan ulang; cek pengaturan notifikasi OS |
| Tidak bisa install | Bukan HTTPS/localhost, atau manifest/ikon gagal dimuat | Cek DevTools → Application → Manifest; produksi wajib HTTPS |
| Setelah deploy masih tampil versi lama | Service worker menyajikan cache lama | Reload paksa / tutup semua tab PWA; naikkan `CACHE_NAME` di `sw.js` |

> **Penting:** Web Push hanya berjalan di **HTTPS** atau **localhost**. Kalau
> frontend diakses lewat IP LAN (`http://192.168.x.x`), notifikasi dan install
> prompt tidak akan aktif.

## 8. Jadi admin

```env
ADMIN_EMAILS=emailkamu@gmail.com,timlain@gmail.com
```

Email di daftar itu otomatis dapat `role = "admin"` setiap kali login Google.
Setelah itu, tombol override di dashboard akan diizinkan backend (endpoint
`/api/admin/override` sekarang menolak non-admin).

## 9. Troubleshooting

| Gejala | Penyebab | Solusi |
|---|---|---|
| `422 credential wajib diisi` | Frontend kirim `{token}` / kosong | Sudah diperbaiki; frontend kirim `{credential}` |
| `401 Token Google tidak valid: audience mismatch` | Client ID FE ≠ BE | Samakan `VITE_GOOGLE_CLIENT_ID` dan `GOOGLE_CLIENT_ID` |
| `500 GOOGLE_CLIENT_ID belum diset` | `.env` tidak terbaca | Pastikan file `.env` ada di folder `geosense-backend` |
| `401 Butuh login` saat override | Belum login / token hilang | Login Google dulu; token disimpan di `localStorage` |
| `403 Akses khusus admin` | Role masih `user` | Tambahkan email ke `ADMIN_EMAILS`, login ulang |
| `connection refused` saat frontend | Base URL salah (dulu port 5000) | Set `VITE_API_URL=http://localhost:8000` |
| Data tidak masuk Supabase | Masih pakai SQLite | Cek `/health` → `dialect` harus `postgresql` |
| `SSL connection is required` | `sslmode` tidak ada | Tambahkan `?sslmode=require` atau `PGSSLMODE=require` |
| Backend mati, frontend bilang "gagal terhubung" | `UnicodeEncodeError` saat print emoji di Windows (cp1252) | Sudah diperbaiki: stdout dipaksa UTF-8 di `main.py` |
| `FATAL: password authentication failed` | Password Supabase salah / sudah di-reset / karakter spesial tidak di-encode | Reset password di Supabase, URL-encode (`$`→`%24`, `!`→`%21`, `@`→`%40`, `#`→`%23`), cek `python check_db.py` |
| `server refused connection` di log | `DATABASE_URL` salah | Pastikan username `postgres.<project-ref>` dan pakai pooler host |

Diagnosa cepat koneksi DB tanpa menyalakan server:

```bash
cd geosense-backend
python check_db.py
```

## 10. Catatan keamanan

- `.env` sudah masuk `.gitignore` — **jangan commit** `DATABASE_URL`, `JWT_SECRET`, maupun `FONNTE_TOKEN`.
- Token Fonnte jangan pernah ditaruh di frontend / dikirim ke browser. Kalau token bocor,
  orang lain bisa mengirim WA dari nomor kamu — langsung hapus/regenerate di dashboard Fonnte.
- Ganti `JWT_SECRET` di produksi: `python -c "import secrets; print(secrets.token_hex(32))"`.
- Login manual email/password di halaman login **masih dummy** (belum ada password
  di database). Untuk sekarang hanya Google login yang benar-benar tervalidasi.
  Kalau nanti mau login manual juga, tambahkan kolom `password_hash` + `passlib[bcrypt]`.
- `phone_number` saat ini disimpan sebagai teks biasa. Kalau perlu, nanti bisa
  ditambah validasi format nomor Indonesia.
