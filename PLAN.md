# PLAN.md — Distributed Number Processing (2 PC)

Dokumen ini adalah rencana kerja untuk mengerjakan **Tugas 1: Implementasi Distributed Memory Menggunakan 2 PC** dengan tema **Distributed Number Processing**, menggunakan Python murni (`socket`, tanpa MPI) dengan 2 peran: **PC1 = Master/Coordinator**, **PC2 = Worker/Processor**.

Deadline tugas: **12 Oktober 2026, 23:55**.

---

## 1. Ringkasan Proyek

Aplikasi akan memproses sekumpulan angka secara terdistribusi:

1. **PC1 (Master)** membangkitkan (atau membaca) daftar angka.
2. Daftar angka dibagi menjadi 2 bagian.
3. Bagian 1 diproses langsung di PC1, bagian 2 dikirim via LAN (TCP socket) ke **PC2 (Worker)**.
4. PC1 dan PC2 masing-masing menghitung hasil **parsial** dari porsi datanya: `sum`, `count`, `min`, `max`.
5. PC2 mengirim hasil parsialnya kembali ke PC1.
6. PC1 **menggabungkan** hasil parsial PC1 + PC2 menjadi **hasil akhir**:
   - `total_sum = sum_pc1 + sum_pc2`
   - `total_count = count_pc1 + count_pc2`
   - `average = total_sum / total_count`
   - `min = min(min_pc1, min_pc2)`, `max = max(max_pc1, max_pc2)`
7. Menampilkan semua informasi wajib (jumlah data, beban per PC, hasil per PC, hasil akhir, waktu eksekusi).
8. Dibandingkan dengan **mode sequential** (1 PC, tanpa jaringan) untuk menghitung **Speedup** dan **Efficiency**.

> Tema ini sengaja dipilih karena **penggabungan hasilnya sederhana** (sum, count, min, max tinggal digabung langsung) — tanpa perlu algoritma statistik gabungan yang rumit (seperti parallel variance). Ini memudahkan implementasi, debugging, dan penjelasan di laporan, tanpa mengurangi pemenuhan ketentuan tugas (split data, kirim-terima via jaringan, komputasi di 2 PC, merge hasil).

---

## 2. Tujuan & Deliverables

| # | Deliverable | Keterangan |
|---|---|---|
| 1 | Source code aplikasi | Python, rapi, modular, ada unit test |
| 2 | Laporan (PDF, maks 10 halaman) | Dibuat terpisah mengikuti `docs/REPORT_OUTLINE.md` |
| 3 | Video demo (MP4, maks 10 menit) | Rekaman PC1 & PC2 berkomunikasi + hasil |
| 4 | **Dokumentasi teknis lengkap** | File `docs/DOCUMENTATION.md` — wajib dibuat **setelah** implementasi selesai, berisi penjelasan lengkap seluruh struktur & kode (lihat §9) |

> Catatan: PLAN.md & AGENTS.md ini bukan laporan tugas — ini adalah dokumen kerja internal (instruksi pengerjaan untuk developer/AI). Laporan PDF tetap harus dibuat terpisah sesuai ketentuan dosen.

---

## 3. Arsitektur Sistem

```
                      LAN (TCP Socket, port default 5050)
   ┌─────────────────────┐                         ┌─────────────────────┐
   │   PC1 — MASTER        │ <---------------------> │  PC2 — WORKER        │
   │ (Coordinator)          │        JSON protocol     │ (Processor)           │
   │                        │                         │                       │
   │ 1. Generate/load angka │                         │                       │
   │ 2. Split jadi 2 bagian │                         │                       │
   │ 3. Hitung sum/count/   │                         │                       │
   │    min/max porsi PC1   │                         │                       │
   │ 4. Kirim porsi ke PC2  │ ───── data chunk ─────> │ 5. Terima data         │
   │                        │                         │ 6. Hitung sum/count/   │
   │                        │                         │    min/max porsi PC2   │
   │ 8. Terima hasil PC2    │ <──── hasil parsial ──  │ 7. Kirim hasil ke PC1  │
   │ 9. Gabung hasil        │                         │                       │
   │    (PC1 + PC2)         │                         │                       │
   │ 10. Tampilkan hasil akhir│                       │                       │
   └─────────────────────┘                         └─────────────────────┘
```

- **Transport**: TCP socket (`socket` standar Python), bukan MPI — lebih mudah dijelaskan & didemokan di 2 PC nyata.
- **Format pesan**: JSON, dengan *length-prefixed framing* (4 byte header panjang pesan + payload) supaya aman untuk pesan besar.
- **Konfigurasi jaringan**: IP & port PC2 disetel lewat file konfigurasi/`argparse`, **tidak di-hardcode**.

---

## 4. Struktur Direktori Project

```
distributed-number-processing/
├── PLAN.md
├── AGENTS.md
├── README.md
├── requirements.txt
├── config/
│   └── config.yaml            # IP, port, ukuran data default, dll
├── src/
│   └── dnp/
│       ├── __init__.py
│       ├── protocol.py        # framing + (de)serialisasi pesan JSON
│       ├── number_processor.py# perhitungan lokal & penggabungan hasil (merge)
│       ├── data_generator.py  # pembangkit daftar angka uji
│       ├── master.py          # logika PC1 (coordinator)
│       ├── worker.py          # logika PC2 (processor)
│       ├── sequential.py      # baseline 1 PC (pembanding speedup/efficiency)
│       └── cli.py             # entry point command-line
├── scripts/
│   ├── run_master.py          # dijalankan di PC1
│   ├── run_worker.py          # dijalankan di PC2
│   ├── run_sequential.py      # baseline, 1 PC
│   └── run_benchmark.py       # otomatisasi uji 3+ ukuran data & hitung speedup/efficiency
├── tests/
│   ├── test_number_processor.py
│   ├── test_protocol.py
│   ├── test_data_generator.py
│   └── test_master_worker_integration.py  # integrasi via localhost (loopback)
├── results/
│   └── benchmark_results.csv  # output otomatis dari run_benchmark.py
├── docs/
│   ├── REPORT_OUTLINE.md      # outline laporan PDF (bukan laporan itu sendiri)
│   └── DOCUMENTATION.md       # dibuat di akhir, lihat §9 (dibuat oleh AI/dev setelah selesai)
```

---

## 5. Desain Komputasi & Penggabungan Hasil

Setiap PC menghitung dari porsi datanya sendiri (semua nilai ini **combinable langsung**, tanpa rumus rumit):

- `count` — jumlah elemen
- `sum` — jumlah seluruh nilai
- `min`, `max`

Penggabungan di PC1 (sangat sederhana, `O(1)`):

```python
total_count = count_pc1 + count_pc2
total_sum   = sum_pc1 + sum_pc2
average     = total_sum / total_count
final_min   = min(min_pc1, min_pc2)
final_max   = max(max_pc1, max_pc2)
```

(Opsional, boleh ditambahkan jika ingin sedikit variasi operasi: `sum_of_squares` per porsi untuk menghitung **variance/std** di akhir dengan rumus `E[X^2] - (E[X])^2` — tetap sederhana karena `sum_of_squares` juga tinggal dijumlahkan. Sifatnya opsional, tidak wajib.)

---

## 6. Tahapan Pengerjaan (Milestones)

- [x] **M1 — Setup project**: struktur folder, `requirements.txt`, `config/config.yaml`, virtualenv.
- [x] **M2 — `number_processor.py`**: fungsi hitung lokal (`process_numbers()`) + fungsi gabung (`merge_results()`). Unit test lolos.
- [x] **M3 — `protocol.py`**: framing & serialisasi JSON. Unit test lolos.
- [x] **M4 — `data_generator.py`**: generator daftar angka (seed tetap untuk reprodusibilitas, ukuran dapat diatur).
- [x] **M5 — `worker.py`**: server socket di PC2 — terima data, proses angka, kirim balik hasil + waktu komputasi lokal.
- [x] **M6 — `master.py`**: client+coordinator di PC1 — generate data, split, proses porsi sendiri, kirim porsi ke PC2, terima hasil, gabung, tampilkan output wajib (lihat §7), ukur total waktu eksekusi.
- [x] **M7 — `sequential.py`**: baseline — proses seluruh data di 1 proses tanpa jaringan, untuk pembanding.
- [x] **M8 — Uji lokal (loopback)**: jalankan master & worker di 1 PC dengan `127.0.0.1` untuk memastikan logika benar sebelum uji jaringan nyata.
- [ ] **M9 — Uji jaringan nyata (2 PC)**: jalankan `worker.py` di PC2, `master.py` di PC1, verifikasi lewat LAN/WiFi yang sama, catat IP kedua PC.
- [x] **M10 — `run_benchmark.py`**: otomatisasi uji dengan minimal 3 ukuran data berbeda (contoh: 100rb, 1 juta, 10 juta angka), simpan ke `results/benchmark_results.csv`, hitung **Speedup** & **Efficiency** otomatis.
- [x] **M11 — Unit test lengkap**: pastikan seluruh modul inti tercakup (`pytest --cov`).
- [x] **M12 — Dokumentasi akhir**: buat `docs/DOCUMENTATION.md` (lihat §9) — **wajib dilakukan terakhir, setelah semua kode final**.
- [ ] **M13 — Laporan PDF & video demo**: disusun manual mengacu `docs/REPORT_OUTLINE.md` dan hasil `results/benchmark_results.csv`.

---

## 7. Output Wajib Ditampilkan ke Layar (sesuai ketentuan tugas poin 6)

`master.py` (dan ringkasan log di `worker.py`) harus menampilkan jelas:

1. Jumlah total angka yang diproses
2. Jumlah/beban angka yang diproses PC1 vs PC2
3. Hasil komputasi PC1 (sum, count, min, max lokal)
4. Hasil komputasi PC2 (sum, count, min, max lokal)
5. Hasil akhir setelah digabungkan (total sum, average, min, max keseluruhan)
6. Waktu eksekusi: waktu kirim data, waktu komputasi tiap PC, total waktu end-to-end

Gunakan format tabel teks sederhana di console (bisa pakai `tabulate` jika tersedia, atau format manual) agar mudah di-screenshot untuk laporan.

---

## 8. Rencana Pengujian

1. **Uji fungsional**: daftar angka kecil (misal 10–20 angka) dengan hasil yang bisa dicek manual.
2. **Uji skala — minimal 3 ukuran berbeda**, contoh:
   - Kecil: 100.000 angka
   - Sedang: 1.000.000 angka
   - Besar: 10.000.000 angka
3. **Catat untuk tiap ukuran**: waktu generate data, waktu kirim data via socket, waktu komputasi PC1, waktu komputasi PC2, waktu merge, total waktu distributed.
4. **Mode sequential** dijalankan juga untuk tiap ukuran data yang sama (di PC1 saja, tanpa jaringan) → `T_sequential`.
5. **Hitung**:
   - `Speedup = T_sequential / T_distributed`
   - `Efficiency = (Speedup / 2) * 100%`
6. **Analisis** (ditulis di laporan, bukan di sini): apakah speedup mendekati 2, apa yang membatasi (overhead jaringan, serialisasi, split tidak seimbang, dsb).

---

## 9. Rencana Dokumentasi Akhir (`docs/DOCUMENTATION.md`)

Setelah seluruh implementasi & testing selesai, **wajib dibuat file markdown baru** `docs/DOCUMENTATION.md` berisi dokumentasi lengkap, minimal mencakup:

1. Struktur folder lengkap (tree) + fungsi masing-masing folder/file
2. Penjelasan **tiap file kode**: tujuan file, fungsi/kelas di dalamnya, parameter, nilai kembali, efek samping
3. Penjelasan protokol komunikasi (format pesan JSON, framing, urutan handshake)
4. Penjelasan cara penggabungan hasil (sum/count/min/max) dan kenapa ini matematis benar
5. Diagram alur (boleh ASCII) proses end-to-end
6. Cara menjalankan aplikasi (step-by-step di PC1 & PC2), termasuk cara ubah konfigurasi IP/port
7. Cara menjalankan unit test & benchmark
8. Ringkasan hasil pengujian (tabel ukuran data vs waktu vs speedup vs efficiency)
9. Keterbatasan & kemungkinan pengembangan lanjutan

Detail aturan penulisan dokumentasi ini ada di `AGENTS.md` §10 — **AI/developer wajib membaca dan mengikuti instruksi tersebut setelah coding selesai.**

---

## 10. Timeline Kerja Menuju Deadline (12 Oktober 2026, 23:55)

| Tahap | Target |
|---|---|
| M1–M4 (fondasi + processor + protokol) | Hari 1 |
| M5–M7 (master/worker/sequential) | Hari 1 |
| M8–M9 (uji loopback & uji 2 PC nyata) | Hari 1–2 |
| M10–M11 (benchmark + unit test lengkap) | Hari 2 |
| M12 (dokumentasi teknis) | Hari 2 |
| M13 (laporan PDF + video demo) | Hari 2–3, sebelum deadline |

(Timeline lebih singkat dibanding tema lain karena komputasi dan penggabungan hasil jauh lebih sederhana.)

---

## 11. Prinsip Desain yang Dipegang

- **Pisahkan logika murni dari I/O**: `number_processor.py` tidak boleh tahu soal socket; `master.py`/`worker.py` hanya orkestrasi.
- **Testable**: semua fungsi inti (proses angka, protokol, split data) harus bisa diuji tanpa jaringan sungguhan.
- **Config over hardcode**: IP, port, ukuran data lewat `config.yaml`/argumen CLI, bukan ditulis langsung di kode.
- **Explicit over implicit**: gunakan *type hints* dan *docstring* di setiap fungsi publik.
- **Reproducible**: gunakan `seed` tetap untuk generator data agar hasil uji konsisten dan bisa dicek ulang.
- **Simple by design**: tema ini sengaja memakai operasi agregat sederhana (sum/count/min/max) agar fokus belajar ada di konsep distributed memory & komunikasi jaringan, bukan di kerumitan matematis.
