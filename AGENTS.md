# AGENTS.md — Instruksi Kerja untuk AI/Developer

Dokumen ini adalah **instruksi operasional** bagi siapa pun (AI coding agent atau manusia) yang mengerjakan implementasi proyek **Distributed Number Processing (2 PC)**. Ikuti dokumen ini bersama `PLAN.md` (rencana & arsitektur) sebagai acuan utama. Tugas dasar aslinya ada di `transkrip_tugas_1_distributed_memory.md`.

---

## 1. Peran & Konteks

Kamu mengerjakan tugas kuliah **Pemrograman Paralel/Terdistribusi**: membangun aplikasi *distributed memory* yang berjalan di **2 PC terpisah** melalui jaringan LAN, dengan tema **Distributed Number Processing**. Bahasa: **Python**. Transport: **TCP socket** (library `socket` standar, bukan MPI) agar mudah dijalankan & dijelaskan di 2 PC fisik yang berbeda.

Inti aplikasi: PC1 membagi sekumpulan angka ke PC1 sendiri dan PC2, keduanya menghitung `sum`, `count`, `min`, `max` dari porsi masing-masing, lalu PC1 **menggabungkan** hasil tersebut menjadi `total_sum`, `total_count`, `average`, `min`, `max` final. **Tema ini sengaja dipilih sederhana** — fokuskan effort pada kebenaran komunikasi jaringan & kebersihan kode, bukan pada rumus matematis rumit.

Kamu **wajib** mengikuti struktur direktori dan milestone yang sudah ditetapkan di `PLAN.md` §4 dan §6. Jangan mengubah struktur besar tanpa alasan kuat — jika terpaksa berbeda, jelaskan alasannya di `docs/DOCUMENTATION.md`.

---

## 2. Prinsip Coding yang Wajib Dipatuhi

1. **Clean & readable code**:
   - Ikuti PEP8. Gunakan `black`/`ruff` jika tersedia untuk formatting.
   - Nama variabel/fungsi deskriptif (`process_numbers`, bukan `f1`).
   - Fungsi pendek, satu tanggung jawab (single responsibility).
2. **Type hints wajib** di semua fungsi publik (parameter & return type).
3. **Docstring wajib** (gaya Google/NumPy) di setiap modul, kelas, dan fungsi publik — jelaskan tujuan, parameter, return value, dan exception yang mungkin dilempar.
4. **Tidak ada magic number/IP hardcode** — semua nilai yang bisa berubah (port, host, ukuran data, timeout) diambil dari `config/config.yaml` atau argumen CLI (`argparse`).
5. **Pisahkan logika murni dari I/O**:
   - `number_processor.py` → fungsi matematis murni, tidak boleh `import socket`.
   - `protocol.py` → hanya urusan serialisasi/pengiriman byte, tidak boleh tahu soal rumus perhitungan.
   - `master.py` / `worker.py` → orkestrasi saja, memanggil fungsi dari modul lain.
6. **Error handling eksplisit**: koneksi gagal, data kosong, worker tidak merespons, dsb. harus ditangani dengan pesan error yang jelas, bukan *silent fail*.
7. **Logging, bukan `print` berantakan**: gunakan modul `logging` untuk log internal (level INFO/DEBUG), dan `print`/tabel ringkas khusus untuk output wajib yang harus dilihat user di layar (lihat `PLAN.md` §7).
8. **Reproducibility**: generator data (`data_generator.py`) menerima parameter `seed` agar hasil pengujian bisa diulang dan diverifikasi.

---

## 3. Struktur Direktori (WAJIB diikuti — lihat detail di `PLAN.md` §4)

```
distributed-number-processing/
├── PLAN.md, AGENTS.md, README.md, requirements.txt
├── config/config.yaml
├── src/dnp/
│   ├── protocol.py, number_processor.py, data_generator.py
│   ├── master.py, worker.py, sequential.py, cli.py
├── scripts/
│   ├── run_master.py, run_worker.py, run_sequential.py, run_benchmark.py
├── tests/
│   ├── test_number_processor.py, test_protocol.py
│   ├── test_data_generator.py, test_master_worker_integration.py
├── results/benchmark_results.csv
└── docs/REPORT_OUTLINE.md, docs/DOCUMENTATION.md
```

---

## 4. Spesifikasi Protokol Komunikasi (`protocol.py`)

- Gunakan **TCP socket**, bukan UDP (butuh reliability, bukan kecepatan maksimum).
- **Framing**: setiap pesan dikirim sebagai `4-byte big-endian length header` + `payload JSON (UTF-8)`. Ini wajib agar penerima tahu kapan satu pesan selesai diterima (socket TCP adalah stream, bukan pesan diskrit).
- Sediakan 2 fungsi inti minimal:
  - `send_message(sock: socket.socket, payload: dict) -> None`
  - `receive_message(sock: socket.socket) -> dict`
- Jenis pesan minimal yang harus didukung (gunakan field `"type"` di JSON untuk membedakan):
  - `"type": "data_chunk"` — Master → Worker: berisi list angka yang harus diproses Worker.
  - `"type": "process_result"` — Worker → Master: berisi `sum`, `count`, `min`, `max` dari porsi Worker + waktu komputasi Worker.
  - (Opsional) `"type": "ping"/"pong"` untuk cek koneksi awal sebelum kirim data besar.
- Worker harus bisa menangani **lebih dari satu permintaan** berturut-turut tanpa crash (gunakan loop server sederhana), supaya bisa dites beberapa ukuran data tanpa restart manual.

---

## 5. Spesifikasi Modul Komputasi (`number_processor.py`)

Implementasikan minimal:

```python
def process_numbers(numbers: list[float]) -> dict:
    """Hitung sum, count, min, max dari satu porsi angka.
    Harus menangani list kosong dengan jelas (raise ValueError atau
    kembalikan nilai sentinel yang didokumentasikan — pilih salah satu
    dan konsisten)."""

def merge_results(result_a: dict, result_b: dict) -> dict:
    """Gabungkan dua hasil parsial menjadi hasil akhir:
    total_sum = sum_a + sum_b
    total_count = count_a + count_b
    average = total_sum / total_count
    min = min(min_a, min_b)
    max = max(max_a, max_b)
    """
```

**Wajib ada unit test** yang membuktikan:
`merge_results(process_numbers(A), process_numbers(B))` menghasilkan `total_sum`, `total_count`, `min`, `max` yang **identik** dengan `process_numbers(A + B)` (dan `average` konsisten dengan `sum(A+B)/len(A+B)`).

(Opsional) Jika ingin menambah variasi, tambahkan `sum_of_squares` di `process_numbers` dan hitung `variance`/`std` di akhir memakai `E[X^2] - (E[X])^2` — tetap tinggal dijumlahkan saat merge, tidak menambah kerumitan berarti.

---

## 6. Alur Kerja Program

### `worker.py` (PC2)
1. Bind & listen di `host:port` dari config.
2. Terima koneksi dari Master.
3. Terima pesan `data_chunk`.
4. Catat waktu mulai → hitung `process_numbers()` → catat waktu selesai.
5. Kirim balik `process_result` berisi hasil (`sum`, `count`, `min`, `max`) + waktu komputasi lokal + jumlah angka yang diproses.
6. Log ke console: jumlah angka diterima, hasil, waktu komputasi.
7. Siap menerima koneksi berikutnya (loop), jangan langsung exit setelah satu request (kecuali diberi flag `--once` untuk kemudahan testing).

### `master.py` (PC1)
1. Baca argumen CLI (jumlah angka, mode `distributed`/`sequential`, alamat worker).
2. Generate/baca daftar angka (`data_generator.py`).
3. Split daftar jadi 2 bagian (near-equal; default 50:50, boleh dibuat proporsi bisa diatur untuk eksperimen tambahan).
4. Catat `t_start`.
5. Kirim bagian ke-2 ke Worker (`data_chunk`), **sambil** PC1 memproses bagian ke-1 secara lokal — idealnya dilakukan **concurrent** (misal pakai `threading`/`concurrent.futures`) supaya benar-benar paralel, bukan sekadar sekuensial kirim-lalu-hitung. Jelaskan pilihan ini di dokumentasi.
6. Terima `process_result` dari Worker.
7. `merge_results()` hasil PC1 + PC2.
8. Catat `t_end`. Hitung `total_time = t_end - t_start`.
9. **Tampilkan ke layar** (lihat `PLAN.md` §7): total angka, beban PC1 vs PC2, hasil PC1, hasil PC2, hasil akhir gabungan, rincian waktu (kirim, komputasi tiap PC, total).
10. Simpan ringkasan run ke `results/benchmark_results.csv` (append) bila dijalankan lewat `run_benchmark.py`.

### `sequential.py` (baseline pembanding — 1 PC)
1. Generate daftar angka yang sama (seed sama dengan mode distributed agar adil).
2. Hitung `process_numbers()` atas **seluruh data sekaligus**, tanpa split, tanpa jaringan.
3. Catat waktu eksekusi → ini jadi `T_sequential` untuk perhitungan speedup.

### `run_benchmark.py`
1. Jalankan mode sequential dan distributed untuk **minimal 3 ukuran data** (ambil dari config, misal `[100_000, 1_000_000, 10_000_000]`).
2. Catat `T_sequential` dan `T_distributed` tiap ukuran.
3. Hitung `Speedup = T_sequential / T_distributed` dan `Efficiency = (Speedup / 2) * 100%`.
4. Tulis semua ke `results/benchmark_results.csv` dengan kolom minimal: `data_size, t_sequential, t_distributed, speedup, efficiency`.
5. Cetak ringkasan tabel ke console.

---

## 7. Konfigurasi (`config/config.yaml`)

Contoh isi minimal yang harus didukung:

```yaml
network:
  worker_host: "0.0.0.0"   # dipakai worker untuk bind
  master_connect_host: "192.168.1.20"  # IP PC2, diisi sesuai jaringan nyata
  port: 5050
  timeout_seconds: 30

data:
  default_size: 1000000
  seed: 42
  benchmark_sizes: [100000, 1000000, 10000000]
  value_range: [1, 1000000]   # rentang angka random yang dibangkitkan
```

Jangan hardcode IP PC2 di dalam kode Python manapun — selalu baca dari sini atau override via `--worker-host` di CLI.

---

## 8. Unit Testing (WAJIB)

Gunakan **pytest**. Minimal cakupan:

| File | Yang diuji |
|---|---|
| `test_number_processor.py` | `process_numbers` pada list kosong/satu elemen/banyak elemen/angka negatif; `merge_results` konsisten dengan hitung langsung atas gabungan data (lihat §5); `average` dihitung benar |
| `test_protocol.py` | `send_message`/`receive_message` round-trip benar (gunakan `socket.socketpair()` agar tidak butuh jaringan asli); pesan besar (>64KB) tetap utuh; pesan tipe tidak dikenal ditangani dengan jelas |
| `test_data_generator.py` | Ukuran data sesuai permintaan; seed sama → hasil sama (determinism); seed beda → hasil beda |
| `test_master_worker_integration.py` | Jalankan `worker.py` di thread/proses terpisah pada `127.0.0.1`, lalu jalankan alur master penuh, verifikasi hasil akhir (`total_sum`, `average`, `min`, `max`) cocok dengan `process_numbers` atas seluruh data asli |

Jalankan dengan:
```bash
pytest tests/ -v
```
Pastikan semua test **lolos** sebelum lanjut ke tahap dokumentasi akhir. Jika memungkinkan tambahkan `pytest-cov` dan laporkan ringkas coverage di `docs/DOCUMENTATION.md`.

---

## 9. Benchmark & Analisis (sesuai `PLAN.md` §8)

- Minimal 3 ukuran data berbeda diuji dan dicatat di `results/benchmark_results.csv`.
- Speedup & Efficiency **dihitung otomatis** oleh `run_benchmark.py`, jangan dihitung manual di laporan tanpa dasar data yang tersimpan.
- Jika hasil benchmark menunjukkan speedup rendah/di bawah 1 untuk data kecil, itu **valid dan perlu dianalisis** (overhead jaringan & serialisasi bisa lebih besar dari manfaat paralelisasi pada data kecil) — jangan "diperbaiki" dengan memanipulasi angka, laporkan apa adanya di dokumentasi/laporan.

---

## 10. INSTRUKSI WAJIB SETELAH SEMUA KODE SELESAI: Dokumentasi Akhir

> **Ini adalah langkah wajib terakhir. Jangan dilewati.**

Setelah seluruh implementasi (kode, unit test, benchmark) selesai dan **semua test lolos**, buat file markdown baru di `docs/DOCUMENTATION.md` yang mendokumentasikan **seluruh struktur kode dan penjelasan setiap bagian kode secara lengkap**. Dokumen ini harus bisa dibaca orang yang belum pernah melihat proyek ini dan langsung paham cara kerja & cara menjalankannya.

Isi minimal `docs/DOCUMENTATION.md`:

1. **Struktur Proyek** — tree direktori lengkap (hasil aktual, bukan rencana) + deskripsi singkat tiap folder.
2. **Penjelasan Tiap File Kode** — untuk SETIAP file di `src/dnp/`, `scripts/`, dan `tests/`, tuliskan:
   - Tujuan file ini apa
   - Daftar fungsi/kelas di dalamnya beserta: parameter, tipe return, apa yang dilakukan (ringkas tapi jelas, boleh sertakan potongan kode penting)
   - Dependensi ke file lain (import apa, dipakai di mana)
3. **Protokol Komunikasi** — format pesan JSON lengkap dengan contoh payload nyata untuk tiap `"type"`, dan penjelasan framing 4-byte length header.
4. **Logika Penggabungan Hasil** — rumus `merge_results` (sum, count, average, min, max) dan bukti korektnya (bisa rujuk ke unit test yang membuktikannya).
5. **Diagram Alur End-to-End** — boleh ASCII diagram, urutan langkah dari generate data sampai hasil akhir tampil.
6. **Cara Menjalankan**:
   - Instalasi (`pip install -r requirements.txt`)
   - Cara ubah `config/config.yaml` (terutama IP PC2)
   - Perintah persis untuk menjalankan di PC1 dan PC2 (`python scripts/run_worker.py ...`, `python scripts/run_master.py ...`)
   - Cara jalankan mode sequential
   - Cara jalankan benchmark otomatis
   - Cara jalankan unit test
7. **Hasil Pengujian** — tabel ringkasan dari `results/benchmark_results.csv` (ukuran data, waktu sequential, waktu distributed, speedup, efficiency) + interpretasi singkat.
8. **Keterbatasan & Potensi Pengembangan** — misalnya: split masih 50:50 statis, belum ada fault-tolerance jika Worker mati di tengah proses, belum mendukung lebih dari 1 Worker, dll.

**Format**: gunakan heading Markdown yang rapi (`##`, `###`), code block untuk cuplikan kode, dan tabel Markdown untuk data tabular. File ini terpisah dari `README.md` (yang cukup berisi quick-start singkat) — `DOCUMENTATION.md` adalah dokumentasi teknis mendalam.

---

## 11. Checklist "Definition of Done"

Sebelum menyatakan pekerjaan selesai, pastikan:

- [ ] Semua modul di `src/dnp/` ada dan sesuai tanggung jawab masing-masing (§3–§6)
- [ ] Tidak ada IP/port hardcode di luar `config/config.yaml`
- [ ] Semua fungsi publik punya type hint + docstring
- [ ] `pytest tests/ -v` → seluruh test **PASS**
- [ ] Berhasil diuji di mode loopback (`127.0.0.1`) **dan** (idealnya) di 2 PC nyata melalui LAN
- [ ] `run_benchmark.py` sudah dijalankan untuk minimal 3 ukuran data, hasil tersimpan di `results/benchmark_results.csv`
- [ ] Output wajib (§7 `PLAN.md`) tampil jelas di console saat `master.py` dijalankan
- [ ] `docs/DOCUMENTATION.md` sudah dibuat lengkap sesuai §10 di atas
- [ ] `README.md` berisi quick-start ringkas (instalasi + cara run cepat, untuk pembaca yang terburu-buru)

---

## 12. Larangan / Batasan

- Jangan gunakan library eksternal berat yang tidak perlu (cukup `pyyaml` untuk config, `pytest` untuk testing, library standar lain seperti `socket`, `struct`, `json`, `argparse`, `logging`, `time`, `random`, `threading`/`concurrent.futures`).
- Jangan gabungkan logika komputasi dengan logika socket dalam satu fungsi — harus terpisah (§2 poin 5) agar mudah diuji dan dijelaskan.
- Jangan tampilkan hasil yang tidak benar-benar dihitung dari proses distributed (semua angka yang tampil harus berasal dari eksekusi nyata, bukan nilai dummy).
- Jangan lewati langkah unit testing maupun dokumentasi akhir — keduanya adalah bagian wajib dari definisi selesai (§11).
