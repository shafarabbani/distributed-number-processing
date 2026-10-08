# DOCUMENTATION.md — Dokumentasi Teknis Distributed Number Processing (2 PC)

Dokumen teknis lengkap proyek **Distributed Number Processing**: implementasi
*distributed memory* dengan 2 peran (**PC1 = Master/Coordinator**, **PC2 =
Worker/Processor**) menggunakan Python murni (`socket` TCP, tanpa MPI).
Ditulis setelah seluruh kode final dan seluruh test lolos (56 test, coverage
93%).

---

## 1. Struktur Proyek (hasil aktual)

```
distributed-number-processing/
├── PLAN.md                        # rencana kerja (dokumen internal)
├── AGENTS.md                      # instruksi pengerjaan (dokumen internal)
├── README.md                      # quick-start singkat
├── requirements.txt               # pyyaml, pytest, pytest-cov
├── config/
│   └── config.yaml                # host, port, timeout, ukuran data, seed
├── src/
│   └── dnp/
│       ├── __init__.py            # penanda paket + daftar submodul
│       ├── protocol.py            # framing 4-byte + serialisasi JSON
│       ├── number_processor.py    # agregasi lokal + merge (murni, tanpa I/O)
│       ├── data_generator.py      # generator angka deterministik (seed)
│       ├── master.py              # orkestrasi PC1 (split, kirim, merge, report)
│       ├── worker.py              # server PC2 (accept loop, proses, balas)
│       ├── sequential.py          # baseline 1 PC untuk pembanding speedup
│       └── cli.py                 # loader config YAML + parser argparse + logging
├── scripts/
│   ├── run_worker.py              #jalankan di PC2
│   ├── run_master.py              # jalankan di PC1
│   ├── run_sequential.py          # jalankan baseline di 1 PC
│   └── run_benchmark.py           # benchmark otomatis >=3 ukuran + CSV
├── tests/
│   ├── conftest.py                # sisipkan src/ ke sys.path untuk pytest
│   ├── test_number_processor.py
│   ├── test_protocol.py
│   ├── test_data_generator.py
│   ├── test_master_worker_integration.py
│   └── test_sequential_and_cli.py # tambahan: baseline, config, report master
├── results/
│   └── benchmark_results.csv      # output run_benchmark.py (append)
└── docs/
    ├── REPORT_OUTLINE.md          # kerangka laporan PDF
    └── DOCUMENTATION.md           # dokumen ini
```

Perbedaan vs rencana `PLAN.md` §4: ditambahkan `tests/conftest.py` dan
`tests/test_sequential_and_cli.py` (test tambahan di luar minimal yang
diwajibkan; konftest diperlukan agar paket `dnp` dapat di-import tanpa
instalasi). Proyek dijalankan dengan layout `src/` tanpa `pip install -e`;
setiap script/tests menambahkan `src/` ke `sys.path` secara eksplisit.

---

## 2. Penjelasan Tiap File Kode

### 2.1 `src/dnp/number_processor.py` — logika komputasi murni

**Tujuan:** satu-satunya tempat rumus agregasi berada; TIDAK mengimpor socket
(prinsip pemisahan logika murni vs I/O, AGENTS.md §2.5).

| Fungsi | Parameter | Return | Perilaku |
|---|---|---|---|
| `process_numbers(numbers)` | `Sequence[float]` | `dict` `{count, sum, min, max}` | Hitung agregat satu porsi. List kosong → `ValueError` (min/max tak terdefinisi). |
| `merge_results(result_a, result_b)` | 2 dict parsial | `dict` `{total_count, total_sum, average, min, max}` | Gabungkan 2 hasil parsial (rumus di §4). Kunci hilang → `ValueError`; `total_count == 0` → `ValueError`. |
| `split_numbers(numbers, split_ratio=0.5)` | daftar + proporsi PC1 (0..1) | `(list, list)` | Bagi near-equal; dipakai master. Ratio di luar (0,1) atau data < 2 elemen → `ValueError`. |

Dependensi: tidak ada (stdlib `typing` saja). Diimpor oleh `master.py`,
`worker.py`, `sequential.py`, dan test.

### 2.2 `src/dnp/protocol.py` — framing & serialisasi

**Tujuan:** mengubah dict pesan ↔ byte di stream TCP; tidak tahu rumus apa pun.

| Fungsi | Signature | Perilaku |
|---|---|---|
| `send_message` | `(sock, payload: dict) -> None` | `json.dumps` → UTF-8 → `struct.pack("!I", len)` + body → `sendall`. Non-dict → `ValueError`; tak ter-serialisasi → `TypeError`; > 4 GB → `ValueError`. |
| `receive_message` | `(sock) -> dict` | Baca header 4 byte → baca body sepanjang `length` → parse JSON. Payload bukan object → `ValueError`; koneksi tertutup di tengah → `ConnectionError`; melebihi `sock.gettimeout()` → `socket.timeout`. |
| `_recv_exactly` (internal) | `(sock, count) -> bytes` | Loop `recv` sampai tepat `count` byte; EOF lebih awal → `ConnectionError`. Wajib karena TCP adalah stream. |
| `validate_message_type` | `(message: dict) -> str` | Ambil `"type"`, pastikan ∈ `MESSAGE_TYPES`; gagal → `ValueError` (penanganan jelas, bukan *silent fail*). |

Konstanta: `HEADER_FORMAT="!I"` (4 byte big-endian unsigned int),
`MESSAGE_TYPES = {"data_chunk","process_result","ping","pong"}`.

### 2.3 `src/dnp/data_generator.py` — generator data uji

| Fungsi | Parameter | Return | Perilaku |
|---|---|---|---|
| `generate_numbers(size, seed, value_range=(1, 1_000_000))` | jumlah, seed, rentang inklusif | `list[int]` | `random.Random(seed).randint(...)` — instance RNG lokal, tidak mengganggu state global. Seed sama ⇒ daftar sama (deterministik, reproducible). `size < 1` atau `min > max` → `ValueError`. |

Determinisme ini yang membuat perbandingan sequential vs distributed **adil**:
dua mode memproses daftar angka yang identik.

### 2.4 `src/dnp/worker.py` — PC2

| Fungsi | Peran |
|---|---|
| `handle_connection(conn) -> dict \| None` | Baca 1 pesan: `ping` → balas `pong` (return `None`); `data_chunk` → ukur waktu → `process_numbers()` → balas `process_result` berisi `result`, `compute_time_seconds`, `request_id`. Tipe lain → `ValueError`. |
| `serve(host, port, timeout_seconds, once=False, max_requests=None, stop_event=None, on_ready=None) -> int` | Bind `AF_INET/SOCK_STREAM` (`SO_REUSEADDR`), `listen`, accept loop. Timeout accept 0.5 s agar `stop_event` responsif. `port=0` → port bebas (dipakai test; `on_ready` melaporkan port aktual). Pesan rusak hanya di-log, koneksi ditutup, loop tetap hidup ⇒ Worker tidak crash antar-request. Return jumlah permintaan terlayani. |

Log via `logging` (`dnp.worker`): jumlah angka diterima, hasil, waktu komputasi.

### 2.5 `src/dnp/master.py` — PC1

| Fungsi | Peran |
|---|---|
| `_dispatch_to_worker(host, port, timeout, numbers)` | Dijalankan di **thread terpisah**: `socket.create_connection` → kirim `data_chunk` (dengan `request_id` uuid4) → terima `process_result`; validasi tipe & `request_id`; mengembalikan hasil Worker + waktu connect/kirim/receive. |
| `execute_distributed(numbers, host, port, timeout_seconds=30, split_ratio=0.5) -> dict` | Inti alur: `split_numbers` → submit `_dispatch_to_worker` ke `ThreadPoolExecutor(1)` → **main thread menghitung porsi PC1 sendiri secara paralel** → `future.result()` → `merge_results` → laporan lengkap + rincian waktu. `request_id` memastikan balasan yang diproses adalah balasan dari request run ini. |
| `print_report(report)` | Cetak 6 output wajib PLAN.md §7 sebagai tabel teks (mudah di-screenshot). |

**Kenapa `ThreadPoolExecutor`, bukan multi-proses?** Bagian Worker adalah
I/O-bound (kirim/terima byte), bagian lokal adalah komputasi Python; thread
cukup untuk overlap keduanya di sisi master tanpa overhead IPC, dan hasilnya
tetap satu proses master yang menggabungkan.

**Laporan** (`dict`) berisi: `load` (jumlah angka PC1 vs PC2), `pc1_result`,
`pc2_result`, `final` (gabungan), `timings` (`connect`, `send`, `receive`,
`compute_pc1`, `compute_pc2`, `merge`, `total_time_seconds` — semua `perf_counter`).

### 2.6 `src/dnp/sequential.py` — baseline

`run_sequential(numbers) -> {"result", "compute_time_seconds"}` — memanggil
`process_numbers` atas SELURUH data tanpa split/jaringan; waktunya menjadi
`T_sequential` untuk speedup.

### 2.7 `src/dnp/cli.py` — konfigurasi & CLI

| Fungsi | Peran |
|---|---|
| `load_config(path=None)` | Baca `config/config.yaml` (`yaml.safe_load`); validasi bagian wajib `network` & `data`; `FileNotFoundError`/`ValueError` eksplisit. Path default dihitung dari `__file__` (root proyek), bukan cwd. |
| `setup_logging(level)` | `logging.basicConfig` format konsol ringkas. |
| `build_master_parser / build_worker_parser / build_sequential_parser` | Argparse dengan **default dari config** + override CLI: `--size --seed --split-ratio --worker-host --port --timeout --once --host --config --log-level`. Tidak ada IP/port hardcode di kode. |

### 2.8 `scripts/run_*.py`

Semua script bootstrap `sys.path` ke `src/`, lalu hanya mengorkestrasi modul
(tidak ada logika bisnis di scripts):

- `run_worker.py` — `cli.load_config` → `worker.serve(host, port, ...)`, tangani `KeyboardInterrupt` dan bind gagal (pesan error jelas + exit 1).
- `run_master.py` — generate data → `execute_distributed` → `print_report`; gagal koneksi → pesan panduan jelas + exit 1.
- `run_sequential.py` — generate data (seed sama) → `run_sequential` → cetak hasil.
- `run_benchmark.py` — lihat §6.

---

## 3. Protokol Komunikasi

### 3.1 Framing

TCP adalah stream byte tanpa batas pesan. Setiap pesan diframe:

```
+----------------+--------------------------+
| 4 byte header  |  N byte payload JSON     |
| panjang payload|  (UTF-8)                 |
| big-endian !I  |                          |
+----------------+--------------------------+
```

Penerima selalu membaca header 4 byte dulu, lalu membaca **tepat** `N` byte
payload (`_recv_exactly`), sehingga fragmentasi TCP tidak pernah memecah
pesan, dan beberapa pesan berturut-turut tetap terpisahkan.

### 3.2 Jenis pesan & contoh payload nyata

**`data_chunk`** (Master → Worker) — untuk 4 angka:

```json
{"type": "data_chunk", "request_id": "9f2c1a...", "data": [744556, 26226, 654634, 935519]}
```

**`process_result`** (Worker → Master):

```json
{
  "type": "process_result",
  "request_id": "9f2c1a...",
  "result": {"count": 4, "sum": 2596935, "min": 26226, "max": 935519},
  "compute_time_seconds": 0.000031
}
```

**`ping` / `pong`** (opsional, cek koneksi):

```json
{"type": "ping"}        ->        {"type": "pong"}
```

Tipe tidak dikenal/`type` hilang → `validate_message_type` melempar
`ValueError` dengan pesan jelas; pada Worker hanya di-log (loop server tetap
hidup).

### 3.3 Urutan handshake satu run

```
Master                          Worker
  | socket.create_connection      |
  | ---- data_chunk ------------> | accept, receive_message
  |                               | process_numbers() (ukur waktu lokal)
  | <--- process_result --------- | send_message
  | receive + validate            |
  | merge_results                 | (siap koneksi berikutnya)
```

---

## 4. Logika Penggabungan Hasil (merge)

`sum`, `count`, `min`, `max` bersifat **combinable langsung** (asosiatif/komutatif):

```
total_sum   = sum_pc1 + sum_pc2
total_count = count_pc1 + count_pc2
average     = total_sum / total_count
min         = min(min_pc1, min_pc2)
max         = max(max_pc1, max_pc2)
```

**Bukti korektnya** ditegakkan oleh test, bukan klaim: class
`TestMergeCorrectness::test_merge_equals_direct_computation`
(`tests/test_number_processor.py`) memverifikasi untuk beberapa ukuran data
(10, 997, 10.000):

```python
merge_results(process_numbers(A), process_numbers(B))
== process_numbers(A + B)   # pada total_sum, total_count, min, max
```

dan `average == sum(A+B)/len(A+B)`. Test integrasi
(`test_master_worker_integration.py`) mengulangi verifikasi yang sama
terhadap hasil yang benar-benar dikirim lewat socket loopback.

Integritas tipe juga terjamin: seluruh nilai adalah bilangan bulat kecil
(JSON exact) atau hasil pembagian (float) — tanpa akumulasi floating point
yang berbeda urutan, karena `sum` tiap porsi dihitung integer lalu
dijumlahkan, `average` dihitung sekali di akhir.

---

## 5. Diagram Alur End-to-End

```
PC1 (Master)                                        PC2 (Worker)
────────────                                        ────────────
1. load config + argumen CLI
2. generate N angka (seed)  ── list identik utk baseline sequential
3. split_numbers: chunk_pc1 | chunk_pc2
4. t_start = perf_counter()
5. ThreadPoolExecutor.submit(
     connect + kirim chunk_pc2  ───────────────────► 6. accept, receive data_chunk
   )                                                 7. t0 → process_numbers(chunk_pc2) → t1
8. (main thread) process_numbers(chunk_pc1)   ‖      8'. balas process_result
   (paralel dg langkah 6–7)                               (result + t1-t0)
9. future.result()  ◄───────────────────────────────
10. merge_results(pc1, pc2)
11. t_end; total_time = t_end - t_start
12. print_report → 6 output wajib ke layar
13. (benchmark) append results/benchmark_results.csv
```

Langkah 5+8 adalah inti "distributed memory": dua PC bekerja bersamaan atas
porsinya masing-masing; hanya hasil parsial kecil yang lewat jaringan pada
akhiran (bukan seluruh data bolak-balik).

---

## 6. Cara Menjalankan

### 6.1 Instalasi (kedua PC)

```bash
# dari folder proyek
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/macOS:
source .venv/bin/activate

pip install -r requirements.txt
```

Prasyarat: Python 3.10+ (dites pada 3.14). Hanya `pyyaml` (+ `pytest`,
`pytest-cov` untuk pengujian).

### 6.2 Konfigurasi (`config/config.yaml`)

| Kunci | Arti |
|---|---|
| `network.worker_host` | alamat bind Worker di PC2 (default `0.0.0.0` = semua interface) |
| `network.master_connect_host` | IP PC2 yang dilihat dari PC1 — **isi sesuai jaringan nyata** |
| `network.port` | port TCP (default 5050) |
| `network.timeout_seconds` | timeout socket |
| `data.default_size` / `data.seed` / `data.value_range` | ukuran/seed/rentang generator |
| `data.benchmark_sizes` | daftar ukuran untuk `run_benchmark.py` |

Cek IP PC2: `ipconfig` (Windows) / `ip a` (Linux). Izinkan port di firewall
PC2, atau gunakan `--port` lain. Semua nilai dapat dioverride CLI
(`--worker-host`, `--port`, `--timeout`, `--size`, `--seed`) tanpa mengubah
file.

### 6.3 Uji 2 PC nyata (LAN/WiFi sama)

```bash
# PC2 — mulai Worker dulu (terminal 1, biarkan berjalan):
python scripts/run_worker.py                  # listen 0.0.0.0:5050

# PC1 — jalankan Master (IP PC2 sesuai ipconfig):
python scripts/run_master.py --worker-host 192.168.1.20 --size 1000000
```

Layar PC1 menampilkan 6 output wajib; layar PC2 menampilkan log per request
(jumlah angka, hasil, waktu komputasi lokal) — cocok untuk video demo.

### 6.4 Uji satu mesin (loopback, tanpa PC kedua)

```bash
# terminal 1:
python scripts/run_worker.py --host 127.0.0.1
# terminal 2:
python scripts/run_master.py --worker-host 127.0.0.1 --size 100000
```

### 6.5 Baseline sequential

```bash
python scripts/run_sequential.py --size 1000000   # seed sama → data identik
```

### 6.6 Benchmark otomatis (speedup & efficiency)

```bash
# 2 PC (PC2 sudah menjalankan run_worker.py):
python scripts/run_benchmark.py --worker-host 192.168.1.20

# 1 mesin (Worker ditanam sebagai thread loopback):
python scripts/run_benchmark.py --embedded-worker
```

Untuk tiap ukuran di `benchmark_sizes`: generate data 1× → run sequential →
run distributed → hitung `Speedup = T_sequential / T_distributed`,
`Efficiency = Speedup/2 × 100%` → append ke `results/benchmark_results.csv`
(kolom: `data_size, t_sequential, t_distributed, speedup, efficiency,
compute_pc1, compute_pc2, send_time, total_count, total_sum, average, min,
max, timestamp`) dan cetak tabel ringkasan.

### 6.7 Unit test

```bash
pytest tests/ -v
pytest tests/ -q --cov=dnp --cov-report=term   # + coverage
```

---

## 7. Hasil Pengujian

### 7.1 Unit & integrasi

**56 test PASS** (`pytest tests/ -v`), coverage paket `dnp` **93%**:
`number_processor` 100%, `data_generator` 100%, `sequential` 100%,
`master` 97%, `protocol` 93%, `worker` 91%, `cli` 85%. Test integrasi
menjalankan Worker sungguhan di thread `127.0.0.1` (port bebas) dan
memverifikasi hasil distributed == perhitungan langsung atas seluruh data,
termasuk multi-request berturut-turut dan penanganan pesan rusak/Worker mati.

### 7.2 Benchmark loopback (1 mesin, `--embedded-worker`, seed 42)

| data_size | T_sequential (s) | T_distributed (s) | Speedup | Efficiency |
|---:|---:|---:|---:|---:|
| 100.000 | 0.0029 | 0.0315 | 0.092× | 4.6% |
| 1.000.000 | 0.0384 | 0.2386 | 0.161× | 8.1% |
| 10.000.000 | 0.3565 | 3.2684 | 0.109× | 5.5% |

**Interpretasi (dilaporkan apa adanya, sesuai AGENTS.md §9):** speedup < 1
di sini **valid dan mudah dijelaskan**. Komputasi agregat (sum/count/min/max)
sangat murah — 10 juta angka hanya ±0,36 s — sedangkan biaya
serialisasi-deserialisasi JSON (±70 MB payload teks) + transfer socket +
latensi koneksi jauh lebih besar (rincian `send_time` 0,52 s dan total 3,3 s
pada 10 juta). Artinya overhead komunikasi ≈ 9× waktu komputasi, sehingga
paralelisasi 2 bagian tidak sempat "membayar balik" biayanya. Ini justru
memperlihatkan syarat munculnya speedup pada distributed memory: **rasio
komputasi-terhadap-komunikasi** harus tinggi (komputasi per byte data jauh
lebih berat, data dikirim biner bukan JSON, atau agregat dihitung streaming
tanpa materialisasi). Pada 2 PC fisik dengan beban komputasi yang sama, angka
communication-bound ini akan tetap < 1; speedup mendekati/melebihi 1 baru
terlihat bila operasi per angka dibuat jauh lebih berat dari agregasi.

Hasil 2 PC nyata (LAN) perlu diuji ulang dan ditambahkan ke tabel ini saat
demo (jalankan §6.3 + `run_benchmark.py` tanpa `--embedded-worker`).

---

## 8. Keterbatasan & Potensi Pengembangan

1. **Split statis 50:50** — ada `--split-ratio` untuk eksperimen, tetapi tidak
   ada load-balancing otomatis berdasarkan kecepatan riil tiap PC.
2. **Serialisasi JSON text** — payload besar; format biner (`array`/`struct`/
   `pickle` internal) akan memangkas biaya transfer berlipat ganda.
3. **Satu Worker, satu request aktif** — `ThreadPoolExecutor(1)`; belum ada
   multi-worker atau pipeline chunk yang overlap antar ukuran besar.
4. **Tanpa fault-tolerance** — Worker mati di tengah proses → Master
   melempar `ConnectionError`/timeout; belum ada retry, checkpoint, atau
   replikasi chunk.
5. **Tidak ada otentikasi/keamanan** — TCP + JSON polos, aman hanya di LAN
   terpecaya; perlu TLS/PSK untuk jaringan terbuka.
6. **Ukuran data dibatasi RAM** — seluruh daftar dimuat di memori kedua sisi;
   belum streaming/chunking multi-kirim.
7. **Opsional yang tidak diambil** — `sum_of_squares`/variance-std (PLAN §5)
   tidak diimplementasikan karena tidak diwajibkan; menambahkan `process_numbers`
   kunci `sum_of_squares` dan `merge_results` penjumlahan + rumus
   `E[X^2] − (E[X])²` akan cukup karena sifatnya tetap combinable.
