# Distributed Number Processing (2 PC)

Tugas 1 Pemrograman Paralel/Terdistribusi — implementasi *distributed memory*
dengan **2 PC** melalui LAN memakai **Python murni (`socket` TCP, tanpa MPI)**:

- **PC1 = Master/Coordinator** — membangkitkan data, membagi angka menjadi 2
  bagian (50:50), memproses bagiannya sendiri **sambil** mengirim bagian lain
  ke PC2, menerima hasil parsial PC2, lalu **menggabungkan**
  (`total_sum`, `total_count`, `average`, `min`, `max`) dan menampilkan
  seluruh output wajib + rincian waktu.
- **PC2 = Worker/Processor** — server TCP yang menerima `data_chunk`,
  menghitung `sum/count/min/max` porsinya, dan membalas `process_result`.

Protocol: pesan JSON dengan *length-prefixed framing* (header 4 byte).
Benchmark otomatis menghitung **Speedup** dan **Efficiency** terhadap mode
sequential (1 PC).

> Dokumentasi teknis mendalam: [`docs/DOCUMENTATION.md`](docs/DOCUMENTATION.md).

---

## 1. Persiapan (lakukan di KEDUA PC)

Prasyarat: **Python 3.10+** (ditest di 3.14) dan salinan folder proyek yang
sama di kedua PC.

### Windows (PowerShell / CMD)

```bat
cd D:\distributed-number-processing

:: 1. Buat virtual environment
python -m venv .venv

:: 2. Aktifkan venv
.venv\Scripts\activate
:: (bila ditahan policy PowerShell: .venv\Scripts\activate.bat)

:: 3. Install dependensi
pip install -r requirements.txt
```

### Linux / macOS

```bash
cd distributed-number-processing
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Setelah aktif, prompt terminal berubah menjadi `(.venv)` dan perintah
`python` memakai interpreter environment tersebut. Kelak keluar venv dengan
`deactivate`.

## 2. Konfigurasi Jaringan

Edit **`config/config.yaml`** (tidak ada IP hardcode di kode):

```yaml
network:
  worker_host: "0.0.0.0"          # PC2 listen di semua interface — biarkan
  master_connect_host: "127.0.0.1" # ganti ke IP LAN PC2, misal 192.168.1.20
  port: 5050
  timeout_seconds: 30
```

Cara cek IP PC2: `ipconfig` (Windows) / `ip a` (Linux). Kedua PC harus berada
di jaringan yang sama (LAN/WiFi). Jika koneksi dari PC1 ditolak, izinkan port
5050 pada firewall PC2. Nilai apa pun bisa juga dioverride lewat CLI
(`--worker-host`, `--port`, dst.) tanpa mengubah file.

## 3. Menjalankan (2 PC nyata)

**Langkah 1 — di PC2 (Worker), biarkan terminal ini terbuka:**

```bash
python scripts/run_worker.py
# log: "Worker listen di 0.0.0.0:5050" → tunggu koneksi PC1 (Ctrl+C untuk berhenti)
```

**Langkah 2 — di PC1 (Master):**

```bash
python scripts/run_master.py --worker-host 192.168.1.20 --size 1000000
```

Layar PC1 menampilkan 6 output wajib: total angka, beban PC1 vs PC2, hasil
parsial PC1, hasil parsial PC2, hasil akhir gabungan, dan rincian waktu
(connect / kirim / terima / komputasi tiap PC / merge / total end-to-end).
Layar PC2 menampilkan log jumlah angka, hasil, dan waktu komputasi lokal.

## 4. Uji Cepat Satu Mesin (tanpa PC kedua)

```bash
# Terminal 1:
python scripts/run_worker.py --host 127.0.0.1
# Terminal 2:
python scripts/run_master.py --worker-host 127.0.0.1 --size 100000
```

## 5. Benchmark: Speedup & Efficiency

```bash
# 2 PC (Worker sudah jalan di PC2), dari PC1:
python scripts/run_benchmark.py --worker-host 192.168.1.20

# Satu mesin saja (Worker ditanam sebagai thread loopback):
python scripts/run_benchmark.py --embedded-worker
```

Untuk tiap ukuran di `benchmark_sizes` (100rb / 1jt / 10jt, ubah di config bila
perlu) program menjalankan mode sequential lalu distributed pada data yang
sama (seed identik), menghitung `Speedup = T_seq / T_dist` dan
`Efficiency = Speedup/2 × 100%`, menyimpan ke
**`results/benchmark_results.csv`**, dan mencetak tabel ringkasan.

## 6. Mode Sequential Saja

```bash
python scripts/run_sequential.py --size 1000000
```

## 7. Menjalankan Test

```bash
pytest tests/ -v                                # semua unit + integrasi (loopback)
pytest tests/ -q --cov=dnp --cov-report=term    # + coverage (93%)
```

## 8. Struktur Proyek

```
config/config.yaml        # IP, port, seed, ukuran data
src/dnp/                  # protocol, number_processor, data_generator,
                          # master, worker, sequential, cli
scripts/run_*.py          # worker / master / sequential / benchmark
tests/                    # 56 test, pytest
results/benchmark_results.csv
docs/DOCUMENTATION.md     # dokumentasi teknis lengkap
docs/REPORT_OUTLINE.md    # kerangka laporan PDF
```

## 9. Troubleshooting

| Gejala | Solusi |
|---|---|
| `GAGAL: tidak dapat berkomunikasi dengan Worker ...` | Pastikan `run_worker.py` sudah berjalan di PC2; cek IP `--worker-host`; cek firewall/port. |
| `ConnectionRefusedError` padahal Worker jalan | IP salah atau kedua PC beda jaringan; tes `ping <IP PC2>` dari PC1. |
| `Only one usage of each socket address` | Port 5050 dipakai proses lain → `--port 5056` di kedua sisi. |
| Kecepatan terasa lambat pada 10 juta angka | Normal: payload JSON besar; lihat analisis di `docs/DOCUMENTATION.md` §7.2. |
