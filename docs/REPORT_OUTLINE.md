# REPORT_OUTLINE.md — Outline Laporan PDF (maks. 10 halaman)

Kerangka laporan Tugas 1 **Distributed Number Processing (2 PC)**. Isi laporan
dibuat terpisah dari dokumen kerja ini; data kuantitatif diambil dari
`results/benchmark_results.csv` dan tangkapan layar output `run_master.py`.

## Halaman 1 — Identitas & Ringkasan
- Judul, nama/NIM, mata kuliah, tanggal.
- Abstrak singkat (5–7 kalimat): apa yang dibangun, bagaimana (TCP socket,
  2 PC), hasil utama (speedup/efficiency), kesimpulan.

## Halaman 2 — Dasar Teori
- Distributed memory vs shared memory.
- Model master/worker, komunikasi pesan (message passing), TCP socket &
  framing.
- Definisi Speedup dan Efficiency (hukum Amdahl disinggung sekilas).

## Halaman 3 — Perancangan Sistem
- Arsitektur 2 PC (salin diagram dari `PLAN.md` §3 / `DOCUMENTATION.md` §5).
- Pembagian tugas PC1 (coordinator) vs PC2 (processor).
- Strategi split data 50:50 dan proses konkuren (thread) — rujuk
  `docs/DOCUMENTATION.md` §3.

## Halaman 4 — Protokol Komunikasi
- Format pesan JSON + length-prefixed framing 4 byte.
- Contoh payload nyata `data_chunk` dan `process_result`.
- Urutan handshake (ping/pong opsional, kirim, proses, balas).

## Halaman 5 — Logika Penggabungan Hasil
- Rumus merge sum/count/min/max dan average; bukti korektnya (sifat
  combinable) + rujukan ke unit test `tests/test_number_processor.py`.

## Halaman 6 — Implementasi
- Struktur folder aktual + tanggung jawab tiap modul (ringkas dari
  `DOCUMENTATION.md` §1–§2).
- Cuplikan kode inti: `process_numbers`, `merge_results`, `send_message`.

## Halaman 7 — Pengujian & Hasil
- Screenshot output wajib `run_master.py` (PC1 dan PC2 berdampingan).
- Tabel benchmark: ukuran data vs T_sequential vs T_distributed vs
  speedup vs efficiency (dari `results/benchmark_results.csv`).

## Halaman 8 — Analisis
- Interpretasi speedup/efficiency: overhead serialisasi JSON & jaringan vs
  waktu komputasi yang sangat kecil; kenapa speedup < 1 valid dan apa yang
  dibutuhkan agar paralelisasi menguntungkan (komputasi berat / data sangat
  besar / PC heterogen).
- Perbandingan loopback vs 2 PC nyata (bila ada).

## Halaman 9 — Keterbatasan & Pengembangan
- Split statis, single worker, tanpa fault-tolerance (dari
  `DOCUMENTATION.md` §8).

## Halaman 10 — Penutup
- Kesimpulan (poin 1–6 ketentuan tugas terpenuhi), referensi
  (dokumentasi Python socket, materi kuliah).
- Lampiran seperlunya: perintah eksekusi, konfigurasi jaringan.
