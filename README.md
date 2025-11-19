# Keyword-Based Domain Crawler

Web crawler untuk mencari dan mengumpulkan informasi domain terkait perjudian menggunakan search engine DuckDuckGo.

## Fitur

- Pencarian domain perjudian melalui DuckDuckGo API
- Anti-duplikasi domain secara otomatis
- Pemfilteran domain umum (YouTube, Wikipedia, Facebook, dll)
- Ekstraksi metadata Open Graph (OG)
- Tangkap screenshot halaman
- Multithreading untuk HTTP request dan multiprocessing untuk screenshot
- ID sekuensial otomatis untuk setiap domain
- Output JSON dengan metadata dan status

## Instalasi

1. Clone repository:
```bash
git clone https://github.com/FechL/keyword-based-crawler.git
cd keyword-based-crawler
```
2. Install dependencies:
```bash
pip install -r requirements.txt
```
3. Install Chromium dan ChromeDriver:
```bash
sudo apt-get install chromium-browser chromium-chromedriver
```
## Penggunaan
```bash
python crawler.py
```
Program akan meminta keyword pencarian. Hasil akan disimpan di folder output.

## Konfigurasi

Edit bagian Configuration di gambling_crawling.py:

- FETCH_TIMEOUT = 10 (detik)
- SCREENSHOT_TIMEOUT = 20 (detik)
- MAX_RESULT = 10 (jumlah domain per run)
- MAX_WORKERS_FETCH = 5 (concurrent workers)

File Penting

- gambling_crawling.py: Script utama
- blocked_domains.txt: Daftar domain yang diblokir
- output/all_domains.txt: Riwayat domain yang sudah diproses
- output/last_id.txt: ID terakhir yang digunakan
- output/*.json: Hasil crawling per run
- output/img/: Folder screenshot

## Output

Setiap run menghasilkan file JSON dengan format timestamp (DDMMYY-HHMM.json) berisi:
- Metadata: total records, timestamp, version, keyword
- Data: array domain dengan ID, URL, OG metadata, status screenshot

## Troubleshooting

Jika tidak ada hasil: Periksa koneksi internet dan DuckDuckGo API

Jika timeout: Naikkan FETCH_TIMEOUT atau SCREENSHOT_TIMEOUT di configuration

Jika screenshot gagal: Pastikan Chromium dan ChromeDriver terinstall dengan versi yang cocok

Lihat CONFIG_REFERENCE.md untuk detail lengkap konfigurasi.
