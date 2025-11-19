#!/usr/bin/env python3
"""
Configuration Reference for gambling_crawling.py

TIMEOUT CONFIGURATION (seconds):
- FETCH_TIMEOUT: Timeout untuk HTTP request ketika fetch URL (default: 10)
- OG_TIMEOUT: Timeout untuk OG metadata extraction (default: 10)
- SCREENSHOT_TIMEOUT: Timeout untuk screenshot capture (default: 30)

Catatan: Jika terjadi timeout, program TIDAK akan retry. Lanjutkan ke URL berikutnya.

PROCESSING CONFIGURATION:
- MAX_RESULT: Jumlah maksimum domain VALID yang diproses per run (default: 10)
- MAX_WORKERS_FETCH: Jumlah worker untuk multithreading HTTP fetch (default: 5)
- MAX_WORKERS_SCREENSHOT: Jumlah worker untuk multiprocessing screenshot (default: cpu_count - 1)

FILE CONFIGURATION:
- OUTPUT_DIR: Direktori output utama (default: /home/aliy/Coding/crawler/output)
- OUTPUT_IMG_DIR: Direktori penyimpanan screenshot (default: output/img)
- LAST_ID_FILE: File tracking ID terakhir (default: output/last_id.txt)
- ALL_DOMAINS_FILE: File tracking semua domain yang pernah di-generate (default: output/all_domains.txt)
- BLOCKED_DOMAINS_FILE: File daftar domain yang diblokir (default: blocked_domains.txt di root)

LOGIC FLOW:
1. Load existing domains dari all_domains.txt
2. Load blocked domains dari blocked_domains.txt
3. Get search results dari DDGS (max 50 hasil untuk handling filter)
4. Filter results dengan kriteria:
   - Skip jika domain invalid ("unknown")
   - Skip jika domain ada di blocked list (substring match)
   - Skip jika domain sudah pernah di-generate (duplikat)
   - Accept jika semua check pass
5. Lanjutkan ke domain berikutnya sampai MAX_RESULT domain valid ditemukan
6. Fetch URL data dengan multithreading (timeout tanpa retry)
7. Take screenshot dengan multiprocessing (timeout tanpa retry)
8. Save hasil ke JSON dengan timestamp
9. Append domain baru ke all_domains.txt
10. Update last_id.txt

SKIP MESSAGES:
- [SKIP] Invalid domain: URL tidak valid/tidak bisa extract domain
- [SKIP] Blocked domain: Domain ada di daftar blocked
- [SKIP] Domain duplicate: Domain sudah pernah di-generate
- [OK] Added domain: Domain diterima dan akan diproses
"""
