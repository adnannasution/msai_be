"""
report_helper.py
Diimport oleh report_agent.py, weekly_report_agent.py, monthly_report_agent.py
Fungsi: simpan report ke tabel `reports` + kirim Expo push notif ke semua device
"""

import os, requests, psycopg2, psycopg2.extras
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL", "")

def get_conn():
    url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url, sslmode="require")

def _execdb(sql, params=None):
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params or ())
                result = cur.fetchall() if cur.description else []
            conn.commit()
        return [dict(r) for r in result]
    except Exception as e:
        print(f"[report_helper DB] {e}")
        return []

def _get_tokens() -> list:
    rows = _execdb("SELECT DISTINCT expo_token FROM device_tokens")
    return [r["expo_token"] for r in rows]

def send_push_to_all(title: str, body: str, report_id: int = None):
    tokens = _get_tokens()
    if not tokens:
        print("[PUSH] Tidak ada device terdaftar, skip.")
        return
    messages = [
        {
            "to":    token,
            "title": title,
            "body":  body,
            "sound": "default",
            "data":  {"report_id": report_id} if report_id else {}
        }
        for token in tokens
    ]
    try:
        resp = requests.post(
            "https://exp.host/--/api/v2/push/send",
            json=messages,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=30
        )
        print(f"[PUSH] ✅ Terkirim ke {len(tokens)} device — HTTP {resp.status_code}")
    except Exception as e:
        print(f"[PUSH] ❌ Error: {e}")

def save_report(type_: str, content: str, periode: str = "") -> int:
    """
    Simpan report ke tabel `reports` dan kirim push notif.

    Args:
        type_   : 'daily' / 'weekly' / 'monthly'
        content : teks report lengkap
        periode : label periode, e.g. "30 April 2026" / "April 2026"

    Returns:
        id report yang tersimpan (int), atau 0 jika gagal
    """
    rows = _execdb(
        "INSERT INTO reports (type, content, periode) VALUES (%s, %s, %s) RETURNING id",
        (type_, content, periode)
    )
    report_id = rows[0]["id"] if rows else 0
    print(f"[report_helper] ✅ Report '{type_}' disimpan — id={report_id}")

    title_map = {
        "daily":   "📊 Daily Report Tersedia",
        "weekly":  "📘 Weekly Executive Review Tersedia",
        "monthly": "📙 Monthly Management Review Tersedia",
    }
    title = title_map.get(type_, "📋 Report Baru Tersedia")
    body  = f"Periode: {periode}" if periode else "Laporan terbaru sudah tersedia."
    send_push_to_all(title, body, report_id)
    return report_id
