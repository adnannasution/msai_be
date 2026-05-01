"""
API Server untuk React Native Expo App
Menggantikan fungsi WhatsApp Fonnte dengan REST API langsung ke app.

Endpoint:
  POST /chat              → chatbot (ganti webhook WA)
  GET  /reports           → list semua report (daily/weekly/monthly)
  GET  /reports/<id>      → detail 1 report
  POST /push/register     → daftar Expo push token user
  GET  /health            → health check

Strategi:
  - Chatbot engine diambil dari main_wa.py (langchain + SQLDatabase)
  - Report disimpan ke tabel `reports` oleh masing-masing agent
  - Push notif dikirim via Expo Push API (gratis)
"""

import os, re, threading, requests, psycopg2, psycopg2.extras
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from langchain_community.utilities import SQLDatabase
from langchain_openai import ChatOpenAI

load_dotenv()

DATABASE_URL    = os.getenv("DATABASE_URL", "")
DINOIKI_API_KEY = os.getenv("DINOIKI_API_KEY", "")
PRISMA_URL      = os.getenv("PRISMA_URL", "")
CHATBOT_API_KEY = os.getenv("CHATBOT_API_KEY", "")
APP_SECRET_KEY  = os.getenv("APP_SECRET_KEY", "")   # header auth dari app
PRISMA_HEADERS  = {"x-chatbot-key": CHATBOT_API_KEY}

# ─── DB RAW (untuk reports & push tokens) ────────────────────────────────────
def get_conn():
    url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url, sslmode="require")

def q(sql, params=None, fetch=True):
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params or ())
                if fetch:
                    return [dict(r) for r in cur.fetchall()]
                conn.commit()
                return []
    except Exception as e:
        print(f"  [DB] {e}")
        return []

def execute(sql, params=None):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or ())
            conn.commit()
    except Exception as e:
        print(f"  [DB EXEC] {e}")

# ─── INIT TABEL (auto-create jika belum ada) ──────────────────────────────────
def init_tables():
    execute("""
        CREATE TABLE IF NOT EXISTS reports (
            id         SERIAL PRIMARY KEY,
            type       VARCHAR(10) NOT NULL,  -- 'daily' / 'weekly' / 'monthly'
            content    TEXT        NOT NULL,
            created_at TIMESTAMP   DEFAULT NOW()
        )
    """)
    execute("""
        CREATE TABLE IF NOT EXISTS push_tokens (
            id         SERIAL PRIMARY KEY,
            user_id    VARCHAR(100) NOT NULL UNIQUE,
            token      TEXT         NOT NULL,
            created_at TIMESTAMP    DEFAULT NOW(),
            updated_at TIMESTAMP    DEFAULT NOW()
        )
    """)
    execute("""
        CREATE TABLE IF NOT EXISTS chat_history (
            id         SERIAL PRIMARY KEY,
            user_id    VARCHAR(100) NOT NULL,
            role       VARCHAR(20)  NOT NULL,  -- 'user' / 'assistant'
            content    TEXT         NOT NULL,
            created_at TIMESTAMP    DEFAULT NOW()
        )
    """)
    print("[DB] Tabel reports, push_tokens, chat_history siap.")

# ─── LLM & DB ENGINE ──────────────────────────────────────────────────────────
db_engine = SQLDatabase.from_uri(
    DATABASE_URL.replace("postgres://", "postgresql://", 1),
    sample_rows_in_table_info=0
)

llm = ChatOpenAI(
    model="gpt-4o",
    openai_api_key=DINOIKI_API_KEY,
    base_url="https://ai.dinoiki.com/v1",
    temperature=0.7,
)

# ─── PRISMA INTEGRATION ───────────────────────────────────────────────────────
def fetch_prisma_schema() -> dict:
    if not PRISMA_URL:
        return {}
    try:
        r = requests.get(f"{PRISMA_URL}/chatbot/schema", headers=PRISMA_HEADERS, timeout=15)
        return r.json()
    except Exception as e:
        print(f"[PRISMA] Gagal fetch schema: {e}")
        return {}

def build_prisma_schema_prompt(schema: dict) -> str:
    if not schema or "tables" not in schema:
        return ""
    lines = [
        "TABEL EKSTERNAL PRISMA TA-ex (data procurement material Turnaround):",
        "Untuk pertanyaan tentang material TA, reservasi, PR, PO, work order turnaround — gunakan query_prisma(sql).",
        "Tabel tersedia di PRISMA (BUKAN di database lokal):",
    ]
    for tbl_name, tbl in schema.get("tables", {}).items():
        col_names    = tbl.get("column_names", [])
        desc         = tbl.get("description", "")
        cols_display = ['"order"' if c == "order" else c for c in col_names]
        lines.append(f'- {tbl_name}: {desc}')
        lines.append(f'  kolom: {", ".join(cols_display)}')
    if "join_hints" in schema:
        lines += ["", "JOIN HINTS:"] + [f"  {k}: {v}" for k, v in schema["join_hints"].items()]
    if "status_logic" in schema:
        lines += ["", "STATUS PROCUREMENT:"] + [f"  {k}: {v}" for k, v in schema["status_logic"].items()]
    if "important_notes" in schema:
        lines += ["", "CATATAN PENTING:"] + [f"  - {n}" for n in schema["important_notes"]]
    lines += [
        "",
        "ATURAN QUERY PRISMA:",
        '- Kolom "order" WAJIB ditulis dengan tanda kutip ganda: "order"',
        "- Selalu gunakan LIMIT maksimal 50",
        "- JANGAN query tabel PRISMA ke database lokal — gunakan query_prisma(sql)",
    ]
    return "\n".join(lines)

def query_prisma(sql: str) -> dict:
    if not PRISMA_URL:
        return {"ok": False, "error": "PRISMA_URL belum dikonfigurasi"}
    try:
        r = requests.post(
            f"{PRISMA_URL}/chatbot/query",
            headers=PRISMA_HEADERS,
            json={"sql": sql},
            timeout=30
        )
        return r.json()
    except Exception as e:
        return {"ok": False, "error": str(e)}

PRISMA_SCHEMA        = fetch_prisma_schema()
PRISMA_SCHEMA_PROMPT = build_prisma_schema_prompt(PRISMA_SCHEMA)
PRISMA_TABLES        = set(PRISMA_SCHEMA.get("allowed_tables", [
    "taex_reservasi", "prisma_reservasi", "kumpulan_summary",
    "sap_pr", "sap_po", "work_order"
]))

# ─── SYSTEM PROMPT (sama dengan main_wa.py) ───────────────────────────────────
CUSTOM_PROMPT = f"""You are a PostgreSQL expert and a helpful AI Assistant for a refinery company.
Given an input question, create a syntactically correct PostgreSQL query to run.
HANYA BERIKAN QUERY SQL MURNI, TANPA MARKDOWN ATAU BACKTICK.

Setelah mendapatkan hasil dari database, berikan jawaban akhir dalam Bahasa Indonesia yang profesional.
Format jawaban untuk mobile app: gunakan teks bersih, boleh gunakan emoji, hindari tabel HTML.

STRUKTUR TABEL TERSEDIA:
{{table_info}}

{PRISMA_SCHEMA_PROMPT}

ATURAN QUERY SQL:
- Pilih tabel yang paling relevan berdasarkan nama tabel dan kolom.
- Jika tabel relevan kosong, jawab: "Data belum tersedia."
- Kolom RU antar tabel mungkin berbeda format, gunakan ILIKE '%RU II%' saat JOIN.
- Selalu gunakan NULLIF(kolom_penyebut, 0) untuk menghindari division by zero.
- Gunakan ROUND(nilai::numeric, 2) untuk pembulatan.
- PENTING: Jangan pernah SELECT * tanpa LIMIT. Selalu gunakan agregasi atau LIMIT 20.
- Untuk pertanyaan "tampilkan semua / dump data" → tolak dengan sopan.
- Untuk pertanyaan di luar konteks kilang → jawab: "Maaf, saya hanya membantu analisis data maintenance kilang."
- Sapaan, terima kasih → balas dengan ramah tanpa query SQL.
"""

# ─── CHAT HISTORY ─────────────────────────────────────────────────────────────
MAX_HISTORY = 10

def get_history(user_id: str) -> list:
    rows = q("""
        SELECT role, content FROM chat_history
        WHERE user_id = %s
        ORDER BY created_at DESC
        LIMIT %s
    """, (user_id, MAX_HISTORY * 2))
    # Balik urutan supaya chronological
    return [{"role": r["role"], "content": r["content"]} for r in reversed(rows)]

def add_history(user_id: str, question: str, answer: str):
    execute("INSERT INTO chat_history (user_id, role, content) VALUES (%s, %s, %s)",
            (user_id, "user", question))
    execute("INSERT INTO chat_history (user_id, role, content) VALUES (%s, %s, %s)",
            (user_id, "assistant", answer))

def clear_history(user_id: str):
    execute("DELETE FROM chat_history WHERE user_id = %s", (user_id,))

# ─── CHATBOT ENGINE ───────────────────────────────────────────────────────────
def run_chat(question: str, user_id: str) -> str:
    table_info = db_engine.get_table_info()
    system     = CUSTOM_PROMPT.replace("{table_info}", table_info)
    history    = get_history(user_id)

    messages = [{"role": "system", "content": system}]
    for h in history:
        messages.append({"role": h["role"], "content": h["content"]})

    # Routing: PRISMA atau local DB
    needs_prisma = any(t in question.lower() for t in [t.lower() for t in PRISMA_TABLES])
    prisma_keywords = ["reservasi", "ta-ex", "taex", "material ta", "sap pr", "sap po",
                       "work order", "turnaround", "procurement"]
    if needs_prisma or any(k in question.lower() for k in prisma_keywords):
        sql_messages = messages + [{"role": "user", "content": (
            f"Buat query SQL PostgreSQL untuk tabel PRISMA TA-ex.\n"
            f"Gunakan HANYA tabel yang disebutkan dalam daftar PRISMA.\n"
            f"HANYA output SQL, tanpa penjelasan.\n\nPertanyaan: {question}"
        )}]
        sql_response = llm.invoke(sql_messages)
        sql_query    = sql_response.content.replace("```sql","").replace("```","").strip()
        prisma_result = query_prisma(sql_query)
        if prisma_result.get("ok"):
            db_result = f"Hasil dari PRISMA TA-ex ({prisma_result.get('rows',0)} baris):\n{prisma_result.get('data',[])}"
        else:
            db_result = f"Query PRISMA gagal: {prisma_result.get('error','Unknown error')}"
    else:
        sql_messages = messages + [{"role": "user", "content": (
            f"Berikan HANYA query SQL PostgreSQL yang valid untuk: {question}. "
            f"Tanpa penjelasan, tanpa markdown."
        )}]
        sql_response = llm.invoke(sql_messages)
        sql_query    = sql_response.content.replace("```sql","").replace("```","").strip()
        try:
            db_result = db_engine.run(sql_query)
        except Exception as e:
            db_result = f"Query error: {str(e)}"

    # Generate jawaban final
    answer_messages = messages + [
        {"role": "user", "content": question},
        {"role": "user", "content": (
            f"Hasil query SQL:\n{db_result}\n\n"
            f"Berikan jawaban final dalam Bahasa Indonesia yang profesional. "
            f"Format teks bersih untuk mobile app, boleh gunakan emoji."
        )}
    ]
    final  = llm.invoke(answer_messages)
    answer = final.content.replace("```sql","").replace("```","").strip()
    answer = re.sub(r'\[CHART\].*?\[/CHART\]', '', answer, flags=re.DOTALL)
    answer = re.sub(r'<[^>]+>', '', answer)
    answer = re.sub(r'\[DOWNLOAD:\w+\]', '', answer).strip()

    add_history(user_id, question, answer)
    return answer

# ─── EXPO PUSH NOTIFICATION ───────────────────────────────────────────────────
def send_expo_push(token: str, title: str, body: str, data: dict = None):
    try:
        payload = {
            "to":    token,
            "title": title,
            "body":  body[:200],   # Expo max body
            "sound": "default",
            "data":  data or {},
        }
        resp = requests.post(
            "https://exp.host/--/api/v2/push/send",
            json=payload,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=15
        )
        result = resp.json()
        print(f"  [PUSH] {token[:30]}... → {result}")
        return result
    except Exception as e:
        print(f"  [PUSH ERROR] {e}")
        return {}

def broadcast_push(title: str, body: str, report_id: int, report_type: str):
    """Kirim push ke semua token terdaftar."""
    tokens = q("SELECT token FROM push_tokens")
    for row in tokens:
        send_expo_push(
            token=row["token"],
            title=title,
            body=body[:100] + "..." if len(body) > 100 else body,
            data={"report_id": report_id, "type": report_type}
        )
        import time; time.sleep(0.1)

# ─── FLASK APP ────────────────────────────────────────────────────────────────
app = Flask(__name__)

def check_auth():
    """Cek APP_SECRET_KEY dari header Authorization."""
    if not APP_SECRET_KEY:
        return True   # jika tidak di-set, bypass (dev mode)
    auth = request.headers.get("Authorization", "")
    return auth == f"Bearer {APP_SECRET_KEY}"

# ── CHATBOT ──────────────────────────────────────────────────────────────────
@app.route("/chat", methods=["POST"])
def chat():
    if not check_auth():
        return jsonify({"error": "Unauthorized"}), 401

    body    = request.get_json(force=True, silent=True) or {}
    user_id = body.get("user_id", "").strip()
    message = body.get("message", "").strip()

    if not user_id or not message:
        return jsonify({"error": "user_id dan message wajib diisi"}), 400

    # Reset history
    if message.lower() in ["/reset", "reset", ".reset"]:
        clear_history(user_id)
        return jsonify({"reply": "🔄 Percakapan direset. Memori sesi sebelumnya dihapus."}), 200

    try:
        reply = run_chat(message, user_id)
        return jsonify({"reply": reply}), 200
    except Exception as e:
        print(f"[CHAT ERROR] {e}")
        return jsonify({"error": "Gagal memproses pertanyaan.", "detail": str(e)}), 500

# ── REPORTS ───────────────────────────────────────────────────────────────────
@app.route("/reports", methods=["GET"])
def list_reports():
    if not check_auth():
        return jsonify({"error": "Unauthorized"}), 401

    report_type = request.args.get("type")     # filter: daily/weekly/monthly
    limit       = int(request.args.get("limit", 20))
    offset      = int(request.args.get("offset", 0))

    if report_type:
        rows = q("""
            SELECT id, type, LEFT(content, 200) AS preview, created_at
            FROM reports
            WHERE type = %s
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """, (report_type, limit, offset))
    else:
        rows = q("""
            SELECT id, type, LEFT(content, 200) AS preview, created_at
            FROM reports
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """, (limit, offset))

    return jsonify({"reports": rows, "count": len(rows)}), 200

@app.route("/reports/<int:report_id>", methods=["GET"])
def get_report(report_id):
    if not check_auth():
        return jsonify({"error": "Unauthorized"}), 401

    rows = q("SELECT id, type, content, created_at FROM reports WHERE id = %s", (report_id,))
    if not rows:
        return jsonify({"error": "Report tidak ditemukan"}), 404
    return jsonify(rows[0]), 200

@app.route("/reports/latest/<report_type>", methods=["GET"])
def get_latest_report(report_type):
    """Ambil report terbaru berdasarkan type."""
    if not check_auth():
        return jsonify({"error": "Unauthorized"}), 401

    if report_type not in ("daily", "weekly", "monthly"):
        return jsonify({"error": "type harus daily/weekly/monthly"}), 400

    rows = q("""
        SELECT id, type, content, created_at FROM reports
        WHERE type = %s
        ORDER BY created_at DESC
        LIMIT 1
    """, (report_type,))
    if not rows:
        return jsonify({"error": f"Belum ada report {report_type}"}), 404
    return jsonify(rows[0]), 200

# ── PUSH TOKEN REGISTRATION ───────────────────────────────────────────────────
@app.route("/push/register", methods=["POST"])
def register_push():
    if not check_auth():
        return jsonify({"error": "Unauthorized"}), 401

    body    = request.get_json(force=True, silent=True) or {}
    user_id = body.get("user_id", "").strip()
    token   = body.get("token", "").strip()

    if not user_id or not token:
        return jsonify({"error": "user_id dan token wajib diisi"}), 400

    execute("""
        INSERT INTO push_tokens (user_id, token, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (user_id)
        DO UPDATE SET token = EXCLUDED.token, updated_at = NOW()
    """, (user_id, token))

    return jsonify({"status": "ok", "message": "Token berhasil didaftarkan"}), 200

@app.route("/push/unregister", methods=["POST"])
def unregister_push():
    if not check_auth():
        return jsonify({"error": "Unauthorized"}), 401

    body    = request.get_json(force=True, silent=True) or {}
    user_id = body.get("user_id", "").strip()
    if not user_id:
        return jsonify({"error": "user_id wajib diisi"}), 400

    execute("DELETE FROM push_tokens WHERE user_id = %s", (user_id,))
    return jsonify({"status": "ok"}), 200

# ── HEALTH ────────────────────────────────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "API Server is running 🚀"}), 200

# ─── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    init_tables()
    port = int(os.getenv("PORT", 5000))
    print(f"🚀 API Server berjalan di port {port}...")
    app.run(host="0.0.0.0", port=port, threaded=True)
