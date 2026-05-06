"""
API Server untuk React Native Android App
Chatbot engine diambil langsung dari main_wa.py — tanpa WA/Fonnte.

Endpoint:
  GET  /health                  → health check
  POST /chat                    → chatbot (dari Android)
  GET  /reports                 → list semua report
  GET  /reports/latest/<type>   → report terbaru by type
  GET  /reports/<id>            → detail 1 report
  POST /push/register           → daftar Expo push token
  POST /push/unregister         → hapus Expo push token
"""

import os, re, requests, psycopg2, psycopg2.extras
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from langchain_community.utilities import SQLDatabase
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage

load_dotenv()

DATABASE_URL    = os.getenv("DATABASE_URL", "")
DINOIKI_API_KEY = os.getenv("DINOIKI_API_KEY", "")
PRISMA_URL      = os.getenv("PRISMA_URL", "")
CHATBOT_API_KEY = os.getenv("CHATBOT_API_KEY", "")
APP_SECRET_KEY  = os.getenv("APP_SECRET_KEY", "")
PRISMA_HEADERS  = {"x-chatbot-key": CHATBOT_API_KEY}

# ─── DB (psycopg2 untuk reports & push_tokens) ───────────────────────────────
def get_conn():
    url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url, sslmode="require")

def q(sql, params=None):
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params or ())
                return [dict(r) for r in cur.fetchall()]
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

def init_tables():
    execute("""
        CREATE TABLE IF NOT EXISTS reports (
            id         SERIAL PRIMARY KEY,
            type       VARCHAR(10) NOT NULL,
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
    print("[DB] Tabel reports & push_tokens siap.")

# ─── LLM & DB ENGINE (langchain) ─────────────────────────────────────────────
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
        "- JANGAN query tabel PRISMA ke database lokal",
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

# ─── SYSTEM PROMPT (dari main_wa.py, format mobile — tanpa format WA) ────────
CUSTOM_PROMPT = """You are a PostgreSQL expert and a helpful AI Assistant for a refinery company.
Given an input question, create a syntactically correct PostgreSQL query to run.
HANYA BERIKAN QUERY SQL MURNI, TANPA MARKDOWN ATAU BACKTICK.

Setelah mendapatkan hasil dari database, berikan jawaban akhir dalam Bahasa Indonesia yang profesional.

STRUKTUR TABEL TERSEDIA:
{table_info}

ATURAN QUERY SQL:
- Pilih tabel yang paling relevan berdasarkan nama tabel dan kolom yang tersedia.
- Jika tabel relevan kosong, jawab: "Data belum tersedia, silakan upload datanya terlebih dahulu."
- Kolom RU antar tabel mungkin berbeda format, gunakan ILIKE '%RU II%' saat JOIN.
- Selalu gunakan NULLIF(kolom_penyebut, 0) untuk menghindari division by zero.
- Gunakan ROUND(nilai::numeric, 2) untuk pembulatan.
- PENTING: Jangan pernah SELECT * tanpa LIMIT. Selalu gunakan agregasi atau LIMIT 20.
- Untuk pertanyaan "tampilkan semua / dump data" → tolak dengan sopan.
- Untuk pertanyaan di luar konteks kilang → jawab: "Maaf, saya hanya membantu analisis data maintenance kilang."
- Sapaan, terima kasih → balas dengan ramah tanpa query SQL.
- Untuk icu_monitoring: kolom utama adalah ru, icu_status, tag_no, issue, mitigation, progress, target_closed, report_date.
- Untuk paf: Plant Availability Factor — kolom type, ru, target_realisasi, value, plan_unplan, month.
- Untuk zero_clamp: kolom ru, area, unit, tag_no_ln, type_damage, status, tanggal_dipasang.
- Untuk power_stream: kolom refinery_unit, type_equipment, equipment, status_operation, desain, kapasitas_max, average_actual.
- Untuk readiness_jetty: kolom refinery_unit, tag_no, status_operation, status_tuks, status_ijin_ops, status_isps, month_update.
- Untuk readiness_tank: kolom refinery_unit, tag_number, status_operational, status_coi, status_atg, month_update.
- Untuk readiness_spm: kolom refinery_unit, tag_no, status_operation, status_laik_operasi, status_ijin_spl, month_update.
- Untuk atg_monitoring: kolom refinery_unit, tag_no_tangki, tag_no_atg, status_atg, status_interkoneksi_atg, month_update.
- Untuk bad_actor_monitoring: kolom ru, tag_number, status, problem, action_plan, progress, target_date, periode.
- Untuk pipeline_inspection: kolom refinery_unit, tag_number, fluida_service, rem_life_years, jumlah_temporary_repair, bulan, tahun.
- Untuk monitoring_operasi: kolom refinery_unit, unit_proses, actual, target_sts, limitasi_alert_process, month_update.
- Untuk anggaran_maintenance: kolom ru, tahun, kategori, tipe, nilai_usd. Tampilkan dengan format USD.
- Untuk tkdn: kolom refinery_unit, bulan, nominal, kdn, persentase, tahun. Tampilkan dengan format Rp.
- Untuk rcps: kolom kilang, traffic, judul_rcps, rcps_no, criticallity.
- Untuk irkap_program: kolom refinery_unit, no_program_kerja, program_kerja, status_step, status_prognosa, nilai_anggaran_idr.
- Untuk master_data_equipment: master data equipment dari SAP IH08 — berisi semua equipment yang terdaftar di sistem. KOLOM YANG TERSEDIA: criticality (A/B/C/Z), equipment (nomor SAP), functional_location, maintenance_plant, location (kode RU/lokasi), cost_center, wbs_element, main_work_center, planner_group, planning_plant, catalog_profile, equipment_category, description (deskripsi teknis), manufacturer, model_type, serial_number, changed_by, changed_on, created_by, created_on, technical_obj_type, manufact_serial_number, manufacturer_drawing_number, manufacturer_part_number, material, material_description, order_no, size_dimension, sort_field_ata. Contoh query: jumlah equipment per criticality, list equipment berdasarkan functional_location, cari by description atau manufacturer. Filter criticality: WHERE criticality = 'A'.

{prisma_schema}

ATURAN FORMAT JAWABAN (MOBILE APP):
1. Jawaban narasi yang jelas dan mudah dibaca di layar HP.
2. Gunakan poin-poin dengan tanda • jika data lebih dari satu.
3. Tebalkan poin penting dengan *teks*.
4. Tambahkan emoji relevan (🏭, 💰, 📊, ✅, ⚠️, 🔧, 🛢️, 🚨, 🔴).
5. Gunakan angka dengan format mudah dibaca (1.234.567 atau Rp 1,2 M).
6. Maksimal 10 item — jika lebih, tampilkan highlight saja.

Question: {input}"""

# ─── CHAT HISTORY (in-memory per user_id) ────────────────────────────────────
MAX_HISTORY = 10
chat_histories: dict[str, list] = {}

def get_history(user_id: str) -> list:
    return chat_histories.get(user_id, [])

def add_history(user_id: str, question: str, answer: str):
    history = chat_histories.get(user_id, [])
    history.append(HumanMessage(content=question))
    history.append(AIMessage(content=answer))
    if len(history) > MAX_HISTORY:
        history = history[-MAX_HISTORY:]
    chat_histories[user_id] = history

def clear_history(user_id: str):
    chat_histories.pop(user_id, None)

# ─── CHATBOT ENGINE (dari main_wa.py, tanpa WA) ───────────────────────────────
def run_chat(question: str, user_id: str) -> str:
    history    = get_history(user_id)
    table_info = db_engine.get_table_info()

    prisma_prompt = PRISMA_SCHEMA_PROMPT or "(PRISMA schema belum tersedia)"
    _prompt = (CUSTOM_PROMPT
        .replace("{table_info}", table_info)
        .replace("{prisma_schema}", prisma_prompt)
        .replace("{input}", "")
        .replace("{{", "{").replace("}}", "}")
    )

    messages = [{"role": "system", "content": _prompt}]
    for msg in history:
        if isinstance(msg, HumanMessage):
            messages.append({"role": "user", "content": msg.content})
        elif isinstance(msg, AIMessage):
            messages.append({"role": "assistant", "content": msg.content})

    # ── Intent detection ──
    _q_lower = question.lower()
    _SPESIFIK_KEYWORDS = [
        "pipeline", "atg", "metering", "rotor", "icu", "bad actor", "paf",
        "zero clamp", "power stream", "anggaran", "tkdn", "rcps", "boc",
        "readiness jetty", "readiness tank", "readiness spm",
        "workplan jetty", "workplan tank", "spm workplan",
        "inspection plan", "monitoring operasi", "irkap", "prokja",
        "reservasi", "turnaround", "inspeksi", "realisasi",
        "bandingkan", "program kerja", "anggaran maintenance",
        "master data", "master data equipment", "equipment master",
    ]
    _SAPAAN_KEYWORDS = [
        "halo", "hai", "hello", "hi ", "selamat pagi", "selamat siang",
        "selamat sore", "selamat malam", "terima kasih", "makasih", "thanks",
        "apa yang bisa", "kamu bisa apa", "kemampuan", "siapa kamu",
    ]

    if any(kw in _q_lower for kw in _SAPAAN_KEYWORDS) and not any(kw in _q_lower for kw in _SPESIFIK_KEYWORDS):
        intent = "SAPAAN"
    elif any(kw in _q_lower for kw in _SPESIFIK_KEYWORDS):
        intent = "SPESIFIK"
    else:
        history_context = ""
        if history:
            last_msgs = history[-4:]
            history_context = "\n".join([
                f"{'User' if isinstance(m, HumanMessage) else 'Bot'}: {m.content[:200]}"
                for m in last_msgs
            ])
        intent_check = llm.invoke([{
            "role": "user",
            "content": (
                f"Konteks percakapan sebelumnya:\n{history_context}\n\n"
                f"Klasifikasikan pertanyaan berikut:\n"
                f"1. SAPAAN — sapaan, terima kasih, tanya kemampuan AI\n"
                f"2. SPESIFIK — menyebut nama tabel/data kilang secara eksplisit\n"
                f"3. AMBIGU — tidak menyebut nama tabel spesifik\n"
                f"Jawab hanya satu kata: SAPAAN, SPESIFIK, atau AMBIGU\n\nPertanyaan: {question}"
            )
        }])
        intent = intent_check.content.strip().upper()

    if "SAPAAN" in intent:
        resp = llm.invoke(messages + [{"role": "user", "content": question}])
        return resp.content

    if "AMBIGU" in intent:
        history_context = ""
        if history:
            history_context = "\n".join([
                f"{'User' if isinstance(m, HumanMessage) else 'Bot'}: {m.content[:200]}"
                for m in history[-4:]
            ])
        clarify = llm.invoke([{
            "role": "user",
            "content": (
                f"Riwayat:\n{history_context}\n\n"
                f"Pertanyaan: {question}\n\n"
                f"Pertanyaan ini kurang lengkap. Identifikasi apa yang kurang "
                f"lalu buat satu kalimat tanya yang natural dalam Bahasa Indonesia. Singkat dan ramah."
            )
        }])
        return clarify.content.strip()

    # ── Cek PRISMA ──
    prisma_check = llm.invoke([{
        "role": "user",
        "content": (
            f"Apakah pertanyaan berikut berkaitan dengan data PRISMA TA-ex "
            f"(reservasi, material TA, PR, PO, work order turnaround, procurement)? "
            f"Jawab hanya YA atau TIDAK.\n\nPertanyaan: {question}"
        )
    }])
    is_prisma = "YA" in prisma_check.content.strip().upper()

    if is_prisma and PRISMA_URL:
        SIMPLE_PATTERNS = ["berapa", "total", "jumlah", "rangkuman", "ringkasan", "summary", "status"]
        COMPLEX_PATTERNS = ["per equipment", "per order", "nilai po", "net price", "harga", "breakdown", "detail"]
        is_simple  = any(p in _q_lower for p in SIMPLE_PATTERNS)
        is_complex = any(p in _q_lower for p in COMPLEX_PATTERNS)

        if is_simple and not is_complex:
            params = {"chatbot_key": CHATBOT_API_KEY}
            if "belum pr" in _q_lower:   params["status"] = "no-pr"
            elif "sudah pr" in _q_lower: params["status"] = "pr-created"
            elif "sudah po" in _q_lower: params["status"] = "po-created"
            elif "partial"  in _q_lower: params["status"] = "partial"
            elif "complete" in _q_lower: params["status"] = "complete"
            if any(k in _q_lower for k in ["rangkuman", "ringkasan", "summary", "total", "berapa"]):
                params["summary_only"] = "true"
            try:
                r = requests.get(f"{PRISMA_URL}/chatbot/tracking", params=params, timeout=30)
                db_result = f"Hasil PRISMA (jalur sederhana):\n{r.json()}"
            except Exception as e:
                db_result = f"Gagal fetch PRISMA: {str(e)}"
        else:
            sql_messages = messages + [{"role": "user", "content": (
                f"Buat query SQL untuk tabel PRISMA TA-ex. "
                f"Kolom 'order' WAJIB pakai tanda kutip ganda. LIMIT 50. "
                f"HANYA SQL murni.\n\nPertanyaan: {question}"
            )}]
            sql_resp  = llm.invoke(sql_messages)
            sql_query = sql_resp.content.replace("```sql","").replace("```","").strip()
            result    = query_prisma(sql_query)
            if result.get("ok"):
                db_result = f"Hasil PRISMA ({result.get('rows',0)} baris):\n{result.get('data',[])}"
            else:
                db_result = f"Query PRISMA gagal: {result.get('error','Unknown error')}"
    else:
        # ── Local DB ──
        sql_messages = messages + [{"role": "user", "content": (
            f"Berikan HANYA query SQL PostgreSQL yang valid untuk: {question}. "
            f"Tanpa penjelasan, tanpa markdown."
        )}]
        sql_resp  = llm.invoke(sql_messages)
        sql_query = sql_resp.content.replace("```sql","").replace("```","").strip()
        try:
            db_result = db_engine.run(sql_query)
        except Exception as e:
            db_result = f"Query error: {str(e)}"

    # ── Generate jawaban final ──
    answer_messages = messages + [
        {"role": "user", "content": question},
        {"role": "user", "content": (
            f"Hasil query:\n{db_result}\n\n"
            f"Berikan jawaban final dalam Bahasa Indonesia yang profesional. "
            f"Format teks bersih untuk mobile app, boleh gunakan emoji dan poin •."
        )}
    ]
    final  = llm.invoke(answer_messages)
    answer = final.content.replace("```sql","").replace("```","").strip()
    answer = re.sub(r'\[CHART\].*?\[/CHART\]', '', answer, flags=re.DOTALL)
    answer = re.sub(r'<[^>]+>', '', answer)
    answer = re.sub(r'\[DOWNLOAD:\w+\]', '', answer).strip()

    add_history(user_id, question, answer)
    return answer

# ─── AUTH ─────────────────────────────────────────────────────────────────────
def check_auth():
    if not APP_SECRET_KEY:
        return True
    return request.headers.get("Authorization","") == f"Bearer {APP_SECRET_KEY}"

# ─── FLASK APP ────────────────────────────────────────────────────────────────
app = Flask(__name__)

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "API Server is running 🚀"}), 200

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

    if message.lower() in ["/reset", "reset", ".reset"]:
        clear_history(user_id)
        return jsonify({"reply": "🔄 Percakapan direset. Silakan ajukan pertanyaan baru."}), 200

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

    report_type = request.args.get("type")
    limit       = int(request.args.get("limit", 20))
    offset      = int(request.args.get("offset", 0))

    if report_type:
        rows = q("""
            SELECT id, type, LEFT(content, 200) AS preview, created_at
            FROM reports WHERE type = %s
            ORDER BY created_at DESC LIMIT %s OFFSET %s
        """, (report_type, limit, offset))
    else:
        rows = q("""
            SELECT id, type, LEFT(content, 200) AS preview, created_at
            FROM reports ORDER BY created_at DESC LIMIT %s OFFSET %s
        """, (limit, offset))

    return jsonify({"reports": rows, "count": len(rows)}), 200

@app.route("/reports/latest/<report_type>", methods=["GET"])
def get_latest_report(report_type):
    if not check_auth():
        return jsonify({"error": "Unauthorized"}), 401
    if report_type not in ("daily", "weekly", "monthly"):
        return jsonify({"error": "type harus daily/weekly/monthly"}), 400
    rows = q("""
        SELECT id, type, content, created_at FROM reports
        WHERE type = %s ORDER BY created_at DESC LIMIT 1
    """, (report_type,))
    if not rows:
        return jsonify({"error": f"Belum ada report {report_type}"}), 404
    return jsonify(rows[0]), 200

@app.route("/reports/<int:report_id>", methods=["GET"])
def get_report(report_id):
    if not check_auth():
        return jsonify({"error": "Unauthorized"}), 401
    rows = q("SELECT id, type, content, created_at FROM reports WHERE id = %s", (report_id,))
    if not rows:
        return jsonify({"error": "Report tidak ditemukan"}), 404
    return jsonify(rows[0]), 200

# ── PUSH TOKEN ────────────────────────────────────────────────────────────────
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
        INSERT INTO push_tokens (user_id, token, updated_at) VALUES (%s, %s, NOW())
        ON CONFLICT (user_id) DO UPDATE SET token = EXCLUDED.token, updated_at = NOW()
    """, (user_id, token))
    return jsonify({"status": "ok"}), 200

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


# ─── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    init_tables()
    port = int(os.getenv("PORT", 5000))
    print(f"🚀 API Server berjalan di port {port}...")

    app.run(host="0.0.0.0", port=port, threaded=True)