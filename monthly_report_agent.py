"""
Monthly Management Review Agent
- Standalone Railway service (worker)
- Query langsung ke PostgreSQL (Railway) — tabel sama dengan daily & weekly
- Generate report via Dinoiki (OpenAI-compatible endpoint)
- Kirim WhatsApp via Fonnte setiap tanggal 1 jam 06.00 WIB (default)
- Format: MONTHLY MANAGEMENT REVIEW — 9 seksi

Strategi ambil data:
  Semua tabel → ambil MAX(kolom_tanggal) = laporan bulan terbaru.
  Untuk trend  → ambil 2 snapshot terbaru (current vs bulan sebelumnya).
"""

import os, time, json, requests, psycopg2, psycopg2.extras, schedule
from openai import OpenAI
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL          = os.getenv("DATABASE_URL", "")
FONNTE_TOKEN          = os.getenv("FONNTE_TOKEN", "")
DINOIKI_API_KEY       = os.getenv("DINOIKI_API_KEY", "")
WA_TARGETS            = os.getenv("MONTHLY_WA_TARGETS", os.getenv("REPORT_WA_TARGETS", ""))
MONTHLY_SEND_TIME_UTC = os.getenv("MONTHLY_SEND_TIME_UTC", "23:00")  # 23:00 UTC = 06:00 WIB
MONTHLY_SEND_DAY      = int(os.getenv("MONTHLY_SEND_DAY", "1"))       # tanggal berapa tiap bulan
WIB                   = timezone(timedelta(hours=7))

# ─── LLM ─────────────────────────────────────────────────────────────────────
llm = OpenAI(
    api_key=DINOIKI_API_KEY,
    base_url="https://ai.dinoiki.com/v1"
)

def ask_llm(prompt: str) -> str:
    resp = llm.chat.completions.create(
        model="gpt-4o",
        temperature=0.3,
        max_tokens=4500,
        messages=[{"role": "user", "content": prompt}]
    )
    return resp.choices[0].message.content

# ─── DB ───────────────────────────────────────────────────────────────────────
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
        print(f"  [DB WARN] {e}")
        return []

# ─── DATA GATHERING (MONTHLY) ─────────────────────────────────────────────────
def gather_monthly_data():
    data = {}

    # 1. BAD ACTOR
    # current → periode terbaru (= laporan bulan ini)
    # prev    → periode sebelumnya (= laporan bulan lalu, untuk trend)
    data["bad_actor_current"] = q("""
        SELECT ru, tag_number, status, problem, action_plan,
               progress, target_date, periode
        FROM bad_actor_monitoring
        WHERE periode = (SELECT MAX(periode) FROM bad_actor_monitoring)
          AND LOWER(COALESCE(status,'')) NOT IN ('closed','complete','selesai','done')
        ORDER BY ru, tag_number
        LIMIT 30
    """)

    data["bad_actor_prev"] = q("""
        SELECT tag_number, status, progress, periode
        FROM bad_actor_monitoring
        WHERE periode = (
            SELECT MAX(periode) FROM bad_actor_monitoring
            WHERE periode < (SELECT MAX(periode) FROM bad_actor_monitoring)
        )
        ORDER BY tag_number
        LIMIT 30
    """)

    # 2. ICU
    # current → report_date terbaru (= laporan bulan ini)
    # prev    → report_date terbaru sebelumnya (= laporan bulan lalu, untuk trend)
    data["icu_current"] = q("""
        SELECT ru, tag_no, icu_status, issue, mitigation,
               progress, target_closed, report_date
        FROM icu_monitoring
        WHERE report_date = (SELECT MAX(report_date) FROM icu_monitoring)
          AND LOWER(COALESCE(icu_status,'')) NOT IN ('closed','resolved','selesai')
        ORDER BY ru, tag_no
        LIMIT 25
    """)

    data["icu_prev"] = q("""
        SELECT ru, tag_no, icu_status, issue, progress, report_date
        FROM icu_monitoring
        WHERE report_date = (
            SELECT MAX(report_date) FROM icu_monitoring
            WHERE report_date < (SELECT MAX(report_date) FROM icu_monitoring)
        )
          AND LOWER(COALESCE(icu_status,'')) NOT IN ('closed','resolved','selesai')
        ORDER BY ru, tag_no
        LIMIT 25
    """)

    # 3. ZERO CLAMP — semua yang masih terpasang
    data["zero_clamp"] = q("""
        SELECT ru, area, unit, tag_no_ln, services, description,
               type_damage, tanggal_dipasang, tanggal_rencana_perbaikan,
               status, remarks
        FROM zero_clamp
        WHERE tanggal_dilepas IS NULL OR TRIM(COALESCE(tanggal_dilepas,'')) = ''
        ORDER BY tanggal_dipasang ASC NULLS LAST
        LIMIT 20
    """)

    # 4. PIPELINE — bulan+tahun terbaru
    data["pipeline"] = q("""
        SELECT refinery_unit, tag_number, fluida_service, nps,
               from_location, to_location, last_measured_thickness,
               rem_life_years, jumlah_temporary_repair, remarks,
               next_inspection_date, bulan, tahun
        FROM pipeline_inspection
        WHERE tahun = (SELECT MAX(tahun) FROM pipeline_inspection)
          AND bulan = (
              SELECT MAX(bulan) FROM pipeline_inspection
              WHERE tahun = (SELECT MAX(tahun) FROM pipeline_inspection)
          )
          AND (rem_life_years < 3 OR jumlah_temporary_repair > 0)
        ORDER BY rem_life_years ASC NULLS LAST
        LIMIT 20
    """)

    # 5. PAF
    # current → code_current = 1
    # prev    → code_current = 0 dengan month_update terbaru (untuk trend)
    data["paf_current"] = q("""
        SELECT ru, type, target_realisasi, value, plan_unplan,
               month_update, color
        FROM paf
        WHERE code_current = 1
          AND LOWER(COALESCE(color,'')) IN ('red','yellow','orange','merah','kuning')
        ORDER BY ru, type
        LIMIT 25
    """)

    data["paf_prev"] = q("""
        SELECT ru, type, value, color, month_update
        FROM paf
        WHERE code_current = 0
          AND month_update = (SELECT MAX(month_update) FROM paf WHERE code_current = 0)
        ORDER BY ru, type
        LIMIT 25
    """)

    # 6. ISSUE PAF — code_current = 1
    data["issue_paf"] = q("""
        SELECT ru, type, date, issue, month_update
        FROM issue_paf
        WHERE code_current = 1
        ORDER BY date DESC NULLS LAST
        LIMIT 20
    """)

    # 7. POWER & UTILITY — code_current = 1, status tidak normal
    data["power_utility"] = q("""
        SELECT refinery_unit, type_equipment, equipment,
               status_operation, status_n0, average_actual,
               desain, kapasitas_max, remark, date_update
        FROM power_stream
        WHERE code_current = 1
          AND LOWER(COALESCE(status_operation,'')) NOT IN ('normal','standby','ok','siaga')
        ORDER BY refinery_unit, type_equipment
        LIMIT 20
    """)

    # 8. CRITICAL EQUIPMENT UTL — code_current = 1
    data["critical_utl"] = q("""
        SELECT refinery_unit, type_equipment, highlight_issue,
               corrective_action, target_corrective, traffic_corrective,
               mitigasi_action, target_mitigasi, traffic_mitigasi
        FROM critical_eqp_utl
        WHERE code_current = 1
          AND TRIM(COALESCE(highlight_issue,'')) != ''
        ORDER BY refinery_unit
        LIMIT 15
    """)

    # 9. READINESS JETTY
    # current → month_update terbaru
    # prev    → month_update terbaru sebelumnya (untuk trend bulanan)
    data["readiness_jetty_current"] = q("""
        SELECT refinery_unit, area, unit, tag_no, status_operation,
               status_tuks, expired_tuks, status_ijin_ops, expired_ijin_ops,
               status_isps, expired_isps, status_struktur, remark_struktur,
               status_trestle, remark_trestle, status_mla, remark_mla,
               status_fire_protection, remark_fire_protection, month_update
        FROM readiness_jetty
        WHERE month_update = (SELECT MAX(month_update) FROM readiness_jetty)
          AND (
            LOWER(COALESCE(status_operation,''))   NOT IN ('normal','siap','ok','ready')
            OR LOWER(COALESCE(status_tuks,''))     NOT IN ('valid','ok','aktif')
            OR LOWER(COALESCE(status_ijin_ops,'')) NOT IN ('valid','ok','aktif')
            OR LOWER(COALESCE(status_isps,''))     NOT IN ('valid','ok','aktif')
          )
        ORDER BY refinery_unit, tag_no
        LIMIT 15
    """)

    data["readiness_jetty_prev"] = q("""
        SELECT refinery_unit, tag_no, status_operation, month_update
        FROM readiness_jetty
        WHERE month_update = (
            SELECT MAX(month_update) FROM readiness_jetty
            WHERE month_update < (SELECT MAX(month_update) FROM readiness_jetty)
        )
          AND (
            LOWER(COALESCE(status_operation,'')) NOT IN ('normal','siap','ok','ready')
          )
        ORDER BY refinery_unit, tag_no
        LIMIT 15
    """)

    # 10. WORKPLAN JETTY
    # current → month_update terbaru, belum done
    # prev    → month_update sebelumnya, belum done (untuk lihat progres)
    data["workplan_jetty_current"] = q("""
        SELECT refinery_unit, area, unit, tag_no, item,
               status_item, remark, rtl_action_plan,
               target, keterangan, status_rtl, month_update
        FROM workplan_jetty
        WHERE month_update = (SELECT MAX(month_update) FROM workplan_jetty)
          AND LOWER(COALESCE(status_item,'')) NOT IN ('done','selesai','complete','closed')
        ORDER BY refinery_unit, tag_no
        LIMIT 20
    """)

    data["workplan_jetty_prev"] = q("""
        SELECT refinery_unit, tag_no, item, status_item, month_update
        FROM workplan_jetty
        WHERE month_update = (
            SELECT MAX(month_update) FROM workplan_jetty
            WHERE month_update < (SELECT MAX(month_update) FROM workplan_jetty)
        )
          AND LOWER(COALESCE(status_item,'')) NOT IN ('done','selesai','complete','closed')
        ORDER BY refinery_unit, tag_no
        LIMIT 20
    """)

    # 11. READINESS TANK
    # current → month_update terbaru, ada issue
    # prev    → month_update sebelumnya, ada issue
    data["readiness_tank_current"] = q("""
        SELECT refinery_unit, area, unit, tag_number,
               type_tangki, service_tangki, prioritas, status_operational,
               status_coi, coi_date_expired, atg_certification_validity,
               date_expired_atg, status_atg, remark_atg,
               status_grounding, status_shell_course, remark_shell_course,
               status_roof, remark_roof, month_update
        FROM readiness_tank
        WHERE month_update = (SELECT MAX(month_update) FROM readiness_tank)
          AND (
            LOWER(COALESCE(status_operational,'')) NOT IN ('normal','ok','siap')
            OR LOWER(COALESCE(status_coi,''))      NOT IN ('valid','ok','aktif')
            OR LOWER(COALESCE(status_atg,''))      NOT IN ('ok','normal','aktif')
          )
        ORDER BY refinery_unit, tag_number
        LIMIT 20
    """)

    data["readiness_tank_prev"] = q("""
        SELECT refinery_unit, tag_number, status_operational, month_update
        FROM readiness_tank
        WHERE month_update = (
            SELECT MAX(month_update) FROM readiness_tank
            WHERE month_update < (SELECT MAX(month_update) FROM readiness_tank)
        )
          AND LOWER(COALESCE(status_operational,'')) NOT IN ('normal','ok','siap')
        ORDER BY refinery_unit, tag_number
        LIMIT 20
    """)

    # 12. WORKPLAN TANK
    data["workplan_tank_current"] = q("""
        SELECT unit, tag_no, item, remark, rtl_action_plan,
               target, keterangan, status_rtl, month_update
        FROM workplan_tank
        WHERE month_update = (SELECT MAX(month_update) FROM workplan_tank)
          AND LOWER(COALESCE(status_rtl,'')) NOT IN ('done','selesai','complete','closed')
        ORDER BY tag_no
        LIMIT 20
    """)

    data["workplan_tank_prev"] = q("""
        SELECT tag_no, item, status_rtl, month_update
        FROM workplan_tank
        WHERE month_update = (
            SELECT MAX(month_update) FROM workplan_tank
            WHERE month_update < (SELECT MAX(month_update) FROM workplan_tank)
        )
          AND LOWER(COALESCE(status_rtl,'')) NOT IN ('done','selesai','complete','closed')
        ORDER BY tag_no
        LIMIT 20
    """)

    # 13. READINESS SPM
    data["readiness_spm_current"] = q("""
        SELECT refinery_unit, area, unit, tag_no, status_operation,
               status_laik_operasi, expired_laik_operasi,
               status_ijin_spl, expired_ijin_spl,
               status_mbc, remark_mbc, status_lds, remark_lds,
               status_mooring_hawser, remark_mooring_hawser,
               status_floating_hose, remark_floating_hose,
               status_cathodic_spl, status_cathodic_spm, month_update
        FROM readiness_spm
        WHERE month_update = (SELECT MAX(month_update) FROM readiness_spm)
          AND (
            LOWER(COALESCE(status_operation,''))       NOT IN ('normal','siap','ok','ready')
            OR LOWER(COALESCE(status_laik_operasi,'')) NOT IN ('valid','ok','aktif')
          )
        ORDER BY refinery_unit, tag_no
        LIMIT 15
    """)

    data["readiness_spm_prev"] = q("""
        SELECT refinery_unit, tag_no, status_operation, month_update
        FROM readiness_spm
        WHERE month_update = (
            SELECT MAX(month_update) FROM readiness_spm
            WHERE month_update < (SELECT MAX(month_update) FROM readiness_spm)
        )
          AND LOWER(COALESCE(status_operation,'')) NOT IN ('normal','siap','ok','ready')
        ORDER BY refinery_unit, tag_no
        LIMIT 15
    """)

    # 14. ATG
    data["atg_current"] = q("""
        SELECT refinery_unit, tag_no_tangki, tag_no_atg,
               status_atg, status_interkoneksi_atg,
               cert_no_atg, date_expired_atg,
               remark, rtl, status_rtl, month_update
        FROM atg_monitoring
        WHERE month_update = (SELECT MAX(month_update) FROM atg_monitoring)
          AND (
            LOWER(COALESCE(status_interkoneksi_atg,'')) NOT IN ('aktif','active','ok')
            OR LOWER(COALESCE(status_atg,''))           NOT IN ('ok','normal','aktif')
          )
        ORDER BY refinery_unit, tag_no_tangki
        LIMIT 20
    """)

    data["atg_prev"] = q("""
        SELECT refinery_unit, tag_no_tangki, status_atg, month_update
        FROM atg_monitoring
        WHERE month_update = (
            SELECT MAX(month_update) FROM atg_monitoring
            WHERE month_update < (SELECT MAX(month_update) FROM atg_monitoring)
        )
          AND LOWER(COALESCE(status_atg,'')) NOT IN ('ok','normal','aktif')
        ORDER BY refinery_unit, tag_no_tangki
        LIMIT 20
    """)

    # 15. MONITORING OPERASI — code_current = 1
    data["monitoring_operasi"] = q("""
        SELECT refinery_unit, unit_proses, unit, actual, target_sts,
               plant_readiness, limitasi_alert_process, mitigasi_process,
               limitasi_alert_sts, mitigasi_sts, month_update
        FROM monitoring_operasi
        WHERE code_current = 1
          AND (
            TRIM(COALESCE(limitasi_alert_process,'')) != ''
            OR (actual IS NOT NULL AND target_sts IS NOT NULL AND actual < target_sts)
          )
        ORDER BY refinery_unit, unit_proses
        LIMIT 20
    """)

    return data


# ─── REPORT GENERATION ────────────────────────────────────────────────────────
def generate_monthly_report(data: dict) -> str:
    now_wib = datetime.now(WIB)

    # Trend bad actor: current vs prev
    curr_tags = {r["tag_number"] for r in data.get("bad_actor_current", [])}
    prev_tags = {r["tag_number"] for r in data.get("bad_actor_prev", [])}
    recurring = sorted(curr_tags & prev_tags)
    new_items = sorted(curr_tags - prev_tags)
    resolved  = sorted(prev_tags - curr_tags)

    # Trend ICU: current vs prev
    icu_curr  = len(data.get("icu_current", []))
    icu_prev  = len(data.get("icu_prev", []))
    icu_trend = "memburuk" if icu_curr > icu_prev else ("membaik" if icu_curr < icu_prev else "stagnan")

    # Trend readiness: hitung berapa tag yang masih bermasalah current vs prev
    jetty_curr = len(data.get("readiness_jetty_current", []))
    jetty_prev = len(data.get("readiness_jetty_prev", []))
    tank_curr  = len(data.get("readiness_tank_current", []))
    tank_prev  = len(data.get("readiness_tank_prev", []))
    spm_curr   = len(data.get("readiness_spm_current", []))
    spm_prev   = len(data.get("readiness_spm_prev", []))
    atg_curr   = len(data.get("atg_current", []))
    atg_prev   = len(data.get("atg_prev", []))

    # Trend workplan: berapa item belum selesai current vs prev
    wj_curr = len(data.get("workplan_jetty_current", []))
    wj_prev = len(data.get("workplan_jetty_prev", []))
    wt_curr = len(data.get("workplan_tank_current", []))
    wt_prev = len(data.get("workplan_tank_prev", []))

    # Ambil info periode dari data
    periode_bad_actor = ""
    if data.get("bad_actor_current"):
        periode_bad_actor = str(data["bad_actor_current"][0].get("periode", ""))

    month_update_readiness = ""
    if data.get("readiness_jetty_current"):
        month_update_readiness = str(data["readiness_jetty_current"][0].get("month_update", ""))
    elif data.get("readiness_tank_current"):
        month_update_readiness = str(data["readiness_tank_current"][0].get("month_update", ""))

    trend_context = {
        "periode_bad_actor": periode_bad_actor,
        "bad_actor_recurring_tags": recurring,
        "bad_actor_new_this_month": new_items,
        "bad_actor_possibly_resolved": resolved,
        "icu_open_current": icu_curr,
        "icu_open_prev": icu_prev,
        "icu_trend": icu_trend,
        "readiness_jetty_issue_current": jetty_curr,
        "readiness_jetty_issue_prev": jetty_prev,
        "readiness_jetty_trend": "memburuk" if jetty_curr > jetty_prev else ("membaik" if jetty_curr < jetty_prev else "stagnan"),
        "readiness_tank_issue_current": tank_curr,
        "readiness_tank_issue_prev": tank_prev,
        "readiness_tank_trend": "memburuk" if tank_curr > tank_prev else ("membaik" if tank_curr < tank_prev else "stagnan"),
        "readiness_spm_issue_current": spm_curr,
        "readiness_spm_issue_prev": spm_prev,
        "readiness_spm_trend": "memburuk" if spm_curr > spm_prev else ("membaik" if spm_curr < spm_prev else "stagnan"),
        "atg_issue_current": atg_curr,
        "atg_issue_prev": atg_prev,
        "atg_trend": "memburuk" if atg_curr > atg_prev else ("membaik" if atg_curr < atg_prev else "stagnan"),
        "workplan_jetty_open_current": wj_curr,
        "workplan_jetty_open_prev": wj_prev,
        "workplan_tank_open_current": wt_curr,
        "workplan_tank_open_prev": wt_prev,
        "month_update_readiness": month_update_readiness,
    }

    summary  = {k: len(v) if isinstance(v, list) else v for k, v in data.items()}
    data_str = json.dumps(data, ensure_ascii=False, default=str, indent=2)
    tgl_susun = now_wib.strftime("%A, %d %B %Y | %H.%M")

    prompt = f"""Kamu adalah sistem pelaporan otomatis Monthly Management Review untuk operasional kilang minyak.

Laporan dibuat pada : {tgl_susun} WIB
Catatan             : Semua data diambil dari snapshot TERBARU masing-masing tabel (MAX bulan/periode per tabel).
                      Periode laporan → baca dari field tanggal di data (periode / report_date / month_update).

JUMLAH ITEM PER TABEL:
{json.dumps(summary, indent=2)}

ANALISA TREND BULAN INI vs BULAN LALU:
{json.dumps(trend_context, ensure_ascii=False, indent=2)}

DATA LENGKAP DARI DATABASE:
{data_str}

INSTRUKSI PENTING:
- Tulis dalam Bahasa Indonesia formal.
- Gunakan tag number / asset number SPESIFIK dari data. Jangan mengarang data.
- Periode laporan → baca dari field tanggal di data (periode bad_actor, report_date icu, month_update readiness).
- Tulis periode di header sebagai: [Bulan Tahun] berdasarkan MAX periode yang ada di data.
- Analisa trend berdasarkan perbandingan data current vs prev yang sudah dihitung.
- Untuk Seksi 7 (Budget), gunakan workplan_jetty dan workplan_tank sebagai proxy progres & delivery program.
- Isi SEMUA 9 seksi. Maksimal 5500 karakter total.

FORMAT WAJIB (ikuti persis termasuk emoji dan garis ━):

📙 MONTHLY MANAGEMENT REVIEW
🗓️ Periode: [Bulan Tahun dari data]
⏰ Disusun pada: {tgl_susun} WIB

🏭 Ringkasan Eksekutif Bulanan
[3-4 kalimat kondisi umum: status overall dgn emoji 🟢/🟡/🟠/🔴, jumlah asset/issue bermasalah, fokus manajemen bulan ini]

━━━━━━━━━━
🔴 1. MANAGEMENT HEADLINE BULAN INI
━━━━━━━━━━
1) [headline utama 1 — spesifik, sebut asset/domain]
2) [headline utama 2]
3) [headline utama 3]

━━━━━━━━━━
📍 2. MONTHLY PERFORMANCE SUMMARY
━━━━━━━━━━
• Operasi: [🟢/🟡/🟠/🔴] [ringkasan 1 kalimat]
• Reliability: [🟢/🟡/🟠/🔴] [ringkasan 1 kalimat]
• PAF / Availability: [🟢/🟡/🟠/🔴] [ringkasan 1 kalimat]
• Integrity / Technical Assurance: [🟢/🟡/🟠/🔴] [ringkasan 1 kalimat]
• Readiness: [🟢/🟡/🟠/🔴] [ringkasan 1 kalimat]
• Program Delivery: [🟢/🟡/🟠/🔴] [ringkasan 1 kalimat]
• Budget Signal: [🟢/🟡/🟠/🔴] [ringkasan 1 kalimat]

Perubahan dibanding bulan lalu:
• Membaik: [area/asset/domain]
• Tetap: [area/asset/domain]
• Memburuk: [area/asset/domain]

━━━━━━━━━━
⚙️ 3. TOP ASSET / ISSUE PRIORITAS BULAN INI
━━━━━━━━━━

1) [🟢/🟡/🟠/🔴] [Judul isu] – Asset No. [TAG] | Unit [RU]
• Status bulan ini: [memburuk/stagnan/berulang/belum close]
• Ringkasan perkembangan: [ringkasan]
• Dampak: [dampak]
• Sumber utama: [nama tabel]
• Arahan bulan depan:
  1. [aksi 1]
  2. [aksi 2]
  3. [aksi 3]
• PIC usulan: [fungsi/tim]
• Target bulan depan: [target]

[ulangi format di atas untuk isu 2, 3, 4]

━━━━━━━━━━
🔥 4. MONTHLY BAD ACTOR & RELIABILITY WATCHLIST
━━━━━━━━━━
Top 5 bad actor bulan ini:
1. [TAG] | [RU] | [🟢/🟡/🟠/🔴] [isu singkat] | trend bulanan: [membaik/tetap/memburuk]
2. [TAG] | [RU] | [🟢/🟡/🟠/🔴] [isu singkat] | trend bulanan: [...]
3. [TAG] | [RU] | [🟢/🟡/🟠/🔴] [isu singkat] | trend bulanan: [...]
4. [TAG] | [RU] | [🟢/🟡/🟠/🔴] [isu singkat] | trend bulanan: [...]
5. [TAG] | [RU] | [🟢/🟡/🟠/🔴] [isu singkat] | trend bulanan: [...]

Catatan bulanan:
• Asset yang tetap muncul sepanjang bulan agar dikategorikan sebagai persistent management concern.
• Asset yang sudah diberi intervensi namun belum membaik perlu direview strategi penanganannya.

━━━━━━━━━━
🛡️ 5. MONTHLY INTEGRITY & TECHNICAL ASSURANCE REVIEW
━━━━━━━━━━
Asset / item yang menjadi concern utama bulan ini:
• [TAG] | [area/unit] | [🟢/🟡/🟠/🔴] [jenis concern]
• [TAG] | [area/unit] | [🟢/🟡/🟠/🔴] [jenis concern]
• [TAG] | [area/unit] | [🟢/🟡/🟠/🔴] [jenis concern]
• [TAG] | [area/unit] | [🟢/🟡/🟠/🔴] [jenis concern]

Highlight review:
1. [asset/item] → [status concern / trend]
2. [asset/item] → [status concern / trend]
3. [asset/item] → [status concern / trend]

Perlu perhatian khusus:
• [asset/item] dengan concern meningkat
• [asset/item] dengan rekomendasi teknis yang belum ditindaklanjuti
• [asset/item] yang mulai berdampak ke readiness / operasi

━━━━━━━━━━
🚢 6. MONTHLY READINESS REVIEW
━━━━━━━━━━
Status readiness bulanan:
• Jetty: [🟢/🟡/🟠/🔴] [ringkasan + trend vs bulan lalu]
• Tank: [🟢/🟡/🟠/🔴] [ringkasan + trend vs bulan lalu]
• SPM: [🟢/🟡/🟠/🔴] [ringkasan + trend vs bulan lalu]
• ATG: [🟢/🟡/🟠/🔴] [ringkasan + trend vs bulan lalu]

Top readiness exception bulan ini:
• [TAG/ITEM] | [domain] | [isu singkat] | [trend bulanan]
• [TAG/ITEM] | [domain] | [isu singkat] | [trend bulanan]
• [TAG/ITEM] | [domain] | [isu singkat] | [trend bulanan]

Area yang perlu percepatan bulan depan:
1. [asset/item] → [tindakan]
2. [asset/item] → [tindakan]
3. [asset/item] → [tindakan]

━━━━━━━━━━
💰 7. MONTHLY PROGRAM DELIVERY & BUDGET REVIEW
━━━━━━━━━━
• Program Delivery: [🟢/🟡/🟠/🔴] [ringkasan progres workplan]
• Budget Realization: [🟢/🟡/🟠/🔴] [ringkasan deviasi]
• Plan vs Actual: [🟢/🟡/🟠/🔴] [ringkasan]
• Forecast bulan depan: [🟢/🟡/🟠/🔴] [ringkasan]

Program / item yang perlu perhatian:
• [PROGRAM/ITEM/TAG] → [status / deviasi / blocker]
• [PROGRAM/ITEM/TAG] → [status / deviasi / blocker]
• [PROGRAM/ITEM/TAG] → [status / deviasi / blocker]

Catatan:
• Program/item yang meleset 2 bulan berturut-turut agar dinaikkan sebagai management review item.
• Budget deviation yang konsisten perlu diberi penjelasan penyebab dan recovery plan.

━━━━━━━━━━
🎯 8. MANAGEMENT FOCUS BULAN DEPAN
━━━━━━━━━━
Mohon fokus bulan depan diarahkan pada:
1. [TAG/PROGRAM] → [target keputusan / target penyelesaian]
2. [TAG/PROGRAM] → [target keputusan / target penyelesaian]
3. [TAG/PROGRAM] → [target keputusan / target penyelesaian]
4. [TAG/PROGRAM] → [target keputusan / target penyelesaian]

━━━━━━━━━━
✅ 9. MANAGEMENT REQUEST
━━━━━━━━━━
Mohon tim terkait menyiapkan update bulanan berbasis asset number / item / program untuk topik berikut:
• [TAG/PROGRAM_1]
• [TAG/PROGRAM_2]
• [TAG/PROGRAM_3]
• [TAG/PROGRAM_4]
• [TAG/PROGRAM_5]

Legend Status:
🟢 Terkendali
🟡 Perlu perhatian
🟠 Perlu tindak lanjut
🔴 Perlu perhatian manajemen segera
_Auto-generated · Monthly Management Review Agent_"""

    return ask_llm(prompt)


# ─── WHATSAPP ─────────────────────────────────────────────────────────────────
def send_wa(target: str, message: str) -> bool:
    try:
        resp = requests.post(
            "https://api.fonnte.com/send",
            headers={"Authorization": FONNTE_TOKEN},
            data={"target": target, "message": message},
            timeout=30
        )
        result = resp.json()
        ok = result.get("status", False)
        print(f"  [WA] {'✅' if ok else '❌'} {target} — {result}")
        return ok
    except Exception as e:
        print(f"  [WA] ❌ {target} — {e}")
        return False


# ─── JOB ──────────────────────────────────────────────────────────────────────
def run_monthly_job():
    now_wib = datetime.now(WIB)
    print(f"\n{'='*55}")
    print(f"[MONTHLY] ▶ {now_wib.strftime('%Y-%m-%d %H:%M WIB')}")
    print(f"{'='*55}")

    print("[1/3] 🔍 Ambil data (MAX bulan/periode per tabel)...")
    data = gather_monthly_data()
    for k, v in data.items():
        if isinstance(v, list):
            print(f"  {k}: {len(v)} baris")

    print("[2/3] 🤖 Generate monthly report (Dinoiki gpt-4o)...")
    try:
        report = generate_monthly_report(data)
        print(f"  ✅ {len(report)} karakter")
    except Exception as e:
        print(f"  ❌ LLM error: {e}")
        return

    targets = [t.strip() for t in WA_TARGETS.split(",") if t.strip()]
    if not targets:
        print("[3/3] ⚠️  Tidak ada target WA — preview:\n")
        print(report)
        return

    print(f"[3/3] 📤 Kirim ke {len(targets)} nomor...")
    for t in targets:
        send_wa(t, report)
        time.sleep(2)
    print("[MONTHLY] ✅ Done.\n")


# ─── SCHEDULER ────────────────────────────────────────────────────────────────
def _schedule_monthly():
    """
    Jalankan setiap tanggal MONTHLY_SEND_DAY jam MONTHLY_SEND_TIME_UTC.
    schedule library tidak support tanggal spesifik tiap bulan,
    jadi kita pakai every().day dan cek tanggalnya manual.
    Default: tanggal 1 jam 23:00 UTC = tanggal 1 jam 06:00 WIB
    """
    def job_wrapper():
        now_utc = datetime.now(timezone.utc)
        if now_utc.day == MONTHLY_SEND_DAY:
            run_monthly_job()

    schedule.every().day.at(MONTHLY_SEND_TIME_UTC).do(job_wrapper)
    print(f"  Jadwal  : setiap tanggal {MONTHLY_SEND_DAY} jam {MONTHLY_SEND_TIME_UTC} UTC  =  tanggal {MONTHLY_SEND_DAY} jam 06:00 WIB")


# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 55)
    print("  Monthly Management Review Agent")
    print("=" * 55)
    print(f"  DB       : {'✅' if DATABASE_URL else '❌ Missing'}")
    print(f"  Fonnte   : {'✅' if FONNTE_TOKEN else '❌ Missing'}")
    print(f"  Dinoiki  : {'✅' if DINOIKI_API_KEY else '❌ Missing'}")
    print(f"  Target WA: {WA_TARGETS or '❌ Missing'}")
    _schedule_monthly()
    print("=" * 55)

    if os.getenv("RUN_NOW", "").lower() in ("1", "true", "yes"):
        print("\n[MONTHLY] RUN_NOW=true — langsung jalankan...")
        run_monthly_job()

    print(f"\n[MONTHLY] ⏳ Menunggu jadwal...\n")
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
