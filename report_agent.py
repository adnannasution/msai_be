"""
Daily Executive Brief Agent
- Standalone Railway service (worker)
- Query langsung ke PostgreSQL
- Generate report via Dinoiki (OpenAI-compatible endpoint)
- Simpan ke DB + kirim Expo push notif ke React Native app
"""

import os, time, json, psycopg2, psycopg2.extras, schedule
from openai import OpenAI
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from report_helper import save_report

load_dotenv()

DATABASE_URL   = os.getenv("DATABASE_URL", "")
DINOIKI_API_KEY= os.getenv("DINOIKI_API_KEY", "")
SEND_TIME_UTC  = os.getenv("REPORT_SEND_TIME_UTC", "23:00")
WIB            = timezone(timedelta(hours=7))

# ─── LLM ─────────────────────────────────────────────────────────────────────
llm = OpenAI(
    api_key=DINOIKI_API_KEY,
    base_url="https://ai.dinoiki.com/v1"
)

def ask_llm(prompt: str) -> str:
    resp = llm.chat.completions.create(
        model="gpt-4o",
        temperature=0.7,
        max_tokens=1500,
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

# ─── DATA GATHERING ───────────────────────────────────────────────────────────
def gather_data():
    data = {}

    data["bad_actor"] = q("""
        SELECT ru, tag_number, status, problem, action_plan,
               progress, target_date, periode
        FROM bad_actor_monitoring
        WHERE periode = (SELECT MAX(periode) FROM bad_actor_monitoring)
          AND LOWER(COALESCE(status,'')) NOT IN ('closed','complete','selesai','done')
        ORDER BY ru, tag_number
        LIMIT 20
    """)

    data["icu"] = q("""
        SELECT ru, tag_no, icu_status, issue, mitigation,
               progress, target_closed, report_date
        FROM icu_monitoring
        WHERE report_date = (SELECT MAX(report_date) FROM icu_monitoring)
          AND LOWER(COALESCE(icu_status,'')) NOT IN ('closed','resolved','selesai')
        ORDER BY ru, tag_no
        LIMIT 20
    """)

    data["zero_clamp"] = q("""
        SELECT ru, area, unit, tag_no_ln, services, description,
               type_damage, tanggal_dipasang, tanggal_rencana_perbaikan,
               status, remarks
        FROM zero_clamp
        WHERE tanggal_dilepas IS NULL OR TRIM(COALESCE(tanggal_dilepas,'')) = ''
        ORDER BY tanggal_dipasang ASC NULLS LAST
        LIMIT 15
    """)

    data["pipeline"] = q("""
        SELECT refinery_unit, tag_number, fluida_service, nps,
               from_location, to_location, last_measured_thickness,
               rem_life_years, jumlah_temporary_repair, remarks,
               next_inspection_date, bulan, tahun
        FROM pipeline_inspection
        WHERE tahun = (SELECT MAX(tahun) FROM pipeline_inspection)
          AND bulan = (
              SELECT bulan FROM pipeline_inspection
              WHERE tahun = (SELECT MAX(tahun) FROM pipeline_inspection)
              ORDER BY bulan DESC LIMIT 1
          )
          AND (rem_life_years < 3 OR jumlah_temporary_repair > 0)
        ORDER BY rem_life_years ASC NULLS LAST
        LIMIT 15
    """)

    data["paf"] = q("""
        SELECT ru, type, target_realisasi, value, plan_unplan,
               month_update, color
        FROM paf
        WHERE month_update = (SELECT MAX(month_update) FROM paf)
          AND LOWER(COALESCE(color,'')) IN ('red','yellow','orange','merah','kuning')
        ORDER BY ru, type
        LIMIT 20
    """)

    data["issue_paf"] = q("""
        SELECT ru, type, date, issue, month_update
        FROM issue_paf
        WHERE month_update = (SELECT MAX(month_update) FROM issue_paf)
        ORDER BY date DESC NULLS LAST
        LIMIT 15
    """)

    data["power_utility"] = q("""
        SELECT refinery_unit, type_equipment, equipment,
               status_operation, status_n0, average_actual,
               desain, kapasitas_max, remark, date_update
        FROM power_stream
        WHERE date_update = (SELECT MAX(date_update) FROM power_stream)
          AND LOWER(COALESCE(status_operation,'')) NOT IN ('normal','standby','ok','siaga')
        ORDER BY refinery_unit, type_equipment
        LIMIT 15
    """)

    data["critical_utl"] = q("""
        SELECT refinery_unit, type_equipment, highlight_issue,
               corrective_action, target_corrective, traffic_corrective,
               mitigasi_action, target_mitigasi, traffic_mitigasi
        FROM critical_eqp_utl
        WHERE month_update = (SELECT MAX(month_update) FROM critical_eqp_utl)
          AND TRIM(COALESCE(highlight_issue,'')) != ''
        ORDER BY refinery_unit
        LIMIT 10
    """)

    data["readiness_jetty"] = q("""
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
        LIMIT 10
    """)

    data["workplan_jetty"] = q("""
        SELECT refinery_unit, area, unit, tag_no, item,
               status_item, remark, rtl_action_plan,
               target, keterangan, status_rtl, month_update
        FROM workplan_jetty
        WHERE month_update = (SELECT MAX(month_update) FROM workplan_jetty)
          AND LOWER(COALESCE(status_item,'')) NOT IN ('done','selesai','complete','closed')
        ORDER BY refinery_unit, tag_no
        LIMIT 15
    """)

    data["readiness_tank"] = q("""
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
        LIMIT 15
    """)

    data["workplan_tank"] = q("""
        SELECT unit, tag_no, item, remark, rtl_action_plan,
               target, keterangan, status_rtl, month_update
        FROM workplan_tank
        WHERE month_update = (SELECT MAX(month_update) FROM workplan_tank)
          AND LOWER(COALESCE(status_rtl,'')) NOT IN ('done','selesai','complete','closed')
        ORDER BY tag_no
        LIMIT 15
    """)

    data["readiness_spm"] = q("""
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
            LOWER(COALESCE(status_operation,''))      NOT IN ('normal','siap','ok','ready')
            OR LOWER(COALESCE(status_laik_operasi,'')) NOT IN ('valid','ok','aktif')
          )
        ORDER BY refinery_unit, tag_no
        LIMIT 10
    """)

    data["atg"] = q("""
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
        LIMIT 15
    """)

    data["monitoring_operasi"] = q("""
        SELECT refinery_unit, unit_proses, unit, actual, target_sts,
               plant_readiness, limitasi_alert_process, mitigasi_process,
               limitasi_alert_sts, mitigasi_sts, month_update
        FROM monitoring_operasi
        WHERE month_update = (SELECT MAX(month_update) FROM monitoring_operasi)
          AND (
            TRIM(COALESCE(limitasi_alert_process,'')) != ''
            OR (actual IS NOT NULL AND target_sts IS NOT NULL AND actual < target_sts)
          )
        ORDER BY refinery_unit, unit_proses
        LIMIT 15
    """)

    return data


# ─── REPORT GENERATION ────────────────────────────────────────────────────────
def generate_report(data: dict) -> str:
    now_wib  = datetime.now(WIB)
    summary  = {k: len(v) for k, v in data.items()}
    data_str = json.dumps(data, ensure_ascii=False, default=str, indent=2)

    prompt = f"""Kamu adalah sistem pelaporan otomatis Daily Executive Brief untuk operasional kilang minyak.

Tanggal: {now_wib.strftime('%A, %d %B %Y')} | Waktu: {now_wib.strftime('%H.%M')} WIB

JUMLAH ITEM PER SEKSI (hanya yang bermasalah/open):
{json.dumps(summary, indent=2)}

DATA LENGKAP DARI DATABASE:
{data_str}

INSTRUKSI:
Buat Daily Executive Brief dalam format WhatsApp. Analisa data, identifikasi asset paling kritis, sebutkan tag/asset number spesifik dari data. Maksimal 3500 karakter.

FORMAT WAJIB:
📌 DAILY EXECUTIVE BRIEF
🗓️ {now_wib.strftime('%A, %d %B %Y')} | ⏰ {now_wib.strftime('%H.%M')} WIB

🏭 RINGKASAN EKSEKUTIF
[2-3 kalimat kondisi umum berdasarkan data nyata]

━━━━━━━━━━
🔴 PRIORITAS HARI INI
━━━━━━━━━━
[3-5 isu paling kritis dengan tag number spesifik, tindak lanjut, dan PIC usulan]

━━━━━━━━━━
📍 STATUS RELIABILITY & OPERASI
━━━━━━━━━━
• Operasi: [🟢/🟡/🟠/🔴 + keterangan]
• Reliability: [status + keterangan]
• PAF/Availability: [status + keterangan]
• Power/Utility: [status + keterangan]
• ICU/Zero Clamp: [status + keterangan]

━━━━━━━━━━
⚙️ BAD ACTOR WATCHLIST
━━━━━━━━━━
[Top 5: Tag | RU | Status | Progress]

━━━━━━━━━━
🚢 READINESS ALERT
━━━━━━━━━━
• Jetty: [status + ringkasan]
• Tank: [status + ringkasan]
• SPM: [status + ringkasan]
• ATG: [status + ringkasan]

━━━━━━━━━━
🎯 TINDAK LANJUT HARI INI
━━━━━━━━━━
[4-5 action item spesifik berbasis data]

Legend: 🟢 Terkendali | 🟡 Watch | 🟠 Action | 🔴 Urgent
_Auto-generated · Daily Report Agent_"""

    return ask_llm(prompt)




# ─── JOB ──────────────────────────────────────────────────────────────────────
def run_report_job():
    now_wib = datetime.now(WIB)
    print(f"\n{'='*50}")
    print(f"[DAILY] ▶ {now_wib.strftime('%Y-%m-%d %H:%M WIB')}")
    print(f"{'='*50}")

    print("[1/3] 🔍 Ambil data...")
    data = gather_data()
    for k, v in data.items():
        print(f"  {k}: {len(v)} baris")

    print("[2/3] 🤖 Generate report (Dinoiki gpt-4o)...")
    try:
        report = generate_report(data)
        print(f"  ✅ {len(report)} karakter")
    except Exception as e:
        print(f"  ❌ {e}")
        return

    # Simpan ke DB + kirim Expo push notif ke semua device
    periode = now_wib.strftime("%A, %d %B %Y")
    save_report("daily", report, periode)
    print(f"  ✅ Report disimpan ke DB & push notif terkirim (periode: {periode})")
    print("[DAILY] ✅ Done.\n")


# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 50)
    print("  Daily Report Agent")
    print("=" * 50)
    print(f"  Jadwal  : {SEND_TIME_UTC} UTC  =  06.00 WIB")
    print(f"  DB      : {'✅' if DATABASE_URL else '❌ Missing'}")
    print(f"  Dinoiki : {'✅' if DINOIKI_API_KEY else '❌ Missing'}")
    print("=" * 50)

    if os.getenv("RUN_NOW", "").lower() in ("1", "true", "yes"):
        print("\n[DAILY] RUN_NOW=true — langsung jalankan...")
        run_report_job()

    schedule.every().day.at(SEND_TIME_UTC).do(run_report_job)
    print(f"\n[DAILY] ⏳ Menunggu jadwal {SEND_TIME_UTC} UTC...\n")

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()