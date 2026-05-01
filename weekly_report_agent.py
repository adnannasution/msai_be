"""
Weekly Executive Review Agent
- Standalone Railway service (worker)
- Query langsung ke PostgreSQL (Railway)
- Generate report via Dinoiki (OpenAI-compatible endpoint)
- Kirim WhatsApp via Fonnte setiap Senin jam 06.00 WIB (default)
- Format: WEEKLY EXECUTIVE REVIEW — 8 seksi (format WhatsApp bold)

Strategi ambil data:
  Semua tabel → ambil MAX(kolom_tanggal) = laporan terbaru = laporan minggu itu.
  Untuk trend → ambil 2 snapshot terbaru (current vs sebelumnya).
"""

import os, time, json, requests, psycopg2, psycopg2.extras, schedule
from openai import OpenAI
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL         = os.getenv("DATABASE_URL", "")
FONNTE_TOKEN         = os.getenv("FONNTE_TOKEN", "")
DINOIKI_API_KEY      = os.getenv("DINOIKI_API_KEY", "")
WA_TARGETS           = os.getenv("WEEKLY_WA_TARGETS", os.getenv("REPORT_WA_TARGETS", ""))
WEEKLY_SEND_TIME_UTC = os.getenv("WEEKLY_SEND_TIME_UTC", "23:00")   # 23:00 UTC = 06:00 WIB
WEEKLY_SEND_DAY      = os.getenv("WEEKLY_SEND_DAY", "sunday").lower()
WIB                  = timezone(timedelta(hours=7))

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

# ─── DATA GATHERING ───────────────────────────────────────────────────────────
def gather_weekly_data():
    data = {}

    # 1. BAD ACTOR
    # current  → periode terbaru (= laporan minggu ini)
    # prev     → periode sebelumnya (= laporan minggu lalu, untuk trend)
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
    # current  → report_date terbaru (= laporan minggu ini)
    # prev     → report_date terbaru sebelumnya (= laporan minggu lalu, untuk trend)
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
    # current  → code_current = 1
    # prev     → code_current = 0 dengan month_update terbaru (untuk trend)
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

    # 9. READINESS JETTY — month_update terbaru, ada concern
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
        LIMIT 15
    """)

    # 10. WORKPLAN JETTY — month_update terbaru, belum done
    data["workplan_jetty"] = q("""
        SELECT refinery_unit, area, unit, tag_no, item,
               status_item, remark, rtl_action_plan,
               target, keterangan, status_rtl, month_update
        FROM workplan_jetty
        WHERE month_update = (SELECT MAX(month_update) FROM workplan_jetty)
          AND LOWER(COALESCE(status_item,'')) NOT IN ('done','selesai','complete','closed')
        ORDER BY refinery_unit, tag_no
        LIMIT 20
    """)

    # 11. READINESS TANK — month_update terbaru, ada issue
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
        LIMIT 20
    """)

    # 12. WORKPLAN TANK — month_update terbaru, belum done
    data["workplan_tank"] = q("""
        SELECT unit, tag_no, item, remark, rtl_action_plan,
               target, keterangan, status_rtl, month_update
        FROM workplan_tank
        WHERE month_update = (SELECT MAX(month_update) FROM workplan_tank)
          AND LOWER(COALESCE(status_rtl,'')) NOT IN ('done','selesai','complete','closed')
        ORDER BY tag_no
        LIMIT 20
    """)

    # 13. READINESS SPM — month_update terbaru, ada issue
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
            LOWER(COALESCE(status_operation,''))       NOT IN ('normal','siap','ok','ready')
            OR LOWER(COALESCE(status_laik_operasi,'')) NOT IN ('valid','ok','aktif')
          )
        ORDER BY refinery_unit, tag_no
        LIMIT 15
    """)

    # 14. ATG — month_update terbaru, ada issue
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
        LIMIT 20
    """)

    # 15. MONITORING OPERASI — code_current = 1, ada limitasi/deviasi
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
def generate_weekly_report(data: dict) -> str:
    now_wib = datetime.now(WIB)

    # Hitung trend bad actor: current vs prev
    curr_tags = {r["tag_number"] for r in data.get("bad_actor_current", [])}
    prev_tags = {r["tag_number"] for r in data.get("bad_actor_prev", [])}
    recurring = sorted(curr_tags & prev_tags)   # muncul di kedua periode
    new_items = sorted(curr_tags - prev_tags)   # baru muncul minggu ini
    resolved  = sorted(prev_tags - curr_tags)   # tidak muncul lagi (kemungkinan resolved)

    # Hitung trend ICU: current vs prev
    icu_curr  = len(data.get("icu_current", []))
    icu_prev  = len(data.get("icu_prev", []))
    icu_trend = "memburuk" if icu_curr > icu_prev else ("membaik" if icu_curr < icu_prev else "stagnan")

    # Ambil info tanggal laporan dari data
    periode_bad_actor = ""
    if data.get("bad_actor_current"):
        periode_bad_actor = str(data["bad_actor_current"][0].get("periode", ""))

    report_date_icu = ""
    if data.get("icu_current"):
        report_date_icu = str(data["icu_current"][0].get("report_date", ""))

    trend_context = {
        "bad_actor_periode_laporan": periode_bad_actor,
        "bad_actor_recurring_tags": recurring,
        "bad_actor_new_this_period": new_items,
        "bad_actor_possibly_resolved": resolved,
        "icu_report_date": report_date_icu,
        "icu_open_current": icu_curr,
        "icu_open_prev": icu_prev,
        "icu_trend": icu_trend,
    }

    summary  = {k: len(v) if isinstance(v, list) else v for k, v in data.items()}
    data_str = json.dumps(data, ensure_ascii=False, default=str, indent=2)
    tgl_susun = now_wib.strftime("%A, %d %B %Y | %H.%M")

    prompt = f"""Kamu adalah sistem pelaporan otomatis Weekly Executive Review untuk operasional kilang minyak (PRISMA).

Laporan dibuat pada : {tgl_susun} WIB
Catatan             : Semua data diambil dari snapshot TERBARU masing-masing tabel (bukan filter tanggal hari ini).
                      Tanggal periode laporan → lihat dari field tanggal di data (periode / report_date / month_update).

JUMLAH ITEM PER TABEL:
{json.dumps(summary, indent=2)}

ANALISA TREND (current vs periode sebelumnya):
{json.dumps(trend_context, ensure_ascii=False, indent=2)}

DATA LENGKAP DARI DATABASE:
{data_str}

INSTRUKSI PENTING:
- Tulis dalam Bahasa Indonesia formal.
- Gunakan tag number / asset number SPESIFIK dari data. Jangan mengarang data.
- Periode laporan → baca dari field tanggal di data (contoh: periode bad_actor, report_date icu, month_update readiness).
- Gunakan format bold WhatsApp dengan tanda bintang (*teks*) persis seperti template.
- Analisa trend berdasarkan perbandingan data aktual current vs prev.
- Untuk Seksi 6 (Budget Signal), gunakan data workplan_jetty dan workplan_tank sebagai proxy progres program.
- Isi SEMUA 8 seksi. Maksimal 5500 karakter total.

FORMAT WAJIB (ikuti persis termasuk bold, emoji, dan garis ━):

*📘 WEEKLY EXECUTIVE REVIEW*
*🗓️ Periode: [ambil dari tanggal di data]*
*⏰ Disusun pada: {tgl_susun} WIB*

*🏭 Ringkasan Eksekutif Mingguan*
[3 kalimat kondisi umum: status overall dgn emoji, jumlah asset bermasalah, fokus utama minggu ini]

━━━━━━━━━━
*🔴 1. ISU PRIORITAS MINGGU INI*
━━━━━━━━━━

*1) [🟢/🟡/🟠/🔴] [Judul isu] – Asset No. [TAG] | Unit [RU]*
* *Status minggu ini:* [memburuk/stagnan/berulang/belum selesai]
* *Ringkasan perkembangan:* [ringkasan]
* *Dampak:* [dampak]
* *Sumber utama:* [nama tabel]
* *Tindak lanjut minggu depan:*
  1. [aksi 1]
  2. [aksi 2]
  3. [aksi 3]
* *PIC usulan:* [fungsi/tim]
* *Target minggu depan:* [target]

[ulangi format di atas untuk isu 2, 3, 4]

━━━━━━━━━━
*📍 2. WEEKLY RELIABILITY PERFORMANCE REVIEW*
━━━━━━━━━━
* *Operasi:* [🟢/🟡/🟠/🔴] [ringkasan 1 kalimat]
* *Reliability:* [🟢/🟡/🟠/🔴] [ringkasan 1 kalimat]
* *PAF / Availability:* [🟢/🟡/🟠/🔴] [ringkasan 1 kalimat]
* *Power / Utility:* [🟢/🟡/🟠/🔴] [ringkasan 1 kalimat]
* *ICU / Monitoring signal:* [🟢/🟡/🟠/🔴] [ringkasan 1 kalimat]

*Asset yang paling mempengaruhi reliability minggu ini:*
* [TAG_1] → [isu singkat]
* [TAG_2] → [isu singkat]
* [TAG_3] → [isu singkat]
* [TAG_4] → [isu singkat]

*Perubahan dibanding minggu lalu:*
* *Membaik:* [asset/area]
* *Tetap / stagnan:* [asset/area]
* *Memburuk:* [asset/area]

━━━━━━━━━━
*⚙️ 3. WEEKLY BAD ACTOR REVIEW*
━━━━━━━━━━
*Top 5 bad actor minggu ini:*
1. *[TAG]* | [RU] | [🟢/🟡/🟠/🔴] [isu singkat] | *trend:* [membaik/tetap/memburuk]
2. *[TAG]* | [RU] | [🟢/🟡/🟠/🔴] [isu singkat] | *trend:* [...]
3. *[TAG]* | [RU] | [🟢/🟡/🟠/🔴] [isu singkat] | *trend:* [...]
4. *[TAG]* | [RU] | [🟢/🟡/🟠/🔴] [isu singkat] | *trend:* [...]
5. *[TAG]* | [RU] | [🟢/🟡/🟠/🔴] [isu singkat] | *trend:* [...]

*Catatan mingguan:*
* Asset yang terus muncul selama *[x]* minggu agar dinaikkan menjadi *management intervention item*.
* Bad actor yang sudah ada action namun tetap berulang perlu direview efektivitas penanganannya.

━━━━━━━━━━
*🛡️ 4. WEEKLY INTEGRITY & TECHNICAL CONDITION REVIEW*
━━━━━━━━━━
*Asset / item yang menjadi concern minggu ini:*
* *[TAG]* | [area/unit] | [🟢/🟡/🟠/🔴] [jenis concern]
* *[TAG]* | [area/unit] | [🟢/🟡/🟠/🔴] [jenis concern]
* *[TAG]* | [area/unit] | [🟢/🟡/🟠/🔴] [jenis concern]
* *[TAG]* | [area/unit] | [🟢/🟡/🟠/🔴] [jenis concern]

*Area review minggu ini:*
1. [asset/item] → [status review / concern]
2. [asset/item] → [status review / concern]
3. [asset/item] → [status review / concern]

*Perlu perhatian khusus:*
* [asset/item] yang concern-nya meningkat
* [asset/item] yang belum ada kejelasan tindak lanjut
* [asset/item] yang berpotensi mempengaruhi readiness / operasi

━━━━━━━━━━
*🚢 5. WEEKLY READINESS REVIEW*
━━━━━━━━━━
*Status readiness minggu ini:*
* *Jetty:* [🟢/🟡/🟠/🔴] [ringkasan]
* *Tank:* [🟢/🟡/🟠/🔴] [ringkasan]
* *SPM:* [🟢/🟡/🟠/🔴] [ringkasan]
* *ATG:* [🟢/🟡/🟠/🔴] [ringkasan]

*Top readiness exception:*
* *[TAG/ITEM]* | [domain] | [isu singkat] | [trend]
* *[TAG/ITEM]* | [domain] | [isu singkat] | [trend]
* *[TAG/ITEM]* | [domain] | [isu singkat] | [trend]

*Item yang perlu percepatan minggu depan:*
1. [asset/item] → [tindakan]
2. [asset/item] → [tindakan]
3. [asset/item] → [tindakan]

━━━━━━━━━━
*💰 6. WEEKLY PROGRAM / BUDGET SIGNAL*
━━━━━━━━━━
* *Program utama:* [🟢/🟡/🟠/🔴] [ringkasan progres]
* *Budget signal:* [🟢/🟡/🟠/🔴] [ringkasan deviasi / early warning]
* *Area yang perlu perhatian:* [program/item/asset]

*Highlight:*
* [program / workplan / item] → [status]
* [program / workplan / item] → [status]
* [program / workplan / item] → [status]

━━━━━━━━━━
*🎯 7. ARAH TINDAK LANJUT MINGGU DEPAN*
━━━━━━━━━━
Mohon dipastikan output berikut tersedia pada minggu depan:
1. *[TAG]* → [target penyelesaian / keputusan]
2. *[TAG]* → [target penyelesaian / keputusan]
3. *[TAG]* → [target penyelesaian / keputusan]
4. *[TAG]* → [target penyelesaian / keputusan]

━━━━━━━━━━
*✅ 8. MANAGEMENT REQUEST*
━━━━━━━━━━
Mohon tim terkait menyampaikan *update mingguan berbasis asset number* untuk item berikut:
* *[TAG_1]*
* *[TAG_2]*
* *[TAG_3]*
* *[TAG_4]*
* *[TAG_5]*

*Legend Status:*
🟢 Terkendali
🟡 Perlu perhatian
🟠 Perlu tindak lanjut
🔴 Perlu perhatian manajemen segera
_Auto-generated · PRISMA Weekly Report Agent_"""

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
def run_weekly_job():
    now_wib = datetime.now(WIB)
    print(f"\n{'='*55}")
    print(f"[WEEKLY] ▶ {now_wib.strftime('%Y-%m-%d %H:%M WIB')}")
    print(f"{'='*55}")

    print("[1/3] 🔍 Ambil data (MAX tanggal per tabel)...")
    data = gather_weekly_data()
    for k, v in data.items():
        if isinstance(v, list):
            print(f"  {k}: {len(v)} baris")

    print("[2/3] 🤖 Generate weekly report (Dinoiki gpt-4o)...")
    try:
        report = generate_weekly_report(data)
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
    print("[WEEKLY] ✅ Done.\n")


# ─── SCHEDULER ────────────────────────────────────────────────────────────────
def _schedule_weekly():
    day_map = {
        "monday":    schedule.every().monday,
        "tuesday":   schedule.every().tuesday,
        "wednesday": schedule.every().wednesday,
        "thursday":  schedule.every().thursday,
        "friday":    schedule.every().friday,
        "saturday":  schedule.every().saturday,
        "sunday":    schedule.every().sunday,
    }
    runner = day_map.get(WEEKLY_SEND_DAY, schedule.every().sunday)
    runner.at(WEEKLY_SEND_TIME_UTC).do(run_weekly_job)
    print(f"  Jadwal  : setiap {WEEKLY_SEND_DAY.capitalize()} {WEEKLY_SEND_TIME_UTC} UTC  =  Senin 06:00 WIB")


# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 55)
    print("  PRISMA Weekly Executive Review Agent")
    print("=" * 55)
    print(f"  DB       : {'✅' if DATABASE_URL else '❌ Missing'}")
    print(f"  Fonnte   : {'✅' if FONNTE_TOKEN else '❌ Missing'}")
    print(f"  Dinoiki  : {'✅' if DINOIKI_API_KEY else '❌ Missing'}")
    print(f"  Target WA: {WA_TARGETS or '❌ Missing'}")
    _schedule_weekly()
    print("=" * 55)

    if os.getenv("RUN_NOW", "").lower() in ("1", "true", "yes"):
        print("\n[WEEKLY] RUN_NOW=true — langsung jalankan...")
        run_weekly_job()

    print(f"\n[WEEKLY] ⏳ Menunggu jadwal...\n")
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
