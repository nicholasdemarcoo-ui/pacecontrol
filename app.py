import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import fitz
from flask import (
    Flask,
    flash,
    redirect,
    render_template,
    request,
    send_from_directory,
    url_for,
)
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

app = Flask(__name__)
app.secret_key = "dev-secret-key"

BASE_DIR = Path(__file__).resolve().parent
DATABASE = BASE_DIR / "tee_sheet.db"
PDF_FOLDER = BASE_DIR / "static" / "archived_pdfs"
PDF_FOLDER.mkdir(parents=True, exist_ok=True)


# ---------------- DATABASE ----------------

def get_connection():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS upload_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT,
                uploaded_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tee_sheets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sheet_date TEXT,
                source_filename TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tee_sheet_rows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tee_sheet_id INTEGER NOT NULL,
                reservation_time TEXT,
                group_name TEXT,
                players TEXT,
                num_players INTEGER,
                walkers INTEGER,
                riders INTEGER,
                group_type TEXT,
                front_course TEXT,
                back_course TEXT,
                rotation TEXT,
                total_time TEXT,
                average_hole TEXT,
                display_order INTEGER DEFAULT 0,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (tee_sheet_id) REFERENCES tee_sheets(id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS archived_days (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_sheet_id INTEGER,
                play_date TEXT,
                title TEXT,
                summary_pdf TEXT,
                source_filename TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS archived_tee_sheet_rows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                archived_day_id INTEGER NOT NULL,
                reservation_time TEXT,
                group_name TEXT,
                players TEXT,
                num_players INTEGER,
                walkers INTEGER,
                riders INTEGER,
                group_type TEXT,
                front_course TEXT,
                back_course TEXT,
                rotation TEXT,
                total_time TEXT,
                average_hole TEXT,
                display_order INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (archived_day_id) REFERENCES archived_days(id)
            )
        """)

        conn.commit()


def log_upload(filename):
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO upload_logs (filename) VALUES (?)",
                (filename,)
            )
            conn.commit()
        return "Saved to database"
    except Exception as e:
        return f"DB error: {e}"


# ---------------- DATA STORAGE ----------------

def row_to_dict(row):
    return {
        "id": row["id"],
        "reservation_time": row["reservation_time"] or "",
        "group_name": row["group_name"] or "",
        "players": row["players"] or "",
        "num_players": str(row["num_players"]) if row["num_players"] is not None else "",
        "walkers": str(row["walkers"]) if row["walkers"] is not None else "",
        "riders": str(row["riders"]) if row["riders"] is not None else "",
        "group_type": row["group_type"] or "",
        "front": row["front_course"] or "",
        "back": row["back_course"] or "",
        "rotation": row["rotation"] or "",
        "total_time": row["total_time"] or "",
        "average_hole": row["average_hole"] or "",
        "display_order": row["display_order"] if row["display_order"] is not None else 0
    }


def archive_day_row_to_dict(row):
    return {
        "id": row["id"],
        "original_sheet_id": row["original_sheet_id"],
        "play_date": row["play_date"],
        "title": row["title"] or "",
        "summary_pdf": row["summary_pdf"] or "",
        "source_filename": row["source_filename"] or "",
        "created_at": row["created_at"],
    }


def get_active_sheet():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, sheet_date, source_filename, updated_at
            FROM tee_sheets
            WHERE is_active = 1
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
        """)
        row = cursor.fetchone()

    if not row:
        return None

    sheet_date = row["sheet_date"] or ""
    formatted_date = ""

    if sheet_date:
        try:
            formatted_date = datetime.strptime(sheet_date, "%Y-%m-%d").strftime("%B %d, %Y")
        except ValueError:
            formatted_date = sheet_date

    return {
        "id": row["id"],
        "date": formatted_date,
        "sheet_date_raw": sheet_date,
        "source_filename": row["source_filename"] or "",
        "updated_at": row["updated_at"]
    }


def get_sheet_rows(sheet_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                id,
                reservation_time,
                group_name,
                players,
                num_players,
                walkers,
                riders,
                group_type,
                front_course,
                back_course,
                rotation,
                total_time,
                average_hole,
                display_order
            FROM tee_sheet_rows
            WHERE tee_sheet_id = ?
            ORDER BY display_order, id
        """, (sheet_id,))
        rows = cursor.fetchall()

    return [row_to_dict(r) for r in rows]


def load_data():
    sheet = get_active_sheet()
    if not sheet:
        return {"sheet_id": None, "rows": [], "date": ""}

    return {
        "sheet_id": sheet["id"],
        "rows": get_sheet_rows(sheet["id"]),
        "date": sheet["date"]
    }


def create_new_sheet(sheet_date, source_filename, rows):
    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE tee_sheets
            SET is_active = 0,
                updated_at = CURRENT_TIMESTAMP
            WHERE is_active = 1
        """)

        cursor.execute("""
            INSERT INTO tee_sheets (sheet_date, source_filename, is_active)
            VALUES (?, ?, 1)
        """, (sheet_date, source_filename))

        tee_sheet_id = cursor.lastrowid

        for idx, row in enumerate(rows):
            cursor.execute("""
                INSERT INTO tee_sheet_rows (
                    tee_sheet_id,
                    reservation_time,
                    group_name,
                    players,
                    num_players,
                    walkers,
                    riders,
                    group_type,
                    front_course,
                    back_course,
                    rotation,
                    total_time,
                    average_hole,
                    display_order
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                tee_sheet_id,
                row.get("reservation_time") or "",
                row.get("group_name") or "",
                row.get("players") or "",
                int(row["num_players"]) if str(row.get("num_players", "")).isdigit() else None,
                int(row["walkers"]) if str(row.get("walkers", "")).isdigit() else None,
                int(row["riders"]) if str(row.get("riders", "")).isdigit() else None,
                row.get("group_type") or "",
                row.get("front") or "",
                row.get("back") or "",
                row.get("rotation") or "",
                row.get("total_time") or "",
                row.get("average_hole") or "",
                idx
            ))

        conn.commit()

    return tee_sheet_id


def resequence_rows(sheet_id):
    rows = get_sheet_rows(sheet_id)

    with get_connection() as conn:
        cursor = conn.cursor()

        for idx, row in enumerate(rows):
            cursor.execute("""
                UPDATE tee_sheet_rows
                SET display_order = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (idx, row["id"]))

        cursor.execute("""
            UPDATE tee_sheets
            SET updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (sheet_id,))

        conn.commit()


def add_row_to_active_sheet(row_data):
    data = load_data()
    sheet_id = data["sheet_id"]

    if not sheet_id:
        return None

    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE tee_sheet_rows
            SET display_order = display_order + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE tee_sheet_id = ?
        """, (sheet_id,))

        cursor.execute("""
            INSERT INTO tee_sheet_rows (
                tee_sheet_id,
                reservation_time,
                group_name,
                players,
                num_players,
                walkers,
                riders,
                group_type,
                front_course,
                back_course,
                rotation,
                total_time,
                average_hole,
                display_order
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
        """, (
            sheet_id,
            row_data.get("reservation_time") or "",
            row_data.get("group_name") or "",
            row_data.get("players") or "",
            int(row_data["num_players"]) if str(row_data.get("num_players", "")).isdigit() else None,
            int(row_data["walkers"]) if str(row_data.get("walkers", "")).isdigit() else None,
            int(row_data["riders"]) if str(row_data.get("riders", "")).isdigit() else None,
            row_data.get("group_type") or "",
            row_data.get("front") or "",
            row_data.get("back") or "",
            row_data.get("rotation") or "",
            row_data.get("total_time") or "",
            row_data.get("average_hole") or ""
        ))

        cursor.execute("""
            UPDATE tee_sheets
            SET updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (sheet_id,))

        conn.commit()

    return True


def delete_row_by_id(sheet_id, row_id):
    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            DELETE FROM tee_sheet_rows
            WHERE id = ? AND tee_sheet_id = ?
        """, (row_id, sheet_id))

        cursor.execute("""
            UPDATE tee_sheets
            SET updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (sheet_id,))

        conn.commit()

    resequence_rows(sheet_id)


def clear_active_sheet():
    sheet = get_active_sheet()
    if not sheet:
        return

    sheet_id = sheet["id"]

    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("DELETE FROM tee_sheet_rows WHERE tee_sheet_id = ?", (sheet_id,))
        cursor.execute("""
            UPDATE tee_sheets
            SET is_active = 0,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (sheet_id,))

        conn.commit()


def get_archived_days():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                id,
                original_sheet_id,
                play_date,
                title,
                summary_pdf,
                source_filename,
                created_at
            FROM archived_days
            ORDER BY play_date DESC, created_at DESC, id DESC
        """)
        rows = cursor.fetchall()

    archived_days = []
    for row in rows:
        item = archive_day_row_to_dict(row)

        play_date = item["play_date"] or ""
        if play_date:
            try:
                item["play_date_formatted"] = datetime.strptime(play_date, "%Y-%m-%d").strftime("%B %d, %Y")
            except ValueError:
                item["play_date_formatted"] = play_date
        else:
            item["play_date_formatted"] = ""

        archived_days.append(item)

    return archived_days


def get_archived_rows(archived_day_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                id,
                reservation_time,
                group_name,
                players,
                num_players,
                walkers,
                riders,
                group_type,
                front_course,
                back_course,
                rotation,
                total_time,
                average_hole,
                display_order
            FROM archived_tee_sheet_rows
            WHERE archived_day_id = ?
            ORDER BY display_order, id
        """, (archived_day_id,))
        rows = cursor.fetchall()

    return [row_to_dict(r) for r in rows]


def generate_archive_pdf(play_date_label, archived_rows, archive_id):
    safe_date = re.sub(r"[^0-9A-Za-z_-]", "-", play_date_label or "unknown-date")
    filename = f"archive_{safe_date}_{archive_id}.pdf"
    filepath = PDF_FOLDER / filename

    c = canvas.Canvas(str(filepath), pagesize=letter)
    width, height = letter
    top_margin = height - 50
    y = top_margin

    def draw_header():
        nonlocal y
        y = top_margin
        c.setFont("Helvetica-Bold", 16)
        c.drawCentredString(width / 2, y, "Tee Sheet Summary")

        y -= 24
        c.setFont("Helvetica", 11)
        c.drawCentredString(width / 2, y, f"Date: {play_date_label}")

        y -= 28
        c.setFont("Helvetica-Bold", 9)
        c.drawString(30, y, "Time")
        c.drawString(78, y, "Group")
        c.drawString(180, y, "Players")
        c.drawString(470, y, "Pace")
        c.drawString(525, y, "Avg/Hole")

        y -= 14
        c.line(30, y, width - 30, y)
        y -= 14
        c.setFont("Helvetica", 8)

    draw_header()

    for row in archived_rows:
        group_name = (row.get("group_name") or "")[:20]
        players = (row.get("players") or "")[:58]
        tee_time = row.get("reservation_time") or ""
        total_time = row.get("total_time") or ""
        avg_hole = row.get("average_hole") or ""

        if y < 55:
            c.showPage()
            draw_header()

        c.drawString(30, y, tee_time)
        c.drawString(78, y, group_name)
        c.drawString(180, y, players)
        c.drawString(470, y, total_time)
        c.drawString(525, y, avg_hole)
        y -= 14

    c.save()
    return filename


def archive_active_sheet():
    active_sheet = get_active_sheet()
    if not active_sheet:
        return False, "No active tee sheet to archive."

    sheet_id = active_sheet["id"]
    rows = get_sheet_rows(sheet_id)

    if not rows:
        return False, "No rows found on the active tee sheet."

    play_date_raw = active_sheet.get("sheet_date_raw") or datetime.now().strftime("%Y-%m-%d")

    try:
        play_date_label = datetime.strptime(play_date_raw, "%Y-%m-%d").strftime("%B %d, %Y")
    except ValueError:
        play_date_label = play_date_raw

    title = f"Tee Sheet Summary - {play_date_label}"
    source_filename = active_sheet.get("source_filename", "")

    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO archived_days (
                original_sheet_id,
                play_date,
                title,
                source_filename
            )
            VALUES (?, ?, ?, ?)
        """, (
            sheet_id,
            play_date_raw,
            title,
            source_filename
        ))

        archived_day_id = cursor.lastrowid

        for idx, row in enumerate(rows):
            cursor.execute("""
                INSERT INTO archived_tee_sheet_rows (
                    archived_day_id,
                    reservation_time,
                    group_name,
                    players,
                    num_players,
                    walkers,
                    riders,
                    group_type,
                    front_course,
                    back_course,
                    rotation,
                    total_time,
                    average_hole,
                    display_order
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                archived_day_id,
                row.get("reservation_time") or "",
                row.get("group_name") or "",
                row.get("players") or "",
                int(row["num_players"]) if str(row.get("num_players", "")).isdigit() else None,
                int(row["walkers"]) if str(row.get("walkers", "")).isdigit() else None,
                int(row["riders"]) if str(row.get("riders", "")).isdigit() else None,
                row.get("group_type") or "",
                row.get("front") or "",
                row.get("back") or "",
                row.get("rotation") or "",
                row.get("total_time") or "",
                row.get("average_hole") or "",
                idx
            ))

        conn.commit()

    archived_rows = get_archived_rows(archived_day_id)
    pdf_filename = generate_archive_pdf(play_date_label, archived_rows, archived_day_id)

    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE archived_days
            SET summary_pdf = ?
            WHERE id = ?
        """, (pdf_filename, archived_day_id))

        cursor.execute("DELETE FROM tee_sheet_rows WHERE tee_sheet_id = ?", (sheet_id,))
        cursor.execute("""
            UPDATE tee_sheets
            SET is_active = 0,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (sheet_id,))

        conn.commit()

    return True, archived_day_id


# ---------------- HELPERS ----------------

def sort_rows_by_time(rows):
    def parse_time(row):
        value = (row.get("reservation_time") or "").strip()
        if not value:
            return datetime.max

        for fmt in ("%I:%M %p", "%H:%M", "%I:%M"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue

        return datetime.max

    rows.sort(key=parse_time)


def save_sorted_rows(sheet_id, rows):
    with get_connection() as conn:
        cursor = conn.cursor()

        for idx, row in enumerate(rows):
            cursor.execute("""
                UPDATE tee_sheet_rows
                SET display_order = ?,
                    reservation_time = ?,
                    group_name = ?,
                    players = ?,
                    num_players = ?,
                    walkers = ?,
                    riders = ?,
                    group_type = ?,
                    front_course = ?,
                    back_course = ?,
                    rotation = ?,
                    total_time = ?,
                    average_hole = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ? AND tee_sheet_id = ?
            """, (
                idx,
                row.get("reservation_time") or "",
                row.get("group_name") or "",
                row.get("players") or "",
                int(row["num_players"]) if str(row.get("num_players", "")).isdigit() else None,
                int(row["walkers"]) if str(row.get("walkers", "")).isdigit() else None,
                int(row["riders"]) if str(row.get("riders", "")).isdigit() else None,
                row.get("group_type") or "",
                row.get("front") or "",
                row.get("back") or "",
                row.get("rotation") or "",
                row.get("total_time") or "",
                row.get("average_hole") or "",
                row["id"],
                sheet_id
            ))

        cursor.execute("""
            UPDATE tee_sheets
            SET updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (sheet_id,))

        conn.commit()


def player_count_options(num):
    return list(range(0, int(num) + 1)) if str(num).isdigit() else [0, 1, 2, 3, 4]


def format_total_time(value):
    value = (value or "").strip()
    digits = "".join(ch for ch in value if ch.isdigit())

    if len(digits) == 3:
        return f"{digits[0]}:{digits[1:]}"
    if len(digits) == 4:
        return f"{digits[:2]}:{digits[2:]}"
    return value


def format_reservation_time(value):
    value = (value or "").strip()
    digits = "".join(ch for ch in value if ch.isdigit())

    if len(digits) == 3:
        return f"{int(digits[0])}:{digits[1:]}"
    if len(digits) == 4:
        hours = str(int(digits[:2]))
        return f"{hours}:{digits[2:]}"
    return value


def apply_derived_fields(row):
    players_text = (row.get("players") or "").strip()
    player_list = [p.strip() for p in players_text.split(",") if p.strip()]

    if player_list:
        row["num_players"] = str(len(player_list))
    else:
        row["num_players"] = str(row.get("num_players") or "").strip()

    walkers = str(row.get("walkers") or "").strip()
    try:
        num_players_int = int(row.get("num_players") or 0)
        walkers_int = int(walkers) if walkers != "" else None
        if walkers_int is not None:
            row["riders"] = str(num_players_int - walkers_int)
        else:
            row["riders"] = ""
    except Exception:
        row["riders"] = ""

    if walkers == "":
        row["group_type"] = ""
    else:
        try:
            walkers_int = int(walkers)
            num_players_int = int(row.get("num_players") or 0)

            if walkers_int == 0:
                row["group_type"] = "Ride"
            elif walkers_int == num_players_int:
                row["group_type"] = "Walk"
            else:
                row["group_type"] = "Mixed"
        except Exception:
            row["group_type"] = ""

    front = str(row.get("front") or "").strip()
    back = str(row.get("back") or "").strip()

    if front and back:
        row["rotation"] = f"{front}-{back}"
        holes = 18
    elif front or back:
        row["rotation"] = front or back
        holes = 9
    else:
        row["rotation"] = ""
        holes = 0

    row["total_time"] = format_total_time(row.get("total_time") or "")
    row["reservation_time"] = format_reservation_time(row.get("reservation_time") or "")

    total_time = row["total_time"]
    row["average_hole"] = ""

    try:
        if total_time and holes > 0 and ":" in total_time:
            hours_part, minutes_part = total_time.split(":")
            total_minutes = int(hours_part) * 60 + int(minutes_part)

            avg_minutes_float = total_minutes / holes
            avg_total_seconds = round(avg_minutes_float * 60)

            avg_minutes = avg_total_seconds // 60
            avg_seconds = avg_total_seconds % 60

            row["average_hole"] = f"{avg_minutes}:{avg_seconds:02d}"
    except Exception:
        row["average_hole"] = ""

    return row


def calculate_summary(rows):
    def round_time_to_minutes(value):
        value = (value or "").strip()
        if not value or ":" not in value:
            return None

        try:
            hours, minutes = value.split(":")
            return int(hours) * 60 + int(minutes)
        except Exception:
            return None

    def minutes_to_round_time(total_minutes):
        if total_minutes is None:
            return "-"

        hours = total_minutes // 60
        minutes = total_minutes % 60
        return f"{hours}:{minutes:02d}"

    valid_rounds = []
    cart_rounds = []
    walk_rounds = []
    mixed_rounds = []

    rotation_buckets = {
        "South-East": [],
        "South-West": [],
        "East-West": [],
        "East-South": [],
        "West-East": [],
        "West-South": [],
        "South": [],
        "West": [],
        "East": []
    }

    fastest_name = ""
    slowest_name = ""

    for row in rows:
        total_time_str = row.get("total_time", "")
        total_minutes = round_time_to_minutes(total_time_str)

        front = (row.get("front") or "").strip()
        back = (row.get("back") or "").strip()
        is_18_holes = bool(front and back)

        try:
            num_players = int(row.get("num_players") or 0)
        except Exception:
            num_players = 0

        try:
            walkers = int(row.get("walkers")) if str(row.get("walkers", "")).strip() != "" else None
        except Exception:
            walkers = None

        try:
            riders = int(row.get("riders")) if str(row.get("riders", "")).strip() != "" else None
        except Exception:
            riders = None

        if is_18_holes and total_minutes is not None:
            valid_rounds.append((total_minutes, row.get("group_name", "")))

            if walkers is not None and riders is not None and num_players > 0:
                if riders == num_players and walkers == 0:
                    cart_rounds.append(total_minutes)
                elif walkers == num_players and riders == 0:
                    walk_rounds.append(total_minutes)
                elif walkers > 0 and riders > 0:
                    mixed_rounds.append(total_minutes)

        rotation = (row.get("rotation") or "").strip()
        if is_18_holes and rotation in rotation_buckets and total_minutes is not None:
            rotation_buckets[rotation].append(total_minutes)

    fastest = "-"
    slowest = "-"
    average = "-"

    if valid_rounds:
        fastest_minutes, fastest_name = min(valid_rounds, key=lambda x: x[0])
        slowest_minutes, slowest_name = max(valid_rounds, key=lambda x: x[0])

        avg_minutes = round(sum(t for t, _ in valid_rounds) / len(valid_rounds))

        fastest = minutes_to_round_time(fastest_minutes)
        slowest = minutes_to_round_time(slowest_minutes)
        average = minutes_to_round_time(avg_minutes)

    cart_avg = minutes_to_round_time(round(sum(cart_rounds) / len(cart_rounds))) if cart_rounds else "-"
    walk_avg = minutes_to_round_time(round(sum(walk_rounds) / len(walk_rounds))) if walk_rounds else "-"
    mixed_avg = minutes_to_round_time(round(sum(mixed_rounds) / len(mixed_rounds))) if mixed_rounds else "-"

    rotation_pace = {}
    for rotation_name, values in rotation_buckets.items():
        if values:
            rotation_pace[rotation_name] = minutes_to_round_time(round(sum(values) / len(values)))
        else:
            rotation_pace[rotation_name] = "-"

    return {
        "groups": len(rows),
        "total_walkers": sum(int(r.get("walkers") or 0) for r in rows if str(r.get("walkers", "")).strip() != ""),
        "total_riders": sum(int(r.get("riders") or 0) for r in rows if str(r.get("riders", "")).strip() != ""),
        "fastest": fastest,
        "fastest_name": fastest_name,
        "slowest": slowest,
        "slowest_name": slowest_name,
        "average": average,
        "cart_avg": cart_avg,
        "walker_avg": walk_avg,
        "mixed_avg": mixed_avg,
        "rotation_pace": rotation_pace
    }


def extract_pdf_text(path):
    rows = []

    time_only_pattern = re.compile(r"^\d{2}:\d{2}\s[AP]M$")
    inline_time_pattern = re.compile(r"^(\d{2}:\d{2}\s[AP]M)\b")
    last_name_pattern = re.compile(r"^([A-Za-z'`\- ]+),\s")
    inline_last_name_pattern = re.compile(r"([A-Za-z'`\- ]+),\s+[A-Za-z]")

    with fitz.open(path) as doc:
        lines = []
        for page in doc:
            for raw_line in page.get_text("text").splitlines():
                line = raw_line.strip()
                if line:
                    lines.append(line)

    i = 0
    while i < len(lines):
        line = lines[i]

        if time_only_pattern.match(line):
            reservation_time = line
            i += 1

            if i < len(lines) and lines[i] == "E":
                i += 1
                continue

            player_last_names = []

            while i < len(lines):
                current = lines[i]

                if time_only_pattern.match(current):
                    break

                if inline_time_pattern.match(current):
                    break

                name_match = last_name_pattern.match(current)
                if name_match:
                    player_last_names.append(name_match.group(1).strip())

                i += 1

            if player_last_names:
                rows.append({
                    "reservation_time": reservation_time,
                    "group_name": f"{player_last_names[0]} Group",
                    "players": ", ".join(player_last_names),
                    "num_players": str(len(player_last_names)),
                    "walkers": "",
                    "riders": "",
                    "group_type": "",
                    "front": "",
                    "back": "",
                    "rotation": "",
                    "total_time": "",
                    "average_hole": ""
                })
            continue

        inline_time_match = inline_time_pattern.match(line)
        if inline_time_match:
            reservation_time = inline_time_match.group(1)

            if re.search(r"\bE\b", line):
                i += 1
                continue

            inline_last_names = []
            for match in inline_last_name_pattern.finditer(line):
                inline_last_names.append(match.group(1).strip())

            if inline_last_names:
                rows.append({
                    "reservation_time": reservation_time,
                    "group_name": f"{inline_last_names[0]} Group",
                    "players": ", ".join(inline_last_names),
                    "num_players": str(len(inline_last_names)),
                    "walkers": "",
                    "riders": "",
                    "group_type": "",
                    "front": "",
                    "back": "",
                    "rotation": "",
                    "total_time": "",
                    "average_hole": ""
                })

        i += 1

    return rows


# ---------------- ROUTES ----------------

@app.route("/")
def home():
    data = load_data()
    return render_template(
        "index.html",
        has_sheet=len(data["rows"]) > 0,
        db_status="Connected",
        upload_status=None,
        uploaded_filename=None,
        extracted_text=None
    )


@app.route("/upload", methods=["POST"])
def upload():
    if "tee_sheet_pdf" not in request.files:
        return redirect("/")

    file = request.files["tee_sheet_pdf"]

    if file.filename == "":
        return redirect("/")

    temp_path = BASE_DIR / "temp.pdf"
    file.save(temp_path)

    rows = extract_pdf_text(temp_path)

    create_new_sheet(datetime.now().strftime("%Y-%m-%d"), file.filename, rows)
    log_upload(file.filename)

    if temp_path.exists():
        temp_path.unlink()

    return redirect("/tee-sheet")


@app.route("/tee-sheet")
def tee_sheet():
    data = load_data()

    rows = data["rows"]
    for row in rows:
        apply_derived_fields(row)

    edit_id = request.args.get("edit")
    edit_id = int(edit_id) if edit_id is not None else None

    return render_template(
        "tee_sheet.html",
        rows=rows,
        tee_sheet_date=data["date"],
        summary=calculate_summary(rows),
        edit_id=edit_id,
        player_count_options=player_count_options
    )


@app.route("/add-reservation", methods=["POST"])
def add_reservation():
    added = add_row_to_active_sheet({
        "reservation_time": "",
        "group_name": "",
        "players": "",
        "num_players": "",
        "walkers": "",
        "riders": "",
        "group_type": "",
        "front": "",
        "back": "",
        "rotation": "",
        "total_time": "",
        "average_hole": ""
    })

    if not added:
        return redirect("/")

    return redirect("/tee-sheet?edit=0#row-0")


@app.route("/save/<int:index>", methods=["POST"])
def save(index):
    data = load_data()
    rows = data["rows"]
    sheet_id = data["sheet_id"]

    if index < 0 or index >= len(rows) or not sheet_id:
        return redirect("/tee-sheet")

    row = rows[index]

    players_text = (request.form.get("players") or "").strip()
    player_list = [p.strip() for p in players_text.split(",") if p.strip()]

    raw_time = (request.form.get("reservation_time") or "").strip()
    row["reservation_time"] = format_reservation_time(raw_time)
    row["players"] = players_text
    row["num_players"] = str(len(player_list))
    row["group_name"] = f"{player_list[0]} Group" if player_list else ""
    row["walkers"] = request.form.get("walkers", "").strip()
    row["front"] = request.form.get("front", "").strip()
    row["back"] = request.form.get("back", "").strip()
    row["total_time"] = (request.form.get("total_time") or "").strip()

    apply_derived_fields(row)

    sort_rows_by_time(rows)
    save_sorted_rows(sheet_id, rows)

    return redirect("/tee-sheet")


@app.route("/delete/<int:index>", methods=["POST"])
def delete(index):
    data = load_data()
    rows = data["rows"]
    sheet_id = data["sheet_id"]

    if 0 <= index < len(rows) and sheet_id:
        row_id = rows[index]["id"]
        delete_row_by_id(sheet_id, row_id)

    return redirect("/tee-sheet")


@app.route("/clear-tee-sheet", methods=["POST"])
def clear():
    clear_active_sheet()
    return redirect("/")


@app.route("/archive-day", methods=["POST"])
def archive_day():
    success, result = archive_active_sheet()

    if not success:
        flash(result, "warning")
        return redirect("/tee-sheet")

    flash("Day archived successfully. The live tee sheet has been reset.", "success")
    return redirect(url_for("history"))


@app.route("/history")
def history():
    archived_days = get_archived_days()
    return render_template("history.html", archived_days=archived_days)


@app.route("/archived-pdfs/<path:filename>")
def archived_pdf(filename):
    return send_from_directory(PDF_FOLDER, filename)


# ---------------- RUN ----------------

if __name__ == "__main__":
    init_db()
    app.run(debug=True)
