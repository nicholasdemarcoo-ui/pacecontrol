import os
import re
import time
from datetime import datetime

import fitz
from dotenv import load_dotenv
from flask import Flask, redirect, render_template, request, jsonify
import pyodbc

import io
from azure.storage.blob import BlobServiceClient
from reportlab.lib import colors
from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

load_dotenv()

app = Flask(__name__)


# ---------------- DATABASE ----------------
def get_connection():
    conn_str = os.getenv("SQL_CONNECTION_STRING")

    if not conn_str:
        raise ValueError("SQL_CONNECTION_STRING is missing")

    attempts = 3
    delay = 2
    last_error = None

    for attempt in range(1, attempts + 1):
        try:
            return pyodbc.connect(conn_str, timeout=30)
        except Exception as e:
            last_error = e
            print(f"DB connection failed (attempt {attempt}): {e}")

            if attempt < attempts:
                time.sleep(delay)
                delay += 2

    raise RuntimeError(f"Database connection failed after {attempts} attempts: {last_error}")


def log_upload(filename):
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT * FROM sysobjects WHERE name='upload_logs' AND xtype='U'
                )
                CREATE TABLE upload_logs (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    filename NVARCHAR(255),
                    uploaded_at DATETIME2 DEFAULT GETDATE()
                )
            """)
            cursor.execute("INSERT INTO upload_logs (filename) VALUES (?)", (filename,))
            conn.commit()
        return "Saved to database"
    except Exception as e:
        return f"DB error: {e}"


# ---------------- LIVE SHEET STORAGE ----------------

def row_to_dict(row):
    return {
        "id": row[0],
        "reservation_time": row[1] or "",
        "group_name": row[2] or "",
        "players": row[3] or "",
        "num_players": str(row[4]) if row[4] is not None else "",
        "walkers": str(row[5]) if row[5] is not None else "",
        "riders": str(row[6]) if row[6] is not None else "",
        "group_type": row[7] or "",
        "front": row[8] or "",
        "back": row[9] or "",
        "rotation": row[10] or "",
        "total_time": row[11] or "",
        "average_hole": row[12] or "",
        "display_order": row[13] if row[13] is not None else 0,
        "tracker_id": row[14],
        "starting_course": row[15],
        "back_nine_course": row[16],
        "course_rotation": row[17],
        "front9_start_time": row[18],
        "front9_finish_time": row[19],
        "back9_start_time": row[20],
        "back9_finish_time": row[21],
        "front9_minutes": row[22],
        "back9_minutes": row[23],
        "turn_gap_minutes": row[24],
        "playing_total_minutes": row[25],
    }


def get_active_sheet():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TOP 1 id, sheet_date, source_filename, updated_at
            FROM dbo.tee_sheets
            WHERE is_active = 1
            ORDER BY updated_at DESC, id DESC
        """)
        row = cursor.fetchone()

    if not row:
        return None

    sheet_date = row[1]

    try:
        formatted_date = sheet_date.strftime("%B %d, %Y") if sheet_date else ""
    except AttributeError:
        formatted_date = str(sheet_date) if sheet_date else ""

    return {
        "id": row[0],
        "sheet_date": sheet_date,
        "date": formatted_date,
        "source_filename": row[2] or "",
        "updated_at": row[3]
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
                display_order,
                tracker_id,
                starting_course,
                back_nine_course,
                course_rotation,
                front9_start_time,
                front9_finish_time,
                back9_start_time,
                back9_finish_time,
                front9_minutes,
                back9_minutes,
                turn_gap_minutes,
                playing_total_minutes
            FROM dbo.tee_sheet_rows
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
            UPDATE dbo.tee_sheets
            SET is_active = 0,
                updated_at = GETDATE()
            WHERE is_active = 1
        """)

        cursor.execute("""
            INSERT INTO dbo.tee_sheets (sheet_date, source_filename, is_active)
            OUTPUT INSERTED.id
            VALUES (?, ?, 1)
        """, (sheet_date, source_filename))

        tee_sheet_id = cursor.fetchone()[0]

        for idx, row in enumerate(rows):
            cursor.execute("""
                INSERT INTO dbo.tee_sheet_rows (
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


def add_row_to_active_sheet(row_data):
    data = load_data()
    sheet_id = data["sheet_id"]

    if not sheet_id:
        return None

    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE dbo.tee_sheet_rows
            SET display_order = display_order + 1,
                updated_at = GETDATE()
            WHERE tee_sheet_id = ?
        """, (sheet_id,))

        cursor.execute("""
            INSERT INTO dbo.tee_sheet_rows (
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
            UPDATE dbo.tee_sheets
            SET updated_at = GETDATE()
            WHERE id = ?
        """, (sheet_id,))

        conn.commit()

    return True


def save_sorted_rows(sheet_id, rows):
    with get_connection() as conn:
        cursor = conn.cursor()

        for idx, row in enumerate(rows):
            cursor.execute("""
                UPDATE dbo.tee_sheet_rows
                SET display_order = ?,
                    tracker_id = ?,
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
                    updated_at = GETDATE()
                WHERE id = ? AND tee_sheet_id = ?
            """, (
                idx,
                row.get("tracker_id"),
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
            UPDATE dbo.tee_sheets
            SET updated_at = GETDATE()
            WHERE id = ?
        """, (sheet_id,))

        conn.commit()


def delete_row_by_id(sheet_id, row_id):
    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            DELETE FROM dbo.tee_sheet_rows
            WHERE id = ? AND tee_sheet_id = ?
        """, (row_id, sheet_id))

        cursor.execute("""
            UPDATE dbo.tee_sheets
            SET updated_at = GETDATE()
            WHERE id = ?
        """, (sheet_id,))

        conn.commit()


def clear_active_sheet():
    sheet = get_active_sheet()
    if not sheet:
        return

    sheet_id = sheet["id"]

    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("DELETE FROM dbo.tee_sheet_rows WHERE tee_sheet_id = ?", (sheet_id,))
        cursor.execute("""
            UPDATE dbo.tee_sheets
            SET is_active = 0,
                updated_at = GETDATE()
            WHERE id = ?
        """, (sheet_id,))

        conn.commit()


# ---------------- API ROUTES ----------------

@app.route("/api/status")
def api_status():
    data = load_data()
    return jsonify({
        "has_sheet": len(data["rows"]) > 0
    })


@app.route("/api/version")
def api_version():
    sheet = get_active_sheet()
    if not sheet or not sheet["updated_at"]:
        return jsonify({"version": 0})

    try:
        version = sheet["updated_at"].timestamp()
    except Exception:
        version = 0

    return jsonify({"version": version})


# ---------------- HELPERS ----------------
def delete_archive_record(record_id):
    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT pdf_url
            FROM dbo.archive_records
            WHERE id = ?
        """, (record_id,))
        row = cursor.fetchone()

        if not row:
            return

        pdf_url = row[0] or ""

        if pdf_url:
            try:
                connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
                container_name = os.getenv("AZURE_STORAGE_CONTAINER", "archive")

                if connection_string:
                    from azure.storage.blob import BlobServiceClient

                    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

                    blob_name = pdf_url.split(f"/{container_name}/", 1)[-1]
                    blob_client = blob_service_client.get_blob_client(
                        container=container_name,
                        blob=blob_name
                    )
                    blob_client.delete_blob()
            except Exception as e:
                print("DELETE ARCHIVE PDF ERROR:", e)

        cursor.execute("""
            DELETE FROM dbo.archive_records
            WHERE id = ?
        """, (record_id,))
        conn.commit()

def get_blob_service_client():
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING is not set")
    return BlobServiceClient.from_connection_string(connection_string)


def generate_archive_pdf_bytes(sheet, rows):
    import io
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

    buffer = io.BytesIO()

    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        leftMargin=12,
        rightMargin=12,
        topMargin=12,
        bottomMargin=12
    )

    styles = getSampleStyleSheet()

    title_style = ParagraphStyle(
        "ArchiveTitle",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=18,
        leading=20,
        alignment=TA_CENTER,
        textColor=colors.black,
        spaceAfter=4
    )

    subtitle_style = ParagraphStyle(
        "ArchiveSubtitle",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=9,
        leading=10,
        alignment=TA_CENTER,
        textColor=colors.HexColor("#666666"),
        spaceAfter=8
    )

    story = []

    story.append(Paragraph("Tee Sheet Archive", title_style))
    story.append(Paragraph(sheet.get("date") or "", subtitle_style))

    total_groups = len(rows)
    total_players = 0
    for row in rows:
        try:
            total_players += int(row.get("num_players") or 0)
        except Exception:
            pass

    summary_data = [[
        f"Groups: {total_groups}",
        f"Players: {total_players}"
    ]]

    summary_table = Table(summary_data, colWidths=[120, 120])
    summary_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f4f7fb")),
        ("TEXTCOLOR", (0, 0), (-1, -1), colors.black),
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#cfd8e3")),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#cfd8e3")),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(summary_table)
    story.append(Spacer(1, 8))

    table_data = [[
        "Time",
        "Group Name",
        "Players",
        "#",
        "Rotation",
        "Total"
    ]]

    for row in rows:
        table_data.append([
            row.get("reservation_time") or "",
            row.get("group_name") or "",
            row.get("players") or "",
            row.get("num_players") or "",
            row.get("rotation") or "",
            row.get("total_time") or ""
        ])

    col_widths = [45, 95, 245, 25, 55, 45]

    data_table = Table(table_data, colWidths=col_widths, repeatRows=1)
    data_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0b57d0")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 7.5),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),

        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 6.5),

        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [
            colors.white,
            colors.HexColor("#f7f9fc")
        ]),

        ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#ccd6e0")),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#bcc8d6")),

        ("ALIGN", (0, 1), (0, -1), "CENTER"),
        ("ALIGN", (3, 1), (5, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),

        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
    ]))

    story.append(data_table)
    doc.build(story)

    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes

    styles = getSampleStyleSheet()
    story = []

    title = f"Tee Sheet Archive - {sheet.get('date') or ''}"
    story.append(Paragraph(title, styles["Title"]))
    story.append(Spacer(1, 12))

    summary_data = [
        ["Sheet Date", sheet.get("date") or ""],
        ["Source File", sheet.get("source_filename") or ""],
        ["Saved From", "Pace Control"]
    ]

    summary_table = Table(summary_data, colWidths=[120, 300])
    summary_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#0b57d0")),
        ("TEXTCOLOR", (0, 0), (0, -1), colors.white),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica-Bold"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("PADDING", (0, 0), (-1, -1), 6),
    ]))
    story.append(summary_table)
    story.append(Spacer(1, 18))

    table_data = [[
        "Time", "Group Name", "Players", "# Players",
        "Walkers", "Riders", "Front", "Back",
        "Rotation", "Total Time", "Average/Hole"
    ]]

    for row in rows:
        table_data.append([
            row.get("reservation_time") or "",
            row.get("group_name") or "",
            row.get("players") or "",
            row.get("num_players") or "",
            row.get("walkers") or "",
            row.get("riders") or "",
            row.get("front") or "",
            row.get("back") or "",
            row.get("rotation") or "",
            row.get("total_time") or "",
            row.get("average_hole") or ""
        ])

    col_widths = [60, 110, 220, 55, 55, 55, 45, 45, 70, 65, 65]

    data_table = Table(table_data, colWidths=col_widths, repeatRows=1)
    data_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0b57d0")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.grey),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f5f5f5")]),
        ("PADDING", (0, 0), (-1, -1), 4),
    ]))

    story.append(data_table)
    doc.build(story)

    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes


def upload_archive_pdf(pdf_bytes, filename):
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_name = os.getenv("AZURE_STORAGE_CONTAINER", "archive")

    if not connection_string:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING is missing")

    from azure.storage.blob import BlobServiceClient

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)

    blob_client.upload_blob(
        pdf_bytes,
        overwrite=True,
        content_type="application/pdf"
    )

    return blob_client.url


def get_archive_records():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                id,
                CONVERT(VARCHAR(50), sheet_date, 107) AS sheet_date_display,
                archive_name,
                CONVERT(VARCHAR(50), saved_at, 100) AS saved_at_display,
                pdf_url
            FROM dbo.archive_records
            ORDER BY saved_at DESC
        """)
        rows = cursor.fetchall()

    return [
        {
            "id": row[0],
            "sheet_date": row[1] or "",
            "archive_name": row[2] or "",
            "saved_at": row[3] or "",
            "pdf_url": row[4] or ""
        }
        for row in rows
    ]


def save_archive_record():
    sheet = get_active_sheet()
    if not sheet:
        raise ValueError("No active sheet found")

    rows = get_sheet_rows(sheet["id"])

    safe_date = (sheet.get("date") or "archive").replace(",", "").replace(" ", "_")
    filename = f"tee_sheet_{safe_date}.pdf"

    pdf_bytes = generate_archive_pdf_bytes(sheet, rows)
    pdf_url = upload_archive_pdf(pdf_bytes, filename)

    if not pdf_url:
        raise ValueError("PDF upload failed: upload_archive_pdf returned no URL")

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO dbo.archive_records (
                sheet_date,
                source_filename,
                archive_name,
                pdf_url
            )
            VALUES (
                (SELECT TOP 1 sheet_date
                 FROM dbo.tee_sheets
                 WHERE is_active = 1
                 ORDER BY updated_at DESC, id DESC),
                ?,
                ?,
                ?
            )
        """, (
            sheet.get("source_filename") or "",
            f"Tee Sheet - {sheet.get('date')}" if sheet.get("date") else "Tee Sheet Archive",
            pdf_url
        ))
        conn.commit()

def get_archive_record_by_id(record_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                id,
                CONVERT(VARCHAR(50), sheet_date, 107) AS sheet_date_display,
                archive_name,
                source_filename,
                CONVERT(VARCHAR(50), saved_at, 100) AS saved_at_display
            FROM dbo.archive_records
            WHERE id = ?
        """, (record_id,))
        row = cursor.fetchone()

    if not row:
        return None

    return {
        "id": row[0],
        "sheet_date": row[1] or "",
        "archive_name": row[2] or "",
        "source_filename": row[3] or "",
        "saved_at": row[4] or ""
    }

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


def format_reservation_time(value, existing_value=""):
    value = (value or "").strip()
    if not value:
        return ""

    normalized = value.upper().replace(".", "").strip()
    digits = "".join(ch for ch in normalized if ch.isdigit())

    meridiem = ""
    if "AM" in normalized:
        meridiem = "AM"
    elif "PM" in normalized:
        meridiem = "PM"
    else:
        existing_upper = (existing_value or "").upper()
        if "AM" in existing_upper:
            meridiem = "AM"
        elif "PM" in existing_upper:
            meridiem = "PM"

    hour = None
    minute = None

    if ":" in value:
        parts = value.split(":")
        if len(parts) == 2:
            try:
                hour = int(parts[0].strip())
                minute = int("".join(ch for ch in parts[1] if ch.isdigit()))
            except ValueError:
                return value
    elif len(digits) == 3:
        hour = int(digits[0])
        minute = int(digits[1:])
    elif len(digits) == 4:
        hour = int(digits[:2])
        minute = int(digits[2:])
    else:
        return value

    if minute < 0 or minute > 59:
        return value

    if not meridiem:
        if hour == 12:
            meridiem = "PM"
        elif 1 <= hour <= 6:
            meridiem = "PM"
        else:
            meridiem = "AM"

    if hour == 0:
        hour = 12
        if not meridiem:
            meridiem = "AM"
    elif hour > 12:
        if hour <= 23:
            hour -= 12
            meridiem = "PM"
        else:
            return value

    if hour < 1 or hour > 12:
        return value

    return f"{hour}:{str(minute).zfill(2)} {meridiem}"


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
        "West-South": []
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
        "total_players": sum(int(r.get("num_players") or 0) for r in rows),
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
@app.route("/archive/delete/<int:record_id>", methods=["POST"])
def archive_delete(record_id):
    delete_archive_record(record_id)
    return redirect("/archive")

@app.route("/archive/save-current", methods=["POST"])
def archive_save_current():
    save_archive_record()
    return redirect("/archive")
    
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


@app.route("/upload", methods=["GET", "POST"])
def upload():
    if request.method == "GET":
        return redirect("/")

    if "tee_sheet_pdf" not in request.files:
        return redirect("/")

    file = request.files["tee_sheet_pdf"]

    if file.filename == "":
        return redirect("/")

    temp_path = "temp.pdf"
    file.save(temp_path)

    try:
        rows = extract_pdf_text(temp_path)
        create_new_sheet(datetime.now().date(), file.filename, rows)
        log_upload(file.filename)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

    return redirect("/tee-sheet")


@app.route("/tee-sheet")
def tee_sheet():
    data = load_data()
    rows = data["rows"]

    for row in rows:
        apply_derived_fields(row)

    edit_id = request.args.get("edit")
    edit_id = int(edit_id) if edit_id is not None else None

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, display_name FROM trackers WHERE is_active = 1")
        trackers = cursor.fetchall()

    summary = calculate_summary(rows)

    return render_template(
        "tee_sheet.html",
        rows=rows,
        tee_sheet_date=data["date"],
        summary=summary,
        edit_id=edit_id,
        player_count_options=player_count_options,
        trackers=trackers
    )
    
@app.route("/archive")
def archive():
    records = get_archive_records()
    return render_template("archive.html", records=records)

@app.route("/archive/view/<int:record_id>")
def archive_view(record_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT pdf_url
            FROM dbo.archive_records
            WHERE id = ?
        """, (record_id,))
        row = cursor.fetchone()

    if not row or not row[0]:
        return redirect("/archive")

    return redirect(row[0])


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
    row["reservation_time"] = format_reservation_time(raw_time, row.get("reservation_time") or "")
    row["players"] = players_text
    row["num_players"] = str(len(player_list))
    row["group_name"] = f"{player_list[0]} Group" if player_list else ""
    row["walkers"] = request.form.get("walkers", "").strip()
    row["front"] = request.form.get("front", "").strip()
    row["back"] = request.form.get("back", "").strip()
    row["total_time"] = (request.form.get("total_time") or "").strip()
    row["tracker_id"] = request.form.get("tracker_id")

    apply_derived_fields(row)

    sort_rows_by_time(rows)
    save_sorted_rows(sheet_id, rows)

    scroll_top = request.form.get("scroll_top", "0")
    return redirect(f"/tee-sheet?scroll_top={scroll_top}")


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


# ---------------- RUN ----------------

if __name__ == "__main__":
    app.run(debug=True)

