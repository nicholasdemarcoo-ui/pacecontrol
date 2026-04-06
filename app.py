import os
import re
from datetime import datetime

import fitz
from dotenv import load_dotenv
from flask import Flask, redirect, render_template, request
from mssql_python import connect

load_dotenv()

app = Flask(__name__)


# ---------------- DATABASE ----------------

def get_connection():
    return connect(os.getenv("SQL_CONNECTION_STRING"))


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
            cursor.execute(
                "INSERT INTO upload_logs (filename) VALUES (?)",
                (filename,)
            )
            conn.commit()
        return "Saved to database"
    except Exception as e:
        return f"DB error: {e}"


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
        "display_order": row[13]
    }


def get_active_sheet():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TOP 1 id, sheet_date, source_filename
            FROM dbo.tee_sheets
            WHERE is_active = 1
            ORDER BY updated_at DESC, id DESC
        """)
        row = cursor.fetchone()

    if not row:
        return None

    return {
        "id": row[0],
        "date": row[1].strftime("%B %d, %Y") if row[1] else "",
        "source_filename": row[2] or ""
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

        # Deactivate current active sheet
        cursor.execute("""
            UPDATE dbo.tee_sheets
            SET is_active = 0,
                updated_at = GETDATE()
            WHERE is_active = 1
        """)

        # Create new sheet
        cursor.execute("""
            INSERT INTO dbo.tee_sheets (sheet_date, source_filename, is_active)
            OUTPUT INSERTED.id
            VALUES (?, ?, 1)
        """, (sheet_date, source_filename))

        tee_sheet_id = cursor.fetchone()[0]

        # Insert rows
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


def get_active_sheet_id():
    sheet = get_active_sheet()
    return sheet["id"] if sheet else None


def add_row_to_active_sheet(row_data, insert_at_top=True):
    sheet_id = get_active_sheet_id()
    if not sheet_id:
        return None

    with get_connection() as conn:
        cursor = conn.cursor()

        if insert_at_top:
            cursor.execute("""
                UPDATE dbo.tee_sheet_rows
                SET display_order = display_order + 1,
                    updated_at = GETDATE()
                WHERE tee_sheet_id = ?
            """, (sheet_id,))
            display_order = 0
        else:
            cursor.execute("""
                SELECT ISNULL(MAX(display_order), -1) + 1
                FROM dbo.tee_sheet_rows
                WHERE tee_sheet_id = ?
            """, (sheet_id,))
            display_order = cursor.fetchone()[0]

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
            OUTPUT INSERTED.id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            row_data.get("average_hole") or "",
            display_order
        ))

        new_row_id = cursor.fetchone()[0]

        cursor.execute("""
            UPDATE dbo.tee_sheets
            SET updated_at = GETDATE()
            WHERE id = ?
        """, (sheet_id,))

        conn.commit()

    return new_row_id


def sort_rows_by_time_db(sheet_id):
    rows = get_sheet_rows(sheet_id)

    def parse_time(row):
        value = (row.get("reservation_time") or "").strip()
        if not value:
            return datetime.max

        for fmt in ("%I:%M %p", "%H:%M"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue

        return datetime.max

    rows.sort(key=parse_time)

    with get_connection() as conn:
        cursor = conn.cursor()

        for idx, row in enumerate(rows):
            cursor.execute("""
                UPDATE dbo.tee_sheet_rows
                SET display_order = ?,
                    updated_at = GETDATE()
                WHERE id = ?
            """, (idx, row["id"]))

        cursor.execute("""
            UPDATE dbo.tee_sheets
            SET updated_at = GETDATE()
            WHERE id = ?
        """, (sheet_id,))

        conn.commit()


# ---------------- HELPERS ----------------

def player_count_options(num):
    return list(range(0, int(num) + 1)) if str(num).isdigit() else [0, 1, 2, 3, 4]


def calculate_summary(rows):
    return {
        "groups": len(rows),
        "total_walkers": sum(int(r.get("walkers") or 0) for r in rows),
        "total_riders": sum(int(r.get("riders") or 0) for r in rows),
        "fastest": "-",
        "slowest": "-",
        "average": "-",
        "cart_avg": "-",
        "walker_avg": "-",
        "mixed_avg": "-",
        "rotation_pace": {
            "South-East": "-",
            "South-West": "-",
            "East-West": "-",
            "East-South": "-",
            "West-East": "-",
            "West-South": "-",
            "South": "-",
            "West": "-",
            "East": "-"
        }
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
        has_sheet=len(data["rows"]) > 0
    )


@app.route("/upload", methods=["POST"])
def upload():
    if "tee_sheet_pdf" not in request.files:
        return redirect("/")

    file = request.files["tee_sheet_pdf"]

    if file.filename == "":
        return redirect("/")

    temp_path = "temp.pdf"
    file.save(temp_path)

    rows = extract_pdf_text(temp_path)

    create_new_sheet(datetime.now().date(), file.filename, rows)
    log_upload(file.filename)

    if os.path.exists(temp_path):
        os.remove(temp_path)

    return redirect("/tee-sheet")


@app.route("/tee-sheet")
def tee_sheet():
    data = load_data()

    edit_id = request.args.get("edit")
    edit_id = int(edit_id) if edit_id is not None else None

    return render_template(
        "tee_sheet.html",
        rows=data["rows"],
        tee_sheet_date=data["date"],
        summary=calculate_summary(data["rows"]),
        edit_id=edit_id,
        player_count_options=player_count_options
    )


@app.route("/add-reservation", methods=["POST"])
def add_reservation():
    new_row_id = add_row_to_active_sheet({
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
    }, insert_at_top=True)

    if new_row_id is None:
        return redirect("/")

    return redirect("/tee-sheet?edit=0#row-0")


@app.route("/save/<int:index>", methods=["POST"])
def save(index):
    data = load_data()

    if index < 0 or index >= len(data["rows"]):
        return redirect("/tee-sheet")

    row = data["rows"][index]
    row_id = row["id"]
    sheet_id = data["sheet_id"]

    players_text = (request.form.get("players") or "").strip()
    player_list = [p.strip() for p in players_text.split(",") if p.strip()]

    reservation_time = request.form.get("reservation_time", "").strip()
    walkers = request.form.get("walkers", "").strip()
    front = request.form.get("front", "").strip()
    back = request.form.get("back", "").strip()
    total_time = request.form.get("total_time", "").strip()

    num_players = len(player_list)
    group_name = f"{player_list[0]} Group" if player_list else ""
    riders = num_players - int(walkers) if walkers.isdigit() else None

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE dbo.tee_sheet_rows
            SET reservation_time = ?,
                group_name = ?,
                players = ?,
                num_players = ?,
                walkers = ?,
                riders = ?,
                front_course = ?,
                back_course = ?,
                total_time = ?,
                updated_at = GETDATE()
            WHERE id = ?
        """, (
            reservation_time,
            group_name,
            players_text,
            num_players if player_list else None,
            int(walkers) if walkers.isdigit() else None,
            riders,
            front,
            back,
            total_time,
            row_id
        ))

        cursor.execute("""
            UPDATE dbo.tee_sheets
            SET updated_at = GETDATE()
            WHERE id = ?
        """, (sheet_id,))

        conn.commit()

    sort_rows_by_time_db(sheet_id)

    return redirect("/tee-sheet")


@app.route("/")
def home():
    try:
        data = load_data()
        return render_template(
            "index.html",
            has_sheet=len(data["rows"]) > 0
        )
    except Exception as e:
        return f"Home error: {e}"


@app.route("/clear-tee-sheet", methods=["POST"])
def clear():
    sheet_id = get_active_sheet_id()

    if sheet_id is not None:
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

    return redirect("/")


# ---------------- RUN ----------------

if __name__ == "__main__":
    app.run(debug=True)