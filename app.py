import json
import os
import re
from datetime import datetime

import fitz
from dotenv import load_dotenv
from flask import Flask, redirect, render_template, request, jsonify
from mssql_python import connect

load_dotenv()

app = Flask(__name__)

STATE_FILE = "tee_sheet_data.json"


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
            cursor.execute("INSERT INTO upload_logs (filename) VALUES (?)", (filename,))
            conn.commit()
        return "Saved to database"
    except Exception as e:
        return f"DB error: {e}"


# ---------------- DATA STORAGE ----------------

def load_data():
    if not os.path.exists(STATE_FILE):
        return {"rows": [], "date": ""}

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_data(data):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f)


# ---------------- NEW VERSION ROUTE ----------------

@app.route("/api/version")
def api_version():
    if not os.path.exists(STATE_FILE):
        return jsonify({"version": 0})

    try:
        version = os.path.getmtime(STATE_FILE)
    except Exception:
        version = 0

    return jsonify({"version": version})


# ---------------- HELPERS ----------------

def sort_rows_by_time(rows):
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
        "rotation_pace": {}
    }


def extract_pdf_text(path):
    rows = []

    time_only_pattern = re.compile(r"^\d{2}:\d{2}\s[AP]M$")
    last_name_pattern = re.compile(r"^([A-Za-z'`\- ]+),\s")

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

            player_last_names = []

            while i < len(lines):
                current = lines[i]

                if time_only_pattern.match(current):
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
                    "front": "",
                    "back": "",
                    "rotation": "",
                    "total_time": "",
                    "average_hole": ""
                })
            continue

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

    data = {
        "rows": rows,
        "date": datetime.now().strftime("%B %d, %Y")
    }

    save_data(data)
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


# ---------------- RUN ----------------

if __name__ == "__main__":
    app.run(debug=True)