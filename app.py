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


# ---------------- API ROUTES ----------------

@app.route("/api/status")
def api_status():
    data = load_data()
    return jsonify({
        "has_sheet": len(data["rows"]) > 0
    })


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


def format_total_time(value):
    value = (value or "").strip()
    digits = "".join(ch for ch in value if ch.isdigit())

    if len(digits) == 3:
        return f"{digits[0]}:{digits[1:]}"
    if len(digits) == 4:
        return f"{digits[:2]}:{digits[2:]}"
    return value


def apply_derived_fields(row):
    players_text = (row.get("players") or "").strip()
    player_list = [p.strip() for p in players_text.split(",") if p.strip()]

    # num players
    if player_list:
        row["num_players"] = str(len(player_list))
    else:
        row["num_players"] = str(row.get("num_players") or "").strip()

    # riders
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

    # group type
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

    # rotation
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

    # total time formatting
    row["total_time"] = format_total_time(row.get("total_time") or "")

    # average per hole
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

        # Summary box pace stats: ONLY 18-hole rounds
        if is_18_holes and total_minutes is not None:
            valid_rounds.append((total_minutes, row.get("group_name", "")))

            if walkers is not None and riders is not None and num_players > 0:
                if riders == num_players and walkers == 0:
                    cart_rounds.append(total_minutes)
                elif walkers == num_players and riders == 0:
                    walk_rounds.append(total_minutes)
                elif walkers > 0 and riders > 0:
                    mixed_rounds.append(total_minutes)

        # Print rotation averages also use only 18-hole rotation rows
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

        # Case 1: normal tee time line by itself
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

        # Case 2: compressed one-line row
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

    for row in data["rows"]:
        apply_derived_fields(row)

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
    data = load_data()

    data["rows"].insert(0, {
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

    save_data(data)
    return redirect("/tee-sheet?edit=0#row-0")


@app.route("/save/<int:index>", methods=["POST"])
def save(index):
    data = load_data()

    if index < 0 or index >= len(data["rows"]):
        return redirect("/tee-sheet")

    row = data["rows"][index]

    players_text = (request.form.get("players") or "").strip()
    player_list = [p.strip() for p in players_text.split(",") if p.strip()]

    row["reservation_time"] = request.form.get("reservation_time", "").strip()
    row["players"] = players_text
    row["num_players"] = str(len(player_list))
    row["group_name"] = f"{player_list[0]} Group" if player_list else ""
    row["walkers"] = request.form.get("walkers", "").strip()
    row["front"] = request.form.get("front", "").strip()
    row["back"] = request.form.get("back", "").strip()
    row["total_time"] = (request.form.get("total_time") or "").strip()

    apply_derived_fields(row)

    sort_rows_by_time(data["rows"])
    save_data(data)

    return redirect("/tee-sheet")


@app.route("/delete/<int:index>", methods=["POST"])
def delete(index):
    data = load_data()

    if 0 <= index < len(data["rows"]):
        data["rows"].pop(index)
        save_data(data)

    return redirect("/tee-sheet")


@app.route("/clear-tee-sheet", methods=["POST"])
def clear():
    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
    return redirect("/")


# ---------------- RUN ----------------

if __name__ == "__main__":
    app.run(debug=True)