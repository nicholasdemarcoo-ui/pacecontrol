import os
import re
import fitz
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = "uploads"

tee_sheet_rows = []


def parse_players(players_text: str):
    return [p.strip() for p in players_text.split(",") if p.strip()]

def player_count_options(num_players):
    try:
        count = int(num_players)
    except (ValueError, TypeError):
        count = 4

    count = max(0, min(count, 4))
    return list(range(count + 1))


def build_group_name(players_text: str):
    players = parse_players(players_text)
    return f"{players[0]} Group" if players else ""


def build_group_type(num_players, walkers, riders):
    try:
        num_players = int(num_players)
        walkers = int(walkers) if walkers != "" else 0
        riders = int(riders) if riders != "" else 0
    except ValueError:
        return ""

    if walkers == num_players and riders == 0:
        return "All Walk"
    if riders == num_players and walkers == 0:
        return "All Ride"
    if walkers == 2 and riders == 2:
        return "2W / 2R"
    if walkers == 3 and riders == 1:
        return "3W / 1R"
    if walkers == 1 and riders == 3:
        return "1W / 3R"
    if walkers == 2 and riders == 1:
        return "2W / 1R"
    if walkers == 1 and riders == 2:
        return "1W / 2R"

    return ""


def build_average_hole(total_time: str):
    if not total_time:
        return ""

    match = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", total_time)
    if not match:
        return ""

    hours = int(match.group(1))
    minutes = int(match.group(2))
    total_minutes = hours * 60 + minutes

    avg_minutes = total_minutes / 18
    avg_whole = int(avg_minutes)
    avg_seconds = int(round((avg_minutes - avg_whole) * 60))

    if avg_seconds == 60:
        avg_whole += 1
        avg_seconds = 0

    return f"{avg_whole:02d}:{avg_seconds:02d}"


def time_sort_key(time_str):
    """
    Converts times like '08:00 AM' into sortable datetime values.
    Blank or invalid times go to the bottom.
    """
    if not time_str:
        return datetime.max

    try:
        return datetime.strptime(time_str.strip(), "%I:%M %p")
    except ValueError:
        return datetime.max


def sort_rows():
    global tee_sheet_rows
    tee_sheet_rows.sort(key=lambda row: time_sort_key(row.get("reservation_time", "")))


def parse_tee_sheet_pdf(pdf_path: str):
    rows = []
    doc = fitz.open(pdf_path)

    full_text = ""
    for page in doc:
        full_text += page.get_text("text") + "\n"

    doc.close()

    lines = [line.strip() for line in full_text.splitlines() if line.strip()]

    time_pattern = re.compile(r"^\d{2}:\d{2}\s(?:AM|PM)$")
    player_pattern = re.compile(r"^[^,]+,\s*[^()]+\(.*\)$")

    time_indices = []
    for idx, line in enumerate(lines):
        if time_pattern.match(line):
            time_indices.append(idx)

    for n, start_idx in enumerate(time_indices):
        reservation_time = lines[start_idx]

        if reservation_time == "06:30 PM":
            break

        end_idx = time_indices[n + 1] if n + 1 < len(time_indices) else len(lines)
        block = lines[start_idx:end_idx]

        player_names = []

        for line in block:
            if player_pattern.match(line):
                clean_name = re.sub(r"\s*\([^)]+\)", "", line).strip()
                player_names.append(clean_name)

        if not player_names:
            continue

        last_names = [name.split(",")[0].strip() for name in player_names]

        rows.append({
            "reservation_time": reservation_time,
            "group_name": f"{last_names[0]} Group",
            "players": ", ".join(last_names),
            "num_players": len(last_names),
            "walkers": "",
            "riders": "",
            "group_type": "",
            "rotation": "",
            "total_time": "",
            "average_hole": ""
        })

    rows.sort(key=lambda row: time_sort_key(row.get("reservation_time", "")))
    return rows


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload_pdf():
    global tee_sheet_rows

    pdf_file = request.files.get("tee_sheet_pdf")
    if not pdf_file or pdf_file.filename == "":
        return redirect(url_for("home"))

    os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)
    save_path = os.path.join(app.config["UPLOAD_FOLDER"], pdf_file.filename)
    pdf_file.save(save_path)

    tee_sheet_rows = parse_tee_sheet_pdf(save_path)
    sort_rows()

    return redirect(url_for("tee_sheet"))


@app.route("/tee-sheet")
def tee_sheet():
    edit_id = request.args.get("edit")
    try:
        edit_id = int(edit_id) if edit_id is not None else None
    except ValueError:
        edit_id = None

    return render_template(
        "tee_sheet.html",
        rows=tee_sheet_rows,
        edit_id=edit_id,
        player_count_options=player_count_options)

@app.route("/add-reservation", methods=["POST"])
def add_reservation():
    global tee_sheet_rows

    tee_sheet_rows.append({
        "reservation_time": "",
        "group_name": "",
        "players": "",
        "num_players": 0,
        "walkers": "",
        "riders": "",
        "group_type": "",
        "rotation": "",
        "total_time": "",
        "average_hole": ""
    })

    new_index = len(tee_sheet_rows) - 1
    return redirect(url_for("tee_sheet", edit=new_index))


@app.route("/save/<int:row_id>", methods=["POST"])
def save_row(row_id):
    global tee_sheet_rows

    if row_id < 0 or row_id >= len(tee_sheet_rows):
        return redirect(url_for("tee_sheet"))

    reservation_time = request.form.get("reservation_time", "").strip()
    players = request.form.get("players", "").strip()
    walkers = request.form.get("walkers", "").strip()
    riders = request.form.get("riders", "").strip()
    rotation = request.form.get("rotation", "").strip()
    total_time = request.form.get("total_time", "").strip()

    player_list = parse_players(players)
    num_players = len(player_list)
    group_name = build_group_name(players)
    group_type = build_group_type(num_players, walkers, riders)
    average_hole = build_average_hole(total_time)

    tee_sheet_rows[row_id] = {
        "reservation_time": reservation_time,
        "group_name": group_name,
        "players": players,
        "num_players": num_players,
        "walkers": walkers,
        "riders": riders,
        "group_type": group_type,
        "rotation": rotation,
        "total_time": total_time,
        "average_hole": average_hole
    }

    sort_rows()
    return redirect(url_for("tee_sheet"))


@app.route("/delete/<int:row_id>", methods=["POST"])
def delete_row(row_id):
    global tee_sheet_rows

    if 0 <= row_id < len(tee_sheet_rows):
        tee_sheet_rows.pop(row_id)

    sort_rows()
    return redirect(url_for("tee_sheet"))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
