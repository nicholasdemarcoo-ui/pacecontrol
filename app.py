import os
import re
import fitz
from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = "uploads"

tee_sheet_rows = []


def parse_players(players_text: str):
    players = [p.strip() for p in players_text.split(",") if p.strip()]
    return players


def build_group_name(players_text: str):
    players = parse_players(players_text)
    if not players:
        return ""
    return f"{players[0]} Group"


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


def parse_tee_sheet_pdf(pdf_path: str):
    rows = []
    doc = fitz.open(pdf_path)

    full_text = ""
    for page in doc:
        full_text += page.get_text("text") + "\n"

    doc.close()

    lines = [line.strip() for line in full_text.splitlines() if line.strip()]

    time_pattern = re.compile(r"^\d{2}:\d{2}\s(?:AM|PM)$")
    player_pattern = re.compile(r"^[A-Za-z' .-]+,\s+[A-Za-z' .-]+.*\([A-Za-z0-9]+\)$")

    i = 0
    while i < len(lines):
        line = lines[i]

        if not time_pattern.match(line):
            i += 1
            continue

        reservation_time = line
        i += 1

        if i >= len(lines):
            break

        status = None
        lookahead_index = i

        while lookahead_index < len(lines) and lookahead_index < i + 8:
            if lines[lookahead_index] in {"R", "E"}:
                status = lines[lookahead_index]
                break
            lookahead_index += 1

        if status != "R":
            continue

        player_names = []

        while i < len(lines):
            current_line = lines[i]

            if time_pattern.match(current_line):
                break

            if player_pattern.match(current_line):
                clean_name = re.sub(r"\s*\([^)]+\)", "", current_line).strip()
                player_names.append(clean_name)

            i += 1

        if player_names:
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

    return redirect(url_for("tee_sheet"))


@app.route("/tee-sheet")
def tee_sheet():
    return render_template("tee_sheet.html", rows=tee_sheet_rows)


@app.route("/edit/<int:row_id>", methods=["GET", "POST"])
def edit_row(row_id):
    global tee_sheet_rows

    if row_id < 0 or row_id >= len(tee_sheet_rows):
        return redirect(url_for("tee_sheet"))

    row = tee_sheet_rows[row_id]

    if request.method == "POST":
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

        return redirect(url_for("tee_sheet"))

    return render_template("edit_row.html", row=row, row_id=row_id)


@app.route("/delete/<int:row_id>", methods=["POST"])
def delete_row(row_id):
    global tee_sheet_rows

    if 0 <= row_id < len(tee_sheet_rows):
        tee_sheet_rows.pop(row_id)

    return redirect(url_for("tee_sheet"))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
