import os
import re
import fitz
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = "uploads"

tee_sheet_rows = []
tee_sheet_date = ""


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


def build_rotation(front: str, back: str):
    front = (front or "").strip()
    back = (back or "").strip()

    if front and back:
        return f"{front}-{back}"
    if front:
        return front
    if back:
        return back
    return ""


def holes_for_rotation(rotation: str):
    if not rotation:
        return 18

    if rotation in {"South", "West", "East"}:
        return 9

    return 18


def time_to_minutes(time_str):
    if not time_str:
        return None

    value = time_str.strip()

    if value.isdigit():
        if len(value) == 3:
            hours = int(value[0])
            minutes = int(value[1:])
        elif len(value) == 4:
            hours = int(value[:2])
            minutes = int(value[2:])
        else:
            return None

        if minutes >= 60:
            return None

        return hours * 60 + minutes

    match = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", value)
    if match:
        hours = int(match.group(1))
        minutes = int(match.group(2))
        if minutes >= 60:
            return None
        return hours * 60 + minutes

    return None


def minutes_to_time(minutes):
    if minutes is None:
        return ""

    hours = minutes // 60
    mins = minutes % 60
    return f"{hours}:{mins:02d}"


def build_average_hole(total_time: str, rotation: str):
    mins = time_to_minutes(total_time)
    if mins is None:
        return ""

    holes = holes_for_rotation(rotation)
    if holes <= 0:
        return ""

    avg_minutes = mins / holes
    avg_whole = int(avg_minutes)
    avg_seconds = int(round((avg_minutes - avg_whole) * 60))

    if avg_seconds == 60:
        avg_whole += 1
        avg_seconds = 0

    return f"{avg_whole:02d}:{avg_seconds:02d}"


def normalize_reservation_time(time_str):
    if not time_str:
        return ""

    value = time_str.strip().upper().replace(".", "")

    formats = [
        "%I:%M %p",
        "%I:%M%p",
        "%H:%M",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.strftime("%I:%M %p")
        except ValueError:
            continue

    return value


def time_sort_key(time_str):
    normalized = normalize_reservation_time(time_str)
    if not normalized:
        return datetime.max

    try:
        return datetime.strptime(normalized, "%I:%M %p")
    except ValueError:
        return datetime.max


def sort_rows():
    global tee_sheet_rows
    tee_sheet_rows.sort(key=lambda row: time_sort_key(row.get("reservation_time", "")))


def extract_sheet_date(full_text: str):
    match = re.search(r"Sheet Date:\s*(\d{2}/\d{2}/\d{4})", full_text)
    if not match:
        return ""

    raw_date = match.group(1)

    try:
        dt = datetime.strptime(raw_date, "%m/%d/%Y")
        return dt.strftime("%m/%d/%Y")
    except ValueError:
        return raw_date


def build_summary(rows):
    total_groups = len(rows)
    total_walkers = 0
    total_riders = 0

    completed = []
    cart_times = []
    walker_times = []
    mixed_times = []

    rotation_buckets = {
        "South-East": [],
        "South-West": [],
        "East-West": [],
        "East-South": [],
        "West-East": [],
        "West-South": [],
        "South": [],
        "West": [],
        "East": [],
    }

    for row in rows:
        walkers_raw = row.get("walkers", "")
        riders_raw = row.get("riders", "")

        try:
            if walkers_raw != "":
                total_walkers += int(walkers_raw)
            if riders_raw != "":
                total_riders += int(riders_raw)
        except Exception:
            pass

        total_time = row.get("total_time", "")
        mins = time_to_minutes(total_time)

        if mins is None:
            continue

        completed.append((mins, row))

        walkers = row.get("walkers")
        riders = row.get("riders")
        num_players = row.get("num_players")
        rotation = row.get("rotation", "")

        try:
            walkers = int(walkers)
            riders = int(riders)
            num_players = int(num_players)
        except Exception:
            continue

        if riders == num_players and num_players > 0:
            cart_times.append(mins)
        elif walkers == num_players and num_players > 0:
            walker_times.append(mins)
        elif walkers > 0 and riders > 0:
            mixed_times.append(mins)

        if rotation in rotation_buckets:
            rotation_buckets[rotation].append(mins)

    fastest = min(completed, default=None, key=lambda x: x[0])
    slowest = max(completed, default=None, key=lambda x: x[0])

    avg = None
    if completed:
        avg = sum(x[0] for x in completed) // len(completed)

    cart_avg = sum(cart_times) // len(cart_times) if cart_times else None
    walker_avg = sum(walker_times) // len(walker_times) if walker_times else None
    mixed_avg = sum(mixed_times) // len(mixed_times) if mixed_times else None

    rotation_pace = {
        key: minutes_to_time(sum(vals) // len(vals)) if vals else ""
        for key, vals in rotation_buckets.items()
    }

    return {
        "groups": total_groups,
        "total_walkers": total_walkers,
        "total_riders": total_riders,
        "fastest": minutes_to_time(fastest[0]) if fastest else "",
        "fastest_name": fastest[1]["group_name"] if fastest else "",
        "slowest": minutes_to_time(slowest[0]) if slowest else "",
        "slowest_name": slowest[1]["group_name"] if slowest else "",
        "average": minutes_to_time(avg),
        "cart_avg": minutes_to_time(cart_avg),
        "walker_avg": minutes_to_time(walker_avg),
        "mixed_avg": minutes_to_time(mixed_avg),
        "rotation_pace": rotation_pace,
    }


def parse_tee_sheet_pdf(pdf_path: str):
    rows = []
    doc = fitz.open(pdf_path)

    full_text = ""
    for page in doc:
        full_text += page.get_text("text") + "\n"

    doc.close()

    parsed_date = extract_sheet_date(full_text)

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
            "front": "",
            "back": "",
            "rotation": "",
            "total_time": "",
            "average_hole": ""
        })

    rows.sort(key=lambda row: time_sort_key(row.get("reservation_time", "")))
    return rows, parsed_date


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload_pdf():
    global tee_sheet_rows, tee_sheet_date

    pdf_file = request.files.get("tee_sheet_pdf")
    if not pdf_file or pdf_file.filename == "":
        return redirect(url_for("home"))

    os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)
    save_path = os.path.join(app.config["UPLOAD_FOLDER"], pdf_file.filename)
    pdf_file.save(save_path)

    tee_sheet_rows, tee_sheet_date = parse_tee_sheet_pdf(save_path)
    sort_rows()

    return redirect(url_for("tee_sheet"))


@app.route("/tee-sheet")
def tee_sheet():
    edit_id = request.args.get("edit")
    try:
        edit_id = int(edit_id) if edit_id is not None else None
    except ValueError:
        edit_id = None

    summary = build_summary(tee_sheet_rows)

    return render_template(
        "tee_sheet.html",
        rows=tee_sheet_rows,
        edit_id=edit_id,
        player_count_options=player_count_options,
        summary=summary,
        tee_sheet_date=tee_sheet_date
    )


@app.route("/add-reservation", methods=["POST"])
def add_reservation():
    global tee_sheet_rows

    new_row = {
        "reservation_time": "",
        "group_name": "",
        "players": "",
        "num_players": 0,
        "walkers": "",
        "riders": "",
        "group_type": "",
        "front": "",
        "back": "",
        "rotation": "",
        "total_time": "",
        "average_hole": ""
    }

    tee_sheet_rows.insert(0, new_row)
    return redirect(url_for("tee_sheet", edit=0))


@app.route("/save/<int:row_id>", methods=["POST"])
def save_row(row_id):
    global tee_sheet_rows

    if row_id < 0 or row_id >= len(tee_sheet_rows):
        return redirect(url_for("tee_sheet"))

    reservation_time = normalize_reservation_time(
        request.form.get("reservation_time", "").strip()
    )
    players = request.form.get("players", "").strip()
    walkers = request.form.get("walkers", "").strip()
    front = request.form.get("front", "").strip()
    back = request.form.get("back", "").strip()

    raw_time = request.form.get("total_time", "").strip()
    mins = time_to_minutes(raw_time)
    total_time = minutes_to_time(mins) if mins is not None else ""

    player_list = parse_players(players)
    num_players = len(player_list)
    group_name = build_group_name(players)

    if walkers == "":
        riders = ""
    else:
        try:
            walkers_int = int(walkers)
            walkers_int = max(0, min(walkers_int, num_players))
            walkers = str(walkers_int)
            riders = str(num_players - walkers_int)
        except ValueError:
            walkers = ""
            riders = ""

    group_type = build_group_type(num_players, walkers, riders)
    rotation = build_rotation(front, back)
    average_hole = build_average_hole(total_time, rotation)

    tee_sheet_rows[row_id] = {
        "reservation_time": reservation_time,
        "group_name": group_name,
        "players": players,
        "num_players": num_players,
        "walkers": walkers,
        "riders": riders,
        "group_type": group_type,
        "front": front,
        "back": back,
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
