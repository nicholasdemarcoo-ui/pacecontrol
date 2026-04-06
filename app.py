import json
import os
from datetime import datetime

import fitz
from dotenv import load_dotenv
from flask import Flask, redirect, render_template, request, url_for
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

    with open(STATE_FILE, "r") as f:
        return json.load(f)


def save_data(data):
    with open(STATE_FILE, "w") as f:
        json.dump(data, f)


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

    with fitz.open(path) as doc:
        for page in doc:
            text = page.get_text()
            for line in text.split("\n"):
                if ":" in line:
                    rows.append({
                        "reservation_time": line[:5],
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

    return rows


# ---------------- ROUTES ----------------

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

    os.remove(temp_path)

    return redirect("/tee-sheet")


@app.route("/upload", methods=["POST"])
def upload():
    file = request.files["tee_sheet_pdf"]

    if not file:
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
    data = load_data()

    data["rows"].append({
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
    return redirect("/tee-sheet")


@app.route("/save/<int:index>", methods=["POST"])
def save(index):
    data = load_data()
    row = data["rows"][index]

    row["reservation_time"] = request.form.get("reservation_time")
    row["players"] = request.form.get("players")
    row["walkers"] = request.form.get("walkers")
    row["front"] = request.form.get("front")
    row["back"] = request.form.get("back")
    row["total_time"] = request.form.get("total_time")

    save_data(data)
    return redirect("/tee-sheet")


@app.route("/delete/<int:index>", methods=["POST"])
def delete(index):
    data = load_data()
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