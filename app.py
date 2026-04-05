import os
import re
import fitz  # PyMuPDF
from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = "uploads"

tee_sheet_rows = []


def extract_last_name(full_name: str) -> str:
    """
    Example:
    'DiFalco, Christopher (D0925)' -> 'DiFalco'
    """
    if "," in full_name:
        return full_name.split(",")[0].strip()
    return full_name.strip()


def clean_player_name(name: str) -> str:
    """
    Keeps 'Last, First' and removes the member code in parentheses.
    Example:
    'DiFalco, Christopher (D0925)' -> 'DiFalco, Christopher'
    """
    return re.sub(r"\s*\([^)]+\)", "", name).strip()


def parse_tee_sheet_pdf(pdf_path: str):
    import fitz
    import re

    rows = []
    doc = fitz.open(pdf_path)

    full_text = ""

    for page in doc:
        full_text += page.get_text()

    doc.close()

    lines = [line.strip() for line in full_text.split("\n") if line.strip()]

    time_pattern = re.compile(r"\d{2}:\d{2} (AM|PM)")

    current_time = None
    current_players = []

    for line in lines:
        # Check if this line is a tee time
        if time_pattern.match(line):
            # Save previous group if exists
            if current_time and current_players:
                last_names = [p.split(",")[0] for p in current_players]

                rows.append({
                    "reservation_time": current_time,
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

            current_time = line
            current_players = []
            continue

        # Skip empty slots
        if line == "E":
            current_time = None
            current_players = []
            continue

        # Capture player lines
        if "," in line and "(" in line:
            # Clean player name
            name = re.sub(r"\s*\([^)]+\)", "", line)
            current_players.append(name)

    # Catch last group
    if current_time and current_players:
        last_names = [p.split(",")[0] for p in current_players]

        rows.append({
            "reservation_time": current_time,
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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
