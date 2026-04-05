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
    rows = []
    doc = fitz.open(pdf_path)

    time_pattern = re.compile(r"^\d{2}:\d{2}$")
    ampm_values = {"AM", "PM"}

    for page in doc:
        words = page.get_text("words")
        if not words:
            continue

        # Each word item:
        # (x0, y0, x1, y1, text, block_no, line_no, word_no)

        # Sort by y then x for stability
        words = sorted(words, key=lambda w: (round(w[1], 1), w[0]))

        time_entries = []

        # Build time rows from the left side of the page
        for i, w in enumerate(words):
            x0, y0, x1, y1, text = w[0], w[1], w[2], w[3], w[4]

            # Time column is on far left
            if x0 > 130:
                continue

            # Match "08:00" then next word "AM"/"PM"
            if time_pattern.match(text):
                ampm = None

                # Look for nearby AM/PM on the same row
                for j in range(i + 1, min(i + 4, len(words))):
                    nw = words[j]
                    nx0, ny0, nx1, ny1, ntext = nw[0], nw[1], nw[2], nw[3], nw[4]
                    if abs(ny0 - y0) < 6 and ntext in ampm_values:
                        ampm = ntext
                        break

                if ampm:
                    full_time = f"{text} {ampm}"
                    time_entries.append({
                        "time": full_time,
                        "y0": y0,
                        "y1": y1
                    })

        if not time_entries:
            continue

        # Remove duplicates that can happen from text extraction
        deduped = []
        seen = set()
        for entry in time_entries:
            key = (entry["time"], round(entry["y0"], 1))
            if key not in seen:
                seen.add(key)
                deduped.append(entry)

        time_entries = deduped

        # For each time row, find if it's a reservation row (R) and collect names
        for idx, entry in enumerate(time_entries):
            row_top = entry["y0"] - 2

            if idx < len(time_entries) - 1:
                row_bottom = time_entries[idx + 1]["y0"] - 2
            else:
                row_bottom = page.rect.height - 1

            # Skip the notes/footer section
            if entry["time"] == "06:30 PM":
                continue

            # Find reservation marker in the second column
            status_words = [
                w for w in words
                if 100 <= w[0] <= 170 and row_top <= w[1] < row_bottom
            ]

            status_texts = [w[4] for w in status_words]

            # Only keep reservation rows, not empty slots
            if "R" not in status_texts:
                continue

            # Find names in the Name column
            # Based on your PDF layout, names sit roughly in this x-range
            name_words = [
                w for w in words
                if 380 <= w[0] <= 760 and row_top <= w[1] < row_bottom
            ]

            if not name_words:
                continue

            # Group words by line (same player line)
            lines = {}
            for w in name_words:
                line_key = round(w[1], 1)
                lines.setdefault(line_key, []).append(w)

            player_lines = []
            for _, line_words in sorted(lines.items(), key=lambda item: item[0]):
                line_words = sorted(line_words, key=lambda w: w[0])
                line_text = " ".join(w[4] for w in line_words).strip()

                # Ignore notes and junk lines
                if not line_text:
                    continue
                if "Bag Storage" in line_text:
                    continue
                if line_text == "--":
                    continue
                if line_text.startswith("Staff Note"):
                    continue
                if line_text.startswith("Member Note"):
                    continue
                if line_text in {"R", "E"}:
                    continue

                # A player line should look like "Last, First ..."
                if "," in line_text:
                    player_lines.append(clean_player_name(line_text))

            if not player_lines:
                continue

            last_names = [extract_last_name(name) for name in player_lines]
            first_last_name = last_names[0]

            row = {
                "reservation_time": entry["time"],
                "group_name": f"{first_last_name} Group",
                "players": ", ".join(last_names),
                "num_players": len(last_names),
                "walkers": "",
                "riders": "",
                "group_type": "",
                "rotation": "",
                "total_time": "",
                "average_hole": ""
            }

            rows.append(row)

    doc.close()

    # Sort by reservation time string order already extracted from PDF flow
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
