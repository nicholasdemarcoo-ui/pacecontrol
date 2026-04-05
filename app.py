import os
from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = "uploads"

# Temporary sample data for the table
tee_sheet_rows = [
    {
        "reservation_time": "8:00 AM",
        "group_name": "Smith Group",
        "players": "Smith, Jones, Brown, Kelly",
        "num_players": 4,
        "walkers": "",
        "riders": "",
        "group_type": "",
        "rotation": "",
        "total_time": "",
        "average_hole": ""
    }
]

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/upload", methods=["POST"])
def upload_pdf():
    pdf_file = request.files.get("tee_sheet_pdf")

    if not pdf_file or pdf_file.filename == "":
        return redirect(url_for("home"))

    os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)
    save_path = os.path.join(app.config["UPLOAD_FOLDER"], pdf_file.filename)
    pdf_file.save(save_path)

    # For now, we are only saving the file.
    # Later we will parse the PDF and replace tee_sheet_rows with real data.
    return redirect(url_for("tee_sheet"))

@app.route("/tee-sheet")
def tee_sheet():
    return render_template("tee_sheet.html", rows=tee_sheet_rows)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
