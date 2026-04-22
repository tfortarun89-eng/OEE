from flask import Flask, send_from_directory, jsonify
import subprocess
import os

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

@app.route("/")
def home():
    return send_from_directory(BASE_DIR, "oee_dashboard.html")

@app.route("/output/oee_data.json")
def json_data():
    return send_from_directory(os.path.join(BASE_DIR, "output"), "oee_data.json")

@app.route("/run-etl")
def run_etl():
    try:
        subprocess.run(["python", "oee_etl.py"], check=True)
        return jsonify({"status": "ETL executed successfully"})
    except Exception as e:
        return jsonify({"error": str(e)})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
