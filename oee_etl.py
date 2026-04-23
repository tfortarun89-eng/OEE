import os
import json
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--folder", default="data")
    parser.add_argument("--output", default="output/oee_data.json")
    args = parser.parse_args()

    # ✅ absolute base path (Render safe)
    base_dir = Path(__file__).parent

    folder = base_dir / args.folder
    output_path = base_dir / args.output

    print("📁 Reading folder:", folder)

    if not folder.exists():
        print("❌ Data folder not found:", folder)
        return

    excel_files = list(folder.glob("*.xlsx"))

    if not excel_files:
        print("❌ No Excel files found")
        return

    all_records = []

    for file in excel_files:
        print("📄 Processing:", file.name)

        try:
            df = pd.read_excel(file)

            # 👉 adjust columns according to your Excel
            for _, row in df.iterrows():
                all_records.append({
                    "date": str(row.get("Date", "")),
                    "shift": str(row.get("Shift", "")),
                    "machine_no": int(row.get("Machine", 0)),
                    "overall_oee": float(row.get("OEE", 0)),
                    "total_actual": float(row.get("Actual", 0)),
                    "total_rej": float(row.get("Reject", 0))
                })

        except Exception as e:
            print("❌ Error reading file:", file.name, e)

    if not all_records:
        print("❌ No data processed")
        return

    # ✅ ensure output folder exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump({
            "records": all_records
        }, f, indent=2)

    print("✅ JSON created at:", output_path)


if __name__ == "__main__":
    main()