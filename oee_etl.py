import json
from pathlib import Path
import pandas as pd

def main():

    base_dir = Path(__file__).parent

    data_folder = base_dir / "data"
    output_file = base_dir / "output" / "oee_data.json"

    print("📁 Data folder:", data_folder)

    if not data_folder.exists():
        print("❌ data folder not found")
        return

    excel_files = list(data_folder.glob("*.xlsx"))

    print("📄 Files found:", excel_files)

    if not excel_files:
        print("❌ No Excel files found")
        return

    records = []

    for file in excel_files:
        print("➡ Processing:", file.name)

        try:
            # ✅ FIX: first sheet auto read
            df = pd.read_excel(file, sheet_name=0)

            print("Columns:", list(df.columns))

            for _, row in df.iterrows():

                # skip empty rows
                if row.isnull().all():
                    continue

                records.append({
                    "date": str(row.iloc[0]) if len(row) > 0 else "",
                    "shift": str(row.iloc[1]) if len(row) > 1 else "",
                    "machine_no": int(row.iloc[2]) if len(row) > 2 and pd.notna(row.iloc[2]) else 0,
                    "overall_oee": float(row.iloc[3]) if len(row) > 3 and pd.notna(row.iloc[3]) else 0,
                    "total_actual": float(row.iloc[4]) if len(row) > 4 and pd.notna(row.iloc[4]) else 0,
                    "total_rej": float(row.iloc[5]) if len(row) > 5 and pd.notna(row.iloc[5]) else 0
                })

        except Exception as e:
            print("❌ Error:", e)

    if not records:
        print("❌ No data extracted from Excel")
        return

    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w") as f:
        json.dump({"records": records}, f, indent=2)

    print("✅ JSON created:", output_file)


if __name__ == "__main__":
    main()