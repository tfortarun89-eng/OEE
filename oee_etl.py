import json
from pathlib import Path
import pandas as pd

def main():

    base_dir = Path(__file__).parent

    data_folder = base_dir / "data"
    output_file = base_dir / "output" / "oee_data.json"

    print("📁 Data folder:", data_folder)

    excel_files = list(data_folder.glob("*.xlsx"))

    print("📄 Files found:", excel_files)

    records = []

    for file in excel_files:
        print("➡ Processing:", file.name)

        try:
            df = pd.read_excel(file, sheet_name=0)

            print("Columns:", list(df.columns))

            for _, row in df.iterrows():

                if row.isnull().all():
                    continue

                records.append({
                    "machine_no": int(row.iloc[0]) if len(row)>0 and pd.notna(row.iloc[0]) else 0,
                    "overall_oee": float(row.iloc[1]) if len(row)>1 and pd.notna(row.iloc[1]) else 0,
                    "total_actual": float(row.iloc[2]) if len(row)>2 and pd.notna(row.iloc[2]) else 0
                })

        except Exception as e:
            print("❌ Error:", e)

    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w") as f:
        json.dump({"records": records}, f, indent=2)

    print("✅ JSON created:", output_file)


if __name__ == "__main__":
    main()