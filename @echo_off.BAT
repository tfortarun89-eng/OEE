@echo off

cd /d %~dp0

echo Running ETL...
py oee_etl.py --folder "data" --output "output/oee_data.json"

echo Uploading JSON...
py upload_json.py

echo Opening dashboard...
start https://oee-1-4n1e.onrender.com/

pause