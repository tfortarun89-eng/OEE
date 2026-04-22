@echo off
cd /d %~dp0

echo Running ETL...
py oee_etl.py --folder data --output output/oee_data.json

echo Starting server...
start http://localhost:8000/oee_dashboard.html

start /B py -m http.server 8000
exit