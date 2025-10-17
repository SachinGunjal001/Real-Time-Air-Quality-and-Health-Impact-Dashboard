@echo off
set log_file=E:\Github\Real-Time-Air-Quality-and-Health-Impact-Dashboard\AirQualityData\RunLog.txt

echo ---------------------------------------------------- >> %log_file%
echo [%date% %time%] Starting Python ETL Job >> %log_file%

cd "E:\Github\Real-Time-Air-Quality-and-Health-Impact-Dashboard\AirQualityData"
"C:\Users\Sachin\AppData\Local\Programs\Python\Python313\python.exe" data.py >> %log_file% 2>&1

if %ERRORLEVEL%==0 (
    echo [%date% %time%] ✅ Python Script executed successfully >> %log_file%
) else (
    echo [%date% %time%] ❌ Python Script failed with Error Code %ERRORLEVEL% >> %log_file%
)

echo [%date% %time%] Python Job Completed. >> %log_file%
echo ---------------------------------------------------- >> %log_file%
exit /b 0
