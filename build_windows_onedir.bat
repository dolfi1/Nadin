@echo off
setlocal EnableExtensions EnableDelayedExpansion

py -m pip install -r requirements.txt
if errorlevel 1 exit /b 1

py -m pip install pyinstaller
if errorlevel 1 exit /b 1

py -m PyInstaller --noconfirm --clean --name Nadin --windowed --onedir desktop_app.py ^
  --add-data "templates;templates" ^
  --add-data "static;static" ^
  --add-data "cards.db;." ^
  --add-data "dlya_anala.xlsx;."
if errorlevel 1 exit /b 1

if exist release.zip del /f /q release.zip
powershell -NoProfile -Command "Compress-Archive -Path 'dist\Nadin' -DestinationPath 'release.zip' -Force"
if errorlevel 1 exit /b 1

echo Build complete: dist\Nadin\Nadin.exe
echo Release archive: release.zip

endlocal
