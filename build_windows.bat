@echo off
setlocal EnableExtensions EnableDelayedExpansion

py -m pip install -r requirements.txt
if errorlevel 1 exit /b 1
py -m pip install pyinstaller
if errorlevel 1 exit /b 1

set "ADD_DATA="
if exist templates (
  set "ADD_DATA=!ADD_DATA! --add-data templates;templates"
)
if exist static (
  set "ADD_DATA=!ADD_DATA! --add-data static;static"
)
if exist dlya_anala.xlsx (
  set "ADD_DATA=!ADD_DATA! --add-data dlya_anala.xlsx;."
)
if exist cards.db (
  set "ADD_DATA=!ADD_DATA! --add-data cards.db;."
)

py -m PyInstaller --noconfirm --clean --name Nadin --windowed --onedir desktop_app.py !ADD_DATA!
if errorlevel 1 exit /b 1

echo Build complete: dist\Nadin\Nadin.exe
endlocal
