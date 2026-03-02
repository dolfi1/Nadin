@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM =========================
REM  CONFIG (edit if needed)
REM =========================
set "APP_NAME=Nadin"
set "ENTRYPOINT=desktop_app.py"

REM Folders/files to include in portable package (if they exist)
set "EXTRA_DIRS=templates static"
set "EXTRA_FILES=cards.db dlya_anala.xlsx README_USER.txt"

set "DO_CLEANUP=0"
if /I "%~1"=="--cleanup" set "DO_CLEANUP=1"
if /I "%~1"=="cleanup" set "DO_CLEANUP=1"

REM =========================
REM  PATHS
REM =========================
set "ROOT=%~dp0"
cd /d "%ROOT%"

set "VENV_DIR=%ROOT%.venv_build"
set "BUILD_DIR=%ROOT%build"
set "DIST_DIR=%ROOT%dist"
set "RELEASE_DIR=%ROOT%release"
set "OUT_DIR=%DIST_DIR%\%APP_NAME%"
set "RELEASE_APP_DIR=%RELEASE_DIR%\%APP_NAME%"

set "PYTHONHOME="
set "PYTHONPATH="

echo.
echo === Building %APP_NAME% (onedir) from %ENTRYPOINT% ===
echo Root: %ROOT%
echo.

REM =========================
REM  1) Create venv
REM =========================
if not exist "%VENV_DIR%\Scripts\python.exe" (
  echo [1/8] Creating venv...
  py -3 -m venv "%VENV_DIR%" 2>nul
  if errorlevel 1 (
    python -m venv "%VENV_DIR%"
    if errorlevel 1 (
      echo ERROR: Cannot create venv. Install Python 3.x on build machine.
      exit /b 1
    )
  )
) else (
  echo [1/8] Venv already exists.
)

set "PY=%VENV_DIR%\Scripts\python.exe"
set "PIP=%VENV_DIR%\Scripts\pip.exe"

REM =========================
REM  2) Install deps
REM =========================
echo [2/8] Installing dependencies...
"%PY%" -m pip install --upgrade pip setuptools wheel || exit /b 1
if exist "%ROOT%requirements.txt" (
  "%PIP%" install -r "%ROOT%requirements.txt" || exit /b 1
) else (
  echo WARN: requirements.txt not found, skipping.
)

"%PIP%" install pyinstaller || exit /b 1

REM =========================
REM  3) Clean old build artifacts
REM =========================
echo [3/8] Cleaning old build/dist/release and caches...
if exist "%BUILD_DIR%" rmdir /s /q "%BUILD_DIR%"
if exist "%DIST_DIR%" rmdir /s /q "%DIST_DIR%"
if exist "%RELEASE_DIR%" rmdir /s /q "%RELEASE_DIR%"
for /d /r "%ROOT%" %%D in (__pycache__) do @if exist "%%D" rmdir /s /q "%%D"
mkdir "%RELEASE_DIR%" >nul 2>&1

REM =========================
REM  4) Build onedir (windowed)
REM =========================
echo [4/8] Running PyInstaller...
"%PY%" -m PyInstaller ^
  --noconfirm ^
  --onedir ^
  --windowed ^
  --name "%APP_NAME%" ^
  --clean ^
  --exclude-module tests ^
  --exclude-module test ^
  --exclude-module pytest ^
  --exclude-module tkinter ^
  "%ROOT%%ENTRYPOINT%" || exit /b 1

REM =========================
REM  5) Copy required user resources into dist folder
REM =========================
echo [5/8] Copying user resources...
if not exist "%OUT_DIR%" (
  echo ERROR: Output folder not found: %OUT_DIR%
  exit /b 1
)

for %%D in (%EXTRA_DIRS%) do (
  if exist "%ROOT%%%D" (
    echo   + dir: %%D
    xcopy "%ROOT%%%D" "%OUT_DIR%\%%D\" /E /I /Y >nul
  )
)

for %%F in (%EXTRA_FILES%) do (
  if exist "%ROOT%%%F" (
    echo   + file: %%F
    copy /Y "%ROOT%%%F" "%OUT_DIR%\%%F" >nul
  )
)

REM =========================
REM  6) Prepare release app folder
REM =========================
echo [6/8] Copying final app folder to release...
if exist "%RELEASE_APP_DIR%" rmdir /s /q "%RELEASE_APP_DIR%"
xcopy "%OUT_DIR%" "%RELEASE_APP_DIR%\" /E /I /Y >nul || exit /b 1

REM =========================
REM  7) Zip only release\Nadin for users
REM =========================
echo [7/8] Creating portable zip...
set "ZIP_PATH=%RELEASE_DIR%\%APP_NAME%_Portable.zip"

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "if(Test-Path '%ZIP_PATH%'){Remove-Item -Force '%ZIP_PATH%'}; Compress-Archive -Path '%RELEASE_APP_DIR%\*' -DestinationPath '%ZIP_PATH%'" || exit /b 1

REM =========================
REM  8) Optional cleanup
REM =========================
if "%DO_CLEANUP%"=="1" (
  echo [8/8] Cleanup enabled. Removing build caches...
  if exist "%BUILD_DIR%" rmdir /s /q "%BUILD_DIR%"
  for /d /r "%ROOT%" %%D in (__pycache__) do @if exist "%%D" rmdir /s /q "%%D"
  if exist "%VENV_DIR%" rmdir /s /q "%VENV_DIR%"
) else (
  echo [8/8] Cleanup skipped. Use build_portable.bat --cleanup to remove caches.
)

echo.
echo DONE ✅
echo Portable folder: %RELEASE_APP_DIR%
echo Zip for users:   %ZIP_PATH%
echo.
echo Give users the ZIP. They unzip and start: %APP_NAME%.exe
echo.

endlocal
exit /b 0
