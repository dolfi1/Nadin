@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM =========================
REM CONFIG
REM =========================
set "APP_NAME=Nadin"
set "ENTRYPOINT=src\main.py"

REM =========================
REM PATHS
REM =========================
set "ROOT=%~dp0.."
for %%I in ("%ROOT%") do set "ROOT=%%~fI"

set "PORTABLE_DIR=%ROOT%\portable"
set "LOG_DIR=%PORTABLE_DIR%\logs"
set "SRC_DIR=%PORTABLE_DIR%\sources"
set "OUT_DIR=%PORTABLE_DIR%\out"

set "DIST_DIR=%OUT_DIR%\dist"
set "WORK_DIR=%OUT_DIR%\build"
set "SPEC_DIR=%OUT_DIR%\spec"

set "FINAL_DIR=%OUT_DIR%\Nadin_Portable"
set "LOG_FILE=%LOG_DIR%\pyinstaller.log"
set "EXTRA_ARGS="
set "PI_ATTEMPT=1"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%OUT_DIR%" mkdir "%OUT_DIR%"

call :resolve_entrypoint
if errorlevel 1 exit /b 11

echo ======================================
echo START BUILD %date% %time%
echo Root      : "%ROOT%"
echo App name  : "%APP_NAME%"
echo Entrypoint: "%ENTRYPOINT%"
echo ======================================

REM =========================
REM STEP 1: Copy all .py into portable\sources
REM =========================
echo [INFO] Syncing sources to "%SRC_DIR%"...

if exist "%SRC_DIR%" rmdir /S /Q "%SRC_DIR%"
mkdir "%SRC_DIR%"

pushd "%ROOT%" || (echo [ERROR] Cannot cd to ROOT & exit /b 10)

robocopy "%ROOT%" "%SRC_DIR%" /E ^
  /XD ".git" "portable" "dist" "build" "__pycache__" ".venv" "venv" ^
  /XF *.* >nul
set "RC=!ERRORLEVEL!"
if !RC! geq 8 (
  echo [ERROR] ROBOCOPY failed while mirroring tree. ErrorLevel=!RC!
  popd
  exit /b 20
)

robocopy "%ROOT%" "%SRC_DIR%" *.py /S ^
  /XD ".git" "portable" "dist" "build" "__pycache__" ".venv" "venv" ^
  >nul
set "RC=!ERRORLEVEL!"
if !RC! geq 8 (
  echo [ERROR] ROBOCOPY failed while copying sources. ErrorLevel=!RC!
  popd
  exit /b 21
)

REM =========================
REM STEP 2: Run PyInstaller
REM =========================
:run_pyinstaller

echo [INFO] Running PyInstaller (attempt !PI_ATTEMPT!)...
echo [INFO] Log: "%LOG_FILE%"

if exist "%DIST_DIR%" rmdir /S /Q "%DIST_DIR%"
if exist "%WORK_DIR%" rmdir /S /Q "%WORK_DIR%"
if exist "%SPEC_DIR%" rmdir /S /Q "%SPEC_DIR%"

py -m PyInstaller "%ENTRYPOINT%" ^
  --name "%APP_NAME%" ^
  --noconfirm ^
  --clean ^
  --distpath "%DIST_DIR%" ^
  --workpath "%WORK_DIR%" ^
  --specpath "%SPEC_DIR%" ^
  !EXTRA_ARGS! ^
  > "%LOG_FILE%" 2>&1

set "PI_RC=%ERRORLEVEL%"

if not "%PI_RC%"=="0" (
  echo [ERROR] PyInstaller failed with code %PI_RC%
  echo See log: "%LOG_FILE%"
  call :print_log_snippets

  if "%PI_ATTEMPT%"=="1" (
    call :autofix_from_log
    if defined RETRY_BUILD (
      set "PI_ATTEMPT=2"
      set "RETRY_BUILD="
      goto :run_pyinstaller
    )
  )

  popd
  exit /b %PI_RC%
)

REM =========================
REM STEP 3: Pack portable folder (only if build OK)
REM =========================
echo [INFO] Packing portable to "%FINAL_DIR%"...

if exist "%FINAL_DIR%" rmdir /S /Q "%FINAL_DIR%"
mkdir "%FINAL_DIR%"

robocopy "%DIST_DIR%\%APP_NAME%" "%FINAL_DIR%\app" /E >nul
set "RC=!ERRORLEVEL!"
if !RC! geq 8 (
  echo [ERROR] ROBOCOPY failed while copying dist to FINAL. ErrorLevel=!RC!
  popd
  exit /b 30
)

robocopy "%SRC_DIR%" "%FINAL_DIR%\sources" /E >nul
copy /Y "%PORTABLE_DIR%\build_portable.bat" "%FINAL_DIR%\build_portable.bat" >nul

popd

echo ======================================
echo [OK] DONE
echo Portable: "%FINAL_DIR%"
echo Log     : "%LOG_FILE%"
echo ======================================
exit /b 0

:resolve_entrypoint
if exist "%ROOT%\%ENTRYPOINT%" exit /b 0
for %%E in ("src\main.py" "src\desktop_app.py" "main.py") do (
  if exist "%ROOT%\%%~E" (
    set "ENTRYPOINT=%%~E"
    echo [WARN] Configured entrypoint not found, switched to "%ENTRYPOINT%"
    exit /b 0
  )
)
echo [ERROR] Entrypoint not found. Checked "%ENTRYPOINT%", "src\main.py", "src\desktop_app.py", "main.py"
exit /b 1

:print_log_snippets
if not exist "%LOG_FILE%" (
  echo [ERROR] Log file not found: "%LOG_FILE%"
  exit /b 0
)

echo ---------- LOG (first 30 lines) ----------
powershell -NoProfile -Command "Get-Content -Path '%LOG_FILE%' -TotalCount 30"
echo ---------- LOG (last 80 lines) -----------
powershell -NoProfile -Command "Get-Content -Path '%LOG_FILE%' -Tail 80"
echo ------------------------------------------
exit /b 0

:autofix_from_log
set "RETRY_BUILD="

findstr /C:"No module named PyInstaller" "%LOG_FILE%" >nul && (
  echo [ERROR] PyInstaller is not installed for launcher "py". Install it and rerun.
  exit /b 0
)

for /f "usebackq delims=" %%M in (`powershell -NoProfile -Command "$mods = Select-String -Path '%LOG_FILE%' -Pattern \"ModuleNotFoundError: No module named '([^']+)'\" -AllMatches | ForEach-Object { $_.Matches } | ForEach-Object { $_.Groups[1].Value } | Select-Object -Unique; $mods -join ' '"`) do set "MISSING_MODULES=%%M"
if defined MISSING_MODULES (
  for %%M in (!MISSING_MODULES!) do (
    echo [INFO] Auto-fix: adding hidden import %%M
    set "EXTRA_ARGS=!EXTRA_ARGS! --hidden-import %%M"
  )
  set "RETRY_BUILD=1"
)

findstr /I /C:"template" /C:"static" /C:"No such file or directory" "%LOG_FILE%" >nul && (
  if exist "%ROOT%\templates" (
    echo [INFO] Auto-fix: adding templates data
    set "EXTRA_ARGS=!EXTRA_ARGS! --add-data \"templates;templates\""
    set "RETRY_BUILD=1"
  )
  if exist "%ROOT%\static" (
    echo [INFO] Auto-fix: adding static data
    set "EXTRA_ARGS=!EXTRA_ARGS! --add-data \"static;static\""
    set "RETRY_BUILD=1"
  )
)

findstr /I /C:"script file" /C:"does not exist" "%LOG_FILE%" >nul && (
  call :resolve_entrypoint
  if not errorlevel 1 set "RETRY_BUILD=1"
)

if defined RETRY_BUILD (
  echo [INFO] Retrying PyInstaller with extra args: !EXTRA_ARGS!
)

exit /b 0
