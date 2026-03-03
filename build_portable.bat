@echo off
setlocal EnableExtensions EnableDelayedExpansion

call :main
exit /b %errorlevel%

:main
set "TRACE=1"
if "%TRACE%"=="1" echo on

set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"

echo.
echo ======================================
echo START BUILD %date% %time%
echo Script: "%~f0"
echo Root  : "%~dp0"
echo ======================================

set "LOG=%ROOT%\release\build_log.txt"

REM --- self-check: forbid dot-only lines like ".." / "..."
for /f "delims=" %%A in ('findstr /n /r "^[.][.][.]*$" "%~f0"') do (
  call :die "Invalid line (dot-only) found in bat: %%A"
)

if not exist "%ROOT%\release" mkdir "%ROOT%\release"
if errorlevel 1 call :die "failed to create release directory"
>"%LOG%" echo ==== BUILD START %date% %time% ====
call :log LOG FILE: %LOG%
call :log BAT FILE: %~f0
call :log START BUILD %date% %time%
call :log Script: %~f0
call :log Root: %~dp0

REM ================== CONFIG ==================
set "APP_NAME=Nadin"
set "ENTRY=web_app.py"

REM release layout
set "RELEASE_BASE=release"
set "PORTABLE_DIR_NAME=NadinPortable"
set "APP_SUBDIR=app"

REM cleanup toggles
set "CLEAN_BUILD_DIST=1"
set "CLEAN_VENV_BUILD=1"
set "CLEAN_PY_IN_PORTABLE=1"

REM ================== ROOT ==================

set "MARKER=%ROOT%\.nadin_root_marker"
set "VENV=%ROOT%\.venv_build"

set "RELEASE_ROOT=%ROOT%\%RELEASE_BASE%\%PORTABLE_DIR_NAME%"
set "RELEASE_APP=%RELEASE_ROOT%\%APP_SUBDIR%"

echo.
echo === Nadin portable build ===
echo ROOT: "%ROOT%"
echo RELEASE_ROOT: "%RELEASE_ROOT%"
echo Reached checkpoint: after ROOT/RELEASE vars
call :log ROOT: %ROOT%
call :log RELEASE_ROOT: %RELEASE_ROOT%

if not exist "%ROOT%\%ENTRY%" (
  echo ERROR: entrypoint not found: "%ROOT%\%ENTRY%"
  call :die "entrypoint not found: %ROOT%\%ENTRY%"
)

REM ===== SAFETY =====
if /I "%ROOT%"=="C:" (
  echo ERROR: ROOT points to C:\ (drive root). Abort.
  call :die "ROOT points to C:\\ (drive root)"
)

if not exist "%MARKER%" (
  echo Creating marker "%MARKER%"
  >"%MARKER%" echo nadin-root
)

if not exist "%ROOT%\build_portable.bat" (
  echo ERROR: build_portable.bat missing in ROOT. Abort.
  call :die "build_portable.bat missing in ROOT"
)

if not exist "%ROOT%\%RELEASE_BASE%" mkdir "%ROOT%\%RELEASE_BASE%"
if errorlevel 1 call :die "failed to create release base directory"
>>"%LOG%" echo [START] %date% %time%
call :log ROOT=%ROOT%
call :log RELEASE_ROOT=%RELEASE_ROOT%

REM ================== PYTHON DETECT ==================
set "PY_CMD="
call :pick_python
if "%PY_CMD%"=="" (
  echo ERROR: Python not found (py launcher / python). Install Python 3.x and retry.
  call :log ERROR: Python not found
  call :die "Python not found"
)

echo Using Python: %PY_CMD%
call :log Using Python: %PY_CMD%

REM ================== CLEAN build/dist ==================
if "%CLEAN_BUILD_DIST%"=="1" (
  echo Cleaning old build/dist...
  if exist "%ROOT%\build" rmdir /s /q "%ROOT%\build"
  if exist "%ROOT%\dist"  rmdir /s /q "%ROOT%\dist"
)

REM ================== VENV ==================
if not exist "%VENV%\Scripts\python.exe" (
  echo Creating build venv...
  %PY_CMD% -m venv "%VENV%"
  if errorlevel 1 (
    echo ERROR: failed to create venv
    call :log ERROR: venv create failed
    call :die "venv creation failed"
  )
)

echo Installing deps...
call "%VENV%\Scripts\python.exe" -m pip install --upgrade pip >>"%LOG%" 2>&1
if errorlevel 1 (
  echo ERROR: pip upgrade failed (see log)
  call :log ERROR: pip upgrade failed
  call :die "pip upgrade failed"
)

call "%VENV%\Scripts\python.exe" -m pip install -r "%ROOT%\requirements.txt" >>"%LOG%" 2>&1
if errorlevel 1 (
  echo ERROR: requirements install failed (see log)
  call :log ERROR: requirements install failed
  call :die "requirements installation failed"
)

call "%VENV%\Scripts\python.exe" -m pip install pyinstaller >>"%LOG%" 2>&1
if errorlevel 1 (
  echo ERROR: pyinstaller install failed (see log)
  call :log ERROR: pyinstaller install failed
  call :die "pyinstaller installation failed"
)

REM ================== BUILD ==================
echo Running PyInstaller...
pushd "%ROOT%"
if errorlevel 1 call :die "failed to enter ROOT directory"
call "%VENV%\Scripts\python.exe" -m PyInstaller ^
  --noconfirm ^
  --clean ^
  --onedir ^
  --name "%APP_NAME%" ^
  "%ENTRY%" >>"%LOG%" 2>&1
set "RC=%errorlevel%"
popd
if errorlevel 1 call :die "failed to leave ROOT directory"

if not "%RC%"=="0" (
  echo ERROR: PyInstaller failed (code %RC%). See "%LOG%".
  call :log ERROR: PyInstaller failed code=%RC%
  call :die "PyInstaller failed with code %RC%"
)

if not exist "%ROOT%\dist\%APP_NAME%\%APP_NAME%.exe" (
  echo ERROR: EXE not found: "%ROOT%\dist\%APP_NAME%\%APP_NAME%.exe"
  call :log ERROR: dist exe missing
  call :die "built EXE not found in dist"
)

REM ================== PREPARE PORTABLE ==================
echo Preparing portable folder...
if exist "%RELEASE_ROOT%" rmdir /s /q "%RELEASE_ROOT%"
if errorlevel 1 call :die "failed to remove previous release root"
mkdir "%RELEASE_ROOT%"
if errorlevel 1 call :die "failed to create release root"
mkdir "%RELEASE_APP%"
if errorlevel 1 call :die "failed to create release app directory"

xcopy "%ROOT%\dist\%APP_NAME%\*" "%RELEASE_APP%\" /E /I /H /Y >nul
if errorlevel 1 (
  echo ERROR: failed to copy dist -> portable
  call :log ERROR: xcopy dist->portable failed
  call :die "copy dist to portable failed"
)

if not exist "%RELEASE_APP%\%APP_NAME%.exe" (
  echo ERROR: EXE not found in portable: "%RELEASE_APP%\%APP_NAME%.exe"
  call :log ERROR: portable exe missing
  call :die "EXE not found in portable"
)

REM стартовый бат для пользователя
>"%RELEASE_ROOT%\Start %APP_NAME%.bat" echo @echo off
>>"%RELEASE_ROOT%\Start %APP_NAME%.bat" echo setlocal
>>"%RELEASE_ROOT%\Start %APP_NAME%.bat" echo cd /d "%%~dp0%APP_SUBDIR%"
>>"%RELEASE_ROOT%\Start %APP_NAME%.bat" echo start "" "%APP_NAME%.exe"
>>"%RELEASE_ROOT%\Start %APP_NAME%.bat" echo exit /b 0

REM ================== CLEAN .py INSIDE PORTABLE ONLY ==================
if "%CLEAN_PY_IN_PORTABLE%"=="1" (
  echo Cleaning *.py in portable (safe)...
  for /r "%RELEASE_ROOT%" %%F in (*.py *.pyc *.pyo *.spec) do del /f /q "%%F" >nul 2>&1
  for /d /r "%RELEASE_ROOT%" %%D in (__pycache__) do rmdir /s /q "%%D" >nul 2>&1
)

REM ================== OPTIONAL ZIP ==================
echo Creating zip...
powershell -NoProfile -Command ^
  "Compress-Archive -Path '%RELEASE_ROOT%\*' -DestinationPath '%ROOT%\%RELEASE_BASE%\%APP_NAME%_Portable.zip' -Force" >nul 2>&1
if errorlevel 1 call :die "zip creation failed"

REM ================== FINAL CLEANUP ==================
if "%CLEAN_BUILD_DIST%"=="1" (
  if exist "%ROOT%\build" rmdir /s /q "%ROOT%\build"
  if errorlevel 1 call :die "failed to remove build directory"
  if exist "%ROOT%\dist"  rmdir /s /q "%ROOT%\dist"
  if errorlevel 1 call :die "failed to remove dist directory"
)

if "%CLEAN_VENV_BUILD%"=="1" (
  if exist "%VENV%" rmdir /s /q "%VENV%"
  if errorlevel 1 call :die "failed to remove build venv"
)

call :log DONE
echo.
echo DONE.
echo Portable folder: "%RELEASE_ROOT%"
echo Zip: "%ROOT%\%RELEASE_BASE%\%APP_NAME%_Portable.zip"
echo Log: "%LOG%"
call :success
exit /b 0

REM ================== FUNCTIONS ==================
:pick_python
REM prefer py launcher if present
where py >nul 2>&1
if not errorlevel 1 (
  REM try explicit 3.12 first
  py -3.12 -c "import sys;print(sys.version)" >nul 2>&1
  if not errorlevel 1 ( set "PY_CMD=py -3.12" & exit /b 0 )

  REM then any 3.x
  py -3 -c "import sys;print(sys.version)" >nul 2>&1
  if not errorlevel 1 ( set "PY_CMD=py -3" & exit /b 0 )
)

REM fallback: python in PATH
where python >nul 2>&1
if not errorlevel 1 (
  python -c "import sys;print(sys.version)" >nul 2>&1
  if not errorlevel 1 ( set "PY_CMD=python" & exit /b 0 )
)

set "PY_CMD="
exit /b 0

:log
if "%LOG%"=="" exit /b 0
>>"%LOG%" echo [%date% %time%] %*
exit /b 0

:die
set "ERR=%errorlevel%"
call :log BUILD FAILED: %* (errorlevel=%ERR%)
echo.
echo BUILD FAILED: %*
echo errorlevel=%ERR%
if exist "%LOG%" (
  echo ---- Last log lines ----
  powershell -NoProfile -Command "Get-Content '%LOG%' -Tail 50"
)
pause
exit /b 1

:success
call :log BUILD SUCCESS
echo.
echo ======================================
echo BUILD SUCCESS
echo ======================================
pause
exit /b 0
