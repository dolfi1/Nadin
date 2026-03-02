@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM ====== CONFIG ======
set "APP_NAME=Nadin"
set "ENTRY=web_app.py"
set "SPEC_FILE=%APP_NAME%.spec"

REM ====== ROOT = folder where this bat is located ======
set "ROOT=%~dp0"
REM remove trailing backslash (optional)
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"

echo.
echo === Nadin portable build ===
echo ROOT: "%ROOT%"

REM ====== SAFETY GUARDS ======
if not exist "%ROOT%\%ENTRY%" (
  echo ERROR: entrypoint not found: "%ROOT%\%ENTRY%"
  exit /b 1
)

REM marker guard - creates a marker so we can verify we're in correct folder
set "MARKER=%ROOT%\.nadin_root_marker"
if not exist "%MARKER%" (
  echo Creating marker: "%MARKER%"
  echo nadin-root>"%MARKER%"
)

REM refuse to run if ROOT is suspiciously short (extra guard)
if "%ROOT%"=="" (
  echo ERROR: ROOT is empty. Abort.
  exit /b 1
)

REM ====== CLEAN ONLY BUILD ARTIFACTS (SAFE) ======
echo.
echo Cleaning old build artifacts...
if exist "%ROOT%\build" rmdir /s /q "%ROOT%\build"
if exist "%ROOT%\dist"  rmdir /s /q "%ROOT%\dist"

REM ====== BUILD VENV ======
set "VENV=%ROOT%\.venv_build"
if not exist "%VENV%\Scripts\python.exe" (
  echo.
  echo Creating build venv...
  py -3.9 -m venv "%VENV%"
  if errorlevel 1 (
    echo ERROR: failed to create venv
    exit /b 1
  )
)

echo.
echo Installing deps...
call "%VENV%\Scripts\python.exe" -m pip install --upgrade pip
if errorlevel 1 exit /b 1

call "%VENV%\Scripts\python.exe" -m pip install -r "%ROOT%\requirements.txt"
if errorlevel 1 exit /b 1

call "%VENV%\Scripts\python.exe" -m pip install pyinstaller
if errorlevel 1 exit /b 1

REM ====== BUILD ======
echo.
echo Running PyInstaller...
pushd "%ROOT%"

REM onedir build
call "%VENV%\Scripts\python.exe" -m PyInstaller ^
  --noconfirm ^
  --clean ^
  --onedir ^
  --name "%APP_NAME%" ^
  "%ENTRY%"

set "RC=%errorlevel%"
popd

if not "%RC%"=="0" (
  echo ERROR: PyInstaller failed with code %RC%
  echo Keeping dist/build for диагностики.
  exit /b %RC%
)

REM ====== VERIFY EXE EXISTS ======
if not exist "%ROOT%\dist\%APP_NAME%\%APP_NAME%.exe" (
  echo ERROR: EXE not found: "%ROOT%\dist\%APP_NAME%\%APP_NAME%.exe"
  echo Build considered failed. Keeping artifacts.
  exit /b 1
)

REM ====== RELEASE FOLDER ======
set "RELEASE=%ROOT%\release\%APP_NAME%"
echo.
echo Preparing release folder: "%RELEASE%"
if exist "%RELEASE%" rmdir /s /q "%RELEASE%"
mkdir "%RELEASE%"

echo Copying dist -> release...
xcopy "%ROOT%\dist\%APP_NAME%\*" "%RELEASE%\" /E /I /H /Y >nul

REM ====== OPTIONAL: ZIP ======
echo.
echo Creating zip...
powershell -NoProfile -Command ^
  "Compress-Archive -Path '%RELEASE%\*' -DestinationPath '%ROOT%\release\%APP_NAME%_Portable.zip' -Force" >nul

REM ====== CLEANUP (ONLY SAFE DIRS) ======
echo.
echo Cleaning build artifacts (safe)...
if exist "%ROOT%\build" rmdir /s /q "%ROOT%\build"
if exist "%ROOT%\dist"  rmdir /s /q "%ROOT%\dist"

echo.
echo DONE: "%ROOT%\release\%APP_NAME%\%APP_NAME%.exe"
echo ZIP:  "%ROOT%\release\%APP_NAME%_Portable.zip"
echo.
pause
exit /b 0
