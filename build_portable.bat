@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"

set "SRC=%ROOT%\src"
set "OUTROOT=%ROOT%\portable"
set "OUTDIR=%OUTROOT%\Nadin_Portable"
set "LOGDIR=%OUTROOT%\logs"

echo ======================================
echo START BUILD %DATE% %TIME%
echo ======================================

if not exist "%SRC%\main.py" (
    echo [ERROR] src\main.py not found
    exit /b 1
)

if not exist "%OUTROOT%" mkdir "%OUTROOT%"
if not exist "%OUTDIR%" mkdir "%OUTDIR%"
if not exist "%LOGDIR%" mkdir "%LOGDIR%"

where python >nul 2>nul || (
    echo [ERROR] Python not found
    exit /b 1
)

echo [INFO] Running PyInstaller...
python -m PyInstaller --noconfirm --clean --onedir "%SRC%\main.py" ^
    --distpath "%OUTDIR%\dist" ^
    --workpath "%OUTROOT%\build" ^
    --specpath "%OUTROOT%" ^
    > "%LOGDIR%\pyinstaller.log" 2>&1

if errorlevel 1 (
    echo [ERROR] PyInstaller failed
    echo See log: "%LOGDIR%\pyinstaller.log"
    exit /b 1
)

echo.
echo DONE.
echo Portable created at:
echo %OUTDIR%
echo.
exit /b 0
