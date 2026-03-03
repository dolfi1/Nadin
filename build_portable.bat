@echo off
setlocal

set "ROOT=%~dp0"
set "APP_NAME=Nadin"
set "ENTRY="

if exist "%ROOT%web_app.py" set "ENTRY=%ROOT%web_app.py"
if "%ENTRY%"=="" if exist "%ROOT%src\web_app.py" set "ENTRY=%ROOT%src\web_app.py"
if "%ENTRY%"=="" if exist "%ROOT%app.py" set "ENTRY=%ROOT%app.py"
if "%ENTRY%"=="" if exist "%ROOT%src\app.py" set "ENTRY=%ROOT%src\app.py"

if "%ENTRY%"=="" (
  echo ERROR: entrypoint not found. Expected one of:
  echo   web_app.py, src\web_app.py, app.py, src\app.py
  pause
  exit /b 1
)

echo Using entrypoint: "%ENTRY%"

REM cleanup old
if exist "%ROOT%build" rmdir /s /q "%ROOT%build"
if exist "%ROOT%dist"  rmdir /s /q "%ROOT%dist"

REM build
py -3 -m PyInstaller ^
  --noconfirm ^
  --clean ^
  --onedir ^
  --name "%APP_NAME%" ^
  "%ENTRY%"

if errorlevel 1 (
  echo BUILD FAILED
  pause
  exit /b 1
)

REM create release
if exist "%ROOT%release" rmdir /s /q "%ROOT%release"
mkdir "%ROOT%release"

xcopy "%ROOT%dist\%APP_NAME%" "%ROOT%release\%APP_NAME%\" /E /I /H /Y

REM cleanup intermediate artifacts
if exist "%ROOT%build" rmdir /s /q "%ROOT%build"
if exist "%ROOT%dist"  rmdir /s /q "%ROOT%dist"

echo.
echo BUILD SUCCESS
echo Portable folder:
echo %ROOT%release\%APP_NAME%
pause
