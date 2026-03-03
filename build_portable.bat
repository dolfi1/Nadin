@echo off
setlocal

set "ROOT=%~dp0"
set "APP_NAME=Nadin"
set "ENTRY=web_app.py"

REM cleanup old
if exist "%ROOT%build" rmdir /s /q "%ROOT%build"
if exist "%ROOT%dist"  rmdir /s /q "%ROOT%dist"

REM build
py -3 -m PyInstaller ^
  --noconfirm ^
  --clean ^
  --onedir ^
  --name "%APP_NAME%" ^
  "%ROOT%%ENTRY%"

if errorlevel 1 (
  echo BUILD FAILED
  pause
  exit /b 1
)

REM create release
if exist "%ROOT%release" rmdir /s /q "%ROOT%release"
mkdir "%ROOT%release"

xcopy "%ROOT%dist\%APP_NAME%" "%ROOT%release\%APP_NAME%\" /E /I /H /Y

echo.
echo BUILD SUCCESS
echo Portable folder:
echo %ROOT%release\%APP_NAME%
pause
