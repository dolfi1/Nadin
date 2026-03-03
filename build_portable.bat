@echo off
setlocal

set "ROOT=%~dp0"
set "APP_NAME=Nadin"
set "ENTRY=%ROOT%src\web_app.py"
set "FINAL_DIR=%ROOT%release\Nadin"
set "MODE=release"
REM если хочешь дебаг — меняй на debug
REM set "MODE=debug"

set "LOG=%ROOT%release\build_portable.log"
set "PYTHON_EXE=py -3"
set "VENV=%ROOT%.venv"
if exist "%VENV%\Scripts\python.exe" set "PYTHON_EXE=\"%VENV%\Scripts\python.exe\""

if not exist "%ENTRY%" call :die "entrypoint not found: %ENTRY%"

set "CONSOLE_FLAG=--noconsole"
if /I "%MODE%"=="debug" set "CONSOLE_FLAG=--console"

echo Using entrypoint: "%ENTRY%"
echo Build mode: %MODE%
echo Log file: "%LOG%"

if exist "%ROOT%release" rmdir /s /q "%ROOT%release"
mkdir "%ROOT%release"

>"%LOG%" echo [build_portable] start %date% %time%
>>"%LOG%" echo ROOT=%ROOT%
>>"%LOG%" echo ENTRY=%ENTRY%
>>"%LOG%" echo MODE=%MODE%
>>"%LOG%" echo CONSOLE_FLAG=%CONSOLE_FLAG%

REM cleanup old
if exist "%ROOT%build" rmdir /s /q "%ROOT%build"
if exist "%ROOT%dist"  rmdir /s /q "%ROOT%dist"

REM build
echo Running PyInstaller... (see log: %LOG%)
call %PYTHON_EXE% -m PyInstaller ^
  --noconfirm ^
  --clean ^
  --onedir ^
  %CONSOLE_FLAG% ^
  --collect-submodules=uvicorn ^
  --collect-submodules=fastapi ^
  --collect-submodules=starlette ^
  --collect-submodules=jinja2 ^
  --collect-submodules=charset_normalizer ^
  --name "%APP_NAME%" ^
  "%ENTRY%" >> "%LOG%" 2>&1

if errorlevel 1 call :die "PyInstaller failed. See %LOG%"

REM create release
echo Copying build output to %FINAL_DIR%...
xcopy "%ROOT%dist\%APP_NAME%\*" "%FINAL_DIR%\" /E /I /H /Y >>"%LOG%" 2>&1
if errorlevel 1 call :die "failed to copy dist to release"

if not exist "%FINAL_DIR%\_internal" call :die "_internal missing in release"
dir "%FINAL_DIR%\_internal" | findstr /i ".dll" >nul || call :die "_internal seems empty (dll not found)"

(
  echo @echo off
  echo setlocal
  echo cd /d "%%~dp0"
  echo start "" "%%~dp0%APP_NAME%.exe"
) > "%FINAL_DIR%\Start.bat"

REM cleanup intermediate artifacts
if exist "%ROOT%build" rmdir /s /q "%ROOT%build"
if exist "%ROOT%dist"  rmdir /s /q "%ROOT%dist"
if exist "%ROOT%Nadin.spec" del /f /q "%ROOT%Nadin.spec"

echo.
echo BUILD SUCCESS
echo Portable folder:
echo %FINAL_DIR%
echo Run application from this folder or via Start.bat.
pause
exit /b 0

:die
echo.
echo ERROR: %~1
echo See log: "%LOG%"
pause
exit /b 1
