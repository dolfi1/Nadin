@echo off
setlocal

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0update.ps1" %*
if errorlevel 1 (
  echo.
  echo Update failed.
  exit /b 1
)

echo Update completed.
