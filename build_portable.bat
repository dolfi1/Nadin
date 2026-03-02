@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM ====== CONFIG ======
set "APP_NAME=Nadin"
set "ENTRY=web_app.py"
set "CLEAN_MODE=release_only"

REM ====== ROOT = folder where this bat is located ======
set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"

set "RELEASE_BASE=%ROOT%\release"
set "RELEASE=%RELEASE_BASE%\%APP_NAME%"
set "LOG=%RELEASE_BASE%\build_log.txt"
set "MARKER=%ROOT%\.nadin_root_marker"
set "VENV=%ROOT%\.venv_build"

echo.
echo === Nadin portable build ===
echo ROOT: "%ROOT%"
echo CLEAN_MODE: "%CLEAN_MODE%"

if not exist "%RELEASE_BASE%" mkdir "%RELEASE_BASE%"

>"%LOG%" echo [START] %date% %time%
call :log ROOT=%ROOT%
call :log CLEAN_MODE=%CLEAN_MODE%

REM ====== SAFETY GUARDS ======
if /I "%ROOT%"=="C:" (
  echo ERROR: ROOT points to C:\ . Abort.
  call :log ERROR: ROOT points to C:\
  exit /b 1
)

if "%ROOT:~3,1%"=="" (
  echo ERROR: ROOT path is suspiciously short: "%ROOT%"
  call :log ERROR: ROOT path is suspiciously short: %ROOT%
  exit /b 1
)

if not exist "%ROOT%\build_portable.bat" (
  echo ERROR: build_portable.bat is missing in ROOT. Abort.
  call :log ERROR: build_portable.bat is missing in ROOT
  exit /b 1
)

if not exist "%ROOT%\%ENTRY%" (
  echo ERROR: entrypoint not found: "%ROOT%\%ENTRY%"
  call :log ERROR: entrypoint not found: %ROOT%\%ENTRY%
  exit /b 1
)

if not exist "%MARKER%" (
  echo WARNING: marker not found. Creating "%MARKER%"
  >"%MARKER%" echo nadin-root
  call :log WARNING: marker created: %MARKER%
)

call :log Guards passed

REM ====== CLEAN BUILD ARTIFACTS ======
echo.
echo Cleaning old build artifacts...
if exist "%ROOT%\build" rmdir /s /q "%ROOT%\build"
if exist "%ROOT%\dist"  rmdir /s /q "%ROOT%\dist"

REM ====== BUILD VENV ======
if not exist "%VENV%\Scripts\python.exe" (
  echo.
  echo Creating build venv...
  py -3.9 -m venv "%VENV%"
  if errorlevel 1 (
    echo ERROR: failed to create venv
    call :log ERROR: failed to create venv
    exit /b 1
  )
)

call :log Using venv: %VENV%

echo.
echo Installing deps...
call "%VENV%\Scripts\python.exe" -m pip install --upgrade pip
if errorlevel 1 (
  call :log ERROR: pip upgrade failed
  exit /b 1
)

call "%VENV%\Scripts\python.exe" -m pip install -r "%ROOT%\requirements.txt"
if errorlevel 1 (
  call :log ERROR: requirements install failed
  exit /b 1
)

call "%VENV%\Scripts\python.exe" -m pip install pyinstaller
if errorlevel 1 (
  call :log ERROR: pyinstaller install failed
  exit /b 1
)

REM ====== BUILD ======
echo.
echo Running PyInstaller...
pushd "%ROOT%"
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
  call :log ERROR: PyInstaller failed with code %RC%
  exit /b %RC%
)

if not exist "%ROOT%\dist\%APP_NAME%\%APP_NAME%.exe" (
  echo ERROR: EXE not found: "%ROOT%\dist\%APP_NAME%\%APP_NAME%.exe"
  call :log ERROR: dist exe missing: %ROOT%\dist\%APP_NAME%\%APP_NAME%.exe
  exit /b 1
)

call :log EXE found in dist: %ROOT%\dist\%APP_NAME%\%APP_NAME%.exe

REM ====== RELEASE FOLDER ======
echo.
echo Preparing release folder: "%RELEASE%"
if exist "%RELEASE%" rmdir /s /q "%RELEASE%"
mkdir "%RELEASE%"

xcopy "%ROOT%\dist\%APP_NAME%\*" "%RELEASE%\" /E /I /H /Y >nul
if errorlevel 1 (
  echo ERROR: failed to copy dist to release
  call :log ERROR: xcopy dist->release failed
  exit /b 1
)

if not exist "%RELEASE%\%APP_NAME%.exe" (
  echo ERROR: EXE not found in release: "%RELEASE%\%APP_NAME%.exe"
  call :log ERROR: release exe missing: %RELEASE%\%APP_NAME%.exe
  exit /b 1
)

call :log EXE found in release: %RELEASE%\%APP_NAME%.exe

REM ====== CLEAN STEP (ONLY AFTER SUCCESSFUL BUILD) ======
if /I "%CLEAN_MODE%"=="release_only" (
  call :clean_release
) else if /I "%CLEAN_MODE%"=="repo_root" (
  call :clean_repo_root
) else (
  echo ERROR: unknown CLEAN_MODE="%CLEAN_MODE%"
  call :log ERROR: unknown CLEAN_MODE=%CLEAN_MODE%
  exit /b 1
)

REM ====== OPTIONAL ZIP ======
echo.
echo Creating zip...
powershell -NoProfile -Command ^
  "Compress-Archive -Path '%RELEASE%\*' -DestinationPath '%RELEASE_BASE%\%APP_NAME%_Portable.zip' -Force" >nul
if errorlevel 1 (
  call :log WARNING: zip creation failed
) else (
  call :log ZIP created: %RELEASE_BASE%\%APP_NAME%_Portable.zip
)

REM ====== CLEAN BUILD ARTIFACTS ======
if exist "%ROOT%\build" rmdir /s /q "%ROOT%\build"
if exist "%ROOT%\dist"  rmdir /s /q "%ROOT%\dist"
call :log build/dist cleaned

echo.
echo DONE: "%RELEASE%\%APP_NAME%.exe"
echo ZIP:  "%RELEASE_BASE%\%APP_NAME%_Portable.zip"
call :log DONE

pause
exit /b 0

:clean_release
echo.
echo Cleaning release folder only...
call :log Clean mode: release_only

for /r "%RELEASE%" %%F in (*.py *.pyc *.pyo *.spec) do (
  del /f /q "%%F"
  call :log Deleted file (release): %%F
)

for /d /r "%RELEASE%" %%D in (__pycache__) do (
  rmdir /s /q "%%D"
  call :log Deleted dir (release): %%D
)

for %%D in (tests .git .github .venv_build nadin_scrapy) do (
  if exist "%RELEASE%\%%D" (
    rmdir /s /q "%RELEASE%\%%D"
    call :log Deleted dir (release): %RELEASE%\%%D
  )
)

for %%F in (requirements.txt README README.txt README.md .gitignore .gitattributes) do (
  if exist "%RELEASE%\%%F" (
    del /f /q "%RELEASE%\%%F"
    call :log Deleted file (release): %RELEASE%\%%F
  )
)

exit /b 0

:clean_repo_root
echo.
echo Cleaning repository root (dangerous mode)...
call :log Clean mode: repo_root

for %%F in (
  web_app.py
  scrape_client.py
  card_bot.py
  constants.py
  logging_setup.py
  desktop_app.py
  app_paths.py
) do (
  if exist "%ROOT%\%%F" (
    echo Will delete: "%ROOT%\%%F"
    call :log Will delete file (repo_root): %ROOT%\%%F
  )
)

for %%D in (tests nadin_scrapy .venv_build __pycache__) do (
  if exist "%ROOT%\%%D" (
    echo Will delete: "%ROOT%\%%D"
    call :log Will delete dir (repo_root): %ROOT%\%%D
  )
)

for %%F in (
  web_app.py
  scrape_client.py
  card_bot.py
  constants.py
  logging_setup.py
  desktop_app.py
  app_paths.py
) do (
  if exist "%ROOT%\%%F" (
    del /f /q "%ROOT%\%%F"
    call :log Deleted file (repo_root): %ROOT%\%%F
  )
)

for %%D in (tests nadin_scrapy .venv_build __pycache__) do (
  if exist "%ROOT%\%%D" (
    rmdir /s /q "%ROOT%\%%D"
    call :log Deleted dir (repo_root): %ROOT%\%%D
  )
)

exit /b 0

:log
>>"%LOG%" echo [%date% %time%] %*
exit /b 0
