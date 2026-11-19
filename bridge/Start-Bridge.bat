@echo off
title Swan Bridge - Swanplicity
cd /d "%~dp0"
echo ============================================
echo   SWAN BRIDGE - by Swanplicity
echo   Miners connect at ws://127.0.0.1:3333
echo   Keep this window open while mining.
echo ============================================
:loop
node swan-bridge.js
echo.
echo Swan Bridge stopped. Restarting in 5 seconds...
timeout /t 5 /nobreak >nul
goto loop
