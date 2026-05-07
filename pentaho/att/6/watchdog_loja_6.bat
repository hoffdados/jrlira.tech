@echo off
title JR Lira Watchdog FAST Loja 6
:loop
echo === FAST --- %DATE% %TIME% ===
call C:\Pentaho\app\sync_vendas_loja_6_santarem_delta.bat
call C:\Pentaho\app\sync_loja_6_santarem.bat
C:\Windows\System32\PING.EXE -n 11 127.0.0.1 > nul
goto loop
