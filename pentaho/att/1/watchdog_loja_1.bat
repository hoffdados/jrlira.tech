@echo off
title JR Lira Watchdog FAST Loja 1
:loop
echo === FAST --- %DATE% %TIME% ===
call C:\Pentaho\app\sync_vendas_loja_1_economico_delta.bat
call C:\Pentaho\app\sync_loja_1_economico.bat
C:\Windows\System32\PING.EXE -n 11 127.0.0.1 > nul
goto loop
