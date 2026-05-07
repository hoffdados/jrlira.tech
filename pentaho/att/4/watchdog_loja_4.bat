@echo off
title JR Lira Watchdog FAST Loja 4
:loop
echo === FAST --- %DATE% %TIME% ===
call C:\Pentaho\app\sync_vendas_loja_4_floresta_delta.bat
call C:\Pentaho\app\sync_loja_4_floresta.bat
C:\Windows\System32\PING.EXE -n 11 127.0.0.1 > nul
goto loop
