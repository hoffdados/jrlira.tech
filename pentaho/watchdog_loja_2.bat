@echo off
title JR Lira Watchdog FAST Loja 2
:loop
echo === FAST --- %DATE% %TIME% ===
call C:\Pentaho\app\sync_vendas_loja_2_br.bat
call C:\Pentaho\app\sync_loja_2_br.bat
ping -n 11 127.0.0.1 > nul
goto loop
