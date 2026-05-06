@echo off
title JR Lira Watchdog FAST Loja 5
:loop
echo === FAST --- %DATE% %TIME% ===
call C:\Pentaho\app\sync_vendas_loja_5_sao-jose.bat
call C:\Pentaho\app\sync_loja_5_sao-jose.bat
ping -n 11 127.0.0.1 > nul
goto loop
