@echo off
title JR Lira Watchdog FAST Loja 3
:loop
echo === FAST --- %DATE% %TIME% ===
call C:\Pentaho\app\sync_vendas_loja_3_joao-pessoa_delta.bat
call C:\Pentaho\app\sync_loja_3_joao-pessoa.bat
ping -n 11 127.0.0.1 > /dev/null
goto loop
