@echo off
title JR Lira Watchdog SLOW Loja 2
:loop
echo === SLOW --- %DATE% %TIME% ===
call C:\Pentaho\app\sync_compras_loja_2_br.bat
call C:\Pentaho\app\sync_devolucoes_compra_loja_2_br.bat
call C:\Pentaho\app\sync_fornecedores_loja_2_br.bat
ping -n 121 127.0.0.1 > nul
goto loop
