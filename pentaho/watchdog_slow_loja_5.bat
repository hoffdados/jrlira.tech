@echo off
title JR Lira Watchdog SLOW Loja 5
:loop
echo === SLOW --- %DATE% %TIME% ===
call C:\Pentaho\app\sync_compras_loja_5_sao-jose_delta.bat
call C:\Pentaho\app\sync_devolucoes_compra_loja_5_sao-jose.bat
call C:\Pentaho\app\sync_fornecedores_loja_5_sao-jose.bat
ping -n 121 127.0.0.1 > /dev/null
goto loop
