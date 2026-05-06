@echo off
title JR Lira Watchdog Loja 5
:loop
echo === Ciclo --- %DATE% %TIME% ===
call C:\Pentaho\app\sync_vendas_loja_5_sao-jose.bat
call C:\Pentaho\app\sync_loja_5_sao-jose.bat
call C:\Pentaho\app\sync_compras_loja_5_sao-jose.bat
call C:\Pentaho\app\sync_fornecedores_loja_5_sao-jose.bat
call C:\Pentaho\app\sync_devolucoes_compra_loja_5_sao-jose.bat
ping -n 31 127.0.0.1 > nul
goto loop
