@echo off
title JR Lira Watchdog Loja 2
:loop
echo === Ciclo --- %DATE% %TIME% ===
call C:\Pentaho\app\sync_vendas_loja_2_br.bat
call C:\Pentaho\app\sync_loja_2_br.bat
call C:\Pentaho\app\sync_compras_loja_2_br.bat
call C:\Pentaho\app\sync_fornecedores_loja_2_br.bat
call C:\Pentaho\app\sync_devolucoes_compra_loja_2_br.bat
ping -n 31 127.0.0.1 > nul
goto loop
