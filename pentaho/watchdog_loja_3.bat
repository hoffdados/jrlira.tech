@echo off
title JR Lira Watchdog Loja 3
:loop
echo === Ciclo --- %DATE% %TIME% ===
call C:\Pentaho\app\sync_vendas_loja_3_joao-pessoa.bat
call C:\Pentaho\app\sync_loja_3_joao-pessoa.bat
call C:\Pentaho\app\sync_compras_loja_3_joao-pessoa.bat
call C:\Pentaho\app\sync_fornecedores_loja_3_joao-pessoa.bat
call C:\Pentaho\app\sync_devolucoes_compra_loja_3_joao-pessoa.bat
ping -n 31 127.0.0.1 > nul
goto loop
