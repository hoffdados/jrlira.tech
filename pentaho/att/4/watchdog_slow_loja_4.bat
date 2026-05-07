@echo off
title JR Lira Watchdog SLOW Loja 4
:loop
echo === SLOW --- %DATE% %TIME% ===
call C:\Pentaho\app\sync_compras_loja_4_floresta_delta.bat
call C:\Pentaho\app\sync_devolucoes_compra_loja_4_floresta.bat
call C:\Pentaho\app\sync_fornecedores_loja_4_floresta.bat
C:\Windows\System32\PING.EXE -n 121 127.0.0.1 > nul
goto loop
