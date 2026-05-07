@echo off
title JR Lira Watchdog SLOW Loja 6
:loop
echo === SLOW --- %DATE% %TIME% ===
call C:\Pentaho\app\sync_compras_loja_6_santarem_delta.bat
call C:\Pentaho\app\sync_devolucoes_compra_loja_6_santarem.bat
call C:\Pentaho\app\sync_fornecedores_loja_6_santarem.bat
C:\Windows\System32\PING.EXE -n 121 127.0.0.1 > nul
goto loop
