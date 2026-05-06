@echo off
title JR Lira Watchdog Loja 6
set ciclo=0
:loop
set /a ciclo+=1
echo === Ciclo %ciclo% --- %DATE% %TIME% ===
call C:\Pentaho\app\sync_vendas_loja_6_santarem.bat
call C:\Pentaho\app\sync_loja_6_santarem.bat
set /a resto=%ciclo% %% 6
if %resto%==0 call C:\Pentaho\app\sync_compras_loja_6_santarem.bat
set /a resto=%ciclo% %% 36
if %resto%==0 call C:\Pentaho\app\sync_fornecedores_loja_6_santarem.bat
if %resto%==0 call C:\Pentaho\app\sync_devolucoes_compra_loja_6_santarem.bat
ping -n 31 127.0.0.1 > nul
goto loop
