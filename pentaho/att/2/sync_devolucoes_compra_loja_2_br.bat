@echo off
TITLE SyncDevolucoesCompra - Loja 2 BR
SET currentdir=%~dp0
SET kitchen=C:\Pentaho\data-integration\Kitchen.bat

echo.
echo ==========================================
echo INICIO: %DATE% %TIME%
echo ==========================================

"%kitchen%" /file:"%currentdir%job_devolucoes_compra_loja_2_br.kjb" "/param:LOJA_ID=2" /level:Basic

echo ==========================================
echo FIM:    %DATE% %TIME%
echo ==========================================
echo.
