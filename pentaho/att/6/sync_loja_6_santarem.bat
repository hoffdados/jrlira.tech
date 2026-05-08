@echo off
TITLE SyncProdutos - Loja 6 Santarem
SET currentdir=%~dp0
SET kitchen=C:\Pentaho\data-integration\Kitchen.bat

echo.
echo ==========================================
echo INICIO: %DATE% %TIME%
echo ==========================================

"%kitchen%" /file:"%currentdir%job_loja_6_santarem.kjb" "/param:LOJA_ID=6" /level:Basic

echo ==========================================
echo FIM:    %DATE% %TIME%
echo ==========================================
echo.
