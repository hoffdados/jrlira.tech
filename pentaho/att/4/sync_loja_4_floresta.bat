@echo off
TITLE SyncProdutos - Loja 4 Floresta
SET currentdir=%~dp0
SET kitchen=C:\Pentaho\data-integration\Kitchen.bat

echo.
echo ==========================================
echo INICIO: %DATE% %TIME%
echo ==========================================

"%kitchen%" /file:"%currentdir%job_loja_4_floresta.kjb" "/param:LOJA_ID=4" /level:Basic

echo ==========================================
echo FIM:    %DATE% %TIME%
echo ==========================================
echo.
