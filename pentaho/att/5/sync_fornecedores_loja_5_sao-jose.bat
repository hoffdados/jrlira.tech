@echo off
TITLE SyncFornecedores - Loja 5 Sao Jose
SET currentdir=%~dp0
SET kitchen=C:\Pentaho\data-integration\Kitchen.bat

echo.
echo ==========================================
echo INICIO: %DATE% %TIME%
echo ==========================================

"%kitchen%" /file:"%currentdir%job_fornecedores_loja_5_sao-jose.kjb" "/param:LOJA_ID=5" /level:Basic

echo ==========================================
echo FIM:    %DATE% %TIME%
echo ==========================================
echo.
