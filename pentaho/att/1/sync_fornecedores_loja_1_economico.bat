@echo off
TITLE SyncFornecedores - Loja 1 Economico
SET currentdir=%~dp0
SET kitchen=C:\Pentaho\data-integration\Kitchen.bat

echo.
echo ==========================================
echo INICIO: %DATE% %TIME%
echo ==========================================

"%kitchen%" /file:"%currentdir%job_fornecedores_loja_1_economico.kjb" "/param:LOJA_ID=1" /level:Basic

echo ==========================================
echo FIM:    %DATE% %TIME%
echo ==========================================
echo.
