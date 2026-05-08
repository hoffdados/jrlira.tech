@echo off
TITLE SyncFornecedores - Loja 3 Joao Pessoa
SET currentdir=%~dp0
SET kitchen=C:\Pentaho\data-integration\Kitchen.bat

echo.
echo ==========================================
echo INICIO: %DATE% %TIME%
echo ==========================================

"%kitchen%" /file:"%currentdir%job_fornecedores_loja_3_joao-pessoa.kjb" "/param:LOJA_ID=3" /level:Basic

echo ==========================================
echo FIM:    %DATE% %TIME%
echo ==========================================
echo.
