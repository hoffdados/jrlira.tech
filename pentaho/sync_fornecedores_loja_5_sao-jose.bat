@echo off
TITLE SyncFornecedores - Loja 5 sao-jose
SET currentdir=%~dp0
SET kitchen=C:\Pentaho\app\data-integration\Kitchen.bat
SET logfile="%currentdir%log_fornecedores_loja_5.txt"

echo. >> %logfile%
echo ========================================== >> %logfile%
echo INICIO: %DATE% %TIME% >> %logfile%
echo ========================================== >> %logfile%

"%kitchen%" /file:"%currentdir%job_fornecedores_loja_5_sao-jose.kjb" "/param:LOJA_ID=5" /level:Basic >> %logfile%

echo ========================================== >> %logfile%
echo FIM:    %DATE% %TIME% >> %logfile%
echo ========================================== >> %logfile%
echo. >> %logfile%
