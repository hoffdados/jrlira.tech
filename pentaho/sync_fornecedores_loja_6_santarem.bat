@echo off
TITLE SyncFornecedores - Loja 6 santarem
SET currentdir=%~dp0
SET kitchen=C:\Pentaho\app\data-integration\Kitchen.bat
SET logfile="%currentdir%log_fornecedores_loja_6.txt"

echo. >> %logfile%
echo ========================================== >> %logfile%
echo INICIO: %DATE% %TIME% >> %logfile%
echo ========================================== >> %logfile%

"%kitchen%" /file:"%currentdir%job_fornecedores_loja_6_santarem.kjb" "/param:LOJA_ID=6" /level:Basic >> %logfile%

echo ========================================== >> %logfile%
echo FIM:    %DATE% %TIME% >> %logfile%
echo ========================================== >> %logfile%
echo. >> %logfile%
