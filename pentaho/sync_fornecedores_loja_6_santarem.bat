@echo off
TITLE SyncFornecedores - Loja 6 santarem
SET currentdir=%~dp0
SET kitchen=C:\Pentaho\data-integration\Kitchen.bat
SET logfile="%currentdir%log_fornecedores_loja_6.txt"

echo. >> %logfile% 2>&1
echo ========================================== >> %logfile% 2>&1
echo INICIO: %DATE% %TIME% >> %logfile% 2>&1
echo ========================================== >> %logfile% 2>&1

"%kitchen%" /file:"%currentdir%job_fornecedores_loja_6_santarem.kjb" "/param:LOJA_ID=6" /level:Basic >> %logfile% 2>&1

echo ========================================== >> %logfile% 2>&1
echo FIM:    %DATE% %TIME% >> %logfile% 2>&1
echo ========================================== >> %logfile% 2>&1
echo. >> %logfile% 2>&1