@echo off
TITLE SyncAcougue - Loja 3 Joao Pessoa
SET currentdir=%~dp0
SET kitchen=C:\Pentaho\app\data-integration\Kitchen.bat
SET logfile="%currentdir%log_loja_3.txt"

echo. >> %logfile%
echo ========================================== >> %logfile%
echo INICIO: %DATE% %TIME% >> %logfile%
echo ========================================== >> %logfile%

"%kitchen%" /file:"%currentdir%job_loja_3_joao-pessoa.kjb" "/param:LOJA_ID=3" /level:Basic >> %logfile%

echo ========================================== >> %logfile%
echo FIM:    %DATE% %TIME% >> %logfile%
echo ========================================== >> %logfile%
echo. >> %logfile%
