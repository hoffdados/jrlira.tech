@echo off
TITLE SyncAcougue - Loja 2 BR
SET currentdir=%~dp0
SET kitchen=C:\Pentaho\app\data-integration\Kitchen.bat
SET logfile="%currentdir%log_loja_2.txt"

echo. >> %logfile%
echo ========================================== >> %logfile%
echo INICIO: %DATE% %TIME% >> %logfile%
echo ========================================== >> %logfile%

"%kitchen%" /file:"%currentdir%job_loja_2_br.kjb" "/param:LOJA_ID=2" /level:Basic >> %logfile%

echo ========================================== >> %logfile%
echo FIM:    %DATE% %TIME% >> %logfile%
echo ========================================== >> %logfile%
echo. >> %logfile%
