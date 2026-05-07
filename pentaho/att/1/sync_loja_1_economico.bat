@echo off
TITLE SyncAcougue - Loja 1 Economico
SET currentdir=%~dp0
SET kitchen=C:\Pentaho\data-integration\Kitchen.bat
SET logfile="%currentdir%log_loja_1.txt"

echo. >> %logfile%
echo ========================================== >> %logfile%
echo INICIO: %DATE% %TIME% >> %logfile%
echo ========================================== >> %logfile%

"%kitchen%" /file:"%currentdir%job_loja_1_economico.kjb" "/param:LOJA_ID=1" /level:Basic >> %logfile%

echo ========================================== >> %logfile%
echo FIM:    %DATE% %TIME% >> %logfile%
echo ========================================== >> %logfile%
echo. >> %logfile%
