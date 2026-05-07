@echo off
TITLE SyncCompras - Loja 3 Joao Paulo
SET currentdir=%~dp0
SET kitchen=C:\Pentaho\data-integration\Kitchen.bat
SET logfile="%currentdir%log_compras_loja_3.txt"

echo. >> %logfile% 2>&1
echo ========================================== >> %logfile% 2>&1
echo INICIO: %DATE% %TIME% >> %logfile% 2>&1
echo ========================================== >> %logfile% 2>&1

"%kitchen%" /file:"%currentdir%job_compras_loja_3_joao-paulo.kjb" "/param:LOJA_ID=3" "/param:DATA_CORTE=2025-01-01" /level:Basic >> %logfile% 2>&1

echo ========================================== >> %logfile% 2>&1
echo FIM:    %DATE% %TIME% >> %logfile% 2>&1
echo ========================================== >> %logfile% 2>&1
echo. >> %logfile% 2>&1
