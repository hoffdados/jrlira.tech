@echo off
TITLE SyncCompras DELTA - Loja 4
SET currentdir=%~dp0
SET kitchen=C:\Pentaho\data-integration\Kitchen.bat
SET logfile="%currentdir%log_compras_loja_4.txt"

REM Calcula data 30 dias atrás
for /f %%i in ('powershell -NoProfile "(Get-Date).AddDays(-30).ToString('yyyy-MM-dd')"') do set DATA_CORTE=%%i

echo. >> %logfile% 2>&1
echo === DELTA INICIO: %DATE% %TIME% (DATA_CORTE=%DATA_CORTE%) === >> %logfile% 2>&1

"%kitchen%" /file:"%currentdir%job_compras_loja_4_floresta.kjb" "/param:LOJA_ID=4" "/param:DATA_CORTE=%DATA_CORTE%" /level:Basic >> %logfile% 2>&1

echo === DELTA FIM: %DATE% %TIME% === >> %logfile% 2>&1
