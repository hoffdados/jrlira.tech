@echo off
TITLE SyncVendas DELTA - Loja 3
SET currentdir=%~dp0
SET kitchen=C:\Pentaho\data-integration\Kitchen.bat
SET logfile="%currentdir%log_vendas_loja_3.txt"

REM Calcula data 7 dias atrás (carga delta)
for /f %%i in ('powershell -NoProfile "(Get-Date).AddDays(-7).ToString('yyyy-MM-dd')"') do set DATA_CORTE=%%i

echo. >> %logfile% 2>&1
echo === DELTA INICIO: %DATE% %TIME% (DATA_CORTE=%DATA_CORTE%) === >> %logfile% 2>&1

"%kitchen%" /file:"%currentdir%job_vendas_loja_3_joao-pessoa.kjb" "/param:LOJA_ID=3" "/param:DATA_CORTE=%DATA_CORTE%" /level:Basic >> %logfile% 2>&1

echo === DELTA FIM: %DATE% %TIME% === >> %logfile% 2>&1
