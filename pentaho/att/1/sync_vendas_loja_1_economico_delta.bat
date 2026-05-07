@echo off
TITLE SyncVendas DELTA - Loja 1
SET currentdir=%~dp0
SET kitchen=C:\Pentaho\data-integration\Kitchen.bat
SET logfile="%currentdir%log_vendas_loja_1.txt"
SET PSEXE=C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe

REM Calcula data 7 dias atrás via arquivo temp (carga delta)
"%PSEXE%" -NoProfile -Command "(Get-Date).AddDays(-7).ToString('yyyy-MM-dd')" > "%TEMP%\dc_vendas_1.txt"
set /p DATA_CORTE=<"%TEMP%\dc_vendas_1.txt"

echo. >> %logfile% 2>&1
echo === DELTA INICIO: %DATE% %TIME% (DATA_CORTE=%DATA_CORTE%) === >> %logfile% 2>&1

"%kitchen%" /file:"%currentdir%job_vendas_loja_1_economico.kjb" "/param:LOJA_ID=1" "/param:DATA_CORTE=%DATA_CORTE%" /level:Basic >> %logfile% 2>&1

echo === DELTA FIM: %DATE% %TIME% === >> %logfile% 2>&1
