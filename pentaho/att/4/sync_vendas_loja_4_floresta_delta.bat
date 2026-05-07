@echo off
TITLE SyncVendas DELTA - Loja 4
SET currentdir=%~dp0
SET kitchen=C:\Pentaho\data-integration\Kitchen.bat
SET logfile="%currentdir%log_vendas_loja_4.txt"
SET PSEXE=C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe

"%PSEXE%" -NoProfile -Command "(Get-Date).AddDays(-7).ToString('yyyy-MM-dd')" > "%TEMP%\dc_vendas_4.txt"
set /p DATA_CORTE=<"%TEMP%\dc_vendas_4.txt"

echo. >> %logfile% 2>&1
echo === DELTA INICIO: %DATE% %TIME% (DATA_CORTE=%DATA_CORTE%) === >> %logfile% 2>&1

"%kitchen%" /file:"%currentdir%job_vendas_loja_4_floresta.kjb" "/param:LOJA_ID=4" "/param:DATA_CORTE=%DATA_CORTE%" /level:Basic >> %logfile% 2>&1

echo === DELTA FIM: %DATE% %TIME% === >> %logfile% 2>&1
