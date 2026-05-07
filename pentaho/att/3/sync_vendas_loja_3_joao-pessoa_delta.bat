@echo off
TITLE SyncVendas DELTA - Loja 3
SET currentdir=%~dp0
SET kitchen=C:\Pentaho\data-integration\Kitchen.bat
SET logfile="%currentdir%log_vendas_loja_3.txt"
SET PSEXE=C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe

"%PSEXE%" -NoProfile -Command "(Get-Date).AddDays(-7).ToString('yyyy-MM-dd') | Out-File -FilePath '%currentdir%dc_vendas_3.txt' -Encoding ascii -NoNewline"
set /p DATA_CORTE=<"%currentdir%dc_vendas_3.txt"

echo. >> %logfile% 2>&1
echo === DELTA INICIO: %DATE% %TIME% (DATA_CORTE=%DATA_CORTE%) === >> %logfile% 2>&1

"%kitchen%" /file:"%currentdir%job_vendas_loja_3_joao-pessoa.kjb" "/param:LOJA_ID=3" "/param:DATA_CORTE=%DATA_CORTE%" /level:Basic >> %logfile% 2>&1

echo === DELTA FIM: %DATE% %TIME% === >> %logfile% 2>&1
