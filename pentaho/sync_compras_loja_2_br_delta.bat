@echo off
TITLE SyncCompras DELTA - Loja 2
SET currentdir=%~dp0
SET kitchen=C:\Pentaho\data-integration\Kitchen.bat
SET logfile="%currentdir%log_compras_loja_2.txt"
SET PSEXE=C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe

"%PSEXE%" -NoProfile -Command "(Get-Date).AddDays(-30).ToString('yyyy-MM-dd') | Out-File -FilePath '%currentdir%dc_compras_2.txt' -Encoding ascii -NoNewline"
set /p DATA_CORTE=<"%currentdir%dc_compras_2.txt"

echo. >> %logfile% 2>&1
echo === DELTA INICIO: %DATE% %TIME% (DATA_CORTE=%DATA_CORTE%) === >> %logfile% 2>&1

"%kitchen%" /file:"%currentdir%job_compras_loja_2_br.kjb" "/param:LOJA_ID=2" "/param:DATA_CORTE=%DATA_CORTE%" /level:Basic >> %logfile% 2>&1

echo === DELTA FIM: %DATE% %TIME% === >> %logfile% 2>&1
