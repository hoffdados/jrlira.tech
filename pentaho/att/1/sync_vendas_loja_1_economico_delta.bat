@echo off
TITLE SyncVendas DELTA - Loja 1
SET currentdir=%~dp0
SET kitchen=C:\Pentaho\data-integration\Kitchen.bat
SET PSEXE=C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe

"%PSEXE%" -NoProfile -Command "(Get-Date).AddDays(-7).ToString('yyyy-MM-dd') | Out-File -FilePath '%currentdir%dc_vendas_1.txt' -Encoding ascii -NoNewline"
set /p DATA_CORTE=<"%currentdir%dc_vendas_1.txt"

echo.
echo === DELTA INICIO: %DATE% %TIME% (DATA_CORTE=%DATA_CORTE%) ===

"%kitchen%" /file:"%currentdir%job_vendas_loja_1_economico.kjb" "/param:LOJA_ID=1" "/param:DATA_CORTE=%DATA_CORTE%" /level:Basic

echo === DELTA FIM: %DATE% %TIME% ===
