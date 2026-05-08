@echo off
TITLE SyncCompras DELTA - Loja 4
SET currentdir=%~dp0
SET kitchen=C:\Pentaho\data-integration\Kitchen.bat
SET PSEXE=C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe

"%PSEXE%" -NoProfile -Command "(Get-Date).AddDays(-30).ToString('yyyy-MM-dd') | Out-File -FilePath '%currentdir%dc_compras_4.txt' -Encoding ascii -NoNewline"
set /p DATA_CORTE=<"%currentdir%dc_compras_4.txt"

echo.
echo === DELTA INICIO: %DATE% %TIME% (DATA_CORTE=%DATA_CORTE%) ===

"%kitchen%" /file:"%currentdir%job_compras_loja_4_floresta.kjb" "/param:LOJA_ID=4" "/param:DATA_CORTE=%DATA_CORTE%" /level:Basic

echo === DELTA FIM: %DATE% %TIME% ===
