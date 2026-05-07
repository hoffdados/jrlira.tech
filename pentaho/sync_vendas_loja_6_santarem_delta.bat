@echo off
TITLE SyncVendas DELTA - Loja 6 Santarem
SET currentdir=%~dp0
SET kitchen=C:\Pentaho\data-integration\Kitchen.bat
SET logfile="%currentdir%log_vendas_loja_6.txt"

REM Calcula data 7 dias atrás (carga delta — só vendas recentes). Variável de ambiente é lida por ${DATA_CORTE} no KTR.
for /f %%i in ('powershell -NoProfile "(Get-Date).AddDays(-7).ToString('yyyy-MM-dd')"') do set DATA_CORTE=%%i

echo. >> %logfile% 2>&1
echo === DELTA INICIO: %DATE% %TIME% (DATA_CORTE=%DATA_CORTE%) === >> %logfile% 2>&1

"%kitchen%" /file:"%currentdir%job_vendas_loja_6_santarem.kjb" "/param:LOJA_ID=6" /level:Basic >> %logfile% 2>&1

echo === DELTA FIM: %DATE% %TIME% === >> %logfile% 2>&1
