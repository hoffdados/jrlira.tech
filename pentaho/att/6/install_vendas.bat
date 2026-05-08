@echo off
REM Cria serviço NSSM pro watchdog VENDAS Loja 6
REM Roda como Administrador

echo Parando servicos antigos...
nssm stop JRLiraSyncLoja6 2>/dev/null
nssm stop JRLiraSyncSlowLoja6 2>/dev/null
nssm stop JRLiraSyncVendasLoja6 2>/dev/null
nssm remove JRLiraSyncVendasLoja6 confirm 2>/dev/null

echo Instalando JRLiraSyncVendasLoja6...
nssm install JRLiraSyncVendasLoja6 "C:\Pentaho\app\watchdog_vendas_loja_6.bat"
nssm set JRLiraSyncVendasLoja6 AppDirectory "C:\Pentaho\app"
nssm set JRLiraSyncVendasLoja6 DisplayName "JR Lira Sync VENDAS Loja 6"
nssm set JRLiraSyncVendasLoja6 Start SERVICE_AUTO_START
nssm set JRLiraSyncVendasLoja6 AppStdout "C:\Pentaho\app\watchdog_vendas.log"
nssm set JRLiraSyncVendasLoja6 AppStderr "C:\Pentaho\app\watchdog_vendas_err.log"

echo Iniciando todos...
nssm start JRLiraSyncLoja6
nssm start JRLiraSyncSlowLoja6
nssm start JRLiraSyncVendasLoja6

echo OK. Servicos:
sc query JRLiraSyncLoja6 | find "STATE"
sc query JRLiraSyncSlowLoja6 | find "STATE"
sc query JRLiraSyncVendasLoja6 | find "STATE"
pause
