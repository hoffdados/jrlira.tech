@echo off
REM Cria serviço NSSM pro watchdog VENDAS Loja 5
REM Roda como Administrador

echo Parando servicos antigos...
nssm stop JRLiraSyncLoja5 2>/dev/null
nssm stop JRLiraSyncSlowLoja5 2>/dev/null
nssm stop JRLiraSyncVendasLoja5 2>/dev/null
nssm remove JRLiraSyncVendasLoja5 confirm 2>/dev/null

echo Instalando JRLiraSyncVendasLoja5...
nssm install JRLiraSyncVendasLoja5 "C:\Pentaho\app\watchdog_vendas_loja_5.bat"
nssm set JRLiraSyncVendasLoja5 AppDirectory "C:\Pentaho\app"
nssm set JRLiraSyncVendasLoja5 DisplayName "JR Lira Sync VENDAS Loja 5"
nssm set JRLiraSyncVendasLoja5 Start SERVICE_AUTO_START
nssm set JRLiraSyncVendasLoja5 AppStdout "C:\Pentaho\app\watchdog_vendas.log"
nssm set JRLiraSyncVendasLoja5 AppStderr "C:\Pentaho\app\watchdog_vendas_err.log"

echo Iniciando todos...
nssm start JRLiraSyncLoja5
nssm start JRLiraSyncSlowLoja5
nssm start JRLiraSyncVendasLoja5

echo OK. Servicos:
sc query JRLiraSyncLoja5 | find "STATE"
sc query JRLiraSyncSlowLoja5 | find "STATE"
sc query JRLiraSyncVendasLoja5 | find "STATE"
pause
