@echo off
REM Cria serviço NSSM pro watchdog VENDAS Loja 1
REM Roda como Administrador

echo Parando servicos antigos...
nssm stop JRLiraSyncLoja1 2>/dev/null
nssm stop JRLiraSyncSlowLoja1 2>/dev/null
nssm stop JRLiraSyncVendasLoja1 2>/dev/null
nssm remove JRLiraSyncVendasLoja1 confirm 2>/dev/null

echo Instalando JRLiraSyncVendasLoja1...
nssm install JRLiraSyncVendasLoja1 "C:\Pentaho\app\watchdog_vendas_loja_1.bat"
nssm set JRLiraSyncVendasLoja1 AppDirectory "C:\Pentaho\app"
nssm set JRLiraSyncVendasLoja1 DisplayName "JR Lira Sync VENDAS Loja 1"
nssm set JRLiraSyncVendasLoja1 Start SERVICE_AUTO_START
nssm set JRLiraSyncVendasLoja1 AppStdout "C:\Pentaho\app\watchdog_vendas.log"
nssm set JRLiraSyncVendasLoja1 AppStderr "C:\Pentaho\app\watchdog_vendas_err.log"

echo Iniciando todos...
nssm start JRLiraSyncLoja1
nssm start JRLiraSyncSlowLoja1
nssm start JRLiraSyncVendasLoja1

echo OK. Servicos:
sc query JRLiraSyncLoja1 | find "STATE"
sc query JRLiraSyncSlowLoja1 | find "STATE"
sc query JRLiraSyncVendasLoja1 | find "STATE"
pause
