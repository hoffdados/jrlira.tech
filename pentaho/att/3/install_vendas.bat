@echo off
REM Cria serviço NSSM pro watchdog VENDAS Loja 3
REM Roda como Administrador

echo Parando servicos antigos...
nssm stop JRLiraSyncLoja3 2>/dev/null
nssm stop JRLiraSyncSlowLoja3 2>/dev/null
nssm stop JRLiraSyncVendasLoja3 2>/dev/null
nssm remove JRLiraSyncVendasLoja3 confirm 2>/dev/null

echo Instalando JRLiraSyncVendasLoja3...
nssm install JRLiraSyncVendasLoja3 "C:\Pentaho\app\watchdog_vendas_loja_3.bat"
nssm set JRLiraSyncVendasLoja3 AppDirectory "C:\Pentaho\app"
nssm set JRLiraSyncVendasLoja3 DisplayName "JR Lira Sync VENDAS Loja 3"
nssm set JRLiraSyncVendasLoja3 Start SERVICE_AUTO_START
nssm set JRLiraSyncVendasLoja3 AppStdout "C:\Pentaho\app\watchdog_vendas.log"
nssm set JRLiraSyncVendasLoja3 AppStderr "C:\Pentaho\app\watchdog_vendas_err.log"

echo Iniciando todos...
nssm start JRLiraSyncLoja3
nssm start JRLiraSyncSlowLoja3
nssm start JRLiraSyncVendasLoja3

echo OK. Servicos:
sc query JRLiraSyncLoja3 | find "STATE"
sc query JRLiraSyncSlowLoja3 | find "STATE"
sc query JRLiraSyncVendasLoja3 | find "STATE"
pause
