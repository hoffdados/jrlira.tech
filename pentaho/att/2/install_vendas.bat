@echo off
REM Cria serviço NSSM pro watchdog VENDAS Loja 2
REM Roda como Administrador

echo Parando servicos antigos...
nssm stop JRLiraSyncLoja2 2>/dev/null
nssm stop JRLiraSyncSlowLoja2 2>/dev/null
nssm stop JRLiraSyncVendasLoja2 2>/dev/null
nssm remove JRLiraSyncVendasLoja2 confirm 2>/dev/null

echo Instalando JRLiraSyncVendasLoja2...
nssm install JRLiraSyncVendasLoja2 "C:\Pentaho\app\watchdog_vendas_loja_2.bat"
nssm set JRLiraSyncVendasLoja2 AppDirectory "C:\Pentaho\app"
nssm set JRLiraSyncVendasLoja2 DisplayName "JR Lira Sync VENDAS Loja 2"
nssm set JRLiraSyncVendasLoja2 Start SERVICE_AUTO_START
nssm set JRLiraSyncVendasLoja2 AppStdout "C:\Pentaho\app\watchdog_vendas.log"
nssm set JRLiraSyncVendasLoja2 AppStderr "C:\Pentaho\app\watchdog_vendas_err.log"

echo Iniciando todos...
nssm start JRLiraSyncLoja2
nssm start JRLiraSyncSlowLoja2
nssm start JRLiraSyncVendasLoja2

echo OK. Servicos:
sc query JRLiraSyncLoja2 | find "STATE"
sc query JRLiraSyncSlowLoja2 | find "STATE"
sc query JRLiraSyncVendasLoja2 | find "STATE"
pause
