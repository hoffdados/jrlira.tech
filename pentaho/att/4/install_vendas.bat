@echo off
REM Cria serviço NSSM pro watchdog VENDAS Loja 4
REM Roda como Administrador

echo Parando servicos antigos...
nssm stop JRLiraSyncLoja4 2>/dev/null
nssm stop JRLiraSyncSlowLoja4 2>/dev/null
nssm stop JRLiraSyncVendasLoja4 2>/dev/null
nssm remove JRLiraSyncVendasLoja4 confirm 2>/dev/null

echo Instalando JRLiraSyncVendasLoja4...
nssm install JRLiraSyncVendasLoja4 "C:\Pentaho\app\watchdog_vendas_loja_4.bat"
nssm set JRLiraSyncVendasLoja4 AppDirectory "C:\Pentaho\app"
nssm set JRLiraSyncVendasLoja4 DisplayName "JR Lira Sync VENDAS Loja 4"
nssm set JRLiraSyncVendasLoja4 Start SERVICE_AUTO_START
nssm set JRLiraSyncVendasLoja4 AppStdout "C:\Pentaho\app\watchdog_vendas.log"
nssm set JRLiraSyncVendasLoja4 AppStderr "C:\Pentaho\app\watchdog_vendas_err.log"

echo Iniciando todos...
nssm start JRLiraSyncLoja4
nssm start JRLiraSyncSlowLoja4
nssm start JRLiraSyncVendasLoja4

echo OK. Servicos:
sc query JRLiraSyncLoja4 | find "STATE"
sc query JRLiraSyncSlowLoja4 | find "STATE"
sc query JRLiraSyncVendasLoja4 | find "STATE"
pause
