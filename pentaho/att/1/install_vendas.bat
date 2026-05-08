@echo off
set NSSM=C:\Pentaho\app\nssm.exe

echo Parando servicos antigos...
%NSSM% stop JRLiraSyncLoja1 2>nul
%NSSM% stop JRLiraSyncSlowLoja1 2>nul
%NSSM% stop JRLiraSyncVendasLoja1 2>nul
%NSSM% remove JRLiraSyncVendasLoja1 confirm 2>nul

echo Instalando JRLiraSyncVendasLoja1...
%NSSM% install JRLiraSyncVendasLoja1 "C:\Pentaho\app\watchdog_vendas_loja_1.bat"
%NSSM% set JRLiraSyncVendasLoja1 AppDirectory "C:\Pentaho\app"
%NSSM% set JRLiraSyncVendasLoja1 DisplayName "JR Lira Sync VENDAS Loja 1"
%NSSM% set JRLiraSyncVendasLoja1 Start SERVICE_AUTO_START
%NSSM% set JRLiraSyncVendasLoja1 AppStdout "C:\Pentaho\app\watchdog_vendas.log"
%NSSM% set JRLiraSyncVendasLoja1 AppStderr "C:\Pentaho\app\watchdog_vendas_err.log"

echo Reconfigurando AppStdout/AppStderr de FAST e SLOW (idempotente)...
%NSSM% set JRLiraSyncLoja1 AppStdout "C:\Pentaho\app\watchdog.log"
%NSSM% set JRLiraSyncLoja1 AppStderr "C:\Pentaho\app\watchdog_err.log"
%NSSM% set JRLiraSyncSlowLoja1 AppStdout "C:\Pentaho\app\watchdog_slow.log"
%NSSM% set JRLiraSyncSlowLoja1 AppStderr "C:\Pentaho\app\watchdog_slow_err.log"

echo Iniciando todos...
%NSSM% start JRLiraSyncLoja1
%NSSM% start JRLiraSyncSlowLoja1
%NSSM% start JRLiraSyncVendasLoja1

echo OK. Servicos:
C:\Windows\System32\sc.exe query JRLiraSyncLoja1 | find "STATE"
C:\Windows\System32\sc.exe query JRLiraSyncSlowLoja1 | find "STATE"
C:\Windows\System32\sc.exe query JRLiraSyncVendasLoja1 | find "STATE"
pause
