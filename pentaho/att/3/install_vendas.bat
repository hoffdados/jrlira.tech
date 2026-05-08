@echo off
set NSSM=C:\Pentaho\app\nssm.exe

echo Parando servicos antigos...
%NSSM% stop JRLiraSyncLoja3 2>nul
%NSSM% stop JRLiraSyncSlowLoja3 2>nul
%NSSM% stop JRLiraSyncVendasLoja3 2>nul
%NSSM% remove JRLiraSyncVendasLoja3 confirm 2>nul

echo Instalando JRLiraSyncVendasLoja3...
%NSSM% install JRLiraSyncVendasLoja3 "C:\Pentaho\app\watchdog_vendas_loja_3.bat"
%NSSM% set JRLiraSyncVendasLoja3 AppDirectory "C:\Pentaho\app"
%NSSM% set JRLiraSyncVendasLoja3 DisplayName "JR Lira Sync VENDAS Loja 3"
%NSSM% set JRLiraSyncVendasLoja3 Start SERVICE_AUTO_START
%NSSM% set JRLiraSyncVendasLoja3 AppStdout "C:\Pentaho\app\watchdog_vendas.log"
%NSSM% set JRLiraSyncVendasLoja3 AppStderr "C:\Pentaho\app\watchdog_vendas_err.log"

echo Reconfigurando AppStdout/AppStderr de FAST e SLOW (idempotente)...
%NSSM% set JRLiraSyncLoja3 AppStdout "C:\Pentaho\app\watchdog.log"
%NSSM% set JRLiraSyncLoja3 AppStderr "C:\Pentaho\app\watchdog_err.log"
%NSSM% set JRLiraSyncSlowLoja3 AppStdout "C:\Pentaho\app\watchdog_slow.log"
%NSSM% set JRLiraSyncSlowLoja3 AppStderr "C:\Pentaho\app\watchdog_slow_err.log"

echo Iniciando todos...
%NSSM% start JRLiraSyncLoja3
%NSSM% start JRLiraSyncSlowLoja3
%NSSM% start JRLiraSyncVendasLoja3

echo OK. Servicos:
C:\Windows\System32\sc.exe query JRLiraSyncLoja3 | find "STATE"
C:\Windows\System32\sc.exe query JRLiraSyncSlowLoja3 | find "STATE"
C:\Windows\System32\sc.exe query JRLiraSyncVendasLoja3 | find "STATE"
pause
