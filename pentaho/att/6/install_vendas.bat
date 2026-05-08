@echo off
set NSSM=C:\Pentaho\app\nssm.exe

echo Parando servicos antigos...
%NSSM% stop JRLiraSyncLoja6 2>nul
%NSSM% stop JRLiraSyncSlowLoja6 2>nul
%NSSM% stop JRLiraSyncVendasLoja6 2>nul
%NSSM% remove JRLiraSyncVendasLoja6 confirm 2>nul

echo Instalando JRLiraSyncVendasLoja6...
%NSSM% install JRLiraSyncVendasLoja6 "C:\Pentaho\app\watchdog_vendas_loja_6.bat"
%NSSM% set JRLiraSyncVendasLoja6 AppDirectory "C:\Pentaho\app"
%NSSM% set JRLiraSyncVendasLoja6 DisplayName "JR Lira Sync VENDAS Loja 6"
%NSSM% set JRLiraSyncVendasLoja6 Start SERVICE_AUTO_START
%NSSM% set JRLiraSyncVendasLoja6 AppStdout "C:\Pentaho\app\watchdog_vendas.log"
%NSSM% set JRLiraSyncVendasLoja6 AppStderr "C:\Pentaho\app\watchdog_vendas_err.log"

echo Reconfigurando AppStdout/AppStderr de FAST e SLOW (idempotente)...
%NSSM% set JRLiraSyncLoja6 AppStdout "C:\Pentaho\app\watchdog.log"
%NSSM% set JRLiraSyncLoja6 AppStderr "C:\Pentaho\app\watchdog_err.log"
%NSSM% set JRLiraSyncSlowLoja6 AppStdout "C:\Pentaho\app\watchdog_slow.log"
%NSSM% set JRLiraSyncSlowLoja6 AppStderr "C:\Pentaho\app\watchdog_slow_err.log"

echo Iniciando todos...
%NSSM% start JRLiraSyncLoja6
%NSSM% start JRLiraSyncSlowLoja6
%NSSM% start JRLiraSyncVendasLoja6

echo OK. Servicos:
C:\Windows\System32\sc.exe query JRLiraSyncLoja6 | find "STATE"
C:\Windows\System32\sc.exe query JRLiraSyncSlowLoja6 | find "STATE"
C:\Windows\System32\sc.exe query JRLiraSyncVendasLoja6 | find "STATE"
pause
