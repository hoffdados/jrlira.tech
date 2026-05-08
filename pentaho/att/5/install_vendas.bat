@echo off
set NSSM=C:\Pentaho\app\nssm.exe

echo Parando servicos antigos...
%NSSM% stop JRLiraSyncLoja5 2>nul
%NSSM% stop JRLiraSyncSlowLoja5 2>nul
%NSSM% stop JRLiraSyncVendasLoja5 2>nul
%NSSM% remove JRLiraSyncVendasLoja5 confirm 2>nul

echo Instalando JRLiraSyncVendasLoja5...
%NSSM% install JRLiraSyncVendasLoja5 "C:\Pentaho\app\watchdog_vendas_loja_5.bat"
%NSSM% set JRLiraSyncVendasLoja5 AppDirectory "C:\Pentaho\app"
%NSSM% set JRLiraSyncVendasLoja5 DisplayName "JR Lira Sync VENDAS Loja 5"
%NSSM% set JRLiraSyncVendasLoja5 Start SERVICE_AUTO_START
%NSSM% set JRLiraSyncVendasLoja5 AppStdout "C:\Pentaho\app\watchdog_vendas.log"
%NSSM% set JRLiraSyncVendasLoja5 AppStderr "C:\Pentaho\app\watchdog_vendas_err.log"

echo Iniciando todos...
%NSSM% start JRLiraSyncLoja5
%NSSM% start JRLiraSyncSlowLoja5
%NSSM% start JRLiraSyncVendasLoja5

echo OK. Servicos:
C:\Windows\System32\sc.exe query JRLiraSyncLoja5 | find "STATE"
C:\Windows\System32\sc.exe query JRLiraSyncSlowLoja5 | find "STATE"
C:\Windows\System32\sc.exe query JRLiraSyncVendasLoja5 | find "STATE"
pause
