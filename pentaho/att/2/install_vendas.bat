@echo off
set NSSM=C:\Pentaho\app\nssm.exe

echo Parando servicos antigos...
%NSSM% stop JRLiraSyncLoja2 2>nul
%NSSM% stop JRLiraSyncSlowLoja2 2>nul
%NSSM% stop JRLiraSyncVendasLoja2 2>nul
%NSSM% remove JRLiraSyncVendasLoja2 confirm 2>nul

echo Instalando JRLiraSyncVendasLoja2...
%NSSM% install JRLiraSyncVendasLoja2 "C:\Pentaho\app\watchdog_vendas_loja_2.bat"
%NSSM% set JRLiraSyncVendasLoja2 AppDirectory "C:\Pentaho\app"
%NSSM% set JRLiraSyncVendasLoja2 DisplayName "JR Lira Sync VENDAS Loja 2"
%NSSM% set JRLiraSyncVendasLoja2 Start SERVICE_AUTO_START
%NSSM% set JRLiraSyncVendasLoja2 AppStdout "C:\Pentaho\app\watchdog_vendas.log"
%NSSM% set JRLiraSyncVendasLoja2 AppStderr "C:\Pentaho\app\watchdog_vendas_err.log"

echo Iniciando todos...
%NSSM% start JRLiraSyncLoja2
%NSSM% start JRLiraSyncSlowLoja2
%NSSM% start JRLiraSyncVendasLoja2

echo OK. Servicos:
C:\Windows\System32\sc.exe query JRLiraSyncLoja2 | find "STATE"
C:\Windows\System32\sc.exe query JRLiraSyncSlowLoja2 | find "STATE"
C:\Windows\System32\sc.exe query JRLiraSyncVendasLoja2 | find "STATE"
pause
