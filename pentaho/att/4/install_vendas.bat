@echo off
set NSSM=C:\Pentaho\app\nssm.exe

echo Parando servicos antigos...
%NSSM% stop JRLiraSyncLoja4 2>nul
%NSSM% stop JRLiraSyncSlowLoja4 2>nul
%NSSM% stop JRLiraSyncVendasLoja4 2>nul
%NSSM% remove JRLiraSyncVendasLoja4 confirm 2>nul

echo Instalando JRLiraSyncVendasLoja4...
%NSSM% install JRLiraSyncVendasLoja4 "C:\Pentaho\app\watchdog_vendas_loja_4.bat"
%NSSM% set JRLiraSyncVendasLoja4 AppDirectory "C:\Pentaho\app"
%NSSM% set JRLiraSyncVendasLoja4 DisplayName "JR Lira Sync VENDAS Loja 4"
%NSSM% set JRLiraSyncVendasLoja4 Start SERVICE_AUTO_START
%NSSM% set JRLiraSyncVendasLoja4 AppStdout "C:\Pentaho\app\watchdog_vendas.log"
%NSSM% set JRLiraSyncVendasLoja4 AppStderr "C:\Pentaho\app\watchdog_vendas_err.log"

echo Iniciando todos...
%NSSM% start JRLiraSyncLoja4
%NSSM% start JRLiraSyncSlowLoja4
%NSSM% start JRLiraSyncVendasLoja4

echo OK. Servicos:
C:\Windows\System32\sc.exe query JRLiraSyncLoja4 | find "STATE"
C:\Windows\System32\sc.exe query JRLiraSyncSlowLoja4 | find "STATE"
C:\Windows\System32\sc.exe query JRLiraSyncVendasLoja4 | find "STATE"
pause
