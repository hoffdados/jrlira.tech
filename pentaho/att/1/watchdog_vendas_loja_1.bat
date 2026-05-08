@echo off
title JR Lira VENDAS Loja 1
set PSEXE=C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe
set PING=C:\Windows\System32\PING.EXE

:loop
%PSEXE% -NoProfile -Command "$d=Get-Date;$h=$d.Hour;$m=$d.Minute;$run=$true;if($h -notin @(10,16,22)){$run=$false};if($m -gt 4){$run=$false};if($run){exit 0}else{exit 1}"
if errorlevel 1 goto skip

echo === VENDAS %DATE% %TIME% ===
call C:\Pentaho\app\sync_vendas_loja_1_economico_delta.bat

:skip
%PING% -n 61 127.0.0.1 > nul
goto loop
