@echo off
title JR Lira SLOW Compras+Dev+Forn Loja 1
set PSEXE=C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe
set PING=C:\Windows\System32\PING.EXE

:loop
%PSEXE% -NoProfile -Command "$d=Get-Date;$h=$d.Hour;$m=$d.Minute;$run=$true;if($m -lt 45 -or $m -gt 49){$run=$false};if($h -eq 6){$run=$false};if($run){exit 0}else{exit 1}"
if errorlevel 1 goto skip

echo === SLOW COMPRAS+DEV+FORN %DATE% %TIME% ===
call C:\Pentaho\app\sync_compras_loja_1_economico_delta.bat
call C:\Pentaho\app\sync_devolucoes_compra_loja_1_economico.bat
call C:\Pentaho\app\sync_fornecedores_loja_1_economico.bat

:skip
%PING% -n 61 127.0.0.1 > nul
goto loop
