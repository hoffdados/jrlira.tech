@echo off
title JR Lira FAST Produtos Loja 5
set PSEXE=C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe
set PING=C:\Windows\System32\PING.EXE

:loop
%PSEXE% -NoProfile -Command "$d=Get-Date;$h=$d.Hour;$m=$d.Minute;$run=$true;if(($h -eq 6 -and $m -ge 55) -or ($h -eq 7 -and $m -le 15)){$run=$false};if($h -in @(10,16,22) -and $m -le 9){$run=$false};if($m -ge 45 -and $m -le 49){$run=$false};if(($m %% 5) -ne 0){$run=$false};if($run){exit 0}else{exit 1}"
if errorlevel 1 goto skip

echo === FAST PRODUTOS %DATE% %TIME% ===
call C:\Pentaho\app\sync_loja_5_sao-jose.bat

:skip
%PING% -n 61 127.0.0.1 > nul
goto loop
