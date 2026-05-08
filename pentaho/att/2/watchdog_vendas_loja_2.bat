@echo off
title JR Lira VENDAS Loja 2
set PSEXE=C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe
set PING=C:\Windows\System32\PING.EXE

:loop
REM auto-kill de java/javaw do Pentaho pendurados ha mais de 15min
%PSEXE% -NoProfile -Command "Get-Process javaw,java -EA SilentlyContinue | Where-Object { $_.Path -like 'C:\Pentaho\*' -and $_.StartTime -lt (Get-Date).AddMinutes(-15) } | Stop-Process -Force -EA SilentlyContinue"

REM VENDAS a cada 30min (m=0 ou m=30) na janela ativa: 06:00-22:30 EXCETO 12:00-13:30
%PSEXE% -NoProfile -Command "$d=Get-Date;$h=$d.Hour;$m=$d.Minute;$t=$h*60+$m;$run=$true;if($t -lt 360 -or $t -ge 1350){$run=$false};if($t -ge 720 -and $t -lt 810){$run=$false};if($run -and ($m -ne 0 -and $m -ne 30)){$run=$false};if($run){exit 0}else{exit 1}"
if errorlevel 1 goto skip

echo === VENDAS %DATE% %TIME% ===
call C:\Pentaho\app\sync_vendas_loja_2_br_delta.bat

:skip
%PING% -n 61 127.0.0.1 > nul
goto loop
