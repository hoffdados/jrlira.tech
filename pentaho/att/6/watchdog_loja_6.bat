@echo off
title JR Lira FAST Produtos Loja 6
set PSEXE=C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe
set PING=C:\Windows\System32\PING.EXE

:loop
REM auto-kill de java/javaw do Pentaho pendurados ha mais de 15min (anti-zumbi)
%PSEXE% -NoProfile -Command "Get-Process javaw,java -EA SilentlyContinue | Where-Object { $_.Path -like 'C:\Pentaho\*' -and $_.StartTime -lt (Get-Date).AddMinutes(-15) } | Stop-Process -Force -EA SilentlyContinue"

REM Janela ativa: 06:00-22:30 EXCETO 12:00-13:30 e 06:55-07:15 (abertura). Cadencia: a cada 5min.
%PSEXE% -NoProfile -Command "$d=Get-Date;$h=$d.Hour;$m=$d.Minute;$t=$h*60+$m;$run=$true;if($t -lt 360 -or $t -ge 1350){$run=$false};if($t -ge 720 -and $t -lt 810){$run=$false};if($t -ge 415 -and $t -le 435){$run=$false};if($run -and ($m %% 5) -ne 0){$run=$false};if($run){exit 0}else{exit 1}"
if errorlevel 1 goto skip

echo === FAST PRODUTOS %DATE% %TIME% ===
call C:\Pentaho\app\sync_loja_6_santarem.bat

:skip
%PING% -n 61 127.0.0.1 > nul
goto loop
