@echo off
title JR Lira SLOW Compras+Dev+Forn Loja 5
set PSEXE=C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe
set PING=C:\Windows\System32\PING.EXE

:loop
REM auto-kill de java/javaw do Pentaho pendurados ha mais de 15min
%PSEXE% -NoProfile -Command "Get-Process javaw,java -EA SilentlyContinue | Where-Object { $_.Path -like 'C:\Pentaho\*' -and $_.StartTime -lt (Get-Date).AddMinutes(-15) } | Stop-Process -Force -EA SilentlyContinue"

REM SLOW roda 3x/dia: 08:00, 14:00, 20:00 (janela inicio ate :04)
%PSEXE% -NoProfile -Command "$d=Get-Date;$h=$d.Hour;$m=$d.Minute;$run=($h -in @(8,14,20) -and $m -le 4);if($run){exit 0}else{exit 1}"
if errorlevel 1 goto skip

echo === SLOW COMPRAS+DEV+FORN %DATE% %TIME% ===
call C:\Pentaho\app\sync_compras_loja_5_sao-jose_delta.bat
call C:\Pentaho\app\sync_devolucoes_compra_loja_5_sao-jose.bat
call C:\Pentaho\app\sync_fornecedores_loja_5_sao-jose.bat

:skip
%PING% -n 61 127.0.0.1 > nul
goto loop
