# Copia os KTRs corrigidos do projeto para C:\Pentaho\app
# Rodar em CADA uma das 6 lojas (loja 1, 2, 3, 4, 5, 6).
# Pode ser executado direto no PowerShell:
#   PowerShell -ExecutionPolicy Bypass -File deploy_ktrs.ps1

$src = $PSScriptRoot
$dst = "C:\Pentaho\app"

if (-not (Test-Path $dst)) {
    Write-Host "ERRO: pasta de destino nao existe: $dst" -ForegroundColor Red
    exit 1
}

$arquivos = @(
    "sync_vendas_historico.ktr",
    "sync_compras_historico.ktr",
    "sync_fornecedores.ktr",
    "sync_delete_vendas_loja.ktr",
    "sync_delete_compras_loja.ktr",
    "sync_delete_fornecedores_loja.ktr",
    "sync_devolucoes_compra_cab.ktr",
    "sync_devolucoes_compra_itens.ktr"
)

foreach ($arq in $arquivos) {
    $origem = Join-Path $src $arq
    $destino = Join-Path $dst $arq
    if (Test-Path $origem) {
        Copy-Item -Path $origem -Destination $destino -Force
        Write-Host "OK  $arq" -ForegroundColor Green
    } else {
        Write-Host "SKIP $arq (nao existe na origem)" -ForegroundColor Yellow
    }
}

Write-Host ""
Write-Host "Pronto. Os KTRs corrigidos estao em $dst." -ForegroundColor Cyan
Write-Host "Agora pode rodar manualmente um dos .bat para testar:" -ForegroundColor Cyan
Write-Host "  $src\sync_vendas_loja_6_santarem.bat" -ForegroundColor Cyan
