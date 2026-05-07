# Manual — Fluxo de Pedido de Fornecedor (NF-e)

Sistema **JR Lira Tech** — manual operacional ponta a ponta do pedido de compra a um fornecedor, da geração até o fechamento da nota fiscal e cobrança de eventuais devoluções/acordos.

---

## 1. Visão Geral

### 1.1 O que é um "pedido de fornecedor"

É a compra que a rede SuperAsa faz de um fornecedor externo (não do CD próprio). Cada pedido segue um ciclo de vida com vários atores e telas. A nota fiscal eletrônica (NF-e) emitida pelo fornecedor é o documento que materializa esse pedido.

### 1.2 Atores

| Perfil | Quem é | O que faz |
|---|---|---|
| **Vendedor** | Pessoa do fornecedor com login no portal `/vendedor` | Monta o pedido, envia, fatura |
| **Comprador** | Funcionário SuperAsa que negocia com fornecedores | Valida pedidos, importa NF-e, vincula |
| **Cadastro** | Funcionário da loja que cuida do cadastro de produtos no ERP | Libera nota pro estoque, valida final |
| **Estoque** | Funcionário do recebimento físico da loja | Confere a mercadoria contra a nota |
| **Auditor** | Auditoria interna | Fecha a auditoria após divergências resolvidas |
| **Admin** | Administrador do sistema | Aprova exceções (emergenciais, validades em risco) |

### 1.3 Duas origens de pedido

1. **Vendedor inicia** (fluxo A) — vendedor monta no portal e envia ao comprador.
2. **Comprador inicia** (fluxo B, novo) — comprador gera rascunho via `/sugestao-compras`, alinha com fornecedor e confirma.

Ambas convergem para o mesmo caminho a partir do status `validado`.

### 1.4 Glossário

- **Rascunho:** pedido ainda em construção, não enviado.
- **Aguardando validação:** vendedor enviou, comprador analisa.
- **Aguardando auditoria:** comprador detectou item >30% acima da sugestão; admin precisa aprovar.
- **Validado:** comprador aprovou, pedido oficial.
- **Faturado:** vendedor emitiu a NF-e e informou o número.
- **Atrasado:** sem faturamento dentro do prazo.
- **Vinculado:** XML chegou e foi associado ao pedido.
- **Rejeitado:** comprador rejeitou (motivo registrado).
- **Cancelado pelo vendedor:** vendedor cancelou (motivo registrado).

---

## 2. Fluxo A — Vendedor inicia

### 2.1 Pré-requisitos

- Vendedor cadastrado em `/fornecedores` com email e telefone.
- Loja-destino cadastrada (`lojas` table com CNPJ, nome e ativo=true).
- Vendedor recebeu credenciais de acesso por email/WhatsApp.

### 2.2 Passo a passo

#### Etapa 1 — Vendedor monta o pedido

- **Tela:** `/vendedor`
- **Ação:** botão "+ Novo Pedido" → escolhe loja-destino e condição de pagamento.
- Acrescenta itens (busca por código de barras → traz descrição + preço sugerido).
- Define qtd e preço por item.
- Pode salvar como **rascunho** sem enviar.

> **Status:** `rascunho`

#### Etapa 2 — Vendedor envia

- Botão "✓ Enviar Pedido" → muda status.
- Sistema envia notificação ao comprador (WhatsApp + email).

> **Status:** `aguardando_validacao`

#### Etapa 3 — Comprador valida

- **Tela:** `/pedidos-comprador`
- Filtra "Aguardando validação" → seleciona pedido na lista esquerda.
- Painel direito mostra:
  - Cabeçalho com fornecedor, vendedor, loja, condição de pagamento.
  - Itens com:
    - Cobertura (estoque + trânsito) ÷ venda média = dias.
    - Sugestão de qtd baseada na cobertura.
    - Indicador 🔴 / 🟡 / 🟢 / ✨ de prioridade.
    - Histórico de preço (última compra desse EAN).
    - Propostas alternativas de outros fornecedores pra mesmo EAN.
- Comprador ajusta `qtd_validada` e `preco_validado`.
- Pode adicionar/remover itens.

#### Etapa 4 — Validação

Ao clicar **"✓ Validar e enviar PDF ao fornecedor"**:

- Sistema calcula sugestão de qtd vs qtd_validada.
- Se algum item passou de **30% acima da sugestão**:
  - Sistema exige justificativa em modal.
  - Pedido vai pra `aguardando_auditoria` e admin é notificado.
- Se nenhum excesso → pedido vira `validado`.

> **Status:** `validado` (ou `aguardando_auditoria`)

#### Etapa 5 — Auditoria (se necessária)

- **Tela:** `/auditoria-pedidos`
- Admin vê justificativas e:
  - Aprova → status `validado`, vendedor recebe email + WhatsApp + PDF.
  - Rejeita → status `rejeitado`, motivo registrado, vendedor é notificado.

#### Etapa 6 — Vendedor fatura

- Vendedor recebe email com PDF do pedido validado.
- Emite NF-e no seu ERP.
- Volta em `/vendedor`, abre o pedido, clica "📄 Faturar" → digita nº NF, data emissão, anexa observações.
- Sistema valida e muda status.

> **Status:** `faturado`

#### Etapa 7 — Cron de prazo

Se o pedido validado não for faturado em **N dias** (configurável por fornecedor — `fornecedores.leadtime_dias`):

- Cron diário muda pra `atrasado`.
- WhatsApp de cobrança é enviado ao vendedor.

> **Status:** `atrasado`

---

## 3. Fluxo B — Comprador inicia (sugestão de compras)

### 3.1 Quando usar

- Reposição proativa (loja precisa, vendedor não enviou pedido).
- Comprador analisou cobertura e quer sugerir lista pro fornecedor.

### 3.2 Passo a passo

#### Etapa 1 — Análise de sugestão

- **Tela:** `/sugestao-compras`
- Comprador seleciona **fornecedor + loja**.
- Sistema cruza:
  - `compras_historico` — todos os EANs já comprados desse fornecedor pra essa loja.
  - `vendas_historico` — vendas dos últimos 90 dias por EAN.
  - `produtos_externo` — estoque atual.
  - `itens_pedido` — quantidade em trânsito (pedidos validados/vinculados).
- Calcula:
  - `media_dia` = total_90d / 90
  - `cobertura` = (estoque + trânsito) / media_dia
  - `sugestao_un` = max(0, 30 × media_dia − estoque − trânsito)
  - `sugestao_caixas` = ceil(sugestao_un / qtdeembalagem)
- Comprador ajusta quantidades manualmente.

#### Etapa 2 — Geração do rascunho

- Botão "Gerar Pedido Rascunho" → cria registro em `pedidos`:
  - `numero_pedido` = `SUG-{timestamp}`
  - `status` = `rascunho`
  - `criado_por_comprador` = nome do usuário logado
  - Sem `vendedor_id` ainda

> **Status:** `rascunho`

#### Etapa 3 — Alinhamento com fornecedor (offline)

- Comprador liga/chama no WhatsApp o fornecedor pra alinhar:
  - Itens
  - Quantidades
  - Preços
  - Prazo de pagamento
- Eventual ajuste do rascunho na tela `/pedidos-comprador` (filtro "📝 Rascunhos (sugestão)").

#### Etapa 4 — Confirmação do pedido

- Em `/pedidos-comprador` o comprador abre o rascunho.
- Edita `qtd_validada` e `preco_validado` se algo mudou no alinhamento.
- Box "📞 Vendedor que negociou" → escolhe um vendedor do fornecedor.
- Clica **"✓ Confirmar pedido (alinhado com fornecedor)"**.
- Sistema:
  - Atribui `vendedor_id`.
  - Aplica regra dos 30% (mesma do fluxo A).
  - Vira `validado` ou `aguardando_auditoria`.
  - Envia email + WhatsApp + PDF pro vendedor.

> **Status:** `validado`

A partir daqui o fluxo segue idêntico ao A: vendedor fatura, etc.

---

## 4. Recebimento da NF-e

### 4.1 Importação do XML

- **Tela:** `/notas-comprador`
- Botão "+ Importar XML" → seleciona arquivo `.xml`.
- Sistema parseia (`src/parsers/nfe.js`):
  - Header: chave NF-e, número, série, fornecedor, valor total, impostos.
  - Itens: cada produto com:
    - `cEAN` (EAN comercial) e `cEANTrib` (EAN tributável).
    - `qCom` / `uCom` (qtd e unidade comercial — como o fornecedor vende).
    - `qTrib` / `uTrib` (qtd e unidade tributável — geralmente UN).
    - Valores fiscais (vProd, vIPI, vST, vFCP, vFrete, vSeguro, etc.).

### 4.2 Cálculo automático de qtd-por-caixa

O sistema tenta detectar quantas unidades vêm em cada caixa:

1. **XML estruturado:** se `uCom != uTrib` (ex.: CX × UN) e razão `qTrib/qCom` é inteira ≥ 2 → usa esse valor com confiança **alta**.
2. **Descrição (fallback):** se `uCom == uTrib` (fornecedor factura tudo em CX/UN) e `cEAN != cEANTrib` (sinal de embalagem) → tenta extrair de strings tipo "12X500ML", "30X150G".
   - Confiança **média** se descrição tem unidade clara.
   - Confiança **baixa** se padrão genérico "NxN".
3. **Cadastro prévio:** se já houve cadastro manual (em `embalagens_fornecedor`) com aquele EAN+CNPJ ou descrição+CNPJ → aplica direto com confiança **cadastrada** (sobrescreve o detectado).
4. **EAN_nota == EAN_trib:** *bypass da regra* — o sistema entende que o produto é vendido unidade-a-unidade (não em caixa). Default: emb=1.

### 4.3 Cálculo de preço unitário

| Caso | Fórmula `preco_unitario_nota` | `preco_unitario_caixa` |
|---|---|---|
| `uCom != uTrib` (caixa × unidade) | `custo_total / qTrib` | `custo_total / qCom` |
| `uCom == uTrib` + qtd-caixa detectada | `custo_total / (qCom × emb)` | `custo_total / qCom` |
| `uCom == uTrib` sem qtd-caixa | `custo_total / qCom` | NULL |

Onde `custo_total = vProd − vDesc + vFrete + vSeg + vOutro + vIPI + vST + vFCP_ST`.

### 4.4 Vinculação ao pedido

#### Caso A — XML tem `pedido_id` no banco para esse fornecedor

- Sistema sugere os pedidos **validados** desse fornecedor com CNPJ batendo.
- Comprador escolhe e clica "Vincular".
- Pedido vira `vinculado`, nota vira `em_validacao_cadastro`.

#### Caso B — Comprador não acha pedido (compra emergencial)

- Comprador clica "⚠ Marcar como Emergencial".
- Nota vai pra `emergencial_pendente`.
- Admin precisa aprovar em `/notas-comprador` (mesma tela, vê notas emergenciais pendentes).
- Aprovação → status `em_validacao_cadastro`.

> **Status nota:** `importada` → `em_validacao_cadastro` (ou `emergencial_pendente`)

---

## 5. Validação no Cadastro

### 5.1 O que o cadastro faz

- **Tela:** `/notas-cadastro` (perfil `cadastro`)
- Cadastro analisa cada item:
  - Se EAN bate com `produtos_externo` → traz custo origem (CD) → calcula `status_preco`.
  - Se não bate → produto **NOVO**, cadastro precisa associar EAN ou cadastrar no Ecocentauro.
- Coluna **Emb** editável: cadastro confirma a qtd-por-caixa.
  - Salvar = aplica imediato (recalcula preço/qtd em UN, autocadastra em `embalagens_fornecedor`, log de auditoria).
- Status do preço:
  - **= IGUAL** (azul): preço NF == custo CD (dif ≤ R$ 0,01)
  - **▼ MENOR** (verde): preço NF < custo CD
  - **▲ MAIOR** (vermelho, letras brancas): preço NF > custo CD (até 15%)
  - **⚠ AUDITAGEM** (amber): variação > 15% (qualquer lado)

### 5.2 Botões de ação

- **+ Marcar Emergencial** → caso comprador esqueceu de marcar
- **✓ Liberar para Estoque** → cadastro libera, nota vai pra `aguardando_estoque`
- **✕ Excluir Nota** → admin pode excluir e pedir reimportação
- **🖨 Vida da Nota** → abre `/nota-historico?id=N` (timeline completa)

> **Status:** `em_validacao_cadastro` → `aguardando_estoque`

---

## 6. Conferência no Estoque

### 6.1 Conferência cega

- **Tela:** `/notas-estoque` (perfil `estoque`)
- Estoque vê a lista de notas liberadas, abre uma e:
  - **Bipa código de barras** (coletor USB ou câmera HTML5).
  - Sistema localiza item correspondente pelo EAN.
  - Estoque digita: **caixas + unidades + validade**.
  - Sistema calcula total e compara com qtd esperada.
- Pode fazer múltiplas rodadas se houver divergência.
- Ao finalizar:
  - Se tudo bate → status `em_auditoria` (cadastro vai validar a NF) ou `fechada` direta (config).
  - Se há divergência → `auditagem` (CD) ou `em_auditoria` (NF-e).

### 6.2 Validade em risco

Após cada item conferido com data de validade:

- Sistema calcula:
  - `estoque_atual` (produtos_externo)
  - `vendas_media_dia` (vendas_historico últimos 60d)
  - `dias_ate_vencer` = validade − hoje
  - `consumo_estimado` = vendas_media × dias_ate_vencer
  - `qtd_em_risco` = max(0, estoque + qtd_recebida − consumo_estimado)
  - `qtd_em_risco_caixas` = ceil(qtd_em_risco / qtd_embalagem) — admin devolve por caixa fechada.
- Se há risco → grava em `validades_em_risco` com status `pendente` e nota fica em `aguardando_admin_validade`.

> **Status:** `em_conferencia` → `em_auditoria` / `auditagem` / `aguardando_admin_validade`

---

## 7. Auditoria Final (NF-e Fornecedor)

### 7.1 Auditoria pelo auditor

- **Tela:** `/notas-auditoria` (perfil `auditor`)
- Auditor revisa divergências, lotes, validades.
- Pode digitar observações por item.
- Botão "✓ Fechar Nota":
  - Se há validade em risco → nota vai pra `aguardando_admin_validade`.
  - Senão → `fechada`.

### 7.2 Decisão admin sobre validade em risco

- **Tela:** `/validades-em-risco` (admin)
- Admin vê cada lote em risco com:
  - Estoque pós-recebimento, vendas/dia, dias até vencer, valor em risco.
  - Motivo: `risco_por_giro`, `sem_historico_vendas`, `ja_vencido`.
- Decide:
  - **Liberado** → nota fecha normal.
  - **Devolução** → cria/agrega registro em `devolucoes`, nota vai pra `aguardando_devolucao`.

### 7.3 Devolução obrigatória

- **Tela:** `/aguardando-devolucao` (cadastro/admin)
- Cadastro emite NF de devolução pro fornecedor no ERP.
- Anexa o XML da devolução (obrigatório) + PDF DANFE (opcional).
- Sistema valida: mesmo CNPJ destinatário, soma confere.
- Notifica vendedor (WhatsApp + email com PDF) automaticamente.
- Após upload, nota vira `fechada`.

> **Status:** `em_auditoria` → `aguardando_admin_validade` → `aguardando_devolucao` → `fechada`

---

## 8. Cobrança automática (acordos comerciais)

### 8.1 Origem

- Repositor da loja vê produto com validade próxima em `/validade-dashboard` (sistema acougue-senhas).
- Clica em "Rebaixar" no produto.
- Sistema consulta origem do produto (`compras_historico`):
  - Se CNPJ é **interno** (loja da rede ou um dos 5 CDs externos cadastrados em `cds_externos`) → **rebaixa interna direta** (sem cobrança).
  - Se CNPJ é **fornecedor externo** → cria **acordo comercial** (`acordos_comerciais`).

### 8.2 Aprovação do acordo

- Sistema notifica todos os compradores ativos por email.
- Comprador entra em `/contas-receber` aba "📝 Acordos Comerciais".
- Vê lista de acordos pendentes (badge com contador).
- Aprova → produto fica com etiqueta vermelha pro repositor (preço rebaixado), watermark de vendas começa.
- Recusa → motivo obrigatório, repositor é informado.

### 8.3 Fechamento do acordo

- Job horário monitora acordos `ativo`:
  - Quando `data_validade < hoje`, fecha acordo automaticamente.
  - Calcula débito = (vendas pós-aprovação até validade, capeado em qtde_acordada) × diferença unitária.
  - Cria registro em `cr_debitos` (sistema jrlira).
  - Notifica vendedor + comprador (email/WhatsApp).

---

## 9. Vida da Nota

Tela `/nota-historico?id=N` mostra **tudo** da nota em formato print-friendly:

- Cabeçalho da nota (fornecedor, valor, datas).
- Pedido B2B vinculado (se houver).
- Timeline com todos os marcos (sync, recebida, liberada, conferida, validada).
- Itens com:
  - Lotes contados pelo estoque (qtd, validade).
  - Divergências detectadas + resoluções.
  - Validades em risco + decisão admin.
- Devoluções vinculadas (com NF, valor, status, vendedor notificado).

**Onde aparece o botão:** `/notas-comprador`, `/notas-cadastro`, `/notas-auditoria`, `/notas-transferencias-cadastro`.

---

## 10. Permissões por perfil

| Tela | admin | comprador | rh | cadastro | estoque | auditor |
|---|---|---|---|---|---|---|
| `/notas-comprador` | ✓ | ✓ | | | | |
| `/notas-cadastro` | ✓ | | | ✓ | | |
| `/notas-estoque` | ✓ | | | | ✓ | |
| `/notas-auditoria` | ✓ | | | | | ✓ |
| `/sugestao-compras` | ✓ | ✓ | | | | |
| `/pedidos-comprador` | ✓ | ✓ | | | | |
| `/auditoria-pedidos` | ✓ | | | | | |
| `/produtos-embalagem` | ✓ | ✓ | | | | |
| `/embalagens-fornecedor` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `/aguardando-devolucao` | ✓ | | | ✓ | | |
| `/validades-em-risco` | ✓ | | | | | |
| `/contas-receber` | ✓ | ✓ | | | | |

**Filtro por loja:** `cadastro`, `estoque`, `auditor` veem **apenas as notas das suas lojas** (campo `lojas` no JWT). `admin`, `rh`, `comprador` veem todas. Usuário com as 6 lojas marcadas vê todas (consequência natural).

---

## 11. Notificações automáticas

### 11.1 Email

- **Pedido validado** → vendedor recebe PDF + texto.
- **Pedido rejeitado** → vendedor.
- **Pedido cancelado pelo vendedor** → comprador.
- **Devolução enviada** → vendedor (com NF e PDF).
- **Acordo aprovado/recusado** → vendedor.
- **Resumo semanal RH/admin** (funcionários em risco/INSS).

### 11.2 WhatsApp (Evolution API)

- Notificação de envio do pedido pro comprador.
- Confirmação de validação pro vendedor.
- Aviso de pedido aguardando auditoria pro admin.
- Aviso de pedido atrasado (sem faturamento).
- Cron horário de alertas pendentes ao admin (validades, devoluções, embalagens).
- **Top semanal de embalagens** (segunda-feira, gamificação).
- Notificação de devolução pro vendedor (com PDF).

---

## 12. Tabelas-chave do banco

### 12.1 Pedido

- `pedidos` — cabeçalho (status, fornecedor, vendedor, loja, valores, condição, validações)
- `itens_pedido` — produtos do pedido (qtd, preço, qtd_validada, preco_validado, justificativa_excesso)

### 12.2 Nota fiscal

- `notas_entrada` — cabeçalho da NF-e (chave, número, série, fornecedor, valor, status, datas dos marcos)
- `itens_nota` — itens com qCom/qTrib/uCom/uTrib, EANs, preços, qtd_por_caixa_nfe, qtd_em_unidades, status_preco
- `conferencias_estoque` + `conferencia_lotes` — contagem por rodada com lotes
- `auditoria_itens` — fechamento por item
- `notas_alertas` — alertas específicos por nota

### 12.3 Embalagens

- `embalagens_fornecedor` — cadastro de qtd-por-caixa por (EAN ou descrição) + CNPJ fornecedor
- `embalagens_fornecedor_log` — auditoria de alterações
- `produtos_embalagem` — cadastro de embalagens do CD (mat_codi)

### 12.4 Validades / acordos

- `validades_em_risco` — análise por giro com decisão admin
- `acordos_comerciais` — solicitações de rebaixa por validade
- `cr_debitos` + `cr_debito_itens` — débitos gerados ao fornecedor
- `devolucoes` + `devolucoes_itens` — devoluções com XML/PDF anexados

---

## 13. Ferramentas auxiliares

### 13.1 Reprocessamento

- `POST /api/notas/reprocessar-qtd-caixa` — re-roda parser de descrição em itens existentes
  - `force=true` recalcula preços de todos
  - `nota_id=N` aplica em uma nota só
- `POST /api/embalagens-fornecedor/aceitar-sugestao-massa` — aceita todas com confiança alta

### 13.2 Diagnóstico

- `GET /api/notas/diag-qtd-caixa?numero=N&serie=S` — vê estado dos campos de qtd/embalagem por item
- `GET /api/sync-status/resumo` — status do sync Pentaho (vendas/compras/produtos)
- `GET /api/sync-status/vendas-detalhe` — diagnóstico detalhado por loja

### 13.3 Cadastro manual de embalagem

- Tela `/embalagens-fornecedor` → botão "+ Cadastrar embalagem"
- Aceita EAN OU (descrição + CNPJ)
- Auto-busca histórico ao digitar EAN
- Após salvar, **recalcula automaticamente** todos os itens já importados desse EAN+fornecedor

---

## 14. Indicadores e relatórios

### 14.1 Cron horário de alertas (admin via WhatsApp)

- Validades em risco pendentes
- Divergências CD pendentes
- Devoluções aguardando XML
- Embalagens fornecedor pendentes
- Embalagens CD ativas sem qtd validada
- SLA de sync atrasado (vendas/compras/produtos por loja)

### 14.2 Gamificação

- Painel de ranking em `/embalagens-fornecedor`:
  - Top 12 por total validado
  - Mês / Semana / Streak (dias consecutivos)
  - Badges: 🌱 Iniciante / 🥉 Bronze (25+) / 🥈 Prata (100+) / 🥇 Ouro (250+) / 💎 Platinum (1000+) / 👑 Lendário (2500+)
- Top semanal por WhatsApp pro admin (segunda-feira de manhã).

---

## 15. Casos extremos / decisões importantes

### 15.1 Item sem EAN

- Cadastro manual em `/embalagens-fornecedor` aceita por descrição + CNPJ.
- Lookup no XML usa descrição normalizada (uppercase, espaços colapsados, sem acentos) + CNPJ.
- Para auto-aplicar em notas existentes, recalcula via mesma chave.

### 15.2 Fornecedor é CD interno

- 5 CNPJs cadastrados em `cds_externos` (CASA BRANCA, ASA FRIOS ITAITUBA/SANTAREM, ATACADAO ASA BRANCA ITAITUBA/NOVO PROGRESSO).
- Quando `validade-dashboard` detecta produto vindo desses CNPJs (ou de qualquer CNPJ de loja interna), trata como **transferência interna** (rebaixa direta, sem cobrança).
- Acordos pendentes criados antes do cadastro foram cancelados (motivo: "Reclassificado: origem é CD interno").

### 15.3 Status_preco — taxonomia

- Aplicada em todo cálculo de preço (importação, reprocessamento, edit linha, etc.).
- Backfill rodou no boot pra reclassificar dados antigos.

---

## 16. Próximos passos (roadmap)

- **Backfill compras/vendas desde 01/01/2025** — atualmente desde 01/07/2025. Vai exigir alteração no Pentaho + carga única.
- **Os 8 Passos** (relatórios pendentes):
  1. Dashboard de acompanhamento de notas (status, valor, paradas)
  2. Relatório de divergências de preço
  3. Relatório de produtos novos
  4. Relatório de emergenciais
  5. SLA do fluxo de notas
  6. Divergências de estoque (C1/C2)
  7. Pedidos por fornecedor/período
  8. Histórico do vendedor no portal

---

**Versão:** 2026-05-04
**Mantenedor:** equipe técnica JR Lira
