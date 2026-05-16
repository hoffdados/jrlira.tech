// Sync de transferências CD→loja do UltraSyst para notas_entrada do JR Lira.
// Roda como cron e via endpoint admin.

const { pool, query: dbQuery } = require('./db');
const ultrasyst = require('./ultrasyst');

const CHAVE_ULTIMO = 'ultrasyst_ultimo_mcp_codi';
const NOPS_ACEITOS = "'031','012'"; // 031=transferência, 012=bonificação
const NATUREZA_POR_NOP = { '031': 'TRANSFERENCIA', '012': 'BONIFICACAO' };
const CD_CNPJ = '17764296000209'; // Atacadão Asa Branca

function limparCnpj(s) {
  return String(s || '').replace(/\D/g, '');
}

async function getUltimoMcpCodi() {
  const r = await dbQuery(`SELECT valor FROM _sync_state WHERE chave=$1`, [CHAVE_ULTIMO]);
  return r[0]?.valor || '0000000';
}

async function setUltimoMcpCodi(mcpCodi) {
  await dbQuery(
    `INSERT INTO _sync_state (chave, valor, atualizado_em) VALUES ($1,$2,NOW())
       ON CONFLICT (chave) DO UPDATE SET valor=EXCLUDED.valor, atualizado_em=NOW()`,
    [CHAVE_ULTIMO, mcpCodi]
  );
}

// Busca lojas cadastradas com CNPJ e mapeia CLI_CODI no UltraSyst (via CLIENTE.CLI_CPF)
async function mapearLojas() {
  const lojas = await dbQuery(
    `SELECT id, nome, cnpj FROM lojas WHERE ativo=TRUE AND cnpj IS NOT NULL AND cnpj <> ''`
  );
  if (!lojas.length) return { porCnpj: {}, porCliCodi: {}, cnpjs: [] };

  const cnpjs = lojas.map(l => limparCnpj(l.cnpj)).filter(Boolean);
  if (!cnpjs.length) return { porCnpj: {}, porCliCodi: {}, cnpjs: [] };

  const lista = cnpjs.map(c => `'${c}'`).join(',');
  const r = await ultrasyst.query(
    `SELECT CLI_CODI, REPLACE(REPLACE(REPLACE(CLI_CPF,'.',''),'-',''),'/','') AS cnpj
       FROM CLIENTE WITH (NOLOCK)
      WHERE REPLACE(REPLACE(REPLACE(CLI_CPF,'.',''),'-',''),'/','') IN (${lista})`
  );
  const ultraPorCnpj = {};
  for (const row of r.rows || []) ultraPorCnpj[row.cnpj] = row.CLI_CODI;

  const porCnpj = {}, porCliCodi = {};
  for (const l of lojas) {
    const cnpj = limparCnpj(l.cnpj);
    const cliCodi = ultraPorCnpj[cnpj];
    if (!cliCodi) continue;
    porCnpj[cnpj] = { ...l, cli_codi: cliCodi };
    porCliCodi[cliCodi] = { ...l, cli_codi: cliCodi };
  }
  return { porCnpj, porCliCodi, cnpjs };
}

// Busca movimentações novas (NOP IN 031/012, FOR_CODI nas lojas, MCP_CODI > último, últimos 24 meses)
async function buscarTransferenciasNovas(cliCodisLojas, ultimoMcpCodi) {
  if (!cliCodisLojas.length) return [];
  const lista = cliCodisLojas.map(c => `'${c}'`).join(',');
  const r = await ultrasyst.query(
    `SELECT TOP 300 m.MCP_CODI, m.FOR_CODI, m.MCP_DTEM, m.MCP_DTEN, m.MCP_DTMV,
            m.MCP_VTOT, m.MCP_STATUS, m.MCP_NNOTAFIS, m.MCP_CHAVENFE,
            m.NOP_CODI, m.MCP_OBSE
       FROM TBMOVCOMPRA m WITH (NOLOCK)
      WHERE m.NOP_CODI IN (${NOPS_ACEITOS})
        AND m.MCP_STATUS <> 'C'
        AND m.MCP_TIPOMOV = 'S'
        AND m.FOR_CODI IN (${lista})
        AND m.MCP_CODI > '${ultimoMcpCodi}'
        AND m.MCP_DTEM >= '2025-07-02'
      ORDER BY m.MCP_CODI ASC`
  );
  return r.rows || [];
}

// Busca itens em batch (todos os MCPs de uma vez).
// EAN: prefere o do item; senão o EAN principal (EAN_NOTA='S'); senão qualquer EAN; por fim MATERIAL.EAN_CODI.
async function buscarItensBatch(empCodi, mcpCodis, mcpTipoMov) {
  if (!mcpCodis.length) return {};
  const lista = mcpCodis.map(c => `'${c}'`).join(',');
  const r = await ultrasyst.query(
    `SELECT i.MCP_CODI, i.MCP_SEQITEM, i.PRO_CODI,
            COALESCE(
              NULLIF(LTRIM(RTRIM(i.EAN_CODI)),''),
              (SELECT TOP 1 LTRIM(RTRIM(EAN_CODI)) FROM EAN WITH (NOLOCK)
                WHERE MAT_CODI = i.PRO_CODI AND EAN_CODI IS NOT NULL AND LTRIM(RTRIM(EAN_CODI)) <> ''
                ORDER BY CASE WHEN EAN_NOTA='S' THEN 0 ELSE 1 END, ID),
              NULLIF(LTRIM(RTRIM(mat.EAN_CODI)),'')
            ) AS ean,
            COALESCE(NULLIF(LTRIM(RTRIM(i.PRO_DESCP)),''), LTRIM(RTRIM(mat.MAT_DESC))) AS descricao,
            i.MCP_QUAN, i.MCP_VUNI, i.LOT_SEQU
       FROM TBITEMCOMPRA i WITH (NOLOCK)
       LEFT JOIN MATERIAL mat WITH (NOLOCK) ON mat.MAT_CODI = i.PRO_CODI
      WHERE i.EMP_CODI = '${empCodi}'
        AND i.MCP_TIPOMOV = '${mcpTipoMov}'
        AND i.MCP_CODI IN (${lista})
      ORDER BY i.MCP_CODI, i.MCP_SEQITEM`
  );
  const porMcp = {};
  for (const row of r.rows || []) {
    if (!porMcp[row.MCP_CODI]) porMcp[row.MCP_CODI] = [];
    porMcp[row.MCP_CODI].push(row);
  }
  return porMcp;
}

// Insere uma transferência como nota_entrada + itens_nota (bulk via UNNEST).
// Sempre cria como em_transito; mudanças de status acontecem via fluxo manual da loja
// ou via matchTransferenciasRecebidas (auto-validar quando bater compras_historico).
async function inserirTransferencia(client, mov, lojaInfo, itens) {
  const dataEmissao = mov.MCP_DTEM ? new Date(mov.MCP_DTEM).toISOString().slice(0, 10) : null;

  const natureza = NATUREZA_POR_NOP[mov.NOP_CODI] || 'TRANSFERENCIA';
  const ins = await client.query(
    `INSERT INTO notas_entrada
        (chave_nfe, numero_nota, serie, fornecedor_nome, fornecedor_cnpj,
         data_emissao, valor_total, status, importado_por, loja_id,
         origem, cd_mov_codi, cd_loja_cli_codi, cd_synced_em, origem_cd_codigo,
         natureza_op)
       VALUES ($1,$2,$3,$4,$5,$6,$7,'em_transito',$8,$9,'cd',$10,$11,NOW(),'srv1-itautuba',$12)
       ON CONFLICT (origem_cd_codigo, cd_mov_codi)
         WHERE origem_cd_codigo IS NOT NULL AND cd_mov_codi IS NOT NULL
         DO NOTHING
       RETURNING id`,
    [
      null, mov.MCP_CODI, null,
      'CD - Centro de Distribuição', CD_CNPJ,
      dataEmissao, mov.MCP_VTOT || 0,
      'sync_ultrasyst', lojaInfo.id,
      mov.MCP_CODI, lojaInfo.cli_codi,
      natureza,
    ]
  );
  if (!ins.rows.length) return null;
  const notaId = ins.rows[0].id;
  if (!itens.length) return notaId;

  // Bulk insert dos itens via UNNEST
  const nota_id   = itens.map(() => notaId);
  const numero    = itens.map(i => Math.floor(i.MCP_SEQITEM || 0));
  const proCodi   = itens.map(i => (i.PRO_CODI || '').trim() || null);
  const ean       = itens.map(i => ((i.ean || '').trim() || null));
  const desc      = itens.map(i => ((i.descricao || '').trim() || null));
  const qtd       = itens.map(i => i.MCP_QUAN || 0);
  const vuni      = itens.map(i => i.MCP_VUNI || 0);
  const vtot      = itens.map(i => (i.MCP_QUAN || 0) * (i.MCP_VUNI || 0));
  const semCod    = itens.map(i => {
    const e = (i.ean || '').replace(/\D/g, '').replace(/^0+/, '');
    return !e; // null/vazio/zeros
  });

  await client.query(
    `INSERT INTO itens_nota
        (nota_id, numero_item, cd_pro_codi, ean_nota, descricao_nota,
         quantidade, preco_unitario_nota, preco_total_nota, produto_novo, sem_codigo_barras)
       SELECT * FROM UNNEST(
         $1::int[], $2::int[], $3::text[], $4::text[], $5::text[],
         $6::numeric[], $7::numeric[], $8::numeric[],
         ARRAY_FILL(FALSE, ARRAY[array_length($1,1)]), $9::bool[]
       )`,
    [nota_id, numero, proCodi, ean, desc, qtd, vuni, vtot, semCod]
  );

  // Cruza com produtos_externo da loja destino: se houver match por EAN, popula
  // custo_fabrica/status_preco/ean_validado pra não cair como 'sem_cadastro'.
  await client.query(`
    UPDATE itens_nota i
       SET custo_fabrica = pe.custoorigem,
           ean_validado = NULLIF(LTRIM(COALESCE(i.ean_nota,''),'0'),''),
           ean_fonte = 'ean_nota',
           produto_novo = FALSE,
           status_preco = CASE
             WHEN pe.custoorigem IS NULL OR i.preco_unitario_nota IS NULL OR i.preco_unitario_nota <= 0 THEN 'sem_cadastro'
             WHEN ABS(i.preco_unitario_nota - pe.custoorigem) <= 0.01 THEN 'igual'
             WHEN ABS(i.preco_unitario_nota - pe.custoorigem) > pe.custoorigem * 0.15 THEN 'auditagem'
             WHEN i.preco_unitario_nota > pe.custoorigem THEN 'maior'
             ELSE 'menor'
           END
      FROM produtos_externo pe
     WHERE i.nota_id = $1
       AND pe.loja_id = $2
       AND NULLIF(LTRIM(pe.codigobarra,'0'),'') = NULLIF(LTRIM(COALESCE(i.ean_nota,''),'0'),'')
       AND NULLIF(LTRIM(COALESCE(i.ean_nota,''),'0'),'') IS NOT NULL
  `, [notaId, lojaInfo.id]);

  // Cria alertas de embalagem pendente: pra cada PRO_CODI deste item que NÃO esteja
  // 'validado' em produtos_embalagem, cria alerta vinculando a nota.
  const proCodisDistintos = [...new Set(proCodi.filter(Boolean))];
  if (proCodisDistintos.length) {
    await client.query(
      `INSERT INTO alertas_admin (tipo, entidade, entidade_id, titulo, mensagem)
         SELECT 'embalagem_pendente_em_transferencia', 'mat_codi', m.mat_codi,
                'Transferência com embalagem pendente — ' || m.mat_codi,
                'Nota CD ' || $2 || ' (loja ' || $3 || ') contém produto ' || m.mat_codi
                  || ' sem embalagem validada. Validar em /produtos-embalagem'
           FROM UNNEST($1::text[]) AS m(mat_codi)
          WHERE NOT EXISTS (
            SELECT 1 FROM produtos_embalagem pe
             WHERE pe.mat_codi = m.mat_codi AND pe.status = 'validado'
          )
         ON CONFLICT (tipo, entidade, entidade_id) WHERE resolvido_em IS NULL DO NOTHING`,
      [proCodisDistintos, mov.MCP_CODI, lojaInfo.id]
    );
  }

  return notaId;
}

// Detecta produtos novos no CD (em MATERIAL ATIVO mas não em produtos_embalagem),
// insere como pendente e cria alerta. Roda no cron junto com o sync de transferências.
async function detectarProdutosNovosCD() {
  const t0 = Date.now();
  const r = await ultrasyst.query(
    `SELECT MAT_CODI, MAT_DESC, EAN_CODI
       FROM MATERIAL WITH (NOLOCK)
      WHERE MAT_SITU = 'A'`
  );
  const todosCD = r.rows || [];
  if (!todosCD.length) return { novos: 0, ms: Date.now() - t0 };

  const matCodis = todosCD.map(m => String(m.MAT_CODI || '').trim()).filter(Boolean);
  if (!matCodis.length) return { novos: 0, ms: Date.now() - t0 };
  // Quais já existem em produtos_embalagem?
  const existentes = await dbQuery(
    `SELECT mat_codi FROM produtos_embalagem WHERE mat_codi = ANY($1::text[])`,
    [matCodis]
  );
  const setExistentes = new Set(existentes.map(e => e.mat_codi));
  const novos = todosCD.filter(m => !setExistentes.has(m.MAT_CODI));
  if (!novos.length) return { novos: 0, ms: Date.now() - t0 };

  // Roda parser pra cada
  const { parseEmbalagem } = require('./parser_embalagem');
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const p of novos) {
      const matCodi = String(p.MAT_CODI || '').trim();
      if (!matCodi) continue;
      const desc = (p.MAT_DESC || '').trim();
      const eanCD = (p.EAN_CODI || '').trim() || null;
      const { qtd, confianca } = parseEmbalagem(desc);
      await client.query(
        `INSERT INTO produtos_embalagem
            (mat_codi, descricao_atual, qtd_sugerida, confianca_parser, status,
             ativo_no_cd, ean_principal_cd, ean_cd_synced_em, criado_em, atualizado_em)
           VALUES ($1::text, $2::text, $3::int, $4::text, 'pendente_validacao', TRUE, $5::text, NOW(), NOW(), NOW())
           ON CONFLICT (mat_codi) DO NOTHING`,
        [matCodi, desc || null, qtd, confianca, eanCD]
      );
      await client.query(
        `INSERT INTO alertas_admin (tipo, entidade, entidade_id, titulo, mensagem)
           VALUES ('produto_novo_cd', 'mat_codi', $1::text,
                   'Produto novo do CD: ' || $1::text,
                   'Validar embalagem e EAN em /produtos-embalagem (descrição: ' || COALESCE($2::text,'sem desc') || ')')
           ON CONFLICT (tipo, entidade, entidade_id) WHERE resolvido_em IS NULL DO NOTHING`,
        [matCodi, desc]
      );
    }
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    throw e;
  } finally { client.release(); }

  console.log(`[ultrasyst novos] ${novos.length} produto(s) novo(s) detectado(s) em ${Date.now() - t0}ms`);
  return { novos: novos.length, ms: Date.now() - t0 };
}

async function syncTransferenciasCD() {
  const t0 = Date.now();
  const stats = { inseridas: 0, ignoradas: 0, total: 0, erros: 0, ultimo: null };

  let mapas;
  try { mapas = await mapearLojas(); }
  catch (e) {
    console.error('[ultrasyst sync] erro mapear lojas:', e.message);
    throw e;
  }
  const cliCodis = Object.keys(mapas.porCliCodi);
  if (!cliCodis.length) {
    console.warn('[ultrasyst sync] nenhuma loja mapeada — verifique cnpj na tabela lojas');
    return stats;
  }

  const ultimo = await getUltimoMcpCodi();
  let movs;
  try { movs = await buscarTransferenciasNovas(cliCodis, ultimo); }
  catch (e) {
    console.error('[ultrasyst sync] erro buscar movs:', e.message);
    throw e;
  }
  stats.total = movs.length;
  if (!movs.length) {
    console.log(`[ultrasyst sync] sem novas transferências (últ=${ultimo})`);
    return stats;
  }

  // Busca itens de TODAS as movs em uma chamada só
  let itensPorMcp = {};
  try {
    itensPorMcp = await buscarItensBatch('001', movs.map(m => m.MCP_CODI), 'S');
  } catch (e) {
    console.error('[ultrasyst sync] erro buscar itens batch:', e.message);
    throw e;
  }

  for (const mov of movs) {
    const lojaInfo = mapas.porCliCodi[mov.FOR_CODI];
    if (!lojaInfo) { stats.ignoradas++; continue; }

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const itens = itensPorMcp[mov.MCP_CODI] || [];
      const notaId = await inserirTransferencia(client, mov, lojaInfo, itens);
      await client.query('COMMIT');
      if (notaId) stats.inseridas++; else stats.ignoradas++;
      stats.ultimo = mov.MCP_CODI;
      await setUltimoMcpCodi(mov.MCP_CODI);
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      stats.erros++;
      console.error(`[ultrasyst sync] erro mov ${mov.MCP_CODI}:`, e.message);
    } finally {
      client.release();
    }
  }

  console.log(`[ultrasyst sync] ${stats.inseridas}/${stats.total} novas em ${Date.now()-t0}ms (últ=${stats.ultimo})`);
  return stats;
}

// Match auto-validar: se a nota está em compras_historico (já validada no Ecocentauro),
// passa direto pra 'validada'. Quando vier de em_transito (loja não passou pelo fluxo
// do app), cria alerta pra cadastro/admin atualizar o JRLira Tech.
async function matchTransferenciasRecebidas() {
  const t0 = Date.now();

  // 1) Identifica notas que casaram em compras_historico mas ainda não estão validadas.
  // Normaliza ambos os lados (zeros à esquerda) — operadores digitam de formas diferentes.
  const candidatas = await dbQuery(
    `SELECT n.id AS nota_id, n.status AS status_anterior, MIN(c.data_entrada) AS data_entrada
       FROM notas_entrada n
       JOIN compras_historico c
         ON c.loja_id = n.loja_id
        AND c.fornecedor_cnpj = $1
        AND REGEXP_REPLACE(c.numeronfe, '^0+', '') = REGEXP_REPLACE(n.cd_mov_codi, '^0+', '')
      WHERE n.origem = 'cd'
        AND n.status <> 'validada'
      GROUP BY n.id, n.status`,
    [CD_CNPJ]
  );
  if (!candidatas.length) {
    const ms = Date.now() - t0;
    console.log(`[ultrasyst match] nenhuma nota a validar em ${ms}ms`);
    return { validadas: 0, alertas: 0, ms };
  }

  const ids = candidatas.map(c => c.nota_id);
  const idsAlerta = candidatas
    .filter(c => c.status_anterior === 'em_transito')
    .map(c => c.nota_id);

  await dbQuery(
    `UPDATE notas_entrada n
        SET status='validada', data_recebimento=ch.data_entrada, validada_em=NOW()
       FROM (
         SELECT id AS nota_id, data_entrada
           FROM UNNEST($1::int[], $2::date[]) AS t(id, data_entrada)
       ) ch
      WHERE n.id = ch.nota_id`,
    [ids, candidatas.map(c => c.data_entrada)]
  );

  let alertasCriados = 0;
  if (idsAlerta.length) {
    const r = await dbQuery(
      `INSERT INTO notas_alertas (nota_id, tipo, mensagem)
         SELECT id, 'validada_no_ecocentauro_pulou_fluxo',
                'Nota validada no Ecocentauro sem passar pelo fluxo do app (recebida → conferência → validação)'
           FROM UNNEST($1::int[]) AS t(id)
         ON CONFLICT (nota_id, tipo) WHERE lido_em IS NULL DO NOTHING
         RETURNING id`,
      [idsAlerta]
    );
    alertasCriados = r.length;
  }

  const ms = Date.now() - t0;
  console.log(`[ultrasyst match] validadas: ${ids.length}, alertas: ${alertasCriados} em ${ms}ms`);
  return { validadas: ids.length, alertas: alertasCriados, ms };
}

// Re-sincroniza notas que estão ABERTAS no CD (mcp_status='A') e ainda em fluxo
// editável no app (em_transito ou recebida). Detecta edições do CD após emissão inicial.
async function ressincronizarTransferenciasAbertas() {
  const t0 = Date.now();
  const stats = { verificadas: 0, ressincronizadas: 0, alertadas: 0, sem_diff: 0, erros: 0 };

  // Pega notas candidatas: origem=cd, em_transito ou recebida, fornecedor=CD legado
  const candidatas = await dbQuery(
    `SELECT id, cd_mov_codi, status, valor_total
       FROM notas_entrada
      WHERE origem = 'cd'
        AND fornecedor_cnpj = $1
        AND status IN ('em_transito','recebida')
        AND cd_mov_codi IS NOT NULL
      ORDER BY id DESC LIMIT 500`, [CD_CNPJ]);
  if (!candidatas.length) return stats;
  stats.verificadas = candidatas.length;

  const mcpCodis = candidatas.map(c => c.cd_mov_codi);
  // Busca header + itens em batch (evita N queries)
  const headerSql = `
    SELECT MCP_CODI, MCP_DTEM, MCP_VTOT, MCP_STATUS
      FROM TBMOVCOMPRA WITH (NOLOCK)
     WHERE EMP_CODI='001' AND MCP_TIPOMOV='S'
       AND MCP_CODI IN (${mcpCodis.map(c => `'${c}'`).join(',')})`;
  const headers = await ultrasyst.query(headerSql);
  const headersByMcp = new Map((headers.rows || []).map(h => [String(h.MCP_CODI).trim(), h]));

  let itensPorMcp = {};
  try { itensPorMcp = await buscarItensBatch('001', mcpCodis, 'S'); } catch (e) { console.error('[ressync itens batch]', e.message); }

  for (const c of candidatas) {
    try {
      const mcpStr = String(c.cd_mov_codi).trim();
      const h = headersByMcp.get(mcpStr);
      if (!h) continue; // não achou no UltraSyst — pode ter sido excluída
      const itensNovos = itensPorMcp[mcpStr] || [];
      const novoValor = parseFloat(h.MCP_VTOT || 0);
      const valorAtual = parseFloat(c.valor_total || 0);

      // Conta itens que tem hoje no app
      const r = await dbQuery(`SELECT COUNT(*)::int AS n FROM itens_nota WHERE nota_id = $1`, [c.id]);
      const nAtual = r[0]?.n || 0;
      const nNovo  = itensNovos.length;

      const diffValor = Math.abs(novoValor - valorAtual) > 0.01;
      const diffItens = nAtual !== nNovo;
      if (!diffValor && !diffItens) { stats.sem_diff++; continue; }

      // Tem diferença. Re-sync (em_transito e recebida — ainda dentro do fluxo "antes de conferir")
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        await client.query(`DELETE FROM itens_nota WHERE nota_id = $1`, [c.id]);
        await client.query(
          `UPDATE notas_entrada
              SET valor_total = $2,
                  cd_synced_em = NOW()
            WHERE id = $1`,
          [c.id, novoValor]
        );
        // Reinsere itens (mesma lógica de inserirTransferencia)
        if (itensNovos.length) {
          const nota_id = itensNovos.map(() => c.id);
          const numero  = itensNovos.map(i => Math.floor(i.MCP_SEQITEM || 0));
          const proCodi = itensNovos.map(i => (i.PRO_CODI || '').trim() || null);
          const ean     = itensNovos.map(i => ((i.ean || '').trim() || null));
          const desc    = itensNovos.map(i => ((i.descricao || '').trim() || null));
          const qtd     = itensNovos.map(i => i.MCP_QUAN || 0);
          const vuni    = itensNovos.map(i => i.MCP_VUNI || 0);
          const vtot    = itensNovos.map(i => (i.MCP_QUAN || 0) * (i.MCP_VUNI || 0));
          const semCod  = itensNovos.map(i => {
            const e = (i.ean || '').replace(/\D/g, '').replace(/^0+/, '');
            return !e;
          });
          await client.query(
            `INSERT INTO itens_nota
                (nota_id, numero_item, cd_pro_codi, ean_nota, descricao_nota,
                 quantidade, preco_unitario_nota, preco_total_nota, produto_novo, sem_codigo_barras)
               SELECT * FROM UNNEST(
                 $1::int[], $2::int[], $3::text[], $4::text[], $5::text[],
                 $6::numeric[], $7::numeric[], $8::numeric[],
                 ARRAY_FILL(FALSE, ARRAY[array_length($1,1)]), $9::bool[]
               )`,
            [nota_id, numero, proCodi, ean, desc, qtd, vuni, vtot, semCod]
          );
        }
        // Alerta admin sobre o re-sync
        await client.query(
          `INSERT INTO alertas_admin (tipo, entidade, entidade_id, titulo, mensagem)
           VALUES ('cd_nota_editada','nota',$1,
                   'Nota CD editada após emissão',
                   $2)`,
          [c.id, `Nota MCP ${c.cd_mov_codi} (status app=${c.status}) foi RE-SINCRONIZADA. Antes: ${nAtual} itens / R$ ${valorAtual.toFixed(2)}. Agora: ${nNovo} itens / R$ ${novoValor.toFixed(2)}.`]
        );
        await client.query('COMMIT');
        stats.ressincronizadas++;
      } catch (e) {
        await client.query('ROLLBACK').catch(() => {});
        stats.erros++;
        console.error(`[ressync ${c.cd_mov_codi}]`, e.message);
      } finally {
        client.release();
      }
    } catch (e) {
      stats.erros++;
      console.error(`[ressync nota ${c.id}]`, e.message);
    }
  }
  stats.ms = Date.now() - t0;
  console.log(`[ressync_transferencias] verif=${stats.verificadas} ressync=${stats.ressincronizadas} sem_diff=${stats.sem_diff} erros=${stats.erros} em ${stats.ms}ms`);
  return stats;
}

module.exports = { syncTransferenciasCD, mapearLojas, matchTransferenciasRecebidas, detectarProdutosNovosCD, ressincronizarTransferenciasAbertas };
