// Sync de transferências dos OUTROS CDs (NP, AsaFrio, AsaSantarem) → lojas ou outros CDs.
// NÃO toca no sync_ultrasyst.js legado (que continua cuidando de ITB → 6 lojas).
//
// Pra cada CD ativo (exceto srv1-itautuba):
//   1. Pega saídas TBMOVCOMPRA com NOP=031, status<>'C', MCP_TIPOMOV='S'
//   2. Filtra FOR_CODI in (cli_codis de LOJAS + outros CDs)
//   3. Pra cada nota, identifica destino:
//      - LOJA (lojas.cnpj) → cria notas_entrada com loja_id
//      - CD (pedidos_distrib_destinos.cnpj tipo='CD') → cria com cd_destino_codigo
//   4. origem_cd_codigo = CD origem (srv2-asafrio etc)

const { pool, query: dbQuery } = require('./db');
const { listarCds, clientePorCodigo } = require('./cds');

const NOPS_ACEITOS = "'031','012'"; // 031=transferência, 012=bonificação
const NATUREZA_POR_NOP = { '031': 'TRANSFERENCIA', '012': 'BONIFICACAO' };
const CD_LEGADO = 'srv1-itautuba'; // exclui do loop pra não conflitar com sync_ultrasyst.js

function limparCnpj(s) { return String(s || '').replace(/\D/g, ''); }

async function getUltimoMcpCodi(cdCodigo) {
  const r = await dbQuery(
    `SELECT valor FROM _sync_state WHERE chave=$1`,
    [`transf_multi_${cdCodigo}_ultimo_mcp_codi`]);
  return r[0]?.valor || '0000000';
}
async function setUltimoMcpCodi(cdCodigo, mcpCodi) {
  await dbQuery(
    `INSERT INTO _sync_state (chave, valor, atualizado_em) VALUES ($1,$2,NOW())
       ON CONFLICT (chave) DO UPDATE SET valor=EXCLUDED.valor, atualizado_em=NOW()`,
    [`transf_multi_${cdCodigo}_ultimo_mcp_codi`, mcpCodi]);
}

// Mapeia destinos possíveis: LOJAS + CDs do grupo (do pedidos_distrib_destinos).
// Pra cada destino, descobre o cli_codi no UltraSyst do CD origem (via CLIENTE.CLI_CPF).
// apenasCD=true → só destinos CD (usado pelo ITB, cujas lojas são tratadas pelo legado sync_ultrasyst.js).
async function mapearDestinos(cli, cdOrigemCodigo, apenasCD = false) {
  const filtroTipo = apenasCD ? `tipo='CD' AND cd_codigo <> $1`
                              : `tipo='LOJA' OR (tipo='CD' AND cd_codigo <> $1)`;
  const destinos = await dbQuery(
    `SELECT tipo, codigo, nome, cnpj, loja_id, cd_codigo
       FROM pedidos_distrib_destinos
      WHERE ativo = TRUE AND cnpj IS NOT NULL AND cnpj <> ''
        AND (${filtroTipo})`,
    [cdOrigemCodigo]);
  if (!destinos.length) return { porCliCodi: {}, cnpjs: [] };

  const cnpjs = destinos.map(d => limparCnpj(d.cnpj)).filter(Boolean);
  if (!cnpjs.length) return { porCliCodi: {}, cnpjs: [] };

  // Busca cli_codi de cada CNPJ no UltraSyst do CD origem
  const lista = cnpjs.map(c => `'${c}'`).join(',');
  const r = await cli.query(
    `SELECT CLI_CODI, REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(CLI_CPF)),'.',''),'/',''),'-','') AS cnpj
       FROM CLIENTE WITH (NOLOCK)
      WHERE REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(CLI_CPF)),'.',''),'/',''),'-','') IN (${lista})`);

  const ultraPorCnpj = {};
  for (const row of r.rows || []) ultraPorCnpj[row.cnpj] = row.CLI_CODI;

  const porCliCodi = {};
  for (const d of destinos) {
    const cnpj = limparCnpj(d.cnpj);
    const cliCodi = ultraPorCnpj[cnpj];
    if (!cliCodi) continue;
    porCliCodi[cliCodi] = { ...d, cli_codi: cliCodi };
  }
  return { porCliCodi, cnpjs };
}

async function buscarTransferenciasNovas(cli, cliCodis, ultimoMcpCodi, top = 500) {
  if (!cliCodis.length) return [];
  const lista = cliCodis.map(c => `'${c}'`).join(',');
  const r = await cli.query(
    `SELECT TOP ${top} m.MCP_CODI, m.FOR_CODI, m.MCP_DTEM, m.MCP_DTEN, m.MCP_DTMV,
            m.MCP_VTOT, m.MCP_STATUS, m.MCP_NNOTAFIS, m.MCP_CHAVENFE,
            m.NOP_CODI, m.MCP_OBSE
       FROM TBMOVCOMPRA m WITH (NOLOCK)
      WHERE m.NOP_CODI IN (${NOPS_ACEITOS})
        AND m.MCP_STATUS <> 'C'
        AND m.MCP_TIPOMOV = 'S'
        AND m.FOR_CODI IN (${lista})
        AND m.MCP_CODI > '${ultimoMcpCodi}'
        AND m.MCP_DTEM >= '2025-07-02'
      ORDER BY m.MCP_CODI ASC`);
  return r.rows || [];
}

async function buscarItensBatch(cli, empCodi, mcpCodis, mcpTipoMov) {
  if (!mcpCodis.length) return {};
  const lista = mcpCodis.map(c => `'${c}'`).join(',');
  const r = await cli.query(
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
      ORDER BY i.MCP_CODI, i.MCP_SEQITEM`);
  const porMcp = {};
  for (const row of r.rows || []) {
    if (!porMcp[row.MCP_CODI]) porMcp[row.MCP_CODI] = [];
    porMcp[row.MCP_CODI].push(row);
  }
  return porMcp;
}

async function inserirTransferencia(client, mov, destinoInfo, cdOrigem, cdNomeOrigem, itens) {
  const dataEmissao = mov.MCP_DTEM ? new Date(mov.MCP_DTEM).toISOString().slice(0, 10) : null;

  // Determinar loja_id e cd_destino_codigo conforme tipo do destino
  const lojaId = destinoInfo.tipo === 'LOJA' ? destinoInfo.loja_id : null;
  const cdDestino = destinoInfo.tipo === 'CD' ? destinoInfo.cd_codigo : null;

  // Verifica se já existe (uniq por (origem_cd_codigo, cd_mov_codi))
  const existe = await client.query(
    `SELECT id FROM notas_entrada
      WHERE origem_cd_codigo = $1 AND cd_mov_codi = $2 LIMIT 1`,
    [cdOrigem, mov.MCP_CODI]);
  if (existe.rows.length) return null;
  const natureza = NATUREZA_POR_NOP[mov.NOP_CODI] || 'TRANSFERENCIA';
  const ins = await client.query(
    `INSERT INTO notas_entrada
        (chave_nfe, numero_nota, serie, fornecedor_nome, fornecedor_cnpj,
         data_emissao, valor_total, status, importado_por, loja_id,
         origem, cd_mov_codi, cd_loja_cli_codi, cd_synced_em,
         origem_cd_codigo, cd_destino_codigo, natureza_op)
       VALUES ($1,$2,$3,$4,$5,$6,$7,'em_transito',$8,$9,'cd',$10,$11,NOW(),$12,$13,$14)
       RETURNING id`,
    [
      null, mov.MCP_CODI, null,
      cdNomeOrigem, '', // fornecedor_cnpj: opcional aqui (CD origem)
      dataEmissao, mov.MCP_VTOT || 0,
      'sync_transf_multi', lojaId,
      mov.MCP_CODI, destinoInfo.cli_codi,
      cdOrigem, cdDestino, natureza,
    ]);
  if (!ins.rows.length) return null;
  const notaId = ins.rows[0].id;
  if (!itens.length) return notaId;

  const nota_id = itens.map(() => notaId);
  const numero  = itens.map(i => Math.floor(i.MCP_SEQITEM || 0));
  const proCodi = itens.map(i => (i.PRO_CODI || '').trim() || null);
  const ean     = itens.map(i => ((i.ean || '').trim() || null));
  const desc    = itens.map(i => ((i.descricao || '').trim() || null));
  const qtd     = itens.map(i => i.MCP_QUAN || 0);
  const vuni    = itens.map(i => i.MCP_VUNI || 0);
  const vtot    = itens.map(i => (i.MCP_QUAN || 0) * (i.MCP_VUNI || 0));
  const semCod  = itens.map(i => {
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
    [nota_id, numero, proCodi, ean, desc, qtd, vuni, vtot, semCod]);

  // Cross-cadastro com produtos_externo SÓ se destino é loja
  if (lojaId) {
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
         AND NULLIF(LTRIM(COALESCE(i.ean_nota,''),'0'),'') IS NOT NULL`,
      [notaId, lojaId]);
  }
  return notaId;
}

// Sync de UM CD origem. Pra ITB (legado), passa apenasCD=true (lojas já tratadas pelo sync_ultrasyst).
async function syncCdOrigem(cd) {
  const t0 = Date.now();
  const cli = await clientePorCodigo(cd.codigo);
  const apenasCD = cd.codigo === CD_LEGADO;
  const destinos = await mapearDestinos(cli, cd.codigo, apenasCD);
  const cliCodis = Object.keys(destinos.porCliCodi);
  if (!cliCodis.length) return { cd: cd.codigo, importadas: 0, motivo: 'sem destinos mapeados', ms: Date.now() - t0 };

  const ultimoMcpCodi = await getUltimoMcpCodi(cd.codigo);
  const movs = await buscarTransferenciasNovas(cli, cliCodis, ultimoMcpCodi);
  if (!movs.length) return { cd: cd.codigo, importadas: 0, ms: Date.now() - t0 };

  const itensPorMcp = await buscarItensBatch(cli, cd.emp_codi || '001', movs.map(m => m.MCP_CODI), 'S');

  // Transação POR NOTA: 1 erro não rolba o batch.
  let importadas = 0;
  let pulou = 0;
  let erros = 0;
  let ultimoErroMsg = null;
  let ultimoErroMcp = null;
  let maiorMcp = ultimoMcpCodi;
  for (const mov of movs) {
    const destinoInfo = destinos.porCliCodi[mov.FOR_CODI];
    if (!destinoInfo) { pulou++; continue; }
    const itens = itensPorMcp[mov.MCP_CODI] || [];
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const notaId = await inserirTransferencia(client, mov, destinoInfo, cd.codigo, cd.nome, itens);
      await client.query('COMMIT');
      if (notaId) importadas++;
      else pulou++;
      if (mov.MCP_CODI > maiorMcp) maiorMcp = mov.MCP_CODI;
    } catch (e) {
      try { await client.query('ROLLBACK'); } catch {}
      erros++;
      ultimoErroMsg = e.message;
      ultimoErroMcp = mov.MCP_CODI;
      console.error(`[sync_transf_multi ${cd.codigo} mcp=${mov.MCP_CODI}]`, e.message);
      // Persiste erro pra investigação posterior
      try {
        await dbQuery(
          `INSERT INTO sync_transf_multi_erros (cd_codigo, mcp_codi, for_codi, erro) VALUES ($1,$2,$3,$4)`,
          [cd.codigo, mov.MCP_CODI, mov.FOR_CODI, e.message.slice(0, 500)]);
      } catch {}
      if (mov.MCP_CODI > maiorMcp) maiorMcp = mov.MCP_CODI;
    } finally {
      client.release();
    }
  }
  await setUltimoMcpCodi(cd.codigo, maiorMcp);
  return {
    cd: cd.codigo, importadas, pulou, erros, candidatas: movs.length, ms: Date.now() - t0,
    ultimo_erro: ultimoErroMsg, ultimo_erro_mcp: ultimoErroMcp,
  };
}

// Roda em TODOS os CDs ativos. Pro legado (ITB), syncCdOrigem só pega destinos CD.
async function syncTransferenciasMulti() {
  const cds = await listarCds(true);
  const out = [];
  for (const cd of cds) {
    if (!cd.url || !cd.token) {
      out.push({ cd: cd.codigo, erro: 'sem url/token' });
      continue;
    }
    try {
      const r = await syncCdOrigem(cd);
      out.push(r);
    } catch (e) {
      console.error(`[sync_transf_multi ${cd.codigo}]`, e.message);
      out.push({ cd: cd.codigo, erro: e.message });
    }
  }
  return out;
}

// Roda CD por CD em loop até a batch retornar candidatas < 500 (esgotou) ou bater MAX_LOOPS
async function syncTransferenciasMultiCompleto({ maxLoopsPorCd = 20 } = {}) {
  const cds = await listarCds(true);
  const out = [];
  for (const cd of cds) {
    if (!cd.url || !cd.token) {
      out.push({ cd: cd.codigo, erro: 'sem url/token' });
      continue;
    }
    let totalImp = 0, totalErros = 0, totalPulou = 0, loops = 0;
    let ultimoErroMsg = null;
    for (let i = 0; i < maxLoopsPorCd; i++) {
      try {
        const r = await syncCdOrigem(cd);
        loops++;
        totalImp += r.importadas || 0;
        totalErros += r.erros || 0;
        totalPulou += r.pulou || 0;
        if (r.ultimo_erro) ultimoErroMsg = r.ultimo_erro;
        // Esgotou (candidatas < 500 = lote menor que o limite)
        if ((r.candidatas || 0) < 500) break;
      } catch (e) {
        console.error(`[sync_transf_multi_completo ${cd.codigo}]`, e.message);
        break;
      }
    }
    out.push({ cd: cd.codigo, loops, importadas: totalImp, erros: totalErros, pulou: totalPulou, ultimo_erro: ultimoErroMsg });
  }
  return out;
}

// Match com compras_historico: detecta transferências que JÁ entraram no Ecocentauro
// da loja destino. Marca como `recebida`. Loop por (cd_origem, loja) pra evitar timeout.
async function matchTransferenciasMultiRecebidas() {
  const t0 = Date.now();
  // 1) Lista CDs (exceto ITB) com seus CNPJs normalizados
  const cds = await dbQuery(
    `SELECT cd_codigo, REGEXP_REPLACE(COALESCE(cnpj,''),'\\D','','g') AS cnpj_n
       FROM pedidos_distrib_destinos
      WHERE tipo='CD' AND cd_codigo <> 'srv1-itautuba'
        AND cd_codigo IS NOT NULL AND cnpj IS NOT NULL`);
  let totalRecebidas = 0;
  // 2) Pra cada (cd, loja), pega notas em_transito e busca em compras_historico
  for (const cd of cds) {
    if (!cd.cnpj_n) continue;
    // Acha lojas que tem notas em_transito desse CD
    const lojas = await dbQuery(
      `SELECT DISTINCT loja_id FROM notas_entrada
        WHERE origem_cd_codigo = $1 AND loja_id IS NOT NULL AND status = 'em_transito'`,
      [cd.cd_codigo]);
    for (const l of lojas) {
      const rows = await dbQuery(
        `WITH notas_pendentes AS (
           SELECT id, cd_mov_codi, REGEXP_REPLACE(cd_mov_codi,'^0+','') AS num_norm
             FROM notas_entrada
            WHERE origem_cd_codigo = $1 AND loja_id = $2 AND status = 'em_transito'
              AND cd_mov_codi IS NOT NULL
         )
         SELECT n.id AS nota_id, MIN(c.data_entrada) AS data_entrada
           FROM notas_pendentes n
           JOIN compras_historico c
             ON c.loja_id = $2
            AND REGEXP_REPLACE(c.numeronfe,'^0+','') = n.num_norm
            AND REGEXP_REPLACE(COALESCE(c.fornecedor_cnpj,''),'\\D','','g') = $3
          GROUP BY n.id`,
        [cd.cd_codigo, l.loja_id, cd.cnpj_n]);
      if (!rows.length) continue;
      await dbQuery(
        `UPDATE notas_entrada n
            SET status = 'recebida', data_recebimento = ch.data_entrada, recebida_em = NOW()
           FROM (SELECT id AS nota_id, data_entrada FROM UNNEST($1::int[], $2::date[]) AS t(id, data_entrada)) ch
          WHERE n.id = ch.nota_id`,
        [rows.map(r => r.nota_id), rows.map(r => r.data_entrada)]);
      totalRecebidas += rows.length;
    }
  }
  return { recebidas: totalRecebidas, ms: Date.now() - t0 };
}

// Ressincroniza status das transferências MULTI no CD origem: detecta canceladas (MCP_STATUS='C')
// e marca como 'cancelada' no notas_entrada. Por lote: 100 notas por iteração.
async function ressincronizarTransferenciasMultiAbertas() {
  const t0 = Date.now();
  const stats = { verificadas: 0, canceladas: 0, erros: 0, ms: 0, por_cd: {} };
  const cds = await listarCds(true);
  for (const cd of cds) {
    // NÃO exclui mais o ITB legado — mesmo critério (MCP_STATUS C ou F sem NF-e) vale pra todos
    if (!cd.url || !cd.token) continue;
    stats.por_cd[cd.codigo] = { verificadas: 0, canceladas: 0 };
    try {
      const cli = await clientePorCodigo(cd.codigo);
      // Pega notas em_transito/recebida desse CD. Pra ITB (legado), notas têm origem_cd_codigo=NULL
      // identificadas por fornecedor_cnpj = CNPJ do CD. Pros outros CDs, por origem_cd_codigo.
      const isLegado = cd.codigo === CD_LEGADO;
      const filtroCd = isLegado
        ? `(origem_cd_codigo IS NULL AND fornecedor_cnpj = '17764296000209')`
        : `(origem_cd_codigo = $1)`;
      const params = isLegado ? [] : [cd.codigo];
      const notas = await dbQuery(
        `SELECT id, cd_mov_codi, data_recebimento FROM notas_entrada
          WHERE ${filtroCd}
            AND cd_mov_codi IS NOT NULL
            AND status IN ('em_transito','recebida')
          ORDER BY data_emissao ASC
          LIMIT 300`, params);
      if (!notas.length) continue;
      // Busca status + número da NF-e no UltraSyst do CD origem
      const codigosSql = notas.map(n => `'${n.cd_mov_codi}'`).join(',');
      const remoto = await cli.query(
        `SELECT MCP_CODI, MCP_STATUS, MCP_NNOTAFIS
           FROM TBMOVCOMPRA WITH (NOLOCK)
          WHERE MCP_TIPOMOV='S' AND MCP_CODI IN (${codigosSql})`);
      const porMcp = {};
      for (const r of remoto.rows || []) porMcp[r.MCP_CODI] = r;
      const canceladas = [];
      const alertas = []; // notas que loja já recebeu mas CD cancelou — só alerta, não cancela
      for (const n of notas) {
        stats.verificadas++;
        const r = porMcp[n.cd_mov_codi];
        const jaRecebida = n.data_recebimento != null;

        if (!r) {
          if (jaRecebida) alertas.push({ id: n.id, motivo: 'Sumiu do UltraSyst, mas loja já recebeu' });
          else canceladas.push({ id: n.id, motivo: 'Removida do UltraSyst (sync auto)' });
          continue;
        }
        if (r.MCP_STATUS === 'C') {
          if (jaRecebida) alertas.push({ id: n.id, motivo: 'Cancelada no UltraSyst, mas loja já recebeu' });
          else canceladas.push({ id: n.id, motivo: 'Cancelada no UltraSyst (sync auto)' });
          continue;
        }
        // MCP_STATUS='F' mas sem NF-e emitida → movimento interno que não vai chegar na loja
        if (r.MCP_STATUS === 'F' && !r.MCP_NNOTAFIS) {
          if (jaRecebida) alertas.push({ id: n.id, motivo: 'Fechada no CD sem NF-e, mas loja já recebeu' });
          else canceladas.push({ id: n.id, motivo: 'Fechada no CD sem NF-e (sync auto)' });
        }
      }
      // Agrupa por motivo e atualiza
      const porMotivo = {};
      for (const c of canceladas) {
        if (!porMotivo[c.motivo]) porMotivo[c.motivo] = [];
        porMotivo[c.motivo].push(c.id);
      }
      for (const [motivo, ids] of Object.entries(porMotivo)) {
        await dbQuery(
          `UPDATE notas_entrada SET status='cancelada', cancelada_em=NOW(),
                                    cancelada_motivo=$2
            WHERE id = ANY($1::int[])`, [ids, motivo]);
      }
      // Cria alertas pras que loja já recebeu — não cancela
      for (const a of alertas) {
        await dbQuery(
          `INSERT INTO alertas_admin (tipo, entidade, entidade_id, titulo, mensagem)
           VALUES ('cd_cancelou_apos_receber','nota',$1,
                   'CD cancelou nota mas loja já tinha recebido',$2)
           ON CONFLICT DO NOTHING`,
          [a.id, a.motivo]
        ).catch(() => {});
      }
      stats.canceladas += canceladas.length;
      stats.alertas = (stats.alertas || 0) + alertas.length;
      stats.por_cd[cd.codigo].verificadas = notas.length;
      stats.por_cd[cd.codigo].canceladas = canceladas.length;
      stats.por_cd[cd.codigo].alertas = alertas.length;
    } catch (e) {
      stats.erros++;
      console.error(`[ressync_transf_multi ${cd.codigo}]`, e.message);
    }
  }
  stats.ms = Date.now() - t0;
  return stats;
}

// Match CD→CD: pra cada nota em_transito de A pra B, vai no UltraSyst do B e procura
// ENTRADA (MCP_TIPOMOV='E') com MCP_NNOTAFIS = cd_mov_codi do A (sem zeros à esquerda)
// e FOR_CODI mapeado pro CNPJ de A na tabela FORNECEDOR.
// Quando acha, marca como recebida com data_recebimento = MCP_DTEM do B.
async function matchTransferenciasCdCdRecebidas() {
  const t0 = Date.now();
  let totalRecebidas = 0;
  const erros = [];

  const cds = await dbQuery(
    `SELECT cd_codigo, REGEXP_REPLACE(COALESCE(cnpj,''),'\\D','','g') AS cnpj_n
       FROM pedidos_distrib_destinos
      WHERE tipo='CD' AND cd_codigo IS NOT NULL AND cnpj IS NOT NULL`);
  const cnpjPorCd = {};
  for (const c of cds) cnpjPorCd[c.cd_codigo] = c.cnpj_n;

  const pares = await dbQuery(
    `SELECT origem_cd_codigo, cd_destino_codigo, COUNT(*)::int AS qtd
       FROM notas_entrada
      WHERE status = 'em_transito'
        AND cd_destino_codigo IS NOT NULL
        AND loja_id IS NULL
        AND cd_mov_codi IS NOT NULL
      GROUP BY origem_cd_codigo, cd_destino_codigo`);

  for (const par of pares) {
    const cdOrigem = par.origem_cd_codigo;
    const cdDestino = par.cd_destino_codigo;
    const cnpjOrigemN = cnpjPorCd[cdOrigem];
    if (!cnpjOrigemN) { erros.push({ par, motivo: 'CNPJ origem nao mapeado' }); continue; }

    let cli;
    try { cli = await clientePorCodigo(cdDestino); }
    catch (e) { erros.push({ par, motivo: 'sem relay destino: ' + e.message }); continue; }

    // 1) Resolve FOR_CODI do CD origem no UltraSyst do destino (FORNECEDOR.FOR_CNPJ)
    const forR = await cli.query(
      `SELECT TOP 1 FOR_CODI
         FROM FORNECEDOR WITH (NOLOCK)
        WHERE REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(FOR_CNPJ)),'.',''),'/',''),'-','') = '${cnpjOrigemN}'`);
    const forCodiOrigem = forR.rows?.[0]?.FOR_CODI;
    if (!forCodiOrigem) { erros.push({ par, motivo: 'FOR_CODI origem nao encontrado no destino' }); continue; }

    // 2) Notas em_transito desse par — usa cd_mov_codi normalizado (sem zeros à esquerda)
    const notas = await dbQuery(
      `SELECT id, cd_mov_codi, REGEXP_REPLACE(cd_mov_codi,'^0+','') AS num_norm
         FROM notas_entrada
        WHERE origem_cd_codigo=$1 AND cd_destino_codigo=$2
          AND status='em_transito' AND cd_mov_codi IS NOT NULL`,
      [cdOrigem, cdDestino]);
    if (!notas.length) continue;

    // 3) Match em lote — MCP_NNOTAFIS no destino contém o número do pedido (cd_mov_codi) sem zeros
    const BATCH = 500;
    for (let i = 0; i < notas.length; i += BATCH) {
      const slice = notas.slice(i, i + BATCH);
      // Passa AMBOS formatos (com zeros e sem) — UltraSyst pode guardar de qualquer jeito
      const formatos = new Set();
      for (const n of slice) {
        formatos.add(n.cd_mov_codi);
        formatos.add(n.num_norm);
      }
      const listaSql = [...formatos].map(s => `'${String(s).replace(/'/g, "''")}'`).join(',');
      const r = await cli.query(
        `SELECT LTRIM(RTRIM(MCP_NNOTAFIS)) AS MCP_NNOTAFIS, MCP_DTEM
           FROM TBMOVCOMPRA WITH (NOLOCK)
          WHERE MCP_TIPOMOV='E'
            AND LTRIM(RTRIM(FOR_CODI)) = '${forCodiOrigem.trim()}'
            AND LTRIM(RTRIM(MCP_NNOTAFIS)) IN (${listaSql})`);
      const porNf = {};
      for (const row of r.rows || []) {
        const nf = String(row.MCP_NNOTAFIS || '').replace(/^0+/, '');
        if (!nf) continue;
        if (!porNf[nf] || (row.MCP_DTEM && row.MCP_DTEM > porNf[nf])) porNf[nf] = row.MCP_DTEM;
      }
      const idsParaUpd = [];
      const datasParaUpd = [];
      for (const n of slice) {
        const dt = porNf[n.num_norm];
        if (dt) {
          idsParaUpd.push(n.id);
          datasParaUpd.push(new Date(dt).toISOString().slice(0, 10));
        }
      }
      if (idsParaUpd.length) {
        await dbQuery(
          `UPDATE notas_entrada n
              SET status='recebida', data_recebimento = ch.data_entrada, recebida_em = NOW()
             FROM (SELECT id AS nota_id, data_entrada
                     FROM UNNEST($1::int[], $2::date[]) AS t(id, data_entrada)) ch
            WHERE n.id = ch.nota_id`,
          [idsParaUpd, datasParaUpd]);
        totalRecebidas += idsParaUpd.length;
      }
    }
  }
  return { recebidas: totalRecebidas, erros, ms: Date.now() - t0 };
}

module.exports = { syncTransferenciasMulti, syncTransferenciasMultiCompleto, syncCdOrigem, matchTransferenciasMultiRecebidas, matchTransferenciasCdCdRecebidas, ressincronizarTransferenciasMultiAbertas };
