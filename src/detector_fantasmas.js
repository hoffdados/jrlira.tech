// Detector de NFs fantasma em compras_historico.
//
// Contexto: o sync Pentaho das lojas só faz UPSERT — nunca apaga. Quando a loja
// cancela ou corrige um lançamento no Ecocentauro, a linha antiga persiste
// pra sempre em compras_historico. Esse detector identifica essas NFs e
// registra em `compras_fantasmas` pra revisão admin.
//
// Heurísticas suportadas:
//  - `twin_substituiu`: existe outra NF (mesma loja+fornecedor) com itens
//    idênticos (mesmo conjunto codigobarra+custo_total) e numeronfe diferente.
//    Quando isso acontece, a versão antiga (com sincronizado_em mais antigo)
//    é o fantasma.
//
// Não cobre: NFs canceladas sem substituta (precisa de sync source-side).

const { query: dbQuery } = require('./db');

// Detecta candidatos com twins. Retorna array de { loja_id, numeronfe,
// fornecedor_cnpj, twin_numeronfe, qtd_itens, valor_total, data_entrada }.
async function detectarTwinsGlobal() {
  return dbQuery(`
    WITH sig AS (
      SELECT loja_id, numeronfe,
             REGEXP_REPLACE(fornecedor_cnpj,'\\D','','g') AS cnpj_norm,
             STRING_AGG(codigobarra || '|' || custo_total::text, ','
                        ORDER BY codigobarra, custo_total) AS s,
             COUNT(*)::int AS qtd_itens,
             SUM(custo_total)::numeric(14,2) AS valor_total,
             MAX(sincronizado_em) AS ultimo_sync,
             MIN(data_entrada::date) AS data_entrada
        FROM compras_historico
       WHERE fornecedor_cnpj IS NOT NULL
       GROUP BY loja_id, numeronfe, cnpj_norm
    ),
    pares AS (
      SELECT s1.loja_id, s1.numeronfe AS fantasma_nfe, s1.cnpj_norm,
             s2.numeronfe AS twin_nfe,
             s1.qtd_itens, s1.valor_total, s1.data_entrada,
             s1.ultimo_sync AS sync_fantasma, s2.ultimo_sync AS sync_twin
        FROM sig s1
        JOIN sig s2
          ON s2.loja_id = s1.loja_id
         AND s2.cnpj_norm = s1.cnpj_norm
         AND s2.numeronfe <> s1.numeronfe
         AND s2.s = s1.s
       WHERE s1.ultimo_sync < s2.ultimo_sync  -- s1 é o mais velho → fantasma
    )
    SELECT loja_id, fantasma_nfe AS numeronfe, cnpj_norm AS fornecedor_cnpj,
           twin_nfe AS twin_numeronfe,
           qtd_itens, valor_total, data_entrada,
           sync_fantasma, sync_twin
      FROM pares
     ORDER BY loja_id, data_entrada DESC`);
}

async function cadastrarFantasmas(rows) {
  if (!rows.length) return 0;
  // Hidrata nome fornecedor (1 query)
  const cnpjs = [...new Set(rows.map(r => r.fornecedor_cnpj))];
  const nomes = await dbQuery(
    `SELECT REGEXP_REPLACE(fornecedor_cnpj,'\\D','','g') AS cnpj,
            MAX(fornecedor_nome) AS nome
       FROM compras_historico
      WHERE REGEXP_REPLACE(fornecedor_cnpj,'\\D','','g') = ANY($1::text[])
      GROUP BY cnpj`,
    [cnpjs]
  ).catch(() => []);
  const nomeMap = new Map(nomes.map(n => [n.cnpj, n.nome]));

  let novos = 0;
  for (const r of rows) {
    const ins = await dbQuery(
      `INSERT INTO compras_fantasmas
         (loja_id, numeronfe, fornecedor_cnpj, fornecedor_nome,
          motivo, twin_numeronfe, qtd_itens, valor_total, data_entrada,
          detalhe_json, status)
       VALUES ($1,$2,$3,$4,'twin_substituiu',$5,$6,$7,$8,$9,'pendente')
       ON CONFLICT (loja_id, numeronfe, fornecedor_cnpj) DO UPDATE SET
         twin_numeronfe = EXCLUDED.twin_numeronfe,
         qtd_itens = EXCLUDED.qtd_itens,
         valor_total = EXCLUDED.valor_total,
         detalhe_json = EXCLUDED.detalhe_json,
         detectado_em = NOW()
       WHERE compras_fantasmas.status = 'pendente'
       RETURNING (xmax = 0) AS inserted`,
      [r.loja_id, r.numeronfe, r.fornecedor_cnpj, nomeMap.get(r.fornecedor_cnpj) || null,
       r.twin_numeronfe, r.qtd_itens, r.valor_total, r.data_entrada,
       JSON.stringify({ sync_fantasma: r.sync_fantasma, sync_twin: r.sync_twin })]
    );
    if (ins[0]?.inserted) novos++;
  }
  return novos;
}

async function rodar() {
  const t0 = Date.now();
  const candidatos = await detectarTwinsGlobal();
  const novos = await cadastrarFantasmas(candidatos);
  const dt = Math.round((Date.now() - t0) / 1000);
  console.log(`[detector_fantasmas] ${candidatos.length} candidato(s), ${novos} novo(s) em ${dt}s`);
  return { candidatos: candidatos.length, novos };
}

async function apagar(fantasmaId, usuario) {
  const [f] = await dbQuery(
    `SELECT loja_id, numeronfe, fornecedor_cnpj FROM compras_fantasmas
      WHERE id = $1 AND status = 'pendente'`, [fantasmaId]);
  if (!f) throw Object.assign(new Error('fantasma nao encontrado ou ja resolvido'), { status: 404 });
  const r = await dbQuery(
    `DELETE FROM compras_historico
      WHERE loja_id = $1
        AND numeronfe = $2
        AND REGEXP_REPLACE(fornecedor_cnpj,'\\D','','g') = $3
      RETURNING id`,
    [f.loja_id, f.numeronfe, f.fornecedor_cnpj]
  );
  await dbQuery(
    `UPDATE compras_fantasmas
        SET status='apagado', resolvido_em=NOW(), resolvido_por=$2, resolucao='apagado_admin'
      WHERE id=$1`,
    [fantasmaId, usuario || null]
  );
  return { rows_apagadas: r.length };
}

async function ignorar(fantasmaId, usuario, motivo) {
  const r = await dbQuery(
    `UPDATE compras_fantasmas
        SET status='ignorado', resolvido_em=NOW(), resolvido_por=$2,
            resolucao=$3
      WHERE id=$1 AND status='pendente' RETURNING 1`,
    [fantasmaId, usuario || null, motivo || 'ignorado_admin']
  );
  if (!r.length) throw Object.assign(new Error('fantasma nao encontrado ou ja resolvido'), { status: 404 });
  return { ok: true };
}

module.exports = { detectarTwinsGlobal, cadastrarFantasmas, rodar, apagar, ignorar };
