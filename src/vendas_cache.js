// Caches diários de vendas pré-computadas (28d/90d).
// Eliminam queries pesadas em vendas_historico + cd_capa do /pedidos-distribuidora/grade.

const { pool, query: dbQuery } = require('./db');

async function atualizarVendasLojaCache() {
  const t0 = Date.now();
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`TRUNCATE TABLE vendas_loja_cache`);
    await client.query(`
      INSERT INTO vendas_loja_cache (loja_id, codbarra_norm, qtd_28d, qtd_90d, ultima_venda)
      SELECT loja_id,
             NULLIF(LTRIM(codigobarra,'0'),'') AS codbarra_norm,
             SUM(CASE WHEN data_venda >= CURRENT_DATE - INTERVAL '28 days' THEN qtd_vendida ELSE 0 END)::numeric(14,3) AS qtd_28d,
             SUM(qtd_vendida)::numeric(14,3) AS qtd_90d,
             MAX(data_venda) AS ultima_venda
        FROM vendas_historico
       WHERE data_venda >= CURRENT_DATE - INTERVAL '90 days'
         AND COALESCE(tipo_saida,'venda') = 'venda'
         AND codigobarra IS NOT NULL
         AND NULLIF(LTRIM(codigobarra,'0'),'') IS NOT NULL
       GROUP BY loja_id, NULLIF(LTRIM(codigobarra,'0'),'')
    `);
    const { rows: [c] } = await client.query(`SELECT COUNT(*)::int AS qtd FROM vendas_loja_cache`);
    await client.query('COMMIT');
    return { tabela: 'vendas_loja_cache', linhas: c.qtd, ms: Date.now() - t0 };
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally { client.release(); }
}

async function atualizarVendasCdCache() {
  const t0 = Date.now();
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`TRUNCATE TABLE vendas_cd_cache`);
    await client.query(`
      INSERT INTO vendas_cd_cache (cd_codigo, ean_norm, qtd_90d, ultima_saida)
      SELECT cap.cd_codigo,
             NULLIF(LTRIM(ce.ean_codi,'0'),'') AS ean_norm,
             SUM(mi.ite_quan * (CASE WHEN cap.cap_devol='S' THEN -1 ELSE 1 END))::numeric(14,3) AS qtd_90d,
             MAX(cap.cap_dtem)::date AS ultima_saida
        FROM cd_capa cap
        JOIN cd_moviitem mi
          ON mi.cd_codigo = cap.cd_codigo AND mi.cap_sequ = cap.cap_sequ
        JOIN cd_ean ce
          ON ce.cd_codigo = cap.cd_codigo AND ce.mat_codi = mi.pro_codi
       WHERE cap.cap_tipo = '3'
         AND cap.cap_dtem >= CURRENT_DATE - INTERVAL '90 days'
         AND NULLIF(LTRIM(ce.ean_codi,'0'),'') IS NOT NULL
       GROUP BY cap.cd_codigo, NULLIF(LTRIM(ce.ean_codi,'0'),'')
    `);
    const { rows: [c] } = await client.query(`SELECT COUNT(*)::int AS qtd FROM vendas_cd_cache`);
    await client.query('COMMIT');
    return { tabela: 'vendas_cd_cache', linhas: c.qtd, ms: Date.now() - t0 };
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally { client.release(); }
}

async function atualizarVendasCdCanonicoCache() {
  const t0 = Date.now();
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`TRUNCATE TABLE vendas_cd_canonico_cache`);
    await client.query(`
      INSERT INTO vendas_cd_canonico_cache (cd_codigo, mat_codi, qtd_90d, ultima_saida)
      SELECT cap.cd_codigo,
             mi.pro_codi AS mat_codi,
             SUM(mi.ite_quan * (CASE WHEN cap.cap_devol='S' THEN -1 ELSE 1 END))::numeric(14,3) AS qtd_90d,
             MAX(cap.cap_dtem)::date AS ultima_saida
        FROM cd_capa cap
        JOIN cd_moviitem mi
          ON mi.cd_codigo = cap.cd_codigo AND mi.cap_sequ = cap.cap_sequ
        JOIN produto_canonico_match pcm
          ON pcm.cd_codigo = cap.cd_codigo AND pcm.mat_codi = mi.pro_codi
       WHERE cap.cap_tipo = '3'
         AND cap.cap_dtem >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY cap.cd_codigo, mi.pro_codi
    `);
    const { rows: [c] } = await client.query(`SELECT COUNT(*)::int AS qtd FROM vendas_cd_canonico_cache`);
    await client.query('COMMIT');
    return { tabela: 'vendas_cd_canonico_cache', linhas: c.qtd, ms: Date.now() - t0 };
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally { client.release(); }
}

async function atualizarTodosCaches() {
  const out = [];
  for (const fn of [atualizarVendasLojaCache, atualizarVendasCdCache, atualizarVendasCdCanonicoCache]) {
    try { out.push(await fn()); }
    catch (e) { out.push({ erro: e.message }); }
  }
  return out;
}

module.exports = { atualizarVendasLojaCache, atualizarVendasCdCache, atualizarVendasCdCanonicoCache, atualizarTodosCaches };
