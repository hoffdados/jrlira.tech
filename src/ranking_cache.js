// Cache do ranking global de produtos por CD. Atualiza periodicamente em background.
// Substitui a query pesada que rodava em cada chamada de /grade.

const { pool, query: dbQuery } = require('./db');
const { listarCds } = require('./cds');

async function atualizarRankingCache(cdCodigo) {
  const t0 = Date.now();
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Calcula ranking via mesma lógica do /grade original
    const { rows } = await client.query(`
      WITH produtos_cd AS (
        SELECT cd_m.mat_codi,
               COALESCE(NULLIF(pe.ean_principal_cd,''), NULLIF(cd_m.ean_codi,'')) AS ean,
               cd_c.pro_prad AS preco_admin
          FROM cd_material cd_m
          LEFT JOIN cd_custoprod cd_c ON cd_c.cd_codigo=cd_m.cd_codigo AND cd_c.pro_codi=cd_m.mat_codi
          LEFT JOIN produtos_embalagem pe ON pe.mat_codi=cd_m.mat_codi
         WHERE cd_m.cd_codigo = $1
           AND (cd_m.mat_situ='A' OR cd_m.mat_situ IS NULL)
           AND COALESCE(NULLIF(pe.ean_principal_cd,''), NULLIF(cd_m.ean_codi,'')) IS NOT NULL
      ),
      vendas_90d AS (
        SELECT NULLIF(LTRIM(codigobarra,'0'),'') AS ean,
               SUM(qtd_vendida) AS qtd
          FROM vendas_historico
         WHERE data_venda >= CURRENT_DATE - INTERVAL '90 days'
           AND COALESCE(tipo_saida,'venda')='venda'
         GROUP BY ean
      )
      SELECT pc.mat_codi,
             COALESCE(v.qtd, 0) * COALESCE(pc.preco_admin, 0) AS valor_vendido,
             ROW_NUMBER() OVER (ORDER BY COALESCE(v.qtd, 0) * COALESCE(pc.preco_admin, 0) DESC) AS posicao
        FROM produtos_cd pc
        LEFT JOIN vendas_90d v ON v.ean = pc.ean
       WHERE COALESCE(v.qtd, 0) > 0
    `, [cdCodigo]);

    // TRUNCATE só do CD em questão
    await client.query(`DELETE FROM pedidos_distrib_ranking_cache WHERE cd_codigo = $1`, [cdCodigo]);
    if (rows.length) {
      await client.query(`
        INSERT INTO pedidos_distrib_ranking_cache (cd_codigo, mat_codi, posicao, valor_vendido)
        SELECT $1, * FROM UNNEST($2::text[], $3::int[], $4::numeric[])
      `, [
        cdCodigo,
        rows.map(r => r.mat_codi),
        rows.map(r => parseInt(r.posicao)),
        rows.map(r => parseFloat(r.valor_vendido)),
      ]);
    }
    await client.query('COMMIT');
    return { cd: cdCodigo, linhas: rows.length, ms: Date.now() - t0 };
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}

async function atualizarRankingCacheAll() {
  const cds = await listarCds(true);
  const out = [];
  for (const cd of cds) {
    try {
      const r = await atualizarRankingCache(cd.codigo);
      out.push(r);
    } catch (e) {
      console.error(`[ranking_cache ${cd.codigo}]`, e.message);
      out.push({ cd: cd.codigo, erro: e.message });
    }
  }
  return out;
}

module.exports = { atualizarRankingCache, atualizarRankingCacheAll };
