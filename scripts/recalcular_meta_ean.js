const { pool } = require('../src/db');
(async () => {
  try {
    const t0 = Date.now();

    // Zera primeiro
    await pool.query(`UPDATE produtos_embalagem SET ean_aponta_para = NULL WHERE ean_aponta_para IS NOT NULL`);
    // Lógica:
    //  - Considera só lojas onde codigo_interno foi sincronizado (KTR novo já rodou)
    //  - Se em PELO MENOS UMA dessas lojas o produto é principal → ignora (mesmo se for agregado em outras)
    //  - Se em TODAS o produto é agregado → marca, e aponta_para = EAN do pai (codigo_interno = produtoprincipal)
    await pool.query(`
      UPDATE produtos_embalagem pe
         SET ean_aponta_para = sub.ean_pai,
             atualizado_em = NOW()
        FROM (
          SELECT ext.codigobarra AS ean_agregado,
                 (SELECT MIN(ext_pai.codigobarra)
                    FROM produtos_externo ext_pai
                   WHERE ext_pai.codigo_interno = ext.produtoprincipal
                     AND ext_pai.loja_id = ext.loja_id) AS ean_pai
            FROM produtos_externo ext
           WHERE ext.codigo_interno IS NOT NULL
             AND ext.produtoprincipal IS NOT NULL
             AND ext.codigo_interno <> ext.produtoprincipal
           GROUP BY ext.codigobarra, ext.produtoprincipal, ext.loja_id
          HAVING NOT EXISTS (
            SELECT 1 FROM produtos_externo ext2
             WHERE ext2.codigobarra = ext.codigobarra
               AND ext2.codigo_interno IS NOT NULL
               AND ext2.codigo_interno = ext2.produtoprincipal
          )
        ) sub
       WHERE pe.ean_principal_jrlira = sub.ean_agregado
         AND sub.ean_pai IS NOT NULL
    `);

    await pool.query(`
      UPDATE produtos_embalagem pe
         SET ean_duplicado_count = COALESCE(d.cnt, 0)
        FROM (
          SELECT ean_principal_jrlira, COUNT(*)::int - 1 AS cnt
            FROM produtos_embalagem
           WHERE ean_principal_jrlira IS NOT NULL
             AND ean_principal_jrlira <> ''
           GROUP BY ean_principal_jrlira
        ) d
       WHERE pe.ean_principal_jrlira = d.ean_principal_jrlira
    `);

    await pool.query(`
      UPDATE produtos_embalagem
         SET ean_duplicado_count = 0
       WHERE ean_principal_jrlira IS NULL OR ean_principal_jrlira = ''
    `);

    const stats = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE ean_aponta_para IS NOT NULL)::int AS aponta_agreg,
        COUNT(*) FILTER (WHERE ean_aponta_para IS NOT NULL AND ativo_no_cd)::int AS aponta_agreg_ativos,
        COUNT(*) FILTER (WHERE ean_duplicado_count > 0)::int AS duplicado,
        COUNT(*) FILTER (WHERE ean_duplicado_count > 0 AND ativo_no_cd)::int AS duplicado_ativos
        FROM produtos_embalagem
    `);
    console.log('STATS:', JSON.stringify(stats.rows[0], null, 2));
    console.log('Tempo:', Date.now() - t0, 'ms');
  } catch (e) {
    console.error('ERRO:', e.message);
    process.exit(1);
  } finally {
    await pool.end();
  }
})();
