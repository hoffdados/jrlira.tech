const { pool } = require('../src/db');
(async () => {
  try {
    // Pega 5 produtos marcados como agregado
    const a = await pool.query(`
      SELECT mat_codi, descricao_atual, ean_principal_jrlira, ean_aponta_para
        FROM produtos_embalagem
       WHERE ean_aponta_para IS NOT NULL AND ativo_no_cd = TRUE
       LIMIT 5
    `);
    for (const p of a.rows) {
      console.log('---');
      console.log(`MAT: ${p.mat_codi} | ${(p.descricao_atual||'').trim()}`);
      console.log(`EAN JR: ${p.ean_principal_jrlira} -> aponta_para: ${p.ean_aponta_para}`);

      const ext = await pool.query(`
        SELECT loja_id, codigobarra, produtoprincipal, descricao
          FROM produtos_externo
         WHERE codigobarra = $1
         ORDER BY loja_id
      `, [p.ean_principal_jrlira]);
      console.log('Em produtos_externo:');
      for (const e of ext.rows) {
        const tipo = (!e.produtoprincipal || e.produtoprincipal === e.codigobarra) ? 'PRINCIPAL' : `agregado→${e.produtoprincipal}`;
        console.log(`  L${e.loja_id} ${tipo} ${(e.descricao||'').trim()}`);
      }
    }
  } catch (e) { console.error(e.message); } finally { await pool.end(); }
})();
