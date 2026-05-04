const { pool } = require('../src/db');
(async () => {
  try {
    // Procurar EAN principal do produto agregado SAND HAVAIANAS
    // produto_principal = '028239' (código interno do Eco)
    const a = await pool.query(`
      SELECT loja_id, codigobarra, produtoprincipal, descricao
        FROM produtos_externo
       WHERE produtoprincipal = '028239'
       ORDER BY loja_id, codigobarra
       LIMIT 30
    `);
    console.log('Linhas onde produtoprincipal=028239:');
    for (const r of a.rows) {
      console.log(`  L${r.loja_id} ean=${r.codigobarra} princ=${r.produtoprincipal} ${r.descricao}`);
    }

    // Stats: produtoprincipal vs codigobarra
    const b = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE produtoprincipal IS NULL)::int AS sem_principal,
        COUNT(*) FILTER (WHERE produtoprincipal = codigobarra)::int AS principal_explicito,
        COUNT(*) FILTER (WHERE produtoprincipal IS NOT NULL AND produtoprincipal <> codigobarra)::int AS agregados,
        COUNT(*)::int AS total
        FROM produtos_externo
    `);
    console.log('Stats geral:', JSON.stringify(b.rows[0], null, 2));
  } catch (e) { console.error(e.message); } finally { await pool.end(); }
})();
