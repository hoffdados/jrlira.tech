const { pool } = require('../src/db');
(async () => {
  try {
    const a = await pool.query(`
      SELECT loja_id,
             COUNT(*)::int AS total,
             COUNT(*) FILTER (WHERE codigo_interno IS NOT NULL)::int AS com_codigo,
             COUNT(*) FILTER (WHERE codigo_interno = produtoprincipal)::int AS principais,
             COUNT(*) FILTER (WHERE codigo_interno IS NOT NULL AND codigo_interno <> produtoprincipal)::int AS agregados
        FROM produtos_externo
       GROUP BY loja_id
       ORDER BY loja_id
    `);
    console.log('Por loja:');
    for (const r of a.rows) {
      console.log(`L${r.loja_id} total=${r.total} com_codigo=${r.com_codigo} principais=${r.principais} agregados=${r.agregados}`);
    }

    // Amostra loja 6 — algumas linhas pra ver se codigo_interno está OK
    const b = await pool.query(`
      SELECT codigobarra, codigo_interno, produtoprincipal, descricao
        FROM produtos_externo
       WHERE loja_id = 6 AND codigo_interno IS NOT NULL
       LIMIT 5
    `);
    console.log('\nAmostra L6:');
    for (const r of b.rows) {
      const tipo = r.codigo_interno === r.produtoprincipal ? 'PRINCIPAL' : `agregado→${r.produtoprincipal}`;
      console.log(`  ean=${r.codigobarra} cod=${r.codigo_interno} ${tipo} ${(r.descricao||'').trim()}`);
    }
  } catch (e) { console.error(e.message); } finally { await pool.end(); }
})();
