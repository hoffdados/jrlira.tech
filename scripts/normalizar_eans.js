const { pool } = require('../src/db');
(async () => {
  try {
    const t0 = Date.now();

    // 1) Normaliza tabelas que NÃO têm constraint unique problemática
    console.log('Normalizando produtos_embalagem...');
    await pool.query(`
      UPDATE produtos_embalagem
         SET ean_principal_cd = NULLIF(LTRIM(COALESCE(ean_principal_cd,''),'0'),''),
             ean_principal_jrlira = NULLIF(LTRIM(COALESCE(ean_principal_jrlira,''),'0'),''),
             ean_sugerido_eco = NULLIF(LTRIM(COALESCE(ean_sugerido_eco,''),'0'),'')
       WHERE ean_principal_cd ~ '^0' OR ean_principal_jrlira ~ '^0' OR ean_sugerido_eco ~ '^0'
    `);

    console.log('Normalizando itens_nota...');
    await pool.query(`UPDATE itens_nota SET ean_nota = NULLIF(LTRIM(COALESCE(ean_nota,''),'0'),'') WHERE ean_nota ~ '^0'`);

    console.log('vendas_historico — agregando duplicatas...');
    await pool.query(`
      WITH dup AS (
        SELECT MIN(id) AS keep_id, loja_id, LTRIM(codigobarra,'0') AS norm, data_venda,
               SUM(qtd_vendida) AS soma, ARRAY_AGG(id) AS ids
          FROM vendas_historico
         WHERE codigobarra ~ '^0' OR codigobarra IN (
           SELECT LTRIM(codigobarra,'0') FROM vendas_historico WHERE codigobarra ~ '^0'
         )
         GROUP BY loja_id, LTRIM(codigobarra,'0'), data_venda
        HAVING COUNT(*) > 1
      )
      UPDATE vendas_historico v
         SET qtd_vendida = dup.soma, codigobarra = dup.norm
        FROM dup
       WHERE v.id = dup.keep_id
    `);
    await pool.query(`
      DELETE FROM vendas_historico v USING (
        SELECT MIN(id) AS keep_id, loja_id, LTRIM(codigobarra,'0') AS norm, data_venda,
               UNNEST(ARRAY_AGG(id)) AS dup_id
          FROM vendas_historico
         WHERE codigobarra ~ '^0' OR codigobarra IN (
           SELECT LTRIM(codigobarra,'0') FROM vendas_historico WHERE codigobarra ~ '^0'
         )
         GROUP BY loja_id, LTRIM(codigobarra,'0'), data_venda
        HAVING COUNT(*) > 1
      ) d WHERE v.id = d.dup_id AND v.id <> d.keep_id
    `);
    console.log('Normalizando vendas_historico restante...');
    await pool.query(`UPDATE vendas_historico SET codigobarra = NULLIF(LTRIM(COALESCE(codigobarra,''),'0'),'') WHERE codigobarra ~ '^0'`);

    console.log('Normalizando compras_historico...');
    await pool.query(`UPDATE compras_historico SET codigobarra = NULLIF(LTRIM(COALESCE(codigobarra,''),'0'),'') WHERE codigobarra ~ '^0'`);

    // 2) produtos_externo: detecta e remove a variante "com zero" quando há colisão
    console.log('\nLimpando duplicatas em produtos_externo...');
    const r = await pool.query(`
      DELETE FROM produtos_externo p
       WHERE p.codigobarra ~ '^0'
         AND EXISTS (
           SELECT 1 FROM produtos_externo q
            WHERE q.loja_id = p.loja_id
              AND q.codigobarra = LTRIM(p.codigobarra,'0')
         )
    `);
    console.log(`Variantes com zero removidas (já existiam sem zero): ${r.rowCount}`);

    // Agora: dentro do mesmo (loja, LTRIM), pode haver MÚLTIPLAS variantes com zero
    // (ex: '07896', '007896'). Mantém a com mais dígitos (= menos zeros)
    const r2 = await pool.query(`
      DELETE FROM produtos_externo p USING (
        SELECT loja_id, LTRIM(codigobarra,'0') AS norm,
               (ARRAY_AGG(codigobarra ORDER BY length(codigobarra) DESC, codigobarra))[1] AS keep
          FROM produtos_externo
         WHERE codigobarra ~ '^0'
         GROUP BY loja_id, LTRIM(codigobarra,'0')
        HAVING COUNT(*) > 1
      ) d
      WHERE p.loja_id = d.loja_id
        AND LTRIM(p.codigobarra,'0') = d.norm
        AND p.codigobarra <> d.keep
    `);
    console.log(`Variantes redundantes (mais zeros) removidas: ${r2.rowCount}`);

    console.log('\nNormalizando produtos_externo...');
    await pool.query(`UPDATE produtos_externo SET codigobarra = NULLIF(LTRIM(COALESCE(codigobarra,''),'0'),'') WHERE codigobarra ~ '^0'`);

    // Marca migration como aplicada pra não tentar de novo
    await pool.query(
      `INSERT INTO _migrations(name) VALUES('20260503_eans_normalizar_dados') ON CONFLICT DO NOTHING`
    );

    console.log(`\nOK em ${Date.now() - t0}ms`);
  } catch (e) {
    console.error('ERRO:', e.message);
    process.exit(1);
  } finally {
    await pool.end();
  }
})();
