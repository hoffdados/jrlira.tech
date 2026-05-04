const { pool } = require('../src/db');

function normalizarDescricao(s) {
  if (!s) return '';
  return String(s)
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .toUpperCase()
    .replace(/\d+\s*[Xx]\s*\d+\s*(ML|L|G|KG|MG|UN|UND)?/g, '')
    .replace(/\b\d+\s*[Xx]\s*\d+\b/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

const THRESHOLD = parseFloat(process.argv[2] || '0.85');

(async () => {
  try {
    const t0 = Date.now();
    const ativos = (await pool.query(`
      SELECT mat_codi, descricao_atual
        FROM produtos_embalagem
       WHERE ativo_no_cd = TRUE
         AND ean_status IN ('sem_ean_jrlira','divergente','sem_ean_cd','sem_ambos')
         AND descricao_atual IS NOT NULL
    `)).rows;
    console.log(`Processando ${ativos.length} produtos ativos com EAN problemático (threshold=${THRESHOLD})...`);

    let aplicados = 0, ignorados = 0;
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      for (const p of ativos) {
        const descNorm = normalizarDescricao(p.descricao_atual);
        if (!descNorm) { ignorados++; continue; }
        const r = await client.query(
          `SELECT codigobarra, MAX(similarity(descricao, $1)) AS score
             FROM produtos_externo
            WHERE similarity(descricao, $1) >= $2
              AND codigobarra IS NOT NULL AND codigobarra <> ''
              AND codigo_interno = produtoprincipal
            GROUP BY codigobarra
            ORDER BY score DESC
            LIMIT 1`,
          [descNorm, THRESHOLD]
        );
        if (!r.rows.length) { ignorados++; continue; }
        const top = r.rows[0];
        await client.query(
          `UPDATE produtos_embalagem
              SET ean_sugerido_eco = $2,
                  ean_sugerido_score = $3,
                  ean_sugerido_em = NOW(),
                  atualizado_em = NOW()
            WHERE mat_codi = $1`,
          [p.mat_codi, top.codigobarra, top.score]
        );
        aplicados++;
        if (aplicados % 100 === 0) process.stdout.write('.');
      }
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      throw e;
    } finally { client.release(); }

    console.log('');
    console.log(`Aplicados: ${aplicados}`);
    console.log(`Ignorados (sem match acima do threshold): ${ignorados}`);
    console.log(`Tempo: ${Date.now() - t0}ms`);
  } catch (e) {
    console.error('ERRO:', e.message);
    process.exit(1);
  } finally { await pool.end(); }
})();
