// Re-popula ean_nota e cd_pro_codi dos itens das notas CD existentes,
// consultando o UltraSyst (TBITEMCOMPRA + MATERIAL + EAN).
const { pool } = require('../src/db');
const ultrasyst = require('../src/ultrasyst');

(async () => {
  try {
    const t0 = Date.now();

    // Pega todas notas CD que ainda têm itens sem ean
    const notas = (await pool.query(`
      SELECT n.id, n.cd_mov_codi
        FROM notas_entrada n
       WHERE n.origem = 'cd'
         AND EXISTS (SELECT 1 FROM itens_nota i WHERE i.nota_id = n.id AND (i.ean_nota IS NULL OR i.ean_nota = ''))
       ORDER BY n.id
    `)).rows;
    console.log(`Notas a repopular: ${notas.length}`);

    let atualizados = 0;
    for (let i = 0; i < notas.length; i += 100) {
      const lote = notas.slice(i, i + 100);
      const mcps = lote.map(n => `'${n.cd_mov_codi}'`).join(',');
      const r = await ultrasyst.query(
        `SELECT i.MCP_CODI, i.MCP_SEQITEM, i.PRO_CODI,
                COALESCE(
                  NULLIF(LTRIM(RTRIM(i.EAN_CODI)),''),
                  (SELECT TOP 1 LTRIM(RTRIM(EAN_CODI)) FROM EAN WITH (NOLOCK)
                    WHERE MAT_CODI = i.PRO_CODI AND EAN_CODI IS NOT NULL AND LTRIM(RTRIM(EAN_CODI)) <> ''
                    ORDER BY CASE WHEN EAN_NOTA='S' THEN 0 ELSE 1 END, ID),
                  NULLIF(LTRIM(RTRIM(mat.EAN_CODI)),'')
                ) AS ean
           FROM TBITEMCOMPRA i WITH (NOLOCK)
           LEFT JOIN MATERIAL mat WITH (NOLOCK) ON mat.MAT_CODI = i.PRO_CODI
          WHERE i.EMP_CODI = '001' AND i.MCP_TIPOMOV = 'S' AND i.MCP_CODI IN (${mcps})`
      );
      // Map MCP_CODI → array de {seq, pro_codi, ean}
      const porMcp = {};
      for (const row of r.rows || []) {
        if (!porMcp[row.MCP_CODI]) porMcp[row.MCP_CODI] = [];
        porMcp[row.MCP_CODI].push({
          seq: Math.floor(row.MCP_SEQITEM || 0),
          pro_codi: (row.PRO_CODI || '').trim() || null,
          ean: (row.ean || '').trim() || null,
        });
      }

      // Atualiza no Postgres
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        for (const n of lote) {
          const itens = porMcp[n.cd_mov_codi] || [];
          for (const it of itens) {
            const r2 = await client.query(
              `UPDATE itens_nota
                  SET ean_nota = COALESCE(NULLIF(ean_nota,''), $3),
                      cd_pro_codi = COALESCE(cd_pro_codi, $4)
                WHERE nota_id = $1 AND numero_item = $2`,
              [n.id, it.seq, it.ean, it.pro_codi]
            );
            atualizados += r2.rowCount;
          }
        }
        await client.query('COMMIT');
      } catch (e) {
        await client.query('ROLLBACK').catch(() => {});
        throw e;
      } finally { client.release(); }
      process.stdout.write('.');
    }
    console.log('');
    console.log(`Itens atualizados: ${atualizados}`);
    console.log(`Tempo: ${Date.now() - t0}ms`);

    // Stats
    const stats = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE i.ean_nota IS NULL OR i.ean_nota = '')::int AS ainda_sem_ean,
        COUNT(*)::int AS total
        FROM itens_nota i
        JOIN notas_entrada n ON n.id = i.nota_id
       WHERE n.origem = 'cd'
    `);
    console.log('Stats CD:', stats.rows[0]);
  } catch (e) {
    console.error('ERRO:', e.message);
    process.exit(1);
  } finally { await pool.end(); }
})();
