const { pool } = require('../src/db');
const ultrasyst = require('../src/ultrasyst');
(async () => {
  try {
    const t0 = Date.now();
    const mats = (await pool.query(`SELECT mat_codi, ativo_no_cd FROM produtos_embalagem`)).rows;
    const matCodis = mats.map(m => m.mat_codi);
    const ativoAntes = Object.fromEntries(mats.map(m => [m.mat_codi, m.ativo_no_cd]));

    const ativoCD = {};
    for (let i = 0; i < matCodis.length; i += 500) {
      const lote = matCodis.slice(i, i + 500);
      const lista = lote.map(c => `'${c}'`).join(',');
      const r = await ultrasyst.query(
        `SELECT MAT_CODI, MAT_SITU FROM MATERIAL WITH (NOLOCK) WHERE MAT_CODI IN (${lista})`
      );
      for (const row of r.rows || []) ativoCD[row.MAT_CODI] = row.MAT_SITU === 'A';
      process.stdout.write('.');
    }
    console.log('');

    const ids = Object.keys(ativoCD);
    const ativos = ids.map(i => ativoCD[i]);
    const reativados = ids.filter(id => !ativoAntes[id] && ativoCD[id]);
    const desativados = ids.filter(id => ativoAntes[id] && !ativoCD[id]);
    const naoEncontrados = matCodis.filter(m => !(m in ativoCD));

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `UPDATE produtos_embalagem n
            SET ativo_no_cd = u.ativo, atualizado_em = NOW()
           FROM (SELECT * FROM UNNEST($1::text[], $2::bool[]) AS t(mat_codi, ativo)) u
          WHERE n.mat_codi = u.mat_codi`,
        [ids, ativos]
      );
      // Quem não veio do UltraSyst (deletado lá) → marcar como inativo
      if (naoEncontrados.length) {
        await client.query(
          `UPDATE produtos_embalagem
              SET ativo_no_cd = FALSE, atualizado_em = NOW()
            WHERE mat_codi = ANY($1::text[])`,
          [naoEncontrados]
        );
      }
      // Reativados voltam pra pendente
      if (reativados.length) {
        await client.query(
          `UPDATE produtos_embalagem
              SET status = 'pendente_validacao',
                  validado_em = NULL,
                  validado_por = NULL,
                  observacao = COALESCE(observacao || ' | ', '') || 'Reativado em ' || TO_CHAR(NOW(),'YYYY-MM-DD'),
                  atualizado_em = NOW()
            WHERE mat_codi = ANY($1::text[])`,
          [reativados]
        );
      }
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      throw e;
    } finally { client.release(); }

    const stats = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE ativo_no_cd)::int AS ativos,
        COUNT(*) FILTER (WHERE NOT ativo_no_cd)::int AS inativos,
        COUNT(*)::int AS total
        FROM produtos_embalagem
    `);
    console.log('Total consultados:', ids.length);
    console.log('Não encontrados no UltraSyst (foram deletados):', naoEncontrados.length);
    console.log('Reativados (volta pra pendente):', reativados.length);
    console.log('Desativados:', desativados.length);
    console.log('---');
    console.log('Total ATIVOS:', stats.rows[0].ativos);
    console.log('Total INATIVOS:', stats.rows[0].inativos);
    console.log('Total geral:', stats.rows[0].total);
    console.log('Tempo:', Date.now() - t0, 'ms');
  } catch (e) {
    console.error('ERRO:', e.message);
    process.exit(1);
  } finally { await pool.end(); }
})();
