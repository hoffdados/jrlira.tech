// Auto-match cross-CD de produtos. Cria registros em produto_canonico + produto_canonico_match
// quando EAN bate exato entre 2+ CDs E descrição é muito similar (Levenshtein <= 4).
//
// Estratégia:
// 1. Para cada EAN distinto que aparece em 2+ CDs no cd_ean, busca os mat_codi correspondentes
// 2. Se a descrição (cd_material.mat_desc) é similar entre eles → cria canônico auto_validado
// 3. Se EAN cruza mas descrições muito diferentes → cria canônico com conflito=true (vai pra revisão)
// 4. Se um mat_codi não tem match em nenhum outro CD → fica sem canônico (pendente)

const { pool } = require('./db');

const norm = e => String(e || '').replace(/^0+/, '') || e;

// Levenshtein O(m*n), normaliza descrição antes (uppercase, sem espaços duplicados, sem pontuação ruidosa)
function normalizarDesc(s) {
  return String(s || '')
    .toUpperCase()
    .replace(/[^A-Z0-9 ]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function levenshtein(a, b) {
  if (a === b) return 0;
  if (!a.length) return b.length;
  if (!b.length) return a.length;
  const m = a.length, n = b.length;
  let prev = Array(n + 1);
  let curr = Array(n + 1);
  for (let j = 0; j <= n; j++) prev[j] = j;
  for (let i = 1; i <= m; i++) {
    curr[0] = i;
    for (let j = 1; j <= n; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      curr[j] = Math.min(prev[j] + 1, curr[j - 1] + 1, prev[j - 1] + cost);
    }
    [prev, curr] = [curr, prev];
  }
  return prev[n];
}

const LEV_MAX = 4; // tolerância pequena — conservador

async function rodarAutoMatch() {
  const client = await pool.connect();
  const stats = { eans_avaliados: 0, canonicos_criados: 0, matches_criados: 0, conflitos: 0 };
  try {
    // 1) EANs presentes em 2+ CDs no cd_ean (já normalizados pra evitar problemas de zero à esquerda)
    const { rows: eansMultiCd } = await client.query(`
      WITH ean_norm AS (
        SELECT cd_codigo, mat_codi, NULLIF(LTRIM(ean_codi,'0'),'') AS ean
          FROM cd_ean
         WHERE NULLIF(LTRIM(ean_codi,'0'),'') IS NOT NULL
      )
      SELECT ean, ARRAY_AGG(DISTINCT cd_codigo) AS cds
        FROM ean_norm
       GROUP BY ean
      HAVING COUNT(DISTINCT cd_codigo) >= 2
    `);
    stats.eans_avaliados = eansMultiCd.length;

    // 2) Pra cada EAN, pega o mat_codi + mat_desc de cada CD
    for (const row of eansMultiCd) {
      const ean = row.ean;

      // Verifica se já existe canônico pra esse EAN (não recria)
      const { rows: jaExiste } = await client.query(
        `SELECT id FROM produto_canonico WHERE ean_canonico = $1 LIMIT 1`, [ean]);
      if (jaExiste.length) continue;

      // Busca todos mat_codi+desc do CD pra esse EAN
      const { rows: matches } = await client.query(`
        SELECT ce.cd_codigo, ce.mat_codi, cm.mat_desc
          FROM cd_ean ce
          JOIN cd_material cm ON cm.cd_codigo = ce.cd_codigo AND cm.mat_codi = ce.mat_codi
         WHERE NULLIF(LTRIM(ce.ean_codi,'0'),'') = $1
      `, [ean]);

      if (matches.length < 2) continue; // precisa de pelo menos 2 CDs reais

      // Avalia similaridade entre todas as descrições
      const descs = matches.map(m => normalizarDesc(m.mat_desc));
      let maxLev = 0;
      for (let i = 0; i < descs.length; i++) {
        for (let j = i + 1; j < descs.length; j++) {
          maxLev = Math.max(maxLev, levenshtein(descs[i], descs[j]));
        }
      }

      // Filtra mat_codis que JÁ estão em outro canônico (não muda vínculo automático)
      const matsLivres = [];
      for (const m of matches) {
        const { rows: jaVinculado } = await client.query(
          `SELECT 1 FROM produto_canonico_match WHERE cd_codigo=$1 AND mat_codi=$2 LIMIT 1`,
          [m.cd_codigo, m.mat_codi]);
        if (!jaVinculado.length) matsLivres.push(m);
      }
      if (matsLivres.length < 2) continue;

      const conflito = maxLev > LEV_MAX;
      const descCanon = matsLivres[0].mat_desc; // primeira descrição
      const { rows: [novo] } = await client.query(`
        INSERT INTO produto_canonico (descricao_canonica, ean_canonico, auto_validado, conflito, criado_por)
        VALUES ($1, $2, $3, $4, 'auto-match') RETURNING id
      `, [descCanon, ean, !conflito, conflito]);

      for (const m of matsLivres) {
        await client.query(`
          INSERT INTO produto_canonico_match (produto_canonico_id, cd_codigo, mat_codi, mat_desc, ean_codi, origem_match)
          VALUES ($1, $2, $3, $4, $5, $6)
          ON CONFLICT (cd_codigo, mat_codi) DO NOTHING
        `, [novo.id, m.cd_codigo, m.mat_codi, m.mat_desc, ean, conflito ? 'auto-conflito' : 'auto']);
        stats.matches_criados++;
      }
      stats.canonicos_criados++;
      if (conflito) stats.conflitos++;
    }
  } finally {
    client.release();
  }
  return stats;
}

// Conta produtos do CD origem que não têm match em algum outro CD ativo
async function contarPendencias() {
  const { rows } = await pool.query(`
    SELECT
      (SELECT COUNT(*)::int FROM produto_canonico WHERE conflito=TRUE AND descartado=FALSE) AS conflitos,
      (SELECT COUNT(*)::int FROM produto_canonico WHERE auto_validado=TRUE AND validado_em IS NULL AND descartado=FALSE) AS aguardando_revisao,
      (SELECT COUNT(*)::int FROM cd_material cm
        WHERE NOT EXISTS (
          SELECT 1 FROM produto_canonico_match m
           WHERE m.cd_codigo = cm.cd_codigo AND m.mat_codi = cm.mat_codi
        ) AND (cm.mat_situ = 'A' OR cm.mat_situ IS NULL)
      ) AS produtos_sem_canonico
  `);
  return rows[0];
}

module.exports = { rodarAutoMatch, contarPendencias, normalizarDesc, levenshtein };
