const express = require('express');
const router = express.Router();
const { pool } = require('../db');
const { autenticar, adminOuCeo } = require('../auth');
const { rodarAutoMatch, contarPendencias, normalizarDesc, levenshtein } = require('../produto_canonico');

const dbQuery = async (sql, params) => (await pool.query(sql, params)).rows;

// GET /pendencias — contadores das 3 abas (Pendentes, Conflitos, Aguardando revisão)
router.get('/pendencias', adminOuCeo, async (req, res) => {
  try {
    const r = await contarPendencias();
    res.json(r);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /auto-match — executa auto-match manualmente
router.post('/auto-match', adminOuCeo, async (req, res) => {
  try {
    const stats = await rodarAutoMatch();
    res.json({ ok: true, ...stats });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /conflitos — lista canônicos marcados como conflito pra revisão
router.get('/conflitos', adminOuCeo, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 100, 500);
    const canonicos = await dbQuery(`
      SELECT pc.id, pc.descricao_canonica, pc.ean_canonico, pc.criado_em
        FROM produto_canonico pc
       WHERE pc.conflito = TRUE AND pc.descartado = FALSE
       ORDER BY pc.criado_em DESC LIMIT $1`, [limit]);
    if (!canonicos.length) return res.json([]);
    const ids = canonicos.map(c => c.id);
    const matches = await dbQuery(`
      SELECT m.produto_canonico_id, m.cd_codigo, m.mat_codi, m.mat_desc, m.ean_codi,
             cm.mat_desc AS desc_atual,
             cd_e.est_quan
        FROM produto_canonico_match m
        LEFT JOIN cd_material cm ON cm.cd_codigo = m.cd_codigo AND cm.mat_codi = m.mat_codi
        LEFT JOIN cd_estoque cd_e ON cd_e.cd_codigo = m.cd_codigo AND cd_e.pro_codi = m.mat_codi
       WHERE m.produto_canonico_id = ANY($1::int[])`, [ids]);
    const matchesPorCanonico = new Map();
    for (const m of matches) {
      if (!matchesPorCanonico.has(m.produto_canonico_id)) matchesPorCanonico.set(m.produto_canonico_id, []);
      matchesPorCanonico.get(m.produto_canonico_id).push(m);
    }
    res.json(canonicos.map(c => ({ ...c, matches: matchesPorCanonico.get(c.id) || [] })));
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /aguardando-revisao — canônicos auto_validados que nunca foram revisados por admin
router.get('/aguardando-revisao', adminOuCeo, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 100, 500);
    const canonicos = await dbQuery(`
      SELECT pc.id, pc.descricao_canonica, pc.ean_canonico, pc.criado_em
        FROM produto_canonico pc
       WHERE pc.auto_validado = TRUE AND pc.validado_em IS NULL AND pc.descartado = FALSE
       ORDER BY pc.criado_em DESC LIMIT $1`, [limit]);
    if (!canonicos.length) return res.json([]);
    const ids = canonicos.map(c => c.id);
    const matches = await dbQuery(`
      SELECT m.produto_canonico_id, m.cd_codigo, m.mat_codi, m.mat_desc, m.ean_codi,
             cd_e.est_quan
        FROM produto_canonico_match m
        LEFT JOIN cd_estoque cd_e ON cd_e.cd_codigo = m.cd_codigo AND cd_e.pro_codi = m.mat_codi
       WHERE m.produto_canonico_id = ANY($1::int[])`, [ids]);
    const map = new Map();
    for (const m of matches) {
      if (!map.has(m.produto_canonico_id)) map.set(m.produto_canonico_id, []);
      map.get(m.produto_canonico_id).push(m);
    }
    res.json(canonicos.map(c => ({ ...c, matches: map.get(c.id) || [] })));
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /pendentes — produtos sem canonico em algum CD (priorizando ITB) + EAN mais usado nas lojas
router.get('/pendentes', adminOuCeo, async (req, res) => {
  try {
    const cdOrigem = String(req.query.cd_origem || 'srv1-itautuba').trim();
    const limit = Math.min(parseInt(req.query.limit) || 100, 500);
    const busca = String(req.query.busca || '').trim().toLowerCase();
    const params = [cdOrigem];
    let where = `WHERE cm.cd_codigo = $1
      AND (cm.mat_situ = 'A' OR cm.mat_situ IS NULL)
      AND NOT EXISTS (
        SELECT 1 FROM produto_canonico_match pcm
         WHERE pcm.cd_codigo = cm.cd_codigo AND pcm.mat_codi = cm.mat_codi
      )`;
    if (busca) {
      params.push(`%${busca}%`);
      where += ` AND (LOWER(cm.mat_desc) ILIKE $${params.length} OR cm.mat_codi ILIKE $${params.length} OR cm.ean_codi ILIKE $${params.length})`;
    }
    params.push(limit);
    const pendentes = await dbQuery(`
      SELECT cm.mat_codi, cm.mat_desc, cm.ean_codi, cd_e.est_quan
        FROM cd_material cm
        LEFT JOIN cd_estoque cd_e ON cd_e.cd_codigo = cm.cd_codigo AND cd_e.pro_codi = cm.mat_codi
        ${where}
       ORDER BY (cd_e.est_quan IS NULL), cd_e.est_quan DESC NULLS LAST, cm.mat_desc
       LIMIT $${params.length}
    `, params);

    // Enriquece com "EAN mais frequente nas lojas" — pega top EAN agrupado por descrição similar
    // Estratégia: pra cada descrição do CD, busca EANs nas lojas via produtos_externo.descricao ILIKE
    // Limita pra evitar query gigante: só os 200 primeiros pendentes
    const eansLojaPorMat = {};
    const topPendentes = pendentes.slice(0, 200);
    for (const p of topPendentes) {
      if (!p.mat_desc) continue;
      // Normaliza descrição pra busca: tira pontuação, pega palavras significativas
      const descBusca = p.mat_desc.replace(/[^A-Za-z0-9 ]/g, ' ').replace(/\s+/g, ' ').trim();
      const palavras = descBusca.split(' ').filter(w => w.length >= 3).slice(0, 4);
      if (palavras.length < 2) continue;
      const ilike = '%' + palavras.join('%') + '%';
      try {
        const rows = await dbQuery(`
          SELECT NULLIF(LTRIM(codigobarra,'0'),'') AS ean,
                 COUNT(DISTINCT loja_id) AS qtd_lojas,
                 MIN(descricao) AS desc_loja
            FROM produtos_externo
           WHERE descricao ILIKE $1
             AND codigobarra IS NOT NULL AND LENGTH(LTRIM(codigobarra,'0')) >= 8
           GROUP BY ean
           ORDER BY qtd_lojas DESC, ean
           LIMIT 3
        `, [ilike]);
        if (rows.length) {
          eansLojaPorMat[p.mat_codi] = rows;
        }
      } catch (e) { /* silencia, segue pro próximo */ }
    }

    res.json({ cd_origem: cdOrigem, total: pendentes.length, pendentes, eans_lojas: eansLojaPorMat });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /buscar-em-cd?cd_codigo=&q= — busca produtos num CD por descrição/EAN/mat_codi pra vincular manualmente
router.get('/buscar-em-cd', adminOuCeo, async (req, res) => {
  try {
    const cd = String(req.query.cd_codigo || '').trim();
    const q = String(req.query.q || '').trim().toLowerCase();
    if (!cd || !q) return res.status(400).json({ erro: 'cd_codigo e q obrigatorios' });
    const rows = await dbQuery(`
      SELECT cm.mat_codi, cm.mat_desc, cm.ean_codi, cd_e.est_quan,
             EXISTS (SELECT 1 FROM produto_canonico_match pcm
                      WHERE pcm.cd_codigo=cm.cd_codigo AND pcm.mat_codi=cm.mat_codi) AS ja_vinculado
        FROM cd_material cm
        LEFT JOIN cd_estoque cd_e ON cd_e.cd_codigo=cm.cd_codigo AND cd_e.pro_codi=cm.mat_codi
       WHERE cm.cd_codigo = $1
         AND (LOWER(cm.mat_desc) ILIKE $2 OR cm.mat_codi ILIKE $2 OR cm.ean_codi ILIKE $2)
       ORDER BY cm.mat_desc LIMIT 50
    `, [cd, `%${q}%`]);
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /criar-do-ean-loja — busca o EAN nas cd_ean dos 4 CDs e cria canônico com tudo que achar.
// Body: { ean, descricao_canonica }
router.post('/criar-do-ean-loja', adminOuCeo, async (req, res) => {
  const { ean, descricao_canonica } = req.body || {};
  if (!ean || !descricao_canonica) return res.status(400).json({ erro: 'ean e descricao_canonica obrigatorios' });
  const eanNorm = String(ean).replace(/^0+/, '');
  const por = req.usuario.email || req.usuario.usuario || `id:${req.usuario.id}`;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Busca mat_codi do EAN em cada CD
    const { rows: encontrados } = await client.query(`
      SELECT ce.cd_codigo, ce.mat_codi, cm.mat_desc, ce.ean_codi
        FROM cd_ean ce
        LEFT JOIN cd_material cm ON cm.cd_codigo=ce.cd_codigo AND cm.mat_codi=ce.mat_codi
       WHERE NULLIF(LTRIM(ce.ean_codi,'0'),'') = $1
    `, [eanNorm]);
    if (!encontrados.length) {
      await client.query('ROLLBACK');
      return res.status(404).json({ erro: 'EAN nao encontrado em nenhum CD', ean: eanNorm });
    }
    // Dedup por CD (pega o primeiro mat_codi de cada CD)
    const porCd = new Map();
    for (const e of encontrados) if (!porCd.has(e.cd_codigo)) porCd.set(e.cd_codigo, e);
    const matchesFinal = [...porCd.values()];

    // Checa se algum mat já está vinculado a outro canônico — se sim, aborta com aviso
    for (const m of matchesFinal) {
      const { rows: ja } = await client.query(
        `SELECT pc.descricao_canonica FROM produto_canonico_match pcm
           JOIN produto_canonico pc ON pc.id=pcm.produto_canonico_id
          WHERE pcm.cd_codigo=$1 AND pcm.mat_codi=$2 LIMIT 1`, [m.cd_codigo, m.mat_codi]);
      if (ja.length) {
        await client.query('ROLLBACK');
        return res.status(409).json({
          erro: 'conflito: produto ja vinculado a outro canonico',
          cd_codigo: m.cd_codigo, mat_codi: m.mat_codi, canonico_atual: ja[0].descricao_canonica
        });
      }
    }

    const { rows: [novo] } = await client.query(`
      INSERT INTO produto_canonico (descricao_canonica, ean_canonico, criado_por, auto_validado, validado_em, validado_por)
      VALUES ($1, $2, $3, TRUE, NOW(), $3) RETURNING id
    `, [descricao_canonica, eanNorm, por]);
    for (const m of matchesFinal) {
      await client.query(`
        INSERT INTO produto_canonico_match (produto_canonico_id, cd_codigo, mat_codi, mat_desc, ean_codi, origem_match, vinculado_por)
        VALUES ($1, $2, $3, $4, $5, 'ean-mercado', $6)
        ON CONFLICT (cd_codigo, mat_codi) DO NOTHING
      `, [novo.id, m.cd_codigo, m.mat_codi, m.mat_desc || null, m.ean_codi || null, por]);
    }
    await client.query('COMMIT');
    res.json({ ok: true, id: novo.id, matches_criados: matchesFinal.length, matches: matchesFinal });
  } catch (e) {
    await client.query('ROLLBACK');
    res.status(500).json({ erro: e.message });
  } finally {
    client.release();
  }
});

// POST /criar — cria canônico vinculando N mat_codis de CDs diferentes
// Body: { descricao_canonica, ean_canonico?, matches: [{cd_codigo, mat_codi, mat_desc, ean_codi}, ...] }
router.post('/criar', adminOuCeo, async (req, res) => {
  const { descricao_canonica, ean_canonico, matches } = req.body || {};
  if (!descricao_canonica || !Array.isArray(matches) || matches.length === 0) {
    return res.status(400).json({ erro: 'descricao_canonica e matches[] obrigatorios' });
  }
  const por = req.usuario.email || req.usuario.usuario || `id:${req.usuario.id}`;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rows: [novo] } = await client.query(`
      INSERT INTO produto_canonico (descricao_canonica, ean_canonico, criado_por, auto_validado, validado_em, validado_por)
      VALUES ($1, $2, $3, TRUE, NOW(), $3) RETURNING id
    `, [descricao_canonica, ean_canonico || null, por]);
    for (const m of matches) {
      if (!m.cd_codigo || !m.mat_codi) continue;
      await client.query(`
        INSERT INTO produto_canonico_match (produto_canonico_id, cd_codigo, mat_codi, mat_desc, ean_codi, origem_match, vinculado_por)
        VALUES ($1, $2, $3, $4, $5, 'manual', $6)
        ON CONFLICT (cd_codigo, mat_codi) DO UPDATE SET
          produto_canonico_id=EXCLUDED.produto_canonico_id,
          origem_match='manual',
          vinculado_em=NOW(),
          vinculado_por=EXCLUDED.vinculado_por
      `, [novo.id, m.cd_codigo, m.mat_codi, m.mat_desc || null, m.ean_codi || null, por]);
    }
    await client.query('COMMIT');
    res.json({ ok: true, id: novo.id });
  } catch (e) {
    await client.query('ROLLBACK');
    res.status(500).json({ erro: e.message });
  } finally {
    client.release();
  }
});

// POST /vincular — adiciona mat de um CD a canônico existente
router.post('/vincular', adminOuCeo, async (req, res) => {
  const { produto_canonico_id, cd_codigo, mat_codi, mat_desc, ean_codi } = req.body || {};
  if (!produto_canonico_id || !cd_codigo || !mat_codi) {
    return res.status(400).json({ erro: 'produto_canonico_id, cd_codigo, mat_codi obrigatorios' });
  }
  const por = req.usuario.email || req.usuario.usuario || `id:${req.usuario.id}`;
  try {
    await pool.query(`
      INSERT INTO produto_canonico_match (produto_canonico_id, cd_codigo, mat_codi, mat_desc, ean_codi, origem_match, vinculado_por)
      VALUES ($1, $2, $3, $4, $5, 'manual', $6)
      ON CONFLICT (cd_codigo, mat_codi) DO UPDATE SET
        produto_canonico_id=EXCLUDED.produto_canonico_id,
        origem_match='manual', vinculado_em=NOW(), vinculado_por=EXCLUDED.vinculado_por
    `, [produto_canonico_id, cd_codigo, mat_codi, mat_desc || null, ean_codi || null, por]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /desvincular — remove match (mat fica pendente)
router.post('/desvincular', adminOuCeo, async (req, res) => {
  const { cd_codigo, mat_codi } = req.body || {};
  if (!cd_codigo || !mat_codi) return res.status(400).json({ erro: 'cd_codigo e mat_codi obrigatorios' });
  try {
    await pool.query(`DELETE FROM produto_canonico_match WHERE cd_codigo=$1 AND mat_codi=$2`, [cd_codigo, mat_codi]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /aprovar/:id — marca canônico como validado (sai da fila aguardando-revisao)
router.post('/aprovar/:id', adminOuCeo, async (req, res) => {
  const id = parseInt(req.params.id);
  if (!id) return res.status(400).json({ erro: 'id inválido' });
  const por = req.usuario.email || req.usuario.usuario || `id:${req.usuario.id}`;
  try {
    await pool.query(`
      UPDATE produto_canonico SET validado_em=NOW(), validado_por=$2, conflito=FALSE WHERE id=$1
    `, [id, por]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /descartar/:id — marca canônico como descartado (não vai mais aparecer)
router.post('/descartar/:id', adminOuCeo, async (req, res) => {
  const id = parseInt(req.params.id);
  if (!id) return res.status(400).json({ erro: 'id inválido' });
  try {
    await pool.query(`UPDATE produto_canonico SET descartado=TRUE WHERE id=$1`, [id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /resolver-conflito/:id — admin escolhe os mat_codis corretos. Body: { matches_manter: [{cd_codigo, mat_codi}] }
router.post('/resolver-conflito/:id', adminOuCeo, async (req, res) => {
  const id = parseInt(req.params.id);
  const { matches_manter } = req.body || {};
  if (!id || !Array.isArray(matches_manter)) return res.status(400).json({ erro: 'id e matches_manter[] obrigatorios' });
  const por = req.usuario.email || req.usuario.usuario || `id:${req.usuario.id}`;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Pega todos vinculados atualmente
    const { rows: atuais } = await client.query(
      `SELECT cd_codigo, mat_codi FROM produto_canonico_match WHERE produto_canonico_id=$1`, [id]);
    const manterSet = new Set(matches_manter.map(m => `${m.cd_codigo}|${m.mat_codi}`));
    for (const a of atuais) {
      if (!manterSet.has(`${a.cd_codigo}|${a.mat_codi}`)) {
        await client.query(
          `DELETE FROM produto_canonico_match WHERE produto_canonico_id=$1 AND cd_codigo=$2 AND mat_codi=$3`,
          [id, a.cd_codigo, a.mat_codi]);
      }
    }
    await client.query(
      `UPDATE produto_canonico SET conflito=FALSE, validado_em=NOW(), validado_por=$2 WHERE id=$1`,
      [id, por]);
    await client.query('COMMIT');
    res.json({ ok: true });
  } catch (e) {
    await client.query('ROLLBACK');
    res.status(500).json({ erro: e.message });
  } finally {
    client.release();
  }
});

module.exports = router;
