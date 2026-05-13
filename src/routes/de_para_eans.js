// De-para EANs: trata UM CD por vez, comparando contra ITB (referência).
// Universo = produtos ATIVOS (vendas 90d > 0) do CD origem cujo EAN NÃO bate em ITB
// e que ainda não foram tratados (vínculo canônico ou marcado exclusivo).
// Pra cada produto pendente: mostra top candidatos do ITB por similaridade.

const express = require('express');
const router = express.Router();
const { query: dbQuery, pool } = require('../db');
const { autenticar } = require('../auth');

const CD_REF = 'srv1-itautuba';
const CDS_VALIDOS = new Set(['srv1-nprogresso', 'srv2-asafrio', 'srv2-asasantarem']);

function soAdmin(req, res, next) {
  if (!['admin','ceo'].includes(req.usuario.perfil)) return res.status(403).json({ erro: 'apenas admin/ceo' });
  next();
}

function validarCdOrigem(cd) {
  if (!CDS_VALIDOS.has(cd)) throw new Error(`cd_origem inválido: ${cd}. Use NP, AsaFrio ou AsaSantarem.`);
}

// SQL universo: ativos do CD origem cujo EAN não bate em ITB
function sqlUniverso(cdOrigem) {
  return `
    SELECT v.ean_norm, v.qtd_90d
      FROM vendas_cd_cache v
     WHERE v.cd_codigo='${cdOrigem}' AND v.qtd_90d > 0
       AND v.ean_norm IS NOT NULL
       AND NOT EXISTS (
         SELECT 1 FROM cd_ean ce
          WHERE ce.cd_codigo='${CD_REF}'
            AND NULLIF(LTRIM(ce.ean_codi,'0'),'') = v.ean_norm
       )
       AND NOT EXISTS (
         SELECT 1 FROM produto_canonico_exclusivo ex
          WHERE ex.cd_origem_codigo = v.cd_codigo
            AND ex.cd_destino_codigo = '${CD_REF}'
            AND ex.mat_codi_origem IN (
              SELECT mat_codi FROM cd_ean
               WHERE cd_codigo = v.cd_codigo AND NULLIF(LTRIM(ean_codi,'0'),'') = v.ean_norm
            )
       )
       AND NOT EXISTS (
         SELECT 1 FROM produto_canonico_match m1
          JOIN produto_canonico_match m2 ON m2.produto_canonico_id = m1.produto_canonico_id
         WHERE m1.cd_codigo = v.cd_codigo AND m2.cd_codigo = '${CD_REF}'
           AND m1.mat_codi IN (
             SELECT mat_codi FROM cd_ean
              WHERE cd_codigo = v.cd_codigo AND NULLIF(LTRIM(ean_codi,'0'),'') = v.ean_norm
           )
       )`;
}

// GET /total?cd_origem=
router.get('/total', autenticar, soAdmin, async (req, res) => {
  try {
    const cd = String(req.query.cd_origem || '');
    validarCdOrigem(cd);
    const r = await dbQuery(`SELECT COUNT(*)::int AS total FROM (${sqlUniverso(cd)}) x`);
    res.json({ total: r[0].total });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /proximo?cd_origem=&indice=N
router.get('/proximo', autenticar, soAdmin, async (req, res) => {
  try {
    const cd = String(req.query.cd_origem || '');
    validarCdOrigem(cd);
    const indice = Math.max(0, parseInt(req.query.indice || '0', 10));

    // Pega 1 produto na ordem qtd_venda DESC
    const r = await dbQuery(`
      WITH base AS (${sqlUniverso(cd)}),
      enriq AS (
        SELECT b.ean_norm, b.qtd_90d,
               (SELECT cm.mat_codi FROM cd_ean ce
                  JOIN cd_material cm ON cm.cd_codigo=ce.cd_codigo AND cm.mat_codi=ce.mat_codi
                 WHERE ce.cd_codigo='${cd}' AND NULLIF(LTRIM(ce.ean_codi,'0'),'') = b.ean_norm
                 LIMIT 1) AS mat_codi,
               (SELECT LTRIM(RTRIM(cm.mat_desc)) FROM cd_ean ce
                  JOIN cd_material cm ON cm.cd_codigo=ce.cd_codigo AND cm.mat_codi=ce.mat_codi
                 WHERE ce.cd_codigo='${cd}' AND NULLIF(LTRIM(ce.ean_codi,'0'),'') = b.ean_norm
                 LIMIT 1) AS mat_desc
          FROM base b
      )
      SELECT * FROM enriq
       ORDER BY qtd_90d DESC NULLS LAST
       OFFSET $1 LIMIT 1`, [indice]);

    if (!r.length) return res.json({ vazio: true });
    const ref = r[0];

    // Candidatos no ITB por similaridade
    const candItb = await dbQuery(`
      SELECT cm.mat_codi, LTRIM(RTRIM(cm.mat_desc)) AS mat_desc,
             similarity(cm.mat_desc, $2) AS sim,
             (SELECT STRING_AGG(DISTINCT NULLIF(LTRIM(ean_codi,'0'),''), ', ')
                FROM cd_ean WHERE cd_codigo='${CD_REF}' AND mat_codi=cm.mat_codi
                  AND ean_codi IS NOT NULL) AS eans_itb
        FROM cd_material cm
       WHERE cm.cd_codigo='${CD_REF}'
         AND cm.mat_desc IS NOT NULL
         AND similarity(cm.mat_desc, $2) > 0.3
       ORDER BY sim DESC LIMIT 5`, [CD_REF, ref.mat_desc || '']);

    // Mercado: tem EAN? candidatos?
    const mercadoEan = await dbQuery(
      `SELECT loja_id, descricao, codigobarra FROM produtos_externo
        WHERE NULLIF(LTRIM(codigobarra,'0'),'') = $1 LIMIT 10`,
      [ref.ean_norm]);
    let mercadoCand = [];
    if (!mercadoEan.length && ref.mat_desc) {
      mercadoCand = await dbQuery(`
        SELECT loja_id, descricao, codigobarra, similarity(descricao, $1) AS sim
          FROM produtos_externo
         WHERE descricao IS NOT NULL
           AND similarity(descricao, $1) > 0.3
         ORDER BY sim DESC LIMIT 3`, [ref.mat_desc]);
    }

    res.json({
      vazio: false,
      indice,
      cd_origem: cd,
      ref: {
        cd_codigo: cd,
        mat_codi: ref.mat_codi,
        mat_desc: ref.mat_desc,
        ean: ref.ean_norm,
        qtd_venda_90d: parseFloat(ref.qtd_90d),
      },
      candidatos_itb: candItb,
      mercado: { encontrado: mercadoEan, candidatos: mercadoCand },
    });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /vincular  body: { cd_origem, mat_codi_origem, mat_codi_itb }
router.post('/vincular', autenticar, soAdmin, async (req, res) => {
  const { cd_origem, mat_codi_origem, mat_codi_itb } = req.body || {};
  if (!cd_origem || !mat_codi_origem || !mat_codi_itb) {
    return res.status(400).json({ erro: 'cd_origem, mat_codi_origem, mat_codi_itb obrigatorios' });
  }
  try { validarCdOrigem(cd_origem); } catch (e) { return res.status(400).json({ erro: e.message }); }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Acha ou cria produto_canonico baseado no ITB
    const { rows: existe } = await client.query(
      `SELECT produto_canonico_id FROM produto_canonico_match
        WHERE cd_codigo=$1 AND mat_codi=$2 LIMIT 1`,
      [CD_REF, mat_codi_itb]);
    let canonicoId;
    if (existe.length) {
      canonicoId = existe[0].produto_canonico_id;
    } else {
      const { rows: mat } = await client.query(
        `SELECT cm.mat_desc, (SELECT NULLIF(LTRIM(ean_codi,'0'),'') FROM cd_ean
           WHERE cd_codigo=$1 AND mat_codi=$2 LIMIT 1) AS ean
           FROM cd_material cm WHERE cd_codigo=$1 AND mat_codi=$2`,
        [CD_REF, mat_codi_itb]);
      const { rows: novo } = await client.query(
        `INSERT INTO produto_canonico (descricao_canonica, ean_canonico, auto_validado, validado_em)
         VALUES ($1, $2, true, NOW()) RETURNING id`,
        [mat[0]?.mat_desc?.trim() || mat_codi_itb, mat[0]?.ean || null]);
      canonicoId = novo[0].id;
      await client.query(
        `INSERT INTO produto_canonico_match (produto_canonico_id, cd_codigo, mat_codi, origem_match)
         VALUES ($1, $2, $3, 'manual') ON CONFLICT DO NOTHING`,
        [canonicoId, CD_REF, mat_codi_itb]);
    }
    // Vincula o mat_codi do CD origem
    await client.query(
      `INSERT INTO produto_canonico_match (produto_canonico_id, cd_codigo, mat_codi, origem_match)
       VALUES ($1, $2, $3, 'manual') ON CONFLICT DO NOTHING`,
      [canonicoId, cd_origem, mat_codi_origem]);
    await client.query('COMMIT');
    res.json({ ok: true, canonico_id: canonicoId });
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch {}
    res.status(500).json({ erro: e.message });
  } finally { client.release(); }
});

// POST /exclusivo  body: { cd_origem, mat_codi_origem }
// Marca: produto do CD origem NÃO precisa estar no ITB (exclusivo desse CD).
router.post('/exclusivo', autenticar, soAdmin, async (req, res) => {
  const { cd_origem, mat_codi_origem } = req.body || {};
  if (!cd_origem || !mat_codi_origem) return res.status(400).json({ erro: 'cd_origem, mat_codi_origem obrigatorios' });
  try { validarCdOrigem(cd_origem); } catch (e) { return res.status(400).json({ erro: e.message }); }
  try {
    await dbQuery(
      `INSERT INTO produto_canonico_exclusivo (cd_origem_codigo, mat_codi_origem, cd_destino_codigo, marcado_por)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (cd_origem_codigo, mat_codi_origem, cd_destino_codigo) DO NOTHING`,
      [cd_origem, mat_codi_origem, CD_REF, req.usuario.email || req.usuario.nome || null]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

module.exports = router;
