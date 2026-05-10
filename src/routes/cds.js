// CRUD de CDs (Centros de Distribuição) — admin gerencia conexões dos relays.
// Endpoint /health e /sample testam o relay sem persistir nada.

const express = require('express');
const router = express.Router();
const { query: dbQuery } = require('../db');
const { apenasAdmin } = require('../auth');
const { listarCds, getCd, cliente, invalidarCache } = require('../cds');

// GET /api/admin/cds — lista (com tokens mascarados)
router.get('/', apenasAdmin, async (req, res) => {
  try {
    const cds = await listarCds(false);
    res.json(cds.map(c => ({
      ...c,
      token: c.token ? `${c.token.slice(0, 4)}…${c.token.slice(-4)}` : null,
      tem_token: !!c.token,
    })));
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/admin/cds — cria
router.post('/', apenasAdmin, async (req, res) => {
  try {
    const { codigo, nome, url, token, emp_codi, loc_codi } = req.body || {};
    if (!codigo || !nome || !url || !token) {
      return res.status(400).json({ erro: 'codigo, nome, url, token sao obrigatorios' });
    }
    const [novo] = await dbQuery(
      `INSERT INTO cds (codigo, nome, url, token, emp_codi, loc_codi)
       VALUES ($1, $2, $3, $4, COALESCE($5, '001'), COALESCE($6, '001'))
       RETURNING id, codigo, nome, url, emp_codi, loc_codi, ativo, criado_em`,
      [codigo.trim().toLowerCase(), nome.trim(), url.trim(), token.trim(), emp_codi, loc_codi]
    );
    invalidarCache();
    res.json(novo);
  } catch (e) {
    if (e.code === '23505') return res.status(409).json({ erro: `codigo "${req.body?.codigo}" ja existe` });
    res.status(500).json({ erro: e.message });
  }
});

// PATCH /api/admin/cds/:id — atualiza (token opcional)
router.patch('/:id', apenasAdmin, async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    const { nome, url, token, emp_codi, loc_codi, ativo } = req.body || {};
    const sets = [];
    const vals = [];
    let i = 1;
    if (nome !== undefined)     { sets.push(`nome=$${i++}`);     vals.push(nome.trim()); }
    if (url !== undefined)      { sets.push(`url=$${i++}`);      vals.push(url.trim()); }
    if (token)                  { sets.push(`token=$${i++}`);    vals.push(token.trim()); }
    if (emp_codi !== undefined) { sets.push(`emp_codi=$${i++}`); vals.push(emp_codi); }
    if (loc_codi !== undefined) { sets.push(`loc_codi=$${i++}`); vals.push(loc_codi); }
    if (ativo !== undefined)    { sets.push(`ativo=$${i++}`);    vals.push(!!ativo); }
    if (!sets.length) return res.status(400).json({ erro: 'nada pra atualizar' });
    sets.push(`atualizado_em=NOW()`);
    vals.push(id);
    await dbQuery(`UPDATE cds SET ${sets.join(',')} WHERE id=$${i}`, vals);
    invalidarCache();
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// DELETE /api/admin/cds/:id
router.delete('/:id', apenasAdmin, async (req, res) => {
  try {
    await dbQuery(`DELETE FROM cds WHERE id=$1`, [parseInt(req.params.id)]);
    invalidarCache();
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/admin/cds/:codigo/health — testa relay
router.get('/:codigo/health', apenasAdmin, async (req, res) => {
  try {
    const cd = await getCd(req.params.codigo);
    if (!cd) return res.status(404).json({ erro: 'CD nao encontrado' });
    const c = cliente(cd);
    const t0 = Date.now();
    const h = await c.health();
    res.json({ ok: true, ms: Date.now() - t0, health: h });
  } catch (e) {
    res.json({ ok: false, erro: e.message, status: e.status || null });
  }
});

// GET /api/admin/cds/:codigo/sample — conta linhas + 5 amostras de uma tabela
router.get('/:codigo/sample', apenasAdmin, async (req, res) => {
  try {
    const tabela = req.query.tabela || 'MATERIAL';
    const cd = await getCd(req.params.codigo);
    if (!cd) return res.status(404).json({ erro: 'CD nao encontrado' });
    const c = cliente(cd);
    const t0 = Date.now();
    const cont = await c.query(
      `SELECT COUNT(*) AS total FROM ${tabela} WITH (NOLOCK)
        WHERE EMP_CODI = '${cd.emp_codi || '001'}'`
    );
    const amostra = await c.query(
      `SELECT TOP 5 * FROM ${tabela} WITH (NOLOCK)
        WHERE EMP_CODI = '${cd.emp_codi || '001'}'`
    );
    res.json({
      ok: true,
      ms: Date.now() - t0,
      tabela,
      total: cont.rows?.[0]?.total ?? null,
      amostra: amostra.rows || [],
    });
  } catch (e) {
    res.json({ ok: false, erro: e.message, status: e.status || null });
  }
});

module.exports = router;
