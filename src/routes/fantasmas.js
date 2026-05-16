const express = require('express');
const router = express.Router();
const { query: dbQuery } = require('../db');
const { autenticar, exigirPerfil } = require('../auth');
const detector = require('../detector_fantasmas');

const adminOuCeo = [autenticar, exigirPerfil('admin', 'ceo')];

// GET /api/admin/fantasmas?status=pendente
router.get('/', adminOuCeo, async (req, res) => {
  try {
    const status = req.query.status || 'pendente';
    const rows = await dbQuery(
      `SELECT id, loja_id, numeronfe, fornecedor_cnpj, fornecedor_nome,
              motivo, twin_numeronfe, qtd_itens, valor_total,
              data_entrada::date AS data_entrada,
              detectado_em, status, resolvido_em, resolvido_por, resolucao
         FROM compras_fantasmas
        WHERE status = $1
        ORDER BY loja_id, data_entrada DESC
        LIMIT 1000`, [status]
    );
    const totais = await dbQuery(
      `SELECT status, COUNT(*)::int AS qtd, COALESCE(SUM(valor_total),0)::numeric(14,2) AS valor
         FROM compras_fantasmas
        GROUP BY status`
    );
    res.json({ status, total: rows.length, fantasmas: rows, totais });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/admin/fantasmas/detectar
router.post('/detectar', adminOuCeo, async (req, res) => {
  try {
    const r = await detector.rodar();
    res.json(r);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/admin/fantasmas/:id/apagar
router.post('/:id/apagar', adminOuCeo, async (req, res) => {
  try {
    const usuario = req.usuario.email || req.usuario.usuario || req.usuario.nome;
    const r = await detector.apagar(parseInt(req.params.id), usuario);
    res.json(r);
  } catch (e) { res.status(e.status || 500).json({ erro: e.message }); }
});

// POST /api/admin/fantasmas/:id/ignorar
router.post('/:id/ignorar', adminOuCeo, async (req, res) => {
  try {
    const usuario = req.usuario.email || req.usuario.usuario || req.usuario.nome;
    const motivo = req.body?.motivo;
    const r = await detector.ignorar(parseInt(req.params.id), usuario, motivo);
    res.json(r);
  } catch (e) { res.status(e.status || 500).json({ erro: e.message }); }
});

// POST /api/admin/fantasmas/apagar-em-massa { ids: [1,2,3] }
router.post('/apagar-em-massa', adminOuCeo, async (req, res) => {
  try {
    const usuario = req.usuario.email || req.usuario.usuario || req.usuario.nome;
    const ids = Array.isArray(req.body?.ids) ? req.body.ids.map(n => parseInt(n)).filter(Boolean) : [];
    if (!ids.length) return res.status(400).json({ erro: 'ids vazio' });
    const resultados = [];
    for (const id of ids) {
      try {
        const r = await detector.apagar(id, usuario);
        resultados.push({ id, ok: true, rows: r.rows_apagadas });
      } catch (e) {
        resultados.push({ id, ok: false, erro: e.message });
      }
    }
    res.json({ resultados });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

module.exports = router;
