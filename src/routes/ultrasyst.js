const express = require('express');
const router = express.Router();
const { apenasAdmin } = require('../auth');
const ultrasyst = require('../ultrasyst');
const sync = require('../sync_ultrasyst');

// GET /api/ultrasyst/health — admin testa conectividade com o relay
router.get('/health', apenasAdmin, async (req, res) => {
  try {
    const r = await ultrasyst.health();
    res.json({ ok: true, relay: r });
  } catch (e) {
    res.status(503).json({ ok: false, erro: e.message, code: e.code, status: e.status });
  }
});

// GET /api/ultrasyst/lojas — busca as 6 lojas no UltraSyst (CLIENTE) por CNPJ
router.get('/lojas', apenasAdmin, async (req, res) => {
  try {
    const r = await ultrasyst.query(
      `SELECT CLI_CODI, RTRIM(CLI_NOME) AS nome, RTRIM(CLI_FANT) AS fantasia, CLI_CPF AS cnpj, CLI_SITU AS situacao
         FROM CLIENTE WITH (NOLOCK)
        WHERE CLI_CODI IN ('0000001661','0000002221','0000002928','0000003271','0000003563','0000003628')
        ORDER BY CLI_CODI`
    );
    res.json(r);
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

// POST /api/ultrasyst/sync — admin dispara sync manual
router.post('/sync', apenasAdmin, async (req, res) => {
  try {
    const stats = await sync.syncTransferenciasCD();
    res.json({ ok: true, stats });
  } catch (e) {
    res.status(500).json({ ok: false, erro: e.message });
  }
});

// POST /api/ultrasyst/match-recebidas — atualiza status das já recebidas
router.post('/match-recebidas', apenasAdmin, async (req, res) => {
  try {
    const stats = await sync.matchTransferenciasRecebidas();
    res.json({ ok: true, stats });
  } catch (e) {
    res.status(500).json({ ok: false, erro: e.message });
  }
});

// GET /api/ultrasyst/mapa-lojas — admin verifica mapeamento CNPJ ↔ CLI_CODI
router.get('/mapa-lojas', apenasAdmin, async (req, res) => {
  try {
    const m = await sync.mapearLojas();
    res.json({
      total_lojas_jr: Object.keys(m.porCnpj).length + (m.cnpjs.length - Object.keys(m.porCnpj).length),
      mapeadas: Object.values(m.porCnpj),
      cnpjs_jr_lira: m.cnpjs,
    });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

module.exports = router;
