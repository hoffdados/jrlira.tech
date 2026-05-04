// Endpoints públicos pro Pentaho consultar último timestamp por loja×tabela.
// Usado pra carga delta (WHERE data_venda > ultimo_sync no Firebird).
const express = require('express');
const router = express.Router();
const { query } = require('../db');

// Não usa autenticar — Pentaho local não tem token. Protege via query param de chave secreta opcional.
function checarChave(req, res) {
  const chave = process.env.SYNC_KEY;
  if (chave && req.query.k !== chave) {
    res.status(401).json({ erro: 'Chave inválida' });
    return false;
  }
  return true;
}

// GET /api/sync-status/ultima-data-venda?loja=N
//  Retorna a data_venda mais recente já carregada pra essa loja.
//  Pentaho usa pra filtrar Firebird: WHERE DATAEFE > '2026-04-25'
router.get('/ultima-data-venda', async (req, res) => {
  if (!checarChave(req, res)) return;
  try {
    const loja = parseInt(req.query.loja);
    if (!loja) return res.status(400).json({ erro: 'loja obrigatória' });
    const r = await query(`SELECT MAX(data_venda) AS ult FROM vendas_historico WHERE loja_id = $1`, [loja]);
    res.json({ loja, ultima: r[0]?.ult || null });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/sync-status/ultima-data-entrada?loja=N
router.get('/ultima-data-entrada', async (req, res) => {
  if (!checarChave(req, res)) return;
  try {
    const loja = parseInt(req.query.loja);
    if (!loja) return res.status(400).json({ erro: 'loja obrigatória' });
    const r = await query(`SELECT MAX(data_entrada) AS ult FROM compras_historico WHERE loja_id = $1`, [loja]);
    res.json({ loja, ultima: r[0]?.ult || null });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/sync-status/resumo
router.get('/resumo', async (req, res) => {
  if (!checarChave(req, res)) return;
  try {
    const v = await query(`SELECT loja_id, COUNT(*)::int qtd, MAX(data_venda) max_venda, MAX(sincronizado_em) ult_sync FROM vendas_historico GROUP BY loja_id ORDER BY loja_id`);
    const c = await query(`SELECT loja_id, COUNT(*)::int qtd, MAX(data_entrada) max_entrada, MAX(sincronizado_em) ult_sync FROM compras_historico GROUP BY loja_id ORDER BY loja_id`);
    const p = await query(`SELECT loja_id, COUNT(*)::int qtd, MAX(sincronizado_em) ult_sync FROM produtos_externo GROUP BY loja_id ORDER BY loja_id`);
    res.json({ vendas: v, compras: c, produtos: p });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/sync-status/vendas-detalhe
// Diagnóstico: mostra MIN/MAX sincronizado_em por loja_id e amostras pra detectar
// quando 1 loja está gravando com loja_id de outra (KTR mal-parametrizado).
router.get('/vendas-detalhe', async (req, res) => {
  if (!checarChave(req, res)) return;
  try {
    const r = await query(`
      SELECT loja_id,
             COUNT(*)::int AS qtd,
             MIN(data_venda) AS min_data,
             MAX(data_venda) AS max_data,
             MIN(sincronizado_em) AS min_sync,
             MAX(sincronizado_em) AS max_sync,
             COUNT(DISTINCT data_venda)::int AS dias_distintos
        FROM vendas_historico
       GROUP BY loja_id
       ORDER BY loja_id
    `);
    const ult = await query(`
      SELECT loja_id, codigobarra, data_venda, qtd_vendida, sincronizado_em
        FROM vendas_historico
       ORDER BY sincronizado_em DESC
       LIMIT 20
    `);
    res.json({ por_loja: r, ultimas_20_gravacoes: ult });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

module.exports = router;
