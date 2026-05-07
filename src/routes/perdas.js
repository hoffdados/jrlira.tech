const express = require('express');
const router = express.Router();
const { query } = require('../db');
const { autenticar } = require('../auth');

// GET /api/perdas/resumo?de=&ate=&loja_id=&fornecedor_cnpj=&agrupar=fornecedor|loja|mes
// Resumo unificado: avarias internas (vendas_historico tipo='avaria') + devoluções de compra
// (devolucoes_compra_historico via Pentaho + devolucoes do jrlira-tech, dedup por chave_nfe).
router.get('/resumo', autenticar, async (req, res) => {
  try {
    const agrupar = String(req.query.agrupar || 'fornecedor').toLowerCase();
    const de = req.query.de || null;
    const ate = req.query.ate || null;
    const lid = req.query.loja_id ? parseInt(req.query.loja_id) : null;
    const cnpj = req.query.fornecedor_cnpj ? String(req.query.fornecedor_cnpj).replace(/\D/g, '') : null;

    // ── AVARIAS (vendas_historico) ──
    const condA = [`vh.tipo_saida = 'avaria'`];
    const paramsA = [];
    if (lid) { paramsA.push(lid); condA.push(`vh.loja_id = $${paramsA.length}::int`); }
    if (de)  { paramsA.push(de);  condA.push(`vh.data_venda >= $${paramsA.length}::date`); }
    if (ate) { paramsA.push(ate); condA.push(`vh.data_venda <= $${paramsA.length}::date`); }

    const avarias = await query(`
      SELECT vh.loja_id,
             COUNT(*)::int AS qtd_eventos,
             COALESCE(SUM(vh.qtd_vendida), 0)::numeric(14,3) AS qtd_unidades,
             COALESCE(SUM(vh.qtd_vendida * COALESCE(pe.custoorigem, 0)), 0)::numeric(14,2) AS valor_estimado
        FROM vendas_historico vh
        LEFT JOIN produtos_externo pe
          ON NULLIF(LTRIM(pe.codigobarra,'0'),'') = NULLIF(LTRIM(vh.codigobarra,'0'),'')
         AND pe.loja_id = vh.loja_id
       WHERE ${condA.join(' AND ')}
       GROUP BY vh.loja_id
       ORDER BY vh.loja_id
    `, paramsA);

    // ── DEVOLUÇÕES DE COMPRA ──
    // Fonte 1: devolucoes_compra_historico (Pentaho — histórico desde 2025-01)
    // Fonte 2: devolucoes do jrlira-tech (com XML anexado — recentes)
    // Dedup: chave_nfe — se já existe na fonte Pentaho, ignora a do jrlira-tech.
    const condD = [];
    const paramsD = [];
    if (lid)  { paramsD.push(lid);  condD.push(`d.loja_id = $${paramsD.length}::int`); }
    if (cnpj) { paramsD.push(cnpj); condD.push(`REGEXP_REPLACE(COALESCE(d.fornecedor_cnpj,''),'\\D','','g') = $${paramsD.length}`); }
    if (de)   { paramsD.push(de);   condD.push(`d.data_devolucao >= $${paramsD.length}::date`); }
    if (ate)  { paramsD.push(ate);  condD.push(`d.data_devolucao <= ($${paramsD.length}::date + INTERVAL '1 day')`); }
    const whereD = condD.length ? `WHERE ${condD.join(' AND ')}` : '';

    const devolucoes = await query(`
      WITH unificada AS (
        SELECT d.loja_id, d.fornecedor_cnpj, d.fornecedor_nome,
               d.data_devolucao, d.valor_total, d.chave_nfe
          FROM devolucoes_compra_historico d
          ${whereD}
        UNION ALL
        SELECT j.loja_id, j.destinatario_cnpj AS fornecedor_cnpj,
               j.destinatario_nome AS fornecedor_nome,
               COALESCE(j.enviada_em, j.criado_em) AS data_devolucao,
               COALESCE(j.valor_xml, j.valor_total) AS valor_total,
               j.xml_chave_nfe AS chave_nfe
          FROM devolucoes j
         WHERE j.tipo = 'fornecedor'
           AND j.status = 'enviada'
           AND NOT EXISTS (
             SELECT 1 FROM devolucoes_compra_historico h
              WHERE h.chave_nfe = j.xml_chave_nfe
                AND h.chave_nfe IS NOT NULL
           )
           ${lid ? `AND j.loja_id = ${lid}` : ''}
           ${cnpj ? `AND REGEXP_REPLACE(COALESCE(j.destinatario_cnpj,''),'\\D','','g') = '${cnpj}'` : ''}
           ${de ? `AND COALESCE(j.enviada_em, j.criado_em) >= '${de}'::date` : ''}
           ${ate ? `AND COALESCE(j.enviada_em, j.criado_em) <= ('${ate}'::date + INTERVAL '1 day')` : ''}
      )
      SELECT loja_id,
             COUNT(*)::int AS qtd_devolucoes,
             COALESCE(SUM(valor_total), 0)::numeric(14,2) AS valor_total
        FROM unificada
       GROUP BY loja_id
       ORDER BY loja_id
    `, paramsD);

    // Total geral
    const total_avaria = avarias.reduce((s, r) => s + parseFloat(r.valor_estimado || 0), 0);
    const total_devolucao = devolucoes.reduce((s, r) => s + parseFloat(r.valor_total || 0), 0);

    res.json({
      filtros: { de, ate, loja_id: lid, fornecedor_cnpj: cnpj, agrupar },
      total: {
        valor_avaria: parseFloat(total_avaria.toFixed(2)),
        valor_devolucao: parseFloat(total_devolucao.toFixed(2)),
        valor_perdas: parseFloat((total_avaria + total_devolucao).toFixed(2)),
      },
      avarias_por_loja: avarias,
      devolucoes_por_loja: devolucoes,
    });
  } catch (e) {
    console.error('[perdas/resumo]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// GET /api/perdas/top-fornecedores?de=&ate=&loja_id=&limit=10
// Ranking de fornecedores que mais geraram devolução de compra no período.
router.get('/top-fornecedores', autenticar, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 10, 50);
    const lid = req.query.loja_id ? parseInt(req.query.loja_id) : null;
    const de  = req.query.de  || null;
    const ate = req.query.ate || null;

    const conds = [];
    const params = [];
    if (lid) { params.push(lid); conds.push(`d.loja_id = $${params.length}::int`); }
    if (de)  { params.push(de);  conds.push(`d.data_devolucao >= $${params.length}::date`); }
    if (ate) { params.push(ate); conds.push(`d.data_devolucao <= ($${params.length}::date + INTERVAL '1 day')`); }
    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';

    const rows = await query(`
      SELECT REGEXP_REPLACE(COALESCE(d.fornecedor_cnpj,''),'\\D','','g') AS cnpj,
             MAX(d.fornecedor_nome) AS nome,
             COUNT(*)::int AS qtd_devolucoes,
             COALESCE(SUM(d.valor_total), 0)::numeric(14,2) AS valor_total
        FROM devolucoes_compra_historico d
        ${where}
       GROUP BY REGEXP_REPLACE(COALESCE(d.fornecedor_cnpj,''),'\\D','','g')
       ORDER BY valor_total DESC
       LIMIT ${limit}
    `, params);
    res.json({ rows });
  } catch (e) {
    console.error('[perdas/top-fornecedores]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

module.exports = router;
