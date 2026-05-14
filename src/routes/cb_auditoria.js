// Casa Branca — Auditoria de Saídas
// CB é fornecedora interna de uso/consumo pra todas as lojas/CDs.
// Cruzamento sempre via MCP_NNOTAFIS (saída CB) = numeronfe (entrada loja),
// normalizado removendo zeros à esquerda.
//
// 3 categorias:
//   ✅ Confirmada       — saída CB UltraSyst tem entrada correspondente em loja/CD
//   ⏳ Em trânsito      — saída CB UltraSyst sem entrada confirmada
//   ⚠️ Origem desconhecida — entrada loja com CNPJ CB mas sem saída no UltraSyst (NF de outro sistema)

const express = require('express');
const router = express.Router();
const { query: dbQuery } = require('../db');
const { autenticar, exigirPerfil } = require('../auth');

const CB_CD_CODIGO = 'srv3-casabranca';
const CB_CNPJ = '07961363000132';
const adminOuCeo = [autenticar, exigirPerfil('admin', 'ceo', 'comprador')];

// GET /api/cb-auditoria/resumo?mes=YYYY-MM
router.get('/resumo', adminOuCeo, async (req, res) => {
  try {
    const mes = String(req.query.mes || new Date().toISOString().slice(0, 7));

    // Confirmadas (saída CB com match em compras_historico) — agregado por loja
    const confirmadas = await dbQuery(`
      SELECT ch.loja_id,
             COUNT(*)::int AS qtd,
             COALESCE(SUM(ch.custo_total), 0)::numeric(14,2) AS valor
        FROM compras_historico ch
       WHERE REGEXP_REPLACE(ch.fornecedor_cnpj, '\\D', '', 'g') = $1
         AND TO_CHAR(ch.data_entrada, 'YYYY-MM') = $2
         AND EXISTS (
           SELECT 1 FROM cd_movcompra mc
            WHERE mc.cd_codigo = $3 AND mc.mcp_tipomov = 'S' AND mc.mcp_status <> 'C'
              AND REGEXP_REPLACE(COALESCE(mc.mcp_nnotafis, ''), '^0+', '') =
                  REGEXP_REPLACE(COALESCE(ch.numeronfe, ''), '^0+', '')
              AND REGEXP_REPLACE(COALESCE(mc.mcp_nnotafis, ''), '^0+', '') <> ''
         )
       GROUP BY ch.loja_id`, [CB_CNPJ, mes, CB_CD_CODIGO]);

    // Origem desconhecida (entrada loja com CNPJ CB sem match em saída CB UltraSyst)
    const desconhecidas = await dbQuery(`
      SELECT ch.loja_id,
             COUNT(*)::int AS qtd,
             COALESCE(SUM(ch.custo_total), 0)::numeric(14,2) AS valor
        FROM compras_historico ch
       WHERE REGEXP_REPLACE(ch.fornecedor_cnpj, '\\D', '', 'g') = $1
         AND TO_CHAR(ch.data_entrada, 'YYYY-MM') = $2
         AND NOT EXISTS (
           SELECT 1 FROM cd_movcompra mc
            WHERE mc.cd_codigo = $3 AND mc.mcp_tipomov = 'S' AND mc.mcp_status <> 'C'
              AND REGEXP_REPLACE(COALESCE(mc.mcp_nnotafis, ''), '^0+', '') =
                  REGEXP_REPLACE(COALESCE(ch.numeronfe, ''), '^0+', '')
              AND REGEXP_REPLACE(COALESCE(mc.mcp_nnotafis, ''), '^0+', '') <> ''
         )
       GROUP BY ch.loja_id`, [CB_CNPJ, mes, CB_CD_CODIGO]);

    // Em trânsito (saída CB UltraSyst sem entrada em qualquer loja)
    const transito = await dbQuery(`
      SELECT COUNT(*)::int AS qtd,
             COALESCE(SUM(mc.mcp_vtot), 0)::numeric(14,2) AS valor
        FROM cd_movcompra mc
       WHERE mc.cd_codigo = $1 AND mc.mcp_tipomov = 'S' AND mc.mcp_status <> 'C'
         AND TO_CHAR(mc.mcp_dtem, 'YYYY-MM') = $2
         AND mc.mcp_nnotafis IS NOT NULL AND mc.mcp_nnotafis <> ''
         AND NOT EXISTS (
           SELECT 1 FROM compras_historico ch
            WHERE REGEXP_REPLACE(ch.fornecedor_cnpj, '\\D', '', 'g') = $3
              AND REGEXP_REPLACE(COALESCE(ch.numeronfe, ''), '^0+', '') =
                  REGEXP_REPLACE(COALESCE(mc.mcp_nnotafis, ''), '^0+', '')
         )`, [CB_CD_CODIGO, mes, CB_CNPJ]);

    // Saídas CB total no mês (pra contexto)
    const saidasCb = await dbQuery(`
      SELECT COUNT(*)::int AS qtd,
             COALESCE(SUM(mcp_vtot), 0)::numeric(14,2) AS valor
        FROM cd_movcompra
       WHERE cd_codigo = $1 AND mcp_tipomov = 'S' AND mcp_status <> 'C'
         AND TO_CHAR(mcp_dtem, 'YYYY-MM') = $2
         AND mcp_nnotafis IS NOT NULL AND mcp_nnotafis <> ''`, [CB_CD_CODIGO, mes]);

    // Lojas pra catálogo (montagem de tabela completa)
    const lojas = await dbQuery(`SELECT id, nome FROM lojas ORDER BY id`);

    // Agrega totais
    const sum = (rows) => rows.reduce((acc, r) => ({ qtd: acc.qtd + r.qtd, valor: acc.valor + parseFloat(r.valor || 0) }), { qtd: 0, valor: 0 });
    const totConf = sum(confirmadas);
    const totDesc = sum(desconhecidas);

    res.json({
      mes,
      cb_cnpj: CB_CNPJ,
      kpis: {
        saidas_cb_mes: { qtd: saidasCb[0]?.qtd || 0, valor: parseFloat(saidasCb[0]?.valor || 0) },
        confirmadas: totConf,
        em_transito: { qtd: transito[0]?.qtd || 0, valor: parseFloat(transito[0]?.valor || 0) },
        origem_desconhecida: totDesc,
      },
      por_loja: { confirmadas, desconhecidas },
      lojas,
    });
  } catch (e) {
    console.error('[cb-auditoria/resumo]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// GET /api/cb-auditoria/produtos?mes=YYYY-MM&loja_id=&limit=
// Top produtos: agrupa por (cd_pro_codi via cd_itemcompra) das saídas CB confirmadas
router.get('/produtos', adminOuCeo, async (req, res) => {
  try {
    const mes = String(req.query.mes || new Date().toISOString().slice(0, 7));
    const lojaId = req.query.loja_id ? parseInt(req.query.loja_id) : null;
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);

    const params = [CB_CD_CODIGO, mes];
    let lojaCond = '';
    if (lojaId) {
      params.push(lojaId);
      lojaCond = `AND ch.loja_id = $${params.length}`;
    }
    params.push(CB_CNPJ);
    const cnpjIdx = params.length;
    params.push(limit);
    const limitIdx = params.length;

    const rows = await dbQuery(`
      WITH saidas_match AS (
        SELECT mc.mcp_codi,
               REGEXP_REPLACE(COALESCE(mc.mcp_nnotafis,''),'^0+','') AS nf_norm
          FROM cd_movcompra mc
         WHERE mc.cd_codigo = $1 AND mc.mcp_tipomov = 'S' AND mc.mcp_status <> 'C'
           AND TO_CHAR(mc.mcp_dtem, 'YYYY-MM') = $2
           AND mc.mcp_nnotafis IS NOT NULL AND mc.mcp_nnotafis <> ''
           AND EXISTS (
             SELECT 1 FROM compras_historico ch
              WHERE REGEXP_REPLACE(ch.fornecedor_cnpj,'\\D','','g') = $${cnpjIdx}
                AND TO_CHAR(ch.data_entrada,'YYYY-MM') = $2
                ${lojaCond}
                AND REGEXP_REPLACE(COALESCE(ch.numeronfe,''),'^0+','') =
                    REGEXP_REPLACE(COALESCE(mc.mcp_nnotafis,''),'^0+','')
           )
      )
      SELECT i.pro_codi,
             COALESCE(m.mat_desc, '(sem descrição)') AS descricao,
             COALESCE(m.mat_refe, '') AS referencia,
             SUM(i.mcp_quan)::numeric(14,3) AS qtd_total,
             SUM(i.mcp_quan * i.mcp_vuni)::numeric(14,2) AS valor_total,
             COUNT(DISTINCT i.mcp_codi)::int AS qtd_notas
        FROM cd_itemcompra i
        JOIN saidas_match s ON s.mcp_codi = i.mcp_codi
        LEFT JOIN cd_material m ON m.cd_codigo = $1 AND m.mat_codi = i.pro_codi
       WHERE i.cd_codigo = $1 AND i.mcp_tipomov = 'S'
       GROUP BY i.pro_codi, m.mat_desc, m.mat_refe
       ORDER BY valor_total DESC
       LIMIT $${limitIdx}`, params);

    res.json({ mes, loja_id: lojaId, total: rows.length, produtos: rows });
  } catch (e) {
    console.error('[cb-auditoria/produtos]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// GET /api/cb-auditoria/notas?mes=YYYY-MM&loja_id=&categoria=confirmadas|desconhecidas|transito
router.get('/notas', adminOuCeo, async (req, res) => {
  try {
    const mes = String(req.query.mes || new Date().toISOString().slice(0, 7));
    const lojaId = req.query.loja_id ? parseInt(req.query.loja_id) : null;
    const categoria = ['confirmadas', 'desconhecidas', 'transito'].includes(req.query.categoria)
      ? req.query.categoria : 'confirmadas';

    if (categoria === 'transito') {
      const rows = await dbQuery(`
        SELECT mc.mcp_codi, mc.mcp_nnotafis, mc.mcp_dtem::date AS data,
               mc.for_codi, mc.mcp_vtot, mc.nop_codi
          FROM cd_movcompra mc
         WHERE mc.cd_codigo = $1 AND mc.mcp_tipomov = 'S' AND mc.mcp_status <> 'C'
           AND TO_CHAR(mc.mcp_dtem,'YYYY-MM') = $2
           AND mc.mcp_nnotafis IS NOT NULL AND mc.mcp_nnotafis <> ''
           AND NOT EXISTS (
             SELECT 1 FROM compras_historico ch
              WHERE REGEXP_REPLACE(ch.fornecedor_cnpj,'\\D','','g') = $3
                AND REGEXP_REPLACE(COALESCE(ch.numeronfe,''),'^0+','') =
                    REGEXP_REPLACE(COALESCE(mc.mcp_nnotafis,''),'^0+','')
           )
         ORDER BY mc.mcp_dtem DESC
         LIMIT 500`, [CB_CD_CODIGO, mes, CB_CNPJ]);
      return res.json({ mes, categoria, total: rows.length, notas: rows });
    }

    // confirmadas / desconhecidas — operam em compras_historico
    const params = [CB_CNPJ, mes];
    let lojaCond = '';
    if (lojaId) { params.push(lojaId); lojaCond = `AND ch.loja_id = $${params.length}`; }
    params.push(CB_CD_CODIGO);
    const cdIdx = params.length;

    const matchClause = `EXISTS (
      SELECT 1 FROM cd_movcompra mc
       WHERE mc.cd_codigo = $${cdIdx} AND mc.mcp_tipomov = 'S' AND mc.mcp_status <> 'C'
         AND REGEXP_REPLACE(COALESCE(mc.mcp_nnotafis,''),'^0+','') =
             REGEXP_REPLACE(COALESCE(ch.numeronfe,''),'^0+','')
         AND REGEXP_REPLACE(COALESCE(mc.mcp_nnotafis,''),'^0+','') <> ''
    )`;
    const filtroMatch = categoria === 'confirmadas' ? matchClause : `NOT ${matchClause}`;

    const rows = await dbQuery(`
      SELECT ch.loja_id, ch.numeronfe, ch.data_entrada::date AS data,
             COALESCE(SUM(ch.custo_total), 0)::numeric(14,2) AS valor,
             COUNT(*)::int AS qtd_itens
        FROM compras_historico ch
       WHERE REGEXP_REPLACE(ch.fornecedor_cnpj,'\\D','','g') = $1
         AND TO_CHAR(ch.data_entrada,'YYYY-MM') = $2
         ${lojaCond}
         AND ${filtroMatch}
       GROUP BY ch.loja_id, ch.numeronfe, ch.data_entrada
       ORDER BY ch.data_entrada DESC
       LIMIT 500`, params);
    res.json({ mes, categoria, loja_id: lojaId, total: rows.length, notas: rows });
  } catch (e) {
    console.error('[cb-auditoria/notas]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

module.exports = router;
