const express = require('express');
const router = express.Router();
const { query } = require('../db');
const { autenticar } = require('../auth');

// GET /api/dashboard/badges
// Retorna contadores de pendência por categoria (filtrados por loja_id se aplicável)
router.get('/badges', autenticar, async (req, res) => {
  try {
    const lojaId = req.usuario.loja_id;
    const filtroLoja = lojaId ? `AND loja_id = ${parseInt(lojaId)}` : '';

    const [
      auditoria_pedidos,
      notas_auditoria,
      aguardando_devolucao,
      validades_em_risco,
      divergencias_cd,
      acordos_pendentes,
      fornecedores_pendentes,
    ] = await Promise.all([
      query(`SELECT COUNT(*)::int AS n FROM pedidos WHERE status = 'aguardando_auditoria' ${lojaId ? 'AND loja_id = $1' : ''}`, lojaId ? [lojaId] : []),
      query(`SELECT COUNT(*)::int AS n FROM notas_entrada WHERE status IN ('em_auditoria','aguardando_admin_validade') ${lojaId ? 'AND loja_id = $1' : ''}`, lojaId ? [lojaId] : []),
      query(`SELECT COUNT(*)::int AS n FROM notas_entrada WHERE status = 'aguardando_devolucao' ${lojaId ? 'AND loja_id = $1' : ''}`, lojaId ? [lojaId] : []),
      query(`SELECT COUNT(*)::int AS n FROM notas_entrada WHERE status = 'aguardando_admin_validade' ${lojaId ? 'AND loja_id = $1' : ''}`, lojaId ? [lojaId] : []),
      query(`SELECT COUNT(*)::int AS n FROM devolucoes WHERE status = 'aguardando'`),
      query(`SELECT COUNT(*)::int AS n FROM acordos_comerciais WHERE status = 'pendente_compras'`),
      query(`SELECT COUNT(*)::int AS n FROM vendedores WHERE status IN ('pendente','aguardando_cadastro')`),
    ]);

    res.json({
      auditoria_pedidos: auditoria_pedidos[0]?.n || 0,
      notas_auditoria: notas_auditoria[0]?.n || 0,
      aguardando_devolucao: aguardando_devolucao[0]?.n || 0,
      validades_em_risco: validades_em_risco[0]?.n || 0,
      divergencias_cd: divergencias_cd[0]?.n || 0,
      auditoria_acordos: acordos_pendentes[0]?.n || 0,
      fornecedores_pendentes: fornecedores_pendentes[0]?.n || 0,
    });
  } catch (err) {
    console.error('[dashboard/badges]', err.message);
    res.status(500).json({ erro: err.message });
  }
});

// GET /api/dashboard/notas
// Painel operacional de notas: agregado por status × loja, totalizadores e
// notas paradas (>N dias no fluxo, ainda não fechadas).
// Filtros opcionais: loja, dataIni (data_emissao), dataFim, fornecedor (substring).
router.get('/notas', autenticar, async (req, res) => {
  try {
    const lojaUsr = req.usuario.loja_id;
    const lojaParam = req.query.loja ? parseInt(req.query.loja) : null;
    const lojaId = lojaUsr || lojaParam;
    const dataIni = req.query.dataIni || null;
    const dataFim = req.query.dataFim || null;
    const fornecedor = (req.query.fornecedor || '').trim();
    const diasParado = parseInt(req.query.diasParado) || 7;

    const conds = [];
    const params = [];
    if (lojaId)  { params.push(lojaId);  conds.push(`n.loja_id = $${params.length}`); }
    if (dataIni) { params.push(dataIni); conds.push(`n.data_emissao >= $${params.length}`); }
    if (dataFim) { params.push(dataFim); conds.push(`n.data_emissao <= $${params.length}`); }
    if (fornecedor) {
      params.push(`%${fornecedor}%`);
      conds.push(`(n.fornecedor_nome ILIKE $${params.length} OR n.fornecedor_cnpj ILIKE $${params.length})`);
    }
    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';

    const [porStatus, porLoja, paradas, tot] = await Promise.all([
      query(`
        SELECT n.status,
               COUNT(*)::int                            AS qtd,
               COALESCE(SUM(n.valor_total),0)::numeric  AS valor,
               COUNT(*) FILTER (WHERE n.emergencial)::int AS qtd_emergencial
          FROM notas_entrada n
          ${where}
         GROUP BY n.status
         ORDER BY n.status
      `, params),
      query(`
        SELECT n.loja_id,
               COALESCE(l.nome, 'Sem loja') AS loja_nome,
               n.status,
               COUNT(*)::int                            AS qtd,
               COALESCE(SUM(n.valor_total),0)::numeric  AS valor
          FROM notas_entrada n
          LEFT JOIN lojas l ON l.id = n.loja_id
          ${where}
         GROUP BY n.loja_id, l.nome, n.status
         ORDER BY n.loja_id, n.status
      `, params),
      query(`
        SELECT n.id, n.numero_nota, n.fornecedor_nome, n.fornecedor_cnpj,
               n.valor_total, n.status, n.loja_id, n.emergencial,
               COALESCE(l.nome,'Sem loja') AS loja_nome,
               n.data_emissao, n.importado_em,
               (EXTRACT(EPOCH FROM (NOW() - n.importado_em)) / 86400)::int AS dias
          FROM notas_entrada n
          LEFT JOIN lojas l ON l.id = n.loja_id
          ${where ? where + ' AND' : 'WHERE'} n.status NOT IN ('fechada','cancelada')
            AND n.importado_em < NOW() - ($${params.length + 1} || ' days')::interval
         ORDER BY n.importado_em ASC
         LIMIT 100
      `, [...params, String(diasParado)]),
      query(`
        SELECT COUNT(*)::int                            AS qtd,
               COALESCE(SUM(n.valor_total),0)::numeric  AS valor,
               COUNT(*) FILTER (WHERE n.emergencial)::int AS qtd_emergencial,
               COUNT(*) FILTER (WHERE n.status = 'fechada')::int AS qtd_fechadas,
               COUNT(*) FILTER (WHERE n.status NOT IN ('fechada','cancelada'))::int AS qtd_em_andamento
          FROM notas_entrada n
          ${where}
      `, params),
    ]);

    res.json({
      por_status: porStatus,
      por_loja: porLoja,
      paradas,
      total: tot[0],
      filtros: { loja: lojaId, dataIni, dataFim, fornecedor, diasParado }
    });
  } catch (err) {
    console.error('[dashboard/notas]', err.message);
    res.status(500).json({ erro: err.message });
  }
});

module.exports = router;
