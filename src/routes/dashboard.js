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

module.exports = router;
