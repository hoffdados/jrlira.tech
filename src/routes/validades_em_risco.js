const express = require('express');
const router = express.Router();
const { query, pool } = require('../db');
const { autenticar, apenasAdmin } = require('../auth');

// GET /api/validades-em-risco?status=&loja_id=&q=
router.get('/', autenticar, async (req, res) => {
  try {
    const where = ['1=1'];
    const params = [];
    if (req.query.status) { params.push(req.query.status); where.push(`v.status = $${params.length}`); }
    if (req.query.loja_id) { params.push(req.query.loja_id); where.push(`v.loja_id = $${params.length}`); }
    if (req.query.q) {
      params.push(`%${req.query.q}%`);
      where.push(`(v.descricao ILIKE $${params.length} OR v.cd_pro_codi ILIKE $${params.length} OR v.ean ILIKE $${params.length})`);
    }
    const limit = Math.min(parseInt(req.query.limit) || 200, 2000);
    const offset = Math.max(parseInt(req.query.offset) || 0, 0);
    const whereSql = where.join(' AND ');

    const stats = await query(
      `SELECT COUNT(*)::int AS total,
              COALESCE(SUM(valor_em_risco),0)::numeric(14,2) AS total_em_risco,
              COUNT(*) FILTER (WHERE motivo_risco='ja_vencido')::int AS ja_vencidos,
              COUNT(*) FILTER (WHERE motivo_risco='sem_historico_vendas')::int AS sem_historico
         FROM validades_em_risco v WHERE ${whereSql}`,
      params
    );
    const rows = await query(
      `SELECT v.*, n.cd_mov_codi, n.conferida_em, n.conferida_por
         FROM validades_em_risco v
         JOIN notas_entrada n ON n.id = v.nota_id
        WHERE ${whereSql}
        ORDER BY v.dias_ate_vencer ASC NULLS LAST, v.valor_em_risco DESC
        LIMIT $${params.length+1} OFFSET $${params.length+2}`,
      [...params, limit, offset]
    );
    res.json({ ...stats[0], limit, offset, rows });
  } catch (e) {
    console.error('[validades GET]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// POST /api/validades-em-risco/:id/decidir  body: { tipo: 'liberado' | 'devolucao', observacao? }
router.post('/:id/decidir', autenticar, async (req, res) => {
  try {
    const { tipo, observacao } = req.body || {};
    if (!['liberado', 'devolucao'].includes(tipo)) return res.status(400).json({ erro: 'tipo inválido' });
    const r = await query(
      `UPDATE validades_em_risco
          SET status=$2, observacao=$3, decidido_em=NOW(), decidido_por=$4
        WHERE id=$1 AND status='pendente'
        RETURNING *`,
      [req.params.id, tipo, observacao || null, req.usuario.nome || req.usuario.usuario]
    );
    if (!r.length) return res.status(404).json({ erro: 'Não encontrada ou já decidida' });
    if (tipo === 'devolucao') {
      await criarOuAdicionarDevolucao(r[0], 'validade_risco', req.usuario);
    }
    await checarLiberacaoNota(r[0].nota_id);
    res.json(r[0]);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

const CD_CNPJ = '17764296000209';

async function criarOuAdicionarDevolucao(validade, origemTipo, usuario) {
  // 1 devolução agregada por nota+destinatario.
  // Destinatário: CD se transferência; senão fornecedor da NF-e.
  const [nota] = await query(
    `SELECT id, loja_id, origem, fornecedor_cnpj, fornecedor_nome FROM notas_entrada WHERE id=$1`,
    [validade.nota_id]
  );
  if (!nota) return;
  const isCD = nota.origem === 'cd' || nota.origem === 'transferencia_loja';
  const destCnpj = isCD ? CD_CNPJ : (nota.fornecedor_cnpj || '').replace(/\D/g, '');
  const destNome = isCD ? 'CD - Centro de Distribuição' : (nota.fornecedor_nome || 'Fornecedor');
  const tipo = isCD ? 'cd' : 'fornecedor';
  let [dev] = await query(
    `SELECT id FROM devolucoes WHERE nota_id=$1 AND destinatario_cnpj=$2 AND status='aguardando'`,
    [validade.nota_id, destCnpj]
  );
  if (!dev) {
    const r = await query(
      `INSERT INTO devolucoes
          (nota_id, loja_id, tipo, destinatario_cnpj, destinatario_nome, motivo, criado_por)
         VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id`,
      [validade.nota_id, nota.loja_id, tipo, destCnpj, destNome,
       'validade_em_risco', usuario.nome || usuario.usuario]
    );
    dev = r[0];
  }
  const cx = validade.qtd_em_risco_caixas || Math.ceil(validade.qtd_em_risco / (validade.qtd_embalagem || 1));
  const total = cx * (validade.qtd_embalagem || 1);
  const valor = total * (validade.valor_unitario || 0);
  await query(
    `INSERT INTO devolucoes_itens
        (devolucao_id, item_nota_id, cd_pro_codi, ean, descricao,
         qtd_caixas, qtd_unidades, qtd_total, valor_unitario, valor_total,
         origem_tipo, origem_id)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
    [dev.id, validade.item_id, validade.cd_pro_codi, validade.ean, validade.descricao,
     cx, 0, total, validade.valor_unitario, valor,
     origemTipo, validade.id]
  );
  // Atualiza valor_total da devolução
  await query(
    `UPDATE devolucoes SET valor_total = (SELECT COALESCE(SUM(valor_total),0) FROM devolucoes_itens WHERE devolucao_id=$1) WHERE id=$1`,
    [dev.id]
  );
}

// POST /api/validades-em-risco/decidir-massa  body: { ids:[], tipo, observacao? }
router.post('/decidir-massa', apenasAdmin, async (req, res) => {
  try {
    const { ids, tipo, observacao } = req.body || {};
    if (!Array.isArray(ids) || !ids.length) return res.status(400).json({ erro: 'ids obrigatório' });
    if (!['liberado', 'devolucao'].includes(tipo)) return res.status(400).json({ erro: 'tipo inválido' });
    const r = await query(
      `UPDATE validades_em_risco
          SET status=$2, observacao=$3, decidido_em=NOW(), decidido_por=$4
        WHERE id = ANY($1::int[]) AND status='pendente'
        RETURNING DISTINCT nota_id`,
      [ids, tipo, observacao || null, req.usuario.nome || req.usuario.usuario]
    );
    for (const row of r) await checarLiberacaoNota(row.nota_id);
    res.json({ ok: true, atualizados: r.length });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

async function checarLiberacaoNota(notaId) {
  const pend = await query(
    `SELECT COUNT(*)::int AS qtd FROM validades_em_risco WHERE nota_id=$1 AND status='pendente'`,
    [notaId]
  );
  if (pend[0].qtd > 0) return;
  const dec = await query(
    `SELECT COUNT(*) FILTER (WHERE status='devolucao')::int AS qt_dev FROM validades_em_risco WHERE nota_id=$1`,
    [notaId]
  );
  const [nota] = await query(`SELECT conferida_com_divergencia, origem FROM notas_entrada WHERE id=$1`, [notaId]);
  const isCD = nota?.origem === 'cd' || nota?.origem === 'transferencia_loja';
  let novoStatus;
  if (dec[0].qt_dev > 0) novoStatus = 'aguardando_devolucao';
  else if (isCD) novoStatus = nota?.conferida_com_divergencia ? 'auditagem' : 'conferida';
  else novoStatus = 'fechada'; // NF-e fornecedor sem devolução = fecha
  await query(`UPDATE notas_entrada SET status=$2 WHERE id=$1 AND status='aguardando_admin_validade'`, [notaId, novoStatus]);
}

module.exports = router;
