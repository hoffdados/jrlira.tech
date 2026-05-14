const express = require('express');
const router = express.Router();
const xlsx = require('xlsx');
const { query } = require('../db');
const { autenticar, apenasAdmin } = require('../auth');

// GET /api/auditagem-divergencias?status=&loja_id=&origem_cd_codigo=&desde=&ate=&limit=&offset=
// Lista apenas divergencias de notas de transferencia CD -> loja (n.origem = 'cd').
// Divergencias de NF-e fornecedor sao tratadas em outro fluxo.
router.get('/', autenticar, async (req, res) => {
  try {
    const where = [`n.origem = 'cd'`];
    const params = [];
    if (req.query.status) { params.push(req.query.status); where.push(`d.status = $${params.length}`); }
    if (req.query.loja_id) { params.push(req.query.loja_id); where.push(`d.loja_id = $${params.length}`); }
    if (req.query.origem_cd_codigo) {
      params.push(String(req.query.origem_cd_codigo).trim());
      where.push(`n.origem_cd_codigo = $${params.length}`);
    }
    if (req.query.desde) { params.push(req.query.desde); where.push(`d.criado_em >= $${params.length}::date`); }
    if (req.query.ate) { params.push(req.query.ate); where.push(`d.criado_em < ($${params.length}::date + INTERVAL '1 day')`); }
    if (req.query.q) {
      params.push(`%${req.query.q}%`);
      where.push(`(d.descricao ILIKE $${params.length} OR d.cd_pro_codi ILIKE $${params.length} OR d.ean_nota ILIKE $${params.length})`);
    }
    const limit = Math.min(parseInt(req.query.limit) || 200, 2000);
    const offset = Math.max(parseInt(req.query.offset) || 0, 0);
    const whereSql = where.join(' AND ');

    const total = await query(
      `SELECT COUNT(*)::int AS total,
              COALESCE(SUM(d.valor_total_diferenca) FILTER (WHERE d.diferenca < 0),0)::numeric(14,2) AS total_falta,
              COALESCE(SUM(d.valor_total_diferenca) FILTER (WHERE d.diferenca > 0),0)::numeric(14,2) AS total_sobra
         FROM auditagem_divergencias d
         JOIN notas_entrada n ON n.id = d.nota_id
        WHERE ${whereSql}`,
      params
    );

    const lst = await query(
      `SELECT d.*, n.cd_mov_codi, n.origem_cd_codigo, n.data_emissao, n.conferida_em, n.conferida_por
         FROM auditagem_divergencias d
         JOIN notas_entrada n ON n.id = d.nota_id
        WHERE ${whereSql}
        ORDER BY d.criado_em DESC, d.id DESC
        LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
      [...params, limit, offset]
    );
    res.json({ total: total[0].total, total_falta: total[0].total_falta, total_sobra: total[0].total_sobra, limit, offset, rows: lst });
  } catch (e) {
    console.error('[auditagem GET]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// GET /api/auditagem-divergencias/stats
router.get('/stats', autenticar, async (req, res) => {
  try {
    const r = await query(`
      SELECT status, loja_id, COUNT(*)::int AS qtd,
             SUM(valor_total_diferenca)::numeric(14,2) AS valor_total
        FROM auditagem_divergencias
       GROUP BY status, loja_id
       ORDER BY status, loja_id
    `);
    res.json({ por_status_loja: r });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/auditagem-divergencias/:id/resolver
//   body: { tipo: 'resolvida_cd' | 'devolvida_loja' | 'baixada', observacao?, numero_nf_devolucao? }
router.post('/:id/resolver', autenticar, async (req, res) => {
  try {
    const { tipo, observacao, numero_nf_devolucao } = req.body || {};
    if (!['resolvida_cd', 'devolvida_loja', 'baixada'].includes(tipo)) {
      return res.status(400).json({ erro: 'tipo inválido' });
    }
    if (tipo === 'devolvida_loja' && !numero_nf_devolucao) {
      return res.status(400).json({ erro: 'numero_nf_devolucao obrigatório pra devolução' });
    }
    const r = await query(
      `UPDATE auditagem_divergencias
          SET status=$2, observacao=$3, numero_nf_devolucao=$4,
              resolvido_em=NOW(), resolvido_por=$5
        WHERE id=$1 AND status='pendente'
        RETURNING *`,
      [req.params.id, tipo, observacao || null, numero_nf_devolucao || null,
       req.usuario.nome || req.usuario.usuario]
    );
    if (!r.length) return res.status(404).json({ erro: 'Divergência não encontrada ou já resolvida' });
    if (tipo === 'devolvida_loja') {
      await criarOuAdicionarDevolucao(r[0], 'auditagem_divergencia', req.usuario, observacao);
    }
    res.json(r[0]);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

const CD_CNPJ_AD = '17764296000209';
async function criarOuAdicionarDevolucao(div, origemTipo, usuario, obs) {
  // Devolução só faz sentido pra FALTA (diferenca < 0). Pra sobra, é o CD que devolve pra ele mesmo.
  const dif = parseFloat(div.diferenca);
  if (dif >= 0) return;
  let [dev] = await query(
    `SELECT id FROM devolucoes WHERE nota_id=$1 AND destinatario_cnpj=$2 AND status='aguardando'`,
    [div.nota_id, CD_CNPJ_AD]
  );
  if (!dev) {
    const r = await query(
      `INSERT INTO devolucoes
          (nota_id, loja_id, tipo, destinatario_cnpj, destinatario_nome, motivo, criado_por)
         VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id`,
      [div.nota_id, div.loja_id, 'cd', CD_CNPJ_AD, 'CD - Centro de Distribuição',
       'divergencia_quantidade', usuario.nome || usuario.usuario]
    );
    dev = r[0];
  }
  // Pra divergência, devolve a qtd em UNIDADES (sem arredondar pra cx — pode ser parcial)
  const qtdUn = Math.abs(dif);
  const valor = qtdUn * (parseFloat(div.valor_unitario) || 0);
  await query(
    `INSERT INTO devolucoes_itens
        (devolucao_id, item_nota_id, cd_pro_codi, ean, descricao,
         qtd_caixas, qtd_unidades, qtd_total, valor_unitario, valor_total,
         origem_tipo, origem_id)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
    [dev.id, div.item_id, div.cd_pro_codi, div.ean_nota, div.descricao,
     0, qtdUn, qtdUn, div.valor_unitario, valor,
     origemTipo, div.id]
  );
  await query(
    `UPDATE devolucoes SET valor_total = (SELECT COALESCE(SUM(valor_total),0) FROM devolucoes_itens WHERE devolucao_id=$1) WHERE id=$1`,
    [dev.id]
  );
}

// POST /api/auditagem-divergencias/resolver-massa
//   body: { ids: [], tipo, observacao?, numero_nf_devolucao? }
router.post('/resolver-massa', apenasAdmin, async (req, res) => {
  try {
    const { ids, tipo, observacao, numero_nf_devolucao } = req.body || {};
    if (!Array.isArray(ids) || !ids.length) return res.status(400).json({ erro: 'ids obrigatório' });
    if (!['resolvida_cd', 'devolvida_loja', 'baixada'].includes(tipo)) {
      return res.status(400).json({ erro: 'tipo inválido' });
    }
    const r = await query(
      `UPDATE auditagem_divergencias
          SET status=$2, observacao=$3, numero_nf_devolucao=$4,
              resolvido_em=NOW(), resolvido_por=$5
        WHERE id = ANY($1::int[]) AND status='pendente'
        RETURNING id`,
      [ids, tipo, observacao || null, numero_nf_devolucao || null,
       req.usuario.nome || req.usuario.usuario]
    );
    res.json({ ok: true, atualizados: r.length });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/auditagem-divergencias/export.xlsx
router.get('/export.xlsx', autenticar, async (req, res) => {
  try {
    const where = [`n.origem = 'cd'`];
    const params = [];
    if (req.query.status) { params.push(req.query.status); where.push(`d.status = $${params.length}`); }
    if (req.query.loja_id) { params.push(req.query.loja_id); where.push(`d.loja_id = $${params.length}`); }
    if (req.query.origem_cd_codigo) {
      params.push(String(req.query.origem_cd_codigo).trim());
      where.push(`n.origem_cd_codigo = $${params.length}`);
    }
    if (req.query.desde) { params.push(req.query.desde); where.push(`d.criado_em >= $${params.length}::date`); }
    if (req.query.ate) { params.push(req.query.ate); where.push(`d.criado_em < ($${params.length}::date + INTERVAL '1 day')`); }
    const whereSql = where.join(' AND ');

    const rows = await query(
      `SELECT d.id, d.loja_id, n.cd_mov_codi, n.data_emissao,
              d.cd_pro_codi, d.descricao, d.ean_nota,
              d.qtd_esperada, d.qtd_contada, d.diferenca,
              d.valor_unitario, d.valor_total_diferenca,
              d.status, d.observacao, d.numero_nf_devolucao,
              d.criado_em, d.resolvido_em, d.resolvido_por
         FROM auditagem_divergencias d
         JOIN notas_entrada n ON n.id = d.nota_id
        WHERE ${whereSql}
        ORDER BY d.loja_id, d.criado_em, d.id`,
      params
    );

    const ws = xlsx.utils.json_to_sheet(rows.map(r => ({
      'ID': r.id,
      'Loja': r.loja_id,
      'MCP CD': r.cd_mov_codi,
      'Data Nota': r.data_emissao ? new Date(r.data_emissao).toISOString().slice(0,10) : '',
      'MAT_CODI': r.cd_pro_codi || '',
      'Descrição': (r.descricao || '').trim(),
      'EAN': r.ean_nota || '',
      'Qtd Esperada': Number(r.qtd_esperada),
      'Qtd Contada': Number(r.qtd_contada),
      'Diferença': Number(r.diferenca),
      'Tipo': Number(r.diferenca) < 0 ? 'FALTA' : 'SOBRA',
      'Preço Unit (R$)': Number(r.valor_unitario || 0),
      'Valor Diferença (R$)': Number(r.valor_total_diferenca || 0),
      'Status': r.status,
      'Observação': r.observacao || '',
      'NF Devolução': r.numero_nf_devolucao || '',
      'Detectada em': r.criado_em ? new Date(r.criado_em).toLocaleString('pt-BR') : '',
      'Resolvida em': r.resolvido_em ? new Date(r.resolvido_em).toLocaleString('pt-BR') : '',
      'Resolvida por': r.resolvido_por || '',
    })));
    ws['!cols'] = [{wch:6},{wch:5},{wch:9},{wch:11},{wch:11},{wch:40},{wch:15},{wch:11},{wch:11},{wch:10},{wch:7},{wch:11},{wch:14},{wch:14},{wch:30},{wch:14},{wch:18},{wch:18},{wch:18}];
    const wb = xlsx.utils.book_new();
    xlsx.utils.book_append_sheet(wb, ws, 'Divergências CD');
    const buf = xlsx.write(wb, { type: 'buffer', bookType: 'xlsx' });
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="auditagem_divergencias_${new Date().toISOString().slice(0,10)}.xlsx"`);
    res.send(buf);
  } catch (e) {
    console.error('[auditagem export]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

module.exports = router;
