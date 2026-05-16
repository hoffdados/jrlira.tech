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
// Aceita transição entre qualquer par. Ao reverter 'devolucao'→'liberado', remove o item da
// devolução vinculada (e cancela a devolução se ficar sem itens).
router.post('/:id/decidir', autenticar, async (req, res) => {
  try {
    const { tipo, observacao } = req.body || {};
    if (!['liberado', 'devolucao'].includes(tipo)) return res.status(400).json({ erro: 'tipo inválido' });
    const [antes] = await query(`SELECT id, status FROM validades_em_risco WHERE id=$1`, [req.params.id]);
    if (!antes) return res.status(404).json({ erro: 'Não encontrada' });
    const r = await query(
      `UPDATE validades_em_risco
          SET status=$2, observacao=$3, decidido_em=NOW(), decidido_por=$4
        WHERE id=$1
        RETURNING *`,
      [req.params.id, tipo, observacao || null, req.usuario.nome || req.usuario.usuario]
    );
    // Reversão devolucao → outro: limpa item da devolução vinculada
    if (antes.status === 'devolucao' && tipo !== 'devolucao') {
      const itensRemov = await query(
        `DELETE FROM devolucoes_itens
           WHERE origem_tipo='validade_risco' AND origem_id=$1
           RETURNING devolucao_id`,
        [req.params.id]
      );
      for (const it of itensRemov) {
        const [resto] = await query(
          `SELECT COUNT(*)::int AS qtd, COALESCE(SUM(valor_total),0)::numeric AS total
             FROM devolucoes_itens WHERE devolucao_id=$1`,
          [it.devolucao_id]
        );
        if (resto.qtd === 0) {
          await query(
            `UPDATE devolucoes
                SET status='cancelada', valor_total=0,
                    observacao = COALESCE(observacao,'') || ' [auto] cancelada por reversão de validade'
              WHERE id=$1 AND status='aguardando'`,
            [it.devolucao_id]
          );
        } else {
          await query(`UPDATE devolucoes SET valor_total=$2 WHERE id=$1`, [it.devolucao_id, resto.total]);
        }
      }
    }
    // Nova decisão devolucao (a partir de qualquer status): cria/adiciona item de devolução
    if (tipo === 'devolucao' && antes.status !== 'devolucao') {
      await criarOuAdicionarDevolucao(r[0], 'validade_risco', req.usuario);
    }
    await checarLiberacaoNota(r[0].nota_id);
    res.json(r[0]);
  } catch (e) {
    console.error('[validades decidir]', e.message);
    res.status(500).json({ erro: e.message });
  }
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
// Aceita transição entre 'pendente'<->'liberado'<->'devolucao'. Ao reverter 'devolucao'→'liberado',
// remove o item correspondente da devolução; se a devolução ficar sem itens, cancela ela.
router.post('/decidir-massa', apenasAdmin, async (req, res) => {
  try {
    const { ids, tipo, observacao } = req.body || {};
    if (!Array.isArray(ids) || !ids.length) return res.status(400).json({ erro: 'ids obrigatório' });
    if (!['liberado', 'devolucao'].includes(tipo)) return res.status(400).json({ erro: 'tipo inválido' });

    const r = await query(
      `WITH antes AS (
         SELECT id, status AS status_anterior FROM validades_em_risco WHERE id = ANY($1::int[])
       ),
       atualizados AS (
         UPDATE validades_em_risco
            SET status=$2, observacao=$3, decidido_em=NOW(), decidido_por=$4
          WHERE id = ANY($1::int[])
          RETURNING id, nota_id
       )
       SELECT u.id, u.nota_id, b.status_anterior
         FROM atualizados u JOIN antes b USING (id)`,
      [ids, tipo, observacao || null, req.usuario.nome || req.usuario.usuario]
    );

    // Reversões (devolucao → outro): remover item da devolução vinculada
    const revertidos = r.filter(x => x.status_anterior === 'devolucao' && tipo !== 'devolucao');
    const devsAfetadas = new Set();
    for (const v of revertidos) {
      const itensRemov = await query(
        `DELETE FROM devolucoes_itens
           WHERE origem_tipo='validade_risco' AND origem_id=$1
           RETURNING devolucao_id`,
        [v.id]
      );
      itensRemov.forEach(x => devsAfetadas.add(x.devolucao_id));
    }
    for (const devId of devsAfetadas) {
      const [resto] = await query(
        `SELECT COUNT(*)::int AS qtd, COALESCE(SUM(valor_total),0)::numeric AS total
           FROM devolucoes_itens WHERE devolucao_id=$1`,
        [devId]
      );
      if (resto.qtd === 0) {
        await query(
          `UPDATE devolucoes
              SET status='cancelada',
                  valor_total=0,
                  observacao = COALESCE(observacao,'') || ' [auto] cancelada por reversão de validades'
            WHERE id=$1 AND status='aguardando'`,
          [devId]
        );
      } else {
        await query(`UPDATE devolucoes SET valor_total=$2 WHERE id=$1`, [devId, resto.total]);
      }
    }

    // Novas decisões (qualquer status anterior → devolucao): gera item de devolução
    if (tipo === 'devolucao') {
      const novosDev = r.filter(x => x.status_anterior !== 'devolucao');
      for (const v of novosDev) {
        const [validade] = await query(`SELECT * FROM validades_em_risco WHERE id=$1`, [v.id]);
        if (validade) await criarOuAdicionarDevolucao(validade, 'validade_risco', req.usuario);
      }
    }

    const notas = [...new Set(r.map(x => x.nota_id))];
    for (const notaId of notas) await checarLiberacaoNota(notaId);
    res.json({ ok: true, atualizados: r.length, revertidos: revertidos.length });
  } catch (e) {
    console.error('[validades decidir-massa]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

async function checarLiberacaoNota(notaId) {
  const pend = await query(
    `SELECT COUNT(*)::int AS qtd FROM validades_em_risco WHERE nota_id=$1 AND status='pendente'`,
    [notaId]
  );
  if (pend[0].qtd > 0) return;
  // Só devoluções AGUARDANDO bloqueiam — canceladas/enviadas liberam a nota.
  // Validades decididas 'devolucao' geram dev em devolucoes; se a dev for cancelada,
  // a nota volta a poder ser finalizada (dispensa de devolução).
  const devsAbertas = await query(
    `SELECT COUNT(*)::int AS qtd FROM devolucoes WHERE nota_id=$1 AND status='aguardando'`,
    [notaId]
  );
  const [nota] = await query(`SELECT conferida_com_divergencia, origem FROM notas_entrada WHERE id=$1`, [notaId]);
  if (!nota) return;
  const isCD = nota.origem === 'cd' || nota.origem === 'transferencia_loja';
  let novoStatus;
  if (devsAbertas[0].qtd > 0) novoStatus = 'aguardando_devolucao';
  else if (isCD) novoStatus = nota.conferida_com_divergencia ? 'auditagem' : 'conferida';
  else novoStatus = 'fechada';
  await query(
    `UPDATE notas_entrada
        SET status=$2::text,
            fechado_em = CASE WHEN $2::text='fechada' THEN COALESCE(fechado_em, NOW()) ELSE fechado_em END
      WHERE id=$1 AND status IN ('aguardando_admin_validade','aguardando_devolucao')`,
    [notaId, novoStatus]
  );
}

// POST /:id/alterar-validade  body: { nova_validade: 'yyyy-mm-dd', observacao? }
// Permitido para admin e auditor — corrigir lancamento errado de validade na conferencia.
// Recalcula dias_ate_vencer, qtd_em_risco, motivo_risco e propaga pra lotes_conferidos.
router.post('/:id/alterar-validade', autenticar, async (req, res) => {
  try {
    if (!['admin', 'auditor'].includes(req.usuario.perfil)) {
      return res.status(403).json({ erro: 'Acesso restrito a admin/auditor' });
    }
    const { nova_validade, observacao } = req.body || {};
    if (!/^\d{4}-\d{2}-\d{2}$/.test(nova_validade || '')) {
      return res.status(400).json({ erro: 'Data invalida (use yyyy-mm-dd)' });
    }
    // Bloqueio de 10 dias minimos (mesma regra da conferencia)
    const dataMin = new Date(); dataMin.setHours(0,0,0,0);
    dataMin.setDate(dataMin.getDate() + 10);
    const minISO = dataMin.toISOString().slice(0, 10);
    if (nova_validade < minISO) {
      return res.status(400).json({ erro: `Validade ${nova_validade} esta abaixo do minimo (hoje + 10 dias). Nao aceitar produto.` });
    }
    const [risco] = await query('SELECT * FROM validades_em_risco WHERE id = $1', [req.params.id]);
    if (!risco) return res.status(404).json({ erro: 'Registro nao encontrado' });

    const dt = new Date(nova_validade + 'T12:00:00');
    const hoje = new Date(); hoje.setHours(12, 0, 0, 0);
    const dias = Math.ceil((dt - hoje) / 86400000);

    const vendasMedia = parseFloat(risco.vendas_media_dia) || 0;
    const qtdRecebida = parseFloat(risco.qtd_recebida_lote) || 0;
    const estoqueAcum = parseFloat(risco.estoque_pos_recebimento) || 0;
    const emb = parseFloat(risco.qtd_embalagem) || 1;
    const precoUnit = parseFloat(risco.valor_unitario) || 0;

    const consumivel = Math.max(0, vendasMedia * Math.max(0, dias));
    const emRisco = Math.max(0, estoqueAcum - consumivel);
    const qtdRiscoUn = vendasMedia <= 0 ? qtdRecebida : emRisco;
    const qtdRiscoCx = emb > 0 ? Math.ceil(qtdRiscoUn / emb) : Math.ceil(qtdRiscoUn);
    const valorRiscoCx = qtdRiscoCx * emb * precoUnit;

    let motivo;
    if (dias < 0) motivo = 'ja_vencido';
    else if (vendasMedia <= 0) motivo = 'sem_historico_vendas';
    else motivo = qtdRiscoUn > 0 ? 'risco_por_giro' : 'risco_por_giro'; // mantem se ainda em risco

    const stamp = new Date().toISOString().slice(0, 10);
    const quem = req.usuario.nome || req.usuario.usuario;
    const linhaLog = `[${stamp} ${quem}] Validade alterada de ${risco.validade} para ${nova_validade}${observacao ? ': ' + observacao : ''}`;
    const novaObs = risco.observacao ? `${risco.observacao}\n${linhaLog}` : linhaLog;

    await query(
      `UPDATE validades_em_risco SET
         validade = $1, dias_ate_vencer = $2,
         qtd_consumivel_ate_vencer = $3, qtd_em_risco = $4, qtd_em_risco_caixas = $5,
         valor_em_risco = $6, motivo_risco = $7, observacao = $8
       WHERE id = $9`,
      [nova_validade, dias, consumivel, qtdRiscoUn, qtdRiscoCx, valorRiscoCx, motivo, novaObs, req.params.id]
    );

    // Propaga pra lotes_conferidos (mesmo nota+item+validade antiga)
    await query(
      `UPDATE lotes_conferidos SET validade = $1
         WHERE nota_id = $2 AND item_id = $3 AND validade = $4`,
      [nova_validade, risco.nota_id, risco.item_id, risco.validade]
    );

    res.json({ ok: true, dias_ate_vencer: dias, motivo_risco: motivo });
  } catch (e) {
    console.error('[validades alterar-validade]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

module.exports = router;
module.exports.checarLiberacaoNota = checarLiberacaoNota;
