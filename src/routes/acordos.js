const express = require('express');
const router = express.Router();
const { pool, query: dbQuery } = require('../db');
const { autenticar, compradorOuAdmin } = require('../auth');
const { criarNotificacao } = require('./notificacoes');

async function notificarCadastros({ titulo, corpo, url }) {
  const dest = await dbQuery(`SELECT id FROM rh_usuarios WHERE perfil IN ('cadastro','admin') AND ativo=TRUE`);
  for (const u of dest) {
    await criarNotificacao({ destinatario_tipo: 'usuario', destinatario_id: u.id, tipo: 'acordo_decisao', titulo, corpo, url });
  }
}

// GET /api/acordos?status=&loja_id=
router.get('/', compradorOuAdmin, async (req, res) => {
  try {
    const { status, loja_id } = req.query;
    const params = [];
    const conds = [];
    if (status)   { params.push(status);                  conds.push(`a.status = $${params.length}`); }
    if (loja_id)  { params.push(parseInt(loja_id));       conds.push(`a.loja_id = $${params.length}`); }
    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';

    const rows = await dbQuery(`
      SELECT a.*,
             l.nome AS loja_nome,
             f.razao_social AS forn_razao,
             f.fantasia     AS forn_fantasia
      FROM acordos_comerciais a
      JOIN lojas l ON l.id = a.loja_id
      LEFT JOIN fornecedores f ON f.id = a.fornecedor_id
      ${where}
      ORDER BY
        CASE a.status
          WHEN 'pendente_compras' THEN 0
          WHEN 'ativo'            THEN 1
          WHEN 'fechado'          THEN 2
          ELSE 3
        END,
        a.solicitado_em DESC
    `, params);
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/acordos/contagem-pendentes
router.get('/contagem-pendentes', compradorOuAdmin, async (req, res) => {
  try {
    const [r] = await dbQuery(`SELECT COUNT(*)::int AS n FROM acordos_comerciais WHERE status = 'pendente_compras'`);
    res.json({ pendentes: r?.n || 0 });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/acordos/:id  — detalhe completo (com vendedores do fornecedor)
router.get('/:id', compradorOuAdmin, async (req, res) => {
  try {
    const [a] = await dbQuery(`
      SELECT a.*, l.nome AS loja_nome, l.cnpj AS loja_cnpj,
             f.razao_social AS forn_razao, f.fantasia AS forn_fantasia,
             ver.estoque_atual AS ver_estoque_atual,
             ver.estoque_pos_recebimento AS ver_estoque_pos_receb,
             ver.qtd_em_risco AS ver_qtd_em_risco,
             ver.qtd_consumivel_ate_vencer AS ver_qtd_consumivel,
             ver.vendas_media_dia AS ver_vendas_media_dia,
             ver.validade AS ver_validade,
             ver.criado_em AS ver_criado_em
      FROM acordos_comerciais a
      JOIN lojas l ON l.id = a.loja_id
      LEFT JOIN fornecedores f ON f.id = a.fornecedor_id
      LEFT JOIN validades_em_risco ver ON ver.id = a.alerta_id
      WHERE a.id = $1
    `, [req.params.id]);
    if (!a) return res.status(404).json({ erro: 'Acordo não encontrado' });

    // Vendas após a data do alerta (coleta de validade) pra esse EAN na loja
    let vendas_pos_coleta = 0;
    if (a.ver_criado_em && a.barcode) {
      const [vp] = await dbQuery(`
        SELECT COALESCE(SUM(qtd_vendida),0)::numeric(14,3) AS qtd
          FROM vendas_historico
         WHERE loja_id = $1
           AND NULLIF(LTRIM(codigobarra,'0'),'') = NULLIF(LTRIM($2,'0'),'')
           AND data_venda >= $3::date
           AND COALESCE(tipo_saida,'venda') = 'venda'`,
        [a.loja_id, a.barcode, a.ver_criado_em]);
      vendas_pos_coleta = parseFloat(vp.qtd || 0);
    }

    // Estimativa de sobra: estoque atual − (qtd_consumivel_ate_vencer estimado)
    const estoque = parseFloat(a.ver_estoque_atual || a.ver_estoque_pos_receb || 0);
    const consumivel = parseFloat(a.ver_qtd_consumivel || 0);
    const sobra_estimada = Math.max(0, estoque - consumivel);

    const validade_info = {
      data_coleta: a.ver_criado_em,
      validade: a.ver_validade,
      estoque_atual: estoque,
      vendas_media_dia: parseFloat(a.ver_vendas_media_dia || 0),
      qtd_em_risco: parseFloat(a.ver_qtd_em_risco || 0),
      qtd_consumivel_ate_vencer: consumivel,
      vendas_pos_coleta,
      sobra_estimada,
      vai_sobrar: sobra_estimada > 0,
    };

    let vendedores = [];
    if (a.fornecedor_id) {
      vendedores = await dbQuery(
        `SELECT id, nome, email, telefone, status FROM vendedores WHERE fornecedor_id = $1 ORDER BY nome`,
        [a.fornecedor_id]
      );
    } else if (a.fornecedor_cnpj) {
      vendedores = await dbQuery(
        `SELECT id, nome, email, telefone, status FROM vendedores
         WHERE REGEXP_REPLACE(COALESCE(fornecedor_cnpj,''),'\\D','','g')
             = REGEXP_REPLACE($1,'\\D','','g')
         ORDER BY nome`,
        [a.fornecedor_cnpj]
      );
    }
    res.json({ ...a, vendedores, validade_info });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/acordos/:id/extrato — vendas dia a dia entre data_acordo e validade
router.get('/:id/extrato', compradorOuAdmin, async (req, res) => {
  try {
    const [a] = await dbQuery(`
      SELECT a.*, l.nome AS loja_nome, l.cnpj AS loja_cnpj
        FROM acordos_comerciais a
        JOIN lojas l ON l.id = a.loja_id
       WHERE a.id = $1`, [req.params.id]);
    if (!a) return res.status(404).json({ erro: 'Acordo não encontrado' });

    const dtInicio = a.aprovado_em || a.solicitado_em;
    const dtFim = a.data_validade || a.fechado_em || new Date();

    const vendas = await dbQuery(`
      SELECT data_venda, SUM(qtd_vendida)::numeric(12,3) AS qtd
        FROM vendas_historico
       WHERE NULLIF(LTRIM(codigobarra,'0'),'') = NULLIF(LTRIM($1,'0'),'')
         AND loja_id = $2
         AND data_venda >= $3::date
         AND data_venda <= $4::date
         AND COALESCE(tipo_saida, 'venda') = 'venda'
       GROUP BY data_venda
       ORDER BY data_venda`,
      [a.barcode, a.loja_id, dtInicio, dtFim]
    );

    const totalVendido = vendas.reduce((s, v) => s + parseFloat(v.qtd), 0);
    const qtdAcordada = parseFloat(a.qtde_acordada) || 0;
    const precoAtual = parseFloat(a.preco_atual) || 0;
    const precoAcordo = parseFloat(a.preco_acordo) || 0;
    const diff = precoAtual - precoAcordo;
    const qtdEfetiva = Math.min(totalVendido, qtdAcordada);
    const debitoCalculado = qtdEfetiva * diff;

    res.json({
      acordo: a,
      vendas,
      resumo: {
        total_vendido: totalVendido,
        qtde_acordada: qtdAcordada,
        qtd_efetiva: qtdEfetiva,
        preco_atual: precoAtual,
        preco_acordo: precoAcordo,
        diferenca_unitaria: diff,
        debito_calculado: debitoCalculado
      }
    });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/acordos/:id/aprovar
// body opcional: { preco_acordo } — comprador pode renegociar e enviar valor diferente
router.post('/:id/aprovar', compradorOuAdmin, async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rows: [a] } = await client.query(
      `SELECT * FROM acordos_comerciais WHERE id = $1 FOR UPDATE`, [req.params.id]
    );
    if (!a) { await client.query('ROLLBACK'); return res.status(404).json({ erro: 'Acordo não encontrado' }); }
    if (a.status !== 'pendente_compras') {
      await client.query('ROLLBACK');
      return res.status(409).json({ erro: `Acordo não está pendente (status atual: ${a.status})` });
    }

    // Preço do acordo — se comprador renegociou, usa novo valor; senão mantém original
    const precoOriginal = parseFloat(a.preco_acordo) || 0;
    const precoNovo = req.body?.preco_acordo != null ? parseFloat(req.body.preco_acordo) : precoOriginal;
    if (isNaN(precoNovo) || precoNovo < 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({ erro: 'preco_acordo inválido' });
    }
    const precoAtual = parseFloat(a.preco_atual) || 0;
    if (precoNovo >= precoAtual) {
      await client.query('ROLLBACK');
      return res.status(400).json({ erro: 'Preço acordo deve ser menor que o preço atual do produto' });
    }
    const renegociado = Math.abs(precoNovo - precoOriginal) > 0.001;

    const aprovador = req.usuario.email || req.usuario.usuario || req.usuario.nome || `id:${req.usuario.id}`;
    // diferenca_unitaria e valor_confessado sao GENERATED ALWAYS — recalculam sozinhas ao atualizar preco_acordo.
    await client.query(
      `UPDATE acordos_comerciais
       SET status = 'ativo', aprovado_por = $1, aprovado_em = NOW(),
           preco_acordo = $2,
           updated_at = NOW()
       WHERE id = $3`,
      [aprovador, precoNovo, a.id]
    );

    // Marca o validade_alertas como rebaixado pra etiqueta vermelha aparecer ao repositor
    await client.query(
      `UPDATE validade_alertas
       SET preco_rebaixado = true,
           data_rebaixamento = NOW(),
           preco_rebaixado_valor = $1
       WHERE id = $2`,
      [precoNovo, a.alerta_id]
    );

    await client.query('COMMIT');

    const corpoNotif = renegociado
      ? `${a.fornecedor_nome || ''} • ${a.produto_nome} • Rebaixar pra R$ ${precoNovo.toFixed(2)} (negociado, era R$ ${precoOriginal.toFixed(2)}) • ${a.qtde_acordada} un`
      : `${a.fornecedor_nome || ''} • ${a.produto_nome} • Rebaixar pra R$ ${precoNovo.toFixed(2)} (${a.qtde_acordada} un)`;
    notificarCadastros({
      titulo: '✅ Acordo aprovado — rebaixar produto',
      corpo: corpoNotif,
      url: `/acordo-extrato.html?id=${a.id}`
    }).catch(() => {});
    res.json({ ok: true, id: a.id, preco_acordo: precoNovo, renegociado });
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    res.status(500).json({ erro: e.message });
  } finally {
    client.release();
  }
});

// POST /api/acordos/:id/recusar  body: { motivo }
router.post('/:id/recusar', compradorOuAdmin, async (req, res) => {
  const motivo = (req.body?.motivo || '').trim();
  if (!motivo) return res.status(400).json({ erro: 'Motivo obrigatório' });
  try {
    const [a] = await dbQuery(`SELECT status FROM acordos_comerciais WHERE id = $1`, [req.params.id]);
    if (!a) return res.status(404).json({ erro: 'Acordo não encontrado' });
    if (a.status !== 'pendente_compras')
      return res.status(409).json({ erro: `Acordo não está pendente (status atual: ${a.status})` });

    const aprovador = req.usuario.email || req.usuario.usuario || req.usuario.nome || `id:${req.usuario.id}`;
    await dbQuery(
      `UPDATE acordos_comerciais
       SET status = 'recusado', aprovado_por = $1, aprovado_em = NOW(),
           motivo_recusa = $2, updated_at = NOW()
       WHERE id = $3`,
      [aprovador, motivo, req.params.id]
    );
    const [info] = await dbQuery(`SELECT fornecedor_nome, produto_nome FROM acordos_comerciais WHERE id=$1`, [req.params.id]);
    notificarCadastros({
      titulo: '❌ Acordo recusado',
      corpo: `${info?.fornecedor_nome || ''} • ${info?.produto_nome || ''} — Motivo: ${motivo}`,
      url: '/auditoria-acordos.html'
    }).catch(() => {});
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/acordos/auditoria/nao-aceitos — agrupa acordos recusados/pendentes/cancelados por fornecedor
router.get('/auditoria/nao-aceitos', compradorOuAdmin, async (req, res) => {
  try {
    const fornecedor_cnpj = (req.query.fornecedor_cnpj || '').replace(/\D/g, '');
    const status = req.query.status || 'recusado,cancelado,pendente_compras';
    const statuses = status.split(',').map(s => s.trim()).filter(Boolean);
    const params = [statuses];
    let where = `a.status = ANY($1::text[])`;
    if (fornecedor_cnpj) {
      params.push(fornecedor_cnpj);
      where += ` AND REGEXP_REPLACE(a.fornecedor_cnpj,'\\D','','g') = $${params.length}`;
    }
    const rows = await dbQuery(
      `SELECT a.id, a.loja_id, l.nome AS loja_nome,
              a.fornecedor_cnpj, a.fornecedor_nome,
              a.barcode, a.produto_nome,
              a.preco_atual, a.preco_acordo, a.diferenca_unitaria, a.qtde_acordada, a.valor_confessado,
              a.data_validade, a.status, a.motivo_recusa,
              a.solicitado_por_nome, a.solicitado_em, a.aprovado_por, a.aprovado_em
         FROM acordos_comerciais a
         LEFT JOIN lojas l ON l.id = a.loja_id
        WHERE ${where}
        ORDER BY a.fornecedor_nome, a.solicitado_em DESC`,
      params
    );
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/acordos/auditoria/resumo-fornecedores — agregado por fornecedor
router.get('/auditoria/resumo-fornecedores', compradorOuAdmin, async (req, res) => {
  try {
    const rows = await dbQuery(
      `SELECT a.fornecedor_cnpj, a.fornecedor_nome,
              COUNT(*) FILTER (WHERE a.status='ativo')             AS aceitos,
              COUNT(*) FILTER (WHERE a.status='recusado')          AS recusados,
              COUNT(*) FILTER (WHERE a.status='cancelado')         AS cancelados,
              COUNT(*) FILTER (WHERE a.status='pendente_compras')  AS pendentes,
              COUNT(*) FILTER (WHERE a.status='fechado')           AS fechados,
              COUNT(*) AS total,
              SUM(a.valor_confessado) FILTER (WHERE a.status='ativo')    AS valor_aceito,
              SUM(a.valor_confessado) FILTER (WHERE a.status='recusado') AS valor_recusado
         FROM acordos_comerciais a
        WHERE a.fornecedor_cnpj IS NOT NULL
        GROUP BY 1, 2
        ORDER BY recusados DESC, cancelados DESC, total DESC`,
    );
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

module.exports = router;
