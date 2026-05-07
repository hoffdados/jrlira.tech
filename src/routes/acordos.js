const express = require('express');
const router = express.Router();
const { pool, query: dbQuery } = require('../db');
const { autenticar, compradorOuAdmin } = require('../auth');

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
             f.razao_social AS forn_razao, f.fantasia AS forn_fantasia
      FROM acordos_comerciais a
      JOIN lojas l ON l.id = a.loja_id
      LEFT JOIN fornecedores f ON f.id = a.fornecedor_id
      WHERE a.id = $1
    `, [req.params.id]);
    if (!a) return res.status(404).json({ erro: 'Acordo não encontrado' });

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
    res.json({ ...a, vendedores });
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

    const aprovador = req.usuario.email || req.usuario.usuario || req.usuario.nome || `id:${req.usuario.id}`;
    await client.query(
      `UPDATE acordos_comerciais
       SET status = 'ativo', aprovado_por = $1, aprovado_em = NOW(), updated_at = NOW()
       WHERE id = $2`,
      [aprovador, a.id]
    );

    // Marca o validade_alertas como rebaixado pra etiqueta vermelha aparecer ao repositor
    await client.query(
      `UPDATE validade_alertas
       SET preco_rebaixado = true,
           data_rebaixamento = NOW(),
           preco_rebaixado_valor = $1
       WHERE id = $2`,
      [a.preco_acordo, a.alerta_id]
    );

    await client.query('COMMIT');
    res.json({ ok: true, id: a.id });
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
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

module.exports = router;
