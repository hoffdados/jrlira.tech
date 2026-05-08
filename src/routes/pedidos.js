const express = require('express');
const router = express.Router();
const PDFDocument = require('pdfkit');
const { pool, query: dbQuery } = require('../db');
const jwt = require('jsonwebtoken');
const { compradorOuAdmin, JWT_SECRET } = require('../auth');
const { enviarEmail } = require('../mailer');
const { enviarWhatsapp } = require('../whatsapp');
const { criarNotificacao } = require('./notificacoes');

const APP_URL = process.env.APP_URL || 'https://jrliratech-production.up.railway.app';

function msgWhatsApp(pedido, evento) {
  const num = pedido.numero_pedido || ('#' + pedido.id);
  const valor = parseFloat(pedido.valor_total || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const loja = pedido.loja_nome || '';
  const link = `${APP_URL}/vendedor`;
  const linhas = {
    validado: `✅ *Pedido validado!*\n\nPedido: *${num}*\nLoja: ${loja}\nTotal: R$ ${valor}\n\nO PDF foi enviado pro seu e-mail. Acesse: ${link}`,
    aguardando_auditoria: `🟠 *Pedido em análise*\n\nPedido: *${num}*\nLoja: ${loja}\n\nSeu pedido foi enviado para análise do Administrador. Você será notificado quando for liberado.`,
    rejeitado: `❌ *Pedido rejeitado*\n\nPedido: *${num}*\nLoja: ${loja}${pedido.motivo_rejeicao ? '\nMotivo: ' + pedido.motivo_rejeicao : ''}\n\nAcesse: ${link}`,
    cancelado_pelo_vendedor: `🚫 *Pedido cancelado pelo fornecedor*\n\nPedido: *${num}*\nLoja: ${loja}\nFornecedor: ${pedido.fornecedor_nome || ''}\nVendedor: ${pedido.vendedor_nome || ''}${pedido.motivo_cancelamento ? '\nMotivo: ' + pedido.motivo_cancelamento : ''}\n\nO vendedor enviará um novo pedido em substituição.`,
  };
  return linhas[evento] || `Pedido ${num} — atualização: ${evento}`;
}

// Middleware especial para PDF: aceita token na query (pra link em <a href>)
function authPdf(req, res, next) {
  const headerToken = req.headers.authorization?.replace('Bearer ', '');
  const queryToken = req.query.token;
  const token = headerToken || queryToken;
  if (!token) return res.status(401).send('Token não fornecido');
  try {
    req.usuario = jwt.verify(token, JWT_SECRET);
    if (!['admin', 'comprador'].includes(req.usuario.perfil)) return res.status(403).send('Acesso restrito');
    next();
  } catch { res.status(401).send('Token inválido'); }
}

// GET /api/pedidos — lista pedidos aguardando validação ou validados
router.get('/', compradorOuAdmin, async (req, res) => {
  try {
    const { status, fornecedor_cnpj } = req.query;
    const params = [], conds = [];
    // Esconde rascunho por default, EXCETO quando o filtro pede explicitamente.
    // Quando pedem rascunho, retorna SÓ os criados via /sugestao-compras (criado_por_comprador NOT NULL)
    if (status === 'rascunho') {
      conds.push("p.status='rascunho'");
      conds.push("p.criado_por_comprador IS NOT NULL");
    } else if (status && status.includes(',')) {
      const lista = status.split(',').map(s => s.trim()).filter(Boolean);
      const placeholders = lista.map(s => { params.push(s); return `$${params.length}`; });
      conds.push(`p.status IN (${placeholders.join(',')})`);
    } else if (status) {
      params.push(status);
      conds.push(`p.status=$${params.length}`);
    } else {
      conds.push("p.status != 'rascunho'");
    }
    if (fornecedor_cnpj) { params.push(fornecedor_cnpj.replace(/\D/g,'')); conds.push(`REGEXP_REPLACE(f.cnpj,'\\D','','g')=$${params.length}`); }
    const where = 'WHERE ' + conds.join(' AND ');
    const rows = await dbQuery(
      `SELECT p.id, p.numero_pedido, p.status, p.valor_total, p.condicao_pagamento,
              p.criado_em, p.enviado_em, p.validado_em, p.validado_por,
              p.editado_na_auditoria, p.auditado_por, p.auditado_em,
              p.cancelado_em, p.cancelado_por, p.motivo_cancelamento,
              p.faturado_em, p.faturado_por, p.numero_nf_faturada,
              p.loja_id, p.criado_por_comprador,
              f.razao_social as fornecedor_nome, f.fantasia as fornecedor_fantasia, f.cnpj as fornecedor_cnpj,
              v.nome as vendedor_nome, v.email as vendedor_email,
              l.nome as loja_nome, n.numero_nota
       FROM pedidos p
       LEFT JOIN fornecedores f ON f.id=p.fornecedor_id
       LEFT JOIN vendedores v ON v.id=p.vendedor_id
       LEFT JOIN lojas l ON l.id=p.loja_id
       LEFT JOIN notas_entrada n ON n.id=p.nota_id
       ${where} ORDER BY p.enviado_em DESC NULLS LAST, p.criado_em DESC`,
      params
    );
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/pedidos/fornecedores-com-historico?loja_id=
router.get('/fornecedores-com-historico', compradorOuAdmin, async (req, res) => {
  try {
    const { loja_id } = req.query;
    if (!loja_id) return res.status(400).json({ erro: 'loja_id obrigatório' });
    const rows = await dbQuery(
      `WITH cnpjs_loja AS (
         SELECT DISTINCT REGEXP_REPLACE(fornecedor_cnpj, '\\D', '', 'g') AS cnpj_num
         FROM compras_historico WHERE loja_id = $1
           AND COALESCE(tipo_entrada, 'compra') = 'compra'
       )
       SELECT * FROM (
         SELECT DISTINCT ON (f.cnpj) f.id, f.razao_social, f.fantasia, f.cnpj
         FROM fornecedores f
         JOIN cnpjs_loja c ON REGEXP_REPLACE(f.cnpj, '\\D', '', 'g') = c.cnpj_num
         WHERE f.ativo = true
         ORDER BY f.cnpj, f.razao_social, f.id
       ) sub ORDER BY razao_social`,
      [loja_id]
    );
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/pedidos/sugestao?fornecedor_cnpj=&loja_id=
router.get('/sugestao', compradorOuAdmin, async (req, res) => {
  try {
    const { fornecedor_cnpj, loja_id } = req.query;
    if (!fornecedor_cnpj || !loja_id) return res.status(400).json({ erro: 'fornecedor_cnpj e loja_id obrigatórios' });
    const cnpj = fornecedor_cnpj.replace(/\D/g, '');

    const rows = await dbQuery(`
      WITH eans_forn AS (
        SELECT DISTINCT codigobarra
        FROM compras_historico
        WHERE REGEXP_REPLACE(fornecedor_cnpj, '\\D', '', 'g') = $1
          AND loja_id = $2
          AND COALESCE(tipo_entrada, 'compra') = 'compra'
      ),
      pe_fallback AS (
        SELECT DISTINCT ON (codigobarra) codigobarra, descricao, custoorigem
        FROM produtos_externo
        ORDER BY codigobarra, loja_id
      ),
      vendas AS (
        SELECT codigobarra,
               COALESCE(SUM(qtd_vendida), 0) AS total_90d,
               MAX(data_venda) AS ultima_venda
        FROM vendas_historico
        WHERE loja_id = $2
          AND data_venda >= CURRENT_DATE - INTERVAL '90 days'
          AND codigobarra IN (SELECT codigobarra FROM eans_forn)
          AND COALESCE(tipo_saida, 'venda') = 'venda'
        GROUP BY codigobarra
      ),
      transito AS (
        SELECT ip.codigo_barras,
               COALESCE(SUM(COALESCE(ip.qtd_validada, ip.quantidade)), 0) AS em_transito
        FROM itens_pedido ip
        JOIN pedidos pp ON pp.id = ip.pedido_id
        LEFT JOIN notas_entrada n ON n.id = pp.nota_id
        WHERE pp.loja_id = $2
          AND (pp.status = 'validado' OR (pp.status = 'vinculado' AND (n.id IS NULL OR n.status != 'fechada')))
          AND ip.codigo_barras IN (SELECT codigobarra FROM eans_forn)
        GROUP BY ip.codigo_barras
      ),
      ultima_compra AS (
        SELECT codigobarra, MAX(data_entrada) AS ultima_compra
        FROM compras_historico
        WHERE loja_id = $2
          AND REGEXP_REPLACE(fornecedor_cnpj, '\\D', '', 'g') = $1
          AND COALESCE(tipo_entrada, 'compra') = 'compra'
        GROUP BY codigobarra
      )
      SELECT
        ef.codigobarra,
        COALESCE(pe.descricao, pfb.descricao, ef.codigobarra) AS descricao,
        COALESCE(pe.estdisponivel, 0)::float AS estdisponivel,
        COALESCE(pe.custoorigem, pfb.custoorigem, 0)::float AS custoorigem,
        COALESCE(pe.qtdeembalagem, 1)::float AS qtdeembalagem,
        COALESCE(v.total_90d, 0)::float AS total_90d,
        v.ultima_venda,
        COALESCE(t.em_transito, 0)::float AS em_transito,
        uc.ultima_compra
      FROM eans_forn ef
      LEFT JOIN produtos_externo pe ON pe.codigobarra = ef.codigobarra AND pe.loja_id = $2
      LEFT JOIN pe_fallback pfb ON pfb.codigobarra = ef.codigobarra
      LEFT JOIN vendas v ON v.codigobarra = ef.codigobarra
      LEFT JOIN transito t ON t.codigo_barras = ef.codigobarra
      LEFT JOIN ultima_compra uc ON uc.codigobarra = ef.codigobarra
      WHERE COALESCE(pe.estdisponivel, 0)::float > 0
         OR COALESCE(v.total_90d, 0)::float > 0
         OR COALESCE(t.em_transito, 0)::float > 0
      ORDER BY COALESCE(pe.descricao, pfb.descricao) NULLS LAST, ef.codigobarra
    `, [cnpj, loja_id]);

    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// PATCH /api/pedidos/:id/atribuir-vendedor — vincula vendedor a um pedido (usado em rascunhos da sugestão)
router.patch('/:id/atribuir-vendedor', compradorOuAdmin, async (req, res) => {
  try {
    const { vendedor_id } = req.body || {};
    if (!vendedor_id) return res.status(400).json({ erro: 'vendedor_id obrigatório' });
    const [ped] = await dbQuery('SELECT status, fornecedor_id FROM pedidos WHERE id=$1', [req.params.id]);
    if (!ped) return res.status(404).json({ erro: 'Pedido não encontrado' });
    // Verifica se vendedor pertence ao fornecedor (por id ou cnpj)
    const [v] = await dbQuery(
      `SELECT v.id FROM vendedores v
         JOIN fornecedores f ON f.id = $2
        WHERE v.id = $1
          AND (v.fornecedor_id = f.id
               OR REGEXP_REPLACE(COALESCE(v.fornecedor_cnpj,''),'\\D','','g')
                = REGEXP_REPLACE(COALESCE(f.cnpj,''),'\\D','','g'))`,
      [vendedor_id, ped.fornecedor_id]
    );
    if (!v) return res.status(400).json({ erro: 'Vendedor não pertence ao fornecedor deste pedido' });
    await dbQuery('UPDATE pedidos SET vendedor_id=$1 WHERE id=$2', [vendedor_id, req.params.id]);
    res.json({ ok: true, vendedor_id });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/pedidos/de-sugestao
router.post('/de-sugestao', compradorOuAdmin, async (req, res) => {
  try {
    const { fornecedor_id, loja_id, condicao_pagamento, observacoes, itens } = req.body;
    if (!fornecedor_id || !loja_id || !itens?.length) return res.status(400).json({ erro: 'Campos obrigatórios faltando' });

    const numero_pedido = `SUG-${Date.now()}`;
    const criadoPor = req.usuario?.nome || req.usuario?.usuario || null;
    const [ped] = await dbQuery(
      `INSERT INTO pedidos (numero_pedido, fornecedor_id, loja_id, status, condicao_pagamento, observacoes, criado_por_comprador)
       VALUES ($1,$2,$3,'rascunho',$4,$5,$6) RETURNING id`,
      [numero_pedido, fornecedor_id, loja_id, condicao_pagamento ?? 28, observacoes || null, criadoPor]
    );

    let valor_total = 0;
    for (const it of itens) {
      const qtd = parseFloat(it.quantidade);
      const preco = parseFloat(it.preco_unitario || 0);
      const vt = qtd * preco;
      valor_total += vt;
      await dbQuery(
        `INSERT INTO itens_pedido (pedido_id, codigo_barras, descricao, quantidade, preco_unitario, valor_total)
         VALUES ($1,$2,$3,$4,$5,$6)`,
        [ped.id, it.codigo_barras || null, it.descricao, qtd, preco, vt]
      );
    }
    await dbQuery('UPDATE pedidos SET valor_total=$1 WHERE id=$2', [valor_total, ped.id]);

    res.json({ id: ped.id, numero_pedido });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// Notas em validação comercial (para tela de notas-cadastro)
// GET /api/pedidos/aguardando-auditoria — apenas admin (definida antes de /:id)
router.get('/aguardando-auditoria', compradorOuAdmin, async (req, res) => {
  try {
    if (req.usuario.perfil !== 'admin') return res.status(403).json({ erro: 'Apenas admin' });
    const rows = await dbQuery(
      `SELECT p.id, p.numero_pedido, p.status, p.valor_total, p.condicao_pagamento, p.preco_valido_ate,
              p.criado_em, p.enviado_em, p.observacoes,
              f.razao_social as fornecedor_nome, f.fantasia as fornecedor_fantasia,
              v.nome as vendedor_nome, l.nome as loja_nome,
              (SELECT COUNT(*) FROM itens_pedido WHERE pedido_id=p.id AND justificativa_excesso IS NOT NULL) AS qtd_excessos
       FROM pedidos p
       LEFT JOIN fornecedores f ON f.id=p.fornecedor_id
       LEFT JOIN vendedores v ON v.id=p.vendedor_id
       LEFT JOIN lojas l ON l.id=p.loja_id
       WHERE p.status='aguardando_auditoria'
       ORDER BY p.criado_em DESC`
    );
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

router.get('/notas-validacao', compradorOuAdmin, async (req, res) => {
  try {
    const rows = await dbQuery(
      `SELECT n.id, n.numero_nota, n.fornecedor_nome, n.fornecedor_cnpj, n.data_emissao, n.valor_total,
              p.id as pedido_id, p.numero_pedido, p.status as pedido_status
       FROM notas_entrada n LEFT JOIN pedidos p ON p.nota_id=n.id
       WHERE n.status='em_validacao_comercial' ORDER BY n.importado_em DESC`
    );
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/pedidos/:id — detalhe completo
router.get('/:id', compradorOuAdmin, async (req, res) => {
  try {
    const rows = await dbQuery(
      `SELECT p.*, f.razao_social, f.fantasia, f.cnpj as fornecedor_cnpj,
              v.nome as vendedor_nome, v.email as vendedor_email, v.telefone as vendedor_tel,
              l.nome as loja_nome, l.cnpj as loja_cnpj
       FROM pedidos p
       LEFT JOIN fornecedores f ON f.id=p.fornecedor_id
       LEFT JOIN vendedores v ON v.id=p.vendedor_id
       LEFT JOIN lojas l ON l.id=p.loja_id
       WHERE p.id=$1`,
      [req.params.id]
    );
    if (!rows.length) return res.status(404).json({ erro: 'Pedido não encontrado' });
    const itens = await dbQuery('SELECT * FROM itens_pedido WHERE pedido_id=$1 ORDER BY id', [req.params.id]);

    // Histórico de preços por código (último preço pago em compras_historico, qualquer fornecedor)
    const codigos_h = [...new Set(itens.map(i => i.codigo_barras).filter(Boolean))];
    let historicoPrecos = {};
    if (codigos_h.length) {
      const r = await dbQuery(`
        SELECT DISTINCT ON (ch.codigobarra) ch.codigobarra,
               ch.data_entrada,
               COALESCE(f.razao_social, ch.fornecedor_cnpj) AS fornecedor_nome,
               CASE WHEN ch.qtd_comprada > 0 THEN ch.custo_total / ch.qtd_comprada ELSE 0 END AS preco_unitario
        FROM compras_historico ch
        LEFT JOIN LATERAL (
          SELECT razao_social FROM fornecedores
          WHERE REGEXP_REPLACE(cnpj,'\\D','','g') = REGEXP_REPLACE(ch.fornecedor_cnpj,'\\D','','g')
          LIMIT 1
        ) f ON true
        WHERE ch.codigobarra = ANY($1) AND ch.loja_id = (SELECT loja_id FROM pedidos WHERE id=$2)
          AND COALESCE(ch.tipo_entrada, 'compra') = 'compra'
        ORDER BY ch.codigobarra, ch.data_entrada DESC
      `, [codigos_h, req.params.id]);
      for (const x of r) historicoPrecos[x.codigobarra] = {
        data: x.data_entrada,
        fornecedor: x.fornecedor_nome,
        preco: parseFloat(x.preco_unitario),
      };
    }

    // Para cada item, busca outras propostas pendentes do MESMO código de barras
    // em pedidos AGUARDANDO_VALIDAÇÃO ou VALIDADOS (não vinculados ainda) — exceto este pedido
    const codigos = [...new Set(itens.map(i => i.codigo_barras).filter(Boolean))];
    let propostas = {};
    if (codigos.length) {
      const r = await dbQuery(`
        SELECT ip.codigo_barras, ip.preco_unitario::float AS preco, ip.quantidade::float AS quantidade,
               p.id AS pedido_id, p.numero_pedido, p.preco_valido_ate, p.condicao_pagamento, p.enviado_em,
               f.fantasia, f.razao_social, v.nome AS vendedor_nome
        FROM itens_pedido ip
        JOIN pedidos p ON p.id = ip.pedido_id
        LEFT JOIN fornecedores f ON f.id = p.fornecedor_id
        LEFT JOIN vendedores v ON v.id = p.vendedor_id
        WHERE ip.codigo_barras = ANY($1)
          AND p.id != $2
          AND p.status IN ('aguardando_validacao','validado')
          AND (p.preco_valido_ate IS NULL OR p.preco_valido_ate >= CURRENT_DATE)
        ORDER BY ip.preco_unitario ASC, p.enviado_em DESC
      `, [codigos, req.params.id]);
      for (const x of r) {
        if (!propostas[x.codigo_barras]) propostas[x.codigo_barras] = [];
        propostas[x.codigo_barras].push(x);
      }
    }

    res.json({ ...rows[0], itens, propostas_alternativas: propostas, historico_precos: historicoPrecos });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// PATCH /api/pedidos/:id/rascunho-validacao — salva qtd/preço/cabeçalho sem validar
router.patch('/:id/rascunho-validacao', compradorOuAdmin, async (req, res) => {
  try {
    const { itens, condicao_pagamento, observacoes } = req.body;
    const ped = await dbQuery('SELECT id, status FROM pedidos WHERE id=$1', [req.params.id]);
    if (!ped.length) return res.status(404).json({ erro: 'Pedido não encontrado' });
    if (ped[0].status === 'vinculado') return res.status(400).json({ erro: 'Pedido já vinculado a NF — não editável' });

    if (Array.isArray(itens)) {
      for (const item of itens) {
        await dbQuery(
          `UPDATE itens_pedido SET qtd_validada=$1, preco_validado=$2 WHERE id=$3 AND pedido_id=$4`,
          [item.qtd_validada ?? null, item.preco_validado ?? null, item.id, req.params.id]
        );
      }
    }
    await dbQuery(
      `UPDATE pedidos SET
         valor_total=(SELECT COALESCE(SUM(COALESCE(qtd_validada,quantidade)*COALESCE(preco_validado,preco_unitario)),0) FROM itens_pedido WHERE pedido_id=$1 AND COALESCE(excluido_pelo_comprador,FALSE)=FALSE),
         condicao_pagamento=COALESCE($2, condicao_pagamento),
         observacoes=COALESCE($3, observacoes)
       WHERE id=$1`,
      [req.params.id, condicao_pagamento || null, observacoes ?? null]
    );
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/pedidos/:id/itens — adiciona item durante validação
router.post('/:id/itens', compradorOuAdmin, async (req, res) => {
  try {
    const { codigo_barras, descricao, quantidade, preco_unitario, produto_novo } = req.body;
    if (!descricao || !quantidade || !preco_unitario) return res.status(400).json({ erro: 'descricao, quantidade e preco obrigatórios' });
    const ped = await dbQuery('SELECT status FROM pedidos WHERE id=$1', [req.params.id]);
    if (!ped.length) return res.status(404).json({ erro: 'Pedido não encontrado' });
    if (ped[0].status === 'vinculado') return res.status(400).json({ erro: 'Pedido vinculado — não editável' });
    const valor_total = parseFloat(quantidade) * parseFloat(preco_unitario);
    const r = await dbQuery(
      `INSERT INTO itens_pedido (pedido_id, codigo_barras, descricao, quantidade, preco_unitario, valor_total, produto_novo, qtd_validada, preco_validado, adicionado_pelo_comprador)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$4,$5,TRUE) RETURNING id`,
      [req.params.id, codigo_barras || null, descricao, quantidade, preco_unitario, valor_total, !!produto_novo]
    );
    await recalcTotalIgnorandoExcluidos(req.params.id);
    res.json({ ok: true, id: r[0].id });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// DELETE /api/pedidos/itens/:itemId — soft delete (marca como excluido_pelo_comprador)
router.delete('/itens/:itemId', compradorOuAdmin, async (req, res) => {
  try {
    const r = await dbQuery(
      `UPDATE itens_pedido SET excluido_pelo_comprador = TRUE
       WHERE id=$1
         AND pedido_id IN (SELECT id FROM pedidos WHERE status != 'vinculado')
       RETURNING pedido_id, adicionado_pelo_comprador`,
      [req.params.itemId]
    );
    if (!r.length) return res.status(404).json({ erro: 'Item não encontrado ou pedido vinculado' });
    // Se foi adicionado pelo comprador e ainda nem foi enviado, hard delete (não polui o histórico)
    if (r[0].adicionado_pelo_comprador) {
      await dbQuery('DELETE FROM itens_pedido WHERE id=$1', [req.params.itemId]);
    }
    await recalcTotalIgnorandoExcluidos(r[0].pedido_id);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

async function recalcTotalIgnorandoExcluidos(pedido_id) {
  await dbQuery(
    `UPDATE pedidos SET valor_total=(
       SELECT COALESCE(SUM(COALESCE(qtd_validada,quantidade)*COALESCE(preco_validado,preco_unitario)),0)
       FROM itens_pedido WHERE pedido_id=$1 AND COALESCE(excluido_pelo_comprador,FALSE)=FALSE
     ) WHERE id=$1`, [pedido_id]
  );
}

// Função interna: calcula sugestões + flag "sem_historico" por item
async function calcularSugestoes(pedido_id) {
  const ped = await dbQuery('SELECT loja_id FROM pedidos WHERE id=$1', [pedido_id]);
  if (!ped.length) return {};
  const itens = await dbQuery('SELECT id, codigo_barras FROM itens_pedido WHERE pedido_id=$1', [pedido_id]);
  const sug = {};
  for (const it of itens) {
    if (!it.codigo_barras) { sug[it.id] = { sugestao: null, media_dia: 0, existe: false }; continue; }
    const [est] = await dbQuery(
      'SELECT estdisponivel FROM produtos_externo WHERE codigobarra=$1 AND loja_id=$2 LIMIT 1',
      [it.codigo_barras, ped[0].loja_id]
    );
    const [trans] = await dbQuery(
      `SELECT COALESCE(SUM(COALESCE(ip.qtd_validada, ip.quantidade)),0) AS em_transito
       FROM itens_pedido ip JOIN pedidos pp ON pp.id=ip.pedido_id LEFT JOIN notas_entrada n ON n.id=pp.nota_id
       WHERE ip.codigo_barras=$1 AND pp.loja_id=$2 AND pp.id!=$3
         AND (pp.status='validado' OR (pp.status='vinculado' AND (n.id IS NULL OR n.status!='fechada')))`,
      [it.codigo_barras, ped[0].loja_id, pedido_id]
    );
    const [vendas] = await dbQuery(
      `SELECT COALESCE(SUM(qtd_vendida),0) AS total_90d FROM vendas_historico
       WHERE codigobarra=$1 AND loja_id=$2 AND data_venda >= CURRENT_DATE - INTERVAL '90 days'
         AND COALESCE(tipo_saida, 'venda') = 'venda'`,
      [it.codigo_barras, ped[0].loja_id]
    );
    const media_dia = parseFloat(vendas?.total_90d || 0) / 90;
    const existe = !!est;
    sug[it.id] = {
      sugestao: media_dia > 0 ? Math.max(0, 30 * media_dia - parseFloat(est?.estdisponivel || 0) - parseFloat(trans?.em_transito || 0)) : null,
      media_dia, existe,
    };
  }
  return sug;
}

const EXCESSO_FATOR = 1.30; // 30% acima da sugestão

// PUT /api/pedidos/:id/validar — comprador valida e ajusta
router.put('/:id/validar', compradorOuAdmin, async (req, res) => {
  try {
    const { itens, condicao_pagamento, observacoes, justificativas } = req.body;
    const rows = await dbQuery(
      `SELECT p.*, f.razao_social, f.fantasia, f.cnpj as fornecedor_cnpj,
              v.nome as vendedor_nome, v.email as vendedor_email, v.telefone as vendedor_tel,
              l.nome as loja_nome, l.cnpj as loja_cnpj
       FROM pedidos p
       LEFT JOIN fornecedores f ON f.id=p.fornecedor_id
       LEFT JOIN vendedores v ON v.id=p.vendedor_id
       LEFT JOIN lojas l ON l.id=p.loja_id
       WHERE p.id=$1`,
      [req.params.id]
    );
    if (!rows.length) return res.status(404).json({ erro: 'Pedido não encontrado' });
    const pedido = rows[0];
    if (['cancelado_pelo_vendedor','faturado','vinculado','rejeitado'].includes(pedido.status))
      return res.status(400).json({ erro: `Pedido com status "${pedido.status}" não pode ser validado` });

    // Atualiza valores validados nos itens
    if (Array.isArray(itens)) {
      for (const item of itens) {
        await dbQuery(
          `UPDATE itens_pedido SET qtd_validada=$1, preco_validado=$2 WHERE id=$3 AND pedido_id=$4`,
          [item.qtd_validada ?? item.quantidade, item.preco_validado ?? item.preco_unitario,
           item.id, req.params.id]
        );
      }
    }

    // Calcula sugestões e detecta excesso (>30% acima) OU produto sem histórico
    const sugestoes = await calcularSugestoes(req.params.id);
    const itensCheck = await dbQuery('SELECT id, codigo_barras, descricao, COALESCE(qtd_validada,quantidade) AS qtd, produto_novo FROM itens_pedido WHERE pedido_id=$1 AND COALESCE(excluido_pelo_comprador,FALSE)=FALSE', [req.params.id]);
    const excessos = [];
    for (const it of itensCheck) {
      const meta = sugestoes[it.id] || { sugestao: null, media_dia: 0, existe: false };
      // Salva sugestão_sistema no item
      await dbQuery('UPDATE itens_pedido SET sugestao_sistema=$1 WHERE id=$2', [meta.sugestao, it.id]);
      const qtd = parseFloat(it.qtd);
      if (qtd <= 0) continue;

      let motivo = null;
      if (meta.sugestao != null && meta.sugestao > 0 && qtd > meta.sugestao * EXCESSO_FATOR) {
        motivo = 'excesso_30pct';
      }

      if (motivo) {
        excessos.push({
          item_id: it.id,
          codigo_barras: it.codigo_barras,
          descricao: it.descricao,
          qtd,
          sugestao: meta.sugestao,
          media_dia: meta.media_dia,
          motivo,
        });
      }
    }

    // Se houver excesso, exige justificativas → vai pra auditoria
    if (excessos.length) {
      const just = justificativas || {};
      const semJustif = excessos.filter(e => !just[e.item_id] || !String(just[e.item_id]).trim());
      if (semJustif.length) {
        return res.status(400).json({
          erro: 'Justifique os itens acima da sugestão (>30%)',
          itens_excesso: excessos,
        });
      }
      // Salva justificativas
      for (const e of excessos) {
        await dbQuery(
          'UPDATE itens_pedido SET justificativa_excesso=$1 WHERE id=$2',
          [String(just[e.item_id]).trim(), e.item_id]
        );
      }
      // Vai pra auditoria
      await dbQuery(
        `UPDATE pedidos SET
           valor_total=(SELECT COALESCE(SUM(COALESCE(qtd_validada,quantidade)*COALESCE(preco_validado,preco_unitario)),0) FROM itens_pedido WHERE pedido_id=$1 AND COALESCE(excluido_pelo_comprador,FALSE)=FALSE),
           condicao_pagamento=$2, observacoes=$3,
           status='aguardando_auditoria'
         WHERE id=$1`,
        [req.params.id, condicao_pagamento ?? pedido.condicao_pagamento, observacoes ?? pedido.observacoes]
      );
      // Notifica vendedor in-app
      const pedAud = await dbQuery(`SELECT p.id, p.numero_pedido, p.vendedor_id, l.nome as loja_nome FROM pedidos p LEFT JOIN lojas l ON l.id=p.loja_id WHERE p.id=$1`, [req.params.id]);
      if (pedAud[0]?.vendedor_id) {
        criarNotificacao({
          destinatario_tipo: 'vendedor', destinatario_id: pedAud[0].vendedor_id,
          tipo: 'pedido_em_analise',
          titulo: `Pedido ${pedAud[0].numero_pedido} em análise`,
          corpo: `${pedAud[0].loja_nome || ''} — pedido enviado pra auditoria do administrador.`,
          url: '/vendedor.html'
        });
      }

      return res.json({
        ok: true, status: 'aguardando_auditoria',
        mensagem: `Pedido enviado para análise do Administrador (${excessos.length} ${excessos.length===1?'item':'itens'} acima da sugestão).`,
      });
    }

    // Sem excesso — fluxo normal (validado + email)
    await dbQuery(
      `UPDATE pedidos SET
         valor_total=(SELECT COALESCE(SUM(COALESCE(qtd_validada,quantidade)*COALESCE(preco_validado,preco_unitario)),0) FROM itens_pedido WHERE pedido_id=$1 AND COALESCE(excluido_pelo_comprador,FALSE)=FALSE),
         condicao_pagamento=$2, observacoes=$3,
         status='validado', validado_em=NOW(), validado_por=$4
       WHERE id=$1`,
      [req.params.id, condicao_pagamento ?? pedido.condicao_pagamento, observacoes ?? pedido.observacoes, req.usuario.nome]
    );

    const pedidoAtualizado = await dbQuery('SELECT * FROM pedidos WHERE id=$1', [req.params.id]);
    const itensFinais = await dbQuery('SELECT * FROM itens_pedido WHERE pedido_id=$1 ORDER BY id', [req.params.id]);

    if (pedido.vendedor_email) {
      const pdfBuffer = await gerarPDF({ ...pedidoAtualizado[0], ...pedido }, itensFinais);
      await enviarEmail(
        pedido.vendedor_email,
        `Pedido ${pedido.numero_pedido} validado — JR Lira`,
        templatePedidoEmail({ pedido: { ...pedidoAtualizado[0], ...pedido } }),
        [{ filename: `pedido-${pedido.numero_pedido}.pdf`, content: pdfBuffer }]
      );
    }
    // Notifica vendedor in-app
    if (pedido.vendedor_id) {
      criarNotificacao({
        destinatario_tipo: 'vendedor', destinatario_id: pedido.vendedor_id,
        tipo: 'pedido_validado',
        titulo: `Pedido ${pedido.numero_pedido} validado ✅`,
        corpo: `${pedido.loja_nome || ''} — pode faturar (prazo 48h).`,
        url: '/vendedor.html'
      });
    }

    res.json({ ok: true, status: 'validado' });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/pedidos/:id/revalidar-atrasado — comprador re-valida pedido atrasado
router.post('/:id/revalidar-atrasado', compradorOuAdmin, async (req, res) => {
  try {
    const r = await dbQuery(
      `UPDATE pedidos SET status='validado', atrasado_em=NULL, validado_em=NOW(), validado_por=$1
       WHERE id=$2 AND status='atrasado' RETURNING id`,
      [req.usuario.nome, req.params.id]
    );
    if (!r.length) return res.status(400).json({ erro: 'Pedido não está atrasado' });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/pedidos/:id/rejeitar — comprador rejeita pedido
router.post('/:id/rejeitar', compradorOuAdmin, async (req, res) => {
  try {
    const { motivo } = req.body;
    const ped = await dbQuery(
      `SELECT p.id, p.status, p.numero_pedido, p.vendedor_id, l.nome as loja_nome
       FROM pedidos p LEFT JOIN lojas l ON l.id=p.loja_id
       WHERE p.id=$1`,
      [req.params.id]
    );
    if (!ped.length) return res.status(404).json({ erro: 'Pedido não encontrado' });
    if (ped[0].status === 'vinculado') return res.status(400).json({ erro: 'Pedido vinculado a NF — não pode rejeitar' });
    await dbQuery(
      `UPDATE pedidos SET status='rejeitado', rejeitado_por=$1, rejeitado_em=NOW(), motivo_rejeicao=$2 WHERE id=$3`,
      [req.usuario.nome, motivo || null, req.params.id]
    );
    if (ped[0].vendedor_id) {
      criarNotificacao({
        destinatario_tipo: 'vendedor', destinatario_id: ped[0].vendedor_id,
        tipo: 'pedido_rejeitado',
        titulo: `Pedido ${ped[0].numero_pedido} rejeitado ❌`,
        corpo: `${ped[0].loja_nome || ''}${motivo ? ` — Motivo: ${motivo}` : ''}`,
        url: '/vendedor.html'
      });
    }
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/pedidos/aguardando-auditoria — apenas admin
// POST /api/pedidos/:id/auditar/aprovar — admin libera
router.post('/:id/auditar/aprovar', compradorOuAdmin, async (req, res) => {
  try {
    if (req.usuario.perfil !== 'admin') return res.status(403).json({ erro: 'Apenas admin' });
    const { ajustes } = req.body; // { item_id: nova_qtd }
    const ped = await dbQuery(
      `SELECT p.*, v.email as vendedor_email FROM pedidos p
       LEFT JOIN vendedores v ON v.id=p.vendedor_id WHERE p.id=$1`,
      [req.params.id]
    );
    if (!ped.length) return res.status(404).json({ erro: 'Pedido não encontrado' });
    if (ped[0].status !== 'aguardando_auditoria') return res.status(400).json({ erro: 'Pedido não está em auditoria' });

    // Aplica ajustes do admin (se houver)
    let foiEditado = false;
    if (ajustes && typeof ajustes === 'object' && Object.keys(ajustes).length) {
      for (const [itemId, qtd] of Object.entries(ajustes)) {
        await dbQuery(
          'UPDATE itens_pedido SET qtd_validada=$1 WHERE id=$2 AND pedido_id=$3',
          [parseFloat(qtd), parseInt(itemId), req.params.id]
        );
      }
      foiEditado = true;
    }

    await dbQuery(
      `UPDATE pedidos SET
         valor_total=(SELECT COALESCE(SUM(COALESCE(qtd_validada,quantidade)*COALESCE(preco_validado,preco_unitario)),0) FROM itens_pedido WHERE pedido_id=$1 AND COALESCE(excluido_pelo_comprador,FALSE)=FALSE),
         status='validado', validado_em=NOW(), validado_por=$2,
         auditado_por=$2, auditado_em=NOW(),
         editado_na_auditoria=$3
       WHERE id=$1`,
      [req.params.id, req.usuario.nome, foiEditado]
    );

    const pedAtu = await dbQuery('SELECT * FROM pedidos WHERE id=$1', [req.params.id]);
    const itens = await dbQuery('SELECT * FROM itens_pedido WHERE pedido_id=$1 ORDER BY id', [req.params.id]);
    if (ped[0].vendedor_email) {
      const pdfBuffer = await gerarPDF({ ...pedAtu[0], ...ped[0] }, itens);
      await enviarEmail(
        ped[0].vendedor_email,
        `Pedido ${ped[0].numero_pedido} validado — JR Lira`,
        templatePedidoEmail({ pedido: { ...pedAtu[0], ...ped[0] } }),
        [{ filename: `pedido-${ped[0].numero_pedido}.pdf`, content: pdfBuffer }]
      );
    }
    // Notifica vendedor in-app
    if (ped[0].vendedor_id) {
      const lojaRow = await dbQuery('SELECT l.nome FROM pedidos p LEFT JOIN lojas l ON l.id=p.loja_id WHERE p.id=$1', [req.params.id]);
      criarNotificacao({
        destinatario_tipo: 'vendedor', destinatario_id: ped[0].vendedor_id,
        tipo: 'pedido_validado',
        titulo: `Pedido ${ped[0].numero_pedido} aprovado pelo admin ✅`,
        corpo: `${lojaRow[0]?.nome || ''} — pode faturar (prazo 48h).`,
        url: '/vendedor.html'
      });
    }
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/pedidos/:id/vincular/:notaId — vincula XML ao pedido
router.post('/:id/vincular/:notaId', compradorOuAdmin, async (req, res) => {
  try {
    await dbQuery('UPDATE pedidos SET nota_id=$1 WHERE id=$2', [req.params.notaId, req.params.id]);
    await dbQuery(
      `UPDATE notas_entrada SET status='em_validacao_comercial', pedido_id=$1 WHERE id=$2`,
      [req.params.id, req.params.notaId]
    );
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/pedidos/:id/liberar — libera nota para importação após validação comercial
router.post('/:id/liberar', compradorOuAdmin, async (req, res) => {
  try {
    const rows = await dbQuery('SELECT nota_id FROM pedidos WHERE id=$1', [req.params.id]);
    if (!rows.length || !rows[0].nota_id) return res.status(400).json({ erro: 'Pedido sem nota vinculada' });
    await dbQuery(
      `UPDATE notas_entrada SET status='importada' WHERE id=$1 AND status='em_validacao_comercial'`,
      [rows[0].nota_id]
    );
    await dbQuery(`UPDATE pedidos SET status='vinculado' WHERE id=$1`, [req.params.id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/pedidos/:id/analise
router.get('/:id/analise', compradorOuAdmin, async (req, res) => {
  try {
    const pedRows = await dbQuery(
      `SELECT p.*, f.leadtime_dias FROM pedidos p
       LEFT JOIN fornecedores f ON f.id=p.fornecedor_id WHERE p.id=$1`,
      [req.params.id]
    );
    if (!pedRows.length) return res.status(404).json({ erro: 'Pedido não encontrado' });
    const ped = pedRows[0];
    const loja_id = ped.loja_id;
    const itens = await dbQuery('SELECT * FROM itens_pedido WHERE pedido_id=$1 ORDER BY id', [req.params.id]);

    const analise = await Promise.all(itens.map(async item => {
      const ean = item.codigo_barras;

      const [est] = await dbQuery(
        'SELECT estdisponivel, custoorigem FROM produtos_externo WHERE codigobarra=$1 AND loja_id=$2 LIMIT 1',
        [ean, loja_id]
      );

      const [transito] = await dbQuery(
        `SELECT COALESCE(SUM(COALESCE(ip.qtd_validada, ip.quantidade)), 0) AS em_transito
         FROM itens_pedido ip
         JOIN pedidos pp ON pp.id = ip.pedido_id
         LEFT JOIN notas_entrada n ON n.id = pp.nota_id
         WHERE ip.codigo_barras=$1 AND pp.loja_id=$2 AND pp.id!=$3
           AND (pp.status='validado' OR (pp.status='vinculado' AND (n.id IS NULL OR n.status!='fechada')))`,
        [ean, loja_id, req.params.id]
      );

      const [vendas] = await dbQuery(
        `SELECT COALESCE(SUM(qtd_vendida),0) AS total_90d,
                MAX(data_venda) AS ultima_venda
         FROM vendas_historico
         WHERE codigobarra=$1 AND loja_id=$2 AND data_venda >= CURRENT_DATE - INTERVAL '90 days'
           AND COALESCE(tipo_saida, 'venda') = 'venda'`,
        [ean, loja_id]
      );

      const ultimas_compras = await dbQuery(
        `SELECT ch.data_entrada, ch.fornecedor_cnpj,
                COALESCE(f.razao_social, ch.fornecedor_cnpj) AS fornecedor_nome,
                ch.qtd_comprada,
                CASE WHEN ch.qtd_comprada > 0 THEN ch.custo_total / ch.qtd_comprada ELSE 0 END AS preco_unitario
         FROM compras_historico ch
         LEFT JOIN LATERAL (
           SELECT razao_social FROM fornecedores
           WHERE REGEXP_REPLACE(cnpj, '\\D', '', 'g') = REGEXP_REPLACE(ch.fornecedor_cnpj, '\\D', '', 'g')
           LIMIT 1
         ) f ON true
         WHERE ch.codigobarra=$1 AND ch.loja_id=$2
           AND COALESCE(ch.tipo_entrada, 'compra') = 'compra'
         ORDER BY ch.data_entrada DESC LIMIT 5`,
        [ean, loja_id]
      );

      const estdisponivel = parseFloat(est?.estdisponivel || 0);
      const em_transito   = parseFloat(transito?.em_transito || 0);
      const total_90d     = parseFloat(vendas?.total_90d || 0);
      const media_dia     = parseFloat((total_90d / 90).toFixed(4));

      return {
        item_id:        item.id,
        codigo_barras:  ean,
        descricao:      item.descricao,
        qtd_pedido:     parseFloat(item.qtd_validada || item.quantidade),
        preco_pedido:   parseFloat(item.preco_validado || item.preco_unitario),
        estdisponivel,
        em_transito,
        media_dia,
        total_90d,
        ultima_venda:   vendas?.ultima_venda || null,
        ultimas_compras,
      };
    }));

    res.json({ leadtime_dias: ped.leadtime_dias || 7, analise });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/pedidos/:id/pdf — aceita token na query pra link <a href>
router.get('/:id/pdf', authPdf, async (req, res) => {
  try {
    const rows = await dbQuery(
      `SELECT p.*,
              COALESCE(f.razao_social, f2.razao_social) AS razao_social,
              COALESCE(f.fantasia, f2.fantasia) AS fantasia,
              COALESCE(f.cnpj, p.fornecedor_cnpj_snapshot, f2.cnpj) AS fornecedor_cnpj,
              v.nome as vendedor_nome, v.email as vendedor_email, v.telefone as vendedor_tel,
              l.nome as loja_nome, l.cnpj as loja_cnpj
       FROM pedidos p
       LEFT JOIN fornecedores f ON f.id=p.fornecedor_id
       LEFT JOIN vendedores v ON v.id=p.vendedor_id
       LEFT JOIN lojas l ON l.id=p.loja_id
       LEFT JOIN LATERAL (
         SELECT razao_social, fantasia, cnpj FROM fornecedores
         WHERE p.fornecedor_id IS NULL AND v.fornecedor_cnpj IS NOT NULL
           AND REGEXP_REPLACE(cnpj,'\\D','','g') = REGEXP_REPLACE(v.fornecedor_cnpj,'\\D','','g')
         LIMIT 1
       ) f2 ON TRUE
       WHERE p.id=$1`,
      [req.params.id]
    );
    if (!rows.length) return res.status(404).end();
    const itens = await dbQuery('SELECT * FROM itens_pedido WHERE pedido_id=$1 ORDER BY id', [req.params.id]);
    const buf = await gerarPDF(rows[0], itens);
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="pedido-${rows[0].numero_pedido || req.params.id}.pdf"`);
    res.send(buf);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// ── PDF ───────────────────────────────────────────────────────────

function gerarPDF(pedido, itens) {
  return new Promise((resolve, reject) => {
    const PAGE_W = 595, PAGE_H = 842, M = 36;
    const X = M, R = PAGE_W - M;
    const W = R - X;
    // bottom = 0 pra desligar auto-pagination ao escrever rodapé.
    // Quebras de página são feitas manualmente verificando PAGE_H - LIMITE_FOOTER.
    const LIMITE_FOOTER = M + 28; // espaço reservado pro rodapé
    const doc = new PDFDocument({ size: 'A4', margins: { top: M, bottom: 0, left: M, right: M }, bufferPages: true });
    const chunks = [];
    doc.on('data', c => chunks.push(c));
    doc.on('end', () => resolve(Buffer.concat(chunks)));
    doc.on('error', reject);

    const fmtMoeda = v => 'R$ ' + parseFloat(v || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    const fmtData = d => d ? new Date(d).toLocaleDateString('pt-BR') : '—';
    const cnpjFmt = c => {
      const s = String(c || '').replace(/\D/g, '');
      if (s.length !== 14) return c || '—';
      return `${s.slice(0,2)}.${s.slice(2,5)}.${s.slice(5,8)}/${s.slice(8,12)}-${s.slice(12)}`;
    };

    // ── PALETA ───────────────────────────────────────────────────
    const COR = {
      primaria: '#0f172a',
      secundaria: '#0ea5e9',
      texto: '#1e293b',
      cinza: '#64748b',
      claro: '#f1f5f9',
      borda: '#cbd5e1',
      alerta: '#b91c1c',
      ok: '#15803d',
      novo: '#0369a1',
    };

    // ── HEADER (loja destino em destaque) ────────────────────────
    doc.fillColor(COR.primaria).rect(0, 0, PAGE_W, 70).fill();
    doc.fillColor('#fff').font('Helvetica-Bold').fontSize(20)
      .text((pedido.loja_nome || 'LOJA').toUpperCase(), X, 20, { width: W, align: 'center' });
    doc.fillColor('#94a3b8').font('Helvetica').fontSize(10)
      .text(`CNPJ ${cnpjFmt(pedido.loja_cnpj)}`, X, 46, { width: W, align: 'center' });

    // Faixa secundária
    doc.fillColor(COR.secundaria).rect(0, 70, PAGE_W, 24).fill();
    doc.fillColor('#fff').font('Helvetica-Bold').fontSize(13)
      .text('PEDIDO DE COMPRA', X, 76, { width: W, align: 'center' });

    // ── CARDS DE INFORMAÇÃO ──────────────────────────────────────
    let y = 110;
    const numFmt = pedido.numero_pedido || ('#' + pedido.id);
    doc.fillColor(COR.cinza).font('Helvetica').fontSize(9);
    doc.text('PEDIDO Nº', X, y);
    doc.text('EMISSÃO', X + 200, y);
    doc.text('CONDIÇÃO', X + 380, y);
    doc.fillColor(COR.texto).font('Helvetica-Bold').fontSize(11);
    doc.text(numFmt, X, y + 12, { width: 195 });
    doc.text(fmtData(pedido.validado_em || pedido.enviado_em), X + 200, y + 12);
    doc.text(pedido.condicao_pagamento === 0 ? 'À vista' : (pedido.condicao_pagamento ? `${pedido.condicao_pagamento} dias` : '—'), X + 380, y + 12);

    y += 38;
    doc.moveTo(X, y).lineTo(R, y).strokeColor(COR.borda).stroke();

    // ── FORNECEDOR / VENDEDOR ────────────────────────────────────
    y += 12;
    const colW = (W - 12) / 2;
    // Box fornecedor
    doc.fillColor(COR.claro).rect(X, y, colW, 70).fill();
    doc.fillColor(COR.cinza).font('Helvetica-Bold').fontSize(8).text('FORNECEDOR', X + 10, y + 8);
    doc.fillColor(COR.texto).font('Helvetica-Bold').fontSize(11)
      .text(pedido.fantasia || pedido.razao_social || '—', X + 10, y + 22, { width: colW - 20 });
    doc.font('Helvetica').fontSize(9).fillColor(COR.cinza);
    if (pedido.razao_social && pedido.fantasia && pedido.razao_social !== pedido.fantasia) {
      doc.text(pedido.razao_social, X + 10, y + 40, { width: colW - 20 });
    }
    doc.text(`CNPJ ${cnpjFmt(pedido.fornecedor_cnpj)}`, X + 10, y + 54, { width: colW - 20 });

    // Box vendedor
    const xV = X + colW + 12;
    doc.fillColor(COR.claro).rect(xV, y, colW, 70).fill();
    doc.fillColor(COR.cinza).font('Helvetica-Bold').fontSize(8).text('VENDEDOR', xV + 10, y + 8);
    doc.fillColor(COR.texto).font('Helvetica-Bold').fontSize(11)
      .text(pedido.vendedor_nome || '—', xV + 10, y + 22, { width: colW - 20 });
    doc.font('Helvetica').fontSize(9).fillColor(COR.cinza);
    if (pedido.vendedor_email) doc.text(pedido.vendedor_email, xV + 10, y + 40, { width: colW - 20 });
    if (pedido.vendedor_tel) doc.text(pedido.vendedor_tel, xV + 10, y + 54, { width: colW - 20 });

    y += 84;

    // ── DETECTA ALTERAÇÕES ───────────────────────────────────────
    const itensComMeta = itens.map(it => {
      const qtdOrig = parseFloat(it.quantidade || 0);
      const precoOrig = parseFloat(it.preco_unitario || 0);
      const qtdVal = parseFloat(it.qtd_validada ?? it.quantidade);
      const precoVal = parseFloat(it.preco_validado ?? it.preco_unitario);
      const adicionado = !!it.adicionado_pelo_comprador;
      const excluido = !!it.excluido_pelo_comprador;
      const qtdMudou = !adicionado && !excluido && Math.abs(qtdVal - qtdOrig) > 0.001;
      const precoMudou = !adicionado && !excluido && Math.abs(precoVal - precoOrig) > 0.0001;
      return { ...it, qtdOrig, precoOrig, qtdVal, precoVal, adicionado, excluido, qtdMudou, precoMudou,
               alterado: qtdMudou || precoMudou || adicionado || excluido };
    });
    const totalAlteracoes = itensComMeta.filter(i => i.alterado).length;

    // ── BANNER DE ALTERAÇÕES ─────────────────────────────────────
    if (totalAlteracoes > 0) {
      doc.fillColor(COR.alerta).rect(X, y, W, 22).fill();
      doc.fillColor('#fff').font('Helvetica-Bold').fontSize(10)
        .text(`ATENCAO: Pedido com ${totalAlteracoes} ${totalAlteracoes===1?'alteracao':'alteracoes'} feita${totalAlteracoes===1?'':'s'} pelo comprador`,
          X, y + 6, { width: W, align: 'center' });
      y += 26;
      doc.fillColor(COR.cinza).font('Helvetica').fontSize(8)
        .text('Legenda:  ALT = qtd ou preco alterado   |   NOVO = item adicionado pelo comprador   |   REM = item removido',
          X, y, { width: W, align: 'center' });
      y += 14;
    }

    // ── TABELA DE ITENS ──────────────────────────────────────────
    // Larguras: marca 40, código 80, descrição 200, qtd 60, preço 75, total 80 = 535 (W=523, ajustar)
    const C = {
      mark:  X,
      code:  X + 40,
      desc:  X + 110,
      qtd:   X + 290,
      preco: X + 360,
      total: X + 445,
    };
    const wMark = 38, wCode = 70, wDesc = 178, wQtd = 68, wPreco = 83, wTotal = 78;

    // Header da tabela
    doc.fillColor(COR.primaria).rect(X, y, W, 22).fill();
    doc.fillColor('#fff').font('Helvetica-Bold').fontSize(8.5);
    doc.text('',           C.mark,  y + 7, { width: wMark, align: 'center' });
    doc.text('CODIGO',     C.code,  y + 7, { width: wCode });
    doc.text('DESCRICAO',  C.desc,  y + 7, { width: wDesc });
    doc.text('QTD',        C.qtd,   y + 7, { width: wQtd, align: 'right' });
    doc.text('PRECO UN.',  C.preco, y + 7, { width: wPreco, align: 'right' });
    doc.text('TOTAL',      C.total, y + 7, { width: wTotal, align: 'right' });
    y += 22;

    // Linhas
    let zebra = false;
    let valorTotalCalc = 0;
    for (const it of itensComMeta) {
      // Calcula altura da linha pela descrição
      doc.font('Helvetica').fontSize(8.5);
      const descTxt = it.descricao + (it.excluido ? '  [REMOVIDO]' : '');
      const descH = doc.heightOfString(descTxt, { width: wDesc });
      const rowH = Math.max(20, descH + 8);

      // Quebra de página
      if (y + rowH > PAGE_H - LIMITE_FOOTER) {
        doc.addPage();
        y = M;
      }

      // Fundo da linha
      let bg = zebra ? '#f8fafc' : '#fff';
      let textCor = COR.texto;
      let marcaTxt = '';
      let marcaCor = null;
      if (it.excluido) { bg = '#fef2f2'; textCor = '#9ca3af'; marcaTxt = 'REM'; marcaCor = '#9ca3af'; }
      else if (it.adicionado) { bg = '#eff6ff'; marcaTxt = 'NOVO'; marcaCor = COR.novo; }
      else if (it.qtdMudou || it.precoMudou) { bg = '#fef3c7'; marcaTxt = 'ALT'; marcaCor = COR.alerta; }

      doc.fillColor(bg).rect(X, y, W, rowH).fill();
      doc.fillColor(textCor);

      // Marca
      if (marcaTxt) {
        doc.fillColor(marcaCor).font('Helvetica-Bold').fontSize(7)
          .text(marcaTxt, C.mark, y + (rowH/2) - 4, { width: wMark, align: 'center' });
      }

      // Código + Descrição
      doc.fillColor(textCor).font('Helvetica').fontSize(8)
        .text(it.codigo_barras || '—', C.code, y + 6, { width: wCode });
      doc.font(it.adicionado ? 'Helvetica-Bold' : 'Helvetica').fontSize(8.5)
        .text(descTxt, C.desc, y + 5, { width: wDesc });

      // Qtd
      doc.font('Helvetica').fontSize(8).fillColor(textCor);
      const qtdLabel = it.qtdMudou
        ? `${it.qtdOrig.toFixed(2)} -> ${it.qtdVal.toFixed(2)}`
        : it.qtdVal.toFixed(2);
      doc.text(qtdLabel, C.qtd, y + 6, { width: wQtd, align: 'right' });

      // Preço
      const precoLabel = it.precoMudou
        ? `${fmtMoeda(it.precoOrig)} -> ${fmtMoeda(it.precoVal)}`
        : fmtMoeda(it.precoVal);
      doc.fontSize(it.precoMudou ? 7 : 8.5).text(precoLabel, C.preco, y + 6, { width: wPreco, align: 'right' });

      // Total
      doc.fontSize(8.5).font('Helvetica-Bold');
      if (it.excluido) doc.text('—', C.total, y + 6, { width: wTotal, align: 'right' });
      else {
        const tot = it.qtdVal * it.precoVal;
        valorTotalCalc += tot;
        doc.text(fmtMoeda(tot), C.total, y + 6, { width: wTotal, align: 'right' });
      }

      // Linha divisória sutil
      doc.strokeColor(COR.borda).moveTo(X, y + rowH).lineTo(R, y + rowH).stroke();

      y += rowH;
      zebra = !zebra;
    }

    // ── TOTAL ────────────────────────────────────────────────────
    if (y + 50 > PAGE_H - LIMITE_FOOTER) { doc.addPage(); y = M; }

    y += 6;
    doc.fillColor(COR.primaria).rect(R - 220, y, 220, 30).fill();
    doc.fillColor('#fff').font('Helvetica-Bold').fontSize(10)
      .text('VALOR TOTAL', R - 210, y + 6);
    doc.fontSize(15)
      .text(fmtMoeda(pedido.valor_total ?? valorTotalCalc), R - 210, y + 6, { width: 200, align: 'right' });
    y += 40;

    // ── RESUMO DE ALTERAÇÕES ─────────────────────────────────────
    if (totalAlteracoes > 0) {
      if (y + 60 > PAGE_H - LIMITE_FOOTER) { doc.addPage(); y = M; }
      doc.fillColor(COR.alerta).font('Helvetica-Bold').fontSize(10)
        .text('RESUMO DAS ALTERACOES', X, y);
      y += 16;
      doc.font('Helvetica').fontSize(8.5).fillColor(COR.texto);
      itensComMeta.filter(i => i.alterado).forEach(i => {
        const partes = [];
        if (i.adicionado) partes.push(`NOVO: ${i.qtdVal.toFixed(2)} x ${fmtMoeda(i.precoVal)}`);
        if (i.excluido) partes.push('REMOVIDO');
        if (i.qtdMudou) partes.push(`qtd ${i.qtdOrig.toFixed(2)} -> ${i.qtdVal.toFixed(2)}`);
        if (i.precoMudou) partes.push(`preco ${fmtMoeda(i.precoOrig)} -> ${fmtMoeda(i.precoVal)}`);
        const linha = `• ${i.descricao}: ${partes.join(' | ')}`;
        const h = doc.heightOfString(linha, { width: W });
        if (y + h > PAGE_H - LIMITE_FOOTER) { doc.addPage(); y = M; }
        doc.text(linha, X, y, { width: W });
        y += h + 2;
      });
      y += 6;
    }

    // ── OBSERVAÇÕES ──────────────────────────────────────────────
    if (pedido.observacoes) {
      if (y + 30 > PAGE_H - LIMITE_FOOTER) { doc.addPage(); y = M; }
      doc.fillColor(COR.cinza).font('Helvetica-Bold').fontSize(8).text('OBSERVACOES', X, y);
      y += 12;
      doc.fillColor(COR.texto).font('Helvetica').fontSize(9)
        .text(pedido.observacoes, X, y, { width: W });
      y += doc.heightOfString(pedido.observacoes, { width: W }) + 6;
    }

    // ── AVISO BOLETO ─────────────────────────────────────────────
    if (y + 25 > PAGE_H - LIMITE_FOOTER) { doc.addPage(); y = M; }
    doc.fillColor(COR.alerta).rect(X, y, W, 22).fillOpacity(0.08).fill().fillOpacity(1);
    doc.strokeColor(COR.alerta).rect(X, y, W, 22).stroke();
    doc.fillColor(COR.alerta).font('Helvetica-Bold').fontSize(9)
      .text('BOLETO SEMPRE EM COTA UNICA — E PROIBIDO PARCELAR', X, y + 7, { width: W, align: 'center' });

    // ── RODAPÉ EM TODAS AS PÁGINAS ───────────────────────────────
    const range = doc.bufferedPageRange();
    for (let i = range.start; i < range.start + range.count; i++) {
      doc.switchToPage(i);
      const yFooter = PAGE_H - 24;
      doc.strokeColor(COR.borda).moveTo(X, yFooter - 4).lineTo(R, yFooter - 4).stroke();
      doc.fillColor(COR.cinza).font('Helvetica').fontSize(8)
        .text('© 2026 Rodrigo Hoff & JRLira Tech. Todos os direitos reservados.',
          X, yFooter, { width: W, align: 'center', lineBreak: false });
      doc.fontSize(7)
        .text(`Pagina ${i + 1} de ${range.count}`, X, yFooter, { width: W, align: 'right', lineBreak: false });
    }
    doc.flushPages();

    doc.end();
  });
}

function templatePedidoEmail({ pedido, disclaimer }) {
  const aviso = disclaimer
    ? `<p style="background:#fef3c7;border-left:4px solid #f59e0b;color:#78350f;padding:10px 12px;font-size:13px;margin:0 0 16px;border-radius:4px"><strong>⚠ ${disclaimer}</strong></p>`
    : '';
  return `
<!DOCTYPE html><html lang="pt-BR"><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f4;padding:32px 0">
<tr><td align="center">
<table width="520" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:12px;overflow:hidden">
<tr><td style="background:#0ea5e9;padding:24px 32px;text-align:center">
  <h1 style="color:#fff;margin:0;font-size:20px">JR Lira Tech</h1>
  <p style="color:rgba(255,255,255,.85);margin:4px 0 0;font-size:13px">Pedido de Compra Aprovado</p>
</td></tr>
<tr><td style="padding:28px 32px">
  ${aviso}
  <p style="color:#333;font-size:14px">Olá, <strong>${pedido.vendedor_nome}</strong>!</p>
  <p style="color:#555;font-size:13px">Seu pedido <strong>${pedido.numero_pedido}</strong> foi validado pela equipe de compras. O PDF com o pedido está em anexo.</p>
  <table width="100%" style="background:#f8f8f8;border-radius:8px;padding:16px;margin:16px 0;font-size:13px">
    <tr><td><strong>Fornecedor:</strong> ${pedido.razao_social || pedido.fantasia}</td></tr>
    <tr><td><strong>Loja:</strong> ${pedido.loja_nome}</td></tr>
    <tr><td><strong>Condição:</strong> ${pedido.condicao_pagamento === 0 ? 'À vista' : `${pedido.condicao_pagamento} dias — cota única`}</td></tr>
    <tr><td><strong>Valor Total:</strong> R$ ${parseFloat(pedido.valor_total||0).toFixed(2).replace('.',',')}</td></tr>
  </table>
  <p style="color:#c00;font-size:12px;font-weight:bold">⚠ Boleto sempre em cota única — é proibido parcelar.</p>
</td></tr>
</table></td></tr></table>
</body></html>`;
}

// POST /api/pedidos/reenviar-emails  body: { ids?: int[], desde?: ISOdate, ate?: ISOdate, disclaimer?: string }
// Reenvia email com PDF pra pedidos validados (default: validados hoje).
router.post('/reenviar-emails', compradorOuAdmin, async (req, res) => {
  try {
    if (req.usuario.perfil !== 'admin') return res.status(403).json({ erro: 'Apenas admin' });
    const { ids, desde, ate } = req.body || {};
    const disclaimer = req.body?.disclaimer || 'Caso este pedido já tenha sido faturado, favor desconsiderar este email.';

    let where = `p.status='validado' AND v.email IS NOT NULL AND v.email <> ''`;
    const params = [];
    if (Array.isArray(ids) && ids.length) {
      params.push(ids);
      where += ` AND p.id = ANY($${params.length}::int[])`;
    } else {
      const di = desde || new Date().toISOString().slice(0,10);
      params.push(di);
      where += ` AND p.validado_em >= $${params.length}::date`;
      if (ate) { params.push(ate); where += ` AND p.validado_em < $${params.length}::date + INTERVAL '1 day'`; }
    }

    const rows = await dbQuery(
      `SELECT p.*,
              COALESCE(f.razao_social, f2.razao_social) AS razao_social,
              COALESCE(f.fantasia, f2.fantasia) AS fantasia,
              COALESCE(f.cnpj, p.fornecedor_cnpj_snapshot, f2.cnpj) AS fornecedor_cnpj,
              v.nome as vendedor_nome, v.email as vendedor_email, v.telefone as vendedor_tel,
              l.nome as loja_nome, l.cnpj as loja_cnpj
         FROM pedidos p
         LEFT JOIN fornecedores f ON f.id=p.fornecedor_id
         LEFT JOIN vendedores v ON v.id=p.vendedor_id
         LEFT JOIN lojas l ON l.id=p.loja_id
         LEFT JOIN LATERAL (
           SELECT razao_social, fantasia, cnpj FROM fornecedores
            WHERE p.fornecedor_id IS NULL AND v.fornecedor_cnpj IS NOT NULL
              AND REGEXP_REPLACE(cnpj,'\\D','','g') = REGEXP_REPLACE(v.fornecedor_cnpj,'\\D','','g')
            LIMIT 1
         ) f2 ON TRUE
        WHERE ${where}
        ORDER BY p.validado_em ASC`,
      params
    );

    const enviados = [];
    const falhas = [];
    for (const ped of rows) {
      try {
        const itens = await dbQuery('SELECT * FROM itens_pedido WHERE pedido_id=$1 ORDER BY id', [ped.id]);
        const pdf = await gerarPDF(ped, itens);
        await enviarEmail(
          ped.vendedor_email,
          `Pedido ${ped.numero_pedido} validado — JR Lira (reenvio)`,
          templatePedidoEmail({ pedido: ped, disclaimer }),
          [{ filename: `pedido-${ped.numero_pedido}.pdf`, content: pdf }]
        );
        enviados.push({ id: ped.id, numero_pedido: ped.numero_pedido, email: ped.vendedor_email });
      } catch (e) {
        falhas.push({ id: ped.id, numero_pedido: ped.numero_pedido, erro: e.message });
      }
    }
    res.json({ ok: true, total: rows.length, enviados: enviados.length, falhas: falhas.length, detalhe: { enviados, falhas } });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

module.exports = router;
module.exports.gerarPDF = gerarPDF;
module.exports.templatePedidoEmail = templatePedidoEmail;
