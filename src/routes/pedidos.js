const express = require('express');
const router = express.Router();
const PDFDocument = require('pdfkit');
const pool = require('../db');
const { compradorOuAdmin } = require('../auth');
const { enviarEmail } = require('../mailer');

// GET /api/pedidos — lista pedidos aguardando validação ou validados
router.get('/', compradorOuAdmin, async (req, res) => {
  try {
    const { status, fornecedor_cnpj } = req.query;
    const params = [], conds = ["p.status != 'rascunho'"];
    if (status)          { params.push(status);                                               conds.push(`p.status=$${params.length}`); }
    if (fornecedor_cnpj) { params.push(fornecedor_cnpj.replace(/\D/g,'')); conds.push(`REGEXP_REPLACE(f.cnpj,'\\D','','g')=$${params.length}`); }
    const where = 'WHERE ' + conds.join(' AND ');
    const rows = await pool.query(
      `SELECT p.id, p.numero_pedido, p.status, p.valor_total, p.condicao_pagamento,
              p.criado_em, p.enviado_em, p.validado_em, p.validado_por,
              f.razao_social as fornecedor_nome, f.fantasia as fornecedor_fantasia,
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
    const rows = await pool.query(
      `WITH cnpjs_loja AS (
         SELECT DISTINCT REGEXP_REPLACE(fornecedor_cnpj, '\\D', '', 'g') AS cnpj_num
         FROM compras_historico WHERE loja_id = $1
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

    const rows = await pool.query(`
      WITH eans_forn AS (
        SELECT DISTINCT codigobarra
        FROM compras_historico
        WHERE REGEXP_REPLACE(fornecedor_cnpj, '\\D', '', 'g') = $1
          AND loja_id = $2
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

// POST /api/pedidos/de-sugestao
router.post('/de-sugestao', compradorOuAdmin, async (req, res) => {
  try {
    const { fornecedor_id, loja_id, condicao_pagamento, observacoes, itens } = req.body;
    if (!fornecedor_id || !loja_id || !itens?.length) return res.status(400).json({ erro: 'Campos obrigatórios faltando' });

    const numero_pedido = `SUG-${Date.now()}`;
    const [ped] = await pool.query(
      `INSERT INTO pedidos (numero_pedido, fornecedor_id, loja_id, status, condicao_pagamento, observacoes)
       VALUES ($1,$2,$3,'rascunho',$4,$5) RETURNING id`,
      [numero_pedido, fornecedor_id, loja_id, condicao_pagamento || 28, observacoes || null]
    );

    let valor_total = 0;
    for (const it of itens) {
      const qtd = parseFloat(it.quantidade);
      const preco = parseFloat(it.preco_unitario || 0);
      const vt = qtd * preco;
      valor_total += vt;
      await pool.query(
        `INSERT INTO itens_pedido (pedido_id, codigo_barras, descricao, quantidade, preco_unitario, valor_total)
         VALUES ($1,$2,$3,$4,$5,$6)`,
        [ped.id, it.codigo_barras || null, it.descricao, qtd, preco, vt]
      );
    }
    await pool.query('UPDATE pedidos SET valor_total=$1 WHERE id=$2', [valor_total, ped.id]);

    res.json({ id: ped.id, numero_pedido });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// Notas em validação comercial (para tela de notas-cadastro)
router.get('/notas-validacao', compradorOuAdmin, async (req, res) => {
  try {
    const rows = await pool.query(
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
    const rows = await pool.query(
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
    const itens = await pool.query('SELECT * FROM itens_pedido WHERE pedido_id=$1 ORDER BY id', [req.params.id]);
    res.json({ ...rows[0], itens });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// PUT /api/pedidos/:id/validar — comprador valida e ajusta
router.put('/:id/validar', compradorOuAdmin, async (req, res) => {
  try {
    const { itens, condicao_pagamento, observacoes } = req.body;
    const rows = await pool.query(
      `SELECT p.*, f.razao_social, f.fantasia, f.cnpj as fornecedor_cnpj,
              v.nome as vendedor_nome, v.email as vendedor_email,
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

    // Atualiza valores validados nos itens
    if (Array.isArray(itens)) {
      for (const item of itens) {
        await pool.query(
          `UPDATE itens_pedido SET qtd_validada=$1, preco_validado=$2 WHERE id=$3 AND pedido_id=$4`,
          [item.qtd_validada ?? item.quantidade, item.preco_validado ?? item.preco_unitario,
           item.id, req.params.id]
        );
      }
    }

    // Recalcula total com valores validados
    await pool.query(
      `UPDATE pedidos SET
         valor_total=(SELECT COALESCE(SUM(COALESCE(qtd_validada,quantidade)*COALESCE(preco_validado,preco_unitario)),0) FROM itens_pedido WHERE pedido_id=$1),
         condicao_pagamento=$2, observacoes=$3,
         status='validado', validado_em=NOW(), validado_por=$4
       WHERE id=$1`,
      [req.params.id, condicao_pagamento || pedido.condicao_pagamento, observacoes ?? pedido.observacoes, req.usuario.nome]
    );

    const pedidoAtualizado = await pool.query('SELECT * FROM pedidos WHERE id=$1', [req.params.id]);
    const itensFinais = await pool.query('SELECT * FROM itens_pedido WHERE pedido_id=$1 ORDER BY id', [req.params.id]);

    // Gera PDF e envia email
    if (pedido.vendedor_email) {
      const pdfBuffer = await gerarPDF({ ...pedidoAtualizado[0], ...pedido }, itensFinais);
      await enviarEmail(
        pedido.vendedor_email,
        `Pedido ${pedido.numero_pedido} validado — JR Lira`,
        templatePedidoEmail({ pedido: { ...pedidoAtualizado[0], ...pedido } }),
        [{ filename: `pedido-${pedido.numero_pedido}.pdf`, content: pdfBuffer }]
      );
    }

    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/pedidos/:id/vincular/:notaId — vincula XML ao pedido
router.post('/:id/vincular/:notaId', compradorOuAdmin, async (req, res) => {
  try {
    await pool.query('UPDATE pedidos SET nota_id=$1 WHERE id=$2', [req.params.notaId, req.params.id]);
    await pool.query(
      `UPDATE notas_entrada SET status='em_validacao_comercial', pedido_id=$1 WHERE id=$2`,
      [req.params.id, req.params.notaId]
    );
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/pedidos/:id/liberar — libera nota para importação após validação comercial
router.post('/:id/liberar', compradorOuAdmin, async (req, res) => {
  try {
    const rows = await pool.query('SELECT nota_id FROM pedidos WHERE id=$1', [req.params.id]);
    if (!rows.length || !rows[0].nota_id) return res.status(400).json({ erro: 'Pedido sem nota vinculada' });
    await pool.query(
      `UPDATE notas_entrada SET status='importada' WHERE id=$1 AND status='em_validacao_comercial'`,
      [rows[0].nota_id]
    );
    await pool.query(`UPDATE pedidos SET status='vinculado' WHERE id=$1`, [req.params.id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/pedidos/:id/analise
router.get('/:id/analise', compradorOuAdmin, async (req, res) => {
  try {
    const pedRows = await pool.query(
      `SELECT p.*, f.leadtime_dias FROM pedidos p
       LEFT JOIN fornecedores f ON f.id=p.fornecedor_id WHERE p.id=$1`,
      [req.params.id]
    );
    if (!pedRows.length) return res.status(404).json({ erro: 'Pedido não encontrado' });
    const ped = pedRows[0];
    const loja_id = ped.loja_id;
    const itens = await pool.query('SELECT * FROM itens_pedido WHERE pedido_id=$1 ORDER BY id', [req.params.id]);

    const analise = await Promise.all(itens.map(async item => {
      const ean = item.codigo_barras;

      const [est] = await pool.query(
        'SELECT estdisponivel, custoorigem FROM produtos_externo WHERE codigobarra=$1 AND loja_id=$2 LIMIT 1',
        [ean, loja_id]
      );

      const [transito] = await pool.query(
        `SELECT COALESCE(SUM(COALESCE(ip.qtd_validada, ip.quantidade)), 0) AS em_transito
         FROM itens_pedido ip
         JOIN pedidos pp ON pp.id = ip.pedido_id
         LEFT JOIN notas_entrada n ON n.id = pp.nota_id
         WHERE ip.codigo_barras=$1 AND pp.loja_id=$2 AND pp.id!=$3
           AND (pp.status='validado' OR (pp.status='vinculado' AND (n.id IS NULL OR n.status!='fechada')))`,
        [ean, loja_id, req.params.id]
      );

      const [vendas] = await pool.query(
        `SELECT COALESCE(SUM(qtd_vendida),0) AS total_90d,
                MAX(data_venda) AS ultima_venda
         FROM vendas_historico
         WHERE codigobarra=$1 AND loja_id=$2 AND data_venda >= CURRENT_DATE - INTERVAL '90 days'`,
        [ean, loja_id]
      );

      const ultimas_compras = await pool.query(
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

// GET /api/pedidos/:id/pdf
router.get('/:id/pdf', compradorOuAdmin, async (req, res) => {
  try {
    const rows = await pool.query(
      `SELECT p.*, f.razao_social, f.fantasia, f.cnpj as fornecedor_cnpj,
              v.nome as vendedor_nome, v.email as vendedor_email,
              l.nome as loja_nome, l.cnpj as loja_cnpj
       FROM pedidos p
       LEFT JOIN fornecedores f ON f.id=p.fornecedor_id
       LEFT JOIN vendedores v ON v.id=p.vendedor_id
       LEFT JOIN lojas l ON l.id=p.loja_id
       WHERE p.id=$1`,
      [req.params.id]
    );
    if (!rows.length) return res.status(404).end();
    const itens = await pool.query('SELECT * FROM itens_pedido WHERE pedido_id=$1 ORDER BY id', [req.params.id]);
    const buf = await gerarPDF(rows[0], itens);
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="pedido-${rows[0].numero_pedido || req.params.id}.pdf"`);
    res.send(buf);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// ── PDF ───────────────────────────────────────────────────────────

function gerarPDF(pedido, itens) {
  return new Promise((resolve, reject) => {
    const doc = new PDFDocument({ margin: 40, size: 'A4' });
    const chunks = [];
    doc.on('data', c => chunks.push(c));
    doc.on('end', () => resolve(Buffer.concat(chunks)));
    doc.on('error', reject);

    const fmtMoeda = v => `R$ ${parseFloat(v || 0).toFixed(2).replace('.', ',').replace(/\B(?=(\d{3})+(?!\d))/g, '.')}`;
    const fmtData = d => d ? new Date(d).toLocaleDateString('pt-BR') : '—';

    // Cabeçalho
    doc.fontSize(18).font('Helvetica-Bold').text('JR Lira Tech', { align: 'center' });
    doc.fontSize(11).font('Helvetica').text('Pedido de Compra', { align: 'center' });
    doc.moveDown(0.5);
    doc.moveTo(40, doc.y).lineTo(555, doc.y).stroke();
    doc.moveDown(0.5);

    // Dados do pedido
    doc.fontSize(10).font('Helvetica-Bold').text('PEDIDO Nº: ', { continued: true }).font('Helvetica').text(pedido.numero_pedido || '—');
    doc.font('Helvetica-Bold').text('Data: ', { continued: true }).font('Helvetica').text(fmtData(pedido.validado_em || pedido.enviado_em));
    doc.font('Helvetica-Bold').text('Condição: ', { continued: true }).font('Helvetica').text(`${pedido.condicao_pagamento || '—'} dias — cota única`);
    doc.moveDown(0.5);

    // Fornecedor / Loja
    doc.font('Helvetica-Bold').fontSize(10).text('FORNECEDOR');
    doc.font('Helvetica').text(pedido.razao_social || pedido.fantasia || '—');
    doc.text(`CNPJ: ${pedido.fornecedor_cnpj || '—'}`);
    doc.text(`Vendedor: ${pedido.vendedor_nome || '—'}`);
    doc.moveDown(0.5);
    doc.font('Helvetica-Bold').text('LOJA DESTINO');
    doc.font('Helvetica').text(pedido.loja_nome || '—');
    doc.text(`CNPJ: ${pedido.loja_cnpj || '—'}`);
    doc.moveDown(0.5);
    doc.moveTo(40, doc.y).lineTo(555, doc.y).stroke();
    doc.moveDown(0.5);

    // Tabela de itens
    const cols = { barcode: 40, desc: 110, qtde: 380, preco: 430, total: 490 };
    doc.font('Helvetica-Bold').fontSize(9);
    doc.text('CÓDIGO', cols.barcode, doc.y, { width: 65 });
    doc.text('DESCRIÇÃO', cols.desc, doc.y - doc.currentLineHeight(), { width: 265 });
    doc.text('QTDE', cols.qtde, doc.y - doc.currentLineHeight(), { width: 45, align: 'right' });
    doc.text('PREÇO', cols.preco, doc.y - doc.currentLineHeight(), { width: 55, align: 'right' });
    doc.text('TOTAL', cols.total, doc.y - doc.currentLineHeight(), { width: 65, align: 'right' });
    doc.moveDown(0.3);
    doc.moveTo(40, doc.y).lineTo(555, doc.y).stroke();
    doc.moveDown(0.2);

    doc.font('Helvetica').fontSize(8);
    for (const item of itens) {
      const y = doc.y;
      const qtde = parseFloat(item.qtd_validada ?? item.quantidade);
      const preco = parseFloat(item.preco_validado ?? item.preco_unitario);
      const total = qtde * preco;
      if (item.produto_novo) doc.fillColor('#e07000'); else doc.fillColor('#000');
      doc.text(item.codigo_barras || '—', cols.barcode, y, { width: 65 });
      doc.text((item.produto_novo ? '★ ' : '') + item.descricao, cols.desc, y, { width: 265 });
      doc.text(qtde.toFixed(2), cols.qtde, y, { width: 45, align: 'right' });
      doc.text(fmtMoeda(preco), cols.preco, y, { width: 55, align: 'right' });
      doc.text(fmtMoeda(total), cols.total, y, { width: 65, align: 'right' });
      doc.fillColor('#000');
      doc.moveDown(0.4);
    }

    doc.moveTo(40, doc.y).lineTo(555, doc.y).stroke();
    doc.moveDown(0.5);
    doc.font('Helvetica-Bold').fontSize(11)
      .text(`VALOR TOTAL: ${fmtMoeda(pedido.valor_total)}`, { align: 'right' });

    if (pedido.observacoes) {
      doc.moveDown(0.5);
      doc.font('Helvetica-Bold').fontSize(9).text('Observações:');
      doc.font('Helvetica').text(pedido.observacoes);
    }

    doc.moveDown(1);
    doc.fontSize(8).fillColor('#c00')
      .text('⚠ BOLETO SEMPRE EM COTA ÚNICA — É PROIBIDO PARCELAR.', { align: 'center' });

    doc.end();
  });
}

function templatePedidoEmail({ pedido }) {
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
  <p style="color:#333;font-size:14px">Olá, <strong>${pedido.vendedor_nome}</strong>!</p>
  <p style="color:#555;font-size:13px">Seu pedido <strong>${pedido.numero_pedido}</strong> foi validado pela equipe de compras. O PDF com o pedido está em anexo.</p>
  <table width="100%" style="background:#f8f8f8;border-radius:8px;padding:16px;margin:16px 0;font-size:13px">
    <tr><td><strong>Fornecedor:</strong> ${pedido.razao_social || pedido.fantasia}</td></tr>
    <tr><td><strong>Loja:</strong> ${pedido.loja_nome}</td></tr>
    <tr><td><strong>Condição:</strong> ${pedido.condicao_pagamento} dias — cota única</td></tr>
    <tr><td><strong>Valor Total:</strong> R$ ${parseFloat(pedido.valor_total||0).toFixed(2).replace('.',',')}</td></tr>
  </table>
  <p style="color:#c00;font-size:12px;font-weight:bold">⚠ Boleto sempre em cota única — é proibido parcelar.</p>
</td></tr>
</table></td></tr></table>
</body></html>`;
}

module.exports = router;
