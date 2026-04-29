const express = require('express');
const router = express.Router();
const PDFDocument = require('pdfkit');
const pool = require('../db');
const { autenticar } = require('../auth');
const { enviarEmail } = require('../mailer');

function compradorOuAdmin(req, res, next) {
  if (!['admin', 'comprador'].includes(req.usuario.perfil))
    return res.status(403).json({ erro: 'Acesso restrito' });
  next();
}

// GET /api/pedidos — lista pedidos aguardando validação ou validados
router.get('/', autenticar, compradorOuAdmin, async (req, res) => {
  try {
    const { status } = req.query;
    const params = status ? [status] : [];
    const where = status ? 'WHERE p.status=$1' : "WHERE p.status != 'rascunho'";
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

// GET /api/pedidos/:id — detalhe completo
router.get('/:id', autenticar, compradorOuAdmin, async (req, res) => {
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
router.put('/:id/validar', autenticar, compradorOuAdmin, async (req, res) => {
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
router.post('/:id/vincular/:notaId', autenticar, compradorOuAdmin, async (req, res) => {
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
router.post('/:id/liberar', autenticar, compradorOuAdmin, async (req, res) => {
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

// GET /api/pedidos/:id/pdf
router.get('/:id/pdf', autenticar, compradorOuAdmin, async (req, res) => {
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

// Notas em validação comercial (para tela de notas-cadastro)
router.get('/notas-validacao', autenticar, compradorOuAdmin, async (req, res) => {
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
