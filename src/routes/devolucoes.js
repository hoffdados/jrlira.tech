const express = require('express');
const multer = require('multer');
const xml2js = require('xml2js');
const PDFDocument = require('pdfkit');
const router = express.Router();
const { query, pool } = require('../db');
const { autenticar } = require('../auth');
const { enviarEmail } = require('../mailer');
const { enviarWhatsapp } = require('../whatsapp');
const { checarLiberacaoNota } = require('./validades_em_risco');

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });
const upload2 = upload.fields([
  { name: 'xml', maxCount: 1 },
  { name: 'pdf', maxCount: 1 },
]);

function gerarResumoPdf(devolucao, itens, xmlInfo) {
  return new Promise((resolve) => {
    const chunks = [];
    const doc = new PDFDocument({ size: 'A4', margin: 36 });
    doc.on('data', c => chunks.push(c));
    doc.on('end', () => resolve(Buffer.concat(chunks)));
    doc.fontSize(16).fillColor('#0f172a').text('Resumo de Devolução — JR Lira Tech', { align: 'center' });
    doc.moveDown(0.5);
    doc.fontSize(9).fillColor('#475569').text('(Resumo gerado a partir do XML — não substitui DANFE oficial)', { align: 'center' });
    doc.moveDown();
    doc.fontSize(10).fillColor('#0f172a');
    doc.text(`NF de devolução: ${xmlInfo.numero || '—'}    Emissão: ${xmlInfo.data || '—'}`);
    doc.text(`Chave NF-e: ${xmlInfo.chave || '—'}`);
    doc.text(`Destinatário: ${devolucao.destinatario_nome} (${devolucao.destinatario_cnpj})`);
    doc.text(`Motivo: ${devolucao.motivo}`);
    doc.moveDown();
    doc.fontSize(11).fillColor('#0f172a').text('Itens devolvidos:', { underline: true });
    doc.moveDown(0.5);
    doc.fontSize(9);
    for (const i of itens) {
      const qt = i.qtd_caixas > 0 ? `${i.qtd_caixas} cx (${parseFloat(i.qtd_total).toFixed(0)} un)` : `${parseFloat(i.qtd_unidades).toFixed(0)} un`;
      doc.fillColor('#0f172a').text(`• ${(i.descricao||'').trim()}`);
      doc.fillColor('#475569').text(`  ${qt} — R$ ${Number(i.valor_total||0).toFixed(2)} ${i.ean ? '· EAN ' + i.ean : ''}`);
      doc.moveDown(0.3);
    }
    doc.moveDown();
    doc.fontSize(11).fillColor('#0f172a').text(`Valor total: R$ ${Number(devolucao.valor_total||0).toFixed(2)}`, { align: 'right' });
    doc.end();
  });
}

function extrairDadosXml(parsed) {
  const nfe = parsed?.nfeProc?.NFe?.infNFe || parsed?.NFe?.infNFe;
  if (!nfe) return null;
  const ide = nfe.ide || {};
  const dest = nfe.dest || {};
  const destCnpj = (dest.CNPJ || dest.CPF || '').replace(/\D/g, '');
  const numero = ide.nNF || '';
  const data = (ide.dhEmi || ide.dEmi || '').slice(0, 10);
  const chave = parsed?.nfeProc?.protNFe?.infProt?.chNFe || (nfe.$ && nfe.$.Id ? nfe.$.Id.replace(/^NFe/, '') : null);
  const tot = nfe.total?.ICMSTot || {};
  const vNF = parseFloat(tot.vNF) || 0;
  const vProd = parseFloat(tot.vProd) || 0;
  const vST = parseFloat(tot.vST) || 0;
  return { numero, data, chave, destCnpj, vNF, vProd, vST };
}

async function notificarVendedor(devolucaoId) {
  try {
    const [d] = await query(
      `SELECT d.*, n.pedido_id, n.cd_mov_codi
         FROM devolucoes d JOIN notas_entrada n ON n.id = d.nota_id
        WHERE d.id = $1`,
      [devolucaoId]
    );
    if (!d || !d.pedido_id) return { skipped: 'sem_pedido' };
    const [ped] = await query(
      `SELECT p.numero_pedido, v.nome AS vend_nome, v.email AS vend_email, v.telefone AS vend_tel,
              f.razao_social AS forn_nome
         FROM pedidos p
         LEFT JOIN vendedores v ON v.id = p.vendedor_id
         LEFT JOIN fornecedores f ON f.id = v.fornecedor_id
        WHERE p.id = $1`,
      [d.pedido_id]
    );
    if (!ped || (!ped.vend_email && !ped.vend_tel)) return { skipped: 'sem_contato_vendedor' };
    const itens = await query(`SELECT * FROM devolucoes_itens WHERE devolucao_id=$1 ORDER BY id`, [devolucaoId]);

    const linhasItens = itens.map(i => {
      const qt = i.qtd_caixas > 0 ? `${i.qtd_caixas} cx` : `${parseFloat(i.qtd_unidades).toFixed(0)} un`;
      return `• ${(i.descricao||'').trim()} — ${qt} (R$ ${Number(i.valor_total||0).toFixed(2)})`;
    }).join('\n');
    const valor = Number(d.valor_total || 0).toFixed(2);

    const canais = [];
    let status = 'enviado';

    // WhatsApp
    if (ped.vend_tel) {
      try {
        const msg = `📦 *JR Lira Tech — Devolução de produtos*\n\nOlá ${ped.vend_nome},\n\nFoi gerada uma NF de devolução pro pedido *${ped.numero_pedido}* (${ped.forn_nome}).\n\nMotivo: ${d.motivo}\nNF de devolução: ${d.xml_numero_nf}\nValor total: R$ ${valor}\n\nItens:\n${linhasItens}`;
        await enviarWhatsapp(ped.vend_tel, msg);
        canais.push('whatsapp');
      } catch (e) { console.error('[notif vend whatsapp]', e.message); status = 'parcial'; }
    }

    // Email
    if (ped.vend_email) {
      try {
        const html = `<div style="font-family:Arial,sans-serif;color:#0f172a">
          <h2 style="color:#0ea5e9">Devolução de produtos — Pedido ${ped.numero_pedido}</h2>
          <p>Olá <strong>${ped.vend_nome}</strong>,</p>
          <p>Foi gerada uma NF de devolução referente ao pedido <strong>${ped.numero_pedido}</strong> (fornecedor ${ped.forn_nome}).</p>
          <p><strong>Motivo:</strong> ${d.motivo}<br>
             <strong>NF de devolução:</strong> ${d.xml_numero_nf}<br>
             <strong>Valor total:</strong> R$ ${valor}</p>
          <p><strong>Itens devolvidos:</strong></p>
          <pre style="background:#f1f5f9;padding:12px;border-radius:6px;font-size:12px">${linhasItens}</pre>
          <p style="color:#64748b;font-size:12px;margin-top:24px">PDF da NF anexado a este email.</p>
        </div>`;
        const anexos = d.pdf_content ? [{ filename: `devolucao_${d.xml_numero_nf}.pdf`, content: d.pdf_content }] : [];
        await enviarEmail(ped.vend_email, `Devolução pedido ${ped.numero_pedido} — JR Lira Tech`, html, anexos);
        canais.push('email');
      } catch (e) { console.error('[notif vend email]', e.message); status = 'parcial'; }
    }

    if (!canais.length) status = 'falhou';
    await query(
      `UPDATE devolucoes
          SET vendedor_notificado_em=NOW(), vendedor_notificacao_canais=$2, vendedor_notificacao_status=$3
        WHERE id=$1`,
      [devolucaoId, canais.join(','), status]
    );
    return { ok: true, canais, status };
  } catch (e) {
    console.error('[notificarVendedor]', e.message);
    await query(
      `UPDATE devolucoes SET vendedor_notificacao_status='erro' WHERE id=$1`,
      [devolucaoId]
    );
    return { erro: e.message };
  }
}

function permitido(req) {
  return req.usuario.perfil === 'admin' || req.usuario.perfil === 'cadastro';
}
function bloquearSeNaoPermitido(req, res) {
  if (!permitido(req)) { res.status(403).json({ erro: 'Acesso restrito a admin/cadastro' }); return true; }
  return false;
}

// GET /api/devolucoes?status=&loja_id=
router.get('/', autenticar, async (req, res) => {
  if (bloquearSeNaoPermitido(req, res)) return;
  try {
    const where = ['1=1'];
    const params = [];
    if (req.query.status) { params.push(req.query.status); where.push(`d.status = $${params.length}`); }
    if (req.query.loja_id) { params.push(req.query.loja_id); where.push(`d.loja_id = $${params.length}`); }
    if (req.query.nota_id) { params.push(req.query.nota_id); where.push(`d.nota_id = $${params.length}::int`); }
    const whereSql = where.join(' AND ');

    const stats = await query(
      `SELECT COUNT(*)::int AS total,
              COALESCE(SUM(valor_total),0)::numeric(14,2) AS total_valor,
              COUNT(*) FILTER (WHERE status='aguardando')::int AS aguardando
         FROM devolucoes d WHERE ${whereSql}`,
      params
    );
    const rows = await query(
      `SELECT d.*, n.cd_mov_codi, n.numero_nota AS nota_origem_numero,
              (SELECT COUNT(*)::int FROM devolucoes_itens i WHERE i.devolucao_id = d.id) AS qtd_itens
         FROM devolucoes d
         JOIN notas_entrada n ON n.id = d.nota_id
        WHERE ${whereSql}
        ORDER BY d.criado_em DESC, d.id DESC
        LIMIT 500`,
      params
    );
    res.json({ ...stats[0], rows });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/devolucoes/divergencias — agregação de diferenças (XML vs esperado)
//   query: agrupar=fornecedor|loja|mes  ; loja_id=  ; fornecedor_cnpj=  ; de=YYYY-MM-DD  ; ate=YYYY-MM-DD
// IMPORTANTE: definir antes de '/:id' pra não cair em router.get('/:id')
router.get('/divergencias', autenticar, async (req, res) => {
  if (bloquearSeNaoPermitido(req, res)) return;
  try {
    const agrupar = String(req.query.agrupar || 'fornecedor').toLowerCase();
    const params = [];
    const conds = [`d.status = 'enviada'`, `d.valor_xml IS NOT NULL`];
    if (req.query.loja_id) { params.push(req.query.loja_id); conds.push(`d.loja_id = $${params.length}::int`); }
    if (req.query.fornecedor_cnpj) {
      params.push(req.query.fornecedor_cnpj.replace(/\D/g, ''));
      conds.push(`REGEXP_REPLACE(d.destinatario_cnpj,'\\D','','g') = $${params.length}`);
    }
    if (req.query.de)  { params.push(req.query.de);  conds.push(`d.enviada_em >= $${params.length}::date`); }
    if (req.query.ate) { params.push(req.query.ate); conds.push(`d.enviada_em <  ($${params.length}::date + INTERVAL '1 day')`); }
    const where = conds.join(' AND ');

    let groupCol, groupLabel;
    if (agrupar === 'loja') {
      groupCol = `d.loja_id::text`;
      groupLabel = `(SELECT nome FROM lojas WHERE id = d.loja_id)`;
    } else if (agrupar === 'mes') {
      groupCol = `to_char(d.enviada_em,'YYYY-MM')`;
      groupLabel = groupCol;
    } else {
      groupCol = `REGEXP_REPLACE(d.destinatario_cnpj,'\\D','','g')`;
      groupLabel = `MAX(d.destinatario_nome)`;
    }

    const rows = await query(
      `SELECT ${groupCol} AS chave, ${groupLabel} AS rotulo,
              COUNT(*)::int AS qtd_devolucoes,
              COALESCE(SUM(d.valor_total),0)::numeric(14,2) AS total_esperado,
              COALESCE(SUM(d.valor_xml),0)::numeric(14,2) AS total_xml,
              COALESCE(SUM(d.diferenca_valor),0)::numeric(14,2) AS total_diferenca,
              COUNT(*) FILTER (WHERE ABS(COALESCE(d.diferenca_valor,0)) >= 0.01)::int AS qtd_com_divergencia
         FROM devolucoes d
        WHERE ${where}
        GROUP BY ${groupCol}
        ORDER BY ABS(COALESCE(SUM(d.diferenca_valor),0)) DESC, total_xml DESC
        LIMIT 200`,
      params
    );

    const totais = await query(
      `SELECT COUNT(*)::int AS qtd_devolucoes,
              COALESCE(SUM(d.valor_total),0)::numeric(14,2) AS total_esperado,
              COALESCE(SUM(d.valor_xml),0)::numeric(14,2) AS total_xml,
              COALESCE(SUM(d.diferenca_valor),0)::numeric(14,2) AS total_diferenca,
              COUNT(*) FILTER (WHERE ABS(COALESCE(d.diferenca_valor,0)) >= 0.01)::int AS qtd_com_divergencia
         FROM devolucoes d
        WHERE ${where}`,
      params
    );

    res.json({ agrupar, total: totais[0], rows });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/devolucoes/divergencias/lista — devoluções individuais com divergência (drill-down)
router.get('/divergencias/lista', autenticar, async (req, res) => {
  if (bloquearSeNaoPermitido(req, res)) return;
  try {
    const params = [];
    const conds = [`d.status = 'enviada'`, `d.valor_xml IS NOT NULL`, `ABS(COALESCE(d.diferenca_valor,0)) >= 0.01`];
    if (req.query.loja_id) { params.push(req.query.loja_id); conds.push(`d.loja_id = $${params.length}::int`); }
    if (req.query.fornecedor_cnpj) {
      params.push(req.query.fornecedor_cnpj.replace(/\D/g, ''));
      conds.push(`REGEXP_REPLACE(d.destinatario_cnpj,'\\D','','g') = $${params.length}`);
    }
    if (req.query.de)  { params.push(req.query.de);  conds.push(`d.enviada_em >= $${params.length}::date`); }
    if (req.query.ate) { params.push(req.query.ate); conds.push(`d.enviada_em <  ($${params.length}::date + INTERVAL '1 day')`); }
    const rows = await query(
      `SELECT d.id, d.nota_id, d.loja_id, d.destinatario_cnpj, d.destinatario_nome,
              d.valor_total, d.valor_xml, d.diferenca_valor,
              d.xml_numero_nf, d.enviada_em, n.numero_nota AS nota_origem_numero
         FROM devolucoes d
         JOIN notas_entrada n ON n.id = d.nota_id
        WHERE ${conds.join(' AND ')}
        ORDER BY ABS(COALESCE(d.diferenca_valor,0)) DESC
        LIMIT 500`,
      params
    );
    res.json({ rows });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/devolucoes/:id (com itens)
router.get('/:id', autenticar, async (req, res) => {
  if (bloquearSeNaoPermitido(req, res)) return;
  try {
    const [d] = await query(
      `SELECT d.*, n.cd_mov_codi, n.numero_nota AS nota_origem_numero, n.fornecedor_nome AS nota_origem_fornec
         FROM devolucoes d JOIN notas_entrada n ON n.id = d.nota_id WHERE d.id=$1`,
      [req.params.id]
    );
    if (!d) return res.status(404).json({ erro: 'Devolução não encontrada' });
    const itens = await query(
      `SELECT * FROM devolucoes_itens WHERE devolucao_id=$1 ORDER BY id`,
      [req.params.id]
    );
    res.json({ devolucao: d, itens });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/devolucoes/:id/upload-xml — anexa XML (obrigatório) + PDF (opcional)
//   multipart fields: 'xml' (obrigatório), 'pdf' (opcional)
router.post('/:id/upload-xml', autenticar, upload2, async (req, res) => {
  if (bloquearSeNaoPermitido(req, res)) return;
  const xmlFile = req.files?.xml?.[0];
  const pdfFile = req.files?.pdf?.[0];
  if (!xmlFile) return res.status(400).json({ erro: 'Arquivo XML obrigatório (campo "xml")' });
  try {
    const [d] = await query(`SELECT * FROM devolucoes WHERE id=$1`, [req.params.id]);
    if (!d) return res.status(404).json({ erro: 'Devolução não encontrada' });
    if (d.status !== 'aguardando') return res.status(400).json({ erro: 'Devolução já tratada' });

    const xmlText = xmlFile.buffer.toString('utf8');
    const parsed = await xml2js.parseStringPromise(xmlText, { explicitArray: false });
    const xmlInfo = extrairDadosXml(parsed);
    if (!xmlInfo) return res.status(400).json({ erro: 'XML inválido — não é NF-e' });

    const destEsperado = (d.destinatario_cnpj || '').replace(/\D/g, '');
    if (xmlInfo.destCnpj !== destEsperado) {
      return res.status(400).json({
        erro: `Destinatário do XML (${xmlInfo.destCnpj}) não confere com o esperado (${destEsperado} — ${d.destinatario_nome})`
      });
    }

    // Decide PDF: usa o anexado, senão gera resumo
    let pdfBuf, pdfMime, pdfOrigem;
    if (pdfFile) {
      pdfBuf = pdfFile.buffer;
      pdfMime = pdfFile.mimetype || 'application/pdf';
      pdfOrigem = 'anexado';
    } else {
      const itens = await query(`SELECT * FROM devolucoes_itens WHERE devolucao_id=$1 ORDER BY id`, [req.params.id]);
      pdfBuf = await gerarResumoPdf(d, itens, xmlInfo);
      pdfMime = 'application/pdf';
      pdfOrigem = 'resumo_gerado';
    }

    await query(
      `UPDATE devolucoes
          SET xml_chave_nfe=$2, xml_numero_nf=$3, xml_data_emissao=$4,
              xml_anexado_em=NOW(), xml_anexado_por=$5,
              xml_content=$6, status='enviada',
              enviada_em=NOW(), enviada_por=$5,
              pdf_content=$7, pdf_mime=$8, pdf_origem=$9,
              valor_xml=$10, valor_xml_vprod=$11, valor_xml_vst=$12
        WHERE id=$1`,
      [req.params.id, xmlInfo.chave, xmlInfo.numero, xmlInfo.data || null,
       req.usuario.nome || req.usuario.usuario, xmlText,
       pdfBuf, pdfMime, pdfOrigem,
       xmlInfo.vNF || null, xmlInfo.vProd || null, xmlInfo.vST || null]
    );

    // Notifica vendedor (se nota tem pedido_id)
    const notif = await notificarVendedor(req.params.id);

    // Libera nota se todas as devoluções foram enviadas
    const pendentes = await query(
      `SELECT COUNT(*)::int AS qtd FROM devolucoes WHERE nota_id=$1 AND status='aguardando'`,
      [d.nota_id]
    );
    if (pendentes[0].qtd === 0) {
      await query(
        `UPDATE notas_entrada SET status='validada', validada_em=COALESCE(validada_em, NOW()),
            validada_por=COALESCE(validada_por, $2)
          WHERE id=$1 AND status = 'aguardando_devolucao'`,
        [d.nota_id, req.usuario.nome || req.usuario.usuario]
      );
    }
    res.json({ ok: true, numero_nf: xmlInfo.numero, chave: xmlInfo.chave, pdf_origem: pdfOrigem, notificacao: notif });
  } catch (e) {
    console.error('[devolucoes upload]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// POST /api/devolucoes/:id/notificar — re-enviar notificação ao vendedor
router.post('/:id/notificar', autenticar, async (req, res) => {
  if (bloquearSeNaoPermitido(req, res)) return;
  try {
    const r = await notificarVendedor(req.params.id);
    res.json(r);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/devolucoes/:id/pdf — baixa PDF (DANFE ou resumo)
router.get('/:id/pdf', autenticar, async (req, res) => {
  if (bloquearSeNaoPermitido(req, res)) return;
  try {
    const r = await query(`SELECT pdf_content, pdf_mime, xml_numero_nf FROM devolucoes WHERE id=$1`, [req.params.id]);
    if (!r.length || !r[0].pdf_content) return res.status(404).json({ erro: 'PDF não disponível' });
    res.setHeader('Content-Type', r[0].pdf_mime || 'application/pdf');
    res.setHeader('Content-Disposition', `inline; filename="devolucao_${r[0].xml_numero_nf || req.params.id}.pdf"`);
    res.send(r[0].pdf_content);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/devolucoes/:id/cancelar
router.post('/:id/cancelar', autenticar, async (req, res) => {
  if (bloquearSeNaoPermitido(req, res)) return;
  try {
    const r = await query(
      `UPDATE devolucoes SET status='cancelada', observacao=$2 WHERE id=$1 AND status='aguardando' RETURNING id, nota_id`,
      [req.params.id, req.body?.observacao || null]
    );
    if (!r.length) return res.status(404).json({ erro: 'Não encontrada ou já tratada' });
    // Dispensa de devolução: reavalia status da nota (pode finalizar se não houver outras devs pendentes)
    if (r[0].nota_id) await checarLiberacaoNota(r[0].nota_id);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

module.exports = router;
