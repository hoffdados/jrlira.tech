const express = require('express');
const router = express.Router();
const multer = require('multer');
const { pool, query: dbQuery } = require('../db');
const { apenasAdmin, compradorOuAdmin } = require('../auth');
const { parseNFeSimples, MAX_XML_BYTES } = require('../parsers/nfe');

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: MAX_XML_BYTES } });

function n(v) { return parseFloat(v) || 0; }

async function atualizarSaldo(client, debito_id) {
  const { rows: [{ soma }] } = await client.query(
    'SELECT COALESCE(SUM(valor),0) AS soma FROM cr_creditos WHERE debito_id=$1', [debito_id]
  );
  const { rows: [deb] } = await client.query('SELECT valor_total, status FROM cr_debitos WHERE id=$1', [debito_id]);
  // Crédito total → baixado; parcial → parcial; zero → mantém aberto/cobrado anterior
  let status;
  const s = parseFloat(soma);
  if (s >= parseFloat(deb.valor_total)) status = 'baixado';
  else if (s > 0) status = 'parcial';
  else status = (deb.status === 'cobrado' ? 'cobrado' : 'aberto');
  await client.query(
    'UPDATE cr_debitos SET valor_creditos=$1, status=$2 WHERE id=$3',
    [soma, status, debito_id]
  );
}

// POST /api/cr/debitos/importar — importar XML de devolução/avaria
router.post('/debitos/importar', compradorOuAdmin, upload.single('xml'), async (req, res) => {
  const client = await pool.connect();
  try {
    if (!req.file) return res.status(400).json({ erro: 'Arquivo XML obrigatório' });
    const loja_id = parseInt(req.body.loja_id) || null;
    if (!loja_id) return res.status(400).json({ erro: 'Loja obrigatória' });

    const nfe = await parseNFeSimples(req.file.buffer);

    // Para devolução que emitimos: dest = fornecedor
    const cnpj_forn = nfe.dest_cnpj.replace(/\D/g, '');
    const forn = await client.query(
      "SELECT id, razao_social FROM fornecedores WHERE REGEXP_REPLACE(cnpj,'\\D','','g')=$1 LIMIT 1",
      [cnpj_forn]
    );

    await client.query('BEGIN');
    const ins = await client.query(
      `INSERT INTO cr_debitos
         (fornecedor_id, fornecedor_cnpj, fornecedor_nome, loja_id,
          numero_nota, chave_nfe, data_emissao, natureza_operacao,
          valor_produtos, valor_total, importado_por)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
       ON CONFLICT (chave_nfe) DO NOTHING
       RETURNING id`,
      [forn.rows[0]?.id || null,
       nfe.dest_cnpj,
       forn.rows[0]?.razao_social || nfe.dest_nome || nfe.dest_cnpj,
       loja_id, nfe.numero_nota, nfe.chave_nfe, nfe.data_emissao,
       nfe.natureza_operacao, nfe.valor_produtos, nfe.valor_total,
       req.usuario.email || req.usuario.usuario]
    );

    if (!ins.rows.length) {
      await client.query('ROLLBACK');
      const existente = await client.query('SELECT id FROM cr_debitos WHERE chave_nfe=$1', [nfe.chave_nfe]);
      return res.status(409).json({ erro: 'Nota já importada', id: existente.rows[0]?.id });
    }
    const deb = ins.rows[0];

    if (nfe.itens.length) {
      const cb = [], dc = [], nc = [], qt = [], vu = [], vt = [];
      for (const it of nfe.itens) {
        cb.push(it.codigo_barras); dc.push(it.descricao); nc.push(it.ncm);
        qt.push(it.quantidade); vu.push(it.valor_unitario); vt.push(it.valor_total);
      }
      await client.query(
        `INSERT INTO cr_debito_itens (debito_id, codigo_barras, descricao, ncm, quantidade, valor_unitario, valor_total)
         SELECT $1, * FROM UNNEST($2::text[], $3::text[], $4::text[], $5::numeric[], $6::numeric[], $7::numeric[])`,
        [deb.id, cb, dc, nc, qt, vu, vt]
      );
    }

    await client.query('COMMIT');
    res.json({ id: deb.id, numero_nota: nfe.numero_nota, valor_total: nfe.valor_total });
  } catch (e) {
    await client.query('ROLLBACK');
    res.status(500).json({ erro: e.message });
  } finally { client.release(); }
});

// GET /api/cr/debitos
router.get('/debitos', compradorOuAdmin, async (req, res) => {
  try {
    const { status, fornecedor_id, loja_id, de, ate } = req.query;
    const conds = [], params = [];
    if (status)       { params.push(status);       conds.push(`d.status=$${params.length}`); }
    if (fornecedor_id){ params.push(fornecedor_id); conds.push(`d.fornecedor_id=$${params.length}`); }
    if (loja_id)      { params.push(loja_id);       conds.push(`d.loja_id=$${params.length}`); }
    if (de)           { params.push(de);            conds.push(`d.data_emissao>=$${params.length}`); }
    if (ate)          { params.push(ate);           conds.push(`d.data_emissao<=$${params.length}`); }
    const where = conds.length ? 'WHERE ' + conds.join(' AND ') : '';
    const { rows } = await pool.query(
      `SELECT d.id, d.numero_nota, d.data_emissao, d.natureza_operacao,
              d.fornecedor_cnpj, d.loja_id,
              COALESCE(f.razao_social, d.fornecedor_nome) AS fornecedor_nome,
              d.valor_total::float, d.valor_creditos::float,
              (d.valor_total - d.valor_creditos)::float AS saldo_aberto,
              d.status, d.importado_em, d.importado_por
       FROM cr_debitos d
       LEFT JOIN fornecedores f ON f.id = d.fornecedor_id
       ${where} ORDER BY d.data_emissao DESC NULLS LAST, d.id DESC`,
      params
    );
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/cr/debitos/:id
router.get('/debitos/:id', compradorOuAdmin, async (req, res) => {
  try {
    const { rows: [deb] } = await pool.query(
      `SELECT d.*, COALESCE(f.razao_social, d.fornecedor_nome) AS fornecedor_nome,
              (d.valor_total - d.valor_creditos)::float AS saldo_aberto,
              d.valor_total::float, d.valor_creditos::float
       FROM cr_debitos d LEFT JOIN fornecedores f ON f.id=d.fornecedor_id
       WHERE d.id=$1`, [req.params.id]
    );
    if (!deb) return res.status(404).json({ erro: 'Não encontrado' });
    const { rows: itens } = await pool.query(
      'SELECT * FROM cr_debito_itens WHERE debito_id=$1 ORDER BY id', [req.params.id]
    );
    const { rows: creditos } = await pool.query(
      'SELECT * FROM cr_creditos WHERE debito_id=$1 ORDER BY registrado_em', [req.params.id]
    );

    // Se é cobrança de avaria/devolução, carrega notas de origem + email/telefone do CD
    let notas_origem = [];
    let contato = {};
    const { rows: [avariaCob] } = await pool.query(
      `SELECT id FROM avarias_cobrancas WHERE cr_debito_id=$1 LIMIT 1`, [req.params.id]);
    if (avariaCob) {
      const { rows } = await pool.query(`
        SELECT ae.tipo,
               dch.numero_nfe AS numero_nota,
               COALESCE(dch.data_devolucao, dch.data_nfe) AS data_nota,
               dch.valor_total::numeric(14,2) AS valor_nota,
               COUNT(*)::int AS qtd_itens,
               SUM(ae.valor_total)::numeric(14,2) AS valor_cobrado
          FROM avaria_eventos ae
          LEFT JOIN devolucoes_compra_itens_historico dci
            ON ae.fonte='devolucoes_compra_historico' AND dci.id = ae.fonte_id
          LEFT JOIN devolucoes_compra_historico dch
            ON dch.loja_id = dci.loja_id AND dch.devolucao_codigo = dci.devolucao_codigo
         WHERE ae.avaria_cobranca_id = $1
         GROUP BY ae.tipo, dch.numero_nfe, COALESCE(dch.data_devolucao, dch.data_nfe), dch.valor_total
         ORDER BY data_nota NULLS LAST`, [avariaCob.id]);
      notas_origem = rows;
    }
    // Vendedores credenciados do fornecedor (tabela vendedores do app)
    let vendedores = [];
    if (deb.fornecedor_id) {
      vendedores = (await pool.query(
        `SELECT id, nome, email, telefone, nome_gerente, telefone_gerente
           FROM vendedores
          WHERE fornecedor_id = $1 AND status = 'aprovado'
          ORDER BY nome`, [deb.fornecedor_id])).rows;
    }
    if (!vendedores.length && deb.fornecedor_cnpj) {
      vendedores = (await pool.query(
        `SELECT id, nome, email, telefone, nome_gerente, telefone_gerente
           FROM vendedores
          WHERE REGEXP_REPLACE(COALESCE(fornecedor_cnpj,''),'\\D','','g')
              = REGEXP_REPLACE($1,'\\D','','g')
            AND status = 'aprovado'
          ORDER BY nome`, [deb.fornecedor_cnpj])).rows;
    }
    // Fallback: cd_fornecedor (UltraSyst) se nenhum vendedor cadastrado
    if (!vendedores.length && deb.fornecedor_cnpj) {
      const { rows: [c] } = await pool.query(
        `SELECT DISTINCT for_email, for_telefone FROM cd_fornecedor
          WHERE REGEXP_REPLACE(COALESCE(for_cgc,''),'\\D','','g') = REGEXP_REPLACE($1,'\\D','','g')
            AND (for_email IS NOT NULL OR for_telefone IS NOT NULL) LIMIT 1`, [deb.fornecedor_cnpj]);
      if (c) contato = { email: c.for_email, telefone: c.for_telefone, fonte: 'ultrasyst' };
    }

    res.json({ ...deb, itens, creditos, notas_origem, vendedores, contato });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/cr/debitos/:id/marcar-cobrado — sinaliza que o débito foi cobrado do fornecedor
// (não baixa: pagamento entra via crédito). Propaga pra avarias_cobrancas.
router.post('/debitos/:id/marcar-cobrado', compradorOuAdmin, async (req, res) => {
  const id = parseInt(req.params.id);
  const por = req.usuario.email || req.usuario.usuario || `id:${req.usuario.id}`;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Só muda pra cobrado se ainda está aberto (ou re-marca quando voltou). Não sobrepõe parcial/baixado.
    await client.query(
      `UPDATE cr_debitos SET status='cobrado' WHERE id=$1 AND status='aberto'`, [id]);
    await client.query(`
      UPDATE avarias_cobrancas
         SET status='cobrado', cobrado_em=NOW(), cobrado_por=$2
       WHERE cr_debito_id=$1 AND status='pendente'`, [id, por]);
    await client.query(`
      UPDATE avaria_eventos
         SET status='cobrado'
       WHERE avaria_cobranca_id IN (SELECT id FROM avarias_cobrancas WHERE cr_debito_id=$1)`, [id]);
    await client.query('COMMIT');
    res.json({ ok: true });
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch {}
    res.status(500).json({ erro: e.message });
  } finally { client.release(); }
});

// POST /api/cr/debitos/:id/enviar — manda cobrança por email e/ou WhatsApp
// body: { canais: ['email','whatsapp'], vendedor_id?, email_override?, telefone_override? }
router.post('/debitos/:id/enviar', compradorOuAdmin, async (req, res) => {
  const { canais, vendedor_id, email_override, telefone_override } = req.body || {};
  if (!Array.isArray(canais) || !canais.length) return res.status(400).json({ erro: 'canais obrigatorio' });
  try {
    const { rows: [deb] } = await pool.query(
      `SELECT d.*, l.nome AS loja_nome FROM cr_debitos d
         LEFT JOIN lojas l ON l.id=d.loja_id WHERE d.id=$1`, [req.params.id]);
    if (!deb) return res.status(404).json({ erro: 'débito não encontrado' });
    const { rows: itens } = await pool.query(
      `SELECT * FROM cr_debito_itens WHERE debito_id=$1 ORDER BY id`, [req.params.id]);

    // Notas origem (se for avaria/devolução)
    let notas = [];
    const { rows: [cob] } = await pool.query(
      `SELECT id FROM avarias_cobrancas WHERE cr_debito_id=$1 LIMIT 1`, [req.params.id]);
    if (cob) {
      const { rows } = await pool.query(`
        SELECT dch.numero_nfe, COALESCE(dch.data_devolucao, dch.data_nfe) AS data_nota, dch.valor_total
          FROM avaria_eventos ae
          LEFT JOIN devolucoes_compra_itens_historico dci ON ae.fonte='devolucoes_compra_historico' AND dci.id=ae.fonte_id
          LEFT JOIN devolucoes_compra_historico dch ON dch.loja_id=dci.loja_id AND dch.devolucao_codigo=dci.devolucao_codigo
         WHERE ae.avaria_cobranca_id=$1 AND dch.numero_nfe IS NOT NULL
         GROUP BY dch.numero_nfe, COALESCE(dch.data_devolucao, dch.data_nfe), dch.valor_total`, [cob.id]);
      notas = rows;
    }

    // Destino: prioridade vendedor_id > overrides > cd_fornecedor
    let email = email_override || null, telefone = telefone_override || null;
    if (vendedor_id) {
      const { rows: [v] } = await pool.query(
        `SELECT email, telefone FROM vendedores WHERE id=$1`, [vendedor_id]);
      if (v) { email = v.email || email; telefone = v.telefone || telefone; }
    } else if (!email && !telefone && deb.fornecedor_cnpj) {
      const { rows: [c] } = await pool.query(
        `SELECT DISTINCT for_email, for_telefone FROM cd_fornecedor
          WHERE REGEXP_REPLACE(COALESCE(for_cgc,''),'\\D','','g') = REGEXP_REPLACE($1,'\\D','','g')
            AND (for_email IS NOT NULL OR for_telefone IS NOT NULL) LIMIT 1`,
        [deb.fornecedor_cnpj]);
      if (c) { email = c.for_email; telefone = c.for_telefone; }
    }

    const fmtBRL = v => 'R$ ' + Number(v||0).toLocaleString('pt-BR', {minimumFractionDigits:2});
    const fmtD = d => d ? new Date(d).toLocaleDateString('pt-BR') : '—';
    const resultado = { email: null, whatsapp: null };

    // EMAIL via Resend
    if (canais.includes('email')) {
      if (!email) { resultado.email = { erro: 'fornecedor sem email no cd_fornecedor' }; }
      else {
        const linhasItens = itens.map(it => `
          <tr><td>${(it.codigo_barras||'').toString().slice(0,14)}</td>
              <td>${(it.descricao||'').replace(/</g,'&lt;')}</td>
              <td style="text-align:right">${Number(it.quantidade).toFixed(0)}</td>
              <td style="text-align:right">${fmtBRL(it.valor_unitario)}</td>
              <td style="text-align:right">${fmtBRL(it.valor_total)}</td></tr>`).join('');
        const linhasNotas = notas.map(n => `
          <tr><td>${n.numero_nfe}</td><td>${fmtD(n.data_nota)}</td><td style="text-align:right">${fmtBRL(n.valor_total)}</td></tr>`).join('');
        const html = `
          <h2>Cobrança JR Lira — ${deb.numero_nota}</h2>
          <p><b>Loja:</b> ${deb.loja_nome || '—'}<br>
             <b>Natureza:</b> ${deb.natureza_operacao || '—'}<br>
             <b>Data:</b> ${fmtD(deb.data_emissao)}</p>
          <h3>Produtos (${itens.length})</h3>
          <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-family:Arial;font-size:12px;width:100%">
            <thead style="background:#f1f5f9"><tr><th>Cód.Barras</th><th>Descrição</th><th>Qtd</th><th>Unit.</th><th>Total</th></tr></thead>
            <tbody>${linhasItens}</tbody>
            <tfoot><tr><td colspan="4" style="text-align:right"><b>TOTAL</b></td><td style="text-align:right"><b>${fmtBRL(deb.valor_total)}</b></td></tr></tfoot>
          </table>
          ${notas.length ? `<h3>Notas referentes</h3>
            <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-family:Arial;font-size:12px">
              <thead style="background:#f1f5f9"><tr><th>NF</th><th>Data</th><th>Valor</th></tr></thead>
              <tbody>${linhasNotas}</tbody>
            </table>` : ''}
          <p style="margin-top:20px;color:#475569;font-size:11px">Cobrança gerada automaticamente em ${new Date().toLocaleDateString('pt-BR')}.</p>`;
        try {
          const { enviarEmail } = require('../mailer');
          await enviarEmail(email,
            `Cobrança JR Lira — ${deb.numero_nota} — ${fmtBRL(deb.valor_total)}`,
            html);
          resultado.email = { ok: true, para: email };
        } catch (e) { resultado.email = { erro: e.message }; }
      }
    }

    // WhatsApp
    if (canais.includes('whatsapp')) {
      if (!telefone) { resultado.whatsapp = { erro: 'fornecedor sem telefone no cd_fornecedor' }; }
      else {
        const linhasN = notas.length ? '\nNotas: ' + notas.map(n => `${n.numero_nfe} (${fmtD(n.data_nota)} ${fmtBRL(n.valor_total)})`).join('; ') : '';
        const msg = `*Cobrança JR Lira*\n${deb.numero_nota}\nLoja: ${deb.loja_nome||'—'}\n${deb.natureza_operacao||''}\nItens: ${itens.length}\nTotal: *${fmtBRL(deb.valor_total)}*${linhasN}`;
        try {
          const { enviarWhatsapp } = require('../whatsapp');
          await enviarWhatsapp(telefone, msg);
          resultado.whatsapp = { ok: true, para: telefone };
        } catch (e) { resultado.whatsapp = { erro: e.message }; }
      }
    }

    res.json(resultado);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// DELETE /api/cr/debitos/:id (admin)
router.delete('/debitos/:id', apenasAdmin, async (req, res) => {
  try {
    await pool.query('DELETE FROM cr_debitos WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/cr/creditos/bonificacao — importa XML de bonificação e vincula ao débito
router.post('/creditos/bonificacao', compradorOuAdmin, upload.single('xml'), async (req, res) => {
  const client = await pool.connect();
  try {
    if (!req.file) return res.status(400).json({ erro: 'Arquivo XML obrigatório' });
    const debito_id = parseInt(req.body.debito_id) || null;
    if (!debito_id) return res.status(400).json({ erro: 'debito_id obrigatório' });

    const nfe = await parseNFeSimples(req.file.buffer);

    const { rows: [deb] } = await client.query('SELECT * FROM cr_debitos WHERE id=$1', [debito_id]);
    if (!deb) return res.status(404).json({ erro: 'Débito não encontrado' });

    await client.query('BEGIN');
    const ins = await client.query(
      `INSERT INTO cr_creditos
         (debito_id, fornecedor_id, fornecedor_cnpj, loja_id, tipo,
          numero_nota, chave_nfe, data_credito, valor, registrado_por)
       VALUES ($1,$2,$3,$4,'bonificacao',$5,$6,$7,$8,$9)
       ON CONFLICT (chave_nfe) DO NOTHING
       RETURNING id`,
      [debito_id, deb.fornecedor_id, nfe.emit_cnpj, deb.loja_id,
       nfe.numero_nota, nfe.chave_nfe, nfe.data_emissao,
       nfe.valor_total, req.usuario.email || req.usuario.usuario]
    );
    if (!ins.rows.length) {
      await client.query('ROLLBACK');
      return res.status(409).json({ erro: 'Nota já registrada' });
    }
    await atualizarSaldo(client, debito_id);
    await client.query('COMMIT');
    res.json({ ok: true });
  } catch (e) {
    await client.query('ROLLBACK');
    res.status(500).json({ erro: e.message });
  } finally { client.release(); }
});

// POST /api/cr/creditos/desconto — lançamento de desconto em boleto
router.post('/creditos/desconto', compradorOuAdmin, async (req, res) => {
  const client = await pool.connect();
  try {
    const { debito_id, nr_nf_boleto, valor_nf_boleto, valor_boleto, valor_desconto, data_credito, observacoes } = req.body;
    if (!debito_id || !valor_desconto)
      return res.status(400).json({ erro: 'debito_id e valor_desconto obrigatórios' });

    const { rows: [deb] } = await client.query('SELECT * FROM cr_debitos WHERE id=$1', [debito_id]);
    if (!deb) return res.status(404).json({ erro: 'Débito não encontrado' });

    await client.query('BEGIN');
    await client.query(
      `INSERT INTO cr_creditos
         (debito_id, fornecedor_id, fornecedor_cnpj, loja_id, tipo,
          nr_nf_boleto, valor_nf_boleto, valor_boleto, valor_desconto,
          data_credito, valor, observacoes, registrado_por)
       VALUES ($1,$2,$3,$4,'desconto_boleto',$5,$6,$7,$8,$9,$10,$11,$12)`,
      [debito_id, deb.fornecedor_id, deb.fornecedor_cnpj, deb.loja_id,
       nr_nf_boleto || null, n(valor_nf_boleto), n(valor_boleto), n(valor_desconto),
       data_credito || null, n(valor_desconto),
       observacoes || null, req.usuario.email || req.usuario.usuario]
    );
    await atualizarSaldo(client, debito_id);
    await client.query('COMMIT');
    res.json({ ok: true });
  } catch (e) {
    await client.query('ROLLBACK');
    res.status(500).json({ erro: e.message });
  } finally { client.release(); }
});

// DELETE /api/cr/creditos/:id
router.delete('/creditos/:id', compradorOuAdmin, async (req, res) => {
  const client = await pool.connect();
  try {
    const { rows: [cred] } = await client.query('SELECT debito_id FROM cr_creditos WHERE id=$1', [req.params.id]);
    if (!cred) return res.status(404).json({ erro: 'Não encontrado' });
    await client.query('BEGIN');
    await client.query('DELETE FROM cr_creditos WHERE id=$1', [req.params.id]);
    if (cred.debito_id) await atualizarSaldo(client, cred.debito_id);
    await client.query('COMMIT');
    res.json({ ok: true });
  } catch (e) {
    await client.query('ROLLBACK');
    res.status(500).json({ erro: e.message });
  } finally { client.release(); }
});

module.exports = router;
