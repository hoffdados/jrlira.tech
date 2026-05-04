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
  const { rows: [deb] } = await client.query('SELECT valor_total FROM cr_debitos WHERE id=$1', [debito_id]);
  const status = parseFloat(soma) >= parseFloat(deb.valor_total) ? 'baixado'
               : parseFloat(soma) > 0 ? 'parcial' : 'aberto';
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
    res.json({ ...deb, itens, creditos });
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
