const express = require('express');
const router = express.Router();
const crypto = require('crypto');
const bcrypt = require('bcryptjs');
const multer = require('multer');
const pool = require('../db');
const { autenticar, apenasAdmin, compradorOuAdmin } = require('../auth');
const { enviarEmail } = require('../mailer');

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 5 * 1024 * 1024 } });

// ── LOJAS ─────────────────────────────────────────────────────────

router.get('/lojas', autenticar, async (req, res) => {
  try {
    const rows = await pool.query('SELECT id, nome, cnpj, ativo FROM lojas ORDER BY id');
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

router.put('/lojas/:id', apenasAdmin, async (req, res) => {
  try {
    const { cnpj } = req.body;
    await pool.query('UPDATE lojas SET cnpj = $1 WHERE id = $2', [cnpj || null, req.params.id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// ── FORNECEDORES ──────────────────────────────────────────────────

router.get('/', autenticar, async (req, res) => {
  try {
    const { q } = req.query;
    const params = q ? [`%${q}%`] : [];
    const where = q ? `WHERE razao_social ILIKE $1 OR fantasia ILIKE $1 OR cnpj ILIKE $1` : '';
    const rows = await pool.query(
      `SELECT id, razao_social, fantasia, cnpj, ativo FROM (
         SELECT DISTINCT ON (cnpj) id, razao_social, fantasia, cnpj, ativo
         FROM fornecedores ${where} ORDER BY cnpj, razao_social, id
       ) sub ORDER BY razao_social`,
      params
    );
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

router.post('/', apenasAdmin, upload.single('foto'), async (req, res) => {
  try {
    const { razao_social, fantasia, cnpj } = req.body;
    if (!razao_social) return res.status(400).json({ erro: 'razao_social obrigatória' });
    const rows = await pool.query(
      `INSERT INTO fornecedores (razao_social, fantasia, cnpj, foto_data, foto_mime)
       VALUES ($1,$2,$3,$4,$5) RETURNING id`,
      [razao_social, fantasia || null, cnpj || null,
       req.file ? req.file.buffer : null, req.file ? req.file.mimetype : null]
    );
    res.json({ ok: true, id: rows[0].id });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

router.put('/:id', apenasAdmin, upload.single('foto'), async (req, res) => {
  try {
    const { razao_social, fantasia, cnpj, ativo } = req.body;
    const fotoClause = req.file ? ', foto_data=$5, foto_mime=$6' : '';
    const params = [razao_social, fantasia || null, cnpj || null, ativo !== 'false', req.params.id];
    if (req.file) params.splice(params.length - 1, 0, req.file.buffer, req.file.mimetype);
    const idPos = req.file ? 7 : 5;
    await pool.query(
      `UPDATE fornecedores SET razao_social=$1, fantasia=$2, cnpj=$3, ativo=$4${fotoClause} WHERE id=$${idPos}`,
      params
    );
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

router.get('/:id/foto', async (req, res) => {
  try {
    const rows = await pool.query('SELECT foto_data, foto_mime FROM fornecedores WHERE id=$1', [req.params.id]);
    if (!rows.length || !rows[0].foto_data) return res.status(404).end();
    res.setHeader('Content-Type', rows[0].foto_mime || 'image/jpeg');
    res.setHeader('Cache-Control', 'public, max-age=86400');
    res.send(rows[0].foto_data);
  } catch { res.status(500).end(); }
});

// ── VENDEDORES ────────────────────────────────────────────────────

router.get('/vendedores/pendentes', compradorOuAdmin, async (req, res) => {
  try {
    const rows = await pool.query(
      `SELECT v.id, v.nome, v.email, v.telefone, v.status, v.criado_em,
              f.id as fornecedor_id, f.fantasia, f.razao_social,
              CASE WHEN v.foto_data IS NOT NULL THEN '/api/fornecedores/vendedor-foto/' || v.id::text ELSE NULL END as foto_url
       FROM vendedores v JOIN fornecedores f ON f.id = v.fornecedor_id
       WHERE v.status = 'pendente'
       ORDER BY v.criado_em ASC`
    );
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

router.get('/:id/vendedores', compradorOuAdmin, async (req, res) => {
  try {
    const rows = await pool.query(
      `SELECT id, nome, cpf, email, telefone, nome_gerente, telefone_gerente,
              status, acesso_expira_em, criado_em,
              CASE WHEN foto_data IS NOT NULL THEN '/api/fornecedores/vendedor-foto/' || id::text ELSE NULL END as foto_url
       FROM vendedores WHERE fornecedor_id=$1 ORDER BY nome`,
      [req.params.id]
    );
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

router.get('/vendedor-foto/:id', async (req, res) => {
  try {
    const rows = await pool.query('SELECT foto_data, foto_mime FROM vendedores WHERE id=$1', [req.params.id]);
    if (!rows.length || !rows[0].foto_data) return res.status(404).end();
    res.setHeader('Content-Type', rows[0].foto_mime || 'image/jpeg');
    res.setHeader('Cache-Control', 'public, max-age=86400');
    res.send(rows[0].foto_data);
  } catch { res.status(500).end(); }
});

// Gerar token de cadastro para vendedor
router.post('/:id/gerar-token', compradorOuAdmin, async (req, res) => {
  try {
    const rows = await pool.query('SELECT id FROM fornecedores WHERE id=$1', [req.params.id]);
    if (!rows.length) return res.status(404).json({ erro: 'Fornecedor não encontrado' });
    const token = crypto.randomBytes(32).toString('hex');
    await pool.query(
      `INSERT INTO vendedores (fornecedor_id, nome, status, token_cadastro)
       VALUES ($1, 'Aguardando cadastro', 'aguardando_cadastro', $2)`,
      [req.params.id, token]
    );
    const link = `${process.env.APP_URL || 'https://jrlira.tech'}/vendedor-cadastro?token=${token}`;
    res.json({ ok: true, link, token });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// Aprovar vendedor
router.post('/vendedores/:vid/aprovar', compradorOuAdmin, async (req, res) => {
  try {
    const rows = await pool.query('SELECT * FROM vendedores WHERE id=$1', [req.params.vid]);
    if (!rows.length) return res.status(404).json({ erro: 'Vendedor não encontrado' });
    const v = rows[0];

    const cfgRows = await pool.query("SELECT valor FROM configuracoes WHERE chave='validade_acesso_vendedor_dias'");
    const dias = parseInt(cfgRows[0]?.valor || '90');

    const chars = 'ABCDEFGHJKMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789';
    const senha = Array.from({ length: 8 }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
    const hash = await bcrypt.hash(senha, 10);

    await pool.query(
      `UPDATE vendedores SET status='aprovado', senha_hash=$1,
       acesso_expira_em=NOW() + ($2 || ' days')::interval WHERE id=$3`,
      [hash, dias, v.id]
    );

    if (v.email) {
      const forn = await pool.query('SELECT fantasia, razao_social FROM fornecedores WHERE id=$1', [v.fornecedor_id]);
      const empresa = forn[0]?.fantasia || forn[0]?.razao_social || '';
      await enviarEmail(v.email, 'Acesso liberado — JR Lira Pedidos', templateAcesso({ nome: v.nome, email: v.email, senha, empresa }));
    }
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// Rejeitar vendedor
router.post('/vendedores/:vid/rejeitar', compradorOuAdmin, async (req, res) => {
  try {
    await pool.query(`UPDATE vendedores SET status='rejeitado' WHERE id=$1`, [req.params.vid]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// Revalidar acesso do vendedor
router.post('/vendedores/:vid/revalidar', compradorOuAdmin, async (req, res) => {
  try {
    const rows = await pool.query('SELECT * FROM vendedores WHERE id=$1', [req.params.vid]);
    if (!rows.length) return res.status(404).json({ erro: 'Vendedor não encontrado' });
    const v = rows[0];

    const cfgRows = await pool.query("SELECT valor FROM configuracoes WHERE chave='validade_acesso_vendedor_dias'");
    const dias = parseInt(cfgRows[0]?.valor || '90');

    const chars = 'ABCDEFGHJKMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789';
    const senha = Array.from({ length: 8 }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
    const hash = await bcrypt.hash(senha, 10);

    await pool.query(
      `UPDATE vendedores SET status='aprovado', senha_hash=$1,
       acesso_expira_em=NOW() + ($2 || ' days')::interval WHERE id=$3`,
      [hash, dias, v.id]
    );

    if (v.email) {
      const forn = await pool.query('SELECT fantasia, razao_social FROM fornecedores WHERE id=$1', [v.fornecedor_id]);
      const empresa = forn[0]?.fantasia || forn[0]?.razao_social || '';
      await enviarEmail(v.email, 'Nova senha — JR Lira Pedidos', templateAcesso({ nome: v.nome, email: v.email, senha, empresa }));
    }
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// Desativar/ativar vendedor
router.patch('/vendedores/:vid/ativo', compradorOuAdmin, async (req, res) => {
  try {
    const { ativo } = req.body;
    const status = ativo ? 'aprovado' : 'inativo';
    await pool.query(`UPDATE vendedores SET status=$1 WHERE id=$2`, [status, req.params.vid]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

function templateAcesso({ nome, email, senha, empresa }) {
  const link = `${process.env.APP_URL || 'https://jrlira.tech'}/vendedor`;
  return `
<!DOCTYPE html><html lang="pt-BR"><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f4;padding:32px 0">
<tr><td align="center">
<table width="520" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.08)">
<tr><td style="background:#0ea5e9;padding:28px 32px;text-align:center">
  <h1 style="color:#fff;margin:0;font-size:22px;font-weight:700">JR Lira Tech</h1>
  <p style="color:rgba(255,255,255,.85);margin:6px 0 0;font-size:14px">Acesso ao Portal de Pedidos</p>
</td></tr>
<tr><td style="padding:32px">
  <p style="color:#333;font-size:15px;margin:0 0 16px">Olá, <strong>${nome}</strong>!</p>
  <p style="color:#555;font-size:14px;margin:0 0 24px">Seu cadastro como vendedor de <strong>${empresa}</strong> foi aprovado. Use os dados abaixo para acessar o portal:</p>
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f8f8f8;border-radius:8px;padding:20px;margin-bottom:24px">
    <tr><td style="padding:6px 0"><span style="color:#888;font-size:12px">E-mail</span><br><strong style="font-size:15px">${email}</strong></td></tr>
    <tr><td style="padding:10px 0 6px;border-top:1px solid #eee"><span style="color:#888;font-size:12px">Senha</span><br><strong style="font-size:20px;letter-spacing:2px;font-family:monospace">${senha}</strong></td></tr>
  </table>
  <a href="${link}" style="display:block;background:#0ea5e9;color:#fff;text-align:center;padding:14px;border-radius:8px;text-decoration:none;font-size:15px;font-weight:700">Acessar o Portal</a>
  <p style="color:#f87171;font-size:12px;margin:16px 0 0;text-align:center">⚠️ Boleto sempre em cota única — é proibido parcelar.</p>
</td></tr>
</table></td></tr></table>
</body></html>`;
}

module.exports = router;
