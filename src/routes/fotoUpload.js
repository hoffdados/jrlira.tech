const express = require('express');
const router = express.Router();
const crypto = require('crypto');
const multer = require('multer');
const { pool, query: dbQuery } = require('../db');
const { autenticar } = require('../auth');
const { enviarEmail } = require('../mailer');

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 }
});

// POST /api/foto-upload/gerar/:funcId  (RH/admin)
router.post('/gerar/:funcId', autenticar, async (req, res) => {
  try {
    const rows = await dbQuery('SELECT id, nome, email FROM funcionarios WHERE id = $1', [req.params.funcId]);
    if (!rows.length) return res.status(404).json({ erro: 'Funcionário não encontrado' });
    const func = rows[0];
    if (!func.email) return res.status(400).json({ erro: 'Funcionário sem e-mail cadastrado' });

    await dbQuery('DELETE FROM foto_tokens WHERE funcionario_id = $1', [func.id]);
    const token = crypto.randomBytes(32).toString('hex');
    await dbQuery(
      "INSERT INTO foto_tokens (token, funcionario_id, expira_em) VALUES ($1, $2, NOW() + INTERVAL '72 hours')",
      [token, func.id]
    );

    const link = `${process.env.APP_URL || 'https://jrlira.tech'}/foto-upload?token=${token}`;
    await enviarEmail(func.email, 'Envio de foto — JR Lira Tech', templateFotoLink({ nome: func.nome, link }));
    res.json({ ok: true });
  } catch (err) {
    console.error('[foto-upload]', err.message);
    res.status(500).json({ erro: 'Erro ao processar requisição' });
  }
});

// GET /api/foto-upload/:token  (público)
router.get('/:token', async (req, res) => {
  try {
    const rows = await dbQuery(
      `SELECT ft.usado, ft.expira_em, f.nome
       FROM foto_tokens ft
       JOIN funcionarios f ON f.id = ft.funcionario_id
       WHERE ft.token = $1`,
      [req.params.token]
    );
    if (!rows.length) return res.status(404).json({ erro: 'Link inválido' });
    const t = rows[0];
    if (t.usado) return res.status(400).json({ erro: 'Este link já foi utilizado' });
    if (new Date(t.expira_em) < new Date()) return res.status(400).json({ erro: 'Link expirado' });
    res.json({ valido: true, nome: t.nome });
  } catch (err) {
    console.error('[foto-upload]', err.message);
    res.status(500).json({ erro: 'Erro ao processar requisição' });
  }
});

// POST /api/foto-upload/:token  (público)
router.post('/:token', upload.single('foto'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ erro: 'Foto obrigatória' });
    const rows = await dbQuery(
      'SELECT funcionario_id, usado, expira_em FROM foto_tokens WHERE token = $1',
      [req.params.token]
    );
    if (!rows.length) return res.status(404).json({ erro: 'Link inválido' });
    const t = rows[0];
    if (t.usado) return res.status(400).json({ erro: 'Este link já foi utilizado' });
    if (new Date(t.expira_em) < new Date()) return res.status(400).json({ erro: 'Link expirado' });

    await dbQuery(
      'UPDATE funcionarios SET foto_data = $1, foto_mime = $2, foto_path = NULL, atualizado_em = NOW() WHERE id = $3',
      [req.file.buffer, req.file.mimetype, t.funcionario_id]
    );
    await dbQuery('UPDATE foto_tokens SET usado = TRUE WHERE token = $1', [req.params.token]);
    res.json({ ok: true });
  } catch (err) {
    console.error('[foto-upload]', err.message);
    res.status(500).json({ erro: 'Erro ao processar requisição' });
  }
});

function templateFotoLink({ nome, link }) {
  return `
<!DOCTYPE html>
<html lang="pt-BR">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f4;padding:32px 0">
    <tr><td align="center">
      <table width="520" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.08)">
        <tr><td style="background:#0ea5e9;padding:28px 32px;text-align:center">
          <h1 style="color:#fff;margin:0;font-size:22px;font-weight:700">JR Lira Tech</h1>
          <p style="color:rgba(255,255,255,.85);margin:6px 0 0;font-size:14px">Envio de foto para cadastro</p>
        </td></tr>
        <tr><td style="padding:32px">
          <p style="color:#333;font-size:15px;margin:0 0 20px">Olá, <strong>${nome}</strong>!</p>
          <p style="color:#555;font-size:14px;margin:0 0 28px;line-height:1.6">O RH da JR Lira solicitou o envio de sua foto para atualização do seu cadastro. Clique no botão abaixo, tire uma selfie e confirme o envio. O link expira em <strong>72 horas</strong>.</p>
          <a href="${link}" style="display:block;background:#0ea5e9;color:#fff;text-align:center;padding:16px;border-radius:8px;text-decoration:none;font-size:16px;font-weight:700">Enviar minha foto</a>
          <p style="color:#aaa;font-size:11px;margin:20px 0 0;text-align:center">Se não reconhece esta solicitação, ignore este e-mail.</p>
        </td></tr>
        <tr><td style="background:#fafafa;padding:16px 32px;text-align:center;border-top:1px solid #eee">
          <p style="color:#bbb;font-size:11px;margin:0">© JR Lira Tech — Sistema de Gestão</p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>`;
}

module.exports = router;
