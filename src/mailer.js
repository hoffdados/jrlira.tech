// Throttle global: Resend limita 5 req/s. Serializa todos envios com gap mínimo de 250ms (4/s, margem segura).
const RESEND_MIN_GAP_MS = 250;
let _ultimoEnvioMs = 0;
let _fila = Promise.resolve();

async function _enviarAgora(destinatario, assunto, html, anexos) {
  const apiKey = process.env.RESEND_API_KEY;
  if (!apiKey) { console.warn('[mailer] RESEND_API_KEY ausente — email não enviado'); return; }
  const from = process.env.MAIL_FROM || 'notificacoes@jrlira.tech';
  const body = { from: `JR Lira Tech <${from}>`, to: destinatario, subject: assunto, html };
  if (anexos.length) {
    body.attachments = anexos.map(a => ({
      filename: a.filename,
      content: Buffer.isBuffer(a.content) ? a.content.toString('base64') : a.content,
    }));
  }
  const res = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.text();
    console.error('[mailer] Resend erro:', err);
    if (res.status === 429) {
      // backoff extra de 1s pra escapar do rate limit
      await new Promise(r => setTimeout(r, 1000));
    }
  }
}

function enviarEmail(destinatario, assunto, html, anexos = []) {
  _fila = _fila.then(async () => {
    const agora = Date.now();
    const espera = Math.max(0, _ultimoEnvioMs + RESEND_MIN_GAP_MS - agora);
    if (espera > 0) await new Promise(r => setTimeout(r, espera));
    _ultimoEnvioMs = Date.now();
    try { await _enviarAgora(destinatario, assunto, html, anexos); }
    catch (e) { console.error('[mailer] excecao:', e.message); }
  });
  return _fila;
}

function templateCredenciais({ nome, usuario, senha, perfil }) {
  const link = process.env.APP_URL || 'https://jrlira.tech';
  const PERFIL_LABEL = { admin: 'Administrador', rh: 'RH', cadastro: 'Cadastro', estoque: 'Estoque', auditor: 'Auditor' };
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
          <p style="color:rgba(255,255,255,.85);margin:6px 0 0;font-size:14px">Credenciais de acesso — ${PERFIL_LABEL[perfil] || perfil}</p>
        </td></tr>
        <tr><td style="padding:32px">
          <p style="color:#333;font-size:15px;margin:0 0 24px">Olá, <strong>${nome}</strong>! Seus dados de acesso ao sistema:</p>
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#f8f8f8;border-radius:8px;padding:20px;margin-bottom:24px">
            <tr><td style="padding:6px 0">
              <span style="color:#888;font-size:12px;text-transform:uppercase;letter-spacing:.5px">Usuário</span><br>
              <strong style="color:#222;font-size:16px;font-family:monospace">${usuario}</strong>
            </td></tr>
            <tr><td style="padding:10px 0 6px;border-top:1px solid #eee">
              <span style="color:#888;font-size:12px;text-transform:uppercase;letter-spacing:.5px">Senha</span><br>
              <strong style="color:#222;font-size:20px;letter-spacing:2px;font-family:monospace">${senha}</strong>
            </td></tr>
          </table>
          <a href="${link}" style="display:block;background:#0ea5e9;color:#fff;text-align:center;padding:14px;border-radius:8px;text-decoration:none;font-size:15px;font-weight:700">Acessar o sistema</a>
          <p style="color:#aaa;font-size:11px;margin:20px 0 0;text-align:center">Guarde este email com segurança. Não compartilhe sua senha.</p>
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

module.exports = { enviarEmail, templateCredenciais };
