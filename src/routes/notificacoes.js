const express = require('express');
const jwt = require('jsonwebtoken');
const { query } = require('../db');
const { JWT_SECRET } = require('../auth');

const router = express.Router();

// Aceita JWT de rh_usuarios OU de vendedor (token assinado com JWT_SECRET+'_vendedor')
function autUsuarioOuVendedor(req, res, next) {
  const t = req.headers.authorization?.replace('Bearer ', '');
  if (!t) return res.status(401).json({ erro: 'Não autenticado' });
  try { req.destino = { tipo: 'usuario', id: jwt.verify(t, JWT_SECRET).id }; return next(); } catch {}
  try { req.destino = { tipo: 'vendedor', id: jwt.verify(t, JWT_SECRET + '_vendedor').id }; return next(); } catch {}
  res.status(401).json({ erro: 'Token inválido' });
}

// GET /api/notificacoes — lista as últimas 30, com flag "lida"
router.get('/', autUsuarioOuVendedor, async (req, res) => {
  try {
    const { tipo, id } = req.destino;
    const rows = await query(
      `SELECT id, tipo, titulo, corpo, url, lida_em, criado_em
         FROM notificacoes
        WHERE destinatario_tipo = $1 AND destinatario_id = $2
        ORDER BY criado_em DESC
        LIMIT 30`,
      [tipo, id]
    );
    const naoLidas = rows.filter(r => !r.lida_em).length;
    res.json({ notificacoes: rows, nao_lidas: naoLidas });
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// POST /api/notificacoes/:id/lida
router.post('/:id/lida', autUsuarioOuVendedor, async (req, res) => {
  try {
    const { tipo, id: destId } = req.destino;
    await query(
      `UPDATE notificacoes SET lida_em = NOW()
        WHERE id = $1 AND destinatario_tipo = $2 AND destinatario_id = $3 AND lida_em IS NULL`,
      [req.params.id, tipo, destId]
    );
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// POST /api/notificacoes/marcar-todas-lidas
router.post('/marcar-todas-lidas', autUsuarioOuVendedor, async (req, res) => {
  try {
    const { tipo, id: destId } = req.destino;
    await query(
      `UPDATE notificacoes SET lida_em = NOW()
        WHERE destinatario_tipo = $1 AND destinatario_id = $2 AND lida_em IS NULL`,
      [tipo, destId]
    );
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// Helper interno pra outros módulos criarem notificação:
// criarNotificacao({ tipo: 'usuario'|'vendedor', id, kind, titulo, corpo?, url? })
async function criarNotificacao({ destinatario_tipo, destinatario_id, tipo, titulo, corpo, url }) {
  if (!destinatario_id) return null;
  try {
    const r = await query(
      `INSERT INTO notificacoes (destinatario_tipo, destinatario_id, tipo, titulo, corpo, url)
       VALUES ($1,$2,$3,$4,$5,$6) RETURNING id`,
      [destinatario_tipo, destinatario_id, tipo, titulo, corpo || null, url || null]
    );
    return r[0].id;
  } catch (err) {
    console.error('[notificacoes] criar falhou:', err.message);
    return null;
  }
}

module.exports = router;
module.exports.criarNotificacao = criarNotificacao;
