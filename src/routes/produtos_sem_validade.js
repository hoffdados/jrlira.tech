// Cadastro de EANs isentos da regra de validade negociada (papel, EPI, limpeza, etc).
// Vendedor não precisa preencher validade pra esses, e o detector de divergência
// comercial ignora lotes desses EANs.
const express = require('express');
const router = express.Router();
const { query } = require('../db');
const { autenticar } = require('../auth');

function podeAdmin(req) {
  return ['admin', 'ceo', 'comprador'].includes(req.usuario.perfil);
}

router.get('/', autenticar, async (req, res) => {
  try {
    const rows = await query(`SELECT * FROM produtos_sem_validade ORDER BY marcado_em DESC`);
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

router.post('/', autenticar, async (req, res) => {
  if (!podeAdmin(req)) return res.status(403).json({ erro: 'Apenas admin/ceo/comprador' });
  try {
    const { codigo_barras, descricao, motivo } = req.body || {};
    if (!codigo_barras) return res.status(400).json({ erro: 'codigo_barras obrigatório' });
    await query(
      `INSERT INTO produtos_sem_validade (codigo_barras, descricao, motivo, marcado_por)
       VALUES ($1,$2,$3,$4)
       ON CONFLICT (codigo_barras) DO UPDATE
         SET descricao=EXCLUDED.descricao, motivo=EXCLUDED.motivo,
             marcado_por=EXCLUDED.marcado_por, marcado_em=NOW()`,
      [String(codigo_barras).trim(), descricao || null, motivo || null, req.usuario.nome]
    );
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

router.delete('/:codigo_barras', autenticar, async (req, res) => {
  if (!podeAdmin(req)) return res.status(403).json({ erro: 'Apenas admin/ceo/comprador' });
  try {
    await query('DELETE FROM produtos_sem_validade WHERE codigo_barras=$1', [req.params.codigo_barras]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

module.exports = router;
