const express = require('express');
const router = express.Router();
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const pool = require('../db');
const { JWT_SECRET } = require('../auth');

router.post('/login', async (req, res) => {
  try {
    const { usuario, senha } = req.body;
    const rows = await pool.query('SELECT * FROM rh_usuarios WHERE usuario = $1 AND ativo = TRUE', [usuario]);
    if (!rows.length) return res.status(401).json({ erro: 'Usuário não encontrado' });
    const u = rows[0];
    const ok = await bcrypt.compare(senha, u.senha_hash);
    if (!ok) return res.status(401).json({ erro: 'Senha incorreta' });
    const token = jwt.sign({ id: u.id, usuario: u.usuario, nome: u.nome, perfil: u.perfil }, JWT_SECRET, { expiresIn: '12h' });
    res.json({ token, nome: u.nome, perfil: u.perfil });
  } catch (err) {
    res.status(500).json({ erro: err.message });
  }
});

module.exports = router;
