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
    const token = jwt.sign({ id: u.id, usuario: u.usuario, nome: u.nome, perfil: u.perfil, loja_id: u.loja_id || null }, JWT_SECRET, { expiresIn: '12h' });
    res.json({ token, nome: u.nome, perfil: u.perfil, loja_id: u.loja_id || null });
  } catch (err) {
    res.status(500).json({ erro: err.message });
  }
});


// GET /api/auth/usuarios (admin)
router.get('/usuarios', async (req, res) => {
  const token = req.headers.authorization?.replace('Bearer ', '');
  if (!token) return res.status(401).json({ erro: 'Não autenticado' });
  try {
    const { JWT_SECRET } = require('../auth');
    const jwt = require('jsonwebtoken');
    const u = jwt.verify(token, JWT_SECRET);
    if (u.perfil !== 'admin') return res.status(403).json({ erro: 'Acesso negado' });
    const rows = await pool.query('SELECT id, usuario, nome, perfil, loja_id, ativo, criado_em FROM rh_usuarios ORDER BY nome');
    res.json(rows);
  } catch (err) { res.status(401).json({ erro: 'Token inválido' }); }
});

// POST /api/auth/usuarios (admin)
router.post('/usuarios', async (req, res) => {
  const token = req.headers.authorization?.replace('Bearer ', '');
  if (!token) return res.status(401).json({ erro: 'Não autenticado' });
  try {
    const { JWT_SECRET } = require('../auth');
    const jwt = require('jsonwebtoken');
    const u = jwt.verify(token, JWT_SECRET);
    if (u.perfil !== 'admin') return res.status(403).json({ erro: 'Acesso negado' });

    const { usuario, nome, senha, perfil, loja_id } = req.body;
    const perfisValidos = ['admin', 'rh', 'cadastro', 'estoque', 'auditor'];
    if (!usuario || !nome || !senha || !perfisValidos.includes(perfil))
      return res.status(400).json({ erro: 'Dados inválidos' });

    const bcrypt = require('bcryptjs');
    const hash = await bcrypt.hash(senha, 10);
    const rows = await pool.query(
      'INSERT INTO rh_usuarios (usuario, nome, senha_hash, perfil, loja_id) VALUES ($1,$2,$3,$4,$5) RETURNING id, usuario, nome, perfil, loja_id',
      [usuario.trim().toLowerCase(), nome.trim(), hash, perfil, loja_id || null]
    );
    res.json(rows[0]);
  } catch (err) {
    if (err.code === '23505') return res.status(409).json({ erro: 'Usuário já existe' });
    res.status(err.name === 'JsonWebTokenError' ? 401 : 500).json({ erro: err.message });
  }
});

// PATCH /api/auth/usuarios/:id (admin)
router.patch('/usuarios/:id', async (req, res) => {
  const token = req.headers.authorization?.replace('Bearer ', '');
  if (!token) return res.status(401).json({ erro: 'Não autenticado' });
  try {
    const { JWT_SECRET } = require('../auth');
    const jwt = require('jsonwebtoken');
    const u = jwt.verify(token, JWT_SECRET);
    if (u.perfil !== 'admin') return res.status(403).json({ erro: 'Acesso negado' });

    const { perfil, ativo, senha, loja_id } = req.body;
    const perfisValidos = ['admin', 'rh', 'cadastro', 'estoque', 'auditor'];

    if (perfil !== undefined) {
      if (!perfisValidos.includes(perfil)) return res.status(400).json({ erro: 'Perfil inválido' });
      await pool.query('UPDATE rh_usuarios SET perfil=$1 WHERE id=$2', [perfil, req.params.id]);
    }
    if (ativo !== undefined) {
      await pool.query('UPDATE rh_usuarios SET ativo=$1 WHERE id=$2', [ativo, req.params.id]);
    }
    if (senha) {
      const bcrypt = require('bcryptjs');
      const hash = await bcrypt.hash(senha, 10);
      await pool.query('UPDATE rh_usuarios SET senha_hash=$1 WHERE id=$2', [hash, req.params.id]);
    }
    if (loja_id !== undefined) {
      await pool.query('UPDATE rh_usuarios SET loja_id=$1 WHERE id=$2', [loja_id || null, req.params.id]);
    }
    res.json({ ok: true });
  } catch (err) {
    res.status(err.name === 'JsonWebTokenError' ? 401 : 500).json({ erro: err.message });
  }
});

module.exports = router;
