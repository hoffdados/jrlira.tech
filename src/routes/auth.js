const express = require('express');
const router = express.Router();
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const { pool, query: dbQuery } = require('../db');
const { JWT_SECRET } = require('../auth');
const { enviarEmail, templateCredenciais } = require('../mailer');

function buildLojas(u) {
  if (u.lojas_ids?.length) return u.lojas_ids.map(Number);
  if (u.loja_id) return [Number(u.loja_id)];
  return null; // admin / sem restrição
}

router.post('/login', async (req, res) => {
  try {
    const { usuario, senha } = req.body;
    const rows = await dbQuery('SELECT * FROM rh_usuarios WHERE usuario = $1 AND ativo = TRUE', [usuario]);
    if (!rows.length) return res.status(401).json({ erro: 'Usuário não encontrado' });
    const u = rows[0];
    const ok = await bcrypt.compare(senha, u.senha_hash);
    if (!ok) return res.status(401).json({ erro: 'Senha incorreta' });
    const lojas = buildLojas(u);
    const token = jwt.sign({ id: u.id, usuario: u.usuario, nome: u.nome, perfil: u.perfil, lojas }, JWT_SECRET, { expiresIn: '12h' });
    res.json({ token, nome: u.nome, perfil: u.perfil, lojas });
  } catch (err) {
    res.status(500).json({ erro: err.message });
  }
});

// GET /api/auth/usuarios (admin)
router.get('/usuarios', async (req, res) => {
  const token = req.headers.authorization?.replace('Bearer ', '');
  if (!token) return res.status(401).json({ erro: 'Não autenticado' });
  try {
    const u = jwt.verify(token, JWT_SECRET);
    if (u.perfil !== 'admin') return res.status(403).json({ erro: 'Acesso negado' });
    const rows = await dbQuery('SELECT id, usuario, nome, email, perfil, loja_id, lojas_ids, ativo, criado_em FROM rh_usuarios ORDER BY nome');
    res.json(rows);
  } catch (err) { res.status(401).json({ erro: 'Token inválido' }); }
});

// POST /api/auth/usuarios (admin)
router.post('/usuarios', async (req, res) => {
  const token = req.headers.authorization?.replace('Bearer ', '');
  if (!token) return res.status(401).json({ erro: 'Não autenticado' });
  try {
    const u = jwt.verify(token, JWT_SECRET);
    if (u.perfil !== 'admin') return res.status(403).json({ erro: 'Acesso negado' });

    const { usuario, nome, email, senha, perfil, lojas_ids } = req.body;
    const perfisValidos = ['admin', 'rh', 'cadastro', 'estoque', 'auditor', 'comprador'];
    if (!usuario || !nome || !senha || !perfisValidos.includes(perfil))
      return res.status(400).json({ erro: 'Dados inválidos' });

    const ids = Array.isArray(lojas_ids) ? lojas_ids.map(Number).filter(Boolean) : [];
    const loja_id = ids.length === 1 ? ids[0] : null;
    const lojas_arr = ids.length > 1 ? ids : null;

    const hash = await bcrypt.hash(senha, 10);
    const rows = await dbQuery(
      'INSERT INTO rh_usuarios (usuario, nome, email, senha_hash, perfil, loja_id, lojas_ids) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id, usuario, nome, email, perfil, loja_id, lojas_ids',
      [usuario.trim().toLowerCase(), nome.trim(), email?.trim() || null, hash, perfil, loja_id, lojas_arr]
    );

    if (email?.trim()) {
      enviarEmail(email.trim(), 'Suas credenciais de acesso — JR Lira Tech',
        templateCredenciais({ nome: nome.trim(), usuario: usuario.trim().toLowerCase(), senha, perfil })
      ).catch(e => console.error('[mailer] erro ao enviar credenciais:', e.message));
    }

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
    const u = jwt.verify(token, JWT_SECRET);
    if (u.perfil !== 'admin') return res.status(403).json({ erro: 'Acesso negado' });

    const { perfil, ativo, senha, email, lojas_ids } = req.body;
    const perfisValidos = ['admin', 'rh', 'cadastro', 'estoque', 'auditor', 'comprador'];

    if (perfil !== undefined) {
      if (!perfisValidos.includes(perfil)) return res.status(400).json({ erro: 'Perfil inválido' });
      await dbQuery('UPDATE rh_usuarios SET perfil=$1 WHERE id=$2', [perfil, req.params.id]);
    }
    if (ativo !== undefined) {
      await dbQuery('UPDATE rh_usuarios SET ativo=$1 WHERE id=$2', [ativo, req.params.id]);
    }
    if (email !== undefined) {
      await dbQuery('UPDATE rh_usuarios SET email=$1 WHERE id=$2', [email?.trim() || null, req.params.id]);
    }
    if (senha) {
      const hash = await bcrypt.hash(senha, 10);
      await dbQuery('UPDATE rh_usuarios SET senha_hash=$1 WHERE id=$2', [hash, req.params.id]);
    }
    if (lojas_ids !== undefined) {
      const ids = Array.isArray(lojas_ids) ? lojas_ids.map(Number).filter(Boolean) : [];
      const loja_id = ids.length === 1 ? ids[0] : null;
      const lojas_arr = ids.length > 1 ? ids : null;
      await dbQuery('UPDATE rh_usuarios SET loja_id=$1, lojas_ids=$2 WHERE id=$3', [loja_id, lojas_arr, req.params.id]);
    }
    res.json({ ok: true });
  } catch (err) {
    res.status(err.name === 'JsonWebTokenError' ? 401 : 500).json({ erro: err.message });
  }
});

// DELETE /api/auth/usuarios/:id (admin)
router.delete('/usuarios/:id', async (req, res) => {
  const token = req.headers.authorization?.replace('Bearer ', '');
  if (!token) return res.status(401).json({ erro: 'Não autenticado' });
  try {
    const u = jwt.verify(token, JWT_SECRET);
    if (u.perfil !== 'admin') return res.status(403).json({ erro: 'Acesso negado' });
    if (u.id == req.params.id) return res.status(400).json({ erro: 'Não é possível excluir o próprio usuário' });
    await dbQuery('DELETE FROM rh_usuarios WHERE id = $1', [req.params.id]);
    res.json({ ok: true });
  } catch (err) {
    res.status(err.name === 'JsonWebTokenError' ? 401 : 500).json({ erro: err.message });
  }
});

// GET /api/auth/db-status (admin)
router.get('/db-status', async (req, res) => {
  const token = req.headers.authorization?.replace('Bearer ', '');
  if (!token) return res.status(401).json({ erro: 'Não autenticado' });
  try {
    const u = jwt.verify(token, JWT_SECRET);
    if (u.perfil !== 'admin') return res.status(403).json({ erro: 'Acesso negado' });

    const [tamanho, tabelas] = await Promise.all([
      pool.query(`SELECT pg_size_pretty(pg_database_size(current_database())) AS tamanho_total,
                         pg_database_size(current_database()) AS bytes_total`),
      pool.query(`SELECT relname AS tabela,
                         pg_size_pretty(pg_total_relation_size(relid)) AS tamanho,
                         pg_total_relation_size(relid) AS bytes,
                         reltuples::bigint AS registros
                  FROM pg_catalog.pg_statio_user_tables
                  ORDER BY pg_total_relation_size(relid) DESC
                  LIMIT 10`)
    ]);

    res.json({
      tamanho_total: tamanho[0].tamanho_total,
      bytes_total: Number(tamanho[0].bytes_total),
      tabelas: tabelas.map(t => ({
        tabela: t.tabela,
        tamanho: t.tamanho,
        registros: Number(t.registros)
      }))
    });
  } catch (err) {
    res.status(err.name === 'JsonWebTokenError' ? 401 : 500).json({ erro: err.message });
  }
});

module.exports = router;
