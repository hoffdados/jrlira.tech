const jwt = require('jsonwebtoken');
const JWT_SECRET = process.env.JWT_SECRET || 'jrlira-secret-2026';

function autenticar(req, res, next) {
  const token = req.headers.authorization?.replace('Bearer ', '');
  if (!token) return res.status(401).json({ erro: 'Token não fornecido' });
  try {
    req.usuario = jwt.verify(token, JWT_SECRET);
    next();
  } catch {
    res.status(401).json({ erro: 'Token inválido' });
  }
}

function apenasAdmin(req, res, next) {
  const token = req.headers.authorization?.replace('Bearer ', '');
  if (!token) return res.status(401).json({ erro: 'Token não fornecido' });
  try {
    req.usuario = jwt.verify(token, JWT_SECRET);
    if (req.usuario.perfil !== 'admin') return res.status(403).json({ erro: 'Acesso negado' });
    next();
  } catch {
    res.status(401).json({ erro: 'Token inválido' });
  }
}

module.exports = { autenticar, apenasAdmin, JWT_SECRET };
