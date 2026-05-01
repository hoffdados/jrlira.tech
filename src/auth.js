const jwt = require('jsonwebtoken');
const JWT_SECRET = process.env.JWT_SECRET;
if (!JWT_SECRET || JWT_SECRET.length < 32) {
  throw new Error('JWT_SECRET ausente ou fraco (defina uma string de pelo menos 32 caracteres no ambiente)');
}

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

function exigirPerfil(...perfis) {
  return (req, res, next) => {
    if (!req.usuario) return res.status(401).json({ erro: 'Não autenticado' });
    if (!perfis.includes(req.usuario.perfil)) return res.status(403).json({ erro: 'Acesso restrito' });
    next();
  };
}

const apenasAdmin = [autenticar, exigirPerfil('admin')];
const compradorOuAdmin = [autenticar, exigirPerfil('admin', 'comprador')];

module.exports = { autenticar, exigirPerfil, apenasAdmin, compradorOuAdmin, JWT_SECRET };
