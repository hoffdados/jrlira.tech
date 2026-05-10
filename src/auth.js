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

// Para endpoints consumidos por <img src> ou <a href> — aceita token via header OU query
function autenticarOuQuery(req, res, next) {
  const token = req.headers.authorization?.replace('Bearer ', '') || req.query.token;
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
const adminOuCeo = [autenticar, exigirPerfil('admin', 'ceo')];

module.exports = { autenticar, autenticarOuQuery, exigirPerfil, apenasAdmin, compradorOuAdmin, adminOuCeo, JWT_SECRET };
