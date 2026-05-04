// ──────────────────────────────────────────────────────────────────
// UltraSyst Relay — endurecido para produção
// Roda na máquina onde está o Power BI Gateway (mesma rede do SQL).
// SOMENTE SELECT. Auth = Bearer + header secreto opcional. Rate limit.
// ──────────────────────────────────────────────────────────────────
require('dotenv').config();
const express = require('express');
const sql = require('mssql');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT          = parseInt(process.env.RELAY_PORT || '8787');
const TOKEN         = process.env.RELAY_TOKEN || '';
const SECRET_HEADER = process.env.RELAY_SECRET_HEADER || ''; // opcional: defesa em profundidade
const SECRET_VALUE  = process.env.RELAY_SECRET_VALUE  || '';
const SQL_HOST      = process.env.SQL_HOST || '127.0.0.1';
const SQL_PORT      = parseInt(process.env.SQL_PORT || '1433');
const SQL_DB        = process.env.SQL_DB || 'ITAUTUBA';
const SQL_USER      = process.env.SQL_USER || 'ASAB';
const SQL_PASS      = process.env.SQL_PASS || '';
const MAX_ROWS      = parseInt(process.env.MAX_ROWS || '5000');
const RATE_PER_MIN  = parseInt(process.env.RATE_PER_MIN || '120'); // por IP
const ALLOWLIST_IPS = (process.env.ALLOWLIST_IPS || '').split(',').map(s => s.trim()).filter(Boolean);
const LOG_DIR       = process.env.LOG_DIR || path.join(__dirname, 'logs');

if (!TOKEN || TOKEN.length < 32) {
  console.error('[FATAL] RELAY_TOKEN ausente ou < 32 chars. Gere com: [Convert]::ToBase64String((1..32 | %{[byte](Get-Random -Max 256)}))');
  process.exit(1);
}
if (!SQL_PASS) {
  console.error('[FATAL] SQL_PASS ausente.');
  process.exit(1);
}
try { fs.mkdirSync(LOG_DIR, { recursive: true }); } catch {}

const sqlConfig = {
  server: SQL_HOST,
  port: SQL_PORT,
  database: SQL_DB,
  user: SQL_USER,
  password: SQL_PASS,
  options: { encrypt: false, trustServerCertificate: true, enableArithAbort: true },
  pool: { max: 5, min: 0, idleTimeoutMillis: 30000 },
  connectionTimeout: 15000,
  requestTimeout: 60000,
};

let poolPromise = null;
async function getPool() {
  if (!poolPromise) {
    poolPromise = new sql.ConnectionPool(sqlConfig).connect().catch(err => {
      poolPromise = null;
      throw err;
    });
  }
  return poolPromise;
}

// ── Auditoria
function log(line, kind = 'access') {
  const file = path.join(LOG_DIR, `${kind}-${new Date().toISOString().slice(0, 10)}.log`);
  fs.appendFile(file, JSON.stringify({ ts: new Date().toISOString(), ...line }) + '\n', () => {});
}
function clientIp(req) {
  return (req.headers['cf-connecting-ip']
       || req.headers['x-forwarded-for']?.split(',')[0]?.trim()
       || req.socket.remoteAddress
       || '').replace('::ffff:', '');
}

// ── Rate limit em memória (60s sliding window)
const buckets = new Map();
function rateOk(ip) {
  const now = Date.now();
  const arr = buckets.get(ip) || [];
  const recent = arr.filter(t => now - t < 60_000);
  if (recent.length >= RATE_PER_MIN) {
    buckets.set(ip, recent);
    return false;
  }
  recent.push(now);
  buckets.set(ip, recent);
  return true;
}
// limpeza periódica
setInterval(() => {
  const now = Date.now();
  for (const [ip, arr] of buckets) {
    const recent = arr.filter(t => now - t < 60_000);
    if (!recent.length) buckets.delete(ip);
    else buckets.set(ip, recent);
  }
}, 60_000).unref();

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '256kb' }));

// Confia em Cloudflare/Tunnel para o IP real
app.set('trust proxy', true);

// ── Camadas de proteção (ordem importa)
app.use((req, res, next) => {
  const ip = clientIp(req);

  // Allowlist IP opcional (ALLOWLIST_IPS=1.2.3.4,5.6.7.8). Se vazia, não filtra.
  if (ALLOWLIST_IPS.length && !ALLOWLIST_IPS.includes(ip)) {
    log({ ip, path: req.path, motivo: 'ip_bloqueado' }, 'deny');
    return res.status(403).json({ erro: 'IP não autorizado' });
  }

  // Rate limit
  if (!rateOk(ip)) {
    log({ ip, path: req.path, motivo: 'rate_limit' }, 'deny');
    return res.status(429).json({ erro: 'Rate limit excedido' });
  }

  // /health não exige token
  if (req.path === '/health') return next();

  // Bearer
  const auth = req.headers.authorization || '';
  const presented = auth.startsWith('Bearer ') ? auth.slice(7) : '';
  const ok = presented.length === TOKEN.length &&
             crypto.timingSafeEqual(Buffer.from(presented.padEnd(TOKEN.length)), Buffer.from(TOKEN));
  if (!ok) {
    log({ ip, path: req.path, motivo: 'token_invalido' }, 'deny');
    return res.status(401).json({ erro: 'Token inválido' });
  }

  // Header secreto opcional (defesa em profundidade)
  if (SECRET_HEADER && SECRET_VALUE) {
    const v = req.headers[SECRET_HEADER.toLowerCase()] || '';
    if (v !== SECRET_VALUE) {
      log({ ip, path: req.path, motivo: 'header_secreto_invalido' }, 'deny');
      return res.status(401).json({ erro: 'Header secreto inválido' });
    }
  }

  next();
});

// ── Health (sem auth, mas com rate limit)
app.get('/health', async (_req, res) => {
  try {
    const pool = await getPool();
    const r = await pool.request().query('SELECT 1 AS ok');
    res.json({ ok: true, sql: r.recordset[0]?.ok === 1, ts: new Date().toISOString() });
  } catch (e) {
    res.status(503).json({ ok: false, erro: e.message });
  }
});

// ── Lista tabelas
app.get('/tables', async (req, res) => {
  try {
    const filtro = (req.query.q || '').toString().toLowerCase();
    const pool = await getPool();
    const r = await pool.request().query(`
      SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
       WHERE TABLE_TYPE = 'BASE TABLE'
       ORDER BY TABLE_NAME
    `);
    let rows = r.recordset;
    if (filtro) rows = rows.filter(x => x.TABLE_NAME.toLowerCase().includes(filtro));
    log({ ip: clientIp(req), path: '/tables', filtro, total: rows.length });
    res.json({ total: rows.length, tabelas: rows });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// ── Colunas
app.get('/columns/:tabela', async (req, res) => {
  try {
    if (!/^[A-Z0-9_]{1,128}$/i.test(req.params.tabela))
      return res.status(400).json({ erro: 'Nome de tabela inválido' });
    const pool = await getPool();
    const r = await pool.request()
      .input('t', sql.VarChar, req.params.tabela)
      .query(`
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
          FROM INFORMATION_SCHEMA.COLUMNS
         WHERE TABLE_NAME = @t
         ORDER BY ORDINAL_POSITION
      `);
    log({ ip: clientIp(req), path: '/columns', tabela: req.params.tabela });
    res.json({ tabela: req.params.tabela, colunas: r.recordset });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// ── Query genérica — SOMENTE SELECT
app.post('/query', async (req, res) => {
  const ip = clientIp(req);
  try {
    const { sql: rawSql, params } = req.body || {};
    if (!rawSql || typeof rawSql !== 'string')
      return res.status(400).json({ erro: 'sql obrigatório' });
    if (rawSql.length > 8000)
      return res.status(413).json({ erro: 'sql muito grande' });

    const trimmed = rawSql.trim();
    const lower = trimmed.toLowerCase();
    if (!(lower.startsWith('select') || lower.startsWith('with'))) {
      log({ ip, sql: trimmed.slice(0, 200), motivo: 'nao_select' }, 'deny');
      return res.status(403).json({ erro: 'Apenas SELECT é permitido' });
    }
    if (/\b(insert|update|delete|drop|alter|truncate|exec|execute|merge|grant|revoke|create|sp_|xp_|openrowset|opendatasource|bulk|backup|restore|shutdown|kill)\b/i.test(trimmed)) {
      log({ ip, sql: trimmed.slice(0, 200), motivo: 'comando_proibido' }, 'deny');
      return res.status(403).json({ erro: 'Comando não permitido' });
    }
    if (trimmed.includes(';')) {
      log({ ip, sql: trimmed.slice(0, 200), motivo: 'multi_statement' }, 'deny');
      return res.status(403).json({ erro: 'Múltiplas instruções não permitidas' });
    }

    const pool = await getPool();
    const reqSql = pool.request();
    if (params && typeof params === 'object') {
      for (const [k, v] of Object.entries(params)) {
        if (!/^[A-Za-z_][A-Za-z0-9_]{0,30}$/.test(k))
          return res.status(400).json({ erro: 'Nome de parâmetro inválido' });
        reqSql.input(k, v);
      }
    }
    const t0 = Date.now();
    const result = await reqSql.query(trimmed);
    const ms = Date.now() - t0;
    let rows = result.recordset || [];
    const truncado = rows.length > MAX_ROWS;
    if (truncado) rows = rows.slice(0, MAX_ROWS);
    log({ ip, path: '/query', sql: trimmed.slice(0, 500), rows: rows.length, ms });
    res.json({ total: rows.length, truncado, rows });
  } catch (e) {
    log({ ip, path: '/query', erro: e.message }, 'error');
    res.status(500).json({ erro: e.message });
  }
});

// 404
app.use((req, res) => res.status(404).json({ erro: 'Não encontrado' }));

app.listen(PORT, '127.0.0.1', () => {
  console.log(`[UltraSyst Relay] ouvindo em 127.0.0.1:${PORT}`);
  console.log(`[UltraSyst Relay] SQL: ${SQL_HOST}:${SQL_PORT}/${SQL_DB} (user ${SQL_USER})`);
  console.log(`[UltraSyst Relay] Rate limit: ${RATE_PER_MIN}/min  Allowlist: ${ALLOWLIST_IPS.length || 'desativada'}`);
  console.log(`[UltraSyst Relay] Logs: ${LOG_DIR}`);
});
