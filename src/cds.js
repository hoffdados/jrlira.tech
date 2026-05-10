// Multi-CD: cada CD tem seu próprio relay HTTP (URL+TOKEN).
// Tabela `cds` no Postgres é a fonte da verdade — admin gerencia via /admin-cds.
//
// O CD legado (sync_cd.js) continua usando ULTRASYST_RELAY_URL/TOKEN do .env
// até a migração full. Este módulo cobre só os CDs novos cadastrados na tabela.

const { query: dbQuery } = require('./db');

let cache = null;
let cacheTs = 0;
const CACHE_MS = 60_000;

async function listarCds(soAtivos = true) {
  const agora = Date.now();
  if (!cache || agora - cacheTs > CACHE_MS) {
    cache = await dbQuery(
      `SELECT id, codigo, nome, url, token, banco, emp_codi, loc_codi, ativo,
              criado_em, atualizado_em
         FROM cds ORDER BY codigo`
    );
    cacheTs = agora;
  }
  return soAtivos ? cache.filter(c => c.ativo) : cache;
}

function invalidarCache() { cache = null; }

async function getCd(codigo) {
  const cds = await listarCds(false);
  return cds.find(c => c.codigo === codigo) || null;
}

// Cliente HTTP pra um CD específico (espelha a interface de src/ultrasyst.js).
// Se cd.banco estiver setado, anexa ?db=<banco> em todos endpoints + header X-DB.
function cliente(cd) {
  if (!cd) throw new Error('CD inexistente');
  if (!cd.url || !cd.token) throw new Error(`CD ${cd.codigo}: URL/token nao configurados`);
  const baseUrl = cd.url.replace(/\/$/, '');
  const dbQs = cd.banco ? `db=${encodeURIComponent(cd.banco)}` : '';
  const join = (path) => {
    if (!dbQs) return path;
    return path + (path.includes('?') ? '&' : '?') + dbQs;
  };

  async function relay(path, opts = {}) {
    const headers = {
      Authorization: `Bearer ${cd.token}`,
      'Content-Type': 'application/json',
    };
    if (cd.banco) headers['X-DB'] = cd.banco;
    const r = await fetch(baseUrl + join(path), {
      method: opts.method || 'GET',
      headers,
      body: opts.body ? JSON.stringify(opts.body) : undefined,
    });
    const text = await r.text();
    let data; try { data = JSON.parse(text); } catch { data = { raw: text }; }
    if (!r.ok) {
      const err = new Error(data.erro || `Relay HTTP ${r.status}`);
      err.status = r.status; err.body = data;
      throw err;
    }
    return data;
  }

  return {
    health: () => relay('/health'),
    listarTabelas: (q) => relay('/tables' + (q ? `?q=${encodeURIComponent(q)}` : '')),
    colunas: (t) => relay('/columns/' + encodeURIComponent(t)),
    query: (sql, params) => relay('/query', { method: 'POST', body: { sql, params } }),
  };
}

async function clientePorCodigo(codigo) {
  const cd = await getCd(codigo);
  if (!cd) throw new Error(`CD ${codigo} nao encontrado`);
  if (!cd.ativo) throw new Error(`CD ${codigo} desativado`);
  return cliente(cd);
}

module.exports = { listarCds, getCd, cliente, clientePorCodigo, invalidarCache };
