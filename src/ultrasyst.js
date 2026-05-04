// Cliente do relay UltraSyst (HTTP -> SQL Server na máquina do Power BI Gateway).
// Configurado via env: ULTRASYST_RELAY_URL e ULTRASYST_RELAY_TOKEN.

const URL = process.env.ULTRASYST_RELAY_URL || '';
const TOKEN = process.env.ULTRASYST_RELAY_TOKEN || '';
const SECRET_HEADER = process.env.ULTRASYST_SECRET_HEADER || '';
const SECRET_VALUE  = process.env.ULTRASYST_SECRET_VALUE  || '';
const CF_ACCESS_ID  = process.env.ULTRASYST_CF_ACCESS_CLIENT_ID || '';
const CF_ACCESS_SEC = process.env.ULTRASYST_CF_ACCESS_CLIENT_SECRET || '';

function ensureConfigured() {
  if (!URL || !TOKEN) {
    const e = new Error('UltraSyst relay não configurado (ULTRASYST_RELAY_URL/_TOKEN)');
    e.code = 'RELAY_NOT_CONFIGURED';
    throw e;
  }
}

async function relayFetch(path, opts = {}) {
  ensureConfigured();
  const headers = {
    Authorization: `Bearer ${TOKEN}`,
    'Content-Type': 'application/json',
    ...(opts.headers || {}),
  };
  if (SECRET_HEADER && SECRET_VALUE) headers[SECRET_HEADER] = SECRET_VALUE;
  if (CF_ACCESS_ID && CF_ACCESS_SEC) {
    headers['CF-Access-Client-Id'] = CF_ACCESS_ID;
    headers['CF-Access-Client-Secret'] = CF_ACCESS_SEC;
  }
  const r = await fetch(URL.replace(/\/$/, '') + path, {
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

async function health() {
  return relayFetch('/health');
}

async function listarTabelas(filtro) {
  const q = filtro ? `?q=${encodeURIComponent(filtro)}` : '';
  return relayFetch('/tables' + q);
}

async function colunas(tabela) {
  return relayFetch('/columns/' + encodeURIComponent(tabela));
}

// Executa um SELECT parametrizado. Params são nomeados: { ano: 2026 } -> @ano
async function query(sqlText, params) {
  return relayFetch('/query', { method: 'POST', body: { sql: sqlText, params } });
}

module.exports = { health, listarTabelas, colunas, query };
