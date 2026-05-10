// Cliente da API REST do APK Ponto de Venda (Ultrasyst).
// Endpoint: http://89.117.76.176:3232/  (configurável via env ULTRASYST_APK_URL)
// Auth: header `ultrasyst: ultrasyst` (API key fixa) + Bearer Token (login).
//
// Login feito 1x — token em cache de memória. Renova ao receber 401.

const BASE_URL = (process.env.ULTRASYST_APK_URL || 'http://89.117.76.176:3232').replace(/\/$/, '');
const API_KEY  = 'ultrasyst';
const EMAIL    = process.env.ULTRASYST_APK_EMAIL || '';
const SENHA    = process.env.ULTRASYST_APK_SENHA || '';

let tokenCache = null;
let tokenAt = 0;
const TOKEN_TTL_MS = 30 * 60 * 1000; // 30 min — renova preventivamente

async function fetchJson(path, opts = {}) {
  const headers = { ultrasyst: API_KEY, 'Content-Type': 'application/json', ...(opts.headers || {}) };
  const r = await fetch(BASE_URL + path, {
    method: opts.method || 'GET',
    headers,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const text = await r.text();
  let data; try { data = JSON.parse(text); } catch { data = { raw: text }; }
  if (!r.ok) {
    const e = new Error(data?.message || data?.erro || `HTTP ${r.status}`);
    e.status = r.status; e.body = data;
    throw e;
  }
  return data;
}

async function login(force = false) {
  if (!force && tokenCache && (Date.now() - tokenAt < TOKEN_TTL_MS)) return tokenCache;
  if (!EMAIL || !SENHA) throw new Error('ULTRASYST_APK_EMAIL/SENHA nao configurados');
  const r = await fetchJson(`/usuario/usuario?email=${encodeURIComponent(EMAIL)}&senha=${encodeURIComponent(SENHA)}`);
  // Resposta tem o token em algum campo — tenta os comuns.
  const token = r?.token || r?.access_token || r?.usuario?.token || r?.data?.token;
  if (!token) throw new Error('Login OK mas sem token na resposta: ' + JSON.stringify(r).slice(0, 200));
  tokenCache = token;
  tokenAt = Date.now();
  return token;
}

// Faz uma chamada autenticada — refaz login se receber 401.
async function callAuth(path, opts = {}) {
  const token = await login();
  const tryOnce = (tok) => fetchJson(path, {
    ...opts,
    headers: { ...(opts.headers || {}), Authorization: `Bearer ${tok}` },
  });
  try {
    return await tryOnce(token);
  } catch (e) {
    if (e.status === 401) {
      const novo = await login(true);
      return tryOnce(novo);
    }
    throw e;
  }
}

// ── Endpoints usados pelo gerador de pedidos ──

// Próximo COD_PEDIDO disponível.
async function getCodigosPedido() {
  return callAuth('/ponto_venda/pedido/codigos');
}

// Cria o cabeçalho do pedido. Body = PedidoObservacao (snake_case).
async function criarPedido(pedido) {
  return callAuth('/ponto_venda/pedidos', { method: 'POST', body: pedido });
}

// Adiciona um item ao pedido. Body = ItemPedido.
async function adicionarItem(codPedido, item) {
  return callAuth(`/ponto_venda/envio/${codPedido}/itens`, { method: 'POST', body: item });
}

// Health rápido — bate no endpoint de códigos (precisa auth, mas confirma vida).
async function health() {
  try {
    const r = await getCodigosPedido();
    return { ok: true, base: BASE_URL, sample: r };
  } catch (e) {
    return { ok: false, base: BASE_URL, erro: e.message, status: e.status || null };
  }
}

module.exports = {
  BASE_URL,
  login,
  health,
  getCodigosPedido,
  criarPedido,
  adicionarItem,
};
