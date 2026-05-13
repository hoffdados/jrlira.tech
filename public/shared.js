// Helpers compartilhados — incluir antes do JS da página: <script src="/shared.js"></script>

// ── PWA: injeta manifest + meta theme-color e registra service worker ──
// Faz isso em runtime pra não precisar editar 50 HTMLs. Idempotente (não duplica tags).
(function setupPWA() {
  if (typeof document === 'undefined') return;
  function injetar() {
    const head = document.head;
    if (!head) return;
    if (!head.querySelector('link[rel="manifest"]')) {
      const l = document.createElement('link');
      l.rel = 'manifest'; l.href = '/manifest.json';
      head.appendChild(l);
    }
    if (!head.querySelector('meta[name="theme-color"]')) {
      const m = document.createElement('meta');
      m.name = 'theme-color'; m.content = '#0f172a';
      head.appendChild(m);
    }
    if (!head.querySelector('link[rel="apple-touch-icon"]')) {
      const a = document.createElement('link');
      a.rel = 'apple-touch-icon'; a.href = '/icons/icon.svg';
      head.appendChild(a);
    }
    if (!head.querySelector('meta[name="mobile-web-app-capable"]')) {
      const c = document.createElement('meta');
      c.name = 'mobile-web-app-capable'; c.content = 'yes';
      head.appendChild(c);
    }
  }
  if (document.head) injetar(); else document.addEventListener('DOMContentLoaded', injetar);

  if ('serviceWorker' in navigator) {
    window.addEventListener('load', () => {
      navigator.serviceWorker.register('/sw.js')
        .catch(err => console.warn('[PWA] SW falhou:', err.message));
    });
  }
})();


const LOJAS = {
  1: 'ECONOMICO',
  2: 'BR',
  3: 'JOAO PESSOA',
  4: 'FLORESTA',
  5: 'SAO JOSE',
  6: 'SANTAREM',
};
const LOJAS_FULL = {
  1: 'SUPERASA ECONOMICO',
  2: 'SUPERASA BR',
  3: 'SUPERASA JOAO PESSOA',
  4: 'SUPERASA FLORESTA',
  5: 'SUPERASA SAO JOSE',
  6: 'SUPERASA SANTAREM',
};

function escapeHtml(s) {
  if (s === null || s === undefined) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function fmtMoeda(v) {
  const n = parseFloat(v) || 0;
  return n.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function fmtData(d) {
  if (!d) return '';
  const dt = typeof d === 'string' ? new Date(d) : d;
  if (isNaN(dt)) return '';
  return dt.toLocaleDateString('pt-BR', { timeZone: 'UTC' });
}

function nomeLoja(id) {
  return LOJAS[id] || `Loja ${id}`;
}

// Anexa o token JWT à URL para endpoints autenticados consumidos por <img>/<a>
function fotoUrl(path) {
  if (!path) return '';
  const t = localStorage.getItem('jrlira_token');
  if (!t) return path;
  return path + (path.includes('?') ? '&' : '?') + 'token=' + encodeURIComponent(t);
}

// Desabilita o botão durante a Promise. Restaura texto e estado mesmo em erro.
async function withLoading(btn, fn, textoCarregando = 'Aguarde...') {
  if (!btn) return fn();
  const original = btn.innerHTML;
  btn.disabled = true;
  btn.innerHTML = textoCarregando;
  try {
    return await fn();
  } finally {
    btn.disabled = false;
    btn.innerHTML = original;
  }
}

// Wrapper de fetch que adiciona Authorization e redireciona ao login se 401
async function apiFetch(url, opts = {}) {
  const token = localStorage.getItem('jrlira_token');
  const headers = { ...(opts.headers || {}) };
  if (token) headers['Authorization'] = 'Bearer ' + token;
  if (opts.body && !(opts.body instanceof FormData) && !headers['Content-Type']) {
    headers['Content-Type'] = 'application/json';
  }
  const r = await fetch(url, { ...opts, headers });
  if (r.status === 401) {
    ['jrlira_token', 'jrlira_nome', 'jrlira_perfil', 'jrlira_lojas'].forEach(k => localStorage.removeItem(k));
    location.href = '/';
    throw new Error('Sessão expirada');
  }
  return r;
}
