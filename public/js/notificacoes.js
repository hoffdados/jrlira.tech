// Badge de notificações in-app — incluir <script src="/js/notificacoes.js"></script>
// Auto-instala se houver token; pollinga /api/notificacoes a cada 60s.
(() => {
  const TOKEN_KEYS = ['jrlira_token', 'vend_token', 'token'];
  function token() { for (const k of TOKEN_KEYS) { const v = localStorage.getItem(k); if (v) return v; } return null; }
  if (!token()) return;

  // CSS injetado
  const css = `
    .notif-fab{position:fixed;bottom:20px;right:20px;width:48px;height:48px;border-radius:50%;background:#1e293b;border:2px solid #334155;color:#e2e8f0;cursor:pointer;display:flex;align-items:center;justify-content:center;font-size:22px;box-shadow:0 4px 12px rgba(0,0,0,.4);z-index:9998;transition:all .15s}
    .notif-fab:hover{background:#334155;transform:scale(1.05)}
    .notif-badge{position:absolute;top:-4px;right:-4px;background:#ef4444;color:#fff;border-radius:10px;min-width:18px;height:18px;font-size:11px;font-weight:700;display:flex;align-items:center;justify-content:center;padding:0 5px}
    .notif-panel{position:fixed;bottom:78px;right:20px;width:360px;max-height:480px;background:#0f172a;border:1px solid #334155;border-radius:8px;display:none;flex-direction:column;z-index:9999;box-shadow:0 8px 24px rgba(0,0,0,.5);font-family:system-ui,-apple-system,sans-serif}
    .notif-panel.open{display:flex}
    .notif-header{padding:10px 14px;border-bottom:1px solid #334155;display:flex;justify-content:space-between;align-items:center;color:#e2e8f0;font-weight:600}
    .notif-header button{background:none;border:0;color:#64748b;cursor:pointer;font-size:12px}
    .notif-header button:hover{color:#94a3b8}
    .notif-list{overflow-y:auto;flex:1}
    .notif-item{padding:10px 14px;border-bottom:1px solid #1e293b;cursor:pointer;color:#cbd5e1;font-size:13px;line-height:1.4}
    .notif-item:hover{background:#1e293b}
    .notif-item.unread{background:rgba(56,189,248,.06);border-left:3px solid #38bdf8}
    .notif-item .titulo{color:#e2e8f0;font-weight:600;margin-bottom:2px}
    .notif-item .corpo{color:#94a3b8;font-size:12px}
    .notif-item .quando{color:#64748b;font-size:11px;margin-top:4px}
    .notif-empty{padding:30px 14px;text-align:center;color:#64748b;font-size:13px}
  `;
  const style = document.createElement('style');
  style.textContent = css;
  document.head.appendChild(style);

  const fab = document.createElement('button');
  fab.className = 'notif-fab';
  fab.title = 'Notificações';
  fab.innerHTML = '<span>🔔</span><span class="notif-badge" style="display:none">0</span>';
  document.body.appendChild(fab);

  const panel = document.createElement('div');
  panel.className = 'notif-panel';
  panel.innerHTML = `
    <div class="notif-header">
      <span>Notificações</span>
      <button id="notif-marcar-todas">Marcar todas como lidas</button>
    </div>
    <div class="notif-list" id="notif-list"></div>`;
  document.body.appendChild(panel);

  const badge = fab.querySelector('.notif-badge');
  const list = panel.querySelector('#notif-list');

  fab.addEventListener('click', () => panel.classList.toggle('open'));
  document.addEventListener('click', (e) => {
    if (!panel.contains(e.target) && e.target !== fab && !fab.contains(e.target)) {
      panel.classList.remove('open');
    }
  });

  panel.querySelector('#notif-marcar-todas').addEventListener('click', async (e) => {
    e.stopPropagation();
    await fetch('/api/notificacoes/marcar-todas-lidas', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${token()}` }
    }).catch(() => {});
    carregar();
  });

  function tempo(ts) {
    const d = (Date.now() - new Date(ts).getTime()) / 1000;
    if (d < 60) return 'agora';
    if (d < 3600) return Math.round(d / 60) + 'min';
    if (d < 86400) return Math.round(d / 3600) + 'h';
    return Math.round(d / 86400) + 'd';
  }

  async function carregar() {
    try {
      const r = await fetch('/api/notificacoes', { headers: { 'Authorization': `Bearer ${token()}` } });
      if (r.status === 401) return; // token inválido, ignora
      const d = await r.json();
      const n = d.nao_lidas || 0;
      badge.textContent = n > 99 ? '99+' : n;
      badge.style.display = n > 0 ? 'flex' : 'none';

      if (!d.notificacoes?.length) {
        list.innerHTML = '<div class="notif-empty">Sem notificações</div>';
        return;
      }
      list.innerHTML = d.notificacoes.map(n => `
        <div class="notif-item ${n.lida_em ? '' : 'unread'}" data-id="${n.id}" data-url="${n.url || ''}">
          <div class="titulo">${escapeHtml(n.titulo)}</div>
          ${n.corpo ? `<div class="corpo">${escapeHtml(n.corpo)}</div>` : ''}
          <div class="quando">${tempo(n.criado_em)} atrás</div>
        </div>
      `).join('');

      list.querySelectorAll('.notif-item').forEach(el => {
        el.addEventListener('click', async () => {
          const id = el.dataset.id;
          const url = el.dataset.url;
          await fetch(`/api/notificacoes/${id}/lida`, {
            method: 'POST', headers: { 'Authorization': `Bearer ${token()}` }
          }).catch(() => {});
          if (url) location.href = url;
          else carregar();
        });
      });
    } catch (e) { /* silencioso */ }
  }

  function escapeHtml(s) {
    return String(s || '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);
  }

  carregar();
  setInterval(carregar, 60000);
})();
