// Service Worker mínimo — só pra cumprir o critério de instalabilidade do PWA.
// NÃO cacheia nada porque o app usa JWT, multi-loja e dados ao vivo. Cache aqui só causa bug
// ("vejo dado de outra loja", "preço antigo após otimização", etc).
//
// Bump a versão quando precisar forçar update em todos os dispositivos instalados.
const VERSION = '2026-05-11.1';

self.addEventListener('install', (e) => {
  self.skipWaiting();
});

self.addEventListener('activate', (e) => {
  e.waitUntil(self.clients.claim());
});

// Pass-through: nunca intercepta. Browser trata fetch normalmente.
self.addEventListener('fetch', (e) => {
  // intencional vazio — sem cache, sem network override.
});

// Mensagem opcional pra forçar update da página (via shared.js).
self.addEventListener('message', (e) => {
  if (e.data === 'SKIP_WAITING') self.skipWaiting();
});
