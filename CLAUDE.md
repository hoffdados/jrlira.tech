# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
npm install          # instalar dependências
node server.js       # iniciar servidor (porta 3001, padrão)
railway up --detach  # deploy no Railway (executar após qualquer alteração no código)
```

Não há build, transpilação, lint ou test runner configurados. Alterações no código exigem reiniciar o servidor manualmente.

**Deploy:** após qualquer alteração de código, executar `railway up --detach` imediatamente — sem esperar confirmação do usuário.

## Stack

- **Backend:** Node.js + Express 5, PostgreSQL (driver `pg`, sem ORM), JWT + bcryptjs
- **Frontend:** HTML/CSS/JS vanilla em `public/` — sem framework, sem build
- **Parsing especializado:** `xml2js` (NF-e XML), `xlsx` (planilhas QLP), `cheerio` (HTM iPonto por posição CSS)
- **Idioma:** PT-BR em todo o código (variáveis, comentários, banco, UI)

## Arquitetura

```
server.js          → entry point: monta rotas, inicializa schema PostgreSQL, serve public/
src/
  db.js            → pool PostgreSQL (DATABASE_URL via .env)
  auth.js          → middleware JWT: autenticar, apenasAdmin
  routes/
    auth.js        → login, CRUD de usuários (admin)
    funcionarios.js→ CRUD funcionários + upload foto (multer)
    ponto.js       → importação/análise de ponto (HTM iPonto)
    importacao.js  → importação de folha QLP (Excel)
    notas.js       → fluxo NF-e completo: cadastro → estoque → auditoria
public/            → páginas HTML estáticas (cada uma autocontida com JS embutido)
uploads/fotos/     → fotos de funcionários (gitignored)
```

## Banco de dados

PostgreSQL sem ORM. Queries diretas com `pool.query` e placeholders `$1, $2`. Schema inicializado no startup via `initDB()` em `server.js` (sem framework de migrations — ALTERs em catch silencioso).

Tabelas principais:
| Tabela | Descrição |
|---|---|
| `rh_usuarios` | Usuários do sistema (perfil, loja_id) |
| `funcionarios` | Cadastro de funcionários |
| `funcionario_eventos` | Histórico de eventos (afastamento, transferência) |
| `ponto_importacoes` / `ponto_registros` | Importações e registros de ponto |
| `notas_entrada` | Cabeçalho das NF-e (status, loja_id) |
| `itens_nota` | Itens das NF-e |
| `conferencias_estoque` | Contagem cega no estoque |
| `auditoria_itens` | Auditoria e fechamento |

## Controle de acesso

`loja_id` em usuários e notas implementa multi-tenant (filtro por loja).

Perfis e permissões:
- `admin` → tudo, gestão de usuários
- `rh` → quadro de lotação, ponto
- `cadastro` → importar e validar NF-e
- `estoque` → conferência cega de itens
- `auditor` → auditoria e fechamento de NF-e

O perfil é lido do JWT (`req.usuario.perfil`). O middleware `autenticar` rejeita tokens inválidos; `apenasAdmin` rejeita não-admins.

## Padrões do frontend

Cada página em `public/` é autocontida: JS embutido em `<script>`, chamadas `fetch` ao `/api/*`, estado em `localStorage` (`token`, `perfil`, `loja_id`, `nome`). Não há roteamento SPA — navegação por links entre HTMLs.

Padrão de renderização: fetch → parse JSON → manipulação direta do DOM. Filtros re-renderizam a lista a cada `oninput`/`onchange`.

## Variáveis de ambiente (.env)

```
DATABASE_URL=   # string de conexão PostgreSQL
JWT_SECRET=     # segredo para assinar tokens
PORT=3001       # porta do servidor
```

Na primeira execução cria usuário padrão: `admin` / `admin123`.
