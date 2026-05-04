# UltraSyst Relay — Instalação segura

Roda na **mesma máquina** onde está o Power BI Gateway. Expõe um endpoint HTTPS
protegido por token + Cloudflare Tunnel. **SOMENTE leitura (SELECT)**, sem porta
aberta no firewall, com rate limit, log de auditoria e bloqueio por IP.

---

## Modelo de segurança

| Camada | Proteção |
|---|---|
| 1. Bind | Servidor escuta só em `127.0.0.1` (não na LAN) |
| 2. Tunnel | Cloudflare Tunnel encapsula em HTTPS (sem porta no firewall) |
| 3. Token | Bearer 32+ chars, comparação `timingSafeEqual` (anti-timing-attack) |
| 4. Header secreto | Header customizado opcional (defesa em profundidade) |
| 5. IP allowlist | Aceita só IPs específicos (Railway) |
| 6. Rate limit | 120 req/min por IP (configurável) |
| 7. SQL Read-only | Bloqueia INSERT/UPDATE/DELETE/EXEC/DROP/etc + multi-statement |
| 8. Auditoria | Log JSON de toda query (acesso, deny, erro) |
| 9. SQL user | Conta SQL com permissão SOMENTE de SELECT (passo 7 abaixo) |

---

## 1) Atualizar o relay (já patcheado pra produção)

Já está com `index.js` reforçado. Se vier de versão antiga, sobrescreva os arquivos.

---

## 2) Instalar Node 20 LTS

https://nodejs.org/ (Windows x64). Confirmar:
```powershell
node --version
npm --version
```

---

## 3) Configurar `.env`

```powershell
cd C:\ultrasyst-relay
copy .env.example .env
notepad .env
```

Preencher:
```
RELAY_PORT=8787
RELAY_TOKEN=<gerar abaixo, 44 chars base64>
RELAY_SECRET_HEADER=x-jrlira-secret
RELAY_SECRET_VALUE=<gerar abaixo, 32 chars>
ALLOWLIST_IPS=<deixar vazio por enquanto>
SQL_HOST=127.0.0.1
SQL_PORT=1433
SQL_DB=ITAUTUBA
SQL_USER=jrlira_relay
SQL_PASS=<senha do user SQL criado no passo 7>
MAX_ROWS=5000
RATE_PER_MIN=120
LOG_DIR=C:\ultrasyst-relay\logs
```

Gerar 2 segredos:
```powershell
[Convert]::ToBase64String((1..32 | ForEach-Object { [byte](Get-Random -Max 256) }))
[Convert]::ToBase64String((1..24 | ForEach-Object { [byte](Get-Random -Max 256) }))
```

**Guardar em local seguro** — vai precisar deles também no Railway.

---

## 4) Instalar dependências e testar

```powershell
cd C:\ultrasyst-relay
npm install
node index.js
```

Resposta esperada:
```
[UltraSyst Relay] ouvindo em 127.0.0.1:8787
[UltraSyst Relay] SQL: 127.0.0.1:1433/ITAUTUBA (user jrlira_relay)
[UltraSyst Relay] Rate limit: 120/min  Allowlist: desativada
[UltraSyst Relay] Logs: C:\ultrasyst-relay\logs
```

Em outra aba:
```powershell
Invoke-RestMethod http://localhost:8787/health
```

---

## 5) Cloudflare Tunnel (HTTPS sem abrir porta)

### 5.1) Criar conta Cloudflare grátis
- https://dash.cloudflare.com/sign-up
- Não precisa transferir domínio. Pode usar grátis com domínio `.workers.dev` ou
  comprar um domínio barato (Registro.br ~R$40/ano, Cloudflare ~$10/ano).

### 5.2) Baixar `cloudflared`
https://github.com/cloudflare/cloudflared/releases/latest →
`cloudflared-windows-amd64.exe` → renomear pra `cloudflared.exe` →
salvar em `C:\cloudflared\`.

### 5.3) Login na Cloudflare
```powershell
C:\cloudflared\cloudflared.exe tunnel login
```
Abre o browser pra autorizar. Selecionar o domínio.

### 5.4) Criar túnel nomeado
```powershell
C:\cloudflared\cloudflared.exe tunnel create ultrasyst
```
Anota o **tunnel ID** que aparece (UUID).

### 5.5) Apontar DNS
```powershell
C:\cloudflared\cloudflared.exe tunnel route dns ultrasyst ultrasyst.SEUDOMINIO.com
```

### 5.6) Configurar `config.yml`
Criar `C:\cloudflared\config.yml`:
```yaml
tunnel: ultrasyst
credentials-file: C:\Users\<USUARIO>\.cloudflared\<TUNNEL-ID>.json

ingress:
  - hostname: ultrasyst.SEUDOMINIO.com
    service: http://127.0.0.1:8787
    originRequest:
      connectTimeout: 30s
      noTLSVerify: false
  - service: http_status:404
```

### 5.7) Rodar
```powershell
C:\cloudflared\cloudflared.exe tunnel --config C:\cloudflared\config.yml run ultrasyst
```

Testar do Railway (ou seu PC):
```bash
curl https://ultrasyst.SEUDOMINIO.com/health
```

### 5.8) (Opcional) Cloudflare Access — autenticação extra

Em Zero Trust → Access → Applications:
- Criar aplicação `ultrasyst.SEUDOMINIO.com`
- Política: aceitar só Service Token (gerar no painel)
- Railway envia headers `CF-Access-Client-Id` + `CF-Access-Client-Secret`
- Bloqueia até quem tem o Bearer Token: precisa também do Service Token Cloudflare

---

## 6) Rodar como serviços Windows (24/7, auto-start)

### Relay
```powershell
npm install -g pm2 pm2-windows-startup
pm2-startup install
cd C:\ultrasyst-relay
pm2 start index.js --name ultrasyst-relay --max-memory-restart 500M
pm2 save
```

### Cloudflared
```powershell
C:\cloudflared\cloudflared.exe service install
```

Ambos sobem no boot.

---

## 7) Criar usuário SQL com SOMENTE leitura (recomendado)

Hoje o relay usa `ASAB` que é admin — perigoso. Criar um user dedicado:

No SSMS, conectar como sa/admin e executar:
```sql
USE master;
CREATE LOGIN jrlira_relay WITH PASSWORD = 'senha_forte_aqui', CHECK_POLICY = ON;

USE ITAUTUBA;
CREATE USER jrlira_relay FOR LOGIN jrlira_relay;
ALTER ROLE db_datareader ADD MEMBER jrlira_relay;
GRANT VIEW DEFINITION TO jrlira_relay;  -- para INFORMATION_SCHEMA
```

Atualizar `.env`:
```
SQL_USER=jrlira_relay
SQL_PASS=<senha_forte>
```

Reiniciar:
```powershell
pm2 restart ultrasyst-relay
```

Mesmo se o token vazar, o atacante **não consegue gravar nada** no UltraSyst.

---

## 8) Configurar no Railway (lado JR Lira)

No `.env` do Railway:
```
ULTRASYST_RELAY_URL=https://ultrasyst.SEUDOMINIO.com
ULTRASYST_RELAY_TOKEN=<RELAY_TOKEN gerado no passo 3>
ULTRASYST_SECRET_HEADER=x-jrlira-secret
ULTRASYST_SECRET_VALUE=<RELAY_SECRET_VALUE gerado no passo 3>
```

Redeploy: `railway up --detach`.

---

## 9) Travar firewall (defesa final)

Quando o tunnel estiver funcionando, **bloquear porta 8787 no firewall externo**:
- O relay já escuta só em `127.0.0.1` (local)
- Garantir que ninguém da LAN consegue acessar diretamente a porta 8787

Windows Firewall:
- Inbound rule: bloquear TCP 8787 de qualquer origem que não seja 127.0.0.1

---

## 10) Verificar logs

```powershell
ls C:\ultrasyst-relay\logs
# access-2026-05-02.log  → toda query bem-sucedida
# deny-2026-05-02.log    → tentativas bloqueadas (token errado, rate limit, IP)
# error-2026-05-02.log   → erros de SQL
```

Se houver muitas linhas em `deny-*`, alguém está tentando força bruta — investigar.

---

## Checklist final

- [ ] `.env` com token 32+ chars e SECRET_HEADER/VALUE
- [ ] User SQL `jrlira_relay` com SOMENTE `db_datareader`
- [ ] Senha do `ASAB` rotacionada (já vazou em troca de mensagens com IA)
- [ ] Cloudflare Tunnel ativo
- [ ] Cloudflare Access (opcional, recomendado)
- [ ] Relay como serviço pm2
- [ ] Cloudflared como serviço Windows
- [ ] Firewall bloqueia 8787 externamente
- [ ] Logs sendo rotacionados (script externo, ex.: `forfiles /p logs /m *.log /d -30 /c "cmd /c del @path"`)
- [ ] Railway com env vars setadas
