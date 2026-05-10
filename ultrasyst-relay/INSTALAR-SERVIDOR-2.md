# Servidor 2 (86.48.17.169) — instalar relay do zero (asa_frio + asa_santarem)

Servidor novo. Replica o setup do servidor 1, configurando 2 bancos desde o
início. Estimativa: 30–45 min se já tiver Cloudflare Tunnel funcionando.

Para os passos 1–10 detalhados, veja **INSTALAR.md**. Este arquivo é a versão
condensada — segue a ordem completa, mas com os valores específicos deste servidor.

---

## 1) Pré-requisitos

- Node 20 LTS instalado (`node --version`)
- SQL Server local com `asa_frio` + `asa_santarem`
- Acesso admin (sa) pra criar user SQL read-only
- Cloudflare Tunnel já configurado na conta (vai criar novo túnel)

---

## 2) Copiar pasta `ultrasyst-relay`

Copia toda a pasta `ultrasyst-relay/` do projeto jrlira-tech pra `C:\ultrasyst-relay`
no servidor 2 (sem `node_modules` e sem `logs`).

```powershell
cd C:\ultrasyst-relay
npm install
```

---

## 3) Criar user SQL read-only nos 2 bancos

No SSMS:
```sql
USE master;
CREATE LOGIN jrlira_relay WITH PASSWORD = '<SENHA_FORTE>', CHECK_POLICY = ON;

USE asa_frio;
CREATE USER jrlira_relay FOR LOGIN jrlira_relay;
ALTER ROLE db_datareader ADD MEMBER jrlira_relay;
GRANT VIEW DEFINITION TO jrlira_relay;

USE asa_santarem;
CREATE USER jrlira_relay FOR LOGIN jrlira_relay;
ALTER ROLE db_datareader ADD MEMBER jrlira_relay;
GRANT VIEW DEFINITION TO jrlira_relay;
```

---

## 4) Gerar tokens e criar `.env`

```powershell
[Convert]::ToBase64String((1..32 | ForEach-Object { [byte](Get-Random -Max 256) }))
[Convert]::ToBase64String((1..24 | ForEach-Object { [byte](Get-Random -Max 256) }))
```

Anota os 2 valores. Cria `C:\ultrasyst-relay\.env`:

```
RELAY_PORT=8787
RELAY_TOKEN=<token 44 chars>
RELAY_SECRET_HEADER=x-jrlira-secret
RELAY_SECRET_VALUE=<secret 32 chars>
ALLOWLIST_IPS=
SQL_HOST=127.0.0.1
SQL_PORT=1433
SQL_DB=asa_frio
SQL_DBS=asa_frio,asa_santarem
SQL_USER=jrlira_relay
SQL_PASS=<senha do passo 3>
MAX_ROWS=5000
RATE_PER_MIN=120
LOG_DIR=C:\ultrasyst-relay\logs
```

---

## 5) Testar local

```powershell
cd C:\ultrasyst-relay
node index.js
```

Deve aparecer:
```
[UltraSyst Relay] ouvindo em 127.0.0.1:8787
[UltraSyst Relay] Bancos permitidos: asa_frio, asa_santarem (default: asa_frio)
```

Em outra aba:
```powershell
Invoke-RestMethod http://localhost:8787/health
Invoke-RestMethod http://localhost:8787/health?db=asa_santarem
```

Os 2 devem retornar `ok=true`. Encerre com Ctrl+C.

---

## 6) Cloudflare Tunnel — novo túnel pra esse servidor

```powershell
C:\cloudflared\cloudflared.exe tunnel login
C:\cloudflared\cloudflared.exe tunnel create ultrasyst-srv2
C:\cloudflared\cloudflared.exe tunnel route dns ultrasyst-srv2 ultrasyst-srv2.SEUDOMINIO.com
```

`C:\cloudflared\config.yml`:
```yaml
tunnel: ultrasyst-srv2
credentials-file: C:\Users\<USUARIO>\.cloudflared\<TUNNEL-ID>.json

ingress:
  - hostname: ultrasyst-srv2.SEUDOMINIO.com
    service: http://127.0.0.1:8787
    originRequest:
      connectTimeout: 30s
  - service: http_status:404
```

Testar:
```powershell
C:\cloudflared\cloudflared.exe tunnel --config C:\cloudflared\config.yml run ultrasyst-srv2
```

Encerra com Ctrl+C depois de validar.

---

## 7) Subir como serviço (24/7)

```powershell
npm install -g pm2 pm2-windows-startup
pm2-startup install
cd C:\ultrasyst-relay
pm2 start index.js --name ultrasyst-relay --max-memory-restart 500M
pm2 save

C:\cloudflared\cloudflared.exe service install
```

---

## 8) Cadastrar no JR Lira (`/admin-cds`, perfil admin)

| Código | Nome | URL | Banco |
|---|---|---|---|
| `srv2-asafrio` | Servidor 2 — Asa Frio | `https://ultrasyst-srv2.SEUDOMINIO.com` | `asa_frio` |
| `srv2-asasantarem` | Servidor 2 — Asa Santarém | `https://ultrasyst-srv2.SEUDOMINIO.com` | `asa_santarem` |

Token e secret são os do passo 4. Clica em **Health** em cada entrada — devem responder `ok` rapidamente.

---

## 9) Bloquear porta 8787 no firewall

Mesmo procedimento do servidor 1: garantir que ninguém da LAN acessa 8787 — só
o cloudflared (que está em 127.0.0.1).

---

## Pronto

Servidor 2 ativo com 2 bancos no JR Lira. Total agora: 4 bancos (2 servidores).
Quando o servidor 3 (147.93.188.238 / C_BRANCA) voltar, repete este procedimento.
