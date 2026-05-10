# Servidor 1 (147.93.177.172) — adicionar acesso ao N_PROGRESSO

Esse servidor já tem o relay rodando (banco ITAUTUBA). Só precisa autorizar o
segundo banco (N_PROGRESSO). 5 minutos de trabalho, sem reinstalar nada novo.

---

## 1) Atualizar o código do relay

```powershell
cd C:\ultrasyst-relay
```

Sobrescreve `index.js` pela versão nova (com suporte a `?db=` e `SQL_DBS`).
Pode copiar do repositório jrlira-tech ou pelo AnyDesk.

---

## 2) Editar `.env` — adicionar SQL_DBS

```powershell
notepad .env
```

Acrescentar a linha (mantendo `SQL_DB=ITAUTUBA` como está):

```
SQL_DBS=ITAUTUBA,N_PROGRESSO
```

(Não precisa repetir ITAUTUBA — o relay já adiciona automático. Mas tudo bem listar.)

---

## 3) Garantir permissão SQL no N_PROGRESSO

No SSMS (logado como sa/admin), conectar no SQL Server local e rodar:

```sql
USE N_PROGRESSO;
CREATE USER jrlira_relay FOR LOGIN jrlira_relay;
ALTER ROLE db_datareader ADD MEMBER jrlira_relay;
GRANT VIEW DEFINITION TO jrlira_relay;
```

Se der erro "User already exists", o user já existe — só precisa garantir o role:

```sql
USE N_PROGRESSO;
ALTER ROLE db_datareader ADD MEMBER jrlira_relay;
```

---

## 4) Reiniciar o relay

```powershell
pm2 restart ultrasyst-relay
pm2 logs ultrasyst-relay --lines 20
```

Deve aparecer:
```
[UltraSyst Relay] Bancos permitidos: ITAUTUBA, N_PROGRESSO (default: ITAUTUBA)
```

---

## 5) Testar

No próprio servidor:
```powershell
$h = @{ Authorization = "Bearer <SEU_TOKEN>"; "x-jrlira-secret" = "<SEU_SECRET>" }
Invoke-RestMethod -Uri "http://localhost:8787/health?db=N_PROGRESSO" -Headers $h
Invoke-RestMethod -Uri "http://localhost:8787/health?db=ITAUTUBA"    -Headers $h
```

Os dois devem responder `ok=true`.

---

## 6) Cadastrar no JR Lira (jrlira.tech)

Acesse `/admin-cds` (perfil admin). Cadastre **2 entradas** (mesmo URL+token, banco diferente):

| Código | Nome | URL | Banco |
|---|---|---|---|
| `srv1-itautuba` | Servidor 1 — ITAUTUBA | `https://ultrasyst.SEUDOMINIO.com` | `ITAUTUBA` |
| `srv1-nprogresso` | Servidor 1 — N_PROGRESSO | `https://ultrasyst.SEUDOMINIO.com` | `N_PROGRESSO` |

Clica em **Health** em cada — confirma `ok` em ms baixo.

---

## Pronto

O relay agora atende os 2 bancos do servidor 1. Cliente continua antigo
funcionando (sem `?db=` cai no ITAUTUBA), e o JR Lira pega N_PROGRESSO via
`?db=N_PROGRESSO`.
