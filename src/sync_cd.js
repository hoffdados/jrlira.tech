// Sync das tabelas espelho do CD (UltraSyst SQL Server) → Postgres.
// Roda a cada 15min; pode ser disparado manualmente via /api/admin/sync-cd-now.
//
// Tabelas espelho:
//   cd_movcompra   — cabeçalho TBMOVCOMPRA (incremental por MCP_DTEN, desde 2025-01-01)
//   cd_itemcompra  — itens TBITEMCOMPRA (acompanha movcompra)
//   cd_custoprod   — TBCUSTOPROD (incremental por MAT_DTAL, LOC_CODI='001')
//   cd_estoque     — ESTOQUE (snapshot full, LOC_CODI='001')
//   cd_vendapro    — TBVENDAPRO (incremental por TAB_DTAL, LOC_CODI='001')
//
// Tudo com WITH (NOLOCK) pra não interferir com o ERP.

const { query: dbQuery } = require('./db');
const ultrasyst = require('./ultrasyst');

const LOC_CD = '001'; // localização do CD (V.C.A.Lira / Atacadão Asa Branca)
const EMP_CD = '001';
const DATA_INICIAL = '2025-01-01'; // teto inicial pra movimentos

// ── Helpers ───────────────────────────────────────────────────────────

async function getEstado(chave) {
  const r = await dbQuery(`SELECT valor FROM _sync_state WHERE chave=$1`, [chave]);
  return r[0]?.valor || null;
}

async function setEstado(chave, valor) {
  await dbQuery(
    `INSERT INTO _sync_state (chave, valor, atualizado_em) VALUES ($1,$2,NOW())
       ON CONFLICT (chave) DO UPDATE SET valor=EXCLUDED.valor, atualizado_em=NOW()`,
    [chave, String(valor || '')]
  );
}

// Insert em lote via UNNEST — rápido pra 10k+ linhas.
async function bulkUpsert(table, columns, rows, conflictTarget) {
  if (!rows.length) return 0;
  const placeholders = columns.map((_, i) => `$${i + 1}::text[]`).join(',');
  const arrays = columns.map(col => rows.map(r => r[col] == null ? null : String(r[col])));
  const updateSet = columns
    .filter(c => !conflictTarget.split(',').map(x => x.trim()).includes(c))
    .map(c => `${c}=EXCLUDED.${c}`).join(',');
  const sql = `
    INSERT INTO ${table} (${columns.join(',')})
    SELECT * FROM UNNEST(${placeholders})
    ON CONFLICT (${conflictTarget}) DO UPDATE SET ${updateSet}, sincronizado_em=NOW()
  `;
  await dbQuery(sql, arrays);
  return rows.length;
}

// Paginação — relay trunca em 5000 linhas. Usa OFFSET/FETCH do SQL Server.
async function paginarQuery(sqlBase, orderBy, pageSize = 5000) {
  const all = [];
  let offset = 0;
  while (true) {
    const r = await ultrasyst.query(
      `${sqlBase} ORDER BY ${orderBy} OFFSET ${offset} ROWS FETCH NEXT ${pageSize} ROWS ONLY`
    );
    const rows = r.rows || [];
    all.push(...rows);
    if (rows.length < pageSize) break;
    offset += pageSize;
  }
  return all;
}

// ── 0) cd_material (cadastro de produtos do CD — snapshot full) ──────

async function syncMaterial() {
  const t0 = Date.now();
  const rows = await paginarQuery(
    `SELECT MAT_CODI, MAT_DESC, MAT_REFE, MAT_SITU, EAN_CODI
       FROM MATERIAL WITH (NOLOCK)`,
    'MAT_CODI'
  );
  if (rows.length) {
    await dbQuery(
      `INSERT INTO cd_material (mat_codi, mat_desc, mat_refe, mat_situ, ean_codi, sincronizado_em)
       SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::timestamptz[])
       ON CONFLICT (mat_codi) DO UPDATE SET
         mat_desc=EXCLUDED.mat_desc, mat_refe=EXCLUDED.mat_refe,
         mat_situ=EXCLUDED.mat_situ, ean_codi=EXCLUDED.ean_codi,
         sincronizado_em=NOW()`,
      [
        rows.map(x => String(x.MAT_CODI || '').trim()),
        rows.map(x => String(x.MAT_DESC || '').trim() || null),
        rows.map(x => String(x.MAT_REFE || '').trim() || null),
        rows.map(x => String(x.MAT_SITU || '').trim() || null),
        rows.map(x => String(x.EAN_CODI || '').trim() || null),
        rows.map(() => new Date().toISOString()),
      ]
    );
  }
  await setEstado('cd_material_ultima_sync', new Date().toISOString());
  return { tabela: 'cd_material', linhas: rows.length, ms: Date.now() - t0 };
}

// ── 1) cd_estoque (snapshot full) ─────────────────────────────────────

async function syncEstoque() {
  const t0 = Date.now();
  const rows = await paginarQuery(
    `SELECT PRO_CODI, EST_QUAN, TAM_CODI
       FROM ESTOQUE WITH (NOLOCK)
      WHERE EMP_CODI = '${EMP_CD}' AND LOC_CODI = '${LOC_CD}'`,
    'PRO_CODI'
  );
  // Snapshot: trunca e reinsere.
  await dbQuery(`TRUNCATE cd_estoque`);
  if (rows.length) {
    const cols = ['pro_codi','est_quan','tam_codi'];
    const arrays = [
      rows.map(x => String(x.PRO_CODI || '').trim()),
      rows.map(x => x.EST_QUAN == null ? '0' : String(x.EST_QUAN)),
      rows.map(x => String(x.TAM_CODI || '').trim()),
    ];
    await dbQuery(
      `INSERT INTO cd_estoque (${cols.join(',')})
       SELECT * FROM UNNEST($1::text[], $2::numeric[], $3::text[])`,
      arrays
    );
  }
  await setEstado('cd_estoque_ultima_sync', new Date().toISOString());
  return { tabela: 'cd_estoque', linhas: rows.length, ms: Date.now() - t0 };
}

// ── 2) cd_custoprod (incremental por MAT_DTAL) ────────────────────────

async function syncCustoProd() {
  const t0 = Date.now();
  // Se nunca rodou: full snapshot (custos antigos com MAT_DTAL anterior a 2025 também).
  // Depois: incremental por MAT_DTAL com janela de 1 dia.
  const ultimo = await getEstado('cd_custoprod_ultimo_dtal');
  const filtroData = ultimo
    ? `AND MAT_DTAL >= '${new Date(new Date(ultimo).getTime() - 86400000).toISOString().slice(0, 10)}'`
    : '';
  const rows = await paginarQuery(
    `SELECT PRO_CODI, PRO_PRCR, PRO_PRAD, PRO_PRCU, PRO_PRMD, MAT_DTAL
       FROM TBCUSTOPROD WITH (NOLOCK)
      WHERE EMP_CODI = '${EMP_CD}' AND LOC_CODI = '${LOC_CD}' ${filtroData}`,
    'PRO_CODI'
  );
  // Inicia com '1900-01-01' pra garantir que qualquer MAT_DTAL real > novoMax.
  let novoMax = ultimo || '1900-01-01T00:00:00.000Z';
  for (const x of rows) if (x.MAT_DTAL && new Date(x.MAT_DTAL) > new Date(novoMax)) novoMax = x.MAT_DTAL;

  if (rows.length) {
    // Bulk upsert via UNNEST (rápido pra cargas grandes).
    await dbQuery(
      `INSERT INTO cd_custoprod (pro_codi, pro_prcr, pro_prad, pro_prcu, pro_prmd, mat_dtal, sincronizado_em)
       SELECT * FROM UNNEST($1::text[], $2::numeric[], $3::numeric[], $4::numeric[], $5::numeric[], $6::timestamptz[], $7::timestamptz[])
       ON CONFLICT (pro_codi) DO UPDATE SET
         pro_prcr=EXCLUDED.pro_prcr, pro_prad=EXCLUDED.pro_prad,
         pro_prcu=EXCLUDED.pro_prcu, pro_prmd=EXCLUDED.pro_prmd,
         mat_dtal=EXCLUDED.mat_dtal, sincronizado_em=NOW()`,
      [
        rows.map(x => String(x.PRO_CODI || '').trim()),
        rows.map(x => x.PRO_PRCR ?? null),
        rows.map(x => x.PRO_PRAD ?? null),
        rows.map(x => x.PRO_PRCU ?? null),
        rows.map(x => x.PRO_PRMD ?? null),
        rows.map(x => x.MAT_DTAL),
        rows.map(() => new Date().toISOString()),
      ]
    );
  }
  await setEstado('cd_custoprod_ultimo_dtal', novoMax);
  await setEstado('cd_custoprod_ultima_sync', new Date().toISOString());
  return { tabela: 'cd_custoprod', linhas: rows.length, ms: Date.now() - t0 };
}

// ── 3) cd_vendapro (incremental por TAB_DTAL — pega Tab.4 etc) ────────

async function syncVendaPro() {
  const t0 = Date.now();
  // Mesmo padrão de syncCustoProd: full na primeira sync, incremental depois.
  const ultimo = await getEstado('cd_vendapro_ultimo_dtal');
  const filtroData = ultimo
    ? `AND TAB_DTAL >= '${new Date(new Date(ultimo).getTime() - 86400000).toISOString().slice(0, 10)}'`
    : '';
  const rows = await paginarQuery(
    `SELECT PRO_CODI, TAB_PRC1, TAB_PRC2, TAB_PRC3, TAB_PRC4, TAB_DTAL
       FROM TBVENDAPRO WITH (NOLOCK)
      WHERE EMP_CODI = '${EMP_CD}' AND LOC_CODI = '${LOC_CD}' ${filtroData}`,
    'PRO_CODI'
  );
  let novoMax = ultimo || '1900-01-01T00:00:00.000Z';
  for (const x of rows) if (x.TAB_DTAL && new Date(x.TAB_DTAL) > new Date(novoMax)) novoMax = x.TAB_DTAL;

  if (rows.length) {
    await dbQuery(
      `INSERT INTO cd_vendapro (pro_codi, tab_prc1, tab_prc2, tab_prc3, tab_prc4, tab_dtal, sincronizado_em)
       SELECT * FROM UNNEST($1::text[], $2::numeric[], $3::numeric[], $4::numeric[], $5::numeric[], $6::timestamptz[], $7::timestamptz[])
       ON CONFLICT (pro_codi) DO UPDATE SET
         tab_prc1=EXCLUDED.tab_prc1, tab_prc2=EXCLUDED.tab_prc2,
         tab_prc3=EXCLUDED.tab_prc3, tab_prc4=EXCLUDED.tab_prc4,
         tab_dtal=EXCLUDED.tab_dtal, sincronizado_em=NOW()`,
      [
        rows.map(x => String(x.PRO_CODI || '').trim()),
        rows.map(x => x.TAB_PRC1 ?? null),
        rows.map(x => x.TAB_PRC2 ?? null),
        rows.map(x => x.TAB_PRC3 ?? null),
        rows.map(x => x.TAB_PRC4 ?? null),
        rows.map(x => x.TAB_DTAL),
        rows.map(() => new Date().toISOString()),
      ]
    );
  }
  await setEstado('cd_vendapro_ultimo_dtal', novoMax);
  await setEstado('cd_vendapro_ultima_sync', new Date().toISOString());
  return { tabela: 'cd_vendapro', linhas: rows.length, ms: Date.now() - t0 };
}

// ── 4) cd_movcompra + cd_itemcompra (incremental por MCP_DTEN) ────────

async function syncCompras() {
  const t0 = Date.now();
  const ultimo = await getEstado('cd_movcompra_ultimo_dten') || `${DATA_INICIAL}T00:00:00`;
  // Janela de 1 dia cobre atrasos de digitação no ERP.
  const desde = new Date(new Date(ultimo).getTime() - 86400000).toISOString().slice(0, 10);
  const movsRaw = await paginarQuery(
    `SELECT MCP_CODI, MCP_TIPOMOV, MCP_DTEN, MCP_DTEM, FOR_CODI, NOP_CODI,
            MCP_VTOT, MCP_NNOTAFIS, MCP_STATUS
       FROM TBMOVCOMPRA WITH (NOLOCK)
      WHERE EMP_CODI = '${EMP_CD}'
        AND MCP_DTEN >= '${desde}'`,
    'MCP_CODI, MCP_TIPOMOV'
  );
  // Dedup por (MCP_CODI, MCP_TIPOMOV) — pode haver duplicatas no SQL Server quando
  // o ERP abre/fecha edição. Mantém o último (maior MCP_DTEN).
  const movMap = new Map();
  for (const m of movsRaw) {
    const k = `${String(m.MCP_CODI).trim()}|${String(m.MCP_TIPOMOV || '').trim()}`;
    const prev = movMap.get(k);
    if (!prev || (m.MCP_DTEN && m.MCP_DTEN >= prev.MCP_DTEN)) movMap.set(k, m);
  }
  const movs = [...movMap.values()];
  let novoMax = ultimo;
  for (const m of movs) if (m.MCP_DTEN && m.MCP_DTEN > novoMax) novoMax = m.MCP_DTEN;

  if (movs.length) {
    await dbQuery(
      `INSERT INTO cd_movcompra (mcp_codi, mcp_tipomov, mcp_dten, mcp_dtem, for_codi, nop_codi,
                                  mcp_vtot, mcp_nnotafis, mcp_status, sincronizado_em)
       SELECT * FROM UNNEST($1::text[], $2::text[], $3::timestamptz[], $4::timestamptz[],
                            $5::text[], $6::text[], $7::numeric[], $8::text[], $9::text[], $10::timestamptz[])
       ON CONFLICT (mcp_codi, mcp_tipomov) DO UPDATE SET
         mcp_dten=EXCLUDED.mcp_dten, for_codi=EXCLUDED.for_codi,
         mcp_vtot=EXCLUDED.mcp_vtot, mcp_status=EXCLUDED.mcp_status,
         sincronizado_em=NOW()`,
      [
        movs.map(m => String(m.MCP_CODI || '').trim()),
        movs.map(m => String(m.MCP_TIPOMOV || '').trim()),
        movs.map(m => m.MCP_DTEN),
        movs.map(m => m.MCP_DTEM),
        movs.map(m => String(m.FOR_CODI || '').trim()),
        movs.map(m => String(m.NOP_CODI || '').trim()),
        movs.map(m => m.MCP_VTOT ?? null),
        movs.map(m => String(m.MCP_NNOTAFIS || '').trim()),
        movs.map(m => String(m.MCP_STATUS || '').trim()),
        movs.map(() => new Date().toISOString()),
      ]
    );
  }

  // Itens das compras trazidas. IN com 13k codigos pode estourar — busca em chunks.
  let itens = [];
  if (movs.length) {
    const CHUNK = 500;
    for (let i = 0; i < movs.length; i += CHUNK) {
      const lote = movs.slice(i, i + CHUNK);
      const codigos = lote.map(m => `'${String(m.MCP_CODI).trim()}'`).join(',');
      // Pode passar de 5000 itens — pagina dentro de cada lote.
      const lotePag = await paginarQuery(
        `SELECT MCP_CODI, MCP_TIPOMOV, MCP_SEQITEM, PRO_CODI, MCP_VUNI, ITE_QUANINV, MCP_QUAN, EAN_CODI
           FROM TBITEMCOMPRA WITH (NOLOCK)
          WHERE EMP_CODI = '${EMP_CD}' AND MCP_CODI IN (${codigos})`,
        'MCP_CODI, MCP_TIPOMOV, MCP_SEQITEM'
      );
      itens.push(...lotePag);
    }
    // Dedup por PK composta
    const itMap = new Map();
    for (const it of itens) {
      const k = `${String(it.MCP_CODI).trim()}|${String(it.MCP_TIPOMOV || '').trim()}|${parseInt(it.MCP_SEQITEM) || 0}`;
      itMap.set(k, it);
    }
    itens = [...itMap.values()];
    if (itens.length) {
      await dbQuery(
        `INSERT INTO cd_itemcompra (mcp_codi, mcp_tipomov, mcp_seqitem, pro_codi, ean_codi,
                                    mcp_vuni, ite_quaninv, mcp_quan, sincronizado_em)
         SELECT * FROM UNNEST($1::text[], $2::text[], $3::int[], $4::text[], $5::text[],
                              $6::numeric[], $7::numeric[], $8::numeric[], $9::timestamptz[])
         ON CONFLICT (mcp_codi, mcp_tipomov, mcp_seqitem) DO UPDATE SET
           pro_codi=EXCLUDED.pro_codi, ean_codi=EXCLUDED.ean_codi,
           mcp_vuni=EXCLUDED.mcp_vuni, ite_quaninv=EXCLUDED.ite_quaninv,
           mcp_quan=EXCLUDED.mcp_quan, sincronizado_em=NOW()`,
        [
          itens.map(it => String(it.MCP_CODI || '').trim()),
          itens.map(it => String(it.MCP_TIPOMOV || '').trim()),
          itens.map(it => parseInt(it.MCP_SEQITEM) || 0),
          itens.map(it => String(it.PRO_CODI || '').trim()),
          itens.map(it => String(it.EAN_CODI || '').trim() || null),
          itens.map(it => it.MCP_VUNI ?? null),
          itens.map(it => it.ITE_QUANINV ?? null),
          itens.map(it => it.MCP_QUAN ?? null),
          itens.map(() => new Date().toISOString()),
        ]
      );
    }
  }

  await setEstado('cd_movcompra_ultimo_dten', novoMax);
  await setEstado('cd_compras_ultima_sync', new Date().toISOString());
  return { tabela: 'cd_movcompra+itens', linhas: movs.length, itens: itens.length, ms: Date.now() - t0 };
}

// ── Master ────────────────────────────────────────────────────────────

async function syncCdAll({ pular = [] } = {}) {
  const resultados = [];
  const tarefas = [
    { nome: 'material',  fn: syncMaterial },
    { nome: 'estoque',   fn: syncEstoque },
    { nome: 'custoprod', fn: syncCustoProd },
    { nome: 'vendapro',  fn: syncVendaPro },
    { nome: 'compras',   fn: syncCompras },
  ];
  for (const t of tarefas) {
    if (pular.includes(t.nome)) continue;
    try {
      const r = await t.fn();
      resultados.push({ ...r, ok: true });
    } catch (e) {
      resultados.push({ tabela: t.nome, ok: false, erro: e.message });
      console.error(`[sync_cd ${t.nome}] falha:`, e.message);
    }
  }
  await setEstado('cd_full_ultima_sync', new Date().toISOString());
  return resultados;
}

module.exports = {
  syncCdAll,
  syncMaterial,
  syncEstoque,
  syncCustoProd,
  syncVendaPro,
  syncCompras,
};
