// Sync das tabelas espelho do CD (UltraSyst SQL Server) → Postgres.
// Roda em LOOP por todos os CDs ativos da tabela `cds`.
// Cada CD tem seu próprio relay HTTP + banco SQL Server.
//
// Tabelas espelho (com cd_codigo na PK composta):
//   cd_material    — MATERIAL (cadastro de produtos, snapshot full)
//   cd_estoque     — ESTOQUE (snapshot full por loc_codi do CD)
//   cd_custoprod   — TBCUSTOPROD (incremental por MAT_DTAL)
//   cd_vendapro    — TBVENDAPRO (incremental por TAB_DTAL)
//   cd_movcompra   — TBMOVCOMPRA (incremental por MCP_DTEN)
//   cd_itemcompra  — TBITEMCOMPRA (acompanha movcompra)
//
// Estado por CD em _sync_state: cd_<codigo>_<recurso>_ultima_sync / _ultimo_dtal / _ultimo_dten

const { query: dbQuery } = require('./db');
const { listarCds, cliente: clienteCd } = require('./cds');

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

// Paginação — relay trunca em 5000 linhas. Usa OFFSET/FETCH do SQL Server.
async function paginarQuery(cli, sqlBase, orderBy, pageSize = 5000) {
  const all = [];
  let offset = 0;
  while (true) {
    const r = await cli.query(
      `${sqlBase} ORDER BY ${orderBy} OFFSET ${offset} ROWS FETCH NEXT ${pageSize} ROWS ONLY`
    );
    const rows = r.rows || [];
    all.push(...rows);
    if (rows.length < pageSize) break;
    offset += pageSize;
  }
  return all;
}

// ── 0) cd_material (snapshot full) ───────────────────────────────────

async function syncMaterial(cd, cli) {
  const t0 = Date.now();
  // Tenta combinações em ordem (com mais → com menos colunas de peso)
  // Nome real descoberto via /columns/MATERIAL: MAT_PESO + MAT_PESOBR
  const tentativas = [
    'MAT_CODI, MAT_DESC, MAT_REFE, MAT_SITU, EAN_CODI, MAT_PESO AS MAT_PSLI, MAT_PESOBR AS MAT_PSBR, GRU_CODI, SGR_CODI',
    'MAT_CODI, MAT_DESC, MAT_REFE, MAT_SITU, EAN_CODI, MAT_PESO AS MAT_PSLI, GRU_CODI, SGR_CODI',
    'MAT_CODI, MAT_DESC, MAT_REFE, MAT_SITU, EAN_CODI, GRU_CODI, SGR_CODI',
    'MAT_CODI, MAT_DESC, MAT_REFE, MAT_SITU, EAN_CODI, GRU_CODI',
    'MAT_CODI, MAT_DESC, MAT_REFE, MAT_SITU, EAN_CODI', // sem peso e sem grupo
  ];
  let rows = null, ultimoErro = null;
  for (const sel of tentativas) {
    try {
      rows = await paginarQuery(cli,
        `SELECT ${sel} FROM MATERIAL WITH (NOLOCK)`,
        'MAT_CODI'
      );
      break;
    } catch (e) {
      ultimoErro = e;
      if (!/Invalid column name|MAT_PS/i.test(e.message)) throw e;
    }
  }
  if (!rows) throw ultimoErro || new Error('Falha em todas as tentativas de SELECT MATERIAL');
  if (rows.length) {
    await dbQuery(
      `INSERT INTO cd_material (cd_codigo, mat_codi, mat_desc, mat_refe, mat_situ, ean_codi,
                                peso_liquido_kg, peso_bruto_kg, gru_codi, sgr_codi, sincronizado_em)
       SELECT $1, * FROM UNNEST($2::text[], $3::text[], $4::text[], $5::text[], $6::text[],
                                $7::numeric[], $8::numeric[], $9::text[], $10::text[], $11::timestamptz[])
       ON CONFLICT (cd_codigo, mat_codi) DO UPDATE SET
         mat_desc=EXCLUDED.mat_desc, mat_refe=EXCLUDED.mat_refe,
         mat_situ=EXCLUDED.mat_situ, ean_codi=EXCLUDED.ean_codi,
         peso_liquido_kg=COALESCE(EXCLUDED.peso_liquido_kg, cd_material.peso_liquido_kg),
         peso_bruto_kg=COALESCE(EXCLUDED.peso_bruto_kg, cd_material.peso_bruto_kg),
         gru_codi=COALESCE(EXCLUDED.gru_codi, cd_material.gru_codi),
         sgr_codi=COALESCE(EXCLUDED.sgr_codi, cd_material.sgr_codi),
         sincronizado_em=NOW()`,
      [
        cd.codigo,
        rows.map(x => String(x.MAT_CODI || '').trim()),
        rows.map(x => String(x.MAT_DESC || '').trim() || null),
        rows.map(x => String(x.MAT_REFE || '').trim() || null),
        rows.map(x => String(x.MAT_SITU || '').trim() || null),
        rows.map(x => String(x.EAN_CODI || '').trim() || null),
        rows.map(x => 'MAT_PSLI' in x && x.MAT_PSLI != null ? Number(x.MAT_PSLI) : null),
        rows.map(x => 'MAT_PSBR' in x && x.MAT_PSBR != null ? Number(x.MAT_PSBR) : null),
        rows.map(x => 'GRU_CODI' in x && x.GRU_CODI != null ? String(x.GRU_CODI).trim() || null : null),
        rows.map(x => 'SGR_CODI' in x && x.SGR_CODI != null ? String(x.SGR_CODI).trim() || null : null),
        rows.map(() => new Date().toISOString()),
      ]
    );
  }
  await setEstado(`cd_${cd.codigo}_material_ultima_sync`, new Date().toISOString());
  return { tabela: 'material', linhas: rows.length, ms: Date.now() - t0 };
}

// ── 0d) cli_codi cache (busca cli_codi de cada destino no banco do CD origem) ──
// Pra calcular trânsito CD→CD (cd_movcompra do CD origem com for_codi=cli_codi do destino).

async function syncCliCodisDestinos(cd, cli) {
  const t0 = Date.now();
  // Pega os CNPJs dos destinos (lojas + outros CDs)
  const destinos = await dbQuery(
    `SELECT DISTINCT cnpj FROM pedidos_distrib_destinos
      WHERE ativo=TRUE AND cnpj IS NOT NULL AND cnpj <> ''
        AND cnpj <> (SELECT cnpj FROM pedidos_distrib_destinos WHERE cd_codigo=$1 LIMIT 1)`,
    [cd.codigo]
  );
  let achados = 0, naoAchados = 0;
  for (const d of destinos) {
    try {
      const r = await cli.query(
        `SELECT TOP 1 CLI_CODI, CLI_RAZS FROM CLIENTE WITH (NOLOCK)
          WHERE REPLACE(REPLACE(REPLACE(CLI_CPF,'.',''),'/',''),'-','') = '${d.cnpj}'`
      );
      const row = r.rows?.[0];
      if (row?.CLI_CODI) {
        await dbQuery(
          `INSERT INTO pedidos_distrib_cli_codi (cd_origem_codigo, cnpj_destino, cli_codi, cli_razs)
           VALUES ($1,$2,$3,$4)
           ON CONFLICT (cd_origem_codigo, cnpj_destino) DO UPDATE SET
             cli_codi=EXCLUDED.cli_codi, cli_razs=EXCLUDED.cli_razs, atualizado_em=NOW()`,
          [cd.codigo, d.cnpj, String(row.CLI_CODI).trim(), String(row.CLI_RAZS || '').trim() || null]
        );
        achados++;
      } else {
        naoAchados++;
      }
    } catch (e) { naoAchados++; }
  }
  await setEstado(`cd_${cd.codigo}_cli_codi_ultima_sync`, new Date().toISOString());
  return { tabela: 'cli_codi', linhas: achados, nao_achados: naoAchados, ms: Date.now() - t0 };
}

// ── 0c) cd_grupo + cd_subgrupo (TBGRUPO + TBSUBGRUPO do UltraSyst) ─────

async function syncGrupos(cd, cli) {
  const t0 = Date.now();
  const rows = await paginarQuery(cli,
    `SELECT GRU_CODI, GRU_DESC, GRU_GIRO, GRU_STSUP FROM TBGRUPO WITH (NOLOCK)`,
    'GRU_CODI'
  );
  await dbQuery(`DELETE FROM cd_grupo WHERE cd_codigo = $1`, [cd.codigo]);
  if (rows.length) {
    await dbQuery(
      `INSERT INTO cd_grupo (cd_codigo, gru_codi, gru_desc, gru_giro, gru_stsup)
       SELECT $1, * FROM UNNEST($2::text[], $3::text[], $4::numeric[], $5::text[])`,
      [
        cd.codigo,
        rows.map(x => String(x.GRU_CODI || '').trim()),
        rows.map(x => String(x.GRU_DESC || '').trim() || null),
        rows.map(x => x.GRU_GIRO != null ? Number(x.GRU_GIRO) : null),
        rows.map(x => String(x.GRU_STSUP || '').trim() || null),
      ]
    );
  }
  await setEstado(`cd_${cd.codigo}_grupo_ultima_sync`, new Date().toISOString());
  return { tabela: 'grupo', linhas: rows.length, ms: Date.now() - t0 };
}

async function syncSubgrupos(cd, cli) {
  const t0 = Date.now();
  const rows = await paginarQuery(cli,
    `SELECT SGR_CODI, GRU_CODI, SGR_DESC, SGR_GIRO, SGR_STSUP FROM TBSUBGRUPO WITH (NOLOCK)`,
    'GRU_CODI, SGR_CODI'
  );
  await dbQuery(`DELETE FROM cd_subgrupo WHERE cd_codigo = $1`, [cd.codigo]);
  if (rows.length) {
    // Dedup
    const seen = new Set();
    const uniq = rows.filter(x => {
      const k = `${String(x.GRU_CODI||'').trim()}|${String(x.SGR_CODI||'').trim()}`;
      if (seen.has(k)) return false;
      seen.add(k); return true;
    });
    await dbQuery(
      `INSERT INTO cd_subgrupo (cd_codigo, gru_codi, sgr_codi, sgr_desc, sgr_giro, sgr_stsup)
       SELECT $1, * FROM UNNEST($2::text[], $3::text[], $4::text[], $5::numeric[], $6::text[])`,
      [
        cd.codigo,
        uniq.map(x => String(x.GRU_CODI || '').trim()),
        uniq.map(x => String(x.SGR_CODI || '').trim()),
        uniq.map(x => String(x.SGR_DESC || '').trim() || null),
        uniq.map(x => x.SGR_GIRO != null ? Number(x.SGR_GIRO) : null),
        uniq.map(x => String(x.SGR_STSUP || '').trim() || null),
      ]
    );
  }
  await setEstado(`cd_${cd.codigo}_subgrupo_ultima_sync`, new Date().toISOString());
  return { tabela: 'subgrupo', linhas: rows.length, ms: Date.now() - t0 };
}

// ── 0b) cd_ean (codigos de barra por produto — tabela EAN do UltraSyst) ─

async function syncEan(cd, cli) {
  const t0 = Date.now();
  const rows = await paginarQuery(cli,
    `SELECT MAT_CODI, EAN_CODI, EAN_NOTA, ID
       FROM EAN WITH (NOLOCK)
      WHERE EAN_CODI IS NOT NULL AND LTRIM(RTRIM(EAN_CODI)) <> ''`,
    'MAT_CODI, ID'
  );
  // Snapshot: deleta tudo desse CD e reinsere
  await dbQuery(`DELETE FROM cd_ean WHERE cd_codigo = $1`, [cd.codigo]);
  if (rows.length) {
    // Dedup por (mat_codi, ean_codi) — pode ter duplicatas no SQL Server
    const seen = new Set();
    const uniq = [];
    for (const r of rows) {
      const m = String(r.MAT_CODI || '').trim();
      const e = String(r.EAN_CODI || '').trim();
      if (!m || !e) continue;
      const k = `${m}|${e}`;
      if (seen.has(k)) continue;
      seen.add(k);
      uniq.push({ ...r, _m: m, _e: e });
    }
    if (uniq.length) {
      await dbQuery(
        `INSERT INTO cd_ean (cd_codigo, mat_codi, ean_codi, ean_nota, ordem)
         SELECT $1, * FROM UNNEST($2::text[], $3::text[], $4::text[], $5::int[])`,
        [
          cd.codigo,
          uniq.map(r => r._m),
          uniq.map(r => r._e),
          uniq.map(r => String(r.EAN_NOTA || 'N').trim().charAt(0) || 'N'),
          uniq.map(r => parseInt(r.ID) || 0),
        ]
      );
    }
  }
  await setEstado(`cd_${cd.codigo}_ean_ultima_sync`, new Date().toISOString());
  return { tabela: 'ean', linhas: rows.length, ms: Date.now() - t0 };
}

// ── 1) cd_estoque (snapshot full por LOC_CODI do CD) ─────────────────

async function syncEstoque(cd, cli) {
  const t0 = Date.now();
  const rows = await paginarQuery(cli,
    `SELECT PRO_CODI, EST_QUAN, TAM_CODI
       FROM ESTOQUE WITH (NOLOCK)
      WHERE EMP_CODI = '${cd.emp_codi}' AND LOC_CODI = '${cd.loc_codi}'`,
    'PRO_CODI'
  );
  // Snapshot: trunca só esse CD e reinsere.
  await dbQuery(`DELETE FROM cd_estoque WHERE cd_codigo = $1`, [cd.codigo]);
  if (rows.length) {
    await dbQuery(
      `INSERT INTO cd_estoque (cd_codigo, pro_codi, est_quan, tam_codi)
       SELECT $1, * FROM UNNEST($2::text[], $3::numeric[], $4::text[])`,
      [
        cd.codigo,
        rows.map(x => String(x.PRO_CODI || '').trim()),
        rows.map(x => x.EST_QUAN == null ? '0' : String(x.EST_QUAN)),
        rows.map(x => String(x.TAM_CODI || '').trim()),
      ]
    );
  }
  await setEstado(`cd_${cd.codigo}_estoque_ultima_sync`, new Date().toISOString());
  return { tabela: 'estoque', linhas: rows.length, ms: Date.now() - t0 };
}

// ── 2) cd_custoprod (incremental por MAT_DTAL) ───────────────────────

async function syncCustoProd(cd, cli) {
  const t0 = Date.now();
  const ultimo = await getEstado(`cd_${cd.codigo}_custoprod_ultimo_dtal`);
  const filtroData = ultimo
    ? `AND MAT_DTAL >= '${new Date(new Date(ultimo).getTime() - 86400000).toISOString().slice(0, 10)}'`
    : '';
  const rows = await paginarQuery(cli,
    `SELECT PRO_CODI, PRO_PRCR, PRO_PRAD, PRO_PRCU, PRO_PRMD, MAT_DTAL
       FROM TBCUSTOPROD WITH (NOLOCK)
      WHERE EMP_CODI = '${cd.emp_codi}' AND LOC_CODI = '${cd.loc_codi}' ${filtroData}`,
    'PRO_CODI'
  );
  let novoMax = ultimo || '1900-01-01T00:00:00.000Z';
  for (const x of rows) if (x.MAT_DTAL && new Date(x.MAT_DTAL) > new Date(novoMax)) novoMax = x.MAT_DTAL;

  if (rows.length) {
    await dbQuery(
      `INSERT INTO cd_custoprod (cd_codigo, pro_codi, pro_prcr, pro_prad, pro_prcu, pro_prmd, mat_dtal, sincronizado_em)
       SELECT $1, * FROM UNNEST($2::text[], $3::numeric[], $4::numeric[], $5::numeric[], $6::numeric[], $7::timestamptz[], $8::timestamptz[])
       ON CONFLICT (cd_codigo, pro_codi) DO UPDATE SET
         pro_prcr=EXCLUDED.pro_prcr, pro_prad=EXCLUDED.pro_prad,
         pro_prcu=EXCLUDED.pro_prcu, pro_prmd=EXCLUDED.pro_prmd,
         mat_dtal=EXCLUDED.mat_dtal, sincronizado_em=NOW()`,
      [
        cd.codigo,
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
  await setEstado(`cd_${cd.codigo}_custoprod_ultimo_dtal`, novoMax);
  await setEstado(`cd_${cd.codigo}_custoprod_ultima_sync`, new Date().toISOString());
  return { tabela: 'custoprod', linhas: rows.length, ms: Date.now() - t0 };
}

// ── 3) cd_vendapro (incremental por TAB_DTAL) ────────────────────────

async function syncVendaPro(cd, cli) {
  const t0 = Date.now();
  const ultimo = await getEstado(`cd_${cd.codigo}_vendapro_ultimo_dtal`);
  const filtroData = ultimo
    ? `AND TAB_DTAL >= '${new Date(new Date(ultimo).getTime() - 86400000).toISOString().slice(0, 10)}'`
    : '';
  const rows = await paginarQuery(cli,
    `SELECT PRO_CODI, TAB_PRC1, TAB_PRC2, TAB_PRC3, TAB_PRC4, TAB_DTAL
       FROM TBVENDAPRO WITH (NOLOCK)
      WHERE EMP_CODI = '${cd.emp_codi}' AND LOC_CODI = '${cd.loc_codi}' ${filtroData}`,
    'PRO_CODI'
  );
  let novoMax = ultimo || '1900-01-01T00:00:00.000Z';
  for (const x of rows) if (x.TAB_DTAL && new Date(x.TAB_DTAL) > new Date(novoMax)) novoMax = x.TAB_DTAL;

  if (rows.length) {
    await dbQuery(
      `INSERT INTO cd_vendapro (cd_codigo, pro_codi, tab_prc1, tab_prc2, tab_prc3, tab_prc4, tab_dtal, sincronizado_em)
       SELECT $1, * FROM UNNEST($2::text[], $3::numeric[], $4::numeric[], $5::numeric[], $6::numeric[], $7::timestamptz[], $8::timestamptz[])
       ON CONFLICT (cd_codigo, pro_codi) DO UPDATE SET
         tab_prc1=EXCLUDED.tab_prc1, tab_prc2=EXCLUDED.tab_prc2,
         tab_prc3=EXCLUDED.tab_prc3, tab_prc4=EXCLUDED.tab_prc4,
         tab_dtal=EXCLUDED.tab_dtal, sincronizado_em=NOW()`,
      [
        cd.codigo,
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
  await setEstado(`cd_${cd.codigo}_vendapro_ultimo_dtal`, novoMax);
  await setEstado(`cd_${cd.codigo}_vendapro_ultima_sync`, new Date().toISOString());
  return { tabela: 'vendapro', linhas: rows.length, ms: Date.now() - t0 };
}

// ── 4) cd_movcompra + cd_itemcompra (incremental por MCP_DTEN) ───────

async function syncCompras(cd, cli) {
  const t0 = Date.now();
  const ultimo = await getEstado(`cd_${cd.codigo}_movcompra_ultimo_dten`) || `${DATA_INICIAL}T00:00:00`;
  const desde = new Date(new Date(ultimo).getTime() - 86400000).toISOString().slice(0, 10);
  const movsRaw = await paginarQuery(cli,
    `SELECT MCP_CODI, MCP_TIPOMOV, MCP_DTEN, MCP_DTEM, FOR_CODI, NOP_CODI,
            MCP_VTOT, MCP_NNOTAFIS, MCP_STATUS
       FROM TBMOVCOMPRA WITH (NOLOCK)
      WHERE EMP_CODI = '${cd.emp_codi}'
        AND MCP_DTEN >= '${desde}'`,
    'MCP_CODI, MCP_TIPOMOV'
  );
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
      `INSERT INTO cd_movcompra (cd_codigo, mcp_codi, mcp_tipomov, mcp_dten, mcp_dtem, for_codi, nop_codi,
                                  mcp_vtot, mcp_nnotafis, mcp_status, sincronizado_em)
       SELECT $1, * FROM UNNEST($2::text[], $3::text[], $4::timestamptz[], $5::timestamptz[],
                                 $6::text[], $7::text[], $8::numeric[], $9::text[], $10::text[], $11::timestamptz[])
       ON CONFLICT (cd_codigo, mcp_codi, mcp_tipomov) DO UPDATE SET
         mcp_dten=EXCLUDED.mcp_dten, for_codi=EXCLUDED.for_codi,
         mcp_vtot=EXCLUDED.mcp_vtot, mcp_status=EXCLUDED.mcp_status,
         sincronizado_em=NOW()`,
      [
        cd.codigo,
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

  let itens = [];
  if (movs.length) {
    const CHUNK = 500;
    for (let i = 0; i < movs.length; i += CHUNK) {
      const lote = movs.slice(i, i + CHUNK);
      const codigos = lote.map(m => `'${String(m.MCP_CODI).trim()}'`).join(',');
      const lotePag = await paginarQuery(cli,
        `SELECT MCP_CODI, MCP_TIPOMOV, MCP_SEQITEM, PRO_CODI, MCP_VUNI, ITE_QUANINV, MCP_QUAN, EAN_CODI
           FROM TBITEMCOMPRA WITH (NOLOCK)
          WHERE EMP_CODI = '${cd.emp_codi}' AND MCP_CODI IN (${codigos})`,
        'MCP_CODI, MCP_TIPOMOV, MCP_SEQITEM'
      );
      itens.push(...lotePag);
    }
    const itMap = new Map();
    for (const it of itens) {
      const k = `${String(it.MCP_CODI).trim()}|${String(it.MCP_TIPOMOV || '').trim()}|${parseInt(it.MCP_SEQITEM) || 0}`;
      itMap.set(k, it);
    }
    itens = [...itMap.values()];
    if (itens.length) {
      await dbQuery(
        `INSERT INTO cd_itemcompra (cd_codigo, mcp_codi, mcp_tipomov, mcp_seqitem, pro_codi, ean_codi,
                                    mcp_vuni, ite_quaninv, mcp_quan, sincronizado_em)
         SELECT $1, * FROM UNNEST($2::text[], $3::text[], $4::int[], $5::text[], $6::text[],
                                  $7::numeric[], $8::numeric[], $9::numeric[], $10::timestamptz[])
         ON CONFLICT (cd_codigo, mcp_codi, mcp_tipomov, mcp_seqitem) DO UPDATE SET
           pro_codi=EXCLUDED.pro_codi, ean_codi=EXCLUDED.ean_codi,
           mcp_vuni=EXCLUDED.mcp_vuni, ite_quaninv=EXCLUDED.ite_quaninv,
           mcp_quan=EXCLUDED.mcp_quan, sincronizado_em=NOW()`,
        [
          cd.codigo,
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

  await setEstado(`cd_${cd.codigo}_movcompra_ultimo_dten`, novoMax);
  await setEstado(`cd_${cd.codigo}_compras_ultima_sync`, new Date().toISOString());
  return { tabela: 'compras', linhas: movs.length, itens: itens.length, ms: Date.now() - t0 };
}

// ── Master ────────────────────────────────────────────────────────────

// Sync de UM CD
async function syncCd(cd) {
  const cli = clienteCd(cd);
  const tarefas = [
    { nome: 'material',  fn: syncMaterial },
    { nome: 'ean',       fn: syncEan },
    { nome: 'grupo',     fn: syncGrupos },
    { nome: 'subgrupo',  fn: syncSubgrupos },
    { nome: 'cli_codi',  fn: syncCliCodisDestinos },
    { nome: 'estoque',   fn: syncEstoque },
    { nome: 'custoprod', fn: syncCustoProd },
    { nome: 'vendapro',  fn: syncVendaPro },
    { nome: 'compras',   fn: syncCompras },
  ];
  const resultados = [];
  for (const t of tarefas) {
    try {
      const r = await t.fn(cd, cli);
      resultados.push({ ...r, ok: true });
    } catch (e) {
      resultados.push({ tabela: t.nome, ok: false, erro: e.message });
      console.error(`[sync_cd ${cd.codigo} ${t.nome}] falha:`, e.message);
    }
  }
  return { cd: cd.codigo, resultados };
}

// Sync de TODOS os CDs ativos
async function syncCdAll({ pular = [] } = {}) {
  const cds = await listarCds(true); // só ativos
  const out = [];
  for (const cd of cds) {
    if (pular.includes(cd.codigo)) continue;
    if (!cd.url || !cd.token) {
      out.push({ cd: cd.codigo, ok: false, erro: 'sem url/token' });
      continue;
    }
    try {
      const r = await syncCd(cd);
      out.push(r);
    } catch (e) {
      console.error(`[sync_cd ${cd.codigo}] fatal:`, e.message);
      out.push({ cd: cd.codigo, ok: false, erro: e.message });
    }
  }
  await setEstado('cd_full_ultima_sync', new Date().toISOString());
  return out;
}

module.exports = {
  syncCd,
  syncCdAll,
  // exports individuais (testes/debug)
  syncMaterial,
  syncEan,
  syncGrupos,
  syncSubgrupos,
  syncEstoque,
  syncCustoProd,
  syncVendaPro,
  syncCompras,
};
