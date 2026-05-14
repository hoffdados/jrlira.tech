// Pedidos pra Distribuidora — Diretor/CEO emite pedido que vira CSV pro importador do UltraSyst.
//
// Modelo:
// - 6 LOJAS (SUPERASA) recebem; 5 CDs (ASA BRANCA, ASA FRIOS, CASA BRANCA) emitem e podem receber entre si.
// - Operador escolhe o CD ORIGEM (de onde sai a mercadoria) + destinos (lojas e/ou outros CDs).
// - CLI_CODI de cada destino é descoberto via relay do CD origem (CLIENTE WHERE CLI_CPF = cnpj).
// - Backend gera 2 CSVs (P_PEDIDOS.csv + P_PEDIDOS_ITENS.csv) no formato oficial.
// - Operador baixa e cola na pasta de importação do UltraSyst do CD origem.

const express = require('express');
const router = express.Router();
const { query: dbQuery } = require('../db');
const { autenticar, exigirPerfil } = require('../auth');
const { listarCds, clientePorCodigo } = require('../cds');

const adminOuCeo = [autenticar, exigirPerfil('admin', 'ceo')];

// Regras de transferência entre CDs (quem pode mandar pra quem)
// Lojas sempre são destino válido pra qualquer CD origem.
// ASA FRIOS não envia pra ASA BRANCA (nem ITAITUBA nem N_PROGRESSO) — só pra outro ASA FRIOS.
// CASA BRANCA é fornecedora interna (uso/consumo) — só envia, nunca recebe transferência.
const REGRAS_CD_DESTINOS = {
  'srv1-itautuba':    ['srv1-nprogresso', 'srv2-asafrio', 'srv2-asasantarem'],
  'srv1-nprogresso':  ['srv1-itautuba',   'srv2-asafrio', 'srv2-asasantarem'],
  'srv2-asafrio':     ['srv2-asasantarem'],
  'srv2-asasantarem': ['srv2-asafrio'],
  'srv3-casabranca':  ['srv1-itautuba', 'srv1-nprogresso', 'srv2-asafrio', 'srv2-asasantarem'],
};

// Defaults do CSV de exemplo
const EMPRESA_PADRAO        = '1';
const LOCALIZACAO_PADRAO    = '1';
const VEN_CODI_PADRAO       = '19';   // IMPORTADOR
const COD_CONDICAO_PADRAO   = '2';
const COD_PAGAMENTO_PADRAO  = '9';
const COD_TIPOVENDA_PADRAO  = '12';
const COD_TABELA_PADRAO     = '1';
const TIPO_CALCULO_PADRAO   = 'E';
const NOME_EMBALAGEM_PADRAO = 'EMB';
const UNIDADE_PADRAO        = 'CX';

// ── Destinos (lojas + CDs) ──

router.get('/destinos', autenticar, async (req, res) => {
  try {
    const rows = await dbQuery(`
      SELECT id, tipo, codigo, nome, cnpj, loja_id, cd_codigo, ativo
        FROM pedidos_distrib_destinos
       WHERE ativo = TRUE
       ORDER BY
         CASE tipo WHEN 'LOJA' THEN 0 ELSE 1 END,
         CASE WHEN tipo = 'LOJA' THEN loja_id END NULLS LAST,
         CASE cd_codigo
           WHEN 'srv1-nprogresso'   THEN 1
           WHEN 'srv2-asafrio'      THEN 2
           WHEN 'srv2-asasantarem'  THEN 3
           WHEN 'srv1-itautuba'     THEN 4
           WHEN 'srv3-casabranca'   THEN 5
           ELSE 9
         END,
         nome
    `);
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// CDs origem: só os que tem pode_emitir=TRUE no destino + estão ativos em /admin-cds
router.get('/cds-origem', adminOuCeo, async (req, res) => {
  try {
    const cds = await listarCds(true);
    const destinos = await dbQuery(
      `SELECT cd_codigo, cnpj, nome FROM pedidos_distrib_destinos
        WHERE tipo='CD' AND cd_codigo IS NOT NULL AND pode_emitir = TRUE AND ativo = TRUE`
    );
    const destMap = new Map(destinos.map(d => [d.cd_codigo, d]));
    const out = cds
      .filter(c => destMap.has(c.codigo))
      .map(c => ({
        codigo: c.codigo,
        nome: destMap.get(c.codigo).nome,
        cnpj: destMap.get(c.codigo).cnpj,
        banco: c.banco,
        url: c.url,
      }));
    res.json(out);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// Diagnóstico: dado um CD origem, busca CLI_CODI de cada destino via relay
router.get('/cli-codi-lookup', adminOuCeo, async (req, res) => {
  try {
    const cdOrigem = String(req.query.cd_origem || '').trim();
    if (!cdOrigem) return res.status(400).json({ erro: 'cd_origem obrigatorio' });
    const destinos = await dbQuery(`SELECT id, tipo, codigo, nome, cnpj FROM pedidos_distrib_destinos WHERE ativo=TRUE`);
    const cli = await clientePorCodigo(cdOrigem);
    const result = [];
    for (const d of destinos) {
      try {
        const r = await cli.query(
          `SELECT TOP 1 CLI_CODI, CLI_NOME AS CLI_RAZS, CLI_CPF FROM CLIENTE WITH (NOLOCK) WHERE REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(CLI_CPF)),'.',''),'/',''),'-','') LIKE '%${String(d.cnpj).replace(/\\D/g,'')}'`
        );
        const row = r.rows?.[0];
        result.push({
          destino_id: d.id,
          tipo: d.tipo, nome: d.nome, cnpj: d.cnpj,
          cli_codi: row?.CLI_CODI || null,
          cli_razs: row?.CLI_RAZS?.trim() || null,
          ok: !!row,
        });
      } catch (e) {
        result.push({ destino_id: d.id, tipo: d.tipo, nome: d.nome, cnpj: d.cnpj, ok: false, erro: e.message });
      }
    }
    res.json({ cd_origem: cdOrigem, destinos: result });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// ── Grade completa (replica relatório do CEO no Excel/PowerBI) ──
//
// GET /grade?cd_origem=X&busca=&so_pedir=true|false&limit=
// Retorna pra cada produto do CD origem:
//   - identidade (cod, desc, ref, ean, prioridade, est_dist, preco_admin, qtd_embalagem)
//   - ranking (posição na soma de vendas R$ de todas lojas, 90d)
//   - destinos[id]: { estoque_un, estoque_cx, transito_un, transito_cx, sugestao_cx, sug_editada }
//   - vendas[loja_id]: { media_28d, preco_atual, ultima_venda }
router.get('/grade', adminOuCeo, async (req, res) => {
  const T0 = Date.now();
  const timings = {};
  const tick = (nome) => { timings[nome] = Date.now() - T0; };
  try {
    const cdOrigem = String(req.query.cd_origem || '').trim();
    if (!cdOrigem) return res.status(400).json({ erro: 'cd_origem obrigatorio' });
    const busca = (req.query.busca || '').toString().trim().toLowerCase();
    const soPedir = req.query.so_pedir === 'true';
    const limit = Math.min(parseInt(req.query.limit) || 500, 3000);
    // Ordenação: 'descricao' (default) | 'ultima_entrada' (MCP_DTEN desc) | 'ranking'
    const ordem = (req.query.ordem || 'descricao').toString().trim();
    // Filtro por sequência MCP_CODI (range inicial/final)
    const mcpDe  = req.query.mcp_de  ? parseInt(req.query.mcp_de)  : null;
    const mcpAte = req.query.mcp_ate ? parseInt(req.query.mcp_ate) : null;
    // Filtros por grupo/subgrupo
    const gruCodi = req.query.gru_codi ? String(req.query.gru_codi).trim() : null;
    const sgrCodi = req.query.sgr_codi ? String(req.query.sgr_codi).trim() : null;

    // 1) Destinos (todos exceto o CD origem mesmo)
    const [origemRow] = await dbQuery(
      `SELECT cnpj FROM pedidos_distrib_destinos WHERE cd_codigo = $1`,
      [cdOrigem]
    );
    const cnpjOrigem = origemRow?.cnpj || '';
    const cdsPermitidos = REGRAS_CD_DESTINOS[cdOrigem] || [];
    const destinos = await dbQuery(
      `SELECT id, tipo, codigo, nome, cnpj, loja_id, cd_codigo
         FROM pedidos_distrib_destinos
        WHERE ativo = TRUE
          AND cnpj <> $1
          AND (tipo = 'LOJA' OR (tipo = 'CD' AND cd_codigo = ANY($2::text[])))
        ORDER BY
          CASE tipo WHEN 'LOJA' THEN 0 ELSE 1 END,
          CASE WHEN tipo = 'LOJA' THEN loja_id END NULLS LAST,
          CASE cd_codigo
            WHEN 'srv1-nprogresso'   THEN 1
            WHEN 'srv2-asafrio'      THEN 2
            WHEN 'srv2-asasantarem'  THEN 3
            WHEN 'srv1-itautuba'     THEN 4
            WHEN 'srv3-casabranca'   THEN 5
            ELSE 9
          END,
          nome`,
      [cnpjOrigem, cdsPermitidos]
    );
    const lojaIds = destinos.filter(d => d.tipo === 'LOJA' && d.loja_id).map(d => d.loja_id);
    const cdDestinos = destinos.filter(d => d.tipo === 'CD' && d.cd_codigo);

    // 2) Catálogo do CD origem
    const params = [cdOrigem];
    let where = `cd_m.cd_codigo = $1 AND (cd_m.mat_situ = 'A' OR cd_m.mat_situ IS NULL)`;
    if (busca) {
      params.push(`%${busca}%`);
      params.push(busca.replace(/[^0-9]/g, ''));
      where += ` AND (LOWER(cd_m.mat_desc) LIKE $${params.length-1} OR cd_m.mat_codi = $${params.length} OR cd_m.ean_codi = $${params.length})`;
    }
    if (soPedir) {
      where += ` AND EXISTS (SELECT 1 FROM pedidos_distrib_quantidades q
                              WHERE q.cd_origem_codigo = $1 AND q.mat_codi = cd_m.mat_codi AND q.qtd > 0)`;
    }
    if (mcpDe != null) {
      params.push(mcpDe);
      where += ` AND uc.ultimo_mcp_codi_int >= $${params.length}`;
    }
    if (mcpAte != null) {
      params.push(mcpAte);
      where += ` AND uc.ultimo_mcp_codi_int <= $${params.length}`;
    }
    if (gruCodi) {
      params.push(gruCodi);
      where += ` AND cd_m.gru_codi = $${params.length}`;
    }
    if (sgrCodi) {
      params.push(sgrCodi);
      where += ` AND cd_m.sgr_codi = $${params.length}`;
    }
    params.push(limit);
    let orderBy = 'descricao';
    if (ordem === 'ultima_entrada') orderBy = 'ultima_entrada DESC NULLS LAST, descricao';
    if (ordem === 'ranking') orderBy = 'descricao'; // ranking só calcula depois, mantém alfabético no SQL
    const produtos = await dbQuery(`
      WITH ult_compra AS (
        SELECT i.cd_codigo, i.pro_codi, MAX(m.mcp_dten) AS ultima_entrada,
               MAX((m.mcp_codi)::int) AS ultimo_mcp_codi_int
          FROM cd_itemcompra i
          JOIN cd_movcompra  m ON m.cd_codigo = i.cd_codigo AND m.mcp_codi = i.mcp_codi AND m.mcp_tipomov = i.mcp_tipomov
         WHERE i.cd_codigo = $1 AND m.mcp_codi ~ '^[0-9]+$' AND m.mcp_tipomov = 'E'
         GROUP BY i.cd_codigo, i.pro_codi
      )
      SELECT cd_m.mat_codi,
             COALESCE(
               NULLIF(cpe_o.ean_principal,''),
               NULLIF(pe.ean_principal_cd,''),
               NULLIF(cd_m.ean_codi,''),
               (SELECT NULLIF(LTRIM(ean_codi,'0'),'') FROM cd_ean
                 WHERE cd_codigo = cd_m.cd_codigo AND mat_codi = cd_m.mat_codi
                 ORDER BY CASE WHEN ean_nota='S' THEN 0 ELSE 1 END, ordem LIMIT 1)
             ) AS ean_codi,
             cd_m.mat_desc AS descricao,
             cd_m.mat_refe AS referencia,
             cd_e.est_quan AS est_dist,
             cd_c.pro_prad AS preco_admin,
             COALESCE(cpe_o.qtd_embalagem, pe.qtd_embalagem) AS qtd_embalagem,
             cpe_o.peso_unidade_kg,
             cpe_o.peso_variavel,
             cpe_o.ean_secundario,
             uc.ultima_entrada,
             uc.ultimo_mcp_codi_int AS ultimo_mcp_codi,
             (pp.mat_codi IS NOT NULL) AS prioritario,
             cd_m.gru_codi,
             cg.gru_desc,
             cd_m.sgr_codi,
             cs.sgr_desc
        FROM cd_material cd_m
        LEFT JOIN cd_estoque   cd_e ON cd_e.cd_codigo = cd_m.cd_codigo AND cd_e.pro_codi = cd_m.mat_codi
        LEFT JOIN cd_custoprod cd_c ON cd_c.cd_codigo = cd_m.cd_codigo AND cd_c.pro_codi = cd_m.mat_codi
        LEFT JOIN produtos_embalagem pe ON pe.mat_codi = cd_m.mat_codi
        LEFT JOIN cd_produtos_embalagem cpe_o
               ON cpe_o.cd_codigo = cd_m.cd_codigo AND cpe_o.mat_codi = cd_m.mat_codi
        LEFT JOIN ult_compra uc ON uc.cd_codigo = cd_m.cd_codigo AND uc.pro_codi = cd_m.mat_codi
        LEFT JOIN pedidos_distrib_prioridades pp
          ON pp.cd_origem_codigo = cd_m.cd_codigo AND pp.mat_codi = cd_m.mat_codi
        LEFT JOIN cd_grupo    cg ON cg.cd_codigo = cd_m.cd_codigo AND cg.gru_codi = cd_m.gru_codi
        LEFT JOIN cd_subgrupo cs ON cs.cd_codigo = cd_m.cd_codigo AND cs.gru_codi = cd_m.gru_codi AND cs.sgr_codi = cd_m.sgr_codi
       WHERE ${where}
       ORDER BY ${orderBy}
       LIMIT $${params.length}
    `, params);
    tick('produtos');
    if (!produtos.length) {
      res.set('Cache-Control', 'private, max-age=60, stale-while-revalidate=120');
      return res.json({ cd_origem: cdOrigem, destinos, produtos: [] });
    }

    const eans = [...new Set(produtos.map(p => p.ean_codi).filter(Boolean))];
    const matCodis = produtos.map(p => p.mat_codi);

    // 3) Estoque + preço atual por (loja_id, ean) — produtos_externo
    // Match por codigobarra OU produtoprincipal (loja pode usar EAN alternativo apontando pro principal)
    const eansNorm = eans.map(e => String(e).replace(/^0+/, '') || e);
    const estLojas = lojaIds.length && eans.length ? await dbQuery(
      `SELECT loja_id,
              NULLIF(LTRIM(codigobarra,'0'),'') AS codbarra,
              NULLIF(LTRIM(produtoprincipal,'0'),'') AS principal,
              estdisponivel, prsugerido
         FROM produtos_externo
        WHERE loja_id = ANY($1::int[])
          AND (NULLIF(LTRIM(codigobarra,'0'),'')      = ANY($2::text[])
            OR NULLIF(LTRIM(produtoprincipal,'0'),'') = ANY($2::text[]))`,
      [lojaIds, eansNorm]
    ) : [];
    tick('estLojas');

    // 4) Vendas: media 28d, ultima_venda, qtd_total 90d (pro ranking)
    // vendas_historico não tem produtoprincipal, então usa também os codigobarra alternativos das lojas
    // que apontam pro principal. Coletamos primeiro essa lista expandida via produtos_externo.
    const eansExpandidos = new Set(eansNorm);
    for (const r of estLojas) {
      if (r.codbarra)  eansExpandidos.add(r.codbarra);
      if (r.principal) eansExpandidos.add(r.principal);
    }
    const eansList = [...eansExpandidos];

    // Lê do cache diário (vendas_loja_cache) — vendas do dia não movem média de 90d.
    // Cache populado por src/vendas_cache.js (cron 1×/dia, ver server.js).
    const vendas = lojaIds.length && eansList.length ? await dbQuery(
      `SELECT loja_id, codbarra_norm AS codbarra, qtd_28d, qtd_90d, ultima_venda
         FROM vendas_loja_cache
        WHERE loja_id = ANY($1::int[])
          AND codbarra_norm = ANY($2::text[])`,
      [lojaIds, eansList]
    ) : [];
    tick('vendas');

    // 5) Estoque dos CDs destino — match via cd_ean (CD destino) com EANs do CD origem
    // Expande pra TODOS os EANs do produto no CD origem (não só o principal)
    // já que UltraSyst aceita múltiplos EANs por produto.
    const eansExpandidosCdOrigem = eansNorm.length ? await dbQuery(
      `SELECT NULLIF(LTRIM(ce.ean_codi,'0'),'') AS ean_codi
         FROM cd_ean ce
         JOIN cd_material m
           ON m.cd_codigo = ce.cd_codigo AND m.mat_codi = ce.mat_codi
         JOIN cd_ean ce2
           ON ce2.cd_codigo = ce.cd_codigo AND ce2.mat_codi = ce.mat_codi
        WHERE ce.cd_codigo = $1
          AND NULLIF(LTRIM(ce2.ean_codi,'0'),'') = ANY($2::text[])`,
      [cdOrigem, eansNorm]
    ) : [];
    // 5-bis) Expande via produtos_externo.produtoprincipal das lojas.
    // Cada loja agrega múltiplos EANs sob o mesmo produtoprincipal — usar isso pra
    // descobrir EAN equivalente que cd_ean do origem não tem (cadastro divergente CD↔CD).
    // Retorna PARES (ean_origem, ean_equivalente) pra montar lookup no JS.
    const ppEquivPairs = eansNorm.length ? await dbQuery(
      `WITH pp_origem AS (
         SELECT DISTINCT NULLIF(LTRIM(codigobarra,'0'),'') AS ean_origem, produtoprincipal
           FROM produtos_externo
          WHERE produtoprincipal IS NOT NULL AND produtoprincipal <> ''
            AND NULLIF(LTRIM(codigobarra,'0'),'') = ANY($1::text[])
       )
       SELECT DISTINCT po.ean_origem, NULLIF(LTRIM(pe.codigobarra,'0'),'') AS ean_eq
         FROM pp_origem po
         JOIN produtos_externo pe ON pe.produtoprincipal = po.produtoprincipal
        WHERE pe.codigobarra IS NOT NULL`,
      [eansNorm]
    ) : [];
    // ppEqMap: ean_origem → Set<eans equivalentes>
    const ppEqMap = new Map();
    for (const r of ppEquivPairs) {
      if (!r.ean_origem || !r.ean_eq) continue;
      if (!ppEqMap.has(r.ean_origem)) ppEqMap.set(r.ean_origem, new Set([r.ean_origem]));
      ppEqMap.get(r.ean_origem).add(r.ean_eq);
    }
    const eansAmpliados = [...new Set([
      ...eansNorm,
      ...eansExpandidosCdOrigem.map(x => x.ean_codi).filter(Boolean),
      ...ppEquivPairs.map(x => x.ean_eq).filter(Boolean),
    ])];

    // 5a) FALLBACK CANÔNICO: produto_canonico_match resolve casos onde EAN não cruza
    // (cadastro divergente entre CDs). Pega mat_codi de cada CD destino pelos mat_codis do CD origem.
    const canonicosOrigem = matCodis.length ? await dbQuery(
      `SELECT pcm.produto_canonico_id, pcm.mat_codi AS mat_origem
         FROM produto_canonico_match pcm
        WHERE pcm.cd_codigo = $1 AND pcm.mat_codi = ANY($2::text[])`,
      [cdOrigem, matCodis]
    ) : [];
    const canonicoIds = [...new Set(canonicosOrigem.map(c => c.produto_canonico_id))];
    const canonicosDestinos = canonicoIds.length && cdDestinos.length ? await dbQuery(
      `SELECT pcm.produto_canonico_id, pcm.cd_codigo, pcm.mat_codi
         FROM produto_canonico_match pcm
        WHERE pcm.produto_canonico_id = ANY($1::int[])
          AND pcm.cd_codigo = ANY($2::text[])`,
      [canonicoIds, cdDestinos.map(d => d.cd_codigo)]
    ) : [];
    // Mapa: `${cd_destino}|${mat_origem}` → mat_destino
    const matCanonicoMap = new Map();
    const matOrigemPorCanonico = new Map();
    for (const c of canonicosOrigem) matOrigemPorCanonico.set(c.produto_canonico_id, c.mat_origem);
    for (const cd of canonicosDestinos) {
      const matOrigem = matOrigemPorCanonico.get(cd.produto_canonico_id);
      if (!matOrigem) continue;
      matCanonicoMap.set(`${cd.cd_codigo}|${matOrigem}`, cd.mat_codi);
    }

    const estCds = cdDestinos.length && eansAmpliados.length ? await dbQuery(
      `SELECT ce.cd_codigo, NULLIF(LTRIM(ce.ean_codi,'0'),'') AS ean_codi, cde.est_quan
         FROM cd_ean ce
         LEFT JOIN cd_estoque cde ON cde.cd_codigo = ce.cd_codigo AND cde.pro_codi = ce.mat_codi
        WHERE ce.cd_codigo = ANY($1::text[])
          AND NULLIF(LTRIM(ce.ean_codi,'0'),'') = ANY($2::text[])`,
      [cdDestinos.map(d => d.cd_codigo), eansAmpliados]
    ) : [];
    tick('estCds');

    // 5a-tri) Estoque via cd_produtos_embalagem do destino — match por ean_principal OU ean_secundario.
    // ean_secundario é a "ponte" cadastrada manualmente que liga produtos com cadastro divergente
    // entre CD origem e destino (ex: ASA FRIO usa ean_principal próprio mas ean_secundario = EAN do ITB).
    const estCdsCpe = cdDestinos.length && eansAmpliados.length ? await dbQuery(
      `SELECT cpe.cd_codigo, cpe.mat_codi,
              NULLIF(LTRIM(cpe.ean_principal,'0'),'')  AS ean_principal,
              NULLIF(LTRIM(cpe.ean_secundario,'0'),'') AS ean_secundario,
              cpe.qtd_embalagem, cpe.peso_unidade_kg, cpe.peso_variavel,
              cde.est_quan
         FROM cd_produtos_embalagem cpe
         LEFT JOIN cd_estoque cde
                ON cde.cd_codigo = cpe.cd_codigo AND cde.pro_codi = cpe.mat_codi
        WHERE cpe.cd_codigo = ANY($1::text[])
          AND (NULLIF(LTRIM(cpe.ean_principal,'0'),'')  = ANY($2::text[])
            OR NULLIF(LTRIM(cpe.ean_secundario,'0'),'') = ANY($2::text[]))`,
      [cdDestinos.map(d => d.cd_codigo), eansAmpliados]
    ) : [];
    tick('estCdsCpe');

    // 5a-bis) Estoque via canônico — quando EAN não cruza, pega direto pelo mat_codi do destino
    const estCdsCanonico = canonicosDestinos.length ? await dbQuery(
      `SELECT cde.cd_codigo, cde.pro_codi AS mat_codi, cde.est_quan
         FROM cd_estoque cde
         JOIN produto_canonico_match pcm
           ON pcm.cd_codigo = cde.cd_codigo AND pcm.mat_codi = cde.pro_codi
        WHERE pcm.produto_canonico_id = ANY($1::int[])`,
      [canonicoIds]
    ) : [];
    tick('estCdsCanonico');

    // 5b1) Trânsito CD→CD destino: SAÍDAS do CD ORIGEM pra cada CD destino (via cli_codi cacheado)
    // que ainda estão 'A' (aberta) ou 'F' fechada mas não chegou no destino.
    // Aqui simplificamos: status='A' = ainda em trânsito.
    const transitoCdDestinos = cdDestinos.length && eansNorm.length ? await dbQuery(
      `WITH cli_codis AS (
        SELECT cd_origem_codigo, cnpj_destino, cli_codi
          FROM pedidos_distrib_cli_codi
         WHERE cd_origem_codigo = $1
       )
       SELECT d.cd_codigo, NULLIF(LTRIM(ce.ean_codi,'0'),'') AS ean,
              SUM(ic.mcp_quan) AS qtd_transito
         FROM pedidos_distrib_destinos d
         JOIN cli_codis cc ON cc.cnpj_destino = d.cnpj
         JOIN cd_movcompra mc
           ON mc.cd_codigo = cc.cd_origem_codigo
          AND mc.mcp_tipomov = 'S'
          AND mc.for_codi = cc.cli_codi
          AND COALESCE(mc.mcp_status, 'A') = 'A'
         JOIN cd_itemcompra ic
           ON ic.cd_codigo = mc.cd_codigo
          AND ic.mcp_codi = mc.mcp_codi
          AND ic.mcp_tipomov = mc.mcp_tipomov
         JOIN cd_ean ce
           ON ce.cd_codigo = mc.cd_codigo
          AND ce.mat_codi = ic.pro_codi
        WHERE d.tipo='CD' AND d.cd_codigo = ANY($2::text[])
          AND NULLIF(LTRIM(ce.ean_codi,'0'),'') = ANY($3::text[])
        GROUP BY d.cd_codigo, ean`,
      [cdOrigem, cdDestinos.map(d => d.cd_codigo), eansNorm]
    ) : [];
    tick('transitoCdDestinos');

    // 5b) "Vendas" dos CDs destino (consumo real) — fonte: CAPA + MOVIITEM (CAP_TIPO='3' = venda externa/rota/atacado).
    // Devolução (CAP_DEVOL='S') subtrai. cd_movcompra registra só transferências internas (NOP=31), não serve.
    // Lê do cache diário (vendas_cd_cache).
    const vendasCds = cdDestinos.length && eansAmpliados.length ? await dbQuery(
      `SELECT cd_codigo, ean_norm AS ean, qtd_90d, ultima_saida
         FROM vendas_cd_cache
        WHERE cd_codigo = ANY($1::text[])
          AND ean_norm = ANY($2::text[])`,
      [cdDestinos.map(d => d.cd_codigo), eansAmpliados]
    ) : [];
    tick('vendasCds');

    // 5b-bis) Vendas CD destino via canônico — também do cache diário.
    // Filtra por (cd_codigo, mat_codi) usando os pares já resolvidos em canonicosDestinos.
    const vendasCdsCanonico = canonicosDestinos.length ? await dbQuery(
      `SELECT v.cd_codigo, v.mat_codi, v.qtd_90d, v.ultima_saida
         FROM vendas_cd_canonico_cache v
         JOIN produto_canonico_match pcm
           ON pcm.cd_codigo = v.cd_codigo AND pcm.mat_codi = v.mat_codi
        WHERE pcm.produto_canonico_id = ANY($1::int[])`,
      [canonicoIds]
    ) : [];
    tick('vendasCdsCanonico');

    // 5b1-bis) Trânsito via canônico — saídas do CD origem com pro_codi=mat_origem direto
    const transitoCdsCanonico = canonicosDestinos.length ? await dbQuery(
      `WITH cli_codis AS (
        SELECT cnpj_destino, cli_codi
          FROM pedidos_distrib_cli_codi
         WHERE cd_origem_codigo = $1
       )
       SELECT d.cd_codigo, ic.pro_codi AS mat_origem,
              SUM(ic.mcp_quan) AS qtd_transito
         FROM pedidos_distrib_destinos d
         JOIN cli_codis cc ON cc.cnpj_destino = d.cnpj
         JOIN cd_movcompra mc
           ON mc.cd_codigo = $1
          AND mc.mcp_tipomov = 'S'
          AND mc.for_codi = cc.cli_codi
          AND COALESCE(mc.mcp_status,'A') = 'A'
         JOIN cd_itemcompra ic
           ON ic.cd_codigo = mc.cd_codigo
          AND ic.mcp_codi = mc.mcp_codi
          AND ic.mcp_tipomov = mc.mcp_tipomov
        WHERE d.tipo='CD' AND d.cd_codigo = ANY($2::text[])
          AND ic.pro_codi = ANY($3::text[])
        GROUP BY d.cd_codigo, ic.pro_codi`,
      [cdOrigem, cdDestinos.map(d => d.cd_codigo), matCodis]
    ) : [];
    tick('transitoCdsCanonico');

    // 6) Trânsito por (loja_id, ean) — itens em notas_entrada origem='cd'/'transferencia_loja' não fechadas
    const _t_transito_start = Date.now();
    const transito = lojaIds.length && eansList.length ? await dbQuery(
      `SELECT n.loja_id,
              NULLIF(LTRIM(COALESCE(i.ean_validado, i.ean_nota),'0'),'') AS codbarra,
              COALESCE(SUM(i.quantidade),0) AS qtd_transito
         FROM itens_nota i
         JOIN notas_entrada n ON n.id = i.nota_id
        WHERE n.loja_id = ANY($1::int[])
          AND n.origem IN ('cd','transferencia_loja')
          AND n.status NOT IN ('fechada','validada','arquivada','cancelada','finalizada_f')
          AND COALESCE(n.mcp_status_cd, 'A') <> 'C'
          AND n.chegou_no_erp_em IS NULL
          AND NULLIF(LTRIM(COALESCE(i.ean_validado, i.ean_nota),'0'),'') = ANY($2::text[])
        GROUP BY n.loja_id, codbarra`,
      [lojaIds, eansList]
    ) : [];
    tick('transito');

    // 7) Quantidades editadas (Sug_Editada)
    const qtds = await dbQuery(
      `SELECT destino_id, mat_codi, qtd FROM pedidos_distrib_quantidades
        WHERE cd_origem_codigo = $1 AND mat_codi = ANY($2::text[])`,
      [cdOrigem, matCodis]
    );
    tick('qtds');

    // 8) Ranking GLOBAL — lê do cache (atualizado em background pelo cron).
    // Cache vazio (cold start) → fallback pra query original.
    let rankingGlobal = await dbQuery(
      `SELECT mat_codi, posicao FROM pedidos_distrib_ranking_cache
        WHERE cd_codigo = $1 ORDER BY posicao`, [cdOrigem]);
    tick('rankingCache');
    if (!rankingGlobal.length) {
      console.warn(`[ranking_cache] sem cache pra ${cdOrigem}, usando query direta (lento)`);
      rankingGlobal = await dbQuery(`
        WITH produtos_cd AS (
          SELECT cd_m.mat_codi,
                 COALESCE(NULLIF(pe.ean_principal_cd,''), NULLIF(cd_m.ean_codi,'')) AS ean,
                 cd_c.pro_prad AS preco_admin
            FROM cd_material cd_m
            LEFT JOIN cd_custoprod cd_c ON cd_c.cd_codigo=cd_m.cd_codigo AND cd_c.pro_codi=cd_m.mat_codi
            LEFT JOIN produtos_embalagem pe ON pe.mat_codi=cd_m.mat_codi
           WHERE cd_m.cd_codigo = $1
             AND (cd_m.mat_situ='A' OR cd_m.mat_situ IS NULL)
             AND COALESCE(NULLIF(pe.ean_principal_cd,''), NULLIF(cd_m.ean_codi,'')) IS NOT NULL
        ),
        vendas_90d AS (
          SELECT NULLIF(LTRIM(codigobarra,'0'),'') AS ean,
                 SUM(qtd_vendida) AS qtd
            FROM vendas_historico
           WHERE data_venda >= CURRENT_DATE - INTERVAL '90 days'
             AND COALESCE(tipo_saida,'venda')='venda'
           GROUP BY ean
        )
        SELECT pc.mat_codi
          FROM produtos_cd pc
          LEFT JOIN vendas_90d v ON v.ean = pc.ean
         WHERE COALESCE(v.qtd, 0) > 0
         ORDER BY COALESCE(v.qtd, 0) * COALESCE(pc.preco_admin, 0) DESC
      `, [cdOrigem]);
    }

    tick('antes_consolidar');
    // ── Consolida tudo no JSON por produto ──
    const norm = e => String(e || '').replace(/^0+/, '') || e;
    const eanIdx = new Map();
    for (const p of produtos) {
      eanIdx.set(norm(p.ean_codi), p);
      p.destinos = {};
      p.vendas = {};
    }

    // Estoque + preço por loja — chave SEMPRE pelo codbarra (que é o EAN real do produto na loja)
    // produtos_externo.produtoprincipal é um ID interno (não EAN), serve só pra agrupar variantes
    const estLojaMap = new Map(); // `${loja_id}|${codbarra}` → { est, preco }
    // Pra agrupar codbarras alternativos vinculados ao mesmo principal (id interno):
    // map principal_id → Set<codbarras> por loja
    const principalCodbarras = new Map(); // `${loja_id}|${principal}` → Set<codbarras>
    for (const r of estLojas) {
      if (!r.codbarra) continue;
      const key = `${r.loja_id}|${r.codbarra}`;
      const acc = estLojaMap.get(key);
      const est = parseFloat(r.estdisponivel) || 0;
      const preco = parseFloat(r.prsugerido) || null;
      if (acc) {
        acc.estdisponivel = (parseFloat(acc.estdisponivel) || 0) + est;
        if (!acc.prsugerido && preco) acc.prsugerido = preco;
      } else {
        estLojaMap.set(key, { estdisponivel: est, prsugerido: preco });
      }
      if (r.principal) {
        const pk = `${r.loja_id}|${r.principal}`;
        if (!principalCodbarras.has(pk)) principalCodbarras.set(pk, new Set());
        principalCodbarras.get(pk).add(r.codbarra);
      }
    }

    // Vendas por loja — agregadas por (loja_id, codbarra). Soma os codbarras vinculados ao ean principal.
    const vendaPorCb = new Map();
    for (const v of vendas) vendaPorCb.set(`${v.loja_id}|${v.codbarra}`, v);

    // Trânsito loja — idem
    const transitoPorCb = new Map();
    for (const t of transito) transitoPorCb.set(`${t.loja_id}|${t.codbarra}`, parseFloat(t.qtd_transito));

    // Estoque CD destino — chave: `${cd_codigo}|${ean_norm}` (match EAN)
    const estCdMap = new Map();
    for (const e of estCds) estCdMap.set(`${e.cd_codigo}|${norm(e.ean_codi)}`, e);
    // estCdsCpe — mapeia ambos ean_principal e ean_secundario pro mesmo record (CD+mat+estoque).
    // Entra como fallback após estCdMap no loop.
    const estCdMapCpe = new Map();
    for (const e of estCdsCpe) {
      const rec = { est_quan: e.est_quan, mat_codi: e.mat_codi,
                    qtd_embalagem: e.qtd_embalagem, peso_unidade_kg: e.peso_unidade_kg,
                    peso_variavel: e.peso_variavel };
      if (e.ean_principal)  estCdMapCpe.set(`${e.cd_codigo}|${e.ean_principal}`, rec);
      if (e.ean_secundario) estCdMapCpe.set(`${e.cd_codigo}|${e.ean_secundario}`, rec);
    }

    // Estoque CD destino via canônico — chave: `${cd_codigo}|${mat_codi_destino}` → mas guardado por mat_origem pra lookup
    // Pra simplificar: vamos guardar por (cd_codigo, mat_codi_origem)
    const estCdMapCanon = new Map();
    for (const e of estCdsCanonico) {
      // Acha mat_origem usando matCanonicoMap reverso: pra cada (cd, mat_destino) tem mat_origem
      for (const [key, matDest] of matCanonicoMap.entries()) {
        const [cd, matOrig] = key.split('|');
        if (cd === e.cd_codigo && matDest === e.mat_codi) {
          estCdMapCanon.set(`${cd}|${matOrig}`, e);
          break;
        }
      }
    }

    // Vendas (consumo) dos CDs destino — EAN
    const vendasCdMap = new Map();
    for (const v of vendasCds) vendasCdMap.set(`${v.cd_codigo}|${v.ean}`, v);

    // Vendas via canônico
    const vendasCdMapCanon = new Map();
    for (const v of vendasCdsCanonico) {
      for (const [key, matDest] of matCanonicoMap.entries()) {
        const [cd, matOrig] = key.split('|');
        if (cd === v.cd_codigo && matDest === v.mat_codi) {
          vendasCdMapCanon.set(`${cd}|${matOrig}`, v);
          break;
        }
      }
    }

    // Trânsito CD destino (saídas do CD origem com mcp_status='A' pra cada CD destino) — via EAN
    const transitoCdMap = new Map();
    for (const t of transitoCdDestinos) transitoCdMap.set(`${t.cd_codigo}|${t.ean}`, parseFloat(t.qtd_transito) || 0);
    // Trânsito via canônico — chave por mat_origem
    const transitoCdMapCanon = new Map();
    for (const t of transitoCdsCanonico) transitoCdMapCanon.set(`${t.cd_codigo}|${t.mat_origem}`, parseFloat(t.qtd_transito) || 0);

    // Sug_Editada
    const sugEditMap = new Map();
    for (const q of qtds) sugEditMap.set(`${q.mat_codi}|${q.destino_id}`, parseFloat(q.qtd));

    // Ranking GLOBAL — já vem ordenado (cache tem posicao; fallback usa ordem da query)
    const rankMap = new Map();
    rankingGlobal.forEach((r, i) => rankMap.set(r.mat_codi, r.posicao || (i + 1)));

    for (const p of produtos) {
      const eanN = norm(p.ean_codi);
      p.ranking = rankMap.get(p.mat_codi) || null;
      const qtdEmb = parseInt(p.qtd_embalagem) || 1;

      for (const d of destinos) {
        const slot = { estoque_un: 0, estoque_cx: 0, transito_un: 0, transito_cx: 0, sugestao_cx: 0, sug_editada: 0 };

        if (d.tipo === 'LOJA' && d.loja_id) {
          // Match direto por codbarra = ean principal do produto
          const e = estLojaMap.get(`${d.loja_id}|${eanN}`);
          const v = vendaPorCb.get(`${d.loja_id}|${eanN}`);
          // NF do CD manda quantidade em CX (1 unid NF = 1 CX). Estoque da loja é em UN de venda.
          const tCx = transitoPorCb.get(`${d.loja_id}|${eanN}`) || 0;
          slot.estoque_un = e ? parseFloat(e.estdisponivel) : 0;
          slot.transito_cx = Math.floor(tCx);
          slot.transito_un = tCx * qtdEmb;
          slot.estoque_cx = qtdEmb > 0 ? Math.floor(slot.estoque_un / qtdEmb) : 0;
          const qtd_90d = v ? parseFloat(v.qtd_90d) || 0 : 0;
          const media_dia = qtd_90d / 90;
          const sug_un = Math.max(0, 35 * media_dia - slot.estoque_un - slot.transito_un);
          slot.sugestao_cx = qtdEmb > 0 ? Math.ceil(sug_un / qtdEmb) : 0;
          if (e || v) {
            p.vendas[d.loja_id] = {
              media_dia,
              preco_atual: e?.prsugerido ? parseFloat(e.prsugerido) : null,
              ultima_venda: v?.ultima_venda || null,
            };
          }
        } else if (d.tipo === 'CD' && d.cd_codigo) {
          // CDs destino controlam estoque na MESMA unidade do CD origem (CX) — não dividir por qtd_embalagem
          // Match por EAN primeiro; se não tiver, tenta EANs equivalentes via produtoprincipal das lojas;
          // depois fallback cd_produtos_embalagem (ean_principal/secundario manual);
          // último fallback: canônico (mat_codi do destino mapeado).
          const eansTentar = ppEqMap.get(eanN) || new Set([eanN]);
          let e = null, v = null, t = 0;
          for (const eq of eansTentar) {
            if (!e || !e.est_quan) e = estCdMap.get(`${d.cd_codigo}|${eq}`) || estCdMapCpe.get(`${d.cd_codigo}|${eq}`) || e;
            if (!v) v = vendasCdMap.get(`${d.cd_codigo}|${eq}`) || v;
            if (!t) t = transitoCdMap.get(`${d.cd_codigo}|${eq}`) || 0;
          }
          if (!e || !e.est_quan) e = estCdMapCanon.get(`${d.cd_codigo}|${p.mat_codi}`) || e;
          if (!v) v = vendasCdMapCanon.get(`${d.cd_codigo}|${p.mat_codi}`) || v;
          if (!t) t = transitoCdMapCanon.get(`${d.cd_codigo}|${p.mat_codi}`) || 0;
          slot.estoque_un = e?.est_quan ? parseFloat(e.est_quan) : 0;
          slot.estoque_cx = Math.floor(slot.estoque_un);
          slot.transito_un = t;
          slot.transito_cx = Math.floor(t);
          const qtd_90d = v ? parseFloat(v.qtd_90d) || 0 : 0;
          const media_dia = qtd_90d / 90;
          const sug_cx = Math.max(0, 35 * media_dia - slot.estoque_un - slot.transito_un);
          slot.sugestao_cx = Math.ceil(sug_cx);
        }

        slot.sug_editada = sugEditMap.get(`${p.mat_codi}|${d.id}`) || 0;
        p.destinos[d.id] = slot;
      }
    }

    // Ordena por ranking se solicitado (ranking só calcula depois — no Node)
    if (ordem === 'ranking') {
      produtos.sort((a, b) => {
        const ra = a.ranking ?? Number.MAX_SAFE_INTEGER;
        const rb = b.ranking ?? Number.MAX_SAFE_INTEGER;
        return ra - rb;
      });
    }

    tick('total');
    // Log timing pra investigar gargalos (só admin via header)
    if (req.query.debug === '1') {
      console.log('[grade] timings:', JSON.stringify(timings));
    }
    res.set('X-Grade-Timings', JSON.stringify(timings));
    res.set('Cache-Control', 'private, max-age=60, stale-while-revalidate=120');
    res.json({ cd_origem: cdOrigem, destinos, produtos });
  } catch (e) {
    console.error('[pedidos-distrib grade]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// PUT /prioridade — toggle prioritário do produto. Body: { cd_origem, mat_codi }
router.put('/prioridade', adminOuCeo, async (req, res) => {
  try {
    const { cd_origem, mat_codi } = req.body || {};
    if (!cd_origem || !mat_codi) return res.status(400).json({ erro: 'cd_origem e mat_codi obrigatorios' });
    const por = req.usuario.email || req.usuario.usuario || req.usuario.nome || `id:${req.usuario.id}`;
    // Toggle: se existir, deleta; senão, insere
    const r = await dbQuery(
      `DELETE FROM pedidos_distrib_prioridades WHERE cd_origem_codigo = $1 AND mat_codi = $2 RETURNING 1`,
      [cd_origem, String(mat_codi).trim()]
    );
    if (!r.length) {
      await dbQuery(
        `INSERT INTO pedidos_distrib_prioridades (cd_origem_codigo, mat_codi, atualizado_por) VALUES ($1,$2,$3)`,
        [cd_origem, String(mat_codi).trim(), por]
      );
      return res.json({ ok: true, prioritario: true });
    }
    res.json({ ok: true, prioritario: false });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// PUT /qtd — salva qtd editada. Body: { cd_origem, destino_id, mat_codi, qtd }
router.put('/qtd', adminOuCeo, async (req, res) => {
  try {
    const { cd_origem, destino_id, mat_codi, qtd } = req.body || {};
    if (!cd_origem || !destino_id || !mat_codi) return res.status(400).json({ erro: 'cd_origem, destino_id, mat_codi obrigatorios' });
    const por = req.usuario.email || req.usuario.usuario || req.usuario.nome || `id:${req.usuario.id}`;
    const qtdNum = parseFloat(qtd) || 0;
    if (qtdNum <= 0) {
      await dbQuery(
        `DELETE FROM pedidos_distrib_quantidades
          WHERE cd_origem_codigo=$1 AND destino_id=$2 AND mat_codi=$3`,
        [cd_origem, parseInt(destino_id), String(mat_codi).trim()]
      );
      return res.json({ ok: true, acao: 'removido' });
    }
    await dbQuery(
      `INSERT INTO pedidos_distrib_quantidades
         (cd_origem_codigo, destino_id, mat_codi, qtd, atualizado_em, atualizado_por)
       VALUES ($1,$2,$3,$4,NOW(),$5)
       ON CONFLICT (cd_origem_codigo, destino_id, mat_codi) DO UPDATE SET
         qtd = EXCLUDED.qtd, atualizado_em = NOW(), atualizado_por = EXCLUDED.atualizado_por`,
      [cd_origem, parseInt(destino_id), String(mat_codi).trim(), qtdNum, por]
    );
    res.json({ ok: true, acao: 'salvo', qtd: qtdNum });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// DELETE /qtd-tudo?cd_origem=X — limpa todas as qtds salvas (rascunho)
router.delete('/qtd-tudo', adminOuCeo, async (req, res) => {
  try {
    const cdOrigem = String(req.query.cd_origem || '').trim();
    if (!cdOrigem) return res.status(400).json({ erro: 'cd_origem obrigatorio' });
    const r = await dbQuery(`DELETE FROM pedidos_distrib_quantidades WHERE cd_origem_codigo = $1`, [cdOrigem]);
    res.json({ ok: true, removidos: r.length });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /debug-ean-cross?cd_origem=&mat_codi= — mostra EANs de UM produto em TODOS os CDs
router.get('/debug-ean-cross', adminOuCeo, async (req, res) => {
  try {
    const cdOrigem = String(req.query.cd_origem || 'srv1-itautuba').trim();
    const matCodi  = String(req.query.mat_codi  || '').trim();
    if (!matCodi) return res.status(400).json({ erro: 'mat_codi obrigatorio' });

    // Produto no CD origem
    const [prod] = await dbQuery(
      `SELECT m.mat_codi, m.mat_desc, m.ean_codi AS ean_material, pe.ean_principal_cd
         FROM cd_material m
         LEFT JOIN produtos_embalagem pe ON pe.mat_codi = m.mat_codi
        WHERE m.cd_codigo = $1 AND m.mat_codi = $2`, [cdOrigem, matCodi]);
    if (!prod) return res.status(404).json({ erro: 'produto nao existe nesse CD' });

    // Todos EANs do produto no cd_ean do CD origem
    const eansOrigem = await dbQuery(
      `SELECT ean_codi, ean_nota, ordem FROM cd_ean
        WHERE cd_codigo = $1 AND mat_codi = $2 ORDER BY ordem`, [cdOrigem, matCodi]);

    // Pra cada outro CD, tenta achar o produto via QUALQUER EAN
    const todosEans = [...new Set([
      prod.ean_principal_cd,
      prod.ean_material,
      ...eansOrigem.map(x => x.ean_codi)
    ].filter(Boolean).map(e => String(e).replace(/^0+/, '') || e))];

    const outrosCds = await dbQuery(
      `SELECT DISTINCT cd_codigo FROM cd_ean WHERE cd_codigo <> $1`, [cdOrigem]);
    const matchPorCd = {};
    for (const cd of outrosCds) {
      const r = await dbQuery(
        `SELECT ce.mat_codi, ce.ean_codi, ce.ean_nota,
                cm.mat_desc, ce_e.est_quan
           FROM cd_ean ce
           LEFT JOIN cd_material cm ON cm.cd_codigo = ce.cd_codigo AND cm.mat_codi = ce.mat_codi
           LEFT JOIN cd_estoque  ce_e ON ce_e.cd_codigo = ce.cd_codigo AND ce_e.pro_codi = ce.mat_codi
          WHERE ce.cd_codigo = $1 AND NULLIF(LTRIM(ce.ean_codi,'0'),'') = ANY($2::text[])`,
        [cd.cd_codigo, todosEans]);
      matchPorCd[cd.cd_codigo] = r;
    }

    res.json({
      produto_no_cd_origem: prod,
      eans_origem_cd_ean: eansOrigem,
      eans_normalizados_buscados: todosEans,
      match_em_outros_cds: matchPorCd,
    });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /debug-cliente?cd_origem=&cnpj= — descobre colunas + tenta achar cliente
router.get('/debug-cliente', adminOuCeo, async (req, res) => {
  try {
    const cdOrigem = String(req.query.cd_origem || 'srv1-itautuba').trim();
    const cnpj = String(req.query.cnpj || '17764296000110').replace(/\D/g, '');
    const { clientePorCodigo } = require('../cds');
    const cli = await clientePorCodigo(cdOrigem);

    // 1) Lista TODAS colunas da CLIENTE
    const cols = await cli.colunas('CLIENTE');
    // Filtra colunas potencialmente úteis (CLI_CODI/nome/cpf/cnpj)
    const colsRelevantes = (cols.colunas || []).filter(c =>
      /CODI|CPF|CGC|NOME|RAZS|RAZ|CNPJ|DESC/i.test(c.COLUMN_NAME)
    );

    // 2) Sample de 3 clientes (todas as colunas) pra ver formato real
    const sample = await cli.query(`SELECT TOP 3 * FROM CLIENTE WITH (NOLOCK)`);

    // 3) Tenta varias estrategias com CLI_CPF
    const tentativas = [
      { nome: 'CLI_CPF LTRIM zeros', sql: `SELECT TOP 5 CLI_CODI, CLI_CPF FROM CLIENTE WITH (NOLOCK) WHERE LTRIM(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(CLI_CPF)),'.',''),'/',''),'-',''), '0') = LTRIM('${cnpj}', '0')` },
      { nome: 'CLI_CPF LIKE %cnpj%', sql: `SELECT TOP 5 CLI_CODI, CLI_CPF FROM CLIENTE WITH (NOLOCK) WHERE CLI_CPF LIKE '%${cnpj}%'` },
      { nome: 'CLI_CPF LIKE primeiros 8', sql: `SELECT TOP 5 CLI_CODI, CLI_CPF FROM CLIENTE WITH (NOLOCK) WHERE CLI_CPF LIKE '%${cnpj.slice(0,8)}%'` },
    ];
    const resultados = [];
    for (const t of tentativas) {
      try {
        const r = await cli.query(t.sql);
        resultados.push({ ...t, total: r.rows?.length || 0, rows: r.rows?.slice(0,3) });
      } catch (e) { resultados.push({ ...t, erro: e.message }); }
    }
    res.json({
      cd_origem: cdOrigem,
      cnpj_procurado: cnpj,
      colunas_relevantes: colsRelevantes,
      sample_clientes: sample.rows,
      tentativas: resultados,
    });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /grupos?cd_origem= — lista de grupos+subgrupos do CD (pra dropdowns)
router.get('/grupos', adminOuCeo, async (req, res) => {
  try {
    const cdOrigem = String(req.query.cd_origem || '').trim();
    if (!cdOrigem) return res.status(400).json({ erro: 'cd_origem obrigatorio' });
    const grupos = await dbQuery(
      `SELECT g.gru_codi, g.gru_desc, COUNT(m.mat_codi)::int AS qtde_produtos
         FROM cd_grupo g
         LEFT JOIN cd_material m ON m.cd_codigo = g.cd_codigo AND m.gru_codi = g.gru_codi
        WHERE g.cd_codigo = $1
        GROUP BY g.gru_codi, g.gru_desc
        ORDER BY g.gru_desc`, [cdOrigem]
    );
    const subgrupos = await dbQuery(
      `SELECT s.gru_codi, s.sgr_codi, s.sgr_desc, COUNT(m.mat_codi)::int AS qtde_produtos
         FROM cd_subgrupo s
         LEFT JOIN cd_material m ON m.cd_codigo = s.cd_codigo AND m.gru_codi = s.gru_codi AND m.sgr_codi = s.sgr_codi
        WHERE s.cd_codigo = $1
        GROUP BY s.gru_codi, s.sgr_codi, s.sgr_desc
        ORDER BY s.gru_codi, s.sgr_desc`, [cdOrigem]
    );
    res.json({ cd_origem: cdOrigem, grupos, subgrupos });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /ultima-sequencia?cd_origem= — último mcp_codi (S=saída) do CD pra sugerir filtro final
router.get('/ultima-sequencia', adminOuCeo, async (req, res) => {
  try {
    const cdOrigem = String(req.query.cd_origem || '').trim();
    if (!cdOrigem) return res.status(400).json({ erro: 'cd_origem obrigatorio' });
    const [r] = await dbQuery(
      `SELECT MAX((mcp_codi)::int) AS ultima
         FROM cd_movcompra
        WHERE cd_codigo = $1
          AND mcp_tipomov = 'E'
          AND mcp_codi ~ '^[0-9]+$'`, [cdOrigem]
    );
    res.json({ cd_origem: cdOrigem, ultima_sequencia: r?.ultima || null });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /debug-nops?cd_codigo= — lista NOPs usados em cd_movcompra desse CD (top por volume)
router.get('/debug-nops', adminOuCeo, async (req, res) => {
  try {
    const cd = String(req.query.cd_codigo || '').trim();
    if (!cd) return res.status(400).json({ erro: 'cd_codigo obrigatorio' });
    const r = await dbQuery(
      `SELECT mc.mcp_tipomov,
              COALESCE(NULLIF(LTRIM(mc.nop_codi,'0'),''),'') AS nop,
              COUNT(DISTINCT mc.mcp_codi)::int AS qtd_movs,
              SUM(ic.mcp_quan)::int AS qtd_itens,
              MIN(mc.mcp_dten) AS primeira, MAX(mc.mcp_dten) AS ultima
         FROM cd_movcompra mc
         JOIN cd_itemcompra ic ON ic.cd_codigo=mc.cd_codigo AND ic.mcp_codi=mc.mcp_codi AND ic.mcp_tipomov=mc.mcp_tipomov
        WHERE mc.cd_codigo=$1 AND mc.mcp_dten >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY mc.mcp_tipomov, nop
        ORDER BY mc.mcp_tipomov, qtd_itens DESC`, [cd]);
    res.json({ cd_codigo: cd, nops_90d: r });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /debug-produto-lojas?desc= — busca produto nas lojas por descrição parcial
router.get('/debug-produto-lojas', adminOuCeo, async (req, res) => {
  try {
    const desc = String(req.query.desc || '').trim();
    if (!desc) return res.status(400).json({ erro: 'desc obrigatorio' });
    const rows = await dbQuery(
      `SELECT loja_id,
              NULLIF(LTRIM(codigobarra,'0'),'') AS ean_codbarra,
              NULLIF(LTRIM(produtoprincipal,'0'),'') AS ean_principal,
              descricao, estdisponivel, prsugerido
         FROM produtos_externo
        WHERE descricao ILIKE $1
        ORDER BY loja_id, descricao
        LIMIT 50`, [`%${desc}%`]);
    const eansEncontrados = [...new Set(rows.flatMap(r => [r.ean_codbarra, r.ean_principal]).filter(Boolean))];
    const vendasNesseEans = eansEncontrados.length ? await dbQuery(
      `SELECT loja_id, NULLIF(LTRIM(codigobarra,'0'),'') AS ean,
              SUM(qtd_vendida) AS total, MIN(data_venda) AS prim, MAX(data_venda) AS ult
         FROM vendas_historico
        WHERE NULLIF(LTRIM(codigobarra,'0'),'') = ANY($1::text[])
          AND data_venda >= CURRENT_DATE - INTERVAL '28 days'
        GROUP BY loja_id, ean
        ORDER BY loja_id, total DESC`, [eansEncontrados]) : [];
    res.json({ matches: rows, eans_encontrados: eansEncontrados, vendas_28d: vendasNesseEans });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /debug-vendas?cd_origem=&mat_codi= — diagnóstico completo de vendas/sugestao de UM produto
router.get('/debug-vendas', adminOuCeo, async (req, res) => {
  try {
    const cdOrigem = String(req.query.cd_origem || 'srv1-itautuba').trim();
    const matCodiRaw = String(req.query.mat_codi || '').trim();
    if (!matCodiRaw) return res.status(400).json({ erro: 'mat_codi obrigatorio' });
    const matCodi = matCodiRaw.padStart(10, '0');

    // 1) Produto e todos EANs no CD origem
    const [produto] = await dbQuery(
      `SELECT m.mat_codi, m.mat_desc, m.ean_codi AS ean_material,
              pe.ean_principal_cd, pe.qtd_embalagem,
              e.est_quan AS est_dist
         FROM cd_material m
         LEFT JOIN produtos_embalagem pe ON pe.mat_codi = m.mat_codi
         LEFT JOIN cd_estoque e ON e.cd_codigo = m.cd_codigo AND e.pro_codi = m.mat_codi
        WHERE m.cd_codigo = $1 AND m.mat_codi = $2`, [cdOrigem, matCodi]);
    if (!produto) return res.status(404).json({ erro: 'produto nao encontrado', mat_codi_buscado: matCodi });

    const eansOrigem = await dbQuery(
      `SELECT ean_codi, ean_nota FROM cd_ean WHERE cd_codigo=$1 AND mat_codi=$2`, [cdOrigem, matCodi]);
    const eansNorm = [...new Set([
      produto.ean_principal_cd, produto.ean_material,
      ...eansOrigem.map(e => e.ean_codi)
    ].filter(Boolean).map(e => String(e).replace(/^0+/, '') || e))];

    // 2) CDs destino e lojas
    const destinos = await dbQuery(
      `SELECT id, tipo, codigo, nome, cnpj, loja_id, cd_codigo
         FROM pedidos_distrib_destinos WHERE ativo=TRUE`);

    // 3) Vendas POS por loja (com tipo_saida)
    const lojaIds = destinos.filter(d => d.tipo === 'LOJA' && d.loja_id).map(d => d.loja_id);
    const vendasPos = lojaIds.length && eansNorm.length ? await dbQuery(
      `SELECT loja_id,
              NULLIF(LTRIM(codigobarra,'0'),'') AS ean,
              tipo_saida,
              SUM(CASE WHEN data_venda >= CURRENT_DATE - INTERVAL '28 days' THEN qtd_vendida ELSE 0 END) AS qtd_28d,
              SUM(CASE WHEN data_venda >= CURRENT_DATE - INTERVAL '90 days' THEN qtd_vendida ELSE 0 END) AS qtd_90d,
              MIN(data_venda) AS primeira, MAX(data_venda) AS ultima
         FROM vendas_historico
        WHERE loja_id = ANY($1::int[])
          AND NULLIF(LTRIM(codigobarra,'0'),'') = ANY($2::text[])
          AND data_venda >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY loja_id, ean, tipo_saida
        ORDER BY loja_id, tipo_saida`, [lojaIds, eansNorm]) : [];

    // 4) Saídas no cd_movcompra de cada CD destino (todas, não só atacado)
    const cdDestinos = destinos.filter(d => d.tipo === 'CD' && d.cd_codigo);
    const saidasCds = cdDestinos.length && eansNorm.length ? await dbQuery(
      `SELECT mc.cd_codigo,
              COALESCE(NULLIF(LTRIM(mc.nop_codi,'0'),''),'') AS nop,
              SUM(CASE WHEN mc.mcp_dten >= CURRENT_DATE - INTERVAL '28 days' THEN ic.mcp_quan ELSE 0 END) AS qtd_28d,
              SUM(CASE WHEN mc.mcp_dten >= CURRENT_DATE - INTERVAL '90 days' THEN ic.mcp_quan ELSE 0 END) AS qtd_90d,
              MIN(mc.mcp_dten) AS primeira, MAX(mc.mcp_dten) AS ultima
         FROM cd_movcompra mc
         JOIN cd_itemcompra ic ON ic.cd_codigo=mc.cd_codigo AND ic.mcp_codi=mc.mcp_codi AND ic.mcp_tipomov=mc.mcp_tipomov
         JOIN cd_ean ce ON ce.cd_codigo=mc.cd_codigo AND ce.mat_codi=ic.pro_codi
        WHERE mc.cd_codigo = ANY($1::text[])
          AND mc.mcp_tipomov='S'
          AND mc.mcp_dten >= CURRENT_DATE - INTERVAL '90 days'
          AND NULLIF(LTRIM(ce.ean_codi,'0'),'') = ANY($2::text[])
        GROUP BY mc.cd_codigo, nop
        ORDER BY mc.cd_codigo, nop`, [cdDestinos.map(d => d.cd_codigo), eansNorm]) : [];

    // 5) Saídas no cd_movcompra do CD ORIGEM (todas as transferências saindo, agrupadas por NOP)
    const saidasOrigem = await dbQuery(
      `SELECT COALESCE(NULLIF(LTRIM(mc.nop_codi,'0'),''),'') AS nop,
              COUNT(*) AS qtd_movs,
              SUM(CASE WHEN mc.mcp_dten >= CURRENT_DATE - INTERVAL '28 days' THEN ic.mcp_quan ELSE 0 END) AS qtd_28d,
              SUM(CASE WHEN mc.mcp_dten >= CURRENT_DATE - INTERVAL '90 days' THEN ic.mcp_quan ELSE 0 END) AS qtd_90d,
              MIN(mc.mcp_dten) AS primeira, MAX(mc.mcp_dten) AS ultima
         FROM cd_movcompra mc
         JOIN cd_itemcompra ic ON ic.cd_codigo=mc.cd_codigo AND ic.mcp_codi=mc.mcp_codi AND ic.mcp_tipomov=mc.mcp_tipomov
        WHERE mc.cd_codigo = $1
          AND mc.mcp_tipomov='S'
          AND ic.pro_codi = $2
          AND mc.mcp_dten >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY nop
        ORDER BY qtd_90d DESC`, [cdOrigem, matCodi]);

    // 6) CAPA + MOVIITEM (vendas atacado/rota) por CD destino e por CD origem
    const capaDestinos = cdDestinos.length && eansNorm.length ? await dbQuery(
      `SELECT cap.cd_codigo,
              cap.cap_tipo,
              cap.cap_devol,
              SUM(CASE WHEN cap.cap_dtem >= CURRENT_DATE - INTERVAL '28 days'
                       THEN mi.ite_quan * (CASE WHEN cap.cap_devol='S' THEN -1 ELSE 1 END)
                       ELSE 0 END) AS qtd_28d,
              SUM(CASE WHEN cap.cap_dtem >= CURRENT_DATE - INTERVAL '90 days'
                       THEN mi.ite_quan * (CASE WHEN cap.cap_devol='S' THEN -1 ELSE 1 END)
                       ELSE 0 END) AS qtd_90d,
              COUNT(DISTINCT cap.cap_sequ) AS qtd_pedidos,
              MIN(cap.cap_dtem) AS primeira, MAX(cap.cap_dtem) AS ultima
         FROM cd_capa cap
         JOIN cd_moviitem mi ON mi.cd_codigo=cap.cd_codigo AND mi.cap_sequ=cap.cap_sequ
         JOIN cd_ean ce ON ce.cd_codigo=cap.cd_codigo AND ce.mat_codi=mi.pro_codi
        WHERE cap.cd_codigo = ANY($1::text[])
          AND cap.cap_dtem >= CURRENT_DATE - INTERVAL '90 days'
          AND NULLIF(LTRIM(ce.ean_codi,'0'),'') = ANY($2::text[])
        GROUP BY cap.cd_codigo, cap.cap_tipo, cap.cap_devol
        ORDER BY cap.cd_codigo, cap.cap_tipo`, [cdDestinos.map(d => d.cd_codigo), eansNorm]) : [];

    const capaOrigem = await dbQuery(
      `SELECT cap.cap_tipo, cap.cap_devol,
              COUNT(DISTINCT cap.cap_sequ) AS qtd_pedidos,
              SUM(CASE WHEN cap.cap_dtem >= CURRENT_DATE - INTERVAL '28 days'
                       THEN mi.ite_quan * (CASE WHEN cap.cap_devol='S' THEN -1 ELSE 1 END)
                       ELSE 0 END) AS qtd_28d,
              MIN(cap.cap_dtem) AS primeira, MAX(cap.cap_dtem) AS ultima
         FROM cd_capa cap
         JOIN cd_moviitem mi ON mi.cd_codigo=cap.cd_codigo AND mi.cap_sequ=cap.cap_sequ
        WHERE cap.cd_codigo=$1 AND mi.pro_codi=$2
          AND cap.cap_dtem >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY cap.cap_tipo, cap.cap_devol`, [cdOrigem, matCodi]);

    res.json({
      cd_origem: cdOrigem,
      produto,
      eans_normalizados: eansNorm,
      destinos_lojas: lojaIds,
      destinos_cds: cdDestinos.map(c => c.cd_codigo),
      vendas_pos_lojas: vendasPos,
      saidas_cds_destino_movcompra: saidasCds,
      saidas_cd_origem_por_nop: saidasOrigem,
      vendas_capa_destinos: capaDestinos,
      vendas_capa_origem: capaOrigem,
    });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /debug-listar-tabelas?cd_codigo=&q= — lista tabelas com nome contendo q
router.get('/debug-listar-tabelas', adminOuCeo, async (req, res) => {
  try {
    const cd = String(req.query.cd_codigo || 'srv1-itautuba').trim();
    const q = String(req.query.q || 'compra').trim();
    const { clientePorCodigo } = require('../cds');
    const cli = await clientePorCodigo(cd);
    const t = await cli.listarTabelas(q);
    res.json({ cd_codigo: cd, q, tabelas: t.tabelas });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /debug-compras-colunas?cd_codigo= — lista colunas da tabela COMPRAS + 3 linhas sample
router.get('/debug-compras-colunas', adminOuCeo, async (req, res) => {
  try {
    const cd = String(req.query.cd_codigo || 'srv1-itautuba').trim();
    const { clientePorCodigo } = require('../cds');
    const cli = await clientePorCodigo(cd);
    const cols = await cli.colunas('COMPRAS');
    const sample = await cli.query(`SELECT TOP 3 * FROM COMPRAS WITH (NOLOCK) ORDER BY MCP_CODI DESC`);
    const max = await cli.query(`SELECT MAX(MCP_CODI) AS max_mcp, MIN(MCP_CODI) AS min_mcp, COUNT(*) AS total FROM COMPRAS WITH (NOLOCK)`);
    res.json({ cd_codigo: cd, colunas: cols.colunas, sample: sample.rows, range: max.rows[0] });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /sync-transf-erros/limpar — apaga erros antigos (admin)
router.post('/sync-transf-erros/limpar', autenticar, async (req, res) => {
  if (!['admin','ceo'].includes(req.usuario.perfil)) return res.status(403).json({ erro: 'apenas admin' });
  try {
    const r = await dbQuery(`DELETE FROM sync_transf_multi_erros RETURNING id`);
    res.json({ ok: true, removidos: r.length });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /sync-transf-erros?cd_codigo= — analise dos erros agrupados
router.get('/sync-transf-erros', autenticar, async (req, res) => {
  if (!['admin','ceo'].includes(req.usuario.perfil)) return res.status(403).json({ erro: 'apenas admin' });
  try {
    const cd = req.query.cd_codigo || null;
    const params = [];
    let where = '1=1';
    if (cd) { params.push(cd); where += ` AND cd_codigo = $${params.length}`; }
    // Top 10 mensagens de erro mais comuns
    const agrupado = await dbQuery(
      `SELECT cd_codigo,
              -- Normaliza mensagem (corta prefixos com IDs pra agrupar)
              REGEXP_REPLACE(erro, '\\(.*?\\)', '(...)', 'g') AS erro_normalizado,
              COUNT(*)::int AS qtd,
              MAX(criado_em) AS ultimo
         FROM sync_transf_multi_erros
        WHERE ${where}
        GROUP BY cd_codigo, erro_normalizado
        ORDER BY qtd DESC LIMIT 20`, params);
    // Sample de 5 erros recentes
    const sample = await dbQuery(
      `SELECT * FROM sync_transf_multi_erros WHERE ${where}
        ORDER BY criado_em DESC LIMIT 5`, params);
    const total = await dbQuery(
      `SELECT cd_codigo, COUNT(*)::int AS total FROM sync_transf_multi_erros
        WHERE ${where} GROUP BY cd_codigo`, params);
    res.json({ total, agrupado, sample });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /refresh-ranking-cache — força recálculo do cache de ranking (admin)
router.post('/refresh-ranking-cache', autenticar, async (req, res) => {
  if (!['admin','ceo'].includes(req.usuario.perfil)) return res.status(403).json({ erro: 'apenas admin' });
  try {
    const { atualizarRankingCacheAll } = require('../ranking_cache');
    const r = await atualizarRankingCacheAll();
    res.json({ ok: true, resultados: r });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /transferencias-multi-status — totais por CD origem
router.get('/transferencias-multi-status', autenticar, async (req, res) => {
  if (!['admin','ceo'].includes(req.usuario.perfil)) return res.status(403).json({ erro: 'apenas admin' });
  try {
    const totaisPorOrigem = await dbQuery(`
      SELECT origem_cd_codigo, COUNT(*)::int AS total,
             COUNT(*) FILTER (WHERE cd_destino_codigo IS NOT NULL)::int AS para_cd,
             COUNT(*) FILTER (WHERE loja_id IS NOT NULL)::int AS para_loja,
             MIN(data_emissao) AS data_min,
             MAX(data_emissao) AS data_max
        FROM notas_entrada
       WHERE origem_cd_codigo IS NOT NULL
       GROUP BY origem_cd_codigo ORDER BY origem_cd_codigo`);
    const erros = await dbQuery(`
      SELECT cd_codigo, COUNT(*)::int AS total_erros,
             MAX(criado_em) AS ultimo_erro_em
        FROM sync_transf_multi_erros
       GROUP BY cd_codigo`);
    res.json({ totais_por_origem: totaisPorOrigem, erros });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /debug-status-ultrasyst — checa MCP_STATUS atual de notas antigas em_transito
router.get('/debug-status-ultrasyst', autenticar, async (req, res) => {
  if (!['admin','ceo'].includes(req.usuario.perfil)) return res.status(403).json({ erro: 'apenas admin' });
  try {
    const cd = req.query.cd_codigo || 'srv2-asafrio';
    const loja = parseInt(req.query.loja_id || '4');
    const { clientePorCodigo } = require('../cds');
    const cli = await clientePorCodigo(cd);
    const notas = await dbQuery(
      `SELECT id, cd_mov_codi, data_emissao
         FROM notas_entrada
        WHERE origem_cd_codigo = $1 AND loja_id = $2 AND status = 'em_transito'
        ORDER BY data_emissao ASC LIMIT 10`, [cd, loja]);
    if (!notas.length) return res.json({ erro: 'sem notas' });
    const codigos = notas.map(n => `'${n.cd_mov_codi}'`).join(',');
    const remoto = await cli.query(
      `SELECT MCP_CODI, MCP_STATUS, MCP_DTEM, MCP_NNOTAFIS
         FROM TBMOVCOMPRA WITH (NOLOCK)
        WHERE MCP_TIPOMOV='S' AND MCP_CODI IN (${codigos})`);
    const porMcp = {};
    for (const r of remoto.rows || []) porMcp[r.MCP_CODI] = r;
    const combinado = notas.map(n => ({
      nota_id: n.id,
      cd_mov_codi: n.cd_mov_codi,
      data_emissao_local: n.data_emissao,
      ultrasyst: porMcp[n.cd_mov_codi] || '(nao existe mais no UltraSyst)',
    }));
    res.json({ cd, loja, total_no_filtro: notas.length, amostra: combinado });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /painel-cd-saidas?cd_origem=X&mes=YYYY-MM — painel de transferências enviadas pelas lojas
router.get('/painel-cd-saidas', autenticar, async (req, res) => {
  try {
    const cdOrigem = String(req.query.cd_origem || '').trim();
    const mes = String(req.query.mes || new Date().toISOString().slice(0,7)); // YYYY-MM
    if (!cdOrigem) return res.status(400).json({ erro: 'cd_origem obrigatorio' });

    // Após backfill 2026-05-13, TODAS as notas CD têm origem_cd_codigo preenchido.
    const filtroNota = `(n.origem_cd_codigo = $1)`;
    const params = [cdOrigem, mes];

    async function agregaStatus(condStatus, paramsLocal) {
      return dbQuery(`
        SELECT n.loja_id,
               COUNT(*)::int AS qtd_notas,
               COALESCE(SUM(n.valor_total),0)::numeric(14,2) AS valor_total,
               COALESCE(SUM((
                 SELECT SUM(COALESCE(i.quantidade,0) * COALESCE(cm.peso_liquido_kg,0))
                   FROM itens_nota i
                   LEFT JOIN cd_material cm ON cm.cd_codigo = $1 AND cm.mat_codi = i.cd_pro_codi
                  WHERE i.nota_id = n.id
               )),0)::numeric(14,3) AS peso_kg
          FROM notas_entrada n
         WHERE ${filtroNota}
           AND n.loja_id IS NOT NULL
           AND ${condStatus}
         GROUP BY n.loja_id`, paramsLocal);
    }

    // Em trânsito: status='em_transito' (sem filtro de mês)
    // Em tratamento: já saiu do trânsito mas ainda não validou (sem filtro de mês)
    // Recebidas no mês: validada no mês selecionado
    // Em trânsito + Em tratamento = T da grade /pedidos-distribuidora (tudo que ainda não chegou no ERP)
    const trans = await agregaStatus(`n.status = 'em_transito'`, [cdOrigem]);
    const trat = await agregaStatus(
      `n.status IN ('recebida','em_conferencia','conferida','auditagem','aguardando_admin_validade','aguardando_devolucao')`,
      [cdOrigem]);
    const receb = await agregaStatus(
      `n.status = 'validada'
       AND TO_CHAR(COALESCE(n.data_recebimento, n.data_emissao),'YYYY-MM') = $2`,
      [cdOrigem, mes]);

    function agregar(rows) {
      let qtd = 0, valor = 0, peso = 0;
      for (const r of rows) {
        qtd += r.qtd_notas;
        valor += parseFloat(r.valor_total||0);
        peso += parseFloat(r.peso_kg||0);
      }
      return { qtd, valor, peso };
    }

    res.json({
      cd_origem: cdOrigem,
      mes,
      totais: {
        em_transito: agregar(trans),
        em_tratamento: agregar(trat),
        recebidas_mes: agregar(receb),
      },
      por_loja: { em_transito: trans, em_tratamento: trat, recebidas_mes: receb },
    });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /painel-cd-saidas-detalhe?cd_origem=&loja_id=&mes=&tipo= — lista notas detalhadas
router.get('/painel-cd-saidas-detalhe', autenticar, async (req, res) => {
  try {
    const cdOrigem = String(req.query.cd_origem || '').trim();
    const lojaId = parseInt(req.query.loja_id);
    const mes = String(req.query.mes || new Date().toISOString().slice(0,7));
    const tipo = String(req.query.tipo || 'em_transito');
    if (!cdOrigem || !lojaId) return res.status(400).json({ erro: 'cd_origem e loja_id obrigatorios' });

    const filtroNota = `(n.origem_cd_codigo = $1)`;

    let condStatus, params;
    if (tipo === 'em_transito') {
      condStatus = `n.status = 'em_transito'`;
      params = [cdOrigem, lojaId];
    } else if (tipo === 'em_tratamento') {
      condStatus = `n.status IN ('recebida','em_conferencia','conferida','auditagem','aguardando_admin_validade','aguardando_devolucao')`;
      params = [cdOrigem, lojaId];
    } else {
      condStatus = `n.status = 'validada'
                    AND TO_CHAR(COALESCE(n.data_recebimento, n.data_emissao),'YYYY-MM') = $3`;
      params = [cdOrigem, lojaId, mes];
    }

    const notas = await dbQuery(`
      SELECT n.id, n.cd_mov_codi, n.numero_nota, n.data_emissao, n.data_recebimento,
             n.valor_total, n.status,
             (SELECT COALESCE(SUM(COALESCE(i.quantidade,0) * COALESCE(cm.peso_liquido_kg,0)),0)
                FROM itens_nota i
                LEFT JOIN cd_material cm ON cm.cd_codigo = $1 AND cm.mat_codi = i.cd_pro_codi
               WHERE i.nota_id = n.id) AS peso_kg
        FROM notas_entrada n
       WHERE ${filtroNota}
         AND n.loja_id = $2
         AND ${condStatus}
       ORDER BY n.data_emissao DESC NULLS LAST
       LIMIT 500`, params);
    res.json({ cd_origem: cdOrigem, loja_id: lojaId, tipo, mes, notas });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// ── PAINEL CD→CD: cross-check transferências entre CDs (leitura) ────────────────
// SLA = 10 dias. Após esse prazo sem entrada no destino → "trânsito perdido" = AUDITAR.

const SLA_CD_CD_DIAS = 10;

// GET /painel-cd-cd-resumo (PÚBLICO) — números agregados, leitura pura, sem auth.
router.get('/painel-cd-cd-resumo', async (req, res) => {
  try {
    const rows = await dbQuery(`
      SELECT origem_cd_codigo, cd_destino_codigo, status, COUNT(*)::int AS qtd,
             COALESCE(SUM(valor_total),0)::numeric(14,2) AS valor
        FROM notas_entrada
       WHERE cd_destino_codigo IS NOT NULL AND loja_id IS NULL
       GROUP BY origem_cd_codigo, cd_destino_codigo, status
       ORDER BY origem_cd_codigo, cd_destino_codigo, status`);
    res.json({ atualizado_em: new Date().toISOString(), rows });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /painel-cd-cd-html (PÚBLICO, sem JS) — página server-rendered direto do banco.
// Imune a qualquer problema de cache/token/JS. Versão definitiva pra acompanhar.
router.get('/painel-cd-cd-html', async (req, res) => {
  try {
    const NOMES = {
      'srv1-itautuba':'Asa Branca ITB','srv1-nprogresso':'N. Progresso',
      'srv2-asafrio':'Asa Frios ITB','srv2-asasantarem':'Asa Frios STM',
    };
    const SLA = SLA_CD_CD_DIAS;
    // Matriz por par × status com contagens e valores
    const matriz = await dbQuery(`
      SELECT origem_cd_codigo, cd_destino_codigo,
             COUNT(*) FILTER (WHERE status='em_transito' AND CURRENT_DATE - data_emissao <= $1)::int AS trans,
             COUNT(*) FILTER (WHERE status='em_transito' AND CURRENT_DATE - data_emissao > $1)::int AS perdido,
             COUNT(*) FILTER (WHERE status IN ('recebida','em_conferencia','conferida','validada','auditagem'))::int AS recebida,
             COUNT(*) FILTER (WHERE status='cancelada')::int AS cancelada,
             COALESCE(SUM(valor_total) FILTER (WHERE status='em_transito' AND CURRENT_DATE - data_emissao > $1),0)::numeric(14,2) AS val_perdido,
             COALESCE(SUM(valor_total) FILTER (WHERE status IN ('recebida','em_conferencia','conferida','validada','auditagem')),0)::numeric(14,2) AS val_recebida,
             ROUND(AVG(data_recebimento - data_emissao) FILTER (WHERE data_recebimento IS NOT NULL),1) AS sla_medio
        FROM notas_entrada
       WHERE cd_destino_codigo IS NOT NULL AND loja_id IS NULL
       GROUP BY origem_cd_codigo, cd_destino_codigo
       ORDER BY origem_cd_codigo, cd_destino_codigo`, [SLA]);
    const totais = matriz.reduce((acc, r) => ({
      trans: acc.trans + r.trans, perdido: acc.perdido + r.perdido,
      recebida: acc.recebida + r.recebida, cancelada: acc.cancelada + r.cancelada,
      val_perdido: acc.val_perdido + parseFloat(r.val_perdido||0),
      val_recebida: acc.val_recebida + parseFloat(r.val_recebida||0),
    }), {trans:0,perdido:0,recebida:0,cancelada:0,val_perdido:0,val_recebida:0});

    const fmtBRL = v => 'R$ ' + Number(v||0).toLocaleString('pt-BR', {minimumFractionDigits:2});
    const linhas = matriz.map(r => `
      <tr>
        <td><b>${NOMES[r.origem_cd_codigo]||r.origem_cd_codigo}</b> → ${NOMES[r.cd_destino_codigo]||r.cd_destino_codigo}</td>
        <td class="n">${r.trans||'—'}</td>
        <td class="n perdido">${r.perdido||'—'}</td>
        <td class="n recebida">${r.recebida||'—'}</td>
        <td class="n">${r.cancelada||'—'}</td>
        <td class="n">${r.sla_medio?r.sla_medio+'d':'—'}</td>
        <td class="n">${fmtBRL(r.val_recebida)}</td>
      </tr>`).join('');

    res.set('Cache-Control', 'no-store');
    res.send(`<!doctype html><html><head><meta charset="utf-8"><title>Painel CD → CD</title>
<style>
body{font-family:system-ui;background:#0f172a;color:#e2e8f0;margin:0;padding:20px}
h1{font-size:20px;margin-bottom:6px}
.aviso{padding:10px;background:#1e293b;border-left:4px solid #0ea5e9;margin:10px 0;font-size:13px;color:#94a3b8}
.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:14px 0}
.card{background:#1e293b;border:1px solid #334155;border-radius:8px;padding:14px}
.card.t{border-left:4px solid #fbbf24}
.card.p{border-left:4px solid #ef4444}
.card.r{border-left:4px solid #34d399}
.card.c{border-left:4px solid #94a3b8}
.card h2{font-size:11px;color:#94a3b8;text-transform:uppercase;margin-bottom:6px}
.card .big{font-size:28px;font-weight:700;color:#fde047}
.card .sub{font-size:11px;color:#94a3b8;margin-top:4px}
table{width:100%;border-collapse:collapse;background:#1e293b;border-radius:6px;overflow:hidden;font-size:13px}
th{background:#0c1628;padding:10px;text-align:left;color:#94a3b8;font-size:11px;text-transform:uppercase}
td{padding:8px 10px;border-top:1px solid #334155}
td.n{text-align:right;font-variant-numeric:tabular-nums}
.perdido{color:#fca5a5;font-weight:700}
.recebida{color:#86efac;font-weight:700}
small{color:#64748b}
a{color:#38bdf8;text-decoration:none}
</style></head><body>
<h1>Painel CD → CD <small style="font-size:12px;color:#64748b">(sem JS, server-rendered)</small></h1>
<div class="aviso">Cruza saídas (MCP_TIPOMOV='S') do CD origem com entradas (MCP_TIPOMOV='E') no destino pelo número do pedido. <b>SLA: ${SLA} dias</b>. <a href="/painel-cd-cd">← Voltar à versão interativa</a></div>
<div class="cards">
  <div class="card t"><h2>⏳ Em trânsito (≤${SLA}d)</h2><div class="big">${totais.trans}</div></div>
  <div class="card p"><h2>🚨 Trânsito perdido (>${SLA}d)</h2><div class="big">${totais.perdido}</div><div class="sub">${fmtBRL(totais.val_perdido)}</div></div>
  <div class="card r"><h2>✓ Recebidas</h2><div class="big">${totais.recebida}</div><div class="sub">${fmtBRL(totais.val_recebida)}</div></div>
  <div class="card c"><h2>✗ Canceladas</h2><div class="big">${totais.cancelada}</div></div>
</div>
<table>
  <thead><tr>
    <th>Rota (origem → destino)</th>
    <th class="n" style="color:#fbbf24">⏳ Trânsito</th>
    <th class="n" style="color:#ef4444">🚨 Perdido</th>
    <th class="n" style="color:#34d399">✓ Recebida</th>
    <th class="n">✗ Cancelada</th>
    <th class="n">SLA médio</th>
    <th class="n">Valor recebido</th>
  </tr></thead>
  <tbody>${linhas}</tbody>
</table>
<p style="color:#64748b;font-size:11px;margin-top:14px">Atualizado: ${new Date().toLocaleString('pt-BR')} • Total de rotas: ${matriz.length}</p>
</body></html>`);
  } catch (e) { res.status(500).send('Erro: ' + e.message); }
});

// GET /painel-cd-cd?cd_origem=&cd_destino=&mes=YYYY-MM
// Retorna 4 cards (em_transito, recebida, transito_perdido, pendencia_origem) por (origem, destino)
router.get('/painel-cd-cd', autenticar, async (req, res) => {
  try {
    const cdOrigem = String(req.query.cd_origem || '').trim() || null;
    const cdDestino = String(req.query.cd_destino || '').trim() || null;
    const mes = String(req.query.mes || new Date().toISOString().slice(0,7));

    // Filtros dinâmicos
    const wheres = [`n.cd_destino_codigo IS NOT NULL`, `n.loja_id IS NULL`];
    const params = [];
    if (cdOrigem)  { params.push(cdOrigem);  wheres.push(`n.origem_cd_codigo = $${params.length}`); }
    if (cdDestino) { params.push(cdDestino); wheres.push(`n.cd_destino_codigo = $${params.length}`); }
    const filtro = wheres.join(' AND ');

    // Em trânsito ≤ SLA: continua sendo "trânsito normal"
    // Em trânsito > SLA sem cd_mov_codi → "trânsito perdido"
    // Em trânsito sem NF (cd_mov_codi NULL) → "pendência no origem"
    const cards = await dbQuery(`
      SELECT n.origem_cd_codigo, n.cd_destino_codigo,
             COUNT(*) FILTER (
               WHERE n.status='em_transito' AND n.cd_mov_codi IS NOT NULL
                 AND CURRENT_DATE - n.data_emissao <= $${params.length+1}
             )::int AS qtd_em_transito,
             COALESCE(SUM(n.valor_total) FILTER (
               WHERE n.status='em_transito' AND n.cd_mov_codi IS NOT NULL
                 AND CURRENT_DATE - n.data_emissao <= $${params.length+1}
             ),0)::numeric(14,2) AS val_em_transito,

             COUNT(*) FILTER (
               WHERE n.status='em_transito' AND n.cd_mov_codi IS NOT NULL
                 AND CURRENT_DATE - n.data_emissao > $${params.length+1}
             )::int AS qtd_perdido,
             COALESCE(SUM(n.valor_total) FILTER (
               WHERE n.status='em_transito' AND n.cd_mov_codi IS NOT NULL
                 AND CURRENT_DATE - n.data_emissao > $${params.length+1}
             ),0)::numeric(14,2) AS val_perdido,

             COUNT(*) FILTER (
               WHERE n.status='em_transito' AND n.cd_mov_codi IS NULL
             )::int AS qtd_pendencia,
             COALESCE(SUM(n.valor_total) FILTER (
               WHERE n.status='em_transito' AND n.cd_mov_codi IS NULL
             ),0)::numeric(14,2) AS val_pendencia,

             COUNT(*) FILTER (
               WHERE n.status IN ('recebida','em_conferencia','conferida','validada','auditagem')
                 AND TO_CHAR(COALESCE(n.data_recebimento,n.data_emissao),'YYYY-MM') = $${params.length+2}
             )::int AS qtd_recebida,
             COALESCE(SUM(n.valor_total) FILTER (
               WHERE n.status IN ('recebida','em_conferencia','conferida','validada','auditagem')
                 AND TO_CHAR(COALESCE(n.data_recebimento,n.data_emissao),'YYYY-MM') = $${params.length+2}
             ),0)::numeric(14,2) AS val_recebida,

             ROUND(AVG(n.data_recebimento - n.data_emissao) FILTER (
               WHERE n.status IN ('recebida','em_conferencia','conferida','validada','auditagem')
                 AND n.data_recebimento IS NOT NULL
                 AND TO_CHAR(COALESCE(n.data_recebimento,n.data_emissao),'YYYY-MM') = $${params.length+2}
             ),1) AS sla_medio_dias
        FROM notas_entrada n
       WHERE ${filtro}
       GROUP BY n.origem_cd_codigo, n.cd_destino_codigo
       ORDER BY n.origem_cd_codigo, n.cd_destino_codigo`,
      [...params, SLA_CD_CD_DIAS, mes]);

    // Agrega totais gerais
    let tot = { em_transito:{qtd:0,val:0}, perdido:{qtd:0,val:0}, pendencia:{qtd:0,val:0}, recebida:{qtd:0,val:0} };
    for (const r of cards) {
      tot.em_transito.qtd += r.qtd_em_transito; tot.em_transito.val += parseFloat(r.val_em_transito||0);
      tot.perdido.qtd     += r.qtd_perdido;     tot.perdido.val     += parseFloat(r.val_perdido||0);
      tot.pendencia.qtd   += r.qtd_pendencia;   tot.pendencia.val   += parseFloat(r.val_pendencia||0);
      tot.recebida.qtd    += r.qtd_recebida;    tot.recebida.val    += parseFloat(r.val_recebida||0);
    }

    res.json({ cd_origem: cdOrigem, cd_destino: cdDestino, mes, sla_dias: SLA_CD_CD_DIAS, totais: tot, por_par: cards });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /painel-cd-cd-detalhe?cd_origem=&cd_destino=&mes=&tipo=
// tipo: em_transito | perdido | pendencia | recebida
router.get('/painel-cd-cd-detalhe', autenticar, async (req, res) => {
  try {
    const cdOrigem = String(req.query.cd_origem || '').trim() || null;
    const cdDestino = String(req.query.cd_destino || '').trim() || null;
    const mes = String(req.query.mes || new Date().toISOString().slice(0,7));
    const tipo = String(req.query.tipo || 'em_transito');

    const wheres = [`n.cd_destino_codigo IS NOT NULL`, `n.loja_id IS NULL`];
    const params = [];
    if (cdOrigem)  { params.push(cdOrigem);  wheres.push(`n.origem_cd_codigo = $${params.length}`); }
    if (cdDestino) { params.push(cdDestino); wheres.push(`n.cd_destino_codigo = $${params.length}`); }

    if (tipo === 'em_transito') {
      params.push(SLA_CD_CD_DIAS);
      wheres.push(`n.status='em_transito' AND n.cd_mov_codi IS NOT NULL AND CURRENT_DATE - n.data_emissao <= $${params.length}`);
    } else if (tipo === 'perdido') {
      params.push(SLA_CD_CD_DIAS);
      wheres.push(`n.status='em_transito' AND n.cd_mov_codi IS NOT NULL AND CURRENT_DATE - n.data_emissao > $${params.length}`);
    } else if (tipo === 'pendencia') {
      wheres.push(`n.status='em_transito' AND n.cd_mov_codi IS NULL`);
    } else {
      params.push(mes);
      wheres.push(`n.status IN ('recebida','em_conferencia','conferida','validada','auditagem')
                   AND TO_CHAR(COALESCE(n.data_recebimento,n.data_emissao),'YYYY-MM') = $${params.length}`);
    }

    const notas = await dbQuery(`
      SELECT n.id, n.origem_cd_codigo, n.cd_destino_codigo, n.cd_mov_codi, n.numero_nota,
             n.data_emissao, n.data_recebimento, n.valor_total, n.status,
             (CURRENT_DATE - n.data_emissao) AS dias_emissao,
             CASE
               WHEN n.data_recebimento IS NOT NULL THEN (n.data_recebimento - n.data_emissao)
               ELSE NULL
             END AS sla_dias,
             (SELECT COUNT(*)::int FROM itens_nota WHERE nota_id = n.id) AS qtd_itens
        FROM notas_entrada n
       WHERE ${wheres.join(' AND ')}
       ORDER BY n.data_emissao DESC NULLS LAST
       LIMIT 500`, params);
    res.json({ tipo, mes, notas });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /painel-cd-cd-nota-itens?nota_id=
router.get('/painel-cd-cd-nota-itens', autenticar, async (req, res) => {
  try {
    const id = parseInt(req.query.nota_id);
    if (!id) return res.status(400).json({ erro: 'nota_id obrigatorio' });
    const itens = await dbQuery(`
      SELECT i.numero_item, i.cd_pro_codi, i.ean_nota, i.descricao_nota,
             i.quantidade, i.preco_unitario_nota, i.preco_total_nota
        FROM itens_nota i WHERE i.nota_id = $1 ORDER BY i.numero_item`, [id]);
    res.json({ itens });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /match-cd-cd — força match agora (admin)
router.post('/match-cd-cd', autenticar, async (req, res) => {
  if (!['admin','ceo'].includes(req.usuario.perfil)) return res.status(403).json({ erro: 'apenas admin' });
  try {
    const { matchTransferenciasCdCdRecebidas } = require('../sync_transferencias_multi');
    const r = await matchTransferenciasCdCdRecebidas();
    res.json({ ok: true, ...r });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /ressync-transferencias-multi — verifica MCP_STATUS no CD e marca canceladas (admin)
router.post('/ressync-transferencias-multi', autenticar, async (req, res) => {
  if (!['admin','ceo'].includes(req.usuario.perfil)) return res.status(403).json({ erro: 'apenas admin' });
  try {
    const { ressincronizarTransferenciasMultiAbertas } = require('../sync_transferencias_multi');
    const r = await ressincronizarTransferenciasMultiAbertas();
    res.json({ ok: true, ...r });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /match-transferencias-multi — força match com compras_historico (admin)
router.post('/match-transferencias-multi', autenticar, async (req, res) => {
  if (!['admin','ceo'].includes(req.usuario.perfil)) return res.status(403).json({ erro: 'apenas admin' });
  try {
    const { matchTransferenciasMultiRecebidas } = require('../sync_transferencias_multi');
    const r = await matchTransferenciasMultiRecebidas();
    res.json({ ok: true, ...r });
  } catch (e) { res.status(500).json({ erro: e.message, stack: e.stack }); }
});

// GET /debug-nota-match?nota_id= — investiga uma nota específica
router.get('/debug-nota-match', autenticar, async (req, res) => {
  if (!['admin','ceo'].includes(req.usuario.perfil)) return res.status(403).json({ erro: 'apenas admin' });
  try {
    const id = parseInt(req.query.nota_id);
    const nota = await dbQuery(
      `SELECT id, cd_mov_codi, loja_id, status, origem_cd_codigo, fornecedor_cnpj,
              REGEXP_REPLACE(cd_mov_codi,'^0+','') AS num_norm
         FROM notas_entrada WHERE id=$1`, [id]);
    if (!nota[0]) return res.json({ erro: 'nota não existe' });
    const n = nota[0];
    // Busca em compras_historico SEM filtro de cnpj
    const matches = await dbQuery(
      `SELECT loja_id, numeronfe,
              REGEXP_REPLACE(numeronfe,'^0+','') AS num_norm,
              fornecedor_cnpj, data_entrada
         FROM compras_historico
        WHERE loja_id = $1 AND REGEXP_REPLACE(numeronfe,'^0+','') = $2
        LIMIT 10`, [n.loja_id, n.num_norm]);
    res.json({ nota: n, compras_historico_matches: matches });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /debug-match-com-cnpj — testa a query EXATA do detector pra UM (cd, loja)
router.get('/debug-match-com-cnpj', autenticar, async (req, res) => {
  if (!['admin','ceo'].includes(req.usuario.perfil)) return res.status(403).json({ erro: 'apenas admin' });
  try {
    const cd = req.query.cd_codigo || 'srv2-asafrio';
    const loja = parseInt(req.query.loja_id || '4');
    const cnpjInfo = await dbQuery(
      `SELECT REGEXP_REPLACE(COALESCE(cnpj,''),'\\D','','g') AS cnpj_n
         FROM pedidos_distrib_destinos WHERE tipo='CD' AND cd_codigo=$1`, [cd]);
    const cnpjN = cnpjInfo[0]?.cnpj_n;
    // Quantas notas pendentes
    const pendentes = await dbQuery(
      `SELECT COUNT(*)::int AS qtd FROM notas_entrada
        WHERE origem_cd_codigo = $1 AND loja_id = $2 AND status = 'em_transito'`,
      [cd, loja]);
    // Quantas casariam (com regex)
    const matchExp = await dbQuery(
      `WITH notas_pendentes AS (
         SELECT id, cd_mov_codi, REGEXP_REPLACE(cd_mov_codi,'^0+','') AS num_norm
           FROM notas_entrada
          WHERE origem_cd_codigo = $1 AND loja_id = $2 AND status = 'em_transito'
            AND cd_mov_codi IS NOT NULL
       )
       SELECT n.id AS nota_id, c.numeronfe, c.fornecedor_cnpj,
              REGEXP_REPLACE(COALESCE(c.fornecedor_cnpj,''),'\\D','','g') AS forn_norm,
              c.data_entrada
         FROM notas_pendentes n
         JOIN compras_historico c
           ON c.loja_id = $2
          AND REGEXP_REPLACE(c.numeronfe,'^0+','') = n.num_norm
        LIMIT 5`, [cd, loja]);
    res.json({ cd, loja, cnpj_destino: cnpjN, pendentes_count: pendentes[0].qtd, sample_matches: matchExp });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /debug-match-multi — investiga por que match não casa
router.get('/debug-match-multi', autenticar, async (req, res) => {
  if (!['admin','ceo'].includes(req.usuario.perfil)) return res.status(403).json({ erro: 'apenas admin' });
  try {
    const cd = req.query.cd_codigo || 'srv2-asafrio';
    // 1) CNPJ do CD em pedidos_distrib_destinos
    const cdInfo = await dbQuery(
      `SELECT codigo, nome, cnpj, REGEXP_REPLACE(COALESCE(cnpj,''),'\\D','','g') AS cnpj_n
         FROM pedidos_distrib_destinos WHERE tipo='CD' AND cd_codigo=$1`, [cd]);
    // 2) Sample de 5 notas em_transito desse CD
    const notas = await dbQuery(
      `SELECT id, cd_mov_codi, loja_id, status, fornecedor_cnpj, origem_cd_codigo
         FROM notas_entrada
        WHERE origem_cd_codigo=$1 AND loja_id IS NOT NULL AND status='em_transito'
        ORDER BY id DESC LIMIT 5`, [cd]);
    // 3) Pra cada uma, busca em compras_historico
    const matches = [];
    for (const n of notas) {
      const r = await dbQuery(
        `SELECT loja_id, numeronfe, fornecedor_cnpj, data_entrada
           FROM compras_historico
          WHERE loja_id = $1
            AND REGEXP_REPLACE(numeronfe,'^0+','') = REGEXP_REPLACE($2,'^0+','')
          ORDER BY data_entrada DESC LIMIT 3`, [n.loja_id, n.cd_mov_codi]);
      matches.push({ nota: n, compras_achadas: r });
    }
    res.json({ cd_info: cdInfo[0], notas_em_transito_sample: notas, matches });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /sync-transferencias-multi — força sync de transferências dos outros CDs (admin)
router.post('/sync-transferencias-multi', autenticar, async (req, res) => {
  if (!['admin','ceo'].includes(req.usuario.perfil)) return res.status(403).json({ erro: 'apenas admin' });
  try {
    const { syncTransferenciasMulti } = require('../sync_transferencias_multi');
    const r = await syncTransferenciasMulti();
    res.json({ ok: true, resultados: r });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /debug-loc-estoque?cd_codigo= — mostra LOC_CODIs reais com estoque no servidor remoto
router.get('/debug-loc-estoque', adminOuCeo, async (req, res) => {
  try {
    const cd = String(req.query.cd_codigo || '').trim();
    if (!cd) return res.status(400).json({ erro: 'cd_codigo obrigatorio' });
    const { clientePorCodigo, getCd } = require('../cds');
    const cli = await clientePorCodigo(cd);
    const cfg = await getCd(cd);
    const r1 = await cli.query(
      `SELECT EMP_CODI, LOC_CODI, COUNT(*) AS qtd, SUM(EST_QUAN) AS soma_est, COUNT(CASE WHEN EST_QUAN > 0 THEN 1 END) AS qtd_positivo
         FROM ESTOQUE WITH (NOLOCK)
         GROUP BY EMP_CODI, LOC_CODI
         ORDER BY soma_est DESC`);
    const r2 = await cli.query(
      `SELECT TOP 5 PRO_CODI, LOC_CODI, EMP_CODI, EST_QUAN
         FROM ESTOQUE WITH (NOLOCK)
        WHERE EST_QUAN > 0`);
    res.json({
      cd_codigo: cd,
      configurado: { emp_codi: cfg.emp_codi, loc_codi: cfg.loc_codi },
      por_loc_emp: r1.rows,
      sample_com_estoque: r2.rows,
    });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /debug-formato-codigos?cd_codigo= — mostra amostra de mat_codi/pro_codi/ean.mat_codi pra detectar mismatch
router.get('/debug-formato-codigos', adminOuCeo, async (req, res) => {
  try {
    const cd = String(req.query.cd_codigo || '').trim();
    if (!cd) return res.status(400).json({ erro: 'cd_codigo obrigatorio' });
    const sample_material = await dbQuery(
      `SELECT mat_codi, length(mat_codi) AS len, mat_desc FROM cd_material
        WHERE cd_codigo = $1 ORDER BY mat_codi LIMIT 5`, [cd]);
    const sample_estoque = await dbQuery(
      `SELECT pro_codi, length(pro_codi) AS len, est_quan FROM cd_estoque
        WHERE cd_codigo = $1 AND est_quan > 0 ORDER BY pro_codi LIMIT 5`, [cd]);
    const sample_ean = await dbQuery(
      `SELECT mat_codi, length(mat_codi) AS len, ean_codi FROM cd_ean
        WHERE cd_codigo = $1 ORDER BY mat_codi LIMIT 5`, [cd]);
    const join_test = await dbQuery(
      `SELECT COUNT(*)::int AS total
         FROM cd_material m
         JOIN cd_estoque e ON e.cd_codigo = m.cd_codigo AND e.pro_codi = m.mat_codi
        WHERE m.cd_codigo = $1`, [cd]);
    const join_loose = await dbQuery(
      `SELECT COUNT(*)::int AS total
         FROM cd_material m
         JOIN cd_estoque e ON e.cd_codigo = m.cd_codigo
                          AND LTRIM(e.pro_codi,'0') = LTRIM(m.mat_codi,'0')
        WHERE m.cd_codigo = $1`, [cd]);
    const join_int = await dbQuery(
      `SELECT COUNT(*)::int AS total
         FROM cd_material m
         JOIN cd_estoque e ON e.cd_codigo = m.cd_codigo
                          AND m.mat_codi ~ '^[0-9]+$' AND e.pro_codi ~ '^[0-9]+$'
                          AND m.mat_codi::bigint = e.pro_codi::bigint
        WHERE m.cd_codigo = $1`, [cd]);
    res.json({
      cd_codigo: cd,
      sample_material, sample_estoque, sample_ean,
      join_strict: join_test[0]?.total,
      join_ltrim_zeros: join_loose[0]?.total,
      join_bigint: join_int[0]?.total,
    });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /sync-cd-status (admin) — diagnóstico do sync_cd
router.get('/sync-cd-status', adminOuCeo, async (req, res) => {
  try {
    const { listarCds } = require('../cds');
    const cds = await listarCds(false); // todos (ativos e inativos)
    const cdsResumo = cds.map(c => ({
      codigo: c.codigo, nome: c.nome, ativo: c.ativo,
      tem_url: !!c.url, tem_token: !!c.token,
      banco: c.banco, emp_codi: c.emp_codi, loc_codi: c.loc_codi,
    }));

    const stats = await dbQuery(`
      SELECT 'cd_material' AS tabela, cd_codigo, COUNT(*)::int AS total
        FROM cd_material GROUP BY cd_codigo
      UNION ALL
      SELECT 'cd_ean', cd_codigo, COUNT(*) FROM cd_ean GROUP BY cd_codigo
      UNION ALL
      SELECT 'cd_estoque', cd_codigo, COUNT(*) FROM cd_estoque GROUP BY cd_codigo
      UNION ALL
      SELECT 'cd_custoprod', cd_codigo, COUNT(*) FROM cd_custoprod GROUP BY cd_codigo
      UNION ALL
      SELECT 'cd_movcompra', cd_codigo, COUNT(*) FROM cd_movcompra GROUP BY cd_codigo
      ORDER BY cd_codigo, tabela
    `);
    const ultimoSync = await dbQuery(
      `SELECT chave, valor FROM _sync_state WHERE chave LIKE 'cd_%_ultima_sync' ORDER BY chave`
    );
    res.json({ cds_cadastrados: cdsResumo, contagens: stats, ultimas_syncs: ultimoSync });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /material-codi-cols?cd_origem= — lista colunas com '_CODI' na MATERIAL
router.get('/material-codi-cols', adminOuCeo, async (req, res) => {
  try {
    const cdOrigem = String(req.query.cd_origem || 'srv1-itautuba').trim();
    const { clientePorCodigo } = require('../cds');
    const cli = await clientePorCodigo(cdOrigem);
    const cols = await cli.colunas('MATERIAL');
    const codiCols = (cols.colunas || []).filter(c =>
      /_CODI$|GRU|SUB|SGR|DPT|SEC|DEP|FAM|CAT|CLASS|GRP/i.test(c.COLUMN_NAME)
    );
    res.json({ cd_origem: cdOrigem, colunas: codiCols });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /descobrir-grupos?cd_origem= — descobre tabelas/colunas de grupo de produto no UltraSyst
router.get('/descobrir-grupos', adminOuCeo, async (req, res) => {
  try {
    const cdOrigem = String(req.query.cd_origem || 'srv1-itautuba').trim();
    const { clientePorCodigo } = require('../cds');
    const cli = await clientePorCodigo(cdOrigem);

    // Tabelas com nomes relacionados a grupo
    const tables = await cli.listarTabelas();
    const tabelasGrupo = (tables.tabelas || []).filter(t =>
      /gru|sub|class|categ|departa|secao|seca|sect|familia|famil/i.test(t.TABLE_NAME)
    );

    // Colunas da MATERIAL relacionadas a grupo
    const colsMaterial = await cli.colunas('MATERIAL');
    const colsGrupoMaterial = (colsMaterial.colunas || []).filter(c =>
      /gru|sub|class|categ|departa|secao|seca|sect|familia|famil/i.test(c.COLUMN_NAME)
    );

    // Pra cada tabela candidata, busca colunas
    const detalhes = {};
    for (const t of tabelasGrupo.slice(0, 20)) {
      try {
        const cols = await cli.colunas(t.TABLE_NAME);
        detalhes[t.TABLE_NAME] = cols.colunas;
      } catch (e) { detalhes[t.TABLE_NAME] = { erro: e.message }; }
    }

    res.json({
      cd_origem: cdOrigem,
      tabelas_grupo_candidatas: tabelasGrupo,
      material_colunas_grupo: colsGrupoMaterial,
      detalhes_tabelas: detalhes,
    });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /cd-colunas-material?cd_origem= — lista colunas da MATERIAL pra descobrir peso
router.get('/cd-colunas-material', adminOuCeo, async (req, res) => {
  try {
    const cdCodigo = String(req.query.cd_origem || '').trim();
    if (!cdCodigo) return res.status(400).json({ erro: 'cd_origem obrigatorio' });
    const { clientePorCodigo } = require('../cds');
    const cli = await clientePorCodigo(cdCodigo);
    const cols = await cli.colunas('MATERIAL');
    res.json(cols);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /sync-cd-now (admin) — força sync_cd imediato (todos CDs ativos)
router.post('/sync-cd-now', adminOuCeo, async (req, res) => {
  try {
    const { syncCdAll } = require('../sync_cd');
    const r = await syncCdAll();
    res.json({ ok: true, resultados: r });
  } catch (e) {
    console.error('[sync-cd-now]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// Trace — executa a MESMA logica do /grade limitada a 1 produto e retorna estados intermediarios
router.get('/grade-trace', adminOuCeo, async (req, res) => {
  try {
    const cdOrigem = String(req.query.cd_origem || '').trim();
    const matCodi  = String(req.query.mat_codi  || '').trim();
    if (!cdOrigem || !matCodi) return res.status(400).json({ erro: 'cd_origem e mat_codi obrigatorios' });

    const trace = {};

    const [origemRow] = await dbQuery(`SELECT cnpj FROM pedidos_distrib_destinos WHERE cd_codigo = $1`, [cdOrigem]);
    const cnpjOrigem = origemRow?.cnpj || '';
    const destinos = await dbQuery(
      `SELECT id, tipo, codigo, nome, cnpj, loja_id, cd_codigo
         FROM pedidos_distrib_destinos
        WHERE ativo = TRUE AND cnpj <> $1
        ORDER BY tipo DESC, nome`, [cnpjOrigem]
    );
    trace.lojaIds = destinos.filter(d => d.tipo === 'LOJA' && d.loja_id).map(d => d.loja_id);

    const produtos = await dbQuery(`
      SELECT cd_m.mat_codi, cd_m.ean_codi AS ean_cd_material,
             pe.ean_principal_cd, pe.qtd_embalagem,
             COALESCE(
               NULLIF(pe.ean_principal_cd,''),
               NULLIF(cd_m.ean_codi,''),
               (SELECT NULLIF(LTRIM(ean_codi,'0'),'') FROM cd_ean
                 WHERE cd_codigo = cd_m.cd_codigo AND mat_codi = cd_m.mat_codi
                 ORDER BY CASE WHEN ean_nota='S' THEN 0 ELSE 1 END, ordem LIMIT 1)
             ) AS ean_resolvido,
             cd_e.est_quan AS est_dist
        FROM cd_material cd_m
        LEFT JOIN cd_estoque cd_e ON cd_e.cd_codigo = cd_m.cd_codigo AND cd_e.pro_codi = cd_m.mat_codi
        LEFT JOIN produtos_embalagem pe ON pe.mat_codi = cd_m.mat_codi
       WHERE cd_m.cd_codigo = $1 AND cd_m.mat_codi = $2`,
      [cdOrigem, matCodi]
    );
    trace.produtos = produtos;
    if (!produtos.length) return res.json({ ...trace, erro: 'mat_codi nao existe nesse CD' });

    const ean = produtos[0].ean_resolvido;
    const eanNorm = String(ean || '').replace(/^0+/, '') || ean;
    trace.ean_resolvido = ean;
    trace.ean_normalizado = eanNorm;

    if (!ean) return res.json({ ...trace, erro: 'sem ean — não dá pra fazer match' });

    trace.estLojas = await dbQuery(
      `SELECT loja_id, NULLIF(LTRIM(codigobarra,'0'),'') AS codbarra,
              NULLIF(LTRIM(produtoprincipal,'0'),'') AS principal, estdisponivel, prsugerido
         FROM produtos_externo
        WHERE loja_id = ANY($1::int[])
          AND (NULLIF(LTRIM(codigobarra,'0'),'') = $2 OR NULLIF(LTRIM(produtoprincipal,'0'),'') = $2)`,
      [trace.lojaIds, eanNorm]
    );
    trace.vendas = await dbQuery(
      `SELECT loja_id, NULLIF(LTRIM(codigobarra,'0'),'') AS codbarra,
              SUM(qtd_vendida) AS qtd_total, MAX(data_venda) AS ultima
         FROM vendas_historico
        WHERE loja_id = ANY($1::int[])
          AND NULLIF(LTRIM(codigobarra,'0'),'') = $2
          AND data_venda >= CURRENT_DATE - INTERVAL '90 days'
          AND COALESCE(tipo_saida,'venda')='venda'
        GROUP BY loja_id, codbarra`,
      [trace.lojaIds, eanNorm]
    );
    trace.cd_ean_count = await dbQuery(
      `SELECT cd_codigo, COUNT(*)::int AS total FROM cd_ean GROUP BY cd_codigo ORDER BY cd_codigo`
    );

    res.json(trace);
  } catch (e) { res.status(500).json({ erro: e.message, stack: e.stack }); }
});

// Debug do trânsito: dado uma nota_id, lista itens + EAN vs EAN principal de produtos_embalagem
// GET /transito-debug?nota_id=X
router.get('/transito-debug', adminOuCeo, async (req, res) => {
  try {
    const notaId = parseInt(req.query.nota_id);
    if (!notaId) return res.status(400).json({ erro: 'nota_id obrigatorio' });
    const [nota] = await dbQuery(
      `SELECT id, numero_nota, cd_mov_codi, loja_id, status, origem,
              fornecedor_cnpj, fornecedor_nome, chegou_no_erp_em, mcp_status_cd
         FROM notas_entrada WHERE id = $1`, [notaId]
    );
    if (!nota) return res.status(404).json({ erro: 'nota nao encontrada' });
    const itens = await dbQuery(
      `SELECT i.id, i.numero_item, i.ean_nota, i.ean_validado, i.descricao_nota,
              i.quantidade, i.cd_pro_codi,
              NULLIF(LTRIM(COALESCE(i.ean_validado, i.ean_nota),'0'),'') AS ean_norm,
              pe.mat_codi AS produto_embalagem_mat_codi,
              pe.descricao_atual AS produto_embalagem_desc,
              pe.ean_principal_cd AS pe_ean_principal_cd,
              pe.qtd_embalagem
         FROM itens_nota i
         LEFT JOIN produtos_embalagem pe
           ON NULLIF(LTRIM(pe.ean_principal_cd,'0'),'') =
              NULLIF(LTRIM(COALESCE(i.ean_validado, i.ean_nota),'0'),'')
        WHERE i.nota_id = $1
        ORDER BY i.numero_item`, [notaId]
    );

    // Pra cada item, veja se aparece como trânsito (ie, query da grade pegaria)
    const filtroOk =
      ['cd','transferencia_loja'].includes(nota.origem) &&
      !['fechada','validada','arquivada','cancelada','finalizada_f'].includes(nota.status) &&
      (nota.mcp_status_cd || 'A') !== 'C' &&
      nota.chegou_no_erp_em == null;

    res.json({
      nota,
      filtro_passa: filtroOk,
      motivos_filtro: filtroOk ? ['nota passa no filtro de trânsito ✓'] : [
        ...(['cd','transferencia_loja'].includes(nota.origem) ? [] : [`origem='${nota.origem}' não é cd/transferencia_loja`]),
        ...(['fechada','validada','arquivada','cancelada','finalizada_f'].includes(nota.status) ? [`status='${nota.status}' está na lista de exclusão`] : []),
        ...((nota.mcp_status_cd || 'A') === 'C' ? [`mcp_status_cd='C' (cancelada no CD)`] : []),
        ...(nota.chegou_no_erp_em ? [`chegou_no_erp_em='${nota.chegou_no_erp_em}' (já chegou ERP, sai do trânsito)`] : []),
      ],
      total_itens: itens.length,
      itens_com_match_produtos_embalagem: itens.filter(i => i.produto_embalagem_mat_codi).length,
      itens,
    });
  } catch (e) { res.status(500).json({ erro: e.message, stack: e.stack }); }
});

// Debug — investiga match de um produto entre cd_material e dados das lojas/CDs
// GET /grade-debug?cd_origem=X&mat_codi=Y
router.get('/grade-debug', adminOuCeo, async (req, res) => {
  try {
    const cdOrigem = String(req.query.cd_origem || '').trim();
    const matCodi  = String(req.query.mat_codi  || '').trim();
    if (!cdOrigem || !matCodi) return res.status(400).json({ erro: 'cd_origem e mat_codi obrigatorios' });

    const [prod] = await dbQuery(
      `SELECT mat_codi, mat_desc, ean_codi, mat_situ FROM cd_material WHERE cd_codigo = $1 AND mat_codi = $2`,
      [cdOrigem, matCodi]
    );
    if (!prod) return res.json({ erro: 'mat_codi nao existe nesse CD', cd_origem: cdOrigem });

    // Cruzamento via produtos_embalagem (preferido pro CD ITAUTUBA)
    const [emProdEmb] = await dbQuery(
      `SELECT mat_codi, ean_principal_cd, ean_principal_jrlira, qtd_embalagem, descricao_atual
         FROM produtos_embalagem WHERE mat_codi = $1`,
      [matCodi]
    );

    // EAN usado pra match: primeiro produtos_embalagem (validado pelo time), depois cd_material
    const eanRaw = prod.ean_codi || emProdEmb?.ean_principal_cd || null;
    const eanNorm = String(eanRaw || '').replace(/^0+/, '') || eanRaw;
    const eanFonte = prod.ean_codi ? 'cd_material' : (emProdEmb?.ean_principal_cd ? 'produtos_embalagem' : 'nenhum');

    // Cd_material em outros CDs (com mesmo EAN)
    const cross = await dbQuery(
      `SELECT cd_codigo, mat_codi, mat_desc, ean_codi FROM cd_material
        WHERE NULLIF(LTRIM(ean_codi,'0'),'') = $1
        ORDER BY cd_codigo`,
      [eanNorm]
    );

    // produtos_externo das lojas
    const externos = await dbQuery(
      `SELECT loja_id, codigobarra, estdisponivel, prsugerido FROM produtos_externo
        WHERE NULLIF(LTRIM(codigobarra,'0'),'') = $1
        ORDER BY loja_id`,
      [eanNorm]
    );

    // vendas (90d)
    const vendas = await dbQuery(
      `SELECT loja_id, COUNT(*) AS dias, SUM(qtd_vendida) AS qtd_total, MAX(data_venda) AS ult
         FROM vendas_historico
        WHERE NULLIF(LTRIM(codigobarra,'0'),'') = $1
          AND data_venda >= CURRENT_DATE - INTERVAL '90 days'
          AND COALESCE(tipo_saida,'venda')='venda'
        GROUP BY loja_id ORDER BY loja_id`,
      [eanNorm]
    );

    // Sample de produtos_externo (pra ver o formato real)
    const sample = await dbQuery(
      `SELECT loja_id, codigobarra, NULLIF(LTRIM(codigobarra,'0'),'') AS ean_norm
         FROM produtos_externo WHERE loja_id = 1 LIMIT 3`
    );

    // Bonus: contagem de cd_material com EAN por CD (vê se vale sincronizar tabela EAN)
    const eanStatsCd = await dbQuery(
      `SELECT cd_codigo,
              COUNT(*)::int AS total_produtos,
              COUNT(NULLIF(ean_codi,''))::int AS com_ean,
              COUNT(*) FILTER (WHERE ean_codi IS NULL OR ean_codi = '')::int AS sem_ean
         FROM cd_material
        GROUP BY cd_codigo ORDER BY cd_codigo`
    );

    res.json({
      produto_no_cd: prod,
      em_produtos_embalagem: emProdEmb || null,
      ean_raw: eanRaw, ean_normalizado: eanNorm, ean_fonte: eanFonte,
      em_outros_cds: cross,
      em_lojas: externos,
      vendas_90d_por_loja: vendas,
      sample_produtos_externo_loja1: sample,
      stats_cd_material_por_cd: eanStatsCd,
    });
  } catch (e) {
    console.error('[grade-debug]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// ── Catálogo de produtos (do CD legado já sincronizado) — usado pela busca antiga ──

router.get('/produtos', adminOuCeo, async (req, res) => {
  try {
    const search = (req.query.search || '').toString().trim();
    const limit  = Math.min(parseInt(req.query.limit) || 50, 200);
    const params = ['srv1-itautuba']; // catalogo legado por enquanto
    let where = `cd_m.cd_codigo = $1 AND (cd_m.mat_situ = 'A' OR cd_m.mat_situ IS NULL)`;
    if (search) {
      params.push(`%${search.toLowerCase()}%`);
      params.push(search.replace(/[^0-9]/g, ''));
      where = `cd_m.cd_codigo = $1 AND (LOWER(cd_m.mat_desc) LIKE $2 OR cd_m.mat_codi = $3 OR cd_m.ean_codi = $3)`;
    }
    params.push(limit);
    const rows = await dbQuery(`
      SELECT cd_m.mat_codi, cd_m.mat_desc, cd_m.mat_refe, cd_m.ean_codi,
             cd_c.pro_prad   AS preco_admin,
             cd_c.pro_prcr   AS preco_compra,
             cd_e.est_quan   AS estoque_cd,
             pe.qtd_embalagem,
             pe.descricao_atual AS desc_local
        FROM cd_material cd_m
        LEFT JOIN cd_custoprod cd_c ON cd_c.cd_codigo = cd_m.cd_codigo AND cd_c.pro_codi = cd_m.mat_codi
        LEFT JOIN cd_estoque   cd_e ON cd_e.cd_codigo = cd_m.cd_codigo AND cd_e.pro_codi = cd_m.mat_codi
        LEFT JOIN produtos_embalagem pe ON pe.mat_codi = cd_m.mat_codi
       WHERE ${where}
       ORDER BY cd_m.mat_desc
       LIMIT $${params.length}
    `, params);
    res.json(rows);
  } catch (e) {
    console.error('[pedidos-distrib produtos]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// ── Helpers de CSV ──

function padNomeCliente(nome) { return String(nome || '').padEnd(60, ' ').slice(0, 60); }
function fmtData(d) {
  const pad = n => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.000`;
}
function fmtHora(d) {
  const pad = n => String(n).padStart(2, '0');
  return `${pad(d.getHours())}:${pad(d.getMinutes())}`;
}
function fmtNum(n, casas = 2) { return Number(n || 0).toFixed(casas); }

const HEADER_PEDIDOS = 'COD_PEDIDO;EMPRESA;LOCALIZACAO;COD_VENDEDOR;COD_CLIENTE;COD_CLIENTE_CADASTRO;COD_CONDICAO;COD_PAGAMENTO;COD_TIPOVENDA;COD_TABELA;DATA_EMISSAO;VALOR_PEDIDO;OBSERVACAO;RETORNO;SIT_RETORNO;NUMPEDIDO;HORA_EMISSAO;COD_MOTIVO;ID;LATITUDE;LONGITUDE;CPF_CNPJ;NOME_CLIENTE';
const HEADER_ITENS   = 'COD_PEDIDO;COD_VENDEDOR;COD_PRODUTO;UNIDADE;QUANTIDADE;VALOR;DESCONTO_UNI;DESCONTO_PER;VALOR_TABELA;TIPO_CALCULO;NOME_EMBALAGEM;QTD_EMBALAGEM;ID';

// ── Geração de CSV (lê qtds salvas em pedidos_distrib_quantidades) ──
//
// Body: { cd_origem_codigo: "srv1-itautuba", observacao?: string }
// Os destinos e itens vêm das qtds salvas via PUT /qtd (cesta tipo planilha).
// Pedido é gerado pra cada destino que tem ao menos 1 item com qtd > 0.
router.post('/', adminOuCeo, async (req, res) => {
  try {
    const { cd_origem_codigo, observacao } = req.body || {};
    if (!cd_origem_codigo) return res.status(400).json({ erro: 'cd_origem_codigo obrigatorio' });
    const por = req.usuario.email || req.usuario.usuario || req.usuario.nome || `id:${req.usuario.id}`;

    // Carrega todas as qtds salvas pra esse CD origem
    const qtds = await dbQuery(
      `SELECT destino_id, mat_codi, qtd FROM pedidos_distrib_quantidades
        WHERE cd_origem_codigo = $1 AND qtd > 0`,
      [cd_origem_codigo]
    );
    if (!qtds.length) return res.status(400).json({ erro: 'nenhuma quantidade > 0 salva pra esse CD origem. Edite a grade primeiro.' });

    // Agrupa por destino
    const porDestino = new Map(); // destino_id -> [{ mat_codi, qtd }]
    for (const q of qtds) {
      if (!porDestino.has(q.destino_id)) porDestino.set(q.destino_id, []);
      porDestino.get(q.destino_id).push({ mat_codi: q.mat_codi, qtd: parseFloat(q.qtd) });
    }
    const destinos = [...porDestino.keys()].map(destino_id => ({ destino_id }));
    const todosMatCodi = [...new Set(qtds.map(q => q.mat_codi))];

    // 1) Resolve dados do CD origem (pra excluir do destino e usar relay)
    const [cdOrigem] = await dbQuery(
      `SELECT codigo, nome, cnpj FROM pedidos_distrib_destinos WHERE tipo='CD' AND cd_codigo = $1`,
      [cd_origem_codigo]
    );
    if (!cdOrigem) return res.status(400).json({ erro: `CD origem "${cd_origem_codigo}" nao cadastrado em destinos` });

    // 2) Carrega dados dos destinos (ids vem do agrupamento das qtds salvas)
    const destIds = [...porDestino.keys()];
    const destRows = await dbQuery(
      `SELECT id, tipo, codigo, nome, cnpj, loja_id FROM pedidos_distrib_destinos
        WHERE id = ANY($1::int[]) AND ativo = TRUE`,
      [destIds]
    );
    const destMap = new Map(destRows.map(d => [d.id, d]));
    for (const id of destIds) {
      if (!destMap.has(id)) return res.status(400).json({ erro: `destino_id ${id} nao encontrado/ativo` });
      const dest = destMap.get(id);
      if (dest.tipo === 'CD' && dest.cnpj === cdOrigem.cnpj) {
        return res.status(400).json({ erro: `nao pode enviar pra si mesmo: ${dest.nome}` });
      }
    }

    // 3) Resolve CLI_CODI de cada destino via relay do CD origem
    let cli;
    try { cli = await clientePorCodigo(cd_origem_codigo); }
    catch (e) { return res.status(400).json({ erro: `relay do CD ${cd_origem_codigo} indisponivel: ${e.message}` }); }

    const cliMap = new Map(); // destino_id -> { CLI_CODI }
    for (const d of destRows) {
      try {
        const r = await cli.query(
          `SELECT TOP 1 CLI_CODI FROM CLIENTE WITH (NOLOCK)
            WHERE REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(CLI_CPF)),'.',''),'/',''),'-','') LIKE '%${String(d.cnpj).replace(/\\D/g,'')}'`
        );
        const cliCodi = r.rows?.[0]?.CLI_CODI;
        if (!cliCodi) {
          return res.status(400).json({ erro: `CLI_CODI nao encontrado pra "${d.nome}" (CNPJ ${d.cnpj}) no banco do CD ${cd_origem_codigo}` });
        }
        cliMap.set(d.id, String(cliCodi).trim());
      } catch (e) {
        return res.status(500).json({ erro: `falha consultando CLIENTE pra "${d.nome}": ${e.message}` });
      }
    }

    // 4) Hidrata todos mat_codi com preço admin do CD origem
    const dadosCd = await dbQuery(
      `SELECT cd_m.mat_codi, cd_m.mat_desc, cd_c.pro_prad, pe.qtd_embalagem
         FROM cd_material cd_m
         LEFT JOIN cd_custoprod cd_c ON cd_c.cd_codigo = cd_m.cd_codigo AND cd_c.pro_codi = cd_m.mat_codi
         LEFT JOIN produtos_embalagem pe ON pe.mat_codi = cd_m.mat_codi
        WHERE cd_m.cd_codigo = $1 AND cd_m.mat_codi = ANY($2::text[])`,
      [cd_origem_codigo, todosMatCodi]
    );
    const cdMap = new Map(dadosCd.map(x => [x.mat_codi, x]));
    for (const m of todosMatCodi) {
      const cd = cdMap.get(m);
      if (!cd) return res.status(400).json({ erro: `mat_codi ${m} nao encontrado no catalogo` });
      if (!(parseFloat(cd.pro_prad) > 0)) return res.status(400).json({ erro: `mat_codi ${m} sem preco admin` });
    }

    // 5) Próximo COD_PEDIDO pra esse CD origem
    const [{ proximo }] = await dbQuery(`
      SELECT COALESCE(MAX(cod_pedido), 0) + 1 AS proximo
        FROM pedidos_distrib_historico
       WHERE cd_origem_codigo = $1
    `, [cd_origem_codigo]);

    const agora = new Date();
    const dataEmissao = fmtData(agora);
    const horaEmissao = fmtHora(agora);
    const obser = String(observacao || `Transferencia ${por}`).slice(0, 120);

    const linhasPedidos = [];
    const linhasItens   = [];
    let codPedido = parseInt(proximo);
    let valorTotalGeral = 0;
    let totalItensGeral = 0;
    const pedidosResumo = [];

    for (const id of destIds) {
      const dest = destMap.get(id);
      const cliCodi = cliMap.get(id);
      const itensDoDestino = porDestino.get(id);
      if (!itensDoDestino?.length) continue;

      // Calcula valor total do pedido somando qtd × preço de cada item
      let valorPedido = 0;
      const linhasItensDest = [];
      for (const it of itensDoDestino) {
        const cd = cdMap.get(it.mat_codi);
        const valor = parseFloat(cd.pro_prad);
        const qtd = it.qtd;
        valorPedido += qtd * valor;
        linhasItensDest.push([
          codPedido,               VEN_CODI_PADRAO,        it.mat_codi,
          UNIDADE_PADRAO,          fmtNum(qtd, 0),         fmtNum(valor, 2),
          '0',                     '0',                    fmtNum(valor, 2),
          TIPO_CALCULO_PADRAO,     NOME_EMBALAGEM_PADRAO,  parseInt(cd.qtd_embalagem) || 1,
          '',
        ].join(';'));
      }

      linhasPedidos.push([
        codPedido,                EMPRESA_PADRAO,         LOCALIZACAO_PADRAO,
        VEN_CODI_PADRAO,          cliCodi,                '0',
        COD_CONDICAO_PADRAO,      COD_PAGAMENTO_PADRAO,   COD_TIPOVENDA_PADRAO,
        COD_TABELA_PADRAO,        dataEmissao,            fmtNum(valorPedido, 2),
        obser,                    '0',                    '1',
        '0',                      horaEmissao,            '0',
        '0',                      '00.00000',             '00.00000',
        dest.cnpj,                padNomeCliente(dest.nome),
      ].join(';'));
      linhasItens.push(...linhasItensDest);

      pedidosResumo.push({
        cod_pedido: codPedido,
        destino_id: dest.id,
        destino_tipo: dest.tipo,
        destino_nome: dest.nome,
        cli_codi: cliCodi,
        valor: valorPedido,
        itens: itensDoDestino.length,
      });
      valorTotalGeral += valorPedido;
      totalItensGeral += itensDoDestino.length;
      codPedido += 1;
    }

    if (!linhasPedidos.length) return res.status(400).json({ erro: 'nenhum pedido valido' });

    const csvPedidos = HEADER_PEDIDOS + '\r\n' + linhasPedidos.join('\r\n') + '\r\n';
    const csvItens   = HEADER_ITENS   + '\r\n' + linhasItens.join('\r\n')   + '\r\n';

    // Salva geração
    const [ger] = await dbQuery(
      `INSERT INTO pedidos_distrib_geracoes
         (gerado_por, cd_origem_codigo, total_pedidos, total_itens, valor_total, observacao,
          p_pedidos_csv, p_pedidos_itens_csv)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id, gerado_em`,
      [por, cd_origem_codigo, pedidosResumo.length, totalItensGeral, valorTotalGeral, obser, csvPedidos, csvItens]
    );

    // Histórico (1 linha por pedido)
    for (const p of pedidosResumo) {
      const dest = destMap.get(p.destino_id);
      await dbQuery(
        `INSERT INTO pedidos_distrib_historico
           (cod_pedido, cd_origem_codigo, loja_id_destino, destino_id, destino_nome, destino_tipo,
            emitido_por, valor_total, total_itens, observacao,
            payload_pedido, payload_itens, status)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'csv_gerado')`,
        [p.cod_pedido, cd_origem_codigo, dest.loja_id, p.destino_id, p.destino_nome, p.destino_tipo,
         por, p.valor, p.itens, obser,
         JSON.stringify(p), JSON.stringify({ geracao_id: ger.id })]
      );
    }

    res.json({
      ok: true,
      geracao_id: ger.id,
      gerado_em: ger.gerado_em,
      cd_origem: cdOrigem,
      total_pedidos: pedidosResumo.length,
      total_itens: totalItensGeral,
      valor_total: valorTotalGeral,
      pedidos: pedidosResumo,
      download: {
        p_pedidos: `/api/pedidos-distribuidora/csv/${ger.id}/P_PEDIDOS.csv`,
        p_pedidos_itens: `/api/pedidos-distribuidora/csv/${ger.id}/P_PEDIDOS_ITENS.csv`,
      },
    });
  } catch (e) {
    console.error('[pedidos-distrib POST]', e.message);
    res.status(e.status || 500).json({ erro: e.message });
  }
});

// ── Download dos CSVs ──
async function autoricar(req, res, next) {
  let token = (req.headers.authorization || '').replace(/^Bearer\s+/i, '');
  if (!token && req.query.token) token = String(req.query.token);
  if (!token) return res.status(401).json({ erro: 'Token ausente' });
  try {
    const jwt = require('jsonwebtoken');
    const { JWT_SECRET } = require('../auth');
    req.usuario = jwt.verify(token, JWT_SECRET);
    next();
  } catch { res.status(401).json({ erro: 'Token invalido' }); }
}

router.get('/csv/:geracao_id/P_PEDIDOS.csv', autoricar, async (req, res) => {
  await enviarCsv(req, res, 'p_pedidos_csv', 'P_PEDIDOS.csv');
});
router.get('/csv/:geracao_id/P_PEDIDOS_ITENS.csv', autoricar, async (req, res) => {
  await enviarCsv(req, res, 'p_pedidos_itens_csv', 'P_PEDIDOS_ITENS.csv');
});

async function enviarCsv(req, res, coluna, filename) {
  try {
    const [r] = await dbQuery(
      `SELECT ${coluna} AS conteudo FROM pedidos_distrib_geracoes WHERE id = $1`,
      [parseInt(req.params.geracao_id)]
    );
    if (!r) return res.status(404).json({ erro: 'geracao nao encontrada' });
    await dbQuery(`UPDATE pedidos_distrib_geracoes SET baixado_em = COALESCE(baixado_em, NOW()) WHERE id = $1`,
      [parseInt(req.params.geracao_id)]);
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.send(r.conteudo);
  } catch (e) { res.status(500).json({ erro: e.message }); }
}

// ── Histórico ──

router.get('/geracoes', autenticar, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 30, 100);
    const rows = await dbQuery(`
      SELECT id, gerado_em, gerado_por, cd_origem_codigo, total_pedidos, total_itens,
             valor_total, observacao, baixado_em
        FROM pedidos_distrib_geracoes
       ORDER BY gerado_em DESC
       LIMIT $1
    `, [limit]);
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

router.get('/historico', autenticar, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const rows = await dbQuery(`
      SELECT h.id, h.cod_pedido, h.cd_origem_codigo, h.destino_tipo, h.destino_nome,
             h.emitido_por, h.emitido_em, h.valor_total, h.total_itens,
             h.observacao, h.status, h.erro_msg
        FROM pedidos_distrib_historico h
       ORDER BY h.emitido_em DESC
       LIMIT $1
    `, [limit]);
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

module.exports = router;
