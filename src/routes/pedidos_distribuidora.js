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
const REGRAS_CD_DESTINOS = {
  'srv1-itautuba':    ['srv1-nprogresso', 'srv2-asafrio', 'srv2-asasantarem'],
  'srv1-nprogresso':  ['srv1-itautuba',   'srv2-asafrio', 'srv2-asasantarem'],
  'srv2-asafrio':     ['srv2-asasantarem'],
  'srv2-asasantarem': ['srv2-asafrio'],
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
       ORDER BY tipo, nome
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
          `SELECT TOP 1 CLI_CODI, CLI_RAZS, CLI_CPF FROM CLIENTE WITH (NOLOCK) WHERE REPLACE(REPLACE(REPLACE(CLI_CPF,'.',''),'/',''),'-','') = '${d.cnpj}'`
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
        ORDER BY tipo DESC, nome`,
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
         WHERE i.cd_codigo = $1 AND m.mcp_codi ~ '^[0-9]+$'
         GROUP BY i.cd_codigo, i.pro_codi
      )
      SELECT cd_m.mat_codi,
             COALESCE(
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
             pe.qtd_embalagem,
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
        LEFT JOIN ult_compra uc ON uc.cd_codigo = cd_m.cd_codigo AND uc.pro_codi = cd_m.mat_codi
        LEFT JOIN pedidos_distrib_prioridades pp
          ON pp.cd_origem_codigo = cd_m.cd_codigo AND pp.mat_codi = cd_m.mat_codi
        LEFT JOIN cd_grupo    cg ON cg.cd_codigo = cd_m.cd_codigo AND cg.gru_codi = cd_m.gru_codi
        LEFT JOIN cd_subgrupo cs ON cs.cd_codigo = cd_m.cd_codigo AND cs.gru_codi = cd_m.gru_codi AND cs.sgr_codi = cd_m.sgr_codi
       WHERE ${where}
       ORDER BY ${orderBy}
       LIMIT $${params.length}
    `, params);
    if (!produtos.length) return res.json({ cd_origem: cdOrigem, destinos, produtos: [] });

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

    // 4) Vendas: media 28d, ultima_venda, qtd_total 90d (pro ranking)
    // vendas_historico não tem produtoprincipal, então usa também os codigobarra alternativos das lojas
    // que apontam pro principal. Coletamos primeiro essa lista expandida via produtos_externo.
    const eansExpandidos = new Set(eansNorm);
    for (const r of estLojas) {
      if (r.codbarra)  eansExpandidos.add(r.codbarra);
      if (r.principal) eansExpandidos.add(r.principal);
    }
    const eansList = [...eansExpandidos];

    const vendas = lojaIds.length && eansList.length ? await dbQuery(
      `SELECT loja_id,
              NULLIF(LTRIM(codigobarra,'0'),'') AS codbarra,
              SUM(CASE WHEN data_venda >= CURRENT_DATE - INTERVAL '28 days' THEN qtd_vendida ELSE 0 END) AS qtd_28d,
              SUM(CASE WHEN data_venda >= CURRENT_DATE - INTERVAL '90 days' THEN qtd_vendida ELSE 0 END) AS qtd_90d,
              MAX(data_venda) AS ultima_venda
         FROM vendas_historico
        WHERE loja_id = ANY($1::int[])
          AND NULLIF(LTRIM(codigobarra,'0'),'') = ANY($2::text[])
          AND COALESCE(tipo_saida,'venda') = 'venda'
          AND data_venda >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY loja_id, codbarra`,
      [lojaIds, eansList]
    ) : [];

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
    const eansAmpliados = [...new Set([...eansNorm, ...eansExpandidosCdOrigem.map(x => x.ean_codi).filter(Boolean)])];

    const estCds = cdDestinos.length && eansAmpliados.length ? await dbQuery(
      `SELECT ce.cd_codigo, NULLIF(LTRIM(ce.ean_codi,'0'),'') AS ean_codi, cde.est_quan
         FROM cd_ean ce
         LEFT JOIN cd_estoque cde ON cde.cd_codigo = ce.cd_codigo AND cde.pro_codi = ce.mat_codi
        WHERE ce.cd_codigo = ANY($1::text[])
          AND NULLIF(LTRIM(ce.ean_codi,'0'),'') = ANY($2::text[])`,
      [cdDestinos.map(d => d.cd_codigo), eansAmpliados]
    ) : [];

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

    // 5b) "Vendas" dos CDs destino (consumo) — APENAS vendas atacado, EXCLUINDO transferências internas (NOP_CODI='031')
    const vendasCds = cdDestinos.length && eansNorm.length ? await dbQuery(
      `SELECT mc.cd_codigo,
              NULLIF(LTRIM(ce.ean_codi,'0'),'') AS ean,
              SUM(CASE WHEN mc.mcp_dten >= CURRENT_DATE - INTERVAL '28 days'
                       THEN ic.mcp_quan ELSE 0 END) AS qtd_28d,
              MAX(mc.mcp_dten) AS ultima_saida
         FROM cd_movcompra mc
         JOIN cd_itemcompra ic
           ON ic.cd_codigo = mc.cd_codigo
          AND ic.mcp_codi = mc.mcp_codi
          AND ic.mcp_tipomov = mc.mcp_tipomov
         JOIN cd_ean ce
           ON ce.cd_codigo = mc.cd_codigo
          AND ce.mat_codi = ic.pro_codi
        WHERE mc.cd_codigo = ANY($1::text[])
          AND mc.mcp_tipomov = 'S'
          AND COALESCE(NULLIF(LTRIM(mc.nop_codi,'0'),''), '') <> '31'
          AND mc.mcp_dten >= CURRENT_DATE - INTERVAL '90 days'
          AND NULLIF(LTRIM(ce.ean_codi,'0'),'') = ANY($2::text[])
        GROUP BY mc.cd_codigo, ean`,
      [cdDestinos.map(d => d.cd_codigo), eansNorm]
    ) : [];

    // 6) Trânsito por (loja_id, ean) — itens em notas_entrada origem='cd'/'transferencia_loja' não fechadas
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

    // 7) Quantidades editadas (Sug_Editada)
    const qtds = await dbQuery(
      `SELECT destino_id, mat_codi, qtd FROM pedidos_distrib_quantidades
        WHERE cd_origem_codigo = $1 AND mat_codi = ANY($2::text[])`,
      [cdOrigem, matCodis]
    );

    // 8) Ranking GLOBAL — soma qtd_vendida × preço_admin de TODOS os produtos do CD (todas lojas, 90d)
    // Não limita aos 500 da página — varre todo o catálogo do CD pra ranquear corretamente.
    const rankingGlobal = await dbQuery(`
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
      SELECT pc.mat_codi,
             COALESCE(v.qtd, 0) * COALESCE(pc.preco_admin, 0) AS valor_vendido
        FROM produtos_cd pc
        LEFT JOIN vendas_90d v ON v.ean = pc.ean
       WHERE COALESCE(v.qtd, 0) > 0
       ORDER BY valor_vendido DESC
    `, [cdOrigem]);

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

    // Estoque CD destino
    const estCdMap = new Map();
    for (const e of estCds) estCdMap.set(`${e.cd_codigo}|${norm(e.ean_codi)}`, e);

    // Vendas (consumo) dos CDs destino
    const vendasCdMap = new Map();
    for (const v of vendasCds) vendasCdMap.set(`${v.cd_codigo}|${v.ean}`, v);

    // Trânsito CD destino (saídas do CD origem com mcp_status='A' pra cada CD destino)
    const transitoCdMap = new Map();
    for (const t of transitoCdDestinos) transitoCdMap.set(`${t.cd_codigo}|${t.ean}`, parseFloat(t.qtd_transito) || 0);

    // Sug_Editada
    const sugEditMap = new Map();
    for (const q of qtds) sugEditMap.set(`${q.mat_codi}|${q.destino_id}`, parseFloat(q.qtd));

    // Ranking GLOBAL — já vem ordenado da SQL (mat_codi → posição)
    // Inclui TODOS os produtos do CD com vendas, não só os 500 da página
    const rankMap = new Map();
    rankingGlobal.forEach((r, i) => rankMap.set(r.mat_codi, i + 1));

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
          const t = transitoPorCb.get(`${d.loja_id}|${eanN}`) || 0;
          slot.estoque_un = e ? parseFloat(e.estdisponivel) : 0;
          slot.transito_un = t;
          slot.estoque_cx = qtdEmb > 0 ? Math.floor(slot.estoque_un / qtdEmb) : 0;
          slot.transito_cx = qtdEmb > 0 ? Math.floor(slot.transito_un / qtdEmb) : 0;
          const qtd_28d = v ? parseFloat(v.qtd_28d) || 0 : 0;
          const media_dia = qtd_28d / 28;
          const sug_un = Math.max(0, 35 * media_dia - slot.estoque_un - slot.transito_un);
          slot.sugestao_cx = qtdEmb > 0 ? Math.ceil(sug_un / qtdEmb) : 0;
          if (e || v) {
            p.vendas[d.loja_id] = {
              media_28d: media_dia,
              preco_atual: e?.prsugerido ? parseFloat(e.prsugerido) : null,
              ultima_venda: v?.ultima_venda || null,
            };
          }
        } else if (d.tipo === 'CD' && d.cd_codigo) {
          // CDs destino controlam estoque na MESMA unidade do CD origem (CX) — não dividir por qtd_embalagem
          const e = estCdMap.get(`${d.cd_codigo}|${eanN}`);
          const v = vendasCdMap.get(`${d.cd_codigo}|${eanN}`);
          const t = transitoCdMap.get(`${d.cd_codigo}|${eanN}`) || 0;
          slot.estoque_un = e?.est_quan ? parseFloat(e.est_quan) : 0;
          slot.estoque_cx = Math.floor(slot.estoque_un);
          slot.transito_un = t;
          slot.transito_cx = Math.floor(t);
          const qtd_28d = v ? parseFloat(v.qtd_28d) || 0 : 0;
          const media_dia = qtd_28d / 28;
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

// GET /debug-cliente?cd_origem=&cnpj= — tenta achar cliente no UltraSyst com varias estrategias
router.get('/debug-cliente', adminOuCeo, async (req, res) => {
  try {
    const cdOrigem = String(req.query.cd_origem || 'srv1-itautuba').trim();
    const cnpj = String(req.query.cnpj || '17764296000110').replace(/\D/g, '');
    const { clientePorCodigo } = require('../cds');
    const cli = await clientePorCodigo(cdOrigem);

    // 1) Sample de 5 clientes pra ver formato real
    const sample = await cli.query(`SELECT TOP 5 CLI_CODI, CLI_RAZS, CLI_CPF FROM CLIENTE WITH (NOLOCK)`);

    // 2) Tenta varias estrategias
    const tentativas = [
      { nome: 'REPLACE pontuacao', sql: `SELECT TOP 5 CLI_CODI, CLI_RAZS, CLI_CPF FROM CLIENTE WITH (NOLOCK) WHERE REPLACE(REPLACE(REPLACE(CLI_CPF,'.',''),'/',''),'-','') = '${cnpj}'` },
      { nome: 'TRIM + REPLACE',     sql: `SELECT TOP 5 CLI_CODI, CLI_RAZS, CLI_CPF FROM CLIENTE WITH (NOLOCK) WHERE REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(CLI_CPF)),'.',''),'/',''),'-','') = '${cnpj}'` },
      { nome: 'LIKE %cnpj%',         sql: `SELECT TOP 5 CLI_CODI, CLI_RAZS, CLI_CPF FROM CLIENTE WITH (NOLOCK) WHERE CLI_CPF LIKE '%${cnpj}%'` },
      { nome: 'LIKE primeiros 8',    sql: `SELECT TOP 5 CLI_CODI, CLI_RAZS, CLI_CPF FROM CLIENTE WITH (NOLOCK) WHERE CLI_CPF LIKE '%${cnpj.slice(0,8)}%'` },
    ];
    const resultados = [];
    for (const t of tentativas) {
      try {
        const r = await cli.query(t.sql);
        resultados.push({ ...t, total: r.rows?.length || 0, rows: r.rows?.slice(0,3) });
      } catch (e) { resultados.push({ ...t, erro: e.message }); }
    }
    res.json({ cd_origem: cdOrigem, cnpj_procurado: cnpj, sample_clientes: sample.rows, tentativas: resultados });
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
          AND mcp_tipomov = 'S'
          AND mcp_codi ~ '^[0-9]+$'`, [cdOrigem]
    );
    res.json({ cd_origem: cdOrigem, ultima_sequencia: r?.ultima || null });
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
            WHERE REPLACE(REPLACE(REPLACE(CLI_CPF,'.',''),'/',''),'-','') = '${d.cnpj}'`
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
