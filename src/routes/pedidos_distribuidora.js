// Pedidos pra Distribuidora — Diretor/CEO emite pedido que vira CSV pro importador do UltraSyst.
//
// Modelo:
// - 6 LOJAS (SUPERASA) recebem; 5 CDs (ASA BRANCA, ASA FRIOS, CASA BRANCA) emitem e podem receber entre si.
// - Operador escolhe o CD ORIGEM (de onde sai a mercadoria) + destinos (lojas e/ou outros CDs).
// - CLI_CODI de cada destino é descoberto via relay do CD origem (CLIENTE WHERE CLI_CGC = cnpj).
// - Backend gera 2 CSVs (P_PEDIDOS.csv + P_PEDIDOS_ITENS.csv) no formato oficial.
// - Operador baixa e cola na pasta de importação do UltraSyst do CD origem.

const express = require('express');
const router = express.Router();
const { query: dbQuery } = require('../db');
const { autenticar, exigirPerfil } = require('../auth');
const { listarCds, clientePorCodigo } = require('../cds');

const adminOuCeo = [autenticar, exigirPerfil('admin', 'ceo')];

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

// CDs origem: lista CDs cadastrados em /admin-cds que tem destino correspondente em pedidos_distrib_destinos
router.get('/cds-origem', adminOuCeo, async (req, res) => {
  try {
    const cds = await listarCds(true); // só ativos
    // Vincula com destinos pra mostrar nome amigável
    const destinos = await dbQuery(
      `SELECT cd_codigo, cnpj, nome FROM pedidos_distrib_destinos WHERE tipo='CD' AND cd_codigo IS NOT NULL`
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
          `SELECT TOP 1 CLI_CODI, CLI_RAZS, CLI_CGC FROM CLIENTE WITH (NOLOCK) WHERE REPLACE(REPLACE(REPLACE(CLI_CGC,'.',''),'/',''),'-','') = '${d.cnpj}'`
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

    // 1) Destinos (todos exceto o CD origem mesmo)
    const [origemRow] = await dbQuery(
      `SELECT cnpj FROM pedidos_distrib_destinos WHERE cd_codigo = $1`,
      [cdOrigem]
    );
    const cnpjOrigem = origemRow?.cnpj || '';
    const destinos = await dbQuery(
      `SELECT id, tipo, codigo, nome, cnpj, loja_id, cd_codigo
         FROM pedidos_distrib_destinos
        WHERE ativo = TRUE AND cnpj <> $1
        ORDER BY tipo DESC, nome`,
      [cnpjOrigem]
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
    params.push(limit);
    const produtos = await dbQuery(`
      SELECT cd_m.mat_codi,
             cd_m.ean_codi,
             COALESCE(pe.descricao_atual, cd_m.mat_desc) AS descricao,
             cd_m.mat_refe AS referencia,
             cd_e.est_quan AS est_dist,
             cd_c.pro_prad AS preco_admin,
             pe.qtd_embalagem
        FROM cd_material cd_m
        LEFT JOIN cd_estoque   cd_e ON cd_e.cd_codigo = cd_m.cd_codigo AND cd_e.pro_codi = cd_m.mat_codi
        LEFT JOIN cd_custoprod cd_c ON cd_c.cd_codigo = cd_m.cd_codigo AND cd_c.pro_codi = cd_m.mat_codi
        LEFT JOIN produtos_embalagem pe ON pe.mat_codi = cd_m.mat_codi
       WHERE ${where}
       ORDER BY descricao
       LIMIT $${params.length}
    `, params);
    if (!produtos.length) return res.json({ cd_origem: cdOrigem, destinos, produtos: [] });

    const eans = [...new Set(produtos.map(p => p.ean_codi).filter(Boolean))];
    const matCodis = produtos.map(p => p.mat_codi);

    // 3) Estoque + preço atual por (loja_id, ean) — produtos_externo
    const estLojas = lojaIds.length && eans.length ? await dbQuery(
      `SELECT loja_id, NULLIF(LTRIM(codigobarra,'0'),'') AS ean, estdisponivel, prsugerido
         FROM produtos_externo
        WHERE loja_id = ANY($1::int[])
          AND NULLIF(LTRIM(codigobarra,'0'),'') = ANY($2::text[])`,
      [lojaIds, eans.map(e => String(e).replace(/^0+/, '') || e)]
    ) : [];

    // 4) Vendas: media 28d, ultima_venda, qtd_total 90d (pro ranking)
    const vendas = lojaIds.length && eans.length ? await dbQuery(
      `SELECT loja_id,
              NULLIF(LTRIM(codigobarra,'0'),'') AS ean,
              SUM(CASE WHEN data_venda >= CURRENT_DATE - INTERVAL '28 days' THEN qtd_vendida ELSE 0 END) AS qtd_28d,
              SUM(CASE WHEN data_venda >= CURRENT_DATE - INTERVAL '90 days' THEN qtd_vendida ELSE 0 END) AS qtd_90d,
              MAX(data_venda) AS ultima_venda
         FROM vendas_historico
        WHERE loja_id = ANY($1::int[])
          AND NULLIF(LTRIM(codigobarra,'0'),'') = ANY($2::text[])
          AND COALESCE(tipo_saida,'venda') = 'venda'
          AND data_venda >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY loja_id, ean`,
      [lojaIds, eans.map(e => String(e).replace(/^0+/, '') || e)]
    ) : [];

    // 5) Estoque dos CDs destino — match por mat_codi pelo EAN do produto origem
    // cd_destino tem seu próprio mat_codi → join via cd_material[destino].ean_codi = ean_origem
    const estCds = cdDestinos.length && eans.length ? await dbQuery(
      `SELECT cd_m.cd_codigo, cd_m.ean_codi, cd_e.est_quan
         FROM cd_material cd_m
         LEFT JOIN cd_estoque cd_e ON cd_e.cd_codigo = cd_m.cd_codigo AND cd_e.pro_codi = cd_m.mat_codi
        WHERE cd_m.cd_codigo = ANY($1::text[]) AND cd_m.ean_codi = ANY($2::text[])`,
      [cdDestinos.map(d => d.cd_codigo), eans]
    ) : [];

    // 6) Trânsito por (loja_id, ean) — itens em notas_entrada origem='cd'/'transferencia_loja' não fechadas
    const transito = lojaIds.length && eans.length ? await dbQuery(
      `SELECT n.loja_id,
              NULLIF(LTRIM(COALESCE(i.ean_validado, i.ean_nota),'0'),'') AS ean,
              COALESCE(SUM(i.quantidade),0) AS qtd_transito
         FROM itens_nota i
         JOIN notas_entrada n ON n.id = i.nota_id
        WHERE n.loja_id = ANY($1::int[])
          AND n.origem IN ('cd','transferencia_loja')
          AND n.status NOT IN ('fechada','validada','arquivada','cancelada')
          AND NULLIF(LTRIM(COALESCE(i.ean_validado, i.ean_nota),'0'),'') = ANY($2::text[])
        GROUP BY n.loja_id, ean`,
      [lojaIds, eans.map(e => String(e).replace(/^0+/, '') || e)]
    ) : [];

    // 7) Quantidades editadas (Sug_Editada)
    const qtds = await dbQuery(
      `SELECT destino_id, mat_codi, qtd FROM pedidos_distrib_quantidades
        WHERE cd_origem_codigo = $1 AND mat_codi = ANY($2::text[])`,
      [cdOrigem, matCodis]
    );

    // 8) Ranking — soma qtd_vendida × preço admin do produto (todas lojas, 90d)
    // Usa preco_admin do CD como proxy do "valor vendido"
    const rankingRows = eans.length ? await dbQuery(
      `SELECT NULLIF(LTRIM(codigobarra,'0'),'') AS ean,
              SUM(qtd_vendida) AS qtd_total
         FROM vendas_historico
        WHERE NULLIF(LTRIM(codigobarra,'0'),'') = ANY($1::text[])
          AND COALESCE(tipo_saida,'venda') = 'venda'
          AND data_venda >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY ean`,
      [eans.map(e => String(e).replace(/^0+/, '') || e)]
    ) : [];

    // ── Consolida tudo no JSON por produto ──
    const norm = e => String(e || '').replace(/^0+/, '') || e;
    const eanIdx = new Map();
    for (const p of produtos) {
      eanIdx.set(norm(p.ean_codi), p);
      p.destinos = {};
      p.vendas = {};
    }

    // Estoque + preço por loja
    const estLojaMap = new Map(); // `${loja_id}|${ean}` → { est, preco }
    for (const r of estLojas) estLojaMap.set(`${r.loja_id}|${r.ean}`, r);

    // Vendas por loja
    const vendaMap = new Map();
    for (const v of vendas) vendaMap.set(`${v.loja_id}|${v.ean}`, v);

    // Estoque CD destino
    const estCdMap = new Map(); // `${cd_codigo}|${ean}` → { est }
    for (const e of estCds) estCdMap.set(`${e.cd_codigo}|${norm(e.ean_codi)}`, e);

    // Trânsito loja
    const transitoMap = new Map();
    for (const t of transito) transitoMap.set(`${t.loja_id}|${t.ean}`, parseFloat(t.qtd_transito));

    // Sug_Editada
    const sugEditMap = new Map(); // `${mat_codi}|${destino_id}` → qtd
    for (const q of qtds) sugEditMap.set(`${q.mat_codi}|${q.destino_id}`, parseFloat(q.qtd));

    // Ranking
    const totaisRanking = rankingRows
      .map(r => {
        const p = eanIdx.get(r.ean);
        const preco = p?.preco_admin ? parseFloat(p.preco_admin) : 0;
        return { ean: r.ean, valor: parseFloat(r.qtd_total) * preco };
      })
      .sort((a, b) => b.valor - a.valor);
    const rankMap = new Map(totaisRanking.map((r, i) => [r.ean, i + 1]));

    for (const p of produtos) {
      const eanN = norm(p.ean_codi);
      p.ranking = rankMap.get(eanN) || null;
      const qtdEmb = parseInt(p.qtd_embalagem) || 1;

      for (const d of destinos) {
        const slot = { estoque_un: 0, estoque_cx: 0, transito_un: 0, transito_cx: 0, sugestao_cx: 0, sug_editada: 0 };

        if (d.tipo === 'LOJA' && d.loja_id) {
          const e = estLojaMap.get(`${d.loja_id}|${eanN}`);
          const v = vendaMap.get(`${d.loja_id}|${eanN}`);
          const t = transitoMap.get(`${d.loja_id}|${eanN}`) || 0;
          slot.estoque_un = e ? parseFloat(e.estdisponivel) : 0;
          slot.transito_un = t;
          slot.estoque_cx = qtdEmb > 0 ? Math.floor(slot.estoque_un / qtdEmb) : 0;
          slot.transito_cx = qtdEmb > 0 ? Math.floor(slot.transito_un / qtdEmb) : 0;
          // Sugestão: max(0, 35 × media_dia − est − trans), em CX
          const media_dia = v ? parseFloat(v.qtd_28d) / 28 : 0;
          const sug_un = Math.max(0, 35 * media_dia - slot.estoque_un - slot.transito_un);
          slot.sugestao_cx = qtdEmb > 0 ? Math.ceil(sug_un / qtdEmb) : 0;
          // Vendas
          if (v) {
            p.vendas[d.loja_id] = {
              media_28d: media_dia,
              preco_atual: e ? parseFloat(e.prsugerido) : null,
              ultima_venda: v.ultima_venda,
            };
          } else if (e) {
            p.vendas[d.loja_id] = { media_28d: 0, preco_atual: parseFloat(e.prsugerido), ultima_venda: null };
          }
        } else if (d.tipo === 'CD' && d.cd_codigo) {
          const e = estCdMap.get(`${d.cd_codigo}|${eanN}`);
          slot.estoque_un = e?.est_quan ? parseFloat(e.est_quan) : 0;
          slot.estoque_cx = qtdEmb > 0 ? Math.floor(slot.estoque_un / qtdEmb) : 0;
          // Trânsito CD destino e sugestão CD: deferido (Task 5)
          slot.transito_un = 0;
          slot.transito_cx = 0;
          slot.sugestao_cx = 0;
        }

        slot.sug_editada = sugEditMap.get(`${p.mat_codi}|${d.id}`) || 0;
        p.destinos[d.id] = slot;
      }
    }

    res.json({ cd_origem: cdOrigem, destinos, produtos });
  } catch (e) {
    console.error('[pedidos-distrib grade]', e.message);
    res.status(500).json({ erro: e.message });
  }
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
            WHERE REPLACE(REPLACE(REPLACE(CLI_CGC,'.',''),'/',''),'-','') = '${d.cnpj}'`
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
