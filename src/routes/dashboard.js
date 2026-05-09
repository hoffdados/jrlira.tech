const express = require('express');
const router = express.Router();
const { query } = require('../db');
const { autenticar } = require('../auth');

// GET /api/dashboard/badges
// Retorna contadores de pendência por categoria (filtrados por loja_id se aplicável)
router.get('/badges', autenticar, async (req, res) => {
  try {
    const lojaId = req.usuario.loja_id;
    const filtroLoja = lojaId ? `AND loja_id = ${parseInt(lojaId)}` : '';

    const [
      auditoria_pedidos,
      notas_auditoria,
      aguardando_devolucao,
      validades_em_risco,
      divergencias_cd,
      acordos_pendentes,
      fornecedores_pendentes,
    ] = await Promise.all([
      query(`SELECT COUNT(*)::int AS n FROM pedidos WHERE status = 'aguardando_auditoria' ${lojaId ? 'AND loja_id = $1' : ''}`, lojaId ? [lojaId] : []),
      query(`SELECT COUNT(*)::int AS n FROM notas_entrada WHERE status IN ('em_auditoria','aguardando_admin_validade') ${lojaId ? 'AND loja_id = $1' : ''}`, lojaId ? [lojaId] : []),
      query(`SELECT COUNT(*)::int AS n FROM notas_entrada WHERE status = 'aguardando_devolucao' ${lojaId ? 'AND loja_id = $1' : ''}`, lojaId ? [lojaId] : []),
      query(`SELECT COUNT(*)::int AS n FROM notas_entrada WHERE status = 'aguardando_admin_validade' ${lojaId ? 'AND loja_id = $1' : ''}`, lojaId ? [lojaId] : []),
      query(`SELECT COUNT(*)::int AS n FROM devolucoes WHERE status = 'aguardando'`),
      query(`SELECT COUNT(*)::int AS n FROM acordos_comerciais WHERE status = 'pendente_compras'`),
      query(`SELECT COUNT(*)::int AS n FROM vendedores WHERE status IN ('pendente','aguardando_cadastro')`),
    ]);

    res.json({
      auditoria_pedidos: auditoria_pedidos[0]?.n || 0,
      notas_auditoria: notas_auditoria[0]?.n || 0,
      aguardando_devolucao: aguardando_devolucao[0]?.n || 0,
      validades_em_risco: validades_em_risco[0]?.n || 0,
      divergencias_cd: divergencias_cd[0]?.n || 0,
      auditoria_acordos: acordos_pendentes[0]?.n || 0,
      fornecedores_pendentes: fornecedores_pendentes[0]?.n || 0,
    });
  } catch (err) {
    console.error('[dashboard/badges]', err.message);
    res.status(500).json({ erro: err.message });
  }
});

// GET /api/dashboard/notas
// Painel operacional de notas: agregado por status × loja, totalizadores e
// notas paradas (>N dias no fluxo, ainda não fechadas).
// Filtros opcionais: loja, dataIni (data_emissao), dataFim, fornecedor (substring).
router.get('/notas', autenticar, async (req, res) => {
  try {
    const lojaUsr = req.usuario.loja_id;
    const lojaParam = req.query.loja ? parseInt(req.query.loja) : null;
    const lojaId = lojaUsr || lojaParam;
    const dataIni = req.query.dataIni || null;
    const dataFim = req.query.dataFim || null;
    const fornecedor = (req.query.fornecedor || '').trim();
    const diasParado = parseInt(req.query.diasParado) || 7;

    const conds = [];
    const params = [];
    if (lojaId)  { params.push(lojaId);  conds.push(`n.loja_id = $${params.length}`); }
    if (dataIni) { params.push(dataIni); conds.push(`n.data_emissao >= $${params.length}`); }
    if (dataFim) { params.push(dataFim); conds.push(`n.data_emissao <= $${params.length}`); }
    if (fornecedor) {
      params.push(`%${fornecedor}%`);
      conds.push(`(n.fornecedor_nome ILIKE $${params.length} OR n.fornecedor_cnpj ILIKE $${params.length})`);
    }
    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';

    const [porStatus, porLoja, paradas, tot] = await Promise.all([
      query(`
        SELECT n.status,
               COUNT(*)::int                            AS qtd,
               COALESCE(SUM(n.valor_total),0)::numeric  AS valor,
               COUNT(*) FILTER (WHERE n.emergencial)::int AS qtd_emergencial
          FROM notas_entrada n
          ${where}
         GROUP BY n.status
         ORDER BY n.status
      `, params),
      query(`
        SELECT n.loja_id,
               COALESCE(l.nome, 'Sem loja') AS loja_nome,
               n.status,
               COUNT(*)::int                            AS qtd,
               COALESCE(SUM(n.valor_total),0)::numeric  AS valor
          FROM notas_entrada n
          LEFT JOIN lojas l ON l.id = n.loja_id
          ${where}
         GROUP BY n.loja_id, l.nome, n.status
         ORDER BY n.loja_id, n.status
      `, params),
      query(`
        SELECT n.id, n.numero_nota, n.fornecedor_nome, n.fornecedor_cnpj,
               n.valor_total, n.status, n.loja_id, n.emergencial,
               COALESCE(l.nome,'Sem loja') AS loja_nome,
               n.data_emissao, n.importado_em,
               (EXTRACT(EPOCH FROM (NOW() - n.importado_em)) / 86400)::int AS dias
          FROM notas_entrada n
          LEFT JOIN lojas l ON l.id = n.loja_id
          ${where ? where + ' AND' : 'WHERE'} n.status NOT IN ('fechada','cancelada')
            AND n.importado_em < NOW() - ($${params.length + 1} || ' days')::interval
         ORDER BY n.importado_em ASC
         LIMIT 100
      `, [...params, String(diasParado)]),
      query(`
        SELECT COUNT(*)::int                            AS qtd,
               COALESCE(SUM(n.valor_total),0)::numeric  AS valor,
               COUNT(*) FILTER (WHERE n.emergencial)::int AS qtd_emergencial,
               COUNT(*) FILTER (WHERE n.status = 'fechada')::int AS qtd_fechadas,
               COUNT(*) FILTER (WHERE n.status NOT IN ('fechada','cancelada'))::int AS qtd_em_andamento
          FROM notas_entrada n
          ${where}
      `, params),
    ]);

    res.json({
      por_status: porStatus,
      por_loja: porLoja,
      paradas,
      total: tot[0],
      filtros: { loja: lojaId, dataIni, dataFim, fornecedor, diasParado }
    });
  } catch (err) {
    console.error('[dashboard/notas]', err.message);
    res.status(500).json({ erro: err.message });
  }
});

// GET /api/dashboard/divergencias-preco
// Compara preco_unitario_nota com custo_emb = custo_fabrica * COALESCE(qtd_embalagem,1).
// Filtros: loja, dataIni/dataFim (data_emissao), fornecedor, pctMin (% minimo de divergencia, default 5).
router.get('/divergencias-preco', autenticar, async (req, res) => {
  try {
    const lojaUsr = req.usuario.loja_id;
    const lojaParam = req.query.loja ? parseInt(req.query.loja) : null;
    const lojaId = lojaUsr || lojaParam;
    // Default: ultimos 90 dias se nao foi passado nada (evita full scan).
    const dataIni = req.query.dataIni || new Date(Date.now() - 90 * 86400 * 1000).toISOString().slice(0,10);
    const dataFim = req.query.dataFim || null;
    const fornecedor = (req.query.fornecedor || '').trim();
    const origem = req.query.origem || 'todas'; // todas | fornecedor | cd
    const pctMin = req.query.pctMin != null ? Math.abs(parseFloat(req.query.pctMin)) : 5; // % minimo absoluto pra entrar
    const sentido = req.query.sentido || 'todos'; // todos | sobrepreco | ganho

    const conds = [`i.custo_fabrica IS NOT NULL`, `i.custo_fabrica > 0`];
    const params = [];
    if (lojaId)  { params.push(lojaId);  conds.push(`n.loja_id = $${params.length}`); }
    if (dataIni) { params.push(dataIni); conds.push(`n.data_emissao >= $${params.length}`); }
    if (dataFim) { params.push(dataFim); conds.push(`n.data_emissao <= $${params.length}`); }
    if (fornecedor) {
      params.push(`%${fornecedor}%`);
      conds.push(`(n.fornecedor_nome ILIKE $${params.length} OR n.fornecedor_cnpj ILIKE $${params.length})`);
    }
    if (origem === 'fornecedor') conds.push(`COALESCE(n.origem,'nfe') = 'nfe'`);
    if (origem === 'cd')         conds.push(`n.origem = 'cd'`);
    const where = `WHERE ${conds.join(' AND ')}`;

    // CTE base: 3 hipoteses pra tratar inconsistencia de unidade entre NF-e e CD.
    // preco_unitario_nota ja vem unitario quando o parser detectou caixa, mas
    // nem sempre acerta. Testa:
    //   H1 unit:  preco_nota                 vs custo
    //   H2 caixa: preco_nota / qtd_embalagem vs custo  (preco veio em caixa, parser falhou)
    //   H3 mult:  preco_nota * qtd_embalagem vs custo  (caso reverso, raro)
    // Vence a hipotese com menor |pct|. Se nao tiver embalagem (qtd null), usa H1.
    //
    // 2 LEFT JOINs separados (mat_codi e ean) pra evitar OR no JOIN (timeout).
    const baseCte = `
      WITH itens_emb AS (
        SELECT i.id AS item_id, i.nota_id, i.cd_pro_codi,
               n.numero_nota, n.fornecedor_nome, n.fornecedor_cnpj, n.data_emissao,
               COALESCE(n.origem,'nfe') AS origem,
               n.loja_id, COALESCE(l.nome,'Sem loja') AS loja_nome,
               i.descricao_nota, i.ean_nota, i.ean_validado,
               i.quantidade, i.preco_unitario_nota, i.custo_fabrica, i.status_preco,
               i.qtd_por_caixa_nfe,
               -- prioridade: CD por mat_codi > CD por EAN > fornecedor (EAN+CNPJ) > parser NFe
               COALESCE(pe1.qtd_embalagem, pe2.qtd_embalagem,
                        ef.qtd_por_caixa,
                        NULLIF(i.qtd_por_caixa_nfe,1)) AS qtd_embalagem,
               COALESCE(pe1.mat_codi, pe2.mat_codi) AS mat_codi,
               -- fonte do dado (pra debug/UI)
               CASE
                 WHEN pe1.qtd_embalagem IS NOT NULL THEN 'cd-cod'
                 WHEN pe2.qtd_embalagem IS NOT NULL THEN 'cd-ean'
                 WHEN ef.qtd_por_caixa IS NOT NULL THEN 'forn'
                 WHEN i.qtd_por_caixa_nfe IS NOT NULL AND i.qtd_por_caixa_nfe > 1 THEN 'nfe'
               END AS emb_fonte,
               COALESCE(pe1.status, pe2.status, ef.status) AS emb_status
          FROM itens_nota i
          JOIN notas_entrada n ON n.id = i.nota_id
          LEFT JOIN lojas l ON l.id = n.loja_id
          LEFT JOIN produtos_embalagem pe1 ON pe1.mat_codi = i.cd_pro_codi
          LEFT JOIN produtos_embalagem pe2 ON pe1.mat_codi IS NULL
                                           AND pe2.ean_principal_cd = COALESCE(NULLIF(i.ean_validado,''), NULLIF(i.ean_nota,''))
          LEFT JOIN embalagens_fornecedor ef
                 ON pe1.mat_codi IS NULL AND pe2.mat_codi IS NULL
                AND ef.ean = COALESCE(NULLIF(i.ean_validado,''), NULLIF(i.ean_nota,''))
                AND COALESCE(ef.fornecedor_cnpj,'') = COALESCE(n.fornecedor_cnpj,'')
          ${where}
      ),
      hipoteses AS (
        SELECT *,
          -- pct das 3 hipoteses (NULL quando custo=0, tratado por NULLIF)
          ((preco_unitario_nota - custo_fabrica) / NULLIF(custo_fabrica,0) * 100)::numeric(12,2) AS pct_h1,
          CASE WHEN COALESCE(qtd_embalagem,1) > 1
               THEN ((preco_unitario_nota / qtd_embalagem - custo_fabrica) / NULLIF(custo_fabrica,0) * 100)::numeric(12,2)
          END AS pct_h2,
          CASE WHEN COALESCE(qtd_embalagem,1) > 1
               THEN ((preco_unitario_nota * qtd_embalagem - custo_fabrica) / NULLIF(custo_fabrica,0) * 100)::numeric(12,2)
          END AS pct_h3
          FROM itens_emb
      ),
      base AS (
        SELECT *,
          -- escolhe hipotese com menor abs(pct)
          CASE
            WHEN pct_h2 IS NOT NULL AND ABS(pct_h2) < ABS(pct_h1) AND (pct_h3 IS NULL OR ABS(pct_h2) <= ABS(pct_h3)) THEN 'h2'
            WHEN pct_h3 IS NOT NULL AND ABS(pct_h3) < ABS(pct_h1) THEN 'h3'
            ELSE 'h1'
          END AS hipotese,
          CASE
            WHEN pct_h2 IS NOT NULL AND ABS(pct_h2) < ABS(pct_h1) AND (pct_h3 IS NULL OR ABS(pct_h2) <= ABS(pct_h3)) THEN preco_unitario_nota / qtd_embalagem
            WHEN pct_h3 IS NOT NULL AND ABS(pct_h3) < ABS(pct_h1) THEN preco_unitario_nota * qtd_embalagem
            ELSE preco_unitario_nota
          END::numeric(14,4) AS preco_unit_efetivo,
          CASE
            WHEN pct_h2 IS NOT NULL AND ABS(pct_h2) < ABS(pct_h1) AND (pct_h3 IS NULL OR ABS(pct_h2) <= ABS(pct_h3)) THEN pct_h2
            WHEN pct_h3 IS NOT NULL AND ABS(pct_h3) < ABS(pct_h1) THEN pct_h3
            ELSE pct_h1
          END AS pct,
          -- diferenca em valor: usa preco_efetivo vs custo_fabrica, multiplicado por qtd da nota
          (CASE
            WHEN pct_h2 IS NOT NULL AND ABS(pct_h2) < ABS(pct_h1) AND (pct_h3 IS NULL OR ABS(pct_h2) <= ABS(pct_h3)) THEN (preco_unitario_nota / qtd_embalagem - custo_fabrica) * quantidade
            WHEN pct_h3 IS NOT NULL AND ABS(pct_h3) < ABS(pct_h1) THEN (preco_unitario_nota * qtd_embalagem - custo_fabrica) * quantidade
            ELSE (preco_unitario_nota - custo_fabrica) * quantidade
          END)::numeric(14,2) AS diferenca_total,
          (CASE
            WHEN pct_h2 IS NOT NULL AND ABS(pct_h2) < ABS(pct_h1) AND (pct_h3 IS NULL OR ABS(pct_h2) <= ABS(pct_h3)) THEN preco_unitario_nota / qtd_embalagem - custo_fabrica
            WHEN pct_h3 IS NOT NULL AND ABS(pct_h3) < ABS(pct_h1) THEN preco_unitario_nota * qtd_embalagem - custo_fabrica
            ELSE preco_unitario_nota - custo_fabrica
          END)::numeric(14,4) AS diferenca_unit
          FROM hipoteses
      )
    `;

    const condDiv = [];
    if (pctMin > 0) condDiv.push(`ABS(b.pct) >= ${Number(pctMin)}`);
    if (sentido === 'sobrepreco') condDiv.push(`b.pct > 0`);
    if (sentido === 'ganho')      condDiv.push(`b.pct < 0`);
    const whereDiv = condDiv.length ? `WHERE ${condDiv.join(' AND ')}` : '';

    const [itens, totaisFornecedor, kpis] = await Promise.all([
      query(`
        ${baseCte}
        SELECT b.* FROM base b
        ${whereDiv}
        ORDER BY ABS(b.diferenca_total) DESC
        LIMIT 500
      `, params),
      query(`
        ${baseCte}
        SELECT b.fornecedor_nome, b.fornecedor_cnpj,
               COUNT(*)::int AS qtd_itens,
               SUM(b.diferenca_total)::numeric(14,2) AS dif_total
          FROM base b
          ${whereDiv}
         GROUP BY b.fornecedor_nome, b.fornecedor_cnpj
         ORDER BY ABS(SUM(b.diferenca_total)) DESC
         LIMIT 30
      `, params),
      query(`
        ${baseCte}
        SELECT COUNT(*)::int AS qtd_itens_total,
               COUNT(*) FILTER (WHERE ABS(b.pct) >= ${Number(pctMin)})::int AS qtd_itens_div,
               COUNT(DISTINCT b.nota_id)::int AS qtd_notas,
               COUNT(DISTINCT b.fornecedor_cnpj)::int AS qtd_fornecedores,
               SUM(b.diferenca_total) FILTER (WHERE ABS(b.pct) >= ${Number(pctMin)})::numeric(14,2) AS dif_total,
               SUM(b.diferenca_total) FILTER (WHERE b.pct >= ${Number(pctMin)})::numeric(14,2) AS dif_sobrepreco,
               SUM(b.diferenca_total) FILTER (WHERE b.pct <= -${Number(pctMin)})::numeric(14,2) AS dif_ganho,
               COUNT(*) FILTER (WHERE b.qtd_embalagem IS NULL)::int AS qtd_sem_emb
          FROM base b
      `, params),
    ]);

    res.json({
      itens,
      por_fornecedor: totaisFornecedor,
      kpis: kpis[0],
      filtros: { loja: lojaId, dataIni, dataFim, fornecedor, pctMin, sentido }
    });
  } catch (err) {
    console.error('[dashboard/divergencias-preco]', err.message);
    res.status(500).json({ erro: err.message });
  }
});

// GET /api/dashboard/produtos-novos
// Itens com produto_novo=TRUE (sem cadastro no Ecocentauro). Mostra trabalho do cadastro.
// Filtros: loja, dataIni/dataFim (data_emissao), fornecedor, status (pendente/validado/todos)
router.get('/produtos-novos', autenticar, async (req, res) => {
  try {
    const lojaUsr = req.usuario.loja_id;
    const lojaParam = req.query.loja ? parseInt(req.query.loja) : null;
    const lojaId = lojaUsr || lojaParam;
    const dataIni = req.query.dataIni || null;
    const dataFim = req.query.dataFim || null;
    const fornecedor = (req.query.fornecedor || '').trim();
    const status = req.query.status || 'pendente'; // pendente | validado | todos

    const conds = [`i.produto_novo = TRUE`];
    const params = [];
    if (status === 'pendente')  conds.push(`i.validado_cadastro = FALSE`);
    if (status === 'validado')  conds.push(`i.validado_cadastro = TRUE`);
    if (lojaId)  { params.push(lojaId);  conds.push(`n.loja_id = $${params.length}`); }
    if (dataIni) { params.push(dataIni); conds.push(`n.data_emissao >= $${params.length}`); }
    if (dataFim) { params.push(dataFim); conds.push(`n.data_emissao <= $${params.length}`); }
    if (fornecedor) {
      params.push(`%${fornecedor}%`);
      conds.push(`(n.fornecedor_nome ILIKE $${params.length} OR n.fornecedor_cnpj ILIKE $${params.length})`);
    }
    const where = `WHERE ${conds.join(' AND ')}`;

    const [kpis, porFornecedor, distintos, itens] = await Promise.all([
      query(`
        SELECT COUNT(*)::int AS qtd_itens,
               COUNT(DISTINCT i.nota_id)::int AS qtd_notas,
               COUNT(DISTINCT n.fornecedor_cnpj)::int AS qtd_fornecedores,
               COUNT(DISTINCT COALESCE(NULLIF(i.ean_validado,''), NULLIF(i.ean_nota,''), i.descricao_nota))::int AS qtd_distintos,
               SUM(i.preco_total_nota)::numeric(14,2) AS valor_total,
               COUNT(*) FILTER (WHERE i.validado_cadastro = TRUE)::int AS qtd_validados,
               COUNT(*) FILTER (WHERE i.validado_cadastro = FALSE)::int AS qtd_pendentes
          FROM itens_nota i
          JOIN notas_entrada n ON n.id = i.nota_id
          ${where}
      `, params),
      query(`
        SELECT n.fornecedor_nome, n.fornecedor_cnpj,
               COUNT(*)::int AS qtd_itens,
               COUNT(DISTINCT COALESCE(NULLIF(i.ean_validado,''), NULLIF(i.ean_nota,''), i.descricao_nota))::int AS qtd_distintos,
               SUM(i.preco_total_nota)::numeric(14,2) AS valor_total,
               COUNT(*) FILTER (WHERE i.validado_cadastro = FALSE)::int AS qtd_pendentes
          FROM itens_nota i
          JOIN notas_entrada n ON n.id = i.nota_id
          ${where}
         GROUP BY n.fornecedor_nome, n.fornecedor_cnpj
         ORDER BY qtd_itens DESC
         LIMIT 30
      `, params),
      query(`
        SELECT
          COALESCE(NULLIF(i.ean_validado,''), NULLIF(i.ean_nota,''), 'sem-ean') AS ean,
          MIN(i.descricao_nota) AS descricao,
          COUNT(*)::int AS ocorrencias,
          COUNT(DISTINCT n.fornecedor_cnpj)::int AS qtd_fornecedores,
          MIN(n.data_emissao) AS primeira,
          MAX(n.data_emissao) AS ultima,
          SUM(i.quantidade)::numeric(14,3) AS qtd_total,
          SUM(i.preco_total_nota)::numeric(14,2) AS valor_total,
          BOOL_OR(i.validado_cadastro) AS algum_validado
        FROM itens_nota i
        JOIN notas_entrada n ON n.id = i.nota_id
        ${where}
        GROUP BY 1
        ORDER BY ocorrencias DESC
        LIMIT 200
      `, params),
      query(`
        SELECT i.id AS item_id, i.nota_id,
               n.numero_nota, n.fornecedor_nome, n.fornecedor_cnpj, n.data_emissao,
               n.loja_id, COALESCE(l.nome,'Sem loja') AS loja_nome,
               i.descricao_nota, i.ean_nota, i.ean_validado,
               i.quantidade, i.preco_unitario_nota, i.preco_total_nota,
               i.validado_cadastro
          FROM itens_nota i
          JOIN notas_entrada n ON n.id = i.nota_id
          LEFT JOIN lojas l ON l.id = n.loja_id
          ${where}
         ORDER BY n.data_emissao DESC, n.id DESC
         LIMIT 300
      `, params),
    ]);

    res.json({
      kpis: kpis[0],
      por_fornecedor: porFornecedor,
      produtos_distintos: distintos,
      itens,
      filtros: { loja: lojaId, dataIni, dataFim, fornecedor, status }
    });
  } catch (err) {
    console.error('[dashboard/produtos-novos]', err.message);
    res.status(500).json({ erro: err.message });
  }
});

// GET /api/dashboard/emergenciais
// Notas emergenciais (sem pedido previo) — compliance de compras.
router.get('/emergenciais', autenticar, async (req, res) => {
  try {
    const lojaUsr = req.usuario.loja_id;
    const lojaParam = req.query.loja ? parseInt(req.query.loja) : null;
    const lojaId = lojaUsr || lojaParam;
    const dataIni = req.query.dataIni || null;
    const dataFim = req.query.dataFim || null;
    const fornecedor = (req.query.fornecedor || '').trim();
    const comprador = (req.query.comprador || '').trim();
    const statusFiltro = req.query.statusFiltro || 'todas'; // todas | pendentes | aprovadas

    const conds = [`n.emergencial = TRUE`];
    const params = [];
    if (statusFiltro === 'pendentes') conds.push(`n.status = 'emergencial_pendente'`);
    if (statusFiltro === 'aprovadas') conds.push(`n.status <> 'emergencial_pendente'`);
    if (lojaId)  { params.push(lojaId);  conds.push(`n.loja_id = $${params.length}`); }
    if (dataIni) { params.push(dataIni); conds.push(`n.data_emissao >= $${params.length}`); }
    if (dataFim) { params.push(dataFim); conds.push(`n.data_emissao <= $${params.length}`); }
    if (fornecedor) {
      params.push(`%${fornecedor}%`);
      conds.push(`(n.fornecedor_nome ILIKE $${params.length} OR n.fornecedor_cnpj ILIKE $${params.length})`);
    }
    if (comprador) {
      params.push(`%${comprador}%`);
      conds.push(`n.importado_por ILIKE $${params.length}`);
    }
    const where = `WHERE ${conds.join(' AND ')}`;

    // mesmo where mas sem o filtro emergencial (pra calcular % do total)
    const condsTodas = conds.filter(c => c !== `n.emergencial = TRUE`);
    const whereTodas = condsTodas.length ? `WHERE ${condsTodas.join(' AND ')}` : '';

    const [kpis, totaisGerais, porFornecedor, porComprador, porLoja, notas] = await Promise.all([
      query(`
        SELECT COUNT(*)::int                            AS qtd,
               COALESCE(SUM(n.valor_total),0)::numeric  AS valor,
               COUNT(*) FILTER (WHERE n.status = 'emergencial_pendente')::int AS qtd_pendentes,
               COALESCE(SUM(n.valor_total) FILTER (WHERE n.status = 'emergencial_pendente'),0)::numeric AS valor_pendente,
               COUNT(DISTINCT n.fornecedor_cnpj)::int   AS qtd_fornecedores,
               COUNT(DISTINCT n.importado_por)::int     AS qtd_compradores
          FROM notas_entrada n
          ${where}
      `, params),
      query(`
        SELECT COUNT(*)::int                            AS qtd_total,
               COALESCE(SUM(n.valor_total),0)::numeric  AS valor_total
          FROM notas_entrada n
          ${whereTodas}
      `, params),
      query(`
        SELECT n.fornecedor_nome, n.fornecedor_cnpj,
               COUNT(*)::int AS qtd,
               COALESCE(SUM(n.valor_total),0)::numeric AS valor
          FROM notas_entrada n
          ${where}
         GROUP BY n.fornecedor_nome, n.fornecedor_cnpj
         ORDER BY qtd DESC
         LIMIT 30
      `, params),
      query(`
        SELECT COALESCE(NULLIF(n.importado_por,''),'(sem usuário)') AS comprador,
               COUNT(*)::int AS qtd,
               COALESCE(SUM(n.valor_total),0)::numeric AS valor
          FROM notas_entrada n
          ${where}
         GROUP BY n.importado_por
         ORDER BY qtd DESC
         LIMIT 30
      `, params),
      query(`
        SELECT n.loja_id,
               COALESCE(l.nome,'Sem loja') AS loja_nome,
               COUNT(*)::int AS qtd,
               COALESCE(SUM(n.valor_total),0)::numeric AS valor
          FROM notas_entrada n
          LEFT JOIN lojas l ON l.id = n.loja_id
          ${where}
         GROUP BY n.loja_id, l.nome
         ORDER BY n.loja_id
      `, params),
      query(`
        SELECT n.id, n.numero_nota, n.fornecedor_nome, n.fornecedor_cnpj,
               n.valor_total, n.status, n.data_emissao, n.importado_em,
               n.importado_por,
               n.loja_id, COALESCE(l.nome,'Sem loja') AS loja_nome
          FROM notas_entrada n
          LEFT JOIN lojas l ON l.id = n.loja_id
          ${where}
         ORDER BY n.data_emissao DESC, n.id DESC
         LIMIT 300
      `, params),
    ]);

    res.json({
      kpis: kpis[0],
      totais_gerais: totaisGerais[0],
      por_fornecedor: porFornecedor,
      por_comprador: porComprador,
      por_loja: porLoja,
      notas,
      filtros: { loja: lojaId, dataIni, dataFim, fornecedor, comprador, statusFiltro }
    });
  } catch (err) {
    console.error('[dashboard/emergenciais]', err.message);
    res.status(500).json({ erro: err.message });
  }
});

// GET /api/dashboard/sla-notas
// Tempo medio que as notas ficam em cada etapa do fluxo. Identifica gargalos.
// Etapas (calculo em horas):
//   1. Cadastro:   importado_em -> validada_em
//   2. Recepcao:   validada_em  -> recebida_em
//   3. Conferencia: recebida_em -> liberada_em
//   4. Auditoria:  liberada_em  -> fechado_em
//   - Lead total:  importado_em -> fechado_em
router.get('/sla-notas', autenticar, async (req, res) => {
  try {
    const lojaUsr = req.usuario.loja_id;
    const lojaParam = req.query.loja ? parseInt(req.query.loja) : null;
    const lojaId = lojaUsr || lojaParam;
    const dataIni = req.query.dataIni || null;
    const dataFim = req.query.dataFim || null;
    const fornecedor = (req.query.fornecedor || '').trim();

    const conds = [];
    const params = [];
    if (lojaId)  { params.push(lojaId);  conds.push(`n.loja_id = $${params.length}`); }
    if (dataIni) { params.push(dataIni); conds.push(`n.data_emissao >= $${params.length}`); }
    if (dataFim) { params.push(dataFim); conds.push(`n.data_emissao <= $${params.length}`); }
    if (fornecedor) {
      params.push(`%${fornecedor}%`);
      conds.push(`(n.fornecedor_nome ILIKE $${params.length} OR n.fornecedor_cnpj ILIKE $${params.length})`);
    }
    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';

    // Etapas e KPIs gerais (em horas)
    const etapas = await query(`
      SELECT
        COUNT(*) FILTER (WHERE n.validada_em IS NOT NULL)::int AS qtd_cadastro,
        AVG(EXTRACT(EPOCH FROM (n.validada_em - n.importado_em))/3600) FILTER (WHERE n.validada_em IS NOT NULL)::numeric(10,2) AS h_cadastro_avg,
        PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (n.validada_em - n.importado_em))/3600) FILTER (WHERE n.validada_em IS NOT NULL)::numeric(10,2) AS h_cadastro_p50,
        PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (n.validada_em - n.importado_em))/3600) FILTER (WHERE n.validada_em IS NOT NULL)::numeric(10,2) AS h_cadastro_p95,

        COUNT(*) FILTER (WHERE n.recebida_em IS NOT NULL AND n.validada_em IS NOT NULL)::int AS qtd_recepcao,
        AVG(EXTRACT(EPOCH FROM (n.recebida_em - n.validada_em))/3600) FILTER (WHERE n.recebida_em IS NOT NULL AND n.validada_em IS NOT NULL)::numeric(10,2) AS h_recepcao_avg,
        PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (n.recebida_em - n.validada_em))/3600) FILTER (WHERE n.recebida_em IS NOT NULL AND n.validada_em IS NOT NULL)::numeric(10,2) AS h_recepcao_p50,
        PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (n.recebida_em - n.validada_em))/3600) FILTER (WHERE n.recebida_em IS NOT NULL AND n.validada_em IS NOT NULL)::numeric(10,2) AS h_recepcao_p95,

        COUNT(*) FILTER (WHERE n.liberada_em IS NOT NULL AND n.recebida_em IS NOT NULL)::int AS qtd_conferencia,
        AVG(EXTRACT(EPOCH FROM (n.liberada_em - n.recebida_em))/3600) FILTER (WHERE n.liberada_em IS NOT NULL AND n.recebida_em IS NOT NULL)::numeric(10,2) AS h_conferencia_avg,
        PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (n.liberada_em - n.recebida_em))/3600) FILTER (WHERE n.liberada_em IS NOT NULL AND n.recebida_em IS NOT NULL)::numeric(10,2) AS h_conferencia_p50,
        PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (n.liberada_em - n.recebida_em))/3600) FILTER (WHERE n.liberada_em IS NOT NULL AND n.recebida_em IS NOT NULL)::numeric(10,2) AS h_conferencia_p95,

        COUNT(*) FILTER (WHERE n.fechado_em IS NOT NULL AND n.liberada_em IS NOT NULL)::int AS qtd_auditoria,
        AVG(EXTRACT(EPOCH FROM (n.fechado_em - n.liberada_em))/3600) FILTER (WHERE n.fechado_em IS NOT NULL AND n.liberada_em IS NOT NULL)::numeric(10,2) AS h_auditoria_avg,
        PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (n.fechado_em - n.liberada_em))/3600) FILTER (WHERE n.fechado_em IS NOT NULL AND n.liberada_em IS NOT NULL)::numeric(10,2) AS h_auditoria_p50,
        PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (n.fechado_em - n.liberada_em))/3600) FILTER (WHERE n.fechado_em IS NOT NULL AND n.liberada_em IS NOT NULL)::numeric(10,2) AS h_auditoria_p95,

        COUNT(*) FILTER (WHERE n.fechado_em IS NOT NULL)::int AS qtd_total_fechadas,
        AVG(EXTRACT(EPOCH FROM (n.fechado_em - n.importado_em))/3600) FILTER (WHERE n.fechado_em IS NOT NULL)::numeric(10,2) AS h_total_avg,
        PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (n.fechado_em - n.importado_em))/3600) FILTER (WHERE n.fechado_em IS NOT NULL)::numeric(10,2) AS h_total_p50,
        PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (n.fechado_em - n.importado_em))/3600) FILTER (WHERE n.fechado_em IS NOT NULL)::numeric(10,2) AS h_total_p95
      FROM notas_entrada n
      ${where}
    `, params);

    // Por loja: lead time medio (importado -> fechado) e medias por etapa
    const porLoja = await query(`
      SELECT n.loja_id,
             COALESCE(l.nome,'Sem loja') AS loja_nome,
             COUNT(*) FILTER (WHERE n.fechado_em IS NOT NULL)::int AS qtd_fechadas,
             AVG(EXTRACT(EPOCH FROM (n.validada_em - n.importado_em))/3600) FILTER (WHERE n.validada_em IS NOT NULL)::numeric(10,1) AS h_cadastro,
             AVG(EXTRACT(EPOCH FROM (n.recebida_em - n.validada_em))/3600) FILTER (WHERE n.recebida_em IS NOT NULL AND n.validada_em IS NOT NULL)::numeric(10,1) AS h_recepcao,
             AVG(EXTRACT(EPOCH FROM (n.liberada_em - n.recebida_em))/3600) FILTER (WHERE n.liberada_em IS NOT NULL AND n.recebida_em IS NOT NULL)::numeric(10,1) AS h_conferencia,
             AVG(EXTRACT(EPOCH FROM (n.fechado_em - n.liberada_em))/3600) FILTER (WHERE n.fechado_em IS NOT NULL AND n.liberada_em IS NOT NULL)::numeric(10,1) AS h_auditoria,
             AVG(EXTRACT(EPOCH FROM (n.fechado_em - n.importado_em))/3600) FILTER (WHERE n.fechado_em IS NOT NULL)::numeric(10,1) AS h_total
        FROM notas_entrada n
        LEFT JOIN lojas l ON l.id = n.loja_id
        ${where}
       GROUP BY n.loja_id, l.nome
       ORDER BY n.loja_id
    `, params);

    // Notas em fluxo agora — quanto tempo no status atual
    const condsAndar = [...conds, `n.status NOT IN ('fechada','cancelada')`];
    const whereAndar = `WHERE ${condsAndar.join(' AND ')}`;
    const emAndamento = await query(`
      SELECT n.status,
             COUNT(*)::int AS qtd,
             AVG(EXTRACT(EPOCH FROM (NOW() - n.importado_em))/3600)::numeric(10,1) AS h_desde_importacao,
             MAX(EXTRACT(EPOCH FROM (NOW() - n.importado_em))/3600)::numeric(10,1) AS h_max
        FROM notas_entrada n
        ${whereAndar}
       GROUP BY n.status
       ORDER BY n.status
    `, params);

    res.json({
      etapas: etapas[0],
      por_loja: porLoja,
      em_andamento: emAndamento,
      filtros: { loja: lojaId, dataIni, dataFim, fornecedor }
    });
  } catch (err) {
    console.error('[dashboard/sla-notas]', err.message);
    res.status(500).json({ erro: err.message });
  }
});

// GET /api/dashboard/divergencias-estoque
// Itens da auditoria com status=divergente — diferenca entre qtd da NF e qtd contada.
// Tipo: falta (contou menos) | sobra (contou mais).
router.get('/divergencias-estoque', autenticar, async (req, res) => {
  try {
    const lojaUsr = req.usuario.loja_id;
    const lojaParam = req.query.loja ? parseInt(req.query.loja) : null;
    const lojaId = lojaUsr || lojaParam;
    const dataIni = req.query.dataIni || null;
    const dataFim = req.query.dataFim || null;
    const fornecedor = (req.query.fornecedor || '').trim();
    const produto = (req.query.produto || '').trim();
    const tipo = req.query.tipo || 'todos'; // todos | falta | sobra

    const conds = [`a.status = 'divergente'`];
    const params = [];
    if (lojaId)  { params.push(lojaId);  conds.push(`n.loja_id = $${params.length}`); }
    if (dataIni) { params.push(dataIni); conds.push(`n.data_emissao >= $${params.length}`); }
    if (dataFim) { params.push(dataFim); conds.push(`n.data_emissao <= $${params.length}`); }
    if (fornecedor) {
      params.push(`%${fornecedor}%`);
      conds.push(`(n.fornecedor_nome ILIKE $${params.length} OR n.fornecedor_cnpj ILIKE $${params.length})`);
    }
    if (produto) {
      params.push(`%${produto}%`);
      conds.push(`(i.descricao_nota ILIKE $${params.length} OR i.ean_validado ILIKE $${params.length} OR i.ean_nota ILIKE $${params.length})`);
    }
    if (tipo === 'falta') conds.push(`a.qtd_contada < i.quantidade`);
    if (tipo === 'sobra') conds.push(`a.qtd_contada > i.quantidade`);
    const where = `WHERE ${conds.join(' AND ')}`;

    const baseFrom = `
      FROM auditoria_itens a
      JOIN itens_nota i ON i.id = a.item_id
      JOIN notas_entrada n ON n.id = i.nota_id
      LEFT JOIN lojas l ON l.id = n.loja_id
    `;

    const [kpis, porFornecedor, porProduto, porLoja, itens] = await Promise.all([
      query(`
        SELECT COUNT(*)::int AS qtd_itens,
               COUNT(*) FILTER (WHERE a.qtd_contada < i.quantidade)::int AS qtd_falta,
               COUNT(*) FILTER (WHERE a.qtd_contada > i.quantidade)::int AS qtd_sobra,
               COALESCE(SUM(GREATEST(i.quantidade - a.qtd_contada, 0) * i.preco_unitario_nota),0)::numeric(14,2) AS valor_falta,
               COALESCE(SUM(GREATEST(a.qtd_contada - i.quantidade, 0) * i.preco_unitario_nota),0)::numeric(14,2) AS valor_sobra,
               COALESCE(SUM((a.qtd_contada - i.quantidade) * i.preco_unitario_nota),0)::numeric(14,2) AS valor_liquido,
               COUNT(DISTINCT i.nota_id)::int AS qtd_notas,
               COUNT(DISTINCT n.fornecedor_cnpj)::int AS qtd_fornecedores
          ${baseFrom}
          ${where}
      `, params),
      query(`
        SELECT n.fornecedor_nome, n.fornecedor_cnpj,
               COUNT(*)::int AS qtd_itens,
               COALESCE(SUM(GREATEST(i.quantidade - a.qtd_contada, 0) * i.preco_unitario_nota),0)::numeric(14,2) AS valor_falta,
               COALESCE(SUM(GREATEST(a.qtd_contada - i.quantidade, 0) * i.preco_unitario_nota),0)::numeric(14,2) AS valor_sobra
          ${baseFrom}
          ${where}
         GROUP BY n.fornecedor_nome, n.fornecedor_cnpj
         ORDER BY (
           COALESCE(SUM(GREATEST(i.quantidade - a.qtd_contada, 0) * i.preco_unitario_nota),0) +
           COALESCE(SUM(GREATEST(a.qtd_contada - i.quantidade, 0) * i.preco_unitario_nota),0)
         ) DESC
         LIMIT 30
      `, params),
      query(`
        SELECT
          COALESCE(NULLIF(i.ean_validado,''), NULLIF(i.ean_nota,''), 'sem-ean') AS ean,
          MIN(i.descricao_nota) AS descricao,
          COUNT(*)::int AS ocorrencias,
          COALESCE(SUM(i.quantidade - a.qtd_contada),0)::numeric(14,3) AS dif_qtd,
          COALESCE(SUM((i.quantidade - a.qtd_contada) * i.preco_unitario_nota),0)::numeric(14,2) AS dif_valor
          ${baseFrom}
          ${where}
         GROUP BY 1
         ORDER BY ABS(SUM((i.quantidade - a.qtd_contada) * i.preco_unitario_nota)) DESC
         LIMIT 50
      `, params),
      query(`
        SELECT n.loja_id,
               COALESCE(l.nome,'Sem loja') AS loja_nome,
               COUNT(*)::int AS qtd_itens,
               COALESCE(SUM(GREATEST(i.quantidade - a.qtd_contada, 0) * i.preco_unitario_nota),0)::numeric(14,2) AS valor_falta,
               COALESCE(SUM(GREATEST(a.qtd_contada - i.quantidade, 0) * i.preco_unitario_nota),0)::numeric(14,2) AS valor_sobra
          ${baseFrom}
          ${where}
         GROUP BY n.loja_id, l.nome
         ORDER BY n.loja_id
      `, params),
      query(`
        SELECT a.item_id, a.qtd_contada, a.lote, a.validade, a.observacao, a.auditado_em, a.auditado_por,
               i.nota_id, i.descricao_nota, i.ean_validado, i.ean_nota,
               i.quantidade AS qtd_nota, i.preco_unitario_nota,
               (a.qtd_contada - i.quantidade)::numeric(14,3) AS diferenca,
               ((a.qtd_contada - i.quantidade) * i.preco_unitario_nota)::numeric(14,2) AS dif_valor,
               n.numero_nota, n.fornecedor_nome, n.fornecedor_cnpj, n.data_emissao, n.loja_id,
               COALESCE(l.nome,'Sem loja') AS loja_nome
          ${baseFrom}
          ${where}
         ORDER BY a.auditado_em DESC NULLS LAST
         LIMIT 500
      `, params),
    ]);

    res.json({
      kpis: kpis[0],
      por_fornecedor: porFornecedor,
      por_produto: porProduto,
      por_loja: porLoja,
      itens,
      filtros: { loja: lojaId, dataIni, dataFim, fornecedor, produto, tipo }
    });
  } catch (err) {
    console.error('[dashboard/divergencias-estoque]', err.message);
    res.status(500).json({ erro: err.message });
  }
});

// GET /api/dashboard/pedidos-fornecedor
// Volume e valor de pedidos por fornecedor/periodo, mais % emergenciais (notas
// sem pedido vinculado) e prazo medio de pagamento.
router.get('/pedidos-fornecedor', autenticar, async (req, res) => {
  try {
    const lojaUsr = req.usuario.loja_id;
    const lojaParam = req.query.loja ? parseInt(req.query.loja) : null;
    const lojaId = lojaUsr || lojaParam;
    const dataIni = req.query.dataIni || null;
    const dataFim = req.query.dataFim || null;
    const fornecedor = (req.query.fornecedor || '').trim();

    // pedidos: filtro por criado_em (nao data_emissao). Para emergenciais usa data_emissao da nota.
    const condsP = [];
    const condsN = [`n.emergencial = TRUE`];
    const params = [];
    if (lojaId)  { params.push(lojaId);  condsP.push(`p.loja_id = $${params.length}`); condsN.push(`n.loja_id = $${params.length}`); }
    if (dataIni) { params.push(dataIni); condsP.push(`p.criado_em::date >= $${params.length}`); condsN.push(`n.data_emissao >= $${params.length}`); }
    if (dataFim) { params.push(dataFim); condsP.push(`p.criado_em::date <= $${params.length}`); condsN.push(`n.data_emissao <= $${params.length}`); }
    if (fornecedor) {
      params.push(`%${fornecedor}%`);
      condsP.push(`(f.razao_social ILIKE $${params.length} OR f.cnpj ILIKE $${params.length})`);
      condsN.push(`(n.fornecedor_nome ILIKE $${params.length} OR n.fornecedor_cnpj ILIKE $${params.length})`);
    }
    const whereP = condsP.length ? `WHERE ${condsP.join(' AND ')}` : '';
    const whereN = `WHERE ${condsN.join(' AND ')}`;

    const [kpisPed, kpisEmerg, porFornecedor, porLoja, porMes, lista] = await Promise.all([
      query(`
        SELECT COUNT(*)::int AS qtd,
               COALESCE(SUM(p.valor_total),0)::numeric AS valor,
               COUNT(*) FILTER (WHERE p.status = 'validado')::int AS qtd_validados,
               COUNT(*) FILTER (WHERE p.status = 'aguardando_auditoria')::int AS qtd_aguardando,
               COUNT(*) FILTER (WHERE p.status = 'rascunho')::int AS qtd_rascunho,
               COUNT(DISTINCT p.fornecedor_id)::int AS qtd_fornecedores,
               COALESCE(AVG(p.condicao_pagamento) FILTER (WHERE p.condicao_pagamento > 0),0)::numeric(10,1) AS prazo_medio
          FROM pedidos p
          LEFT JOIN fornecedores f ON f.id = p.fornecedor_id
          ${whereP}
      `, params),
      query(`
        SELECT COUNT(*)::int AS qtd,
               COALESCE(SUM(n.valor_total),0)::numeric AS valor
          FROM notas_entrada n
          ${whereN}
      `, params),
      query(`
        WITH ped AS (
          SELECT COALESCE(f.cnpj, '') AS cnpj,
                 COALESCE(f.razao_social, '(sem nome)') AS nome,
                 COUNT(*)::int AS qtd_pedidos,
                 COALESCE(SUM(p.valor_total),0)::numeric AS valor_pedidos,
                 COALESCE(AVG(p.condicao_pagamento) FILTER (WHERE p.condicao_pagamento > 0),0)::numeric(10,1) AS prazo_medio
            FROM pedidos p
            LEFT JOIN fornecedores f ON f.id = p.fornecedor_id
            ${whereP}
           GROUP BY f.cnpj, f.razao_social
        ),
        emerg AS (
          SELECT COALESCE(n.fornecedor_cnpj,'') AS cnpj,
                 COALESCE(MIN(n.fornecedor_nome),'(sem nome)') AS nome,
                 COUNT(*)::int AS qtd_emergenciais,
                 COALESCE(SUM(n.valor_total),0)::numeric AS valor_emergenciais
            FROM notas_entrada n
            ${whereN}
           GROUP BY n.fornecedor_cnpj
        )
        SELECT COALESCE(p.cnpj, e.cnpj) AS cnpj,
               COALESCE(p.nome, e.nome) AS nome,
               COALESCE(p.qtd_pedidos,0) AS qtd_pedidos,
               COALESCE(p.valor_pedidos,0) AS valor_pedidos,
               COALESCE(p.prazo_medio,0) AS prazo_medio,
               COALESCE(e.qtd_emergenciais,0) AS qtd_emergenciais,
               COALESCE(e.valor_emergenciais,0) AS valor_emergenciais
          FROM ped p
          FULL OUTER JOIN emerg e ON e.cnpj = p.cnpj AND p.cnpj <> ''
         ORDER BY (COALESCE(p.valor_pedidos,0) + COALESCE(e.valor_emergenciais,0)) DESC
         LIMIT 50
      `, params),
      query(`
        SELECT p.loja_id,
               COALESCE(l.nome,'Sem loja') AS loja_nome,
               COUNT(*)::int AS qtd,
               COALESCE(SUM(p.valor_total),0)::numeric AS valor,
               COALESCE(AVG(p.condicao_pagamento) FILTER (WHERE p.condicao_pagamento > 0),0)::numeric(10,1) AS prazo_medio
          FROM pedidos p
          LEFT JOIN lojas l ON l.id = p.loja_id
          LEFT JOIN fornecedores f ON f.id = p.fornecedor_id
          ${whereP}
         GROUP BY p.loja_id, l.nome
         ORDER BY p.loja_id
      `, params),
      query(`
        SELECT TO_CHAR(p.criado_em, 'YYYY-MM') AS mes,
               COUNT(*)::int AS qtd,
               COALESCE(SUM(p.valor_total),0)::numeric AS valor
          FROM pedidos p
          LEFT JOIN fornecedores f ON f.id = p.fornecedor_id
          ${whereP}
         GROUP BY 1
         ORDER BY 1 DESC
         LIMIT 24
      `, params),
      query(`
        SELECT p.id, p.numero_pedido, p.status, p.valor_total, p.condicao_pagamento,
               p.criado_em, p.enviado_em, p.validado_em,
               COALESCE(f.razao_social,'-') AS fornecedor_nome,
               f.cnpj AS fornecedor_cnpj,
               p.loja_id, COALESCE(l.nome,'Sem loja') AS loja_nome,
               p.nota_id
          FROM pedidos p
          LEFT JOIN fornecedores f ON f.id = p.fornecedor_id
          LEFT JOIN lojas l ON l.id = p.loja_id
          ${whereP}
         ORDER BY p.criado_em DESC
         LIMIT 300
      `, params),
    ]);

    res.json({
      kpis_pedidos: kpisPed[0],
      kpis_emergenciais: kpisEmerg[0],
      por_fornecedor: porFornecedor,
      por_loja: porLoja,
      por_mes: porMes,
      lista,
      filtros: { loja: lojaId, dataIni, dataFim, fornecedor }
    });
  } catch (err) {
    console.error('[dashboard/pedidos-fornecedor]', err.message);
    res.status(500).json({ erro: err.message });
  }
});

module.exports = router;
