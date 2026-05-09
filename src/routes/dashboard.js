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
// Lista itens cujo preco da NF-e divergiu do custo de fabrica (custoorigem).
// Filtros: loja, dataIni/dataFim (data_emissao), fornecedor, status_preco (default: 'maior','auditagem').
router.get('/divergencias-preco', autenticar, async (req, res) => {
  try {
    const lojaUsr = req.usuario.loja_id;
    const lojaParam = req.query.loja ? parseInt(req.query.loja) : null;
    const lojaId = lojaUsr || lojaParam;
    const dataIni = req.query.dataIni || null;
    const dataFim = req.query.dataFim || null;
    const fornecedor = (req.query.fornecedor || '').trim();
    const tiposParam = (req.query.tipos || 'maior,auditagem').split(',').map(s=>s.trim()).filter(Boolean);

    const conds = [`i.custo_fabrica IS NOT NULL`, `i.custo_fabrica > 0`];
    const params = [];
    if (tiposParam.length) {
      params.push(tiposParam);
      conds.push(`i.status_preco = ANY($${params.length})`);
    }
    if (lojaId)  { params.push(lojaId);  conds.push(`n.loja_id = $${params.length}`); }
    if (dataIni) { params.push(dataIni); conds.push(`n.data_emissao >= $${params.length}`); }
    if (dataFim) { params.push(dataFim); conds.push(`n.data_emissao <= $${params.length}`); }
    if (fornecedor) {
      params.push(`%${fornecedor}%`);
      conds.push(`(n.fornecedor_nome ILIKE $${params.length} OR n.fornecedor_cnpj ILIKE $${params.length})`);
    }
    const where = `WHERE ${conds.join(' AND ')}`;

    const [itens, totaisFornecedor, totaisStatus, kpis] = await Promise.all([
      query(`
        SELECT i.id AS item_id, i.nota_id,
               n.numero_nota, n.fornecedor_nome, n.fornecedor_cnpj, n.data_emissao,
               n.loja_id, COALESCE(l.nome,'Sem loja') AS loja_nome,
               i.descricao_nota, i.ean_nota, i.ean_validado,
               i.quantidade, i.preco_unitario_nota, i.custo_fabrica, i.status_preco,
               (i.preco_unitario_nota - i.custo_fabrica)::numeric(14,4) AS diferenca_unit,
               (i.quantidade * (i.preco_unitario_nota - i.custo_fabrica))::numeric(14,2) AS diferenca_total,
               CASE WHEN i.custo_fabrica > 0
                    THEN ((i.preco_unitario_nota - i.custo_fabrica) / i.custo_fabrica * 100)::numeric(8,2)
                    ELSE NULL END AS pct
          FROM itens_nota i
          JOIN notas_entrada n ON n.id = i.nota_id
          LEFT JOIN lojas l ON l.id = n.loja_id
          ${where}
         ORDER BY ABS(i.quantidade * (i.preco_unitario_nota - i.custo_fabrica)) DESC
         LIMIT 500
      `, params),
      query(`
        SELECT n.fornecedor_nome, n.fornecedor_cnpj,
               COUNT(*)::int AS qtd_itens,
               SUM(i.quantidade * (i.preco_unitario_nota - i.custo_fabrica))::numeric(14,2) AS dif_total
          FROM itens_nota i
          JOIN notas_entrada n ON n.id = i.nota_id
          ${where}
         GROUP BY n.fornecedor_nome, n.fornecedor_cnpj
         ORDER BY ABS(SUM(i.quantidade * (i.preco_unitario_nota - i.custo_fabrica))) DESC
         LIMIT 30
      `, params),
      query(`
        SELECT i.status_preco,
               COUNT(*)::int AS qtd,
               SUM(i.quantidade * (i.preco_unitario_nota - i.custo_fabrica))::numeric(14,2) AS dif_total
          FROM itens_nota i
          JOIN notas_entrada n ON n.id = i.nota_id
          ${where}
         GROUP BY i.status_preco
         ORDER BY i.status_preco
      `, params),
      query(`
        SELECT COUNT(*)::int AS qtd_itens,
               COUNT(DISTINCT i.nota_id)::int AS qtd_notas,
               COUNT(DISTINCT n.fornecedor_cnpj)::int AS qtd_fornecedores,
               SUM(i.quantidade * (i.preco_unitario_nota - i.custo_fabrica))::numeric(14,2) AS dif_total,
               SUM(CASE WHEN i.status_preco='maior' THEN i.quantidade*(i.preco_unitario_nota-i.custo_fabrica) ELSE 0 END)::numeric(14,2) AS dif_maior,
               SUM(CASE WHEN i.status_preco='auditagem' THEN i.quantidade*(i.preco_unitario_nota-i.custo_fabrica) ELSE 0 END)::numeric(14,2) AS dif_auditagem
          FROM itens_nota i
          JOIN notas_entrada n ON n.id = i.nota_id
          ${where}
      `, params),
    ]);

    res.json({
      itens,
      por_fornecedor: totaisFornecedor,
      por_status: totaisStatus,
      kpis: kpis[0],
      filtros: { loja: lojaId, dataIni, dataFim, fornecedor, tipos: tiposParam }
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

module.exports = router;
