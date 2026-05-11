// Cross-check app x ERP (Ecocentauro). Roda 1x ao dia.
//
// Categorias:
//
//   FINALIZADAS_ECO  = nota CHEGOU NO ERP da loja (compras_historico)
//                      MAS NÃO foi finalizada no nosso app
//                      (ou nem foi importada no app — XML perdido)
//                      Excluindo: notas com mcp_status='C' no CD (canceladas)
//
//   N_FINALIZADAS_ECO = nota foi FECHADA no app (status fechada/validada)
//                       MAS NÃO chegou no ERP da loja (sem compras_historico)
//                       Janela: importado_em > 24h atrás
//
// Match nota_entrada ↔ compras_historico: loja_id + numero_nota + fornecedor_cnpj
// Match nota_entrada ↔ cd_movcompra (pra mcp_status): cd_codigo do CD origem (via fornecedor_cnpj→destino) + mcp_nnotafis = numero_nota

const express = require('express');
const router = express.Router();
const { query: dbQuery } = require('../db');
const { autenticar, exigirPerfil, JWT_SECRET } = require('../auth');
const jwt = require('jsonwebtoken');

const apenasAdmin = [autenticar, exigirPerfil('admin')];

// Histórico anterior a esta data é considerado "lixo" (notas legadas, sem rastreabilidade no app).
// Detector só processa notas que entraram no ERP a partir desta data (inclusive).
const DATA_CORTE_ECO = '2026-05-09';

function autenticarComQS(req, res, next) {
  let token = (req.headers.authorization || '').replace(/^Bearer\s+/i, '');
  if (!token && req.query.token) token = String(req.query.token);
  if (!token) return res.status(401).json({ erro: 'Token ausente' });
  try {
    req.usuario = jwt.verify(token, JWT_SECRET);
    if (req.usuario.perfil !== 'admin') return res.status(403).json({ erro: 'Acesso negado' });
    next();
  } catch { res.status(401).json({ erro: 'Token invalido' }); }
}

// Detector — 3 cenários:
//
// A) UPDATE notas existentes que chegaram no ERP, status intermediário no app,
//    NÃO canceladas no CD (mcp_status != 'C')
//      → status='finalizada_f', auditoria_eco_status='finalizadas_eco'
//
// B) INSERT notas que vieram de CDs cadastrados, chegaram no ERP, MAS não existem em notas_entrada
//    NÃO canceladas no CD
//      → status='finalizada_f', auditoria_eco_status='finalizadas_eco', motivo='NUNCA IMPORTADA'
//
// C) UPDATE notas fechadas no app que NÃO chegaram no ERP em >24h
//      → mantém status original, marca auditoria_eco_status='n_finalizadas_eco'
async function detectarStatusEco() {
  // Atualiza cache mcp_status_cd das notas com origem=cd via JOIN com cd_movcompra do CD origem
  // CD origem = pedidos_distrib_destinos onde CNPJ bate com fornecedor_cnpj da nota
  // Match no cd_movcompra: mcp_codi = cd_mov_codi (NÃO mcp_nnotafis, que vem vazio)
  await dbQuery(`
    UPDATE notas_entrada n
       SET mcp_status_cd = sub.mcp_status
      FROM (
        SELECT DISTINCT ON (n2.id) n2.id, mc.mcp_status
          FROM notas_entrada n2
          JOIN pedidos_distrib_destinos d
            ON d.tipo='CD' AND d.cd_codigo IS NOT NULL
           AND COALESCE(NULLIF(d.cnpj,''),'') = COALESCE(NULLIF(n2.fornecedor_cnpj,''),'')
          JOIN cd_movcompra mc
            ON mc.cd_codigo = d.cd_codigo
           AND REGEXP_REPLACE(COALESCE(mc.mcp_codi,''), '^0+', '') =
               REGEXP_REPLACE(COALESCE(n2.cd_mov_codi, n2.numero_nota, ''), '^0+', '')
         WHERE n2.origem IN ('cd','transferencia_loja')
           AND (n2.mcp_status_cd IS NULL OR n2.mcp_status_cd <> mc.mcp_status)
         ORDER BY n2.id, mc.mcp_dten DESC
      ) sub
     WHERE sub.id = n.id
  `);

  // Atualiza cache chegou_no_erp_em via compras_historico — DOIS UPDATEs separados
  // (origem='nfe' usa match direto = pode usar índice; origem='cd' usa REGEXP)

  // Cache 1: NF-e fornecedor — match direto numero_nota=numeronfe (rápido com índice)
  await dbQuery(`
    UPDATE notas_entrada n
       SET chegou_no_erp_em = sub.data_entrada
      FROM (
        SELECT n2.id, MIN(c.data_entrada) AS data_entrada
          FROM notas_entrada n2
          JOIN compras_historico c
            ON c.loja_id = n2.loja_id
           AND c.fornecedor_cnpj = n2.fornecedor_cnpj
           AND c.numeronfe = n2.numero_nota
         WHERE n2.chegou_no_erp_em IS NULL
           AND n2.origem = 'nfe'
           AND n2.fornecedor_cnpj IS NOT NULL
           AND n2.numero_nota IS NOT NULL
         GROUP BY n2.id
      ) sub
     WHERE sub.id = n.id
  `);

  // Cache 2: transferências CD — match com normalização de zeros via cd_mov_codi
  await dbQuery(`
    UPDATE notas_entrada n
       SET chegou_no_erp_em = sub.data_entrada
      FROM (
        SELECT n2.id, MIN(c.data_entrada) AS data_entrada
          FROM notas_entrada n2
          JOIN compras_historico c
            ON c.loja_id = n2.loja_id
           AND c.fornecedor_cnpj = n2.fornecedor_cnpj
           AND REGEXP_REPLACE(c.numeronfe, '^0+', '') =
               REGEXP_REPLACE(COALESCE(n2.cd_mov_codi, n2.numero_nota), '^0+', '')
         WHERE n2.chegou_no_erp_em IS NULL
           AND n2.origem IN ('cd','transferencia_loja')
           AND n2.fornecedor_cnpj IS NOT NULL
         GROUP BY n2.id
      ) sub
     WHERE sub.id = n.id
  `);

  // Cenário A: notas EXISTENTES que chegaram no ERP, status nao fechado, e NAO canceladas no CD
  // Inclui NF-e fornecedor E transferências CD
  const updatedA = await dbQuery(`
    UPDATE notas_entrada
       SET status = 'finalizada_f',
           auditoria_eco_status = 'finalizadas_eco',
           auditoria_eco_em = NOW(),
           finalizada_f_em = NOW(),
           finalizada_f_motivo = 'chegou no ERP mas pulou cadastro/conferencia/auditoria do app'
     WHERE chegou_no_erp_em IS NOT NULL
       AND chegou_no_erp_em >= $1::date
       AND origem IN ('nfe','cd','transferencia_loja')
       AND status NOT IN ('fechada','validada','arquivada','cancelada','finalizada_f')
       AND COALESCE(mcp_status_cd, 'A') <> 'C'
     RETURNING id, loja_id, numero_nota, fornecedor_cnpj
  `, [DATA_CORTE_ECO]);

  // Cenário B: notas que NAO existem em notas_entrada mas chegaram no ERP via CD
  const insertedB = await dbQuery(`
    WITH cnpjs_cd AS (
      SELECT DISTINCT cnpj, nome FROM pedidos_distrib_destinos WHERE tipo = 'CD' AND cnpj IS NOT NULL
    ),
    candidatas AS (
      SELECT c.loja_id, c.numeronfe, c.fornecedor_cnpj,
             cd.nome AS fornecedor_nome,
             MIN(c.data_emissao) AS data_emissao,
             MIN(c.data_entrada) AS data_entrada,
             SUM(COALESCE(c.custo_total, 0)) AS valor_total
        FROM compras_historico c
        JOIN cnpjs_cd cd ON cd.cnpj = c.fornecedor_cnpj
       WHERE c.data_entrada >= $1::date
         AND NOT EXISTS (
           SELECT 1 FROM notas_entrada n
            WHERE n.loja_id = c.loja_id AND n.numero_nota = c.numeronfe
              AND COALESCE(NULLIF(n.fornecedor_cnpj,''),'') =
                  COALESCE(NULLIF(c.fornecedor_cnpj,''),'')
         )
       GROUP BY c.loja_id, c.numeronfe, c.fornecedor_cnpj, cd.nome
    )
    INSERT INTO notas_entrada (
      loja_id, numero_nota, fornecedor_cnpj, fornecedor_nome,
      data_emissao, valor_total, status, origem,
      auditoria_eco_status, auditoria_eco_em,
      finalizada_f_em, finalizada_f_motivo,
      chegou_no_erp_em, importado_em
    )
    SELECT loja_id, numeronfe, fornecedor_cnpj, fornecedor_nome,
           data_emissao, valor_total, 'finalizada_f', 'cd',
           'finalizadas_eco', NOW(),
           NOW(), 'NUNCA IMPORTADA — XML nao processado, mas chegou na loja',
           data_entrada, NOW()
      FROM candidatas
    RETURNING id, loja_id, numero_nota, fornecedor_cnpj
  `, [DATA_CORTE_ECO]);

  // Cenário C: notas FECHADAS no app que NAO chegaram no ERP em >24h
  const updatedC = await dbQuery(`
    UPDATE notas_entrada
       SET auditoria_eco_status = 'n_finalizadas_eco',
           auditoria_eco_em = NOW()
     WHERE status IN ('fechada','validada')
       AND origem IN ('nfe','cd','transferencia_loja')
       AND chegou_no_erp_em IS NULL
       AND importado_em < NOW() - INTERVAL '24 hours'
       AND importado_em >= $1::date
       AND auditoria_eco_status IS NULL
       AND COALESCE(mcp_status_cd, 'A') <> 'C'
     RETURNING id, loja_id, numero_nota, fornecedor_cnpj
  `, [DATA_CORTE_ECO]);

  // Cenário D: TRANSITO_PERDIDO — notas em_transito ha >30 dias que nao chegaram ao ERP
  // Não aplica cutoff (problema é independente da data de importação)
  const updatedD = await dbQuery(`
    UPDATE notas_entrada
       SET auditoria_eco_status = 'transito_perdido',
           auditoria_eco_em = NOW()
     WHERE status IN ('em_transito','aguardando_estoque','em_conferencia','em_estoque',
                      'em_validacao_cadastro','em_validacao_comercial','aguardando_auditoria',
                      'importada')
       AND origem IN ('nfe','cd','transferencia_loja')
       AND chegou_no_erp_em IS NULL
       AND data_emissao IS NOT NULL
       AND data_emissao < CURRENT_DATE - INTERVAL '30 days'
       AND auditoria_eco_status IS NULL
       AND COALESCE(mcp_status_cd, 'A') <> 'C'
     RETURNING id, loja_id, numero_nota, fornecedor_cnpj
  `);

  return {
    updated_finalizadas_eco: updatedA,
    inserted_finalizadas_eco: insertedB,
    updated_n_finalizadas_eco: updatedC,
    updated_transito_perdido: updatedD,
  };
}

// POST /detectar (admin) — dispara manual
router.post('/detectar', apenasAdmin, async (req, res) => {
  try {
    const r = await detectarStatusEco();
    res.json({
      ok: true,
      finalizadas_eco_atualizadas: r.updated_finalizadas_eco.length,
      finalizadas_eco_criadas:     r.inserted_finalizadas_eco.length,
      n_finalizadas_eco_marcadas:  r.updated_n_finalizadas_eco.length,
      transito_perdido_marcadas:   r.updated_transito_perdido.length,
      total: r.updated_finalizadas_eco.length + r.inserted_finalizadas_eco.length + r.updated_n_finalizadas_eco.length + r.updated_transito_perdido.length,
    });
  } catch (e) {
    console.error('[finalizadas_eco detectar]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// GET / — lista por categoria. ?categoria=finalizadas_eco|n_finalizadas_eco
router.get('/', apenasAdmin, async (req, res) => {
  try {
    const cat = req.query.categoria || 'finalizadas_eco';
    const filtro = req.query.filtro || 'pendentes';
    const lojaId = req.query.loja_id ? parseInt(req.query.loja_id) : null;
    const limit  = Math.min(parseInt(req.query.limit) || 100, 500);

    if (!['finalizadas_eco', 'n_finalizadas_eco', 'transito_perdido'].includes(cat)) {
      return res.status(400).json({ erro: 'categoria invalida' });
    }

    const params = [cat];
    let where = `n.auditoria_eco_status = $1`;
    if (filtro === 'pendentes')   where += ` AND n.justificativa_diretor IS NULL`;
    if (filtro === 'justificadas') where += ` AND n.justificativa_diretor IS NOT NULL`;
    if (lojaId) {
      params.push(lojaId);
      where += ` AND n.loja_id = $${params.length}`;
    }
    params.push(limit);

    const rows = await dbQuery(`
      SELECT n.id, n.numero_nota, n.serie, n.fornecedor_cnpj, n.fornecedor_nome,
             n.loja_id, l.nome AS loja_nome,
             n.origem, n.status, n.data_emissao, n.valor_total,
             n.mcp_status_cd, n.chegou_no_erp_em,
             n.auditoria_eco_status, n.auditoria_eco_em,
             n.finalizada_f_motivo,
             n.justificativa_diretor, n.justificada_em, n.justificada_por,
             n.importado_em, n.fechado_em
        FROM notas_entrada n
        LEFT JOIN lojas l ON l.id = n.loja_id
       WHERE ${where}
       ORDER BY n.auditoria_eco_em DESC NULLS LAST
       LIMIT $${params.length}
    `, params);
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// PUT /:id/justificar
router.put('/:id/justificar', apenasAdmin, async (req, res) => {
  try {
    const { justificativa } = req.body || {};
    if (!justificativa?.trim()) return res.status(400).json({ erro: 'justificativa obrigatoria' });
    const por = req.usuario.email || req.usuario.usuario || req.usuario.nome || `id:${req.usuario.id}`;
    await dbQuery(
      `UPDATE notas_entrada
         SET justificativa_diretor = $1, justificada_em = NOW(), justificada_por = $2
       WHERE id = $3 AND auditoria_eco_status IS NOT NULL`,
      [justificativa.trim(), por, parseInt(req.params.id)]
    );
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /resumo — contagem por (categoria, loja)
router.get('/resumo', apenasAdmin, async (req, res) => {
  try {
    const rows = await dbQuery(`
      SELECT n.auditoria_eco_status AS categoria,
             n.loja_id, l.nome AS loja_nome,
             COUNT(*)::int AS total,
             COUNT(*) FILTER (WHERE n.justificativa_diretor IS NULL)::int AS pendentes
        FROM notas_entrada n
        LEFT JOIN lojas l ON l.id = n.loja_id
       WHERE n.auditoria_eco_status IS NOT NULL
       GROUP BY n.auditoria_eco_status, n.loja_id, l.nome
       ORDER BY n.auditoria_eco_status, n.loja_id
    `);
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// Limpa "lixo pré-cutoff": remove notas criadas pelo detector antes do cutoff
// e desmarca auditoria_eco_status de notas pré-cutoff.
router.post('/limpar-pre-cutoff', apenasAdmin, async (req, res) => {
  try {
    // Deleta QUALQUER nota com status='finalizada_f' que chegou ao ERP antes do cutoff
    // (todas vieram do detector legado — chegou_no_erp_em < cutoff é prova de lixo)
    const deletadas = await dbQuery(`
      DELETE FROM notas_entrada
       WHERE status = 'finalizada_f'
         AND chegou_no_erp_em IS NOT NULL
         AND chegou_no_erp_em < $1::date
       RETURNING id, numero_nota, loja_id`, [DATA_CORTE_ECO]);

    // Notas finalizada_f sem chegou_no_erp_em (criação antiga, nem caiu no ERP) também são lixo
    const deletadasNull = await dbQuery(`
      DELETE FROM notas_entrada
       WHERE status = 'finalizada_f'
         AND chegou_no_erp_em IS NULL
         AND COALESCE(finalizada_f_motivo,'') LIKE 'NUNCA IMPORTADA%'
       RETURNING id`);

    // UPDATE auditoria_eco_status NULL pra notas pré-cutoff que ainda estão em outros status
    const desmarcadas = await dbQuery(`
      UPDATE notas_entrada
         SET auditoria_eco_status = NULL,
             auditoria_eco_em = NULL
       WHERE auditoria_eco_status IS NOT NULL
         AND (chegou_no_erp_em IS NULL OR chegou_no_erp_em < $1::date)
       RETURNING id`, [DATA_CORTE_ECO]);

    res.json({
      ok: true,
      deletadas: deletadas.length + deletadasNull.length,
      desmarcadas: desmarcadas.length,
    });
  } catch (e) {
    console.error('[finalizadas_f limpar]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// Diagnóstico (token via QS)
router.get('/diagnosticar/:numero_nota', autenticarComQS, async (req, res) => {
  try {
    const num = String(req.params.numero_nota).trim();
    const numNorm = String(num).replace(/^0+/, '') || num;
    const noNotas = await dbQuery(
      `SELECT id, numero_nota, serie, fornecedor_cnpj, fornecedor_nome,
              loja_id, status, origem, data_emissao, valor_total,
              cd_mov_codi, cd_loja_cli_codi,
              importado_em, finalizada_f_em, finalizada_f_motivo,
              mcp_status_cd, chegou_no_erp_em,
              auditoria_eco_status, auditoria_eco_em,
              justificativa_diretor, justificada_por
         FROM notas_entrada
        WHERE REGEXP_REPLACE(COALESCE(numero_nota, ''), '^0+', '') = $1
           OR REGEXP_REPLACE(COALESCE(cd_mov_codi, ''), '^0+', '') = $1
        ORDER BY id DESC`, [numNorm]
    );
    const noCompras = await dbQuery(
      `SELECT loja_id, numeronfe, fornecedor_cnpj,
              MIN(data_emissao) AS data_emissao, MIN(data_entrada) AS data_entrada,
              COUNT(*)::int AS itens, SUM(qtd_comprada)::float AS qtd_total
         FROM compras_historico
        WHERE REGEXP_REPLACE(COALESCE(numeronfe,''),'^0+','') = $1
        GROUP BY loja_id, numeronfe, fornecedor_cnpj ORDER BY loja_id`, [numNorm]
    );
    const noCdMov = await dbQuery(
      `SELECT cd_codigo, mcp_codi, mcp_tipomov, mcp_status, mcp_dten, mcp_nnotafis, for_codi, mcp_vtot
         FROM cd_movcompra
        WHERE REGEXP_REPLACE(COALESCE(mcp_nnotafis,''),'^0+','') = $1
           OR REGEXP_REPLACE(COALESCE(mcp_codi,''),'^0+','') = $1
        ORDER BY mcp_dten DESC`, [numNorm]
    );
    res.json({
      numero_nota: num,
      em_notas_entrada: noNotas,
      em_compras_historico: noCompras,
      em_cd_movcompra: noCdMov,
    });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

router.detectarFinalizadasF = detectarStatusEco; // mantém nome do export pra compatibilidade do server.js
router.detectarStatusEco = detectarStatusEco;
module.exports = router;
