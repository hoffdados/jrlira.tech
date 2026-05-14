// Trade Marketing — Receita Passiva por aluguel de pontos de exposicao.
// Modelo: comprador negocia ponto -> contrato com periodo+valor -> sistema gera
// uma cobranca por mes (competencia). Aparece em /contas-receber aba MKT.

const express = require('express');
const router = express.Router();
const { pool, query: dbQuery } = require('../db');
const { autenticar, exigirPerfil } = require('../auth');

const podeNegociar = [autenticar, exigirPerfil('admin', 'comprador', 'ceo')];
const adminOuCeo = [autenticar, exigirPerfil('admin', 'ceo')];

// Adiciona N meses a uma data (mantem dia se possivel)
function addMeses(dataIso, n) {
  const d = new Date(dataIso + 'T12:00:00');
  const day = d.getDate();
  d.setDate(1);
  d.setMonth(d.getMonth() + n);
  const lastDay = new Date(d.getFullYear(), d.getMonth() + 1, 0).getDate();
  d.setDate(Math.min(day, lastDay));
  return d.toISOString().slice(0, 10);
}

// Lista de competencias (yyyy-mm-01) entre data_inicio e data_fim (inclusive)
function competenciasDoContrato(dataInicio, dataFim) {
  const out = [];
  const ini = new Date(dataInicio + 'T12:00:00');
  const fim = new Date(dataFim + 'T12:00:00');
  let y = ini.getFullYear(), m = ini.getMonth();
  while (y < fim.getFullYear() || (y === fim.getFullYear() && m <= fim.getMonth())) {
    out.push(`${y}-${String(m + 1).padStart(2, '0')}-01`);
    m++;
    if (m > 11) { m = 0; y++; }
  }
  return out;
}

// ── PONTOS DE EXPOSICAO ──────────────────────────────────────────────

// GET /api/mkt/pontos — lista (admin/ceo veem todos; outros so ativos)
router.get('/pontos', autenticar, async (req, res) => {
  try {
    const incluirInativos = req.query.todos === '1';
    const params = [];
    let where = '1=1';
    if (req.query.loja_id) { params.push(req.query.loja_id); where += ` AND p.loja_id = $${params.length}`; }
    if (!incluirInativos) where += ` AND p.ativo = TRUE`;
    const rows = await dbQuery(
      `SELECT p.*, l.nome AS loja_nome,
              (SELECT COUNT(*)::int FROM mkt_contratos c
                WHERE c.ponto_id = p.id AND c.status = 'ativo'
                  AND c.data_inicio <= CURRENT_DATE AND c.data_fim >= CURRENT_DATE) AS contratos_ativos
         FROM pontos_exposicao p
         LEFT JOIN lojas l ON l.id = p.loja_id
        WHERE ${where}
        ORDER BY p.loja_id, p.tipo, p.codigo`,
      params
    );
    res.json(rows);
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// POST /api/mkt/pontos (admin/ceo)
router.post('/pontos', ...adminOuCeo, async (req, res) => {
  try {
    const { loja_id, codigo, tipo, descricao, area_m2, observacao } = req.body || {};
    if (!loja_id || !codigo || !tipo) return res.status(400).json({ erro: 'loja_id, codigo e tipo obrigatorios' });
    const r = await dbQuery(
      `INSERT INTO pontos_exposicao (loja_id, codigo, tipo, descricao, area_m2, observacao, criado_por)
       VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
      [loja_id, codigo.trim().toUpperCase(), tipo, descricao || null,
       area_m2 ? parseFloat(area_m2) : null, observacao || null,
       req.usuario.nome || req.usuario.usuario]
    );
    res.json(r[0]);
  } catch (err) {
    if (err.code === '23505') return res.status(409).json({ erro: 'Codigo ja existe nesta loja' });
    res.status(500).json({ erro: err.message });
  }
});

// PATCH /api/mkt/pontos/:id (admin/ceo)
router.patch('/pontos/:id', ...adminOuCeo, async (req, res) => {
  try {
    const updates = [], params = [];
    for (const k of ['codigo', 'tipo', 'descricao', 'area_m2', 'observacao', 'ativo']) {
      if (req.body[k] === undefined) continue;
      let v = req.body[k];
      if (k === 'codigo' && v) v = String(v).trim().toUpperCase();
      if (k === 'area_m2' && v !== null && v !== '') v = parseFloat(v);
      params.push(v);
      updates.push(`${k} = $${params.length}`);
    }
    if (!updates.length) return res.status(400).json({ erro: 'Nada para atualizar' });
    params.push(req.params.id);
    const r = await dbQuery(
      `UPDATE pontos_exposicao SET ${updates.join(', ')} WHERE id = $${params.length} RETURNING *`,
      params
    );
    if (!r.length) return res.status(404).json({ erro: 'Nao encontrado' });
    res.json(r[0]);
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// ── CONTRATOS ──────────────────────────────────────────────────────────

// GET /api/mkt/contratos — lista com filtros loja/status/fornecedor/vigencia
router.get('/contratos', autenticar, async (req, res) => {
  try {
    const params = [];
    const where = ['1=1'];
    if (req.query.loja_id) { params.push(req.query.loja_id); where.push(`c.loja_id = $${params.length}`); }
    if (req.query.status) { params.push(req.query.status); where.push(`c.status = $${params.length}`); }
    if (req.query.fornecedor_cnpj) {
      params.push(req.query.fornecedor_cnpj.replace(/\D/g, ''));
      where.push(`REGEXP_REPLACE(COALESCE(c.fornecedor_cnpj,''),'\\D','','g') = $${params.length}`);
    }
    if (req.query.vigencia === 'ativos_hoje') {
      where.push(`c.data_inicio <= CURRENT_DATE AND c.data_fim >= CURRENT_DATE AND c.status = 'ativo'`);
    } else if (req.query.vigencia === 'futuros') {
      where.push(`c.data_inicio > CURRENT_DATE AND c.status = 'ativo'`);
    } else if (req.query.vigencia === 'encerrados') {
      where.push(`c.data_fim < CURRENT_DATE`);
    }
    if (req.query.q) {
      params.push(`%${req.query.q}%`);
      where.push(`(c.fornecedor_nome ILIKE $${params.length} OR p.codigo ILIKE $${params.length})`);
    }
    const rows = await dbQuery(
      `SELECT c.*, p.codigo AS ponto_codigo, p.tipo AS ponto_tipo, p.descricao AS ponto_descricao,
              l.nome AS loja_nome,
              (SELECT json_agg(json_build_object(
                  'id', co.id, 'competencia', co.competencia, 'valor', co.valor,
                  'vencimento', co.vencimento, 'status', co.status,
                  'pago_em', co.pago_em, 'pago_valor', co.pago_valor, 'pago_por', co.pago_por,
                  'forma_pagamento', co.forma_pagamento, 'observacao', co.observacao
                ) ORDER BY co.competencia)
                FROM mkt_cobrancas co WHERE co.contrato_id = c.id) AS cobrancas,
              (SELECT COUNT(*)::int FROM mkt_cobrancas co WHERE co.contrato_id = c.id) AS cobrancas_total,
              (SELECT COUNT(*)::int FROM mkt_cobrancas co WHERE co.contrato_id = c.id AND co.status = 'paga') AS cobrancas_pagas,
              (SELECT COALESCE(SUM(pago_valor),0)::numeric(12,2) FROM mkt_cobrancas co WHERE co.contrato_id = c.id AND co.status = 'paga') AS total_recebido,
              (SELECT COALESCE(SUM(valor),0)::numeric(12,2) FROM mkt_cobrancas co WHERE co.contrato_id = c.id AND co.status IN ('aberta','parcial')) AS total_aberto
         FROM mkt_contratos c
         JOIN pontos_exposicao p ON p.id = c.ponto_id
         LEFT JOIN lojas l ON l.id = c.loja_id
        WHERE ${where.join(' AND ')}
        ORDER BY c.data_inicio DESC, c.id DESC`,
      params
    );
    res.json(rows);
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// POST /api/mkt/contratos — cria contrato + gera cobrancas (admin/comprador/ceo)
router.post('/contratos', ...podeNegociar, async (req, res) => {
  const client = await pool.connect();
  try {
    const {
      ponto_id, fornecedor_id, fornecedor_cnpj, fornecedor_nome,
      data_inicio, data_fim, valor_mensal, renovacao_auto, observacao
    } = req.body || {};

    if (!ponto_id || !fornecedor_nome || !data_inicio || !data_fim || !valor_mensal) {
      return res.status(400).json({ erro: 'ponto_id, fornecedor_nome, data_inicio, data_fim e valor_mensal obrigatorios' });
    }
    if (String(data_fim) < String(data_inicio)) {
      return res.status(400).json({ erro: 'data_fim deve ser maior ou igual a data_inicio' });
    }

    await client.query('BEGIN');

    // Pega loja_id do ponto e checa conflito de periodo (1 contrato ativo por ponto/periodo)
    const { rows: [ponto] } = await client.query(
      'SELECT loja_id FROM pontos_exposicao WHERE id = $1 AND ativo = TRUE',
      [ponto_id]
    );
    if (!ponto) { await client.query('ROLLBACK'); return res.status(400).json({ erro: 'Ponto nao encontrado ou inativo' }); }

    const { rows: conflitos } = await client.query(
      `SELECT id, data_inicio, data_fim, fornecedor_nome
         FROM mkt_contratos
        WHERE ponto_id = $1 AND status = 'ativo'
          AND NOT (data_fim < $2::date OR data_inicio > $3::date)`,
      [ponto_id, data_inicio, data_fim]
    );
    if (conflitos.length) {
      await client.query('ROLLBACK');
      return res.status(409).json({
        erro: `Ponto ja ocupado no periodo: contrato #${conflitos[0].id} (${conflitos[0].fornecedor_nome}, ${conflitos[0].data_inicio} - ${conflitos[0].data_fim})`
      });
    }

    const { rows: [c] } = await client.query(
      `INSERT INTO mkt_contratos
         (ponto_id, loja_id, fornecedor_id, fornecedor_cnpj, fornecedor_nome,
          data_inicio, data_fim, valor_mensal, renovacao_auto, observacao, criado_por)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
       RETURNING *`,
      [ponto_id, ponto.loja_id,
       fornecedor_id || null,
       fornecedor_cnpj ? String(fornecedor_cnpj).replace(/\D/g, '') : null,
       String(fornecedor_nome).trim(),
       data_inicio, data_fim, parseFloat(valor_mensal),
       !!renovacao_auto, observacao || null,
       req.usuario.nome || req.usuario.usuario]
    );

    // Gera cobrancas (1 por competencia) com vencimento = dia 10 do mes seguinte
    const comps = competenciasDoContrato(data_inicio, data_fim);
    for (const comp of comps) {
      const venc = addMeses(comp, 1);
      const vencDt = new Date(venc + 'T12:00:00');
      vencDt.setDate(10);
      const vencStr = vencDt.toISOString().slice(0, 10);
      await client.query(
        `INSERT INTO mkt_cobrancas (contrato_id, competencia, valor, vencimento)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (contrato_id, competencia) DO NOTHING`,
        [c.id, comp, parseFloat(valor_mensal), vencStr]
      );
    }

    await client.query('COMMIT');
    res.json({ ok: true, contrato: c, cobrancas_geradas: comps.length });
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    res.status(500).json({ erro: err.message });
  } finally { client.release(); }
});

// PATCH /api/mkt/contratos/:id — cancelar/encerrar/observacao
router.patch('/contratos/:id', ...podeNegociar, async (req, res) => {
  try {
    const { status, motivo_cancelamento, observacao, renovacao_auto } = req.body || {};
    const updates = [], params = [];
    if (status !== undefined) {
      if (!['ativo', 'cancelado', 'encerrado'].includes(status)) return res.status(400).json({ erro: 'status invalido' });
      params.push(status); updates.push(`status = $${params.length}`);
      if (status === 'cancelado') {
        params.push(req.usuario.nome || req.usuario.usuario);
        updates.push(`cancelado_por = $${params.length}`);
        updates.push(`cancelado_em = NOW()`);
        params.push(motivo_cancelamento || null);
        updates.push(`motivo_cancelamento = $${params.length}`);
      }
    }
    if (observacao !== undefined) { params.push(observacao); updates.push(`observacao = $${params.length}`); }
    if (renovacao_auto !== undefined) { params.push(!!renovacao_auto); updates.push(`renovacao_auto = $${params.length}`); }
    if (!updates.length) return res.status(400).json({ erro: 'Nada para atualizar' });
    params.push(req.params.id);
    const r = await dbQuery(
      `UPDATE mkt_contratos SET ${updates.join(', ')} WHERE id = $${params.length} RETURNING *`,
      params
    );
    if (!r.length) return res.status(404).json({ erro: 'Nao encontrado' });
    // Se cancelou, marca cobrancas abertas como canceladas
    if (status === 'cancelado') {
      await dbQuery(
        `UPDATE mkt_cobrancas SET status = 'cancelada'
          WHERE contrato_id = $1 AND status IN ('aberta','parcial')`,
        [req.params.id]
      );
    }
    res.json(r[0]);
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// ── COBRANCAS ───────────────────────────────────────────────────────────

// GET /api/mkt/cobrancas — lista pra aba MKT em /contas-receber
router.get('/cobrancas', autenticar, async (req, res) => {
  try {
    const params = [];
    const where = ['1=1'];
    if (req.query.status) { params.push(req.query.status); where.push(`co.status = $${params.length}`); }
    if (req.query.loja_id) { params.push(req.query.loja_id); where.push(`c.loja_id = $${params.length}`); }
    if (req.query.competencia) {
      params.push(req.query.competencia);
      where.push(`co.competencia = $${params.length}::date`);
    }
    if (req.query.fornecedor_cnpj) {
      params.push(req.query.fornecedor_cnpj.replace(/\D/g, ''));
      where.push(`REGEXP_REPLACE(COALESCE(c.fornecedor_cnpj,''),'\\D','','g') = $${params.length}`);
    }
    if (req.query.q) {
      params.push(`%${req.query.q}%`);
      where.push(`(c.fornecedor_nome ILIKE $${params.length} OR p.codigo ILIKE $${params.length})`);
    }
    const rows = await dbQuery(
      `SELECT co.id, co.contrato_id, co.competencia, co.valor, co.vencimento, co.status,
              co.pago_em, co.pago_valor, co.pago_por, co.forma_pagamento, co.observacao,
              c.fornecedor_nome, c.fornecedor_cnpj, c.loja_id, c.data_inicio, c.data_fim,
              c.valor_mensal,
              p.codigo AS ponto_codigo, p.tipo AS ponto_tipo, p.descricao AS ponto_descricao,
              l.nome AS loja_nome,
              CASE
                WHEN co.status = 'aberta' AND co.vencimento < CURRENT_DATE THEN
                  (CURRENT_DATE - co.vencimento)::int
                ELSE 0
              END AS dias_atraso
         FROM mkt_cobrancas co
         JOIN mkt_contratos c ON c.id = co.contrato_id
         JOIN pontos_exposicao p ON p.id = c.ponto_id
         LEFT JOIN lojas l ON l.id = c.loja_id
        WHERE ${where.join(' AND ')}
        ORDER BY co.vencimento, co.competencia`,
      params
    );
    const totais = await dbQuery(
      `SELECT
         COALESCE(SUM(co.valor) FILTER (WHERE co.status = 'aberta'),0)::numeric(14,2) AS total_aberto,
         COALESCE(SUM(co.pago_valor) FILTER (WHERE co.status IN ('paga','parcial')),0)::numeric(14,2) AS total_recebido,
         COALESCE(SUM(co.valor) FILTER (WHERE co.status = 'aberta' AND co.vencimento < CURRENT_DATE),0)::numeric(14,2) AS total_atraso,
         COUNT(*)::int AS qtd
       FROM mkt_cobrancas co
       JOIN mkt_contratos c ON c.id = co.contrato_id
       WHERE ${where.join(' AND ')}`,
      params
    );
    res.json({ rows, ...totais[0] });
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// PATCH /api/mkt/cobrancas/:id/pagar — lanca pagamento
router.patch('/cobrancas/:id/pagar', ...podeNegociar, async (req, res) => {
  try {
    const { pago_valor, pago_em, forma_pagamento, observacao } = req.body || {};
    if (pago_valor == null) return res.status(400).json({ erro: 'pago_valor obrigatorio' });
    const { rows: [co] } = await pool.query('SELECT valor FROM mkt_cobrancas WHERE id = $1', [req.params.id]);
    if (!co) return res.status(404).json({ erro: 'Nao encontrada' });
    const v = parseFloat(pago_valor);
    const valor = parseFloat(co.valor);
    const novoStatus = Math.abs(v - valor) < 0.01 ? 'paga' : (v > 0 ? 'parcial' : 'aberta');
    const r = await dbQuery(
      `UPDATE mkt_cobrancas
          SET pago_valor = $1, pago_em = COALESCE($2::timestamptz, NOW()),
              forma_pagamento = $3, observacao = COALESCE($4, observacao),
              pago_por = $5, status = $6
        WHERE id = $7 RETURNING *`,
      [v, pago_em || null, forma_pagamento || null, observacao || null,
       req.usuario.nome || req.usuario.usuario, novoStatus, req.params.id]
    );
    res.json(r[0]);
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// POST /api/mkt/cobrancas/:id/desfazer-pagamento (admin/ceo)
router.post('/cobrancas/:id/desfazer-pagamento', ...adminOuCeo, async (req, res) => {
  try {
    const r = await dbQuery(
      `UPDATE mkt_cobrancas SET status = 'aberta', pago_em = NULL, pago_valor = NULL,
              pago_por = NULL, forma_pagamento = NULL
        WHERE id = $1 RETURNING *`,
      [req.params.id]
    );
    if (!r.length) return res.status(404).json({ erro: 'Nao encontrada' });
    res.json(r[0]);
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// ── INDICADORES ─────────────────────────────────────────────────────────

// GET /api/mkt/resumo — KPIs gerais
router.get('/resumo', autenticar, async (req, res) => {
  try {
    const [k] = await dbQuery(`
      WITH ano AS (SELECT DATE_TRUNC('year', CURRENT_DATE) AS y0)
      SELECT
        (SELECT COUNT(*)::int FROM pontos_exposicao WHERE ativo = TRUE) AS pontos_ativos,
        (SELECT COUNT(*)::int FROM pontos_exposicao p WHERE ativo = TRUE
            AND EXISTS (SELECT 1 FROM mkt_contratos c
                         WHERE c.ponto_id = p.id AND c.status = 'ativo'
                           AND c.data_inicio <= CURRENT_DATE AND c.data_fim >= CURRENT_DATE)) AS pontos_ocupados,
        (SELECT COUNT(*)::int FROM mkt_contratos WHERE status = 'ativo'
            AND data_inicio <= CURRENT_DATE AND data_fim >= CURRENT_DATE) AS contratos_vigentes,
        (SELECT COALESCE(SUM(valor),0)::numeric(14,2) FROM mkt_cobrancas WHERE status = 'aberta') AS total_aberto,
        (SELECT COALESCE(SUM(valor),0)::numeric(14,2) FROM mkt_cobrancas
            WHERE status = 'aberta' AND vencimento < CURRENT_DATE) AS total_atraso,
        (SELECT COALESCE(SUM(pago_valor),0)::numeric(14,2) FROM mkt_cobrancas
            WHERE status = 'paga' AND pago_em >= (SELECT y0 FROM ano)) AS recebido_ano,
        (SELECT COALESCE(SUM(valor_mensal),0)::numeric(14,2) FROM mkt_contratos
            WHERE status = 'ativo' AND data_inicio <= CURRENT_DATE AND data_fim >= CURRENT_DATE) AS receita_mensal_estimada
    `);
    res.json(k);
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// GET /api/mkt/vencendo — contratos vencendo nos proximos N dias (default 30)
router.get('/vencendo', autenticar, async (req, res) => {
  try {
    const dias = parseInt(req.query.dias) || 30;
    const rows = await dbQuery(
      `SELECT c.id, c.fornecedor_nome, c.fornecedor_cnpj, c.valor_mensal,
              c.data_inicio, c.data_fim,
              (c.data_fim - CURRENT_DATE)::int AS dias_para_vencer,
              p.codigo AS ponto_codigo, p.tipo AS ponto_tipo,
              l.nome AS loja_nome
         FROM mkt_contratos c
         JOIN pontos_exposicao p ON p.id = c.ponto_id
         LEFT JOIN lojas l ON l.id = c.loja_id
        WHERE c.status = 'ativo'
          AND c.data_fim BETWEEN CURRENT_DATE AND (CURRENT_DATE + ($1 || ' days')::interval)
        ORDER BY c.data_fim`,
      [dias]
    );
    res.json(rows);
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// POST /api/mkt/regenerar-cobrancas-mes — admin pode forcar geracao do mes corrente
// Usado pelo cron mensal e disponivel manualmente.
async function gerarCobrancasDoMes(competencia) {
  // competencia: 'yyyy-mm-01'. Pra cada contrato ativo vigente que ainda nao tem cobranca do mes, cria.
  const r = await dbQuery(
    `WITH alvo AS (
       SELECT c.id, c.valor_mensal,
              (DATE_TRUNC('month', $1::date) + INTERVAL '9 days')::date AS venc
         FROM mkt_contratos c
        WHERE c.status = 'ativo'
          AND c.data_inicio <= ($1::date + INTERVAL '1 month' - INTERVAL '1 day')::date
          AND c.data_fim   >= $1::date
          AND NOT EXISTS (
            SELECT 1 FROM mkt_cobrancas co
             WHERE co.contrato_id = c.id AND co.competencia = $1::date
          )
     )
     INSERT INTO mkt_cobrancas (contrato_id, competencia, valor, vencimento)
     SELECT id, $1::date, valor_mensal, venc FROM alvo
     RETURNING id`,
    [competencia]
  );
  return r.length;
}

router.post('/regenerar-cobrancas-mes', ...adminOuCeo, async (req, res) => {
  try {
    const hoje = new Date();
    const competencia = req.body?.competencia
      || `${hoje.getFullYear()}-${String(hoje.getMonth() + 1).padStart(2, '0')}-01`;
    const criadas = await gerarCobrancasDoMes(competencia);
    res.json({ ok: true, competencia, cobrancas_criadas: criadas });
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

module.exports = router;
module.exports.gerarCobrancasDoMes = gerarCobrancasDoMes;
