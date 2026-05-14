// Trade Marketing — Receita Passiva por aluguel de pontos de exposicao.
// Modelo: comprador negocia ponto -> contrato com periodo+valor -> sistema gera
// uma cobranca por mes (competencia). Aparece em /contas-receber aba MKT.

const express = require('express');
const router = express.Router();
const multer = require('multer');
const { pool, query: dbQuery } = require('../db');
const { autenticar, exigirPerfil } = require('../auth');
const { enviarEmail } = require('../mailer');
const { enviarWhatsapp } = require('../whatsapp');

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 5 * 1024 * 1024 } });

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
      `SELECT p.id, p.loja_id, p.codigo, p.tipo, p.descricao, p.area_m2, p.observacao,
              p.ativo, p.criado_em, p.criado_por,
              p.foto_atualizada_em,
              (p.foto_data IS NOT NULL) AS tem_foto,
              l.nome AS loja_nome,
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

// ── HISTORICO E FOTO DO PONTO ──────────────────────────────────────

// GET /api/mkt/pontos/:id/historico — preco medio e ultimos contratos
router.get('/pontos/:id/historico', autenticar, async (req, res) => {
  try {
    const [stats] = await dbQuery(
      `SELECT COUNT(*)::int AS contratos_total,
              COALESCE(AVG(valor_mensal),0)::numeric(12,2) AS preco_medio,
              COALESCE(MAX(valor_mensal),0)::numeric(12,2) AS preco_max,
              COALESCE(MIN(valor_mensal),0)::numeric(12,2) AS preco_min,
              COALESCE(SUM(valor_mensal * GREATEST(1,
                (EXTRACT(YEAR FROM AGE(data_fim, data_inicio)) * 12 +
                 EXTRACT(MONTH FROM AGE(data_fim, data_inicio)) + 1)::int)),0)::numeric(14,2) AS receita_total_potencial
         FROM mkt_contratos
        WHERE ponto_id = $1`,
      [req.params.id]
    );
    const ultimos = await dbQuery(
      `SELECT id, fornecedor_nome, data_inicio, data_fim, valor_mensal, status
         FROM mkt_contratos
        WHERE ponto_id = $1
        ORDER BY data_inicio DESC
        LIMIT 10`,
      [req.params.id]
    );
    res.json({ stats, ultimos });
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// POST /api/mkt/pontos/:id/foto — upload (admin/ceo)
router.post('/pontos/:id/foto', ...adminOuCeo, upload.single('foto'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ erro: 'Foto obrigatoria' });
    const r = await dbQuery(
      `UPDATE pontos_exposicao
          SET foto_data = $1, foto_mime = $2, foto_atualizada_em = NOW()
        WHERE id = $3 RETURNING id`,
      [req.file.buffer, req.file.mimetype, req.params.id]
    );
    if (!r.length) return res.status(404).json({ erro: 'Nao encontrado' });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// GET /api/mkt/pontos/:id/foto — serve binary (sem autenticar: <img> usa tag direta)
router.get('/pontos/:id/foto', async (req, res) => {
  try {
    const r = await dbQuery('SELECT foto_data, foto_mime FROM pontos_exposicao WHERE id = $1', [req.params.id]);
    if (!r.length || !r[0].foto_data) return res.status(404).end();
    res.setHeader('Content-Type', r[0].foto_mime || 'image/jpeg');
    res.setHeader('Cache-Control', 'public, max-age=3600');
    res.end(r[0].foto_data);
  } catch (err) { res.status(500).end(); }
});

// ── JOBS AUTOMATIZADOS ─────────────────────────────────────────────

// 1) Renovacao automatica — contratos com renovacao_auto=true cujo data_fim < hoje
//    E ainda nao renovados (renovado_para_contrato_id IS NULL).
//    Cria novo contrato com mesma duracao (em meses), comecando dia seguinte ao fim.
async function rodarRenovacaoMkt() {
  const expirados = await dbQuery(
    `SELECT * FROM mkt_contratos
      WHERE renovacao_auto = TRUE
        AND status = 'ativo'
        AND data_fim < CURRENT_DATE
        AND renovado_para_contrato_id IS NULL`
  );
  let criados = 0;
  for (const c of expirados) {
    try {
      // Calcula duracao em meses (data_fim - data_inicio + 1)
      const ini = new Date(c.data_inicio); ini.setUTCHours(12, 0, 0, 0);
      const fim = new Date(c.data_fim); fim.setUTCHours(12, 0, 0, 0);
      const meses = (fim.getUTCFullYear() - ini.getUTCFullYear()) * 12
                  + (fim.getUTCMonth() - ini.getUTCMonth()) + 1;
      // Novo periodo: dia seguinte ao fim antigo, mesma duracao
      const novoIni = new Date(fim); novoIni.setUTCDate(novoIni.getUTCDate() + 1);
      const novoFim = new Date(novoIni);
      novoFim.setUTCMonth(novoFim.getUTCMonth() + meses);
      novoFim.setUTCDate(novoFim.getUTCDate() - 1);
      const niso = novoIni.toISOString().slice(0, 10);
      const fiso = novoFim.toISOString().slice(0, 10);

      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        const { rows: [novo] } = await client.query(
          `INSERT INTO mkt_contratos
             (ponto_id, loja_id, fornecedor_id, fornecedor_cnpj, fornecedor_nome,
              data_inicio, data_fim, valor_mensal, renovacao_auto, observacao,
              criado_por, renovado_de_contrato_id)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
           RETURNING id`,
          [c.ponto_id, c.loja_id, c.fornecedor_id, c.fornecedor_cnpj, c.fornecedor_nome,
           niso, fiso, c.valor_mensal, true,
           `Renovado automaticamente de #${c.id}. ${c.observacao || ''}`.trim(),
           'sistema-renovacao-auto', c.id]
        );
        // Cobrancas do novo contrato
        const comps = competenciasDoContrato(niso, fiso);
        for (const comp of comps) {
          const vencDt = new Date(comp + 'T12:00:00');
          vencDt.setMonth(vencDt.getMonth() + 1);
          vencDt.setDate(10);
          await client.query(
            `INSERT INTO mkt_cobrancas (contrato_id, competencia, valor, vencimento)
             VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`,
            [novo.id, comp, c.valor_mensal, vencDt.toISOString().slice(0, 10)]
          );
        }
        // Marca o contrato antigo como renovado
        await client.query(
          `UPDATE mkt_contratos SET renovado_para_contrato_id = $1, status = 'encerrado' WHERE id = $2`,
          [novo.id, c.id]
        );
        await client.query('COMMIT');
        criados++;
      } catch (e) {
        await client.query('ROLLBACK').catch(() => {});
        console.error('[mkt renovacao]', c.id, e.message);
      } finally { client.release(); }
    } catch (e) { console.error('[mkt renovacao]', e.message); }
  }
  if (criados > 0) console.log(`[mkt] ${criados} contrato(s) renovado(s) automaticamente`);
  return criados;
}

// 2) Alertas de vencimento — pra cada contrato vigente, 30d e 7d antes de vencer,
//    avisa compradores (rh_usuarios perfil=comprador, ativo, com email/telefone).
async function rodarAlertasVencimentoMkt() {
  const COMPRADORES = await dbQuery(
    `SELECT nome, email, telefone FROM rh_usuarios
      WHERE perfil = 'comprador' AND ativo = TRUE`
  ).catch(() => []);
  if (!COMPRADORES.length) return 0;

  // 30d
  const ate30 = await dbQuery(
    `SELECT c.*, p.codigo AS ponto_codigo, l.nome AS loja_nome
       FROM mkt_contratos c
       JOIN pontos_exposicao p ON p.id = c.ponto_id
       LEFT JOIN lojas l ON l.id = c.loja_id
      WHERE c.status = 'ativo'
        AND c.aviso_30d_enviado_em IS NULL
        AND c.data_fim BETWEEN CURRENT_DATE AND (CURRENT_DATE + INTERVAL '30 days')`
  );
  for (const c of ate30) {
    await notificarVencimentoMkt(c, 30, COMPRADORES);
    await dbQuery('UPDATE mkt_contratos SET aviso_30d_enviado_em = NOW() WHERE id = $1', [c.id]);
  }
  // 7d
  const ate7 = await dbQuery(
    `SELECT c.*, p.codigo AS ponto_codigo, l.nome AS loja_nome
       FROM mkt_contratos c
       JOIN pontos_exposicao p ON p.id = c.ponto_id
       LEFT JOIN lojas l ON l.id = c.loja_id
      WHERE c.status = 'ativo'
        AND c.aviso_7d_enviado_em IS NULL
        AND c.data_fim BETWEEN CURRENT_DATE AND (CURRENT_DATE + INTERVAL '7 days')`
  );
  for (const c of ate7) {
    await notificarVencimentoMkt(c, 7, COMPRADORES);
    await dbQuery('UPDATE mkt_contratos SET aviso_7d_enviado_em = NOW() WHERE id = $1', [c.id]);
  }
  const total = ate30.length + ate7.length;
  if (total > 0) console.log(`[mkt] ${total} alerta(s) de vencimento enviados`);
  return total;
}

async function notificarVencimentoMkt(c, dias, compradores) {
  const fim = new Date(c.data_fim).toLocaleDateString('pt-BR', { timeZone: 'UTC' });
  const valor = parseFloat(c.valor_mensal).toFixed(2);
  const titulo = `Contrato MKT vence em ${dias}d — ${c.fornecedor_nome}`;
  const corpoTxt =
    `Contrato de exposicao #${c.id}\n` +
    `Fornecedor: ${c.fornecedor_nome}\n` +
    `Loja: ${c.loja_nome || 'L' + c.loja_id} | Ponto: ${c.ponto_codigo}\n` +
    `Vencimento: ${fim} (${dias} dias)\n` +
    `Valor mensal: R$ ${valor}\n` +
    (c.renovacao_auto ? '\n[Renovacao automatica esta ATIVA — sera renovado sem acao]' : '\nNegocie renovacao ou nao renovar.');
  const corpoHtml =
    `<h3>${titulo}</h3>` +
    `<p><strong>Fornecedor:</strong> ${c.fornecedor_nome}<br>` +
    `<strong>Loja:</strong> ${c.loja_nome || 'L' + c.loja_id}<br>` +
    `<strong>Ponto:</strong> ${c.ponto_codigo}<br>` +
    `<strong>Vencimento:</strong> ${fim} (${dias} dias)<br>` +
    `<strong>Valor mensal:</strong> R$ ${valor}</p>` +
    `<p>${c.renovacao_auto ? '✅ <em>Renovacao automatica ATIVA — sera renovado sem acao.</em>' : '⚠ <em>Negocie renovacao ou deixe encerrar.</em>'}</p>`;
  for (const u of compradores) {
    if (u.email) enviarEmail(u.email, titulo, corpoHtml).catch(e => console.error('[mkt mail]', e.message));
    if (u.telefone) enviarWhatsapp(u.telefone, `*${titulo}*\n\n${corpoTxt}`).catch(e => console.error('[mkt wa]', e.message));
  }
}

// 3) Cobranca automatica pro fornecedor — quando cobranca atinge vencimento,
//    manda email pro fornecedor (se tem email cadastrado em fornecedores).
//    Reenvia a cada 7d de atraso (max 4 avisos = ~30d apos vencimento).
async function rodarCobrancaAutomaticaMkt() {
  // Cobranca: vencimento HOJE ou atrasada >= 7d desde ultimo aviso (ou nunca enviada)
  const cobs = await dbQuery(
    `SELECT co.*, c.fornecedor_nome, c.fornecedor_cnpj, c.fornecedor_id,
            p.codigo AS ponto_codigo, p.tipo AS ponto_tipo,
            l.nome AS loja_nome,
            f.email AS fornecedor_email, f.telefone AS fornecedor_telefone
       FROM mkt_cobrancas co
       JOIN mkt_contratos c ON c.id = co.contrato_id
       JOIN pontos_exposicao p ON p.id = c.ponto_id
       LEFT JOIN lojas l ON l.id = c.loja_id
       LEFT JOIN fornecedores f ON f.id = c.fornecedor_id
          OR REGEXP_REPLACE(COALESCE(f.cnpj,''),'\\D','','g') = REGEXP_REPLACE(COALESCE(c.fornecedor_cnpj,''),'\\D','','g')
      WHERE co.status IN ('aberta','parcial')
        AND co.vencimento <= CURRENT_DATE
        AND co.avisos_count < 5
        AND (co.atraso_aviso_em IS NULL OR co.atraso_aviso_em < NOW() - INTERVAL '7 days')`
  );
  let enviados = 0;
  for (const co of cobs) {
    try {
      if (!co.fornecedor_email && !co.fornecedor_telefone) continue;
      const venc = new Date(co.vencimento).toLocaleDateString('pt-BR', { timeZone: 'UTC' });
      const comp = new Date(co.competencia + 'T12:00:00').toLocaleDateString('pt-BR', { month: '2-digit', year: 'numeric', timeZone: 'UTC' });
      const valor = parseFloat(co.valor).toFixed(2);
      const dias = Math.max(0, Math.floor((Date.now() - new Date(co.vencimento + 'T12:00:00')) / 86400000));
      const atraso = dias > 0;
      const titulo = atraso
        ? `Cobranca em atraso (${dias}d) — Exposicao ${co.ponto_codigo} ${co.loja_nome || ''}`
        : `Cobranca de Exposicao — ${comp} (vence ${venc})`;
      const html =
        `<h3>${titulo}</h3>` +
        `<p>Prezado(a) <strong>${co.fornecedor_nome}</strong>,</p>` +
        `<p>Cobranca referente ao aluguel do ponto de exposicao da marca em nossa loja:</p>` +
        `<ul>` +
          `<li><strong>Loja:</strong> ${co.loja_nome || 'L' + co.loja_id}</li>` +
          `<li><strong>Ponto:</strong> ${co.ponto_codigo} (${co.ponto_tipo})</li>` +
          `<li><strong>Competencia:</strong> ${comp}</li>` +
          `<li><strong>Valor:</strong> R$ ${valor}</li>` +
          `<li><strong>Vencimento:</strong> ${venc}${atraso ? ` <span style="color:red"><strong>(em atraso ${dias} dias)</strong></span>` : ''}</li>` +
        `</ul>` +
        `<p>Forma de pagamento: abatimento em proxima NF, deposito ou bonificacao em mercadoria. Entre em contato com nosso comprador para alinhamento.</p>` +
        `<p style="color:#888;font-size:12px">SuperAsa - JR Lira Tech</p>`;
      if (co.fornecedor_email) await enviarEmail(co.fornecedor_email, titulo, html).catch(e => console.error('[mkt cob mail]', e.message));
      const txtWa =
        `*${titulo}*\n\n` +
        `Loja ${co.loja_nome || 'L' + co.loja_id} | Ponto ${co.ponto_codigo}\n` +
        `Competencia ${comp}\nValor R$ ${valor}\nVencimento ${venc}${atraso ? ` (atraso ${dias}d)` : ''}\n\n` +
        `Combinar pagamento com nosso comprador.`;
      if (co.fornecedor_telefone) await enviarWhatsapp(co.fornecedor_telefone, txtWa).catch(e => console.error('[mkt cob wa]', e.message));
      await dbQuery(
        `UPDATE mkt_cobrancas
            SET atraso_aviso_em = NOW(),
                cobranca_enviada_em = COALESCE(cobranca_enviada_em, NOW()),
                avisos_count = COALESCE(avisos_count,0) + 1
          WHERE id = $1`,
        [co.id]
      );
      enviados++;
    } catch (e) { console.error('[mkt cobrar]', co.id, e.message); }
  }
  if (enviados > 0) console.log(`[mkt] ${enviados} cobranca(s) enviadas a fornecedor`);
  return enviados;
}

// POST endpoints manuais (admin/ceo) — dispara o job sob demanda
router.post('/jobs/renovacao', ...adminOuCeo, async (req, res) => {
  try { res.json({ ok: true, renovados: await rodarRenovacaoMkt() }); }
  catch (e) { res.status(500).json({ erro: e.message }); }
});
router.post('/jobs/alertas-vencimento', ...adminOuCeo, async (req, res) => {
  try { res.json({ ok: true, enviados: await rodarAlertasVencimentoMkt() }); }
  catch (e) { res.status(500).json({ erro: e.message }); }
});
router.post('/jobs/cobranca-automatica', ...adminOuCeo, async (req, res) => {
  try { res.json({ ok: true, enviados: await rodarCobrancaAutomaticaMkt() }); }
  catch (e) { res.status(500).json({ erro: e.message }); }
});

module.exports = router;
module.exports.gerarCobrancasDoMes = gerarCobrancasDoMes;
module.exports.rodarRenovacaoMkt = rodarRenovacaoMkt;
module.exports.rodarAlertasVencimentoMkt = rodarAlertasVencimentoMkt;
module.exports.rodarCobrancaAutomaticaMkt = rodarCobrancaAutomaticaMkt;
