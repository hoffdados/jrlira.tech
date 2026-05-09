const express = require('express');
const router = express.Router();
const { query: dbQuery, pool } = require('../db');
const { autenticar } = require('../auth');

// Recalcula preço/qtd_em_unidades dos itens de notas existentes que casam com este EAN+fornecedor.
// Aplica somente em itens onde un_comercial = un_tributavel (caso "tudo em CX").
// Não propaga pra notas já fechadas/validadas (estoque/auditoria já mexeram).
async function recalcularItensFornecedor(ean, fornCnpj, qtdPorCaixa, opts = {}) {
  if (!ean || !qtdPorCaixa || qtdPorCaixa < 2) return { recalculados: 0 };
  const excluirItemId = opts.excluirItemId || null;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const r = await client.query(
      `WITH alvo AS (
         SELECT i.id, i.qtd_comercial, i.preco_total_nota, i.custo_fabrica
           FROM itens_nota i
           JOIN notas_entrada n ON n.id = i.nota_id
          WHERE NULLIF(LTRIM(COALESCE(i.ean_validado, i.ean_nota,''),'0'),'')
                = NULLIF(LTRIM($1::text,'0'),'')
            AND ($2::text IS NULL
                 OR REGEXP_REPLACE(COALESCE(n.fornecedor_cnpj,''),'\\D','','g') = $2::text)
            AND COALESCE(UPPER(i.un_comercial),'') = COALESCE(UPPER(i.un_tributavel),'')
            AND i.qtd_comercial > 0
            AND n.status NOT IN ('fechada','validada')
            AND ($4::int IS NULL OR i.id <> $4::int)
       )
       UPDATE itens_nota i
          SET qtd_por_caixa_nfe = $3::int,
              qtd_em_unidades = a.qtd_comercial * $3::numeric,
              preco_unitario_nota = ROUND((a.preco_total_nota / (a.qtd_comercial * $3::numeric))::numeric, 4),
              preco_unitario_caixa = ROUND((a.preco_total_nota / a.qtd_comercial)::numeric, 4),
              status_preco = CASE
                WHEN a.custo_fabrica IS NULL THEN 'sem_cadastro'
                WHEN ABS((a.preco_total_nota / (a.qtd_comercial * $3::numeric)) - a.custo_fabrica) <= 0.01 THEN 'igual'
                WHEN a.custo_fabrica > 0
                  AND ABS((a.preco_total_nota / (a.qtd_comercial * $3::numeric)) - a.custo_fabrica) / a.custo_fabrica > 0.15
                  THEN 'auditagem'
                WHEN (a.preco_total_nota / (a.qtd_comercial * $3::numeric)) > a.custo_fabrica THEN 'maior'
                ELSE 'menor'
              END
         FROM alvo a
        WHERE i.id = a.id
        RETURNING i.id`,
      [ean, fornCnpj || null, qtdPorCaixa, excluirItemId]
    );
    await client.query('COMMIT');
    return { recalculados: r.rows.length };
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    throw e;
  } finally { client.release(); }
}

// GET /api/embalagens-fornecedor?status=&q=&fornecedor_cnpj=&limit=&offset=
router.get('/', autenticar, async (req, res) => {
  try {
    const { status, q, fornecedor_cnpj } = req.query;
    const limit = Math.min(Math.max(parseInt(req.query.limit) || 200, 1), 2000);
    const offset = Math.max(parseInt(req.query.offset) || 0, 0);
    const where = ['1=1'];
    const params = [];
    if (status) { params.push(status); where.push(`status = $${params.length}`); }
    if (fornecedor_cnpj) {
      params.push(String(fornecedor_cnpj).replace(/\D/g, ''));
      where.push(`REGEXP_REPLACE(COALESCE(fornecedor_cnpj,''),'\\D','','g') = $${params.length}`);
    }
    if (q) {
      params.push(`%${q}%`);
      where.push(`(descricao ILIKE $${params.length} OR ean ILIKE $${params.length} OR fornecedor_nome ILIKE $${params.length})`);
    }
    const whereSql = where.join(' AND ');

    const total = await dbQuery(`SELECT COUNT(*)::int AS total FROM embalagens_fornecedor WHERE ${whereSql}`, params);
    const rows = await dbQuery(
      `SELECT id, ean, descricao, fornecedor_cnpj, fornecedor_nome,
              qtd_por_caixa, qtd_sugerida_nfe, qtd_sugerida_nfe_em,
              qtd_sugerida_nfe_nota_id, qtd_sugerida_nfe_confianca,
              status, validado_em, validado_por, observacao,
              criado_em, atualizado_em
         FROM embalagens_fornecedor
        WHERE ${whereSql}
        ORDER BY (status='pendente_validacao') DESC,
                 atualizado_em DESC
        LIMIT $${params.length+1} OFFSET $${params.length+2}`,
      [...params, limit, offset]
    );
    res.json({ total: total[0].total, limit, offset, rows });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/embalagens-fornecedor/stats
router.get('/stats', autenticar, async (req, res) => {
  try {
    const r = await dbQuery(`
      SELECT status, COUNT(*)::int AS qtd
        FROM embalagens_fornecedor
       GROUP BY status
       ORDER BY status
    `);
    const fornecedores = await dbQuery(`
      SELECT fornecedor_nome, COUNT(*)::int AS qtd
        FROM embalagens_fornecedor
       WHERE status = 'pendente_validacao'
         AND fornecedor_nome IS NOT NULL
       GROUP BY fornecedor_nome
       ORDER BY qtd DESC
       LIMIT 10
    `);
    res.json({ por_status: r, top_fornecedores_pendentes: fornecedores });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/embalagens-fornecedor/:id/validar  body: { qtd_por_caixa, observacao? }
router.post('/:id/validar', autenticar, async (req, res) => {
  try {
    const { qtd_por_caixa, observacao } = req.body || {};
    const qtd = parseInt(qtd_por_caixa);
    if (!Number.isFinite(qtd) || qtd < 1) return res.status(400).json({ erro: 'qtd_por_caixa inválida' });
    const r = await dbQuery(
      `UPDATE embalagens_fornecedor
          SET qtd_por_caixa = $2,
              status = 'validado',
              validado_em = NOW(),
              validado_por = $3,
              observacao = $4,
              atualizado_em = NOW()
        WHERE id = $1
        RETURNING id, ean, fornecedor_cnpj, qtd_por_caixa, status`,
      [req.params.id, qtd, req.usuario.nome || req.usuario.usuario, observacao || null]
    );
    if (!r.length) return res.status(404).json({ erro: 'Não encontrado' });
    const ret = r[0];
    const recalc = qtd >= 2 ? await recalcularItensFornecedor(ret.ean, ret.fornecedor_cnpj, qtd) : { recalculados: 0 };
    res.json({ ...ret, ...recalc });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/embalagens-fornecedor/:id/aceitar-sugestao — promove qtd_sugerida_nfe a qtd_por_caixa
router.post('/:id/aceitar-sugestao', autenticar, async (req, res) => {
  try {
    const r = await dbQuery(
      `UPDATE embalagens_fornecedor
          SET qtd_por_caixa = qtd_sugerida_nfe,
              status = 'validado',
              validado_em = NOW(),
              validado_por = $2,
              atualizado_em = NOW()
        WHERE id = $1 AND qtd_sugerida_nfe IS NOT NULL
        RETURNING id, ean, fornecedor_cnpj, qtd_por_caixa, status`,
      [req.params.id, req.usuario.nome || req.usuario.usuario]
    );
    if (!r.length) return res.status(404).json({ erro: 'Sugestão NF-e não encontrada' });
    const ret = r[0];
    const recalc = ret.qtd_por_caixa >= 2 ? await recalcularItensFornecedor(ret.ean, ret.fornecedor_cnpj, ret.qtd_por_caixa) : { recalculados: 0 };
    res.json({ ...ret, ...recalc });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/embalagens-fornecedor/:id/ignorar
router.post('/:id/ignorar', autenticar, async (req, res) => {
  try {
    const r = await dbQuery(
      `UPDATE embalagens_fornecedor
          SET status = 'ignorado',
              atualizado_em = NOW(),
              validado_por = $2
        WHERE id = $1
        RETURNING id`,
      [req.params.id, req.usuario.nome || req.usuario.usuario]
    );
    if (!r.length) return res.status(404).json({ erro: 'Não encontrado' });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/embalagens-fornecedor — cadastra manualmente
// body: { ean, descricao?, fornecedor_cnpj?, fornecedor_nome?, qtd_por_caixa, observacao? }
router.post('/', autenticar, async (req, res) => {
  try {
    const { ean, descricao, fornecedor_cnpj, fornecedor_nome, qtd_por_caixa, observacao } = req.body || {};
    const eanNorm = String(ean || '').replace(/\D/g, '').replace(/^0+/, '');
    if (!eanNorm) return res.status(400).json({ erro: 'EAN obrigatório' });
    const qtd = parseInt(qtd_por_caixa);
    if (!Number.isFinite(qtd) || qtd < 1) return res.status(400).json({ erro: 'qtd_por_caixa inválida (>= 1)' });
    const cnpjNorm = fornecedor_cnpj ? String(fornecedor_cnpj).replace(/\D/g, '') : null;
    const r = await dbQuery(
      `INSERT INTO embalagens_fornecedor
         (ean, descricao, fornecedor_cnpj, fornecedor_nome, qtd_por_caixa,
          status, validado_em, validado_por, observacao)
       VALUES ($1::text, $2::text, $3::text, $4::text, $5::int,
               'validado', NOW(), $6::text, $7::text)
       ON CONFLICT (ean, COALESCE(fornecedor_cnpj, ''::character varying)) WHERE ean IS NOT NULL DO UPDATE
         SET qtd_por_caixa = EXCLUDED.qtd_por_caixa,
             descricao = COALESCE(EXCLUDED.descricao, embalagens_fornecedor.descricao),
             fornecedor_nome = COALESCE(EXCLUDED.fornecedor_nome, embalagens_fornecedor.fornecedor_nome),
             status = 'validado',
             validado_em = NOW(),
             validado_por = $6::text,
             observacao = COALESCE($7::text, embalagens_fornecedor.observacao),
             atualizado_em = NOW()
       RETURNING id, ean, fornecedor_cnpj, qtd_por_caixa, status`,
      [eanNorm, descricao || null, cnpjNorm, fornecedor_nome || null, qtd,
       req.usuario.nome || req.usuario.usuario, observacao || null]
    );
    const ret = r[0];
    const recalc = qtd >= 2 ? await recalcularItensFornecedor(ret.ean, ret.fornecedor_cnpj, qtd) : { recalculados: 0 };
    res.json({ ok: true, ...ret, ...recalc });
  } catch (e) {
    console.error('[embalagens-fornecedor POST]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// GET /api/embalagens-fornecedor/lookup?q=  — busca em itens_nota pra auto-completar cadastro
// (não exclui já cadastrados — só pra preencher descrição/fornecedor)
router.get('/lookup', autenticar, async (req, res) => {
  try {
    const q = String(req.query.q || '').trim();
    if (q.length < 4) return res.json([]);
    const eanNorm = q.replace(/\D/g, '').replace(/^0+/, '');
    const params = [eanNorm || null, `%${q}%`];
    const rows = await dbQuery(
      `SELECT NULLIF(LTRIM(COALESCE(i.ean_validado, i.ean_nota,''),'0'),'') AS ean,
              i.descricao_nota AS descricao,
              REGEXP_REPLACE(COALESCE(n.fornecedor_cnpj,''),'\\D','','g') AS fornecedor_cnpj,
              n.fornecedor_nome,
              n.data_emissao
         FROM itens_nota i
         JOIN notas_entrada n ON n.id = i.nota_id
        WHERE n.origem = 'nfe'
          AND (
            ($1::text IS NOT NULL AND NULLIF(LTRIM(COALESCE(i.ean_validado, i.ean_nota,''),'0'),'') = $1)
            OR i.descricao_nota ILIKE $2
          )
        ORDER BY n.data_emissao DESC NULLS LAST
        LIMIT 10`,
      params
    );
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/embalagens-fornecedor/produtos-sem-cadastro?fornecedor_cnpj=&q=
// Lista EANs de itens_nota que ainda não estão em embalagens_fornecedor (pra cadastro em massa)
router.get('/produtos-sem-cadastro', autenticar, async (req, res) => {
  try {
    const { fornecedor_cnpj, q } = req.query;
    const where = ['n.origem = \'nfe\''];
    const params = [];
    if (fornecedor_cnpj) {
      params.push(String(fornecedor_cnpj).replace(/\D/g, ''));
      where.push(`REGEXP_REPLACE(COALESCE(n.fornecedor_cnpj,''),'\\D','','g') = $${params.length}`);
    }
    if (q) {
      params.push(`%${q}%`);
      where.push(`(i.descricao_nota ILIKE $${params.length} OR i.ean_validado ILIKE $${params.length} OR i.ean_nota ILIKE $${params.length})`);
    }
    const rows = await dbQuery(
      `SELECT DISTINCT ON (NULLIF(LTRIM(COALESCE(i.ean_validado, i.ean_nota,''),'0'),''),
                          REGEXP_REPLACE(COALESCE(n.fornecedor_cnpj,''),'\\D','','g'))
              NULLIF(LTRIM(COALESCE(i.ean_validado, i.ean_nota,''),'0'),'') AS ean,
              MIN(i.descricao_nota) OVER (PARTITION BY NULLIF(LTRIM(COALESCE(i.ean_validado, i.ean_nota,''),'0'),''), REGEXP_REPLACE(COALESCE(n.fornecedor_cnpj,''),'\\D','','g')) AS descricao,
              REGEXP_REPLACE(COALESCE(n.fornecedor_cnpj,''),'\\D','','g') AS fornecedor_cnpj,
              n.fornecedor_nome,
              MAX(n.data_emissao) OVER (PARTITION BY NULLIF(LTRIM(COALESCE(i.ean_validado, i.ean_nota,''),'0'),''), REGEXP_REPLACE(COALESCE(n.fornecedor_cnpj,''),'\\D','','g')) AS ultima_compra
         FROM itens_nota i
         JOIN notas_entrada n ON n.id = i.nota_id
        WHERE ${where.join(' AND ')}
          AND NULLIF(LTRIM(COALESCE(i.ean_validado, i.ean_nota,''),'0'),'') IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM embalagens_fornecedor ef
             WHERE ef.ean = NULLIF(LTRIM(COALESCE(i.ean_validado, i.ean_nota,''),'0'),'')
               AND COALESCE(ef.fornecedor_cnpj,'') = COALESCE(REGEXP_REPLACE(COALESCE(n.fornecedor_cnpj,''),'\\D','','g'),'')
          )
        ORDER BY NULLIF(LTRIM(COALESCE(i.ean_validado, i.ean_nota,''),'0'),''),
                 REGEXP_REPLACE(COALESCE(n.fornecedor_cnpj,''),'\\D','','g'),
                 n.data_emissao DESC
        LIMIT 500`,
      params
    );
    res.json(rows);
  } catch (e) {
    console.error('[produtos-sem-cadastro]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// GET /api/embalagens-fornecedor/ranking — gamificação
router.get('/ranking', autenticar, async (req, res) => {
  try {
    const mes = await dbQuery(`
      SELECT validado_por, COUNT(*)::int AS qtd
        FROM embalagens_fornecedor
       WHERE status = 'validado'
         AND validado_por IS NOT NULL
         AND validado_em >= date_trunc('month', NOW())
       GROUP BY validado_por
       ORDER BY qtd DESC
       LIMIT 10
    `);
    const semana = await dbQuery(`
      SELECT validado_por, COUNT(*)::int AS qtd
        FROM embalagens_fornecedor
       WHERE status = 'validado'
         AND validado_por IS NOT NULL
         AND validado_em >= date_trunc('week', NOW())
       GROUP BY validado_por
       ORDER BY qtd DESC
       LIMIT 10
    `);
    const total = await dbQuery(`
      SELECT validado_por, COUNT(*)::int AS total,
             MAX(validado_em) AS ultima_em,
             COUNT(DISTINCT DATE(validado_em))::int AS dias_ativos
        FROM embalagens_fornecedor
       WHERE status = 'validado' AND validado_por IS NOT NULL
       GROUP BY validado_por
       ORDER BY total DESC
    `);
    // Streak: dias consecutivos com >=1 validação por usuário (até hoje)
    const streaks = await dbQuery(`
      WITH dias AS (
        SELECT validado_por, DATE(validado_em) AS dia
          FROM embalagens_fornecedor
         WHERE status = 'validado' AND validado_por IS NOT NULL
         GROUP BY validado_por, DATE(validado_em)
      ),
      grupos AS (
        SELECT validado_por, dia,
               dia - (ROW_NUMBER() OVER (PARTITION BY validado_por ORDER BY dia))::int AS grupo
          FROM dias
      ),
      seqs AS (
        SELECT validado_por, grupo, COUNT(*)::int AS tamanho, MAX(dia) AS fim
          FROM grupos
         GROUP BY validado_por, grupo
      )
      SELECT s.validado_por, s.tamanho AS streak_atual
        FROM seqs s
       WHERE s.fim >= CURRENT_DATE - INTERVAL '1 day'
       ORDER BY s.tamanho DESC
    `);
    const streakMap = Object.fromEntries(streaks.map(s => [s.validado_por, s.streak_atual]));
    function badgeOf(t) {
      if (t >= 2500) return { nivel: 'Lendário', emoji: '👑' };
      if (t >= 1000) return { nivel: 'Platinum',  emoji: '💎' };
      if (t >= 250)  return { nivel: 'Ouro',      emoji: '🥇' };
      if (t >= 100)  return { nivel: 'Prata',     emoji: '🥈' };
      if (t >= 25)   return { nivel: 'Bronze',    emoji: '🥉' };
      return         { nivel: 'Iniciante',        emoji: '🌱' };
    }
    const totalEnriched = total.map(t => ({
      ...t,
      streak_atual: streakMap[t.validado_por] || 0,
      ...badgeOf(t.total)
    }));
    res.json({ mes, semana, total: totalEnriched });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/embalagens-fornecedor/aceitar-sugestao-massa — aceita todas com confiança alta pendentes
router.post('/aceitar-sugestao-massa', autenticar, async (req, res) => {
  try {
    if (req.usuario.perfil !== 'admin') return res.status(403).json({ erro: 'Apenas admin' });
    const { confianca = 'alta' } = req.body || {};
    const r = await dbQuery(
      `UPDATE embalagens_fornecedor
          SET qtd_por_caixa = qtd_sugerida_nfe,
              status = 'validado',
              validado_em = NOW(),
              validado_por = $1,
              atualizado_em = NOW()
        WHERE status = 'pendente_validacao'
          AND qtd_sugerida_nfe IS NOT NULL
          AND qtd_sugerida_nfe_confianca = $2
        RETURNING id`,
      [req.usuario.nome || req.usuario.usuario, confianca]
    );
    res.json({ ok: true, atualizados: r.length });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/embalagens-fornecedor/backfill-descricao
// Percorre itens_nota com padrao NxM na descricao (ex: "6X2500ML") e gera
// sugestao em embalagens_fornecedor com confianca='descricao' pra produtos
// (ean+cnpj) ainda nao cadastrados. Nao toca em itens_nota.
// Body opcional: { fornecedor_cnpj, dryRun, limit }
router.post('/backfill-descricao', autenticar, async (req, res) => {
  try {
    if (req.usuario.perfil !== 'admin') return res.status(403).json({ erro: 'Apenas admin' });
    const { parseEmbalagem } = require('../parser_embalagem');
    const fornecedorCnpj = req.body?.fornecedor_cnpj || null;
    const dryRun = !!req.body?.dryRun;
    const limit = Math.min(parseInt(req.body?.limit) || 10000, 50000);

    const params = [];
    let whereCnpj = '';
    if (fornecedorCnpj) {
      params.push(String(fornecedorCnpj).replace(/\D/g,''));
      whereCnpj = `AND REGEXP_REPLACE(COALESCE(n.fornecedor_cnpj,''),'\\D','','g') = $${params.length}`;
    }
    params.push(limit);

    // Pega 1 item por (ean, cnpj) — mais recente — onde nao existe cadastro ainda
    const itens = await dbQuery(
      `SELECT DISTINCT ON (ean_norm, cnpj_norm)
              i.id, i.descricao_nota, i.nota_id,
              ean_norm AS ean, n.fornecedor_cnpj, n.fornecedor_nome, cnpj_norm
         FROM (
           SELECT i.*,
                  COALESCE(NULLIF(i.ean_validado,''), NULLIF(i.ean_nota,'')) AS ean_norm
             FROM itens_nota i
            WHERE COALESCE(NULLIF(i.ean_validado,''), NULLIF(i.ean_nota,'')) IS NOT NULL
              AND i.descricao_nota ~* '\\m\\d+\\s*[xX]\\s*\\d+\\s*(ML|L|G|GR|KG|MG|UN|UND|CM|MM)\\M'
         ) i
         JOIN notas_entrada n ON n.id = i.nota_id,
         LATERAL (SELECT REGEXP_REPLACE(COALESCE(n.fornecedor_cnpj,''),'\\D','','g') AS cnpj_norm) c
         WHERE n.origem = 'nfe'
           ${whereCnpj}
           AND NOT EXISTS (
             SELECT 1 FROM embalagens_fornecedor ef
              WHERE ef.ean = ean_norm
                AND COALESCE(ef.fornecedor_cnpj,'') = COALESCE(n.fornecedor_cnpj,'')
                AND ef.qtd_por_caixa IS NOT NULL
           )
         ORDER BY ean_norm, cnpj_norm, i.id DESC
         LIMIT $${params.length}`,
      params
    );

    let analisados = 0, sugeridos = 0, ignorados = 0;
    const exemplos = [];
    const client = await pool.connect();
    try {
      if (!dryRun) await client.query('BEGIN');
      for (const it of itens) {
        analisados++;
        const p = parseEmbalagem(it.descricao_nota || '');
        if (!p.qtd || p.qtd < 2 || p.confianca !== 'alta') { ignorados++; continue; }
        if (exemplos.length < 30) exemplos.push({ ean: it.ean, desc: it.descricao_nota, qtd: p.qtd, fornecedor_nome: it.fornecedor_nome });
        if (dryRun) { sugeridos++; continue; }
        await client.query(
          `INSERT INTO embalagens_fornecedor
             (ean, descricao, fornecedor_cnpj, fornecedor_nome,
              qtd_sugerida_nfe, qtd_sugerida_nfe_em, qtd_sugerida_nfe_nota_id,
              qtd_sugerida_nfe_confianca, status)
           VALUES ($1,$2,$3,$4,$5,NOW(),$6,'descricao','pendente_validacao')
           ON CONFLICT (ean, COALESCE(fornecedor_cnpj, ''::character varying)) WHERE ean IS NOT NULL DO UPDATE
             SET descricao = COALESCE(EXCLUDED.descricao, embalagens_fornecedor.descricao),
                 qtd_sugerida_nfe = EXCLUDED.qtd_sugerida_nfe,
                 qtd_sugerida_nfe_em = EXCLUDED.qtd_sugerida_nfe_em,
                 qtd_sugerida_nfe_nota_id = EXCLUDED.qtd_sugerida_nfe_nota_id,
                 qtd_sugerida_nfe_confianca = EXCLUDED.qtd_sugerida_nfe_confianca,
                 atualizado_em = NOW()`,
          [it.ean, it.descricao_nota, it.fornecedor_cnpj, it.fornecedor_nome, p.qtd, it.nota_id]
        );
        sugeridos++;
      }
      if (!dryRun) await client.query('COMMIT');
    } catch (e) {
      if (!dryRun) await client.query('ROLLBACK').catch(()=>{});
      throw e;
    } finally { client.release(); }
    res.json({ ok: true, dryRun, total_pares: itens.length, analisados, sugeridos, ignorados, exemplos });
  } catch (e) {
    console.error('[backfill-descricao]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

module.exports = router;
module.exports.recalcularItensFornecedor = recalcularItensFornecedor;
