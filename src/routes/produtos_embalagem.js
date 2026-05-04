const express = require('express');
const router = express.Router();
const multer = require('multer');
const xlsx = require('xlsx');
const { pool, query: dbQuery } = require('../db');
const { autenticar, apenasAdmin } = require('../auth');
const { parseEmbalagem } = require('../parser_embalagem');
const ultrasyst = require('../ultrasyst');

// Normaliza EAN: trim, só dígitos, remove zeros à esquerda
function normalizarEan(s) {
  if (!s) return null;
  const so = String(s).replace(/\D/g, '').replace(/^0+/, '');
  return so || null;
}

function calcularEanStatus(eanCd, eanJr) {
  const a = normalizarEan(eanCd);
  const b = normalizarEan(eanJr);
  if (!a && !b) return 'sem_ambos';
  if (!a) return 'sem_ean_cd';
  if (!b) return 'sem_ean_jrlira';
  if (a === b) return 'ok';
  return 'divergente';
}

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 20 * 1024 * 1024 } });

// GET /api/produtos-embalagem?status=&q=&limit=&offset=
router.get('/', autenticar, async (req, res) => {
  try {
    const { status, q } = req.query;
    const limit  = Math.min(Math.max(parseInt(req.query.limit) || 200, 1), 2000);
    const offset = Math.max(parseInt(req.query.offset) || 0, 0);

    const where = ['1=1'];
    const params = [];
    if (status) { params.push(status); where.push(`status = $${params.length}`); }
    if (req.query.ean_status) {
      params.push(req.query.ean_status);
      where.push(`ean_status = $${params.length}`);
    }
    if (req.query.ativo === 'true') where.push(`ativo_no_cd = TRUE`);
    if (req.query.aponta_agregado === 'true') where.push(`ean_aponta_para IS NOT NULL`);
    if (req.query.duplicado === 'true') where.push(`ean_duplicado_count > 0`);
    if (req.query.sugestao_nfe === 'true') {
      where.push(`qtd_sugerida_nfe IS NOT NULL AND (qtd_embalagem IS NULL OR qtd_embalagem <> qtd_sugerida_nfe)`);
    }
    if (q) {
      params.push(`%${q}%`);
      where.push(`(mat_codi ILIKE $${params.length} OR descricao_atual ILIKE $${params.length} OR ean_principal_jrlira ILIKE $${params.length} OR ean_principal_cd ILIKE $${params.length})`);
    }
    const whereSql = where.join(' AND ');

    const total = await dbQuery(
      `SELECT COUNT(*)::int AS total FROM produtos_embalagem WHERE ${whereSql}`,
      params
    );
    const paramsList = [...params, limit, offset];
    const rows = await dbQuery(
      `SELECT mat_codi, descricao_atual, descricao_anterior, unidade,
              qtd_embalagem, qtd_sugerida, confianca_parser, status,
              ativo_no_cd, observacao, validado_em, validado_por,
              ean_principal_cd, ean_principal_jrlira, ean_status,
              ean_cd_synced_em, ean_validado_em, ean_validado_por,
              ean_aponta_para, ean_duplicado_count,
              qtd_sugerida_nfe, qtd_sugerida_nfe_fornecedor, qtd_sugerida_nfe_em,
              qtd_sugerida_nfe_nota_id, qtd_sugerida_nfe_confianca
         FROM produtos_embalagem
        WHERE ${whereSql}
        ORDER BY (qtd_sugerida_nfe IS NOT NULL AND (qtd_embalagem IS NULL OR qtd_embalagem <> qtd_sugerida_nfe)) DESC,
                 (status = 'pendente_validacao') DESC,
                 (status = 'divergente_descricao_mudou') DESC,
                 (status = 'divergente_qtd_planilha') DESC,
                 (ean_status = 'divergente') DESC,
                 (ean_status = 'sem_ean_cd') DESC,
                 mat_codi
        LIMIT $${paramsList.length - 1} OFFSET $${paramsList.length}`,
      paramsList
    );
    res.json({ total: total[0].total, limit, offset, rows });
  } catch (e) {
    console.error('[produtos-embalagem GET]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// GET /api/produtos-embalagem/stats
router.get('/stats', autenticar, async (req, res) => {
  try {
    const r = await dbQuery(`
      SELECT status, COUNT(*)::int AS qtd
        FROM produtos_embalagem
       GROUP BY status
       ORDER BY status
    `);
    const t = await dbQuery(`
      SELECT confianca_parser, COUNT(*)::int AS qtd
        FROM produtos_embalagem
       WHERE status <> 'validado'
       GROUP BY confianca_parser
    `);
    const sugNfe = await dbQuery(`
      SELECT COUNT(*)::int AS qtd
        FROM produtos_embalagem
       WHERE qtd_sugerida_nfe IS NOT NULL
         AND (qtd_embalagem IS NULL OR qtd_embalagem <> qtd_sugerida_nfe)
    `);
    res.json({ por_status: r, pendentes_por_confianca: t, sugestoes_nfe_pendentes: sugNfe[0]?.qtd || 0 });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

// POST /api/produtos-embalagem/import — Excel inicial
router.post('/import', apenasAdmin, upload.single('arquivo'), async (req, res) => {
  if (!req.file) return res.status(400).json({ erro: 'Arquivo ausente (campo "arquivo")' });

  let resumo = { total_excel: 0, inseridos: 0, atualizados: 0, ignorados: 0, divergentes: 0, validados_direto: 0 };

  try {
    const wb = xlsx.read(req.file.buffer, { type: 'buffer' });
    const ws = wb.Sheets[wb.SheetNames[0]];
    const linhas = xlsx.utils.sheet_to_json(ws);
    resumo.total_excel = linhas.length;

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      for (const lin of linhas) {
        const mat = String(lin.COD_PRODUTO || '').trim();
        if (!mat) { resumo.ignorados++; continue; }
        const desc = lin.DESCRICAO ? String(lin.DESCRICAO).trim() : null;
        const unidade = lin.UNIDADE ? String(lin.UNIDADE).trim() : null;
        const qtdExcel = lin.UNIDADE2 != null ? parseInt(lin.UNIDADE2) : null;

        const ativo = !!desc;
        const { qtd: sugerida, confianca } = parseEmbalagem(desc);

        // Regra: se Excel tem qtd e bate com sugestão → validado direto.
        // Se diverge → status divergente_qtd_planilha (user revisa).
        // Se sem qtd no Excel mas tem desc → pendente_validacao.
        // Se inativo (sem desc) e tem qtd → preserva como validado (histórico).
        let status, qtdFinal;
        if (!ativo) {
          status = 'validado';
          qtdFinal = qtdExcel;
        } else if (qtdExcel == null) {
          status = 'pendente_validacao';
          qtdFinal = null;
        } else if (sugerida === qtdExcel) {
          status = 'validado';
          qtdFinal = qtdExcel;
          resumo.validados_direto++;
        } else {
          status = 'divergente_qtd_planilha';
          qtdFinal = qtdExcel;
          resumo.divergentes++;
        }

        const r = await client.query(
          `INSERT INTO produtos_embalagem
              (mat_codi, descricao_atual, unidade, qtd_embalagem, qtd_sugerida,
               confianca_parser, status, ativo_no_cd,
               validado_em, validado_por, criado_em, atualizado_em)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,
                     CASE WHEN $7='validado' THEN NOW() ELSE NULL END,
                     CASE WHEN $7='validado' THEN $9 ELSE NULL END,
                     NOW(), NOW())
             ON CONFLICT (mat_codi) DO UPDATE SET
               descricao_atual = EXCLUDED.descricao_atual,
               unidade = EXCLUDED.unidade,
               qtd_embalagem = EXCLUDED.qtd_embalagem,
               qtd_sugerida = EXCLUDED.qtd_sugerida,
               confianca_parser = EXCLUDED.confianca_parser,
               status = EXCLUDED.status,
               ativo_no_cd = EXCLUDED.ativo_no_cd,
               atualizado_em = NOW()
             RETURNING (xmax = 0) AS inserido`,
          [mat, desc, unidade, qtdFinal, sugerida, confianca, status, ativo, 'import_excel']
        );
        if (r.rows[0].inserido) resumo.inseridos++; else resumo.atualizados++;
      }
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      throw e;
    } finally {
      client.release();
    }
    res.json({ ok: true, resumo });
  } catch (e) {
    console.error('[produtos-embalagem IMPORT]', e);
    res.status(500).json({ erro: e.message });
  }
});

// POST /api/produtos-embalagem/:mat/validar  body: { qtd_embalagem, observacao? }
router.post('/:mat/validar', autenticar, async (req, res) => {
  try {
    const { qtd_embalagem, observacao } = req.body || {};
    const qtd = parseInt(qtd_embalagem);
    if (!Number.isFinite(qtd) || qtd < 1) {
      return res.status(400).json({ erro: 'qtd_embalagem inválida' });
    }
    const r = await dbQuery(
      `UPDATE produtos_embalagem
          SET qtd_embalagem = $2, status = 'validado', observacao = $3,
              validado_em = NOW(), validado_por = $4, atualizado_em = NOW()
        WHERE mat_codi = $1
        RETURNING mat_codi, qtd_embalagem, status, validado_em`,
      [req.params.mat, qtd, observacao || null, req.usuario.nome || req.usuario.usuario]
    );
    if (!r.length) return res.status(404).json({ erro: 'Produto não encontrado' });
    res.json(r[0]);
  } catch (e) {
    console.error('[produtos-embalagem VALIDAR]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// POST /api/produtos-embalagem/:mat/aceitar-sugestao-nfe — promove sugestão NF-e a qtd validada
router.post('/:mat/aceitar-sugestao-nfe', autenticar, async (req, res) => {
  try {
    const r = await dbQuery(
      `UPDATE produtos_embalagem
          SET qtd_embalagem = qtd_sugerida_nfe,
              status = 'validado',
              validado_em = NOW(),
              validado_por = $2,
              observacao = COALESCE(observacao || ' | ', '')
                         || 'qtd NF-e ' || qtd_sugerida_nfe
                         || ' (' || COALESCE(qtd_sugerida_nfe_fornecedor,'?') || ')',
              qtd_sugerida_nfe = NULL,
              qtd_sugerida_nfe_fornecedor = NULL,
              qtd_sugerida_nfe_em = NULL,
              qtd_sugerida_nfe_nota_id = NULL,
              qtd_sugerida_nfe_confianca = NULL,
              atualizado_em = NOW()
        WHERE mat_codi = $1 AND qtd_sugerida_nfe IS NOT NULL
        RETURNING mat_codi, qtd_embalagem, status`,
      [req.params.mat, req.usuario.nome || req.usuario.usuario]
    );
    if (!r.length) return res.status(404).json({ erro: 'Sugestão NF-e não encontrada' });
    res.json(r[0]);
  } catch (e) {
    console.error('[aceitar-sugestao-nfe]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// POST /api/produtos-embalagem/:mat/ignorar-sugestao-nfe — remove sugestão sem alterar qtd
router.post('/:mat/ignorar-sugestao-nfe', autenticar, async (req, res) => {
  try {
    const r = await dbQuery(
      `UPDATE produtos_embalagem
          SET qtd_sugerida_nfe = NULL,
              qtd_sugerida_nfe_fornecedor = NULL,
              qtd_sugerida_nfe_em = NULL,
              qtd_sugerida_nfe_nota_id = NULL,
              qtd_sugerida_nfe_confianca = NULL,
              atualizado_em = NOW()
        WHERE mat_codi = $1 AND qtd_sugerida_nfe IS NOT NULL
        RETURNING mat_codi`,
      [req.params.mat]
    );
    if (!r.length) return res.status(404).json({ erro: 'Sugestão NF-e não encontrada' });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

// POST /api/produtos-embalagem/aceitar-sugestao-massa — aceita todas com confiança alta pendentes
router.post('/aceitar-sugestao-massa', apenasAdmin, async (req, res) => {
  try {
    const { confianca = 'alta' } = req.body || {};
    const r = await dbQuery(
      `UPDATE produtos_embalagem
          SET qtd_embalagem = qtd_sugerida,
              status = 'validado',
              validado_em = NOW(),
              validado_por = $1,
              atualizado_em = NOW()
        WHERE status IN ('pendente_validacao','divergente_descricao_mudou','divergente_qtd_planilha')
          AND confianca_parser = $2
          AND qtd_sugerida IS NOT NULL
        RETURNING mat_codi`,
      [req.usuario.nome || req.usuario.usuario, confianca]
    );
    res.json({ ok: true, atualizados: r.length });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

// POST /api/produtos-embalagem/import-eans — sobe planilha de-para EAN
router.post('/import-eans', apenasAdmin, upload.single('arquivo'), async (req, res) => {
  if (!req.file) return res.status(400).json({ erro: 'Arquivo ausente (campo "arquivo")' });

  const resumo = { total: 0, atualizados: 0, ignorados: 0, sem_ean_planilha: 0 };

  try {
    const wb = xlsx.read(req.file.buffer, { type: 'buffer' });
    const linhas = xlsx.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]]);
    resumo.total = linhas.length;

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      for (const lin of linhas) {
        const mat = String(lin.COD_PRODUTO || '').trim();
        if (!mat) { resumo.ignorados++; continue; }
        const ean = lin.ecocentauro ? String(lin.ecocentauro).trim() : null;
        if (!ean) { resumo.sem_ean_planilha++; continue; }

        const r = await client.query(
          `UPDATE produtos_embalagem
              SET ean_principal_jrlira = $2, atualizado_em = NOW()
            WHERE mat_codi = $1
            RETURNING mat_codi`,
          [mat, ean]
        );
        if (r.rows.length) resumo.atualizados++; else resumo.ignorados++;
      }
      // Recalcula ean_status pra todos
      await client.query(`
        UPDATE produtos_embalagem
           SET ean_status = CASE
             WHEN COALESCE(NULLIF(REGEXP_REPLACE(ean_principal_cd, '\\D', '', 'g'), ''), '') = ''
              AND COALESCE(NULLIF(REGEXP_REPLACE(ean_principal_jrlira, '\\D', '', 'g'), ''), '') = ''
                  THEN 'sem_ambos'
             WHEN COALESCE(NULLIF(REGEXP_REPLACE(ean_principal_cd, '\\D', '', 'g'), ''), '') = ''
                  THEN 'sem_ean_cd'
             WHEN COALESCE(NULLIF(REGEXP_REPLACE(ean_principal_jrlira, '\\D', '', 'g'), ''), '') = ''
                  THEN 'sem_ean_jrlira'
             WHEN LTRIM(COALESCE(ean_principal_cd, ''), '0') =
                  LTRIM(COALESCE(ean_principal_jrlira, ''), '0')
                  THEN 'ok'
             ELSE 'divergente'
           END
      `);
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      throw e;
    } finally {
      client.release();
    }
    res.json({ ok: true, resumo });
  } catch (e) {
    console.error('[import-eans]', e);
    res.status(500).json({ erro: e.message });
  }
});

// POST /api/produtos-embalagem/sync-eans-cd — busca EAN principal de cada MAT_CODI no UltraSyst
router.post('/sync-eans-cd', apenasAdmin, async (req, res) => {
  try {
    const t0 = Date.now();
    const mats = await dbQuery(`SELECT mat_codi FROM produtos_embalagem ORDER BY mat_codi`);
    const matCodis = mats.map(m => m.mat_codi);
    if (!matCodis.length) return res.json({ ok: true, total: 0 });

    const eansPorMat = {};
    // Lotes de 500 pra não estourar query
    for (let i = 0; i < matCodis.length; i += 500) {
      const lote = matCodis.slice(i, i + 500);
      const lista = lote.map(c => `'${c}'`).join(',');
      // 1) tenta tabela EAN com EAN_NOTA='S' (multi-EAN)
      const r1 = await ultrasyst.query(
        `SELECT MAT_CODI, LTRIM(RTRIM(EAN_CODI)) AS ean
           FROM EAN WITH (NOLOCK)
          WHERE EAN_NOTA='S' AND MAT_CODI IN (${lista})`
      );
      for (const row of r1.rows || []) eansPorMat[row.MAT_CODI] = row.ean;
      // 2) fallback MATERIAL.EAN_CODI pros que não acharam
      const semEan = lote.filter(m => !eansPorMat[m]);
      if (semEan.length) {
        const lista2 = semEan.map(c => `'${c}'`).join(',');
        const r2 = await ultrasyst.query(
          `SELECT MAT_CODI, LTRIM(RTRIM(EAN_CODI)) AS ean
             FROM MATERIAL WITH (NOLOCK)
            WHERE MAT_CODI IN (${lista2}) AND EAN_CODI IS NOT NULL`
        );
        for (const row of r2.rows || []) {
          if (row.ean) eansPorMat[row.MAT_CODI] = row.ean;
        }
      }
    }

    // Bulk update
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const ids = Object.keys(eansPorMat);
      const eans = ids.map(i => eansPorMat[i]);
      await client.query(
        `UPDATE produtos_embalagem n
            SET ean_principal_cd = u.ean,
                ean_cd_synced_em = NOW(),
                atualizado_em = NOW()
           FROM (SELECT * FROM UNNEST($1::text[], $2::text[]) AS t(mat_codi, ean)) u
          WHERE n.mat_codi = u.mat_codi`,
        [ids, eans]
      );
      await client.query(`
        UPDATE produtos_embalagem
           SET ean_status = CASE
             WHEN COALESCE(NULLIF(REGEXP_REPLACE(ean_principal_cd, '\\D', '', 'g'), ''), '') = ''
              AND COALESCE(NULLIF(REGEXP_REPLACE(ean_principal_jrlira, '\\D', '', 'g'), ''), '') = ''
                  THEN 'sem_ambos'
             WHEN COALESCE(NULLIF(REGEXP_REPLACE(ean_principal_cd, '\\D', '', 'g'), ''), '') = ''
                  THEN 'sem_ean_cd'
             WHEN COALESCE(NULLIF(REGEXP_REPLACE(ean_principal_jrlira, '\\D', '', 'g'), ''), '') = ''
                  THEN 'sem_ean_jrlira'
             WHEN LTRIM(COALESCE(ean_principal_cd, ''), '0') =
                  LTRIM(COALESCE(ean_principal_jrlira, ''), '0')
                  THEN 'ok'
             ELSE 'divergente'
           END
      `);
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      throw e;
    } finally { client.release(); }

    res.json({ ok: true, atualizados: Object.keys(eansPorMat).length, ms: Date.now() - t0 });
  } catch (e) {
    console.error('[sync-eans-cd]', e);
    res.status(500).json({ erro: e.message });
  }
});

// POST /api/produtos-embalagem/:mat/validar-ean   body: { ean_principal_jrlira }
router.post('/:mat/validar-ean', autenticar, async (req, res) => {
  try {
    const { ean_principal_jrlira } = req.body || {};
    const ean = ean_principal_jrlira ? String(ean_principal_jrlira).trim() : null;
    const r = await dbQuery(
      `UPDATE produtos_embalagem
          SET ean_principal_jrlira = $2::text,
              ean_status = CASE
                WHEN COALESCE(NULLIF(REGEXP_REPLACE(ean_principal_cd, '\\D', '', 'g'), ''), '') = ''
                 AND COALESCE(NULLIF(REGEXP_REPLACE($2::text, '\\D', '', 'g'), ''), '') = ''
                     THEN 'sem_ambos'
                WHEN COALESCE(NULLIF(REGEXP_REPLACE(ean_principal_cd, '\\D', '', 'g'), ''), '') = ''
                     THEN 'sem_ean_cd'
                WHEN COALESCE(NULLIF(REGEXP_REPLACE($2::text, '\\D', '', 'g'), ''), '') = ''
                     THEN 'sem_ean_jrlira'
                WHEN LTRIM(COALESCE(ean_principal_cd, ''), '0') =
                     LTRIM(COALESCE($2::text, ''), '0')
                     THEN 'ok'
                ELSE 'divergente'
              END,
              ean_validado_em = NOW(),
              ean_validado_por = $3,
              atualizado_em = NOW()
        WHERE mat_codi = $1
        RETURNING mat_codi, ean_principal_jrlira, ean_status`,
      [req.params.mat, ean, req.usuario.nome || req.usuario.usuario]
    );
    if (!r.length) return res.status(404).json({ erro: 'Produto não encontrado' });

    // Recalcula meta deste item: aponta_para + duplicado_count
    const meta = await dbQuery(
      `WITH apontaPara AS (
         SELECT (SELECT ext.produtoprincipal
                   FROM produtos_externo ext
                  WHERE ext.codigobarra = $2::text
                    AND ext.produtoprincipal IS NOT NULL
                    AND ext.produtoprincipal <> ext.codigobarra
                  LIMIT 1) AS principal
       ),
       dup AS (
         SELECT COUNT(*)::int - 1 AS cnt
           FROM produtos_embalagem
          WHERE ean_principal_jrlira = $2::text
       ),
       outros AS (
         SELECT mat_codi, descricao_atual
           FROM produtos_embalagem
          WHERE ean_principal_jrlira = $2::text
            AND mat_codi <> $1
       )
       UPDATE produtos_embalagem pe
          SET ean_aponta_para = ap.principal,
              ean_duplicado_count = GREATEST(d.cnt, 0)
         FROM apontaPara ap, dup d
        WHERE pe.mat_codi = $1
       RETURNING pe.ean_aponta_para, pe.ean_duplicado_count,
                 (SELECT json_agg(json_build_object('mat',mat_codi,'desc',COALESCE(descricao_atual,''))) FROM outros) AS outros_mat`,
      [req.params.mat, ean || '']
    );

    // Atualiza duplicado_count também nos OUTROS produtos com mesmo EAN
    if (ean) {
      await dbQuery(`
        UPDATE produtos_embalagem pe
           SET ean_duplicado_count = sub.cnt
          FROM (
            SELECT ean_principal_jrlira, COUNT(*)::int - 1 AS cnt
              FROM produtos_embalagem
             WHERE ean_principal_jrlira = $1::text
             GROUP BY ean_principal_jrlira
          ) sub
         WHERE pe.ean_principal_jrlira = sub.ean_principal_jrlira
      `, [ean]);
    }

    res.json({ ...r[0], meta: meta[0] });
  } catch (e) {
    console.error('[validar-ean]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// Normaliza descrição pra busca: remove números seguidos de X, lowercase, sem acentos
function normalizarDescricao(s) {
  if (!s) return '';
  return String(s)
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .toUpperCase()
    .replace(/\d+\s*[Xx]\s*\d+\s*(ML|L|G|KG|MG|UN|UND)?/g, '')  // tira "12X18G"
    .replace(/\b\d+\s*[Xx]\s*\d+\b/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

// GET /api/produtos-embalagem/:mat/sugestoes-ean
router.get('/:mat/sugestoes-ean', autenticar, async (req, res) => {
  try {
    const r = await dbQuery(
      `SELECT mat_codi, descricao_atual, ean_principal_jrlira, ean_principal_cd
         FROM produtos_embalagem WHERE mat_codi = $1`,
      [req.params.mat]
    );
    if (!r.length) return res.status(404).json({ erro: 'Produto não encontrado' });
    const prod = r[0];
    const descNorm = normalizarDescricao(prod.descricao_atual);
    if (!descNorm) return res.json({ produto: prod, descricao_normalizada: '', candidatos: [] });

    // Top candidatos por similaridade. SOMENTE EANs principais (codigo_interno = produtoprincipal).
    // EANs agregados são excluídos pra evitar inconsistência no estoque da loja.
    const candidatos = await dbQuery(
      `SELECT codigobarra,
              MIN(descricao) AS descricao,
              MAX(similarity(descricao, $1)) AS score,
              ARRAY_AGG(DISTINCT loja_id ORDER BY loja_id) AS lojas,
              MAX(qtdeembalagem) AS qtd_embalagem_eco
         FROM produtos_externo
        WHERE similarity(descricao, $1) > 0.25
          AND codigobarra IS NOT NULL AND codigobarra <> ''
          AND codigo_interno = produtoprincipal
        GROUP BY codigobarra
        ORDER BY score DESC
        LIMIT 10`,
      [descNorm]
    );

    // Pra cada candidato, conta MAT_CODIs do CD que já usam esse EAN (alerta N:1)
    const eans = candidatos.map(c => c.codigobarra);
    let usoAtualPorEan = {};
    if (eans.length) {
      const uso = await dbQuery(
        `SELECT ean_principal_jrlira, ARRAY_AGG(json_build_object('mat',mat_codi,'desc',COALESCE(descricao_atual,''))) AS produtos
           FROM produtos_embalagem
          WHERE ean_principal_jrlira = ANY($1::text[])
            AND mat_codi <> $2
          GROUP BY ean_principal_jrlira`,
        [eans, prod.mat_codi]
      );
      for (const u of uso) usoAtualPorEan[u.ean_principal_jrlira] = u.produtos;
    }

    res.json({
      produto: prod,
      descricao_normalizada: descNorm,
      candidatos: candidatos.map(c => ({
        ...c,
        score: Number(c.score).toFixed(3),
        ja_usado_em: usoAtualPorEan[c.codigobarra] || []
      }))
    });
  } catch (e) {
    console.error('[sugestoes-ean]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// POST /api/produtos-embalagem/sugerir-massa — aceita auto sugestão score >= threshold pros sem EAN
router.post('/sugerir-massa', apenasAdmin, async (req, res) => {
  try {
    const threshold = parseFloat(req.body?.threshold || 0.85);
    const ativos = await dbQuery(
      `SELECT mat_codi, descricao_atual
         FROM produtos_embalagem
        WHERE ativo_no_cd = TRUE
          AND ean_status IN ('sem_ean_jrlira','divergente','sem_ean_cd','sem_ambos')
          AND descricao_atual IS NOT NULL`
    );
    let aplicados = 0, ignorados = 0;
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      for (const p of ativos) {
        const descNorm = normalizarDescricao(p.descricao_atual);
        if (!descNorm) { ignorados++; continue; }
        const r = await client.query(
          `SELECT codigobarra, MAX(similarity(descricao, $1)) AS score
             FROM produtos_externo
            WHERE similarity(descricao, $1) >= $2
              AND codigobarra IS NOT NULL AND codigobarra <> ''
            GROUP BY codigobarra
            ORDER BY score DESC
            LIMIT 1`,
          [descNorm, threshold]
        );
        if (!r.rows.length) { ignorados++; continue; }
        const top = r.rows[0];
        await client.query(
          `UPDATE produtos_embalagem
              SET ean_sugerido_eco = $2,
                  ean_sugerido_score = $3,
                  ean_sugerido_em = NOW(),
                  atualizado_em = NOW()
            WHERE mat_codi = $1`,
          [p.mat_codi, top.codigobarra, top.score]
        );
        aplicados++;
      }
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      throw e;
    } finally { client.release(); }
    res.json({ ok: true, aplicados, ignorados, threshold });
  } catch (e) {
    console.error('[sugerir-massa]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// POST /api/produtos-embalagem/sync-status-ativo-cd
// Atualiza produtos_embalagem.ativo_no_cd baseado em MATERIAL.MAT_SITU do UltraSyst.
// Se um produto era inativo e fica ativo, status volta pra pendente_validacao
// (precisa nova revisão por você).
router.post('/sync-status-ativo-cd', apenasAdmin, async (req, res) => {
  try {
    const t0 = Date.now();
    const mats = await dbQuery(`SELECT mat_codi, ativo_no_cd FROM produtos_embalagem`);
    const matCodis = mats.map(m => m.mat_codi);
    const ativoAntes = Object.fromEntries(mats.map(m => [m.mat_codi, m.ativo_no_cd]));

    const ativoCD = {};
    for (let i = 0; i < matCodis.length; i += 500) {
      const lote = matCodis.slice(i, i + 500);
      const lista = lote.map(c => `'${c}'`).join(',');
      const r = await ultrasyst.query(
        `SELECT MAT_CODI, MAT_SITU FROM MATERIAL WITH (NOLOCK) WHERE MAT_CODI IN (${lista})`
      );
      for (const row of r.rows || []) ativoCD[row.MAT_CODI] = row.MAT_SITU === 'A';
    }

    const ids = Object.keys(ativoCD);
    const ativos = ids.map(i => ativoCD[i]);
    let mudancas = { ativados: 0, desativados: 0, sem_mudanca: 0 };

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      // Atualiza ativo_no_cd
      await client.query(
        `UPDATE produtos_embalagem n
            SET ativo_no_cd = u.ativo,
                atualizado_em = NOW()
           FROM (SELECT * FROM UNNEST($1::text[], $2::bool[]) AS t(mat_codi, ativo)) u
          WHERE n.mat_codi = u.mat_codi`,
        [ids, ativos]
      );
      // Detecta reativações: estava FALSE → virou TRUE → status volta pra pendente
      const reativados = ids.filter(id => !ativoAntes[id] && ativoCD[id]);
      if (reativados.length) {
        await client.query(
          `UPDATE produtos_embalagem
              SET status = 'pendente_validacao',
                  validado_em = NULL,
                  validado_por = NULL,
                  observacao = COALESCE(observacao || ' | ', '') || 'Reativado em ' || TO_CHAR(NOW(),'YYYY-MM-DD'),
                  atualizado_em = NOW()
            WHERE mat_codi = ANY($1::text[])`,
          [reativados]
        );
        mudancas.ativados = reativados.length;
      }
      mudancas.desativados = ids.filter(id => ativoAntes[id] && !ativoCD[id]).length;
      mudancas.sem_mudanca = ids.length - mudancas.ativados - mudancas.desativados;
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      throw e;
    } finally { client.release(); }

    res.json({ ok: true, total_consultados: ids.length, ...mudancas, ms: Date.now() - t0 });
  } catch (e) {
    console.error('[sync-status-ativo-cd]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// POST /api/produtos-embalagem/recalcular-meta-ean
//  - ean_aponta_para: se EAN_jrlira é agregado em produtos_externo, salva o principal
//  - ean_duplicado_count: quantos OUTROS MAT_CODIs do CD usam o mesmo ean_principal_jrlira
router.post('/recalcular-meta-ean', apenasAdmin, async (req, res) => {
  try {
    const t0 = Date.now();
    // 1) zera primeiro
    await dbQuery(`UPDATE produtos_embalagem SET ean_aponta_para = NULL WHERE ean_aponta_para IS NOT NULL`);
    // 2) ean_aponta_para — agregado em TODAS as lojas + busca o EAN do pai (codigo_interno = produtoprincipal)
    await dbQuery(`
      UPDATE produtos_embalagem pe
         SET ean_aponta_para = sub.ean_pai,
             atualizado_em = NOW()
        FROM (
          SELECT ext.codigobarra AS ean_agregado,
                 (SELECT MIN(ext_pai.codigobarra)
                    FROM produtos_externo ext_pai
                   WHERE ext_pai.codigo_interno = ext.produtoprincipal
                     AND ext_pai.loja_id = ext.loja_id) AS ean_pai
            FROM produtos_externo ext
           WHERE ext.codigo_interno IS NOT NULL
             AND ext.produtoprincipal IS NOT NULL
             AND ext.codigo_interno <> ext.produtoprincipal
           GROUP BY ext.codigobarra, ext.produtoprincipal, ext.loja_id
          HAVING NOT EXISTS (
            SELECT 1 FROM produtos_externo ext2
             WHERE ext2.codigobarra = ext.codigobarra
               AND ext2.codigo_interno = ext2.produtoprincipal
          )
        ) sub
       WHERE pe.ean_principal_jrlira = sub.ean_agregado
         AND sub.ean_pai IS NOT NULL
    `);
    // 3) ean_duplicado_count — quantos outros MAT_CODIs usam o mesmo EAN
    await dbQuery(`
      UPDATE produtos_embalagem pe
         SET ean_duplicado_count = COALESCE(d.cnt, 0)
        FROM (
          SELECT ean_principal_jrlira, COUNT(*)::int - 1 AS cnt
            FROM produtos_embalagem
           WHERE ean_principal_jrlira IS NOT NULL
             AND ean_principal_jrlira <> ''
           GROUP BY ean_principal_jrlira
        ) d
       WHERE pe.ean_principal_jrlira = d.ean_principal_jrlira
    `);
    await dbQuery(`UPDATE produtos_embalagem SET ean_duplicado_count = 0 WHERE ean_principal_jrlira IS NULL OR ean_principal_jrlira = ''`);

    const stats = await dbQuery(`
      SELECT
        COUNT(*) FILTER (WHERE ean_aponta_para IS NOT NULL)::int AS aponta_agregado,
        COUNT(*) FILTER (WHERE ean_duplicado_count > 0)::int AS com_duplicado,
        COUNT(*) FILTER (WHERE ean_duplicado_count > 0 AND ativo_no_cd)::int AS com_duplicado_ativos
        FROM produtos_embalagem
    `);
    res.json({ ok: true, stats: stats[0], ms: Date.now() - t0 });
  } catch (e) {
    console.error('[recalcular-meta-ean]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// GET /api/produtos-embalagem/relatorio-divergencias-cd
// Exporta XLSX com produtos onde o CD precisa ajustar (sem EAN, divergente, sugestões)
router.get('/relatorio-divergencias-cd', autenticar, async (req, res) => {
  try {
    const rows = await dbQuery(`
      SELECT
        pe.mat_codi,
        pe.descricao_atual,
        pe.ean_principal_cd,
        pe.ean_principal_jrlira,
        pe.ean_sugerido_eco,
        pe.ean_sugerido_score,
        pe.ean_status,
        CASE
          WHEN pe.ean_principal_cd IS NULL OR pe.ean_principal_cd = '' THEN 'CD sem EAN'
          WHEN pe.ean_principal_cd ~ '^0+$' THEN 'CD com placeholder zero'
          WHEN LTRIM(pe.ean_principal_cd, '0') = pe.mat_codi THEN 'CD usa MAT_CODI como EAN'
          WHEN pe.ean_principal_jrlira IS NOT NULL
               AND LTRIM(COALESCE(pe.ean_principal_cd,''), '0') <> LTRIM(COALESCE(pe.ean_principal_jrlira,''),'0')
            THEN 'EAN CD diferente do JR Lira'
          ELSE 'OK'
        END AS problema,
        COALESCE(pe.ean_principal_jrlira, pe.ean_sugerido_eco) AS ean_correto_sugerido
      FROM produtos_embalagem pe
      WHERE pe.ativo_no_cd = TRUE
        AND pe.ean_status IN ('sem_ean_cd','divergente','sem_ambos','sem_ean_jrlira')
      ORDER BY pe.ean_status, pe.mat_codi
    `);

    const xlsx = require('xlsx');
    const ws = xlsx.utils.json_to_sheet(rows.map(r => ({
      'MAT_CODI': r.mat_codi,
      'Descrição': (r.descricao_atual || '').trim(),
      'EAN no CD (atual)': r.ean_principal_cd || '',
      'EAN JR Lira (validado)': r.ean_principal_jrlira || '',
      'EAN Sugerido (Eco)': r.ean_sugerido_eco || '',
      'Score': r.ean_sugerido_score ? Number(r.ean_sugerido_score).toFixed(2) : '',
      'Problema': r.problema,
      'EAN correto pra ajustar no CD': r.ean_correto_sugerido || '',
      'Status EAN': r.ean_status,
    })));
    // Ajusta largura
    ws['!cols'] = [
      { wch: 12 }, { wch: 45 }, { wch: 18 }, { wch: 18 },
      { wch: 18 }, { wch: 7 }, { wch: 30 }, { wch: 22 }, { wch: 18 }
    ];
    const wb = xlsx.utils.book_new();
    xlsx.utils.book_append_sheet(wb, ws, 'Divergencias CD');
    const buf = xlsx.write(wb, { type: 'buffer', bookType: 'xlsx' });
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="divergencias_cd_${new Date().toISOString().slice(0,10)}.xlsx"`);
    res.send(buf);
  } catch (e) {
    console.error('[relatorio-divergencias-cd]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// GET /api/produtos-embalagem/stats-ean
router.get('/stats-ean', autenticar, async (req, res) => {
  try {
    const r = await dbQuery(`
      SELECT ean_status, COUNT(*)::int AS qtd
        FROM produtos_embalagem
        WHERE ativo_no_cd = TRUE
       GROUP BY ean_status
       ORDER BY ean_status
    `);
    res.json({ por_status: r });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

module.exports = router;
