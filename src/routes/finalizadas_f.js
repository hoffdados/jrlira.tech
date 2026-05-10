// Notas Finalizadas_F: notas que saíram do CD, chegaram na loja (sync Pentaho confirmou),
// mas pularam nosso fluxo de cadastro/conferência/auditoria.
//
// Detector:
// - notas_entrada origem='cd'/'transferencia_loja'
// - status NOT IN (fechada/validada/arquivada/cancelada/finalizada_f)
// - existe match em compras_historico (loja_id + numeronfe + fornecedor_cnpj)
// → marca como 'finalizada_f', registra motivo, sai do trânsito
//
// Diretor (admin) justifica via /auditoria-finalizada-f.

const express = require('express');
const router = express.Router();
const { query: dbQuery } = require('../db');
const { autenticar, exigirPerfil, JWT_SECRET } = require('../auth');
const jwt = require('jsonwebtoken');

const apenasAdmin = [autenticar, exigirPerfil('admin')];

// Aceita token via Authorization header OU ?token= (pra abrir url no browser)
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

// Detecta e marca/cria notas como finalizada_f.
// 2 cenários:
//   A) Nota EXISTE em notas_entrada (origem=cd) mas pulou o fluxo → UPDATE pra finalizada_f
//   B) Nota NÃO EXISTE em notas_entrada (Pentaho recebeu mas XML nunca foi importado) → INSERT como finalizada_f
async function detectarFinalizadasF() {
  // Cenário A: UPDATE
  const updated = await dbQuery(`
    UPDATE notas_entrada
       SET status = 'finalizada_f',
           finalizada_f_em = NOW(),
           finalizada_f_motivo = 'detectada via compras_historico (chegou na loja sem passar pelo app)'
     WHERE id IN (
       SELECT DISTINCT n.id
         FROM notas_entrada n
         JOIN compras_historico c
           ON c.loja_id = n.loja_id
          AND c.numeronfe = n.numero_nota
          AND COALESCE(NULLIF(c.fornecedor_cnpj,''),'') =
              COALESCE(NULLIF(n.fornecedor_cnpj,''),'')
        WHERE n.origem IN ('cd','transferencia_loja')
          AND n.status NOT IN ('fechada','validada','arquivada','cancelada','finalizada_f')
     )
     RETURNING id, loja_id, numero_nota, fornecedor_cnpj, fornecedor_nome
  `);

  // Cenário B: INSERT — notas em compras_historico vindas de CDs cadastrados que nunca foram importadas
  // Usa CNPJ dos destinos pra saber quais CNPJs são "CD origem"
  const inserted = await dbQuery(`
    WITH cnpjs_cd AS (
      SELECT DISTINCT cnpj, nome FROM pedidos_distrib_destinos WHERE tipo = 'CD' AND cnpj IS NOT NULL
    ),
    candidatas AS (
      SELECT c.loja_id, c.numeronfe, c.fornecedor_cnpj,
             cd.nome AS fornecedor_nome,
             MIN(c.data_emissao) AS data_emissao,
             SUM(COALESCE(c.custo_total, 0)) AS valor_total
        FROM compras_historico c
        JOIN cnpjs_cd cd ON cd.cnpj = c.fornecedor_cnpj
       WHERE NOT EXISTS (
         SELECT 1 FROM notas_entrada n
          WHERE n.loja_id = c.loja_id
            AND n.numero_nota = c.numeronfe
            AND COALESCE(NULLIF(n.fornecedor_cnpj,''),'') =
                COALESCE(NULLIF(c.fornecedor_cnpj,''),'')
       )
       GROUP BY c.loja_id, c.numeronfe, c.fornecedor_cnpj, cd.nome
    )
    INSERT INTO notas_entrada (
      loja_id, numero_nota, fornecedor_cnpj, fornecedor_nome, data_emissao, valor_total,
      status, origem, finalizada_f_em, finalizada_f_motivo, importado_em
    )
    SELECT loja_id, numeronfe, fornecedor_cnpj, fornecedor_nome, data_emissao, valor_total,
           'finalizada_f', 'cd', NOW(),
           'NUNCA IMPORTADA — XML nao foi processado mas chegou na loja (compras_historico)',
           NOW()
      FROM candidatas
    RETURNING id, loja_id, numero_nota, fornecedor_cnpj, fornecedor_nome
  `);

  return { updated, inserted };
}

// Endpoint manual pra disparar detecção
router.post('/detectar', apenasAdmin, async (req, res) => {
  try {
    const r = await detectarFinalizadasF();
    res.json({
      ok: true,
      atualizadas: r.updated.length,
      criadas: r.inserted.length,
      total: r.updated.length + r.inserted.length,
      detalhes: { updated: r.updated, inserted: r.inserted },
    });
  } catch (e) {
    console.error('[finalizadas_f detectar]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// Lista notas finalizada_f (com filtro: pendentes de justificativa por padrão)
router.get('/', apenasAdmin, async (req, res) => {
  try {
    const filtro = req.query.filtro || 'pendentes'; // pendentes | justificadas | todas
    const lojaId = req.query.loja_id ? parseInt(req.query.loja_id) : null;
    const limit  = Math.min(parseInt(req.query.limit) || 100, 500);

    const params = [];
    let where = `n.status = 'finalizada_f'`;
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
             n.origem, n.data_emissao, n.valor_total,
             n.finalizada_f_em, n.finalizada_f_motivo,
             n.justificativa_diretor, n.justificada_em, n.justificada_por,
             (SELECT MIN(data_entrada) FROM compras_historico c
               WHERE c.loja_id = n.loja_id AND c.numeronfe = n.numero_nota
                 AND COALESCE(NULLIF(c.fornecedor_cnpj,''),'') =
                     COALESCE(NULLIF(n.fornecedor_cnpj,''),'')) AS data_entrada_loja
        FROM notas_entrada n
        LEFT JOIN lojas l ON l.id = n.loja_id
       WHERE ${where}
       ORDER BY n.finalizada_f_em DESC NULLS LAST
       LIMIT $${params.length}
    `, params);
    res.json(rows);
  } catch (e) {
    console.error('[finalizadas_f get]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// PUT /:id/justificar — Diretor justifica
router.put('/:id/justificar', apenasAdmin, async (req, res) => {
  try {
    const { justificativa } = req.body || {};
    if (!justificativa?.trim()) return res.status(400).json({ erro: 'justificativa obrigatoria' });
    const por = req.usuario.email || req.usuario.usuario || req.usuario.nome || `id:${req.usuario.id}`;
    await dbQuery(
      `UPDATE notas_entrada
         SET justificativa_diretor = $1, justificada_em = NOW(), justificada_por = $2
       WHERE id = $3 AND status = 'finalizada_f'`,
      [justificativa.trim(), por, parseInt(req.params.id)]
    );
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// Resumo: contagem por loja (pendentes vs justificadas)
router.get('/resumo', apenasAdmin, async (req, res) => {
  try {
    const rows = await dbQuery(`
      SELECT n.loja_id, l.nome AS loja_nome,
             COUNT(*)::int AS total,
             COUNT(*) FILTER (WHERE n.justificativa_diretor IS NULL)::int AS pendentes,
             COUNT(*) FILTER (WHERE n.justificativa_diretor IS NOT NULL)::int AS justificadas
        FROM notas_entrada n
        LEFT JOIN lojas l ON l.id = n.loja_id
       WHERE n.status = 'finalizada_f'
       GROUP BY n.loja_id, l.nome
       ORDER BY n.loja_id
    `);
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// Diagnóstico de uma nota: por que não foi marcada como finalizada_f?
router.get('/diagnosticar/:numero_nota', autenticarComQS, async (req, res) => {
  try {
    const num = String(req.params.numero_nota).trim();

    const noNotas = await dbQuery(
      `SELECT id, numero_nota, serie, fornecedor_cnpj, fornecedor_nome,
              loja_id, status, origem, data_emissao, valor_total,
              importado_em, finalizada_f_em
         FROM notas_entrada
        WHERE numero_nota = $1
        ORDER BY id DESC`, [num]
    );

    const noCompras = await dbQuery(
      `SELECT loja_id, numeronfe, fornecedor_cnpj,
              MIN(data_emissao) AS data_emissao, MIN(data_entrada) AS data_entrada,
              COUNT(*)::int AS itens, SUM(qtd_comprada)::float AS qtd_total
         FROM compras_historico
        WHERE numeronfe = $1
        GROUP BY loja_id, numeronfe, fornecedor_cnpj
        ORDER BY loja_id`, [num]
    );

    // Pra cada nota_entrada, simula o JOIN
    const matches = await dbQuery(
      `SELECT n.id AS nota_id, n.loja_id AS nota_loja, n.numero_nota, n.fornecedor_cnpj AS nota_cnpj, n.status, n.origem,
              c.loja_id AS compra_loja, c.fornecedor_cnpj AS compra_cnpj, c.data_entrada
         FROM notas_entrada n
         LEFT JOIN compras_historico c
           ON c.loja_id = n.loja_id
          AND c.numeronfe = n.numero_nota
          AND COALESCE(NULLIF(c.fornecedor_cnpj,''),'') =
              COALESCE(NULLIF(n.fornecedor_cnpj,''),'')
        WHERE n.numero_nota = $1
        ORDER BY n.id DESC`, [num]
    );

    // Critérios atuais
    const criterio = {
      origem_aceitas: ['cd','transferencia_loja'],
      status_excluidos: ['fechada','validada','arquivada','cancelada','finalizada_f'],
    };

    res.json({
      numero_nota: num,
      em_notas_entrada: noNotas,
      em_compras_historico: noCompras,
      tentativa_join: matches,
      criterio,
      diagnostico: noNotas.map(n => {
        const motivos = [];
        if (!criterio.origem_aceitas.includes(n.origem))
          motivos.push(`origem='${n.origem}' nao esta em (${criterio.origem_aceitas.join(',')})`);
        if (criterio.status_excluidos.includes(n.status))
          motivos.push(`status='${n.status}' esta na lista de exclusao`);
        const compraMatch = noCompras.find(c =>
          c.loja_id === n.loja_id &&
          (c.fornecedor_cnpj || '') === (n.fornecedor_cnpj || ''));
        if (!compraMatch)
          motivos.push(`sem match em compras_historico (loja_id=${n.loja_id}, fornecedor_cnpj=${n.fornecedor_cnpj || '(vazio)'})`);
        return {
          nota_id: n.id, loja_id: n.loja_id, status: n.status, origem: n.origem,
          fornecedor_cnpj: n.fornecedor_cnpj,
          ja_finalizada_f: !!n.finalizada_f_em,
          motivos_pra_nao_marcar: motivos.length ? motivos : ['DEVERIA ESTAR MARCADA — bug ou cron nao rodou ainda'],
        };
      }),
    });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

router.detectarFinalizadasF = detectarFinalizadasF;
module.exports = router;
