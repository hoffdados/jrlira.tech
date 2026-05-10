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
const { autenticar, exigirPerfil } = require('../auth');

const apenasAdmin = [autenticar, exigirPerfil('admin')];

// Detecta e marca notas como finalizada_f (chamada pelo cron e via endpoint)
async function detectarFinalizadasF() {
  const r = await dbQuery(`
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
  return r;
}

// Endpoint manual pra disparar detecção
router.post('/detectar', apenasAdmin, async (req, res) => {
  try {
    const marcadas = await detectarFinalizadasF();
    res.json({ ok: true, total: marcadas.length, notas: marcadas });
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

router.detectarFinalizadasF = detectarFinalizadasF;
module.exports = router;
