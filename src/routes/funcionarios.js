const express = require('express');
const router = express.Router();
const multer = require('multer');
const dbMod = require('../db');
const pool = dbMod.pool;
const { autenticar, autenticarOuQuery } = require('../auth');
const { enviarWhatsapp } = require('../whatsapp');

// helper: query simples (mantém compat com restante do arquivo que usa pool.query e espera array)
const dbQuery = dbMod.query;

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 5 * 1024 * 1024 }
});

const FOTO_URL = (id) => `/api/funcionarios/${id}/foto`;

// Listar (paginado: { rows, total, limit, offset })
router.get('/', autenticar, async (req, res) => {
  try {
    const { status, loja_id, grupo_cargo, cargo, q } = req.query;
    const limit  = Math.min(Math.max(parseInt(req.query.limit)  || 500, 1), 2000);
    const offset = Math.max(parseInt(req.query.offset) || 0, 0);

    let where = ['1=1'];
    let params = [];
    const { perfil, lojas } = req.usuario;
    if (perfil === 'rh' && lojas?.length) {
      params.push(lojas.map(Number).filter(Boolean));
      where.push(`loja_id = ANY($${params.length})`);
    }
    if (status) { params.push(status); where.push(`status = $${params.length}`); }
    if (loja_id) { params.push(loja_id); where.push(`loja_id = $${params.length}`); }
    if (grupo_cargo) { params.push(grupo_cargo); where.push(`grupo_cargo = $${params.length}`); }
    if (cargo) { params.push(cargo); where.push(`cargo = $${params.length}`); }
    if (q) { params.push(`%${q}%`); where.push(`(nome ILIKE $${params.length} OR matricula ILIKE $${params.length})`); }

    const whereSql = where.join(' AND ');
    const totalRes = await dbQuery(`SELECT COUNT(*)::int AS total FROM funcionarios WHERE ${whereSql}`, params);
    const total = totalRes[0]?.total || 0;

    params.push(limit, offset);
    const rows = await dbQuery(
      `SELECT id, matricula, nome, email, cargo, grupo_cargo, loja_id, status, data_admissao,
        CASE WHEN foto_data IS NOT NULL THEN '/api/funcionarios/' || id::text || '/foto'
             WHEN foto_path IS NOT NULL THEN foto_path
             ELSE NULL
        END as foto_path
       FROM funcionarios WHERE ${whereSql} ORDER BY nome
       LIMIT $${params.length - 1} OFFSET $${params.length}`,
      params
    );
    res.json({ rows, total, limit, offset });
  } catch (err) {
    console.error('[funcionarios listar] erro:', err.message);
    res.status(500).json({ erro: 'Erro ao listar funcionários' });
  }
});

// Painel RH com indicadores agregados
router.get('/painel-rh', autenticar, async (req, res) => {
  try {
    const { perfil, lojas } = req.usuario;
    let lojaFilter = '';
    const params = [];
    // Filtro por usuário (gerente vê só sua loja)
    if (perfil === 'rh' && lojas?.length) {
      params.push(lojas.map(Number).filter(Boolean));
      lojaFilter = `AND f.loja_id = ANY($${params.length})`;
    }
    // Filtro adicional via query (admin pode escolher loja específica)
    if (req.query.loja_id) {
      params.push(parseInt(req.query.loja_id));
      lojaFilter += ` AND f.loja_id = $${params.length}`;
    }

    // 1) Totais por loja (ativos, atestados hoje, faltas injustificadas no período de folha 26→25)
    // Início do período: dia 26 do mês corrente (se hoje >= 26) ou dia 26 do mês anterior
    const totaisPorLoja = await dbQuery(
      `WITH periodo AS (
         SELECT (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '25 days') + INTERVAL '25 days')::date AS inicio
       )
       SELECT
         f.loja_id,
         COUNT(*) FILTER (WHERE f.status = 'ATIVO') AS ativos,
         COUNT(*) FILTER (WHERE f.status = 'DESLIGADO') AS desligados,
         COUNT(DISTINCT e.funcionario_id) FILTER (WHERE e.tipo = 'atestado_medico'
           AND e.data_inicio <= CURRENT_DATE
           AND COALESCE(e.data_fim, e.data_inicio) >= CURRENT_DATE) AS atestados_hoje,
         COUNT(*) FILTER (WHERE e.tipo = 'falta_injustificada'
           AND e.data_inicio >= (SELECT inicio FROM periodo)) AS faltas_mes
       FROM funcionarios f
       LEFT JOIN funcionario_eventos e ON e.funcionario_id = f.id
       WHERE f.loja_id IS NOT NULL ${lojaFilter}
       GROUP BY f.loja_id ORDER BY f.loja_id`,
      params
    );

    // 2) Top funcionários "em risco" (score)
    const emRisco = await dbQuery(
      `SELECT
         f.id, f.matricula, f.nome, f.loja_id, f.cargo,
         (SELECT COUNT(*) FROM funcionario_eventos
          WHERE funcionario_id = f.id AND tipo = 'atestado_medico'
            AND data_inicio >= CURRENT_DATE - INTERVAL '60 days') AS atestados_60d,
         (SELECT COUNT(*) FROM funcionario_eventos
          WHERE funcionario_id = f.id AND tipo IN ('advertencia_verbal','advertencia_escrita')
            AND data_inicio >= CURRENT_DATE - INTERVAL '90 days') AS advertencias_90d,
         (SELECT COUNT(*) FROM funcionario_eventos
          WHERE funcionario_id = f.id AND tipo = 'suspensao'
            AND data_inicio >= CURRENT_DATE - INTERVAL '90 days') AS suspensoes_90d,
         (SELECT COUNT(*) FROM funcionario_eventos
          WHERE funcionario_id = f.id AND tipo = 'falta_injustificada'
            AND data_inicio >= CURRENT_DATE - INTERVAL '90 days') AS faltas_90d,
         (SELECT COUNT(*) FROM funcionario_eventos
          WHERE funcionario_id = f.id AND tipo = 'atraso'
            AND data_inicio >= CURRENT_DATE - INTERVAL '30 days') AS atrasos_30d
       FROM funcionarios f
       WHERE f.status = 'ATIVO' ${lojaFilter}
       ORDER BY (
         (SELECT COUNT(*) FROM funcionario_eventos WHERE funcionario_id = f.id
            AND tipo = 'atestado_medico' AND data_inicio >= CURRENT_DATE - INTERVAL '60 days') * 30 +
         (SELECT COUNT(*) FROM funcionario_eventos WHERE funcionario_id = f.id
            AND tipo IN ('advertencia_verbal','advertencia_escrita') AND data_inicio >= CURRENT_DATE - INTERVAL '90 days') * 25 +
         (SELECT COUNT(*) FROM funcionario_eventos WHERE funcionario_id = f.id
            AND tipo = 'suspensao' AND data_inicio >= CURRENT_DATE - INTERVAL '90 days') * 50 +
         (SELECT COUNT(*) FROM funcionario_eventos WHERE funcionario_id = f.id
            AND tipo = 'falta_injustificada' AND data_inicio >= CURRENT_DATE - INTERVAL '90 days') * 20 +
         (SELECT COUNT(*) FROM funcionario_eventos WHERE funcionario_id = f.id
            AND tipo = 'atraso' AND data_inicio >= CURRENT_DATE - INTERVAL '30 days') * 5
       ) DESC
       LIMIT 10`,
      params
    );
    for (const f of emRisco) {
      f.score = (parseInt(f.atestados_60d)||0)*30 + (parseInt(f.advertencias_90d)||0)*25
              + (parseInt(f.suspensoes_90d)||0)*50 + (parseInt(f.faltas_90d)||0)*20
              + (parseInt(f.atrasos_30d)||0)*5;
    }

    // 3) Aniversariantes da semana
    const aniversariantes = await dbQuery(
      `SELECT id, matricula, nome, data_nascimento, loja_id
       FROM funcionarios f
       WHERE status = 'ATIVO' AND data_nascimento IS NOT NULL ${lojaFilter}
         AND EXTRACT(DOY FROM TO_DATE(
              EXTRACT(YEAR FROM CURRENT_DATE) || '-' ||
              EXTRACT(MONTH FROM data_nascimento) || '-' ||
              EXTRACT(DAY FROM data_nascimento), 'YYYY-MM-DD'))
            BETWEEN EXTRACT(DOY FROM CURRENT_DATE) AND EXTRACT(DOY FROM CURRENT_DATE + INTERVAL '7 days')
       ORDER BY EXTRACT(MONTH FROM data_nascimento), EXTRACT(DAY FROM data_nascimento)`,
      params
    );

    // 4) Tempo de casa (1, 5, 10 anos completados no mês corrente)
    const tempoCasa = await dbQuery(
      `SELECT id, matricula, nome, data_admissao, loja_id,
              EXTRACT(YEAR FROM AGE(CURRENT_DATE, data_admissao))::int AS anos
       FROM funcionarios f
       WHERE status = 'ATIVO' AND data_admissao IS NOT NULL ${lojaFilter}
         AND EXTRACT(MONTH FROM data_admissao) = EXTRACT(MONTH FROM CURRENT_DATE)
         AND EXTRACT(YEAR FROM AGE(CURRENT_DATE, data_admissao))::int IN (1, 5, 10, 15, 20)
       ORDER BY anos DESC, EXTRACT(DAY FROM data_admissao)`,
      params
    );

    // 5) Alerta INSS — funcionários com 14+ dias de atestado em 60 dias
    const alertaInss = await dbQuery(
      `SELECT f.id, f.matricula, f.nome, f.loja_id, f.cargo,
              COALESCE(SUM(GREATEST(1, COALESCE(e.dias_afastado, (e.data_fim - e.data_inicio + 1)))), 0)::int AS dias_60
       FROM funcionarios f
       JOIN funcionario_eventos e ON e.funcionario_id = f.id
       WHERE f.status NOT IN ('DESLIGADO')
         AND e.tipo IN ('atestado_medico','licenca_maternidade','afastamento_inss','acidente_trabalho')
         AND e.data_inicio >= CURRENT_DATE - INTERVAL '60 days'
         ${lojaFilter}
       GROUP BY f.id, f.matricula, f.nome, f.loja_id, f.cargo
       HAVING COALESCE(SUM(GREATEST(1, COALESCE(e.dias_afastado, (e.data_fim - e.data_inicio + 1)))), 0) >= 14
       ORDER BY dias_60 DESC`,
      params
    );

    res.json({
      totais_por_loja: totaisPorLoja,
      em_risco: emRisco,
      alerta_inss: alertaInss,
      aniversariantes_semana: aniversariantes,
      tempo_de_casa_mes: tempoCasa,
    });
  } catch (err) {
    console.error('[painel-rh]', err.message);
    res.status(500).json({ erro: 'Erro ao carregar painel' });
  }
});

// Catálogo de tipos de licença / afastamento (auto-fill de dias)
router.get('/tipos-licenca', autenticar, async (req, res) => {
  try {
    const rows = await dbQuery(
      `SELECT codigo, nome, dias_padrao, dias_max, exige_atestado, remunerada,
              status_funcionario, base_legal, descricao
       FROM tipos_licenca ORDER BY ordem, nome`
    );
    res.json(rows);
  } catch (err) {
    console.error('[tipos-licenca]', err.message);
    res.status(500).json({ erro: 'Erro ao carregar catálogo' });
  }
});

// Busca CID-10 (autocomplete)
router.get('/cid/buscar', autenticar, async (req, res) => {
  try {
    const q = String(req.query.q || '').trim();
    if (!q) return res.json([]);
    const rows = await dbQuery(
      `SELECT codigo, descricao FROM cid_codigos
       WHERE codigo ILIKE $1 OR descricao ILIKE $2
       ORDER BY codigo LIMIT 20`,
      [q + '%', '%' + q + '%']
    );
    res.json(rows);
  } catch (err) { console.error('[funcionarios]', err.message); res.status(500).json({ erro: 'Erro interno' }); }
});

// Listar terceirizadas distintas (para popular dropdown)
router.get('/terceirizadas', autenticar, async (req, res) => {
  try {
    const rows = await dbQuery(
      `SELECT DISTINCT terceirizada FROM funcionarios
       WHERE terceirizada IS NOT NULL AND terceirizada <> ''
       ORDER BY terceirizada`
    );
    res.json(rows.map(r => r.terceirizada));
  } catch (err) { console.error('[funcionarios]', err.message); res.status(500).json({ erro: 'Erro interno' }); }
});

// Buscar por matrícula (para lookup nos outros apps)
router.get('/matricula/:matricula', autenticar, async (req, res) => {
  try {
    const rows = await dbQuery('SELECT id, matricula, nome, cargo, grupo_cargo, loja_id FROM funcionarios WHERE matricula = $1', [req.params.matricula]);
    if (!rows.length) return res.status(404).json({ erro: 'Não encontrado' });
    res.json(rows[0]);
  } catch (err) { console.error('[funcionarios]', err.message); res.status(500).json({ erro: 'Erro interno' }); }
});

// Foto do funcionário (auth via header OU ?token= para uso em <img src>)
router.get('/:id/foto', autenticarOuQuery, async (req, res) => {
  try {
    const rows = await dbQuery('SELECT foto_data, foto_mime FROM funcionarios WHERE id = $1', [req.params.id]);
    if (!rows.length || !rows[0].foto_data) return res.status(404).end();
    res.setHeader('Content-Type', rows[0].foto_mime || 'image/jpeg');
    res.setHeader('Cache-Control', 'private, max-age=3600');
    res.send(rows[0].foto_data);
  } catch (err) {
    console.error('[foto] erro:', err.message);
    res.status(500).end();
  }
});

// Detalhe
router.get('/:id', autenticar, async (req, res) => {
  try {
    const rows = await dbQuery(
      `SELECT id, matricula, nome, cpf, pis, data_nascimento, sexo, escolaridade, raca, estado_civil,
        cargo, grupo_cargo, nivel, loja_id, terceirizada, salario, status, data_admissao, data_demissao,
        causa_afastamento, motivo_afastamento,
        cep, logradouro, numero, complemento, bairro, cidade, uf, telefone, email,
        criado_em, atualizado_em,
        CASE WHEN foto_data IS NOT NULL THEN '/api/funcionarios/' || id::text || '/foto'
             WHEN foto_path IS NOT NULL THEN foto_path
             ELSE NULL
        END as foto_path
       FROM funcionarios WHERE id = $1`,
      [req.params.id]
    );
    if (!rows.length) return res.status(404).json({ erro: 'Não encontrado' });
    const f = rows[0];
    const eventos = await dbQuery(
      `SELECT id, tipo, data_inicio, data_fim, descricao,
              loja_origem_id, loja_destino_id,
              cid, cid_descricao, medico_nome, medico_crm, clinica, dias_afastado,
              gravidade, motivo_categoria, minutos,
              anexo_mime, anexo_nome, (anexo IS NOT NULL) AS tem_anexo,
              cargo_anterior, cargo_novo, salario_anterior::float, salario_novo::float,
              criado_por, criado_em, editado_por, editado_em
       FROM funcionario_eventos WHERE funcionario_id = $1
       ORDER BY data_inicio DESC, id DESC`, [f.id]);
    const dias_60 = await diasAtestadoUltimos(f.id, 60);
    const alerta_inss = dias_60 >= 14;
    res.json({ ...f, eventos, dias_atestado_60d: dias_60, alerta_inss });
  } catch (err) { console.error('[funcionarios]', err.message); res.status(500).json({ erro: 'Erro interno' }); }
});

// Criar
router.post('/', autenticar, upload.single('foto'), async (req, res) => {
  try {
    const {
      matricula, nome, cpf, pis, data_nascimento, sexo, escolaridade, raca, estado_civil,
      cargo, grupo_cargo, nivel, loja_id, terceirizada, salario, status, data_admissao,
      cep, logradouro, numero, complemento, bairro, cidade, uf, telefone, email
    } = req.body;
    const rows = await dbQuery(`
      INSERT INTO funcionarios (matricula, nome, cpf, pis, data_nascimento, sexo, escolaridade, raca, estado_civil,
        cargo, grupo_cargo, nivel, loja_id, terceirizada, salario, status, data_admissao,
        cep, logradouro, numero, complemento, bairro, cidade, uf, telefone, email, foto_data, foto_mime)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28)
      RETURNING id
    `, [matricula, nome, cpf, pis, data_nascimento||null, sexo, escolaridade, raca, estado_civil,
        cargo, grupo_cargo, nivel, loja_id||null, terceirizada||null, salario||null, status||'ATIVO', data_admissao||null,
        cep, logradouro, numero, complemento, bairro, cidade, uf, telefone, email,
        req.file ? req.file.buffer : null, req.file ? req.file.mimetype : null]);
    res.json({ ok: true, id: rows[0].id });
  } catch (err) { console.error('[funcionarios]', err.message); res.status(500).json({ erro: 'Erro interno' }); }
});

// Atualizar
router.put('/:id', autenticar, upload.single('foto'), async (req, res) => {
  try {
    const {
      matricula, nome, cpf, pis, data_nascimento, sexo, escolaridade, raca, estado_civil,
      cargo, grupo_cargo, nivel, loja_id, terceirizada, salario, status, data_admissao, data_demissao,
      causa_afastamento, motivo_afastamento,
      cep, logradouro, numero, complemento, bairro, cidade, uf, telefone, email
    } = req.body;

    const fotoClause = req.file ? ', foto_data = $30, foto_mime = $31' : '';
    const params = [
      matricula, nome, cpf, pis, data_nascimento||null, sexo, escolaridade, raca, estado_civil,
      cargo, grupo_cargo, nivel, loja_id||null, terceirizada||null, salario||null,
      status || null, data_admissao || null,
      data_demissao||null, causa_afastamento, motivo_afastamento,
      cep, logradouro, numero, complemento, bairro, cidade, uf, telefone, email,
      req.params.id
    ];
    if (req.file) params.splice(params.length - 1, 0, req.file.buffer, req.file.mimetype);

    const idPos = req.file ? 32 : 30;
    // status e data_admissao usam COALESCE para nao sobrescrever com NULL quando o
    // form nao envia o campo (era a causa do "status null" reclamado pelo RH).
    await pool.query(`
      UPDATE funcionarios SET
        matricula=$1, nome=$2, cpf=$3, pis=$4, data_nascimento=$5, sexo=$6,
        escolaridade=$7, raca=$8, estado_civil=$9, cargo=$10, grupo_cargo=$11,
        nivel=$12, loja_id=$13, terceirizada=$14, salario=$15,
        status=COALESCE($16, status), data_admissao=COALESCE($17, data_admissao),
        data_demissao=$18, causa_afastamento=$19, motivo_afastamento=$20,
        cep=$21, logradouro=$22, numero=$23, complemento=$24, bairro=$25,
        cidade=$26, uf=$27, telefone=$28, email=$29${fotoClause},
        atualizado_em = NOW()
      WHERE id = $${idPos}
    `, params);
    res.json({ ok: true });
  } catch (err) { console.error('[funcionarios]', err.message); res.status(500).json({ erro: 'Erro interno' }); }
});

// ── EVENTOS — source-of-truth do cadastro ──────────────────────────
const TIPOS_EVENTO = new Set([
  'admissao','demissao','transferencia','mudanca_cargo','promocao','aumento_salario',
  'atestado_medico','licenca_maternidade','licenca_paternidade',
  'afastamento_inss','acidente_trabalho','retorno',
  'licenca_doenca_familia','licenca_casamento','licenca_luto',
  'licenca_doacao_sangue','licenca_servico_eleitoral_juri',
  'falta_justificada','falta_injustificada','atraso',
  'advertencia_verbal','advertencia_escrita','suspensao',
  'ferias','folga_compensatoria','treinamento',
  'feedback_positivo','aniversario','tempo_de_casa',
  'outro',
]);

// Status atual = derivado dos eventos (último que afeta status)
async function recalcularCadastro(client, funcionario_id) {
  const { rows: evs } = await client.query(
    `SELECT * FROM funcionario_eventos WHERE funcionario_id = $1 ORDER BY data_inicio ASC, id ASC`,
    [funcionario_id]
  );
  let data_admissao = null;
  let data_demissao = null;
  let status = 'ATIVO';
  let loja_id = null;
  let cargo = null;
  let salario = null;
  let causa = null;
  let motivo = null;
  const hoje = new Date().toISOString().substring(0, 10);

  for (const e of evs) {
    const di = e.data_inicio?.toISOString?.().substring(0,10) || e.data_inicio;
    const df = e.data_fim?.toISOString?.().substring(0,10) || e.data_fim;
    if (e.tipo === 'admissao') { data_admissao = di; status = 'ATIVO'; data_demissao = null; }
    else if (e.tipo === 'demissao') { data_demissao = di; status = 'DESLIGADO'; causa = e.motivo_categoria; motivo = e.descricao; }
    else if (e.tipo === 'transferencia' && e.loja_destino_id) { loja_id = e.loja_destino_id; }
    else if (e.tipo === 'mudanca_cargo' || e.tipo === 'promocao') {
      if (e.cargo_novo) cargo = e.cargo_novo;
      if (e.salario_novo) salario = parseFloat(e.salario_novo);
    }
    else if (e.tipo === 'aumento_salario' && e.salario_novo) { salario = parseFloat(e.salario_novo); }
    else if (e.tipo === 'atestado_medico' && status === 'ATIVO' && (!df || df >= hoje) && di <= hoje) status = 'ATESTADO';
    else if (e.tipo === 'licenca_maternidade' && (!df || df >= hoje) && di <= hoje) status = 'LICENSA MATERNIDADE';
    else if (e.tipo === 'afastamento_inss' && (!df || df >= hoje) && di <= hoje) status = 'INSS';
    else if (e.tipo === 'acidente_trabalho' && (!df || df >= hoje) && di <= hoje) status = 'INSS';
    else if (e.tipo === 'licenca_doenca_familia' && (!df || df >= hoje) && di <= hoje) status = 'AFASTADO';
    else if (e.tipo === 'retorno' && di <= hoje) status = 'ATIVO';
  }

  const updates = [], params = [];
  if (data_admissao !== null) { params.push(data_admissao); updates.push(`data_admissao=$${params.length}`); }
  if (data_demissao !== null) { params.push(data_demissao); updates.push(`data_demissao=$${params.length}`); }
  else { updates.push('data_demissao=NULL'); }
  params.push(status); updates.push(`status=$${params.length}`);
  if (loja_id !== null) { params.push(loja_id); updates.push(`loja_id=$${params.length}`); }
  if (cargo !== null) { params.push(cargo); updates.push(`cargo=$${params.length}`); }
  if (salario !== null) { params.push(salario); updates.push(`salario=$${params.length}`); }
  if (causa !== null) { params.push(causa); updates.push(`causa_afastamento=$${params.length}`); }
  if (motivo !== null) { params.push(motivo); updates.push(`motivo_afastamento=$${params.length}`); }

  params.push(funcionario_id);
  await client.query(
    `UPDATE funcionarios SET ${updates.join(', ')}, atualizado_em = NOW() WHERE id = $${params.length}`,
    params
  );
}

// Soma dias de atestado em janela de N dias (para alerta INSS)
async function diasAtestadoUltimos(funcionario_id, dias) {
  const { rows } = await pool.query(
    `SELECT COALESCE(SUM(GREATEST(1, COALESCE(dias_afastado, (data_fim - data_inicio + 1)))), 0) AS soma
     FROM funcionario_eventos
     WHERE funcionario_id = $1
       AND tipo IN ('atestado_medico','licenca_maternidade','afastamento_inss','acidente_trabalho')
       AND data_inicio >= CURRENT_DATE - $2::int * INTERVAL '1 day'`,
    [funcionario_id, dias]
  );
  return parseInt(rows[0]?.soma) || 0;
}

router.post('/:id/eventos', autenticar, upload.single('anexo'), async (req, res) => {
  const client = await pool.connect();
  try {
    const b = req.body;
    const tipo = String(b.tipo || '').trim();
    if (!TIPOS_EVENTO.has(tipo)) return res.status(400).json({ erro: 'Tipo de evento inválido' });
    if (!b.data_inicio) return res.status(400).json({ erro: 'data_inicio obrigatória' });

    // Validações específicas
    const { rows: [func] } = await client.query('SELECT * FROM funcionarios WHERE id = $1', [req.params.id]);
    if (!func) return res.status(404).json({ erro: 'Funcionário não encontrado' });

    if (tipo === 'admissao') {
      const { rows: ja } = await client.query(
        `SELECT id FROM funcionario_eventos WHERE funcionario_id = $1 AND tipo = 'admissao'`,
        [req.params.id]
      );
      if (ja.length) return res.status(400).json({ erro: 'Funcionário já possui evento de admissão' });
    }

    if (tipo === 'atestado_medico' && !req.file) {
      return res.status(400).json({ erro: 'PDF do atestado é obrigatório' });
    }

    if (tipo === 'advertencia_escrita' && !req.file) {
      return res.status(400).json({ erro: 'Documento assinado é obrigatório para advertência escrita' });
    }

    const dias_afastado = b.data_fim && b.data_inicio
      ? Math.max(1, Math.round((new Date(b.data_fim) - new Date(b.data_inicio)) / 86400000) + 1)
      : (b.dias_afastado ? parseInt(b.dias_afastado) : null);

    await client.query('BEGIN');
    const { rows: [novo] } = await client.query(`
      INSERT INTO funcionario_eventos
        (funcionario_id, tipo, data_inicio, data_fim, descricao,
         loja_origem_id, loja_destino_id,
         cid, cid_descricao, medico_nome, medico_crm, clinica, dias_afastado,
         gravidade, motivo_categoria, minutos,
         anexo, anexo_mime, anexo_nome,
         cargo_anterior, cargo_novo, salario_anterior, salario_novo,
         criado_por)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)
      RETURNING id
    `, [
      req.params.id, tipo, b.data_inicio, b.data_fim || null, b.descricao || null,
      b.loja_origem_id ? parseInt(b.loja_origem_id) : (tipo === 'transferencia' ? func.loja_id : null),
      b.loja_destino_id ? parseInt(b.loja_destino_id) : null,
      b.cid || null, b.cid_descricao || null, b.medico_nome || null, b.medico_crm || null, b.clinica || null,
      dias_afastado,
      b.gravidade ? parseInt(b.gravidade) : null,
      b.motivo_categoria || null,
      b.minutos ? parseInt(b.minutos) : null,
      req.file ? req.file.buffer : null, req.file ? req.file.mimetype : null,
      req.file ? req.file.originalname : null,
      tipo === 'mudanca_cargo' || tipo === 'promocao' ? (b.cargo_anterior || func.cargo) : null,
      b.cargo_novo || null,
      tipo === 'aumento_salario' || tipo === 'mudanca_cargo' || tipo === 'promocao' ? (b.salario_anterior || func.salario) : null,
      b.salario_novo ? parseFloat(b.salario_novo) : null,
      req.usuario.nome,
    ]);

    // Source-of-truth: recalcula cadastro a partir dos eventos
    await recalcularCadastro(client, req.params.id);
    await client.query('COMMIT');

    // Alerta INSS: soma 14+ dias de atestado em 60 dias
    let alerta_inss = false;
    const soma60 = await diasAtestadoUltimos(req.params.id, 60);
    if (soma60 >= 14) alerta_inss = true;

    // Notificação WhatsApp (best-effort, não bloqueante)
    if (func.telefone) {
      const fmtData = (s) => { if (!s) return ''; const d = new Date(s); return d.toLocaleDateString('pt-BR',{timeZone:'UTC'}); };
      let msg = null;
      if (tipo === 'atestado_medico') {
        msg = `📋 *Atestado registrado*\n\nOlá, ${func.nome}.\nSeu atestado médico foi registrado pelo RH:\n• Início: ${fmtData(b.data_inicio)}${b.data_fim ? `\n• Fim: ${fmtData(b.data_fim)}` : ''}${dias_afastado ? `\n• Dias: ${dias_afastado}` : ''}\n\nDúvidas? Procure o RH.`;
      } else if (tipo === 'advertencia_verbal') {
        msg = `⚠ *Advertência verbal*\n\nOlá, ${func.nome}.\nFoi registrada uma advertência verbal em sua ficha:\n${b.descricao || '(sem descrição)'}\n\nProcure o RH para esclarecimentos.`;
      } else if (tipo === 'advertencia_escrita') {
        msg = `⚠ *Advertência escrita*\n\nOlá, ${func.nome}.\nFoi registrada uma advertência escrita em sua ficha. O documento assinado já está arquivado.\n\nProcure o RH para esclarecimentos.`;
      } else if (tipo === 'suspensao') {
        msg = `🚫 *Suspensão registrada*\n\nOlá, ${func.nome}.\nFoi registrada uma suspensão em sua ficha:\n• Início: ${fmtData(b.data_inicio)}${b.data_fim ? `\n• Fim: ${fmtData(b.data_fim)}` : ''}\n\nProcure o RH para esclarecimentos.`;
      } else if (tipo === 'feedback_positivo') {
        msg = `🌟 *Reconhecimento*\n\nOlá, ${func.nome}!\n${b.descricao || 'O RH registrou um feedback positivo sobre o seu trabalho. Parabéns!'}`;
      } else if (tipo === 'promocao' && b.cargo_novo) {
        msg = `🎉 *Promoção registrada*\n\nParabéns, ${func.nome}!\nNovo cargo: *${b.cargo_novo}*${b.salario_novo ? `\nNovo salário: R$ ${parseFloat(b.salario_novo).toFixed(2).replace('.', ',')}` : ''}`;
      } else if (tipo === 'aumento_salario' && b.salario_novo) {
        msg = `💰 *Aumento salarial*\n\nParabéns, ${func.nome}!\nNovo salário: R$ ${parseFloat(b.salario_novo).toFixed(2).replace('.', ',')}`;
      }
      if (msg) {
        Promise.resolve().then(() => enviarWhatsapp(func.telefone, msg))
          .catch(err => console.error('[whatsapp evento]', err.message));
      }
    }

    res.json({ ok: true, id: novo.id, alerta_inss, dias_60: soma60 });
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('[evento] erro:', err.message);
    res.status(500).json({ erro: 'Erro ao registrar evento' });
  } finally {
    client.release();
  }
});

router.delete('/eventos/:eventoId', autenticar, async (req, res) => {
  const client = await pool.connect();
  try {
    const { rows: [ev] } = await client.query('SELECT funcionario_id FROM funcionario_eventos WHERE id = $1', [req.params.eventoId]);
    if (!ev) return res.status(404).json({ erro: 'Evento não encontrado' });
    await client.query('BEGIN');
    await client.query('DELETE FROM funcionario_eventos WHERE id = $1', [req.params.eventoId]);
    await recalcularCadastro(client, ev.funcionario_id);
    await client.query('COMMIT');
    res.json({ ok: true });
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('[evento delete]', err.message);
    res.status(500).json({ erro: 'Erro ao remover evento' });
  } finally {
    client.release();
  }
});

// Limpeza de anexos: 30 dias após demissão
router.post('/limpar-anexos-antigos', autenticar, async (req, res) => {
  try {
    if (req.usuario.perfil !== 'admin') return res.status(403).json({ erro: 'Apenas admin' });
    const { rows } = await pool.query(
      `UPDATE funcionario_eventos
         SET anexo = NULL, anexo_mime = NULL, anexo_nome = NULL
       WHERE anexo IS NOT NULL
         AND funcionario_id IN (
           SELECT id FROM funcionarios
           WHERE status = 'DESLIGADO'
             AND data_demissao IS NOT NULL
             AND data_demissao < CURRENT_DATE - INTERVAL '30 days'
         )
       RETURNING id`
    );
    res.json({ ok: true, anexos_removidos: rows.length });
  } catch (err) { console.error('[funcionarios]', err.message); res.status(500).json({ erro: 'Erro interno' }); }
});

// Anexo do evento (PDF do atestado, doc da advertência etc)
router.get('/eventos/:eventoId/anexo', autenticar, async (req, res) => {
  try {
    const { rows: [r] } = await pool.query(
      'SELECT anexo, anexo_mime, anexo_nome FROM funcionario_eventos WHERE id = $1',
      [req.params.eventoId]
    );
    if (!r || !r.anexo) return res.status(404).end();
    res.setHeader('Content-Type', r.anexo_mime || 'application/octet-stream');
    if (r.anexo_nome) res.setHeader('Content-Disposition', `inline; filename="${r.anexo_nome}"`);
    res.send(r.anexo);
  } catch (err) { res.status(500).end(); }
});

module.exports = router;
