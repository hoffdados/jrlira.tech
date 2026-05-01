const express = require('express');
const router = express.Router();
const multer = require('multer');
const pool = require('../db');
const { autenticar } = require('../auth');

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 5 * 1024 * 1024 }
});

const FOTO_URL = (id) => `/api/funcionarios/${id}/foto`;

// Listar
router.get('/', autenticar, async (req, res) => {
  try {
    const { status, loja_id, grupo_cargo, cargo, q } = req.query;
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
    const rows = await pool.query(
      `SELECT id, matricula, nome, email, cargo, grupo_cargo, loja_id, status, data_admissao,
        CASE WHEN foto_data IS NOT NULL THEN '/api/funcionarios/' || id::text || '/foto'
             WHEN foto_path IS NOT NULL THEN foto_path
             ELSE NULL
        END as foto_path
       FROM funcionarios WHERE ${where.join(' AND ')} ORDER BY nome`,
      params
    );
    res.json(rows);
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// Buscar por matrícula (para lookup nos outros apps)
router.get('/matricula/:matricula', autenticar, async (req, res) => {
  try {
    const rows = await pool.query('SELECT id, matricula, nome, cargo, grupo_cargo, loja_id FROM funcionarios WHERE matricula = $1', [req.params.matricula]);
    if (!rows.length) return res.status(404).json({ erro: 'Não encontrado' });
    res.json(rows[0]);
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// Foto do funcionário (público — consumido por <img src>)
router.get('/:id/foto', async (req, res) => {
  try {
    const rows = await pool.query('SELECT foto_data, foto_mime FROM funcionarios WHERE id = $1', [req.params.id]);
    if (!rows.length || !rows[0].foto_data) return res.status(404).end();
    res.setHeader('Content-Type', rows[0].foto_mime || 'image/jpeg');
    res.setHeader('Cache-Control', 'public, max-age=86400');
    res.send(rows[0].foto_data);
  } catch (err) { res.status(500).end(); }
});

// Detalhe
router.get('/:id', autenticar, async (req, res) => {
  try {
    const rows = await pool.query(
      `SELECT id, matricula, nome, cpf, pis, data_nascimento, sexo, escolaridade, raca, estado_civil,
        cargo, grupo_cargo, nivel, loja_id, salario, status, data_admissao, data_demissao,
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
    const eventos = await pool.query('SELECT * FROM funcionario_eventos WHERE funcionario_id = $1 ORDER BY data_inicio DESC', [f.id]);
    res.json({ ...f, eventos });
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// Criar
router.post('/', autenticar, upload.single('foto'), async (req, res) => {
  try {
    const {
      matricula, nome, cpf, pis, data_nascimento, sexo, escolaridade, raca, estado_civil,
      cargo, grupo_cargo, nivel, loja_id, salario, status, data_admissao,
      cep, logradouro, numero, complemento, bairro, cidade, uf, telefone, email
    } = req.body;
    const rows = await pool.query(`
      INSERT INTO funcionarios (matricula, nome, cpf, pis, data_nascimento, sexo, escolaridade, raca, estado_civil,
        cargo, grupo_cargo, nivel, loja_id, salario, status, data_admissao,
        cep, logradouro, numero, complemento, bairro, cidade, uf, telefone, email, foto_data, foto_mime)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27)
      RETURNING id
    `, [matricula, nome, cpf, pis, data_nascimento||null, sexo, escolaridade, raca, estado_civil,
        cargo, grupo_cargo, nivel, loja_id||null, salario||null, status||'ATIVO', data_admissao||null,
        cep, logradouro, numero, complemento, bairro, cidade, uf, telefone, email,
        req.file ? req.file.buffer : null, req.file ? req.file.mimetype : null]);
    res.json({ ok: true, id: rows[0].id });
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// Atualizar
router.put('/:id', autenticar, upload.single('foto'), async (req, res) => {
  try {
    const {
      matricula, nome, cpf, pis, data_nascimento, sexo, escolaridade, raca, estado_civil,
      cargo, grupo_cargo, nivel, loja_id, salario, status, data_admissao, data_demissao,
      causa_afastamento, motivo_afastamento,
      cep, logradouro, numero, complemento, bairro, cidade, uf, telefone, email
    } = req.body;

    const fotoClause = req.file ? ', foto_data = $29, foto_mime = $30' : '';
    const params = [
      matricula, nome, cpf, pis, data_nascimento||null, sexo, escolaridade, raca, estado_civil,
      cargo, grupo_cargo, nivel, loja_id||null, salario||null, status, data_admissao||null,
      data_demissao||null, causa_afastamento, motivo_afastamento,
      cep, logradouro, numero, complemento, bairro, cidade, uf, telefone, email,
      req.params.id
    ];
    if (req.file) params.splice(params.length - 1, 0, req.file.buffer, req.file.mimetype);

    const idPos = req.file ? 31 : 29;
    await pool.query(`
      UPDATE funcionarios SET
        matricula=$1, nome=$2, cpf=$3, pis=$4, data_nascimento=$5, sexo=$6,
        escolaridade=$7, raca=$8, estado_civil=$9, cargo=$10, grupo_cargo=$11,
        nivel=$12, loja_id=$13, salario=$14, status=$15, data_admissao=$16,
        data_demissao=$17, causa_afastamento=$18, motivo_afastamento=$19,
        cep=$20, logradouro=$21, numero=$22, complemento=$23, bairro=$24,
        cidade=$25, uf=$26, telefone=$27, email=$28${fotoClause},
        atualizado_em = NOW()
      WHERE id = $${idPos}
    `, params);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// Eventos
router.post('/:id/eventos', autenticar, async (req, res) => {
  try {
    const { tipo, data_inicio, data_fim, descricao } = req.body;
    await pool.query(
      'INSERT INTO funcionario_eventos (funcionario_id, tipo, data_inicio, data_fim, descricao, criado_por) VALUES ($1,$2,$3,$4,$5,$6)',
      [req.params.id, tipo, data_inicio, data_fim||null, descricao, req.usuario.nome]
    );
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

router.delete('/eventos/:eventoId', autenticar, async (req, res) => {
  try {
    await pool.query('DELETE FROM funcionario_eventos WHERE id = $1', [req.params.eventoId]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

module.exports = router;
