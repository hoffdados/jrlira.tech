const express = require('express');
const router = express.Router();
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const pool = require('../db');
const { autenticar, apenasAdmin } = require('../auth');

const FOTOS_DIR = path.join(__dirname, '../../uploads/fotos');
const storage = multer.diskStorage({
  destination: (req, file, cb) => { fs.mkdirSync(FOTOS_DIR, { recursive: true }); cb(null, FOTOS_DIR); },
  filename: (req, file, cb) => { cb(null, `func-${Date.now()}${path.extname(file.originalname)}`); }
});
const upload = multer({ storage, limits: { fileSize: 5 * 1024 * 1024 } });

// Listar
router.get('/', autenticar, async (req, res) => {
  try {
    const { status, loja_id, grupo_cargo, q } = req.query;
    let where = ['1=1'];
    let params = [];
    if (status) { params.push(status); where.push(`status = $${params.length}`); }
    if (loja_id) { params.push(loja_id); where.push(`loja_id = $${params.length}`); }
    if (grupo_cargo) { params.push(grupo_cargo); where.push(`grupo_cargo = $${params.length}`); }
    if (q) { params.push(`%${q}%`); where.push(`(nome ILIKE $${params.length} OR matricula ILIKE $${params.length})`); }
    const rows = await pool.query(
      `SELECT id, matricula, nome, cargo, grupo_cargo, loja_id, status, data_admissao, foto_path FROM funcionarios WHERE ${where.join(' AND ')} ORDER BY nome`,
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

// Detalhe
router.get('/:id', autenticar, async (req, res) => {
  try {
    const rows = await pool.query('SELECT * FROM funcionarios WHERE id = $1', [req.params.id]);
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
    const foto_path = req.file ? `/uploads/fotos/${req.file.filename}` : null;
    const rows = await pool.query(`
      INSERT INTO funcionarios (matricula, nome, cpf, pis, data_nascimento, sexo, escolaridade, raca, estado_civil,
        cargo, grupo_cargo, nivel, loja_id, salario, status, data_admissao,
        cep, logradouro, numero, complemento, bairro, cidade, uf, telefone, email, foto_path)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)
      RETURNING id
    `, [matricula, nome, cpf, pis, data_nascimento||null, sexo, escolaridade, raca, estado_civil,
        cargo, grupo_cargo, nivel, loja_id||null, salario||null, status||'ATIVO', data_admissao||null,
        cep, logradouro, numero, complemento, bairro, cidade, uf, telefone, email, foto_path]);
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

    let foto_path = undefined;
    if (req.file) foto_path = `/uploads/fotos/${req.file.filename}`;

    const fotoClause = foto_path ? ', foto_path = $29' : '';
    const params = [
      matricula, nome, cpf, pis, data_nascimento||null, sexo, escolaridade, raca, estado_civil,
      cargo, grupo_cargo, nivel, loja_id||null, salario||null, status, data_admissao||null,
      data_demissao||null, causa_afastamento, motivo_afastamento,
      cep, logradouro, numero, complemento, bairro, cidade, uf, telefone, email,
      req.params.id
    ];
    if (foto_path) params.splice(params.length - 1, 0, foto_path);

    const idPos = foto_path ? 30 : 29;
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
