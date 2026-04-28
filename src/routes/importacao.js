const express = require('express');
const router = express.Router();
const multer = require('multer');
const XLSX = require('xlsx');
const pool = require('../db');
const { autenticar, apenasAdmin } = require('../auth');

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 20 * 1024 * 1024 } });

function excelDateToISO(serial) {
  if (!serial || isNaN(serial)) return null;
  const date = new Date(Math.round((serial - 25569) * 86400 * 1000));
  if (isNaN(date.getTime())) return null;
  return date.toISOString().split('T')[0];
}

function toDate(v) {
  return typeof v === 'number' ? excelDateToISO(v) : limpar(v);
}

function mapSexo(v) {
  if (!v) return null;
  const s = String(v).trim().toUpperCase();
  if (s === 'M' || s.startsWith('MAS') || s.startsWith('HOM')) return 'M';
  if (s === 'F' || s.startsWith('FEM') || s.startsWith('MUL')) return 'F';
  return s.charAt(0) || null;
}

function limpar(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s === '' ? null : s;
}

function detectarSheet(sheetNames) {
  return sheetNames.find(n => n === 'qlp') ||
         sheetNames.find(n => n === 'QLP') ||
         sheetNames[0];
}

function mapearColunas(headers) {
  const col = (name) => headers.findIndex(h => h && String(h).toLowerCase().trim() === name.toLowerCase());
  const colInc = (name) => headers.findIndex(h => h && String(h).toLowerCase().includes(name.toLowerCase()));

  // id_loja: tenta match exato primeiro, fallback para Empresa
  const idxLojaId  = col('id_loja');
  const idxEmpresa = colInc('Empresa');

  return {
    idxNome:      colInc('Nome'),
    idxAdm:       colInc('Admiss'),
    idxDem:       colInc('Demiss'),
    idxCargo:     colInc('Descrição Cargo') !== -1 ? colInc('Descrição Cargo') : (colInc('Cargo') !== -1 ? colInc('Cargo') : colInc('Descri')),
    idxGrupo:     col('grupo cargo') !== -1 ? col('grupo cargo') : colInc('Grupo'),
    idxNivel:     col('nivel') !== -1 ? col('nivel') : colInc('Nivel'),
    idxLojaId,
    idxEmpresa,
    idxStatus:    colInc('Status'),
    idxNasc:      colInc('Nasc'),
    idxSexo:      colInc('Sexo'),
    idxEscol:     col('escolaridade') !== -1 ? col('escolaridade') : colInc('Escolar'),
    idxCpf:       colInc('CPF'),
    idxCracha:    colInc('Cracha') !== -1 ? colInc('Cracha') : colInc('Crachá'),
    idxSalario:   colInc('Salar'),
    idxCausa:     colInc('Causa'),
    idxMotivo:    colInc('Motivo'),
    idxRaca:      colInc('Raca') !== -1 ? colInc('Raca') : colInc('Raça'),
    idxEstCivil:  colInc('Estado Civil') !== -1 ? colInc('Estado Civil') : colInc('Civil'),
  };
}

function extrairDados(r, idx) {
  const cracha = limpar(r[idx.idxCracha]);

  let loja_id = null;
  if (idx.idxLojaId !== -1 && r[idx.idxLojaId] != null) {
    loja_id = parseInt(r[idx.idxLojaId]) || null;
  } else if (idx.idxEmpresa !== -1) {
    const emp = limpar(r[idx.idxEmpresa]);
    loja_id = emp ? parseInt(emp) || null : null;
  }

  return {
    matricula:          cracha,
    nome:               limpar(r[idx.idxNome]),
    cargo:              limpar(r[idx.idxCargo]),
    grupo_cargo:        limpar(r[idx.idxGrupo]),
    nivel:              limpar(r[idx.idxNivel]),
    loja_id,
    status:             limpar(r[idx.idxStatus]) || 'ATIVO',
    data_admissao:      toDate(r[idx.idxAdm]),
    data_demissao:      toDate(r[idx.idxDem]),
    data_nascimento:    toDate(r[idx.idxNasc]),
    sexo:               mapSexo(r[idx.idxSexo]),
    escolaridade:       limpar(r[idx.idxEscol]),
    cpf:                limpar(r[idx.idxCpf]),
    salario:            r[idx.idxSalario] != null ? parseFloat(r[idx.idxSalario]) || null : null,
    raca:               limpar(r[idx.idxRaca]),
    estado_civil:       limpar(r[idx.idxEstCivil]),
    causa_afastamento:  limpar(r[idx.idxCausa]),
    motivo_afastamento: limpar(r[idx.idxMotivo]),
  };
}

// ── PREVIEW ───────────────────────────────────────────────────────
router.post('/qlp/preview', autenticar, upload.single('arquivo'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ erro: 'Arquivo obrigatório' });

    const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
    const sheetName = detectarSheet(wb.SheetNames);
    const ws = wb.Sheets[sheetName];
    const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });
    const headers = rows[0] || [];
    const dataRows = rows.slice(1).filter(r => r.some(c => c !== null));
    const idx = mapearColunas(headers);

    const funcionarios = dataRows
      .map(r => extrairDados(r, idx))
      .filter(f => f.nome && f.matricula);

    res.json({ sheet: sheetName, total: funcionarios.length, amostra: funcionarios.slice(0, 5) });
  } catch (err) {
    res.status(500).json({ erro: err.message });
  }
});

// ── IMPORTAR ─────────────────────────────────────────────────────
router.post('/qlp', autenticar, upload.single('arquivo'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ erro: 'Arquivo obrigatório' });

    const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
    const sheetName = detectarSheet(wb.SheetNames);
    const ws = wb.Sheets[sheetName];
    const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });
    const headers = rows[0] || [];
    const dataRows = rows.slice(1).filter(r => r.some(c => c !== null));
    const idx = mapearColunas(headers);

    let inseridos = 0, atualizados = 0, ignorados = 0;

    for (const r of dataRows) {
      const d = extrairDados(r, idx);
      if (!d.nome || !d.matricula) { ignorados++; continue; }

      const existing = await pool.query('SELECT id FROM funcionarios WHERE matricula = $1', [d.matricula]);

      if (existing.length) {
        await pool.query(`
          UPDATE funcionarios SET
            nome=$1, cargo=$2, grupo_cargo=$3, nivel=$4, loja_id=$5, status=$6,
            data_admissao=$7, data_demissao=$8, data_nascimento=$9, sexo=$10,
            escolaridade=$11, cpf=$12, raca=$13, estado_civil=$14,
            causa_afastamento=$15, motivo_afastamento=$16, salario=$17, atualizado_em=NOW()
          WHERE matricula=$18
        `, [d.nome, d.cargo, d.grupo_cargo, d.nivel, d.loja_id, d.status,
            d.data_admissao, d.data_demissao, d.data_nascimento, d.sexo,
            d.escolaridade, d.cpf, d.raca, d.estado_civil,
            d.causa_afastamento, d.motivo_afastamento, d.salario, d.matricula]);
        atualizados++;
      } else {
        await pool.query(`
          INSERT INTO funcionarios
            (matricula, nome, cargo, grupo_cargo, nivel, loja_id, status,
             data_admissao, data_demissao, data_nascimento, sexo,
             escolaridade, cpf, raca, estado_civil, causa_afastamento, motivo_afastamento, salario)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
        `, [d.matricula, d.nome, d.cargo, d.grupo_cargo, d.nivel, d.loja_id, d.status,
            d.data_admissao, d.data_demissao, d.data_nascimento, d.sexo,
            d.escolaridade, d.cpf, d.raca, d.estado_civil,
            d.causa_afastamento, d.motivo_afastamento, d.salario]);
        inseridos++;
      }
    }

    res.json({ ok: true, inseridos, atualizados, ignorados, sheet: sheetName });
  } catch (err) {
    console.error('QLP import error:', err);
    res.status(500).json({ erro: err.message });
  }
});

module.exports = router;
