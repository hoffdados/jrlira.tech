const express = require('express');
const router = express.Router();
const multer = require('multer');
const XLSX = require('xlsx');
const pool = require('../db');
const { autenticar, apenasAdmin } = require('../auth');

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 20 * 1024 * 1024 } });

// Converte serial Excel para Date string YYYY-MM-DD
function excelDateToISO(serial) {
  if (!serial || isNaN(serial)) return null;
  const date = new Date(Math.round((serial - 25569) * 86400 * 1000));
  if (isNaN(date.getTime())) return null;
  return date.toISOString().split('T')[0];
}

function limpar(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s === '' ? null : s;
}

// ── PREVIEW ───────────────────────────────────────────────────────
router.post('/qlp/preview', autenticar, upload.single('arquivo'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ erro: 'Arquivo obrigatório' });

    const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
    const sheetNames = wb.SheetNames;

    // Usa aba "QLP (2)" se existir (ativos), senão "QLP", senão primeira
    const sheetName = sheetNames.find(n => n === 'QLP (2)') ||
                      sheetNames.find(n => n === 'QLP') ||
                      sheetNames[0];

    const ws = wb.Sheets[sheetName];
    const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });

    // Primeira linha = cabeçalho
    const headers = rows[0] || [];
    const dataRows = rows.slice(1).filter(r => r.some(c => c !== null));

    // Detecta índice das colunas pelo cabeçalho
    const col = (name) => headers.findIndex(h => h && String(h).toLowerCase().includes(name.toLowerCase()));

    const idxNome     = col('Nome');
    const idxAdm      = col('Admiss');
    const idxDem      = col('Demiss');
    const idxCargo    = col('Cargo') !== -1 ? col('Cargo') : col('Descri');
    const idxGrupo    = col('Grupo');
    const idxNivel    = col('Nivel');
    const idxEmpresa  = col('Empresa');
    const idxStatus   = col('Status');
    const idxNasc     = col('Nasc');
    const idxSexo     = col('Sexo');
    const idxEscol    = col('Escolar');
    const idxCpf      = col('CPF');
    const idxCracha   = col('Cracha') !== -1 ? col('Cracha') : col('Crachá');
    const idxSalario  = col('Salar');
    const idxCausa    = col('Causa');
    const idxMotivo   = col('Motivo');
    const idxRaca     = col('Raca') !== -1 ? col('Raca') : col('Raça');
    const idxEstCivil = col('Civil') !== -1 ? col('Civil') : col('Estado');

    const funcionarios = dataRows.map(r => {
      const cracha = limpar(r[idxCracha]);
      const empresaRaw = limpar(r[idxEmpresa]);
      const loja_id = empresaRaw ? parseInt(empresaRaw) || null : null;

      return {
        matricula: cracha,
        nome: limpar(r[idxNome]),
        cargo: limpar(r[idxCargo]),
        grupo_cargo: limpar(r[idxGrupo]),
        nivel: limpar(r[idxNivel]),
        loja_id,
        status: limpar(r[idxStatus]) || 'ATIVO',
        data_admissao: typeof r[idxAdm] === 'number' ? excelDateToISO(r[idxAdm]) : limpar(r[idxAdm]),
        data_demissao: typeof r[idxDem] === 'number' ? excelDateToISO(r[idxDem]) : limpar(r[idxDem]),
        data_nascimento: typeof r[idxNasc] === 'number' ? excelDateToISO(r[idxNasc]) : limpar(r[idxNasc]),
        sexo: limpar(r[idxSexo]),
        escolaridade: limpar(r[idxEscol]),
        cpf: limpar(r[idxCpf]),
        raca: limpar(r[idxRaca]),
        estado_civil: limpar(r[idxEstCivil]),
        causa_afastamento: limpar(r[idxCausa]),
        motivo_afastamento: limpar(r[idxMotivo]),
      };
    }).filter(f => f.nome && f.matricula);

    res.json({
      sheet: sheetName,
      total: funcionarios.length,
      amostra: funcionarios.slice(0, 5)
    });
  } catch (err) {
    res.status(500).json({ erro: err.message });
  }
});

// ── IMPORTAR ─────────────────────────────────────────────────────
router.post('/qlp', autenticar, upload.single('arquivo'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ erro: 'Arquivo obrigatório' });

    const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
    const sheetNames = wb.SheetNames;
    const sheetName = sheetNames.find(n => n === 'QLP (2)') ||
                      sheetNames.find(n => n === 'QLP') ||
                      sheetNames[0];

    const ws = wb.Sheets[sheetName];
    const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });
    const headers = rows[0] || [];
    const dataRows = rows.slice(1).filter(r => r.some(c => c !== null));

    const col = (name) => headers.findIndex(h => h && String(h).toLowerCase().includes(name.toLowerCase()));

    const idxNome     = col('Nome');
    const idxAdm      = col('Admiss');
    const idxDem      = col('Demiss');
    const idxCargo    = col('Cargo') !== -1 ? col('Cargo') : col('Descri');
    const idxGrupo    = col('Grupo');
    const idxNivel    = col('Nivel');
    const idxEmpresa  = col('Empresa');
    const idxStatus   = col('Status');
    const idxNasc     = col('Nasc');
    const idxSexo     = col('Sexo');
    const idxEscol    = col('Escolar');
    const idxCpf      = col('CPF');
    const idxCracha   = col('Cracha') !== -1 ? col('Cracha') : col('Crachá');
    const idxRaca     = col('Raca') !== -1 ? col('Raca') : col('Raça');
    const idxEstCivil = col('Civil') !== -1 ? col('Civil') : col('Estado');
    const idxCausa    = col('Causa');
    const idxMotivo   = col('Motivo');

    let inseridos = 0;
    let atualizados = 0;
    let ignorados = 0;

    for (const r of dataRows) {
      const cracha = limpar(r[idxCracha]);
      const nome   = limpar(r[idxNome]);
      if (!nome || !cracha) { ignorados++; continue; }

      const empresaRaw = limpar(r[idxEmpresa]);
      const loja_id = empresaRaw ? parseInt(empresaRaw) || null : null;

      const dados = {
        matricula:         cracha,
        nome,
        cargo:             limpar(r[idxCargo]),
        grupo_cargo:       limpar(r[idxGrupo]),
        nivel:             limpar(r[idxNivel]),
        loja_id,
        status:            limpar(r[idxStatus]) || 'ATIVO',
        data_admissao:     typeof r[idxAdm] === 'number' ? excelDateToISO(r[idxAdm]) : limpar(r[idxAdm]),
        data_demissao:     typeof r[idxDem] === 'number' ? excelDateToISO(r[idxDem]) : limpar(r[idxDem]),
        data_nascimento:   typeof r[idxNasc] === 'number' ? excelDateToISO(r[idxNasc]) : limpar(r[idxNasc]),
        sexo:              limpar(r[idxSexo]),
        escolaridade:      limpar(r[idxEscol]),
        cpf:               limpar(r[idxCpf]),
        raca:              limpar(r[idxRaca]),
        estado_civil:      limpar(r[idxEstCivil]),
        causa_afastamento: limpar(r[idxCausa]),
        motivo_afastamento:limpar(r[idxMotivo]),
      };

      const existing = await pool.query('SELECT id FROM funcionarios WHERE matricula = $1', [dados.matricula]);

      if (existing.length) {
        await pool.query(`
          UPDATE funcionarios SET
            nome=$1, cargo=$2, grupo_cargo=$3, nivel=$4, loja_id=$5, status=$6,
            data_admissao=$7, data_demissao=$8, data_nascimento=$9, sexo=$10,
            escolaridade=$11, cpf=$12, raca=$13, estado_civil=$14,
            causa_afastamento=$15, motivo_afastamento=$16, atualizado_em=NOW()
          WHERE matricula=$17
        `, [dados.nome, dados.cargo, dados.grupo_cargo, dados.nivel, dados.loja_id, dados.status,
            dados.data_admissao, dados.data_demissao, dados.data_nascimento, dados.sexo,
            dados.escolaridade, dados.cpf, dados.raca, dados.estado_civil,
            dados.causa_afastamento, dados.motivo_afastamento, dados.matricula]);
        atualizados++;
      } else {
        await pool.query(`
          INSERT INTO funcionarios
            (matricula, nome, cargo, grupo_cargo, nivel, loja_id, status,
             data_admissao, data_demissao, data_nascimento, sexo,
             escolaridade, cpf, raca, estado_civil, causa_afastamento, motivo_afastamento)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
        `, [dados.matricula, dados.nome, dados.cargo, dados.grupo_cargo, dados.nivel, dados.loja_id, dados.status,
            dados.data_admissao, dados.data_demissao, dados.data_nascimento, dados.sexo,
            dados.escolaridade, dados.cpf, dados.raca, dados.estado_civil,
            dados.causa_afastamento, dados.motivo_afastamento]);
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
