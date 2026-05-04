const express = require('express');
const router = express.Router();
const multer = require('multer');
const XLSX = require('xlsx');
const { pool, query: dbQuery } = require('../db');
const { autenticar } = require('../auth');

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

// Mapeia nome da empresa (coluna "Empresa" da QLP) para loja_id
const EMPRESA_PARA_LOJA = {
  'SUPERASA ECONOMICO': 1,
  'SUPERASA BR': 2,
  'SUPERASA JOAO PESSOA': 3,
  'SUPERASA FLORESTA': 4,
  'SUPERASA SAO JOSE': 5,
  'SUPERASA SANTAREM': 6,
};
function empresaToLojaId(s) {
  const n = String(s || '').normalize('NFD').replace(/\p{Mn}/gu, '').trim().toUpperCase();
  if (EMPRESA_PARA_LOJA[n]) return EMPRESA_PARA_LOJA[n];
  const i = parseInt(n);
  return Number.isFinite(i) && i > 0 ? i : null;
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
    idxTerceirizada: col('terceirizada') !== -1 ? col('terceirizada') : colInc('Terceiriz'),
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
    loja_id = empresaToLojaId(r[idx.idxEmpresa]);
  }

  return {
    matricula:          cracha,
    nome:               limpar(r[idx.idxNome]),
    cargo:              limpar(r[idx.idxCargo]),
    grupo_cargo:        limpar(r[idx.idxGrupo]),
    nivel:              limpar(r[idx.idxNivel]),
    loja_id,
    terceirizada:       idx.idxTerceirizada !== -1 ? limpar(r[idx.idxTerceirizada]) : null,
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

    res.json({ sheet: sheetName, total: funcionarios.length, amostra: funcionarios });
  } catch (err) {
    console.error('[qlp preview]', err.message);
    res.status(500).json({ erro: 'Erro ao processar planilha' });
  }
});

// ── IMPORTAR (bulk upsert via UNNEST) ────────────────────────────
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

    const validos = [];
    let ignorados = 0;
    for (const r of dataRows) {
      const d = extrairDados(r, idx);
      if (!d.nome || !d.matricula) { ignorados++; continue; }
      validos.push(d);
    }

    if (!validos.length) {
      return res.json({ ok: true, inseridos: 0, atualizados: 0, ignorados, sheet: sheetName });
    }

    const cols = ['matricula','nome','cargo','grupo_cargo','nivel','loja_id','status',
                  'data_admissao','data_demissao','data_nascimento','sexo','escolaridade',
                  'cpf','raca','estado_civil','causa_afastamento','motivo_afastamento',
                  'salario','terceirizada'];

    // Upsert em lotes de 500 para evitar param limits
    const CHUNK = 500;
    let inseridos = 0, atualizados = 0;

    for (let i = 0; i < validos.length; i += CHUNK) {
      const chunk = validos.slice(i, i + CHUNK);
      const arr = (k) => chunk.map(d => d[k]);

      // xmax=0 indica INSERT; xmax<>0 indica UPDATE (linha pré-existia)
      const sql = `
        INSERT INTO funcionarios
          (matricula, nome, cargo, grupo_cargo, nivel, loja_id, status,
           data_admissao, data_demissao, data_nascimento, sexo, escolaridade,
           cpf, raca, estado_civil, causa_afastamento, motivo_afastamento,
           salario, terceirizada)
        SELECT * FROM UNNEST(
          $1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::int[], $7::text[],
          $8::date[], $9::date[], $10::date[], $11::text[], $12::text[],
          $13::text[], $14::text[], $15::text[], $16::text[], $17::text[],
          $18::numeric[], $19::text[]
        )
        ON CONFLICT (matricula) DO UPDATE SET
          nome=EXCLUDED.nome, cargo=EXCLUDED.cargo, grupo_cargo=EXCLUDED.grupo_cargo,
          nivel=EXCLUDED.nivel, loja_id=EXCLUDED.loja_id, status=EXCLUDED.status,
          data_admissao=EXCLUDED.data_admissao, data_demissao=EXCLUDED.data_demissao,
          data_nascimento=EXCLUDED.data_nascimento, sexo=EXCLUDED.sexo,
          escolaridade=EXCLUDED.escolaridade, cpf=EXCLUDED.cpf, raca=EXCLUDED.raca,
          estado_civil=EXCLUDED.estado_civil, causa_afastamento=EXCLUDED.causa_afastamento,
          motivo_afastamento=EXCLUDED.motivo_afastamento, salario=EXCLUDED.salario,
          terceirizada=EXCLUDED.terceirizada, atualizado_em=NOW()
        RETURNING (xmax = 0) AS inserted
      `;
      const params = cols.map(c => arr(c));
      const { rows: ret } = await pool.query(sql, params);
      for (const r of ret) {
        if (r.inserted) inseridos++; else atualizados++;
      }
    }

    res.json({ ok: true, inseridos, atualizados, ignorados, sheet: sheetName });
  } catch (err) {
    console.error('[qlp] erro:', err.message);
    res.status(500).json({ erro: 'Erro ao importar QLP' });
  }
});

module.exports = router;
