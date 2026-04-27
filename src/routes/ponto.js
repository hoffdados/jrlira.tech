const express = require('express');
const router = express.Router();
const multer = require('multer');
const cheerio = require('cheerio');
const pool = require('../db');
const { autenticar, apenasAdmin } = require('../auth');

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 50 * 1024 * 1024 } });

// ── PARSER HTM iPonto ─────────────────────────────────────────────
function parseHoraInterval(str) {
  if (!str || !str.trim()) return null;
  const m = str.trim().match(/^(\d{1,3}):(\d{2})$/);
  if (!m) return null;
  return `${m[1]}:${m[2]}:00`;
}

function parseTime(str) {
  if (!str || !str.trim()) return null;
  const s = str.trim();
  if (s === 'DSR' || s === 'Folga' || s === 'Falta' || s === '') return null;
  const m = s.match(/^(\d{1,2}):(\d{2})$/);
  if (!m) return null;
  return s;
}

function parsePontoHTM(html, prefixo) {
  const $ = cheerio.load(html);
  const funcionarios = [];

  // Cada funcionário é separado por uma tabela de cabeçalho
  // Busca todas as células com "Crachá:"
  const crachaLabels = $('td, span, div').filter((i, el) => {
    const txt = $(el).text().trim();
    return txt === 'Crachá:' || txt === 'Cracha:';
  });

  crachaLabels.each((idx, el) => {
    try {
      // O valor do crachá está na célula seguinte
      const crachaEl = $(el).next('td, span');
      const cracha = crachaEl.text().trim() || $(el).parent().next('td').text().trim();
      if (!cracha || !/^\d+$/.test(cracha.replace(/\s/g, ''))) return;

      const matricula = `${prefixo}-${cracha.trim()}`;

      // Navega pela estrutura para encontrar Nome
      let nome = '';
      let cargo = '';
      let admissao = null;

      // Procura no contexto próximo
      const context = $(el).closest('table').find('td');
      context.each((i, td) => {
        const t = $(td).text().trim();
        if (t === 'Nome:') nome = $(td).next('td').text().trim();
        if (t === 'Cargo:') cargo = $(td).next('td').text().trim();
        if (t === 'Admissão:') {
          const admStr = $(td).next('td').text().trim();
          if (admStr.match(/\d{2}\/\d{2}\/\d{4}/)) {
            const [d, mo, y] = admStr.split('/');
            admissao = `${y}-${mo}-${d}`;
          }
        }
      });

      // Linhas de ponto: células com padrão DD/MM + dia semana
      const registros = [];
      const dataPattern = /^(\d{2})\/(\d{2})\s*([a-záéíóúüãõ]{3})$/i;

      $('td, span').filter((i, td) => dataPattern.test($(td).text().trim())).each((i, tdData) => {
        const txt = $(tdData).text().trim();
        const m = txt.match(dataPattern);
        if (!m) return;

        const row = $(tdData).closest('tr');
        const cells = row.find('td, span').map((j, c) => $(c).text().trim()).get();

        // Encontra posição da célula de data no array de células
        let dataIdx = -1;
        cells.forEach((c, j) => { if (c === txt) dataIdx = j; });
        if (dataIdx < 0) return;

        const remaining = cells.slice(dataIdx + 1).filter(c => c !== '');

        // Tab é o primeiro valor após a data (3 dígitos ou "DSR"/"Folga")
        const tab = remaining[0] || '';
        const times = remaining.slice(1);

        const is_dsr = times.some(t => t === 'DSR');
        const is_folga = times.some(t => t === 'Folga');
        const sem_marcacao = tab === '000' && times.filter(t => /^\d{1,2}:\d{2}$/.test(t)).length === 0;

        const timePairs = times.filter(t => /^\d{1,2}:\d{2}$/.test(t));
        // Últimas entradas podem ser H.Trab e extras — h_trab é o último valor de horas do formato HH:MM que excede 04:00
        let h_trab = null;
        let h_extra = null;
        const punchTimes = [];

        for (const t of timePairs) {
          const [hh] = t.split(':').map(Number);
          // Marcações de ponto são tipicamente entre 04:00-23:59
          // H.Trab e extras são acumulados (podem ser 07:20, 00:29 etc.)
          // Estratégia: os últimos 1-2 valores após as batidas são totais
          punchTimes.push(t);
        }

        // Os 2 últimos valores numéricos (se existirem) podem ser H.Trab e H.Extra
        // Heurística: se tivermos >2 valores e os primeiros são pares ent/sai, os extras vêm no fim
        // Pares de batida são identificados como valores que alternam ent/sai
        // Usamos todos os valores e pegamos os pares iniciais como batidas
        const [ent1, sai1, ent2, sai2, ent3, sai3, ent4, sai4, ent5, sai5, ...extras] = punchTimes;

        // H.Trab: primeiro valor em extras que for ≥ '01:00' (horas trabalhadas no dia)
        if (extras.length > 0) h_trab = extras[0];
        if (extras.length > 1) h_extra = extras[1];

        // Ano do período: será passado junto com a importação
        registros.push({
          dia: m[1], mes: m[2],
          tab: tab || null,
          ent1: parseTime(ent1), sai1: parseTime(sai1),
          ent2: parseTime(ent2), sai2: parseTime(sai2),
          ent3: parseTime(ent3), sai3: parseTime(sai3),
          ent4: parseTime(ent4), sai4: parseTime(sai4),
          ent5: parseTime(ent5), sai5: parseTime(sai5),
          h_trab: parseHoraInterval(h_trab),
          h_extra: parseHoraInterval(h_extra),
          is_dsr, is_folga, sem_marcacao
        });
      });

      if (registros.length > 0) {
        funcionarios.push({ matricula, cracha: cracha.trim(), nome, cargo, admissao, registros });
      }
    } catch (e) {
      // Ignora erros em funcionários individuais
    }
  });

  return funcionarios;
}

// ── IMPORTAR HTM ──────────────────────────────────────────────────
router.post('/importar', autenticar, upload.single('arquivo'), async (req, res) => {
  try {
    const { prefixo, periodo_inicio, periodo_fim } = req.body;
    if (!req.file) return res.status(400).json({ erro: 'Arquivo obrigatório' });
    if (!prefixo) return res.status(400).json({ erro: 'Prefixo da loja obrigatório' });
    if (!periodo_inicio || !periodo_fim) return res.status(400).json({ erro: 'Período obrigatório' });

    const html = req.file.buffer.toString('latin1');
    const funcionarios = parsePontoHTM(html, prefixo);

    if (funcionarios.length === 0) {
      return res.status(400).json({ erro: 'Nenhum funcionário encontrado no arquivo' });
    }

    // Calcula ano a partir do periodo_inicio
    const ano = periodo_inicio.split('-')[0];

    // Cria importação
    const impRows = await pool.query(`
      INSERT INTO ponto_importacoes (nome_arquivo, prefixo_loja, periodo_inicio, periodo_fim, total_funcionarios, importado_por)
      VALUES ($1, $2, $3, $4, $5, $6) RETURNING id
    `, [req.file.originalname, prefixo, periodo_inicio, periodo_fim, funcionarios.length, req.usuario.nome]);
    const importacao_id = impRows[0].id;

    let total_registros = 0;
    const nao_encontrados = [];

    for (const func of funcionarios) {
      // Tenta vincular ao funcionário cadastrado
      const fRows = await pool.query('SELECT id FROM funcionarios WHERE matricula = $1', [func.matricula]);
      const funcionario_id = fRows.length ? fRows[0].id : null;
      if (!funcionario_id) nao_encontrados.push(func.matricula);

      for (const r of func.registros) {
        const data = `${ano}-${r.mes.padStart(2,'0')}-${r.dia.padStart(2,'0')}`;
        await pool.query(`
          INSERT INTO ponto_registros
            (importacao_id, matricula, funcionario_id, data, tab,
             ent1, sai1, ent2, sai2, ent3, sai3, ent4, sai4, ent5, sai5,
             h_trab, h_extra, is_dsr, is_folga, sem_marcacao)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
          ON CONFLICT (matricula, data, importacao_id) DO NOTHING
        `, [importacao_id, func.matricula, funcionario_id, data, r.tab,
            r.ent1, r.sai1, r.ent2, r.sai2, r.ent3, r.sai3, r.ent4, r.sai4, r.ent5, r.sai5,
            r.h_trab, r.h_extra, r.is_dsr, r.is_folga, r.sem_marcacao]);
        total_registros++;
      }
    }

    await pool.query('UPDATE ponto_importacoes SET total_registros = $1 WHERE id = $2', [total_registros, importacao_id]);

    res.json({
      ok: true,
      importacao_id,
      total_funcionarios: funcionarios.length,
      total_registros,
      nao_encontrados
    });
  } catch (err) {
    console.error('Ponto importar error:', err);
    res.status(500).json({ erro: err.message });
  }
});

// ── LISTAR IMPORTAÇÕES ────────────────────────────────────────────
router.get('/importacoes', autenticar, async (req, res) => {
  try {
    const rows = await pool.query('SELECT * FROM ponto_importacoes ORDER BY importado_em DESC LIMIT 50');
    res.json(rows);
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// ── REGISTROS POR MATRÍCULA ───────────────────────────────────────
router.get('/registros/:matricula', autenticar, async (req, res) => {
  try {
    const { importacao_id } = req.query;
    const params = [req.params.matricula];
    let extra = '';
    if (importacao_id) { params.push(importacao_id); extra = ` AND importacao_id = $${params.length}`; }
    const rows = await pool.query(
      `SELECT * FROM ponto_registros WHERE matricula = $1${extra} ORDER BY data`,
      params
    );
    res.json(rows);
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// ── RESUMO POR IMPORTAÇÃO ─────────────────────────────────────────
router.get('/resumo/:importacao_id', autenticar, async (req, res) => {
  try {
    const rows = await pool.query(`
      SELECT
        pr.matricula,
        f.nome,
        COUNT(*) FILTER (WHERE NOT is_dsr AND NOT is_folga AND NOT sem_marcacao) as dias_trab,
        COUNT(*) FILTER (WHERE is_folga) as folgas,
        COUNT(*) FILTER (WHERE is_dsr) as dsrs,
        COUNT(*) FILTER (WHERE sem_marcacao AND NOT is_dsr AND NOT is_folga) as sem_marcacao
      FROM ponto_registros pr
      LEFT JOIN funcionarios f ON f.matricula = pr.matricula
      WHERE pr.importacao_id = $1
      GROUP BY pr.matricula, f.nome
      ORDER BY f.nome NULLS LAST
    `, [req.params.importacao_id]);
    res.json(rows);
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

module.exports = router;
