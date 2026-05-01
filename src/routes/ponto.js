const express = require('express');
const router = express.Router();
const multer = require('multer');
const cheerio = require('cheerio');
const pool = require('../db');
const { autenticar } = require('../auth');

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

function extrairPeriodo(html) {
  // Strip tags e &nbsp; antes de aplicar regex
  const text = html.replace(/<[^>]+>/g, ' ').replace(/&nbsp;/gi, ' ').replace(/\s+/g, ' ');
  const m = text.match(/Per.{1,30}ncia[^]*?(\d{2})\/(\d{2})\/(\d{4})[^]*?(\d{2})\/(\d{2})\/(\d{4})/i);
  if (!m) return { periodo_inicio: null, periodo_fim: null };
  return {
    periodo_inicio: `${m[3]}-${m[2]}-${m[1]}`,
    periodo_fim:    `${m[6]}-${m[5]}-${m[4]}`
  };
}

function parsePontoHTM(html, prefixo) {
  const $ = cheerio.load(html);
  const PAGE_H = 1056;

  // Extrai todos os divs posicionados com top > 0 (não-aninhados)
  const items = [];
  $('div[style]').each((i, el) => {
    const style = $(el).attr('style') || '';
    const tm = style.match(/top:\s*(\d+)px/);
    const lm = style.match(/left:\s*(\d+)px/);
    if (!tm || !lm) return;
    const top = parseInt(tm[1]);
    const left = parseInt(lm[1]);
    if (top === 0) return;
    const text = $(el).text().replace(/[ \s]+/g, ' ').trim();
    if (text) {
      items.push({ top, left, page: Math.floor(top / PAGE_H), rel: top % PAGE_H, text });
    }
  });

  // Agrupa por página
  const pages = {};
  for (const item of items) {
    if (!pages[item.page]) pages[item.page] = [];
    pages[item.page].push(item);
  }

  // Busca valor na página por posição relativa
  const at = (pageItems, relTop, leftMin, leftMax, tol = 6) =>
    (pageItems.find(i => Math.abs(i.rel - relTop) <= tol && i.left >= leftMin && i.left < leftMax) || {}).text || null;

  const funcionarios = [];

  for (const pageItems of Object.values(pages)) {
    const cracha = at(pageItems, 149, 95, 145);
    if (!cracha || !/^\d+$/.test(cracha.trim())) continue;

    const matricula = `${prefixo}-${cracha.trim()}`;
    const nome = at(pageItems, 168, 95, 450) || '';
    const cargo = at(pageItems, 187, 100, 450) || '';
    const admStr = at(pageItems, 282, 100, 180);
    let admissao = null;
    if (admStr && /\d{2}\/\d{2}\/\d{4}/.test(admStr)) {
      const [d, m, y] = admStr.split('/');
      admissao = `${y}-${m}-${d}`;
    }

    const registros = [];
    // Linhas de dados: rel 349 a 730, step 14px
    for (let rel = 349; rel <= 730; rel += 14) {
      const dateStr = at(pageItems, rel, 30, 65);
      if (!dateStr || !/^\d{2}\/\d{2}$/.test(dateStr)) continue;

      const [dia, mes] = dateStr.split('/');
      const tab = at(pageItems, rel, 80, 108);
      const ent1raw = at(pageItems, rel, 105, 143);

      const is_dsr   = ent1raw === 'DSR';
      const is_folga = ent1raw === 'FOLGA' || ent1raw === 'Folga';
      const sai1  = at(pageItems, rel, 143, 178);
      const ent2  = at(pageItems, rel, 178, 213);
      const sai2  = at(pageItems, rel, 213, 248);
      const ent3  = at(pageItems, rel, 248, 283);
      const sai3  = at(pageItems, rel, 283, 318);
      const ent4  = at(pageItems, rel, 318, 353);
      const sai4  = at(pageItems, rel, 353, 388);
      const ent5  = at(pageItems, rel, 388, 423);
      const sai5  = at(pageItems, rel, 423, 458);
      const h_trab  = at(pageItems, rel, 458, 495);
      const h_extra = at(pageItems, rel, 495, 535);

      const sem_marcacao = !is_dsr && !is_folga && !ent1raw;

      registros.push({
        dia, mes, tab,
        ent1: parseTime(is_dsr || is_folga ? null : ent1raw),
        sai1: parseTime(sai1), ent2: parseTime(ent2), sai2: parseTime(sai2),
        ent3: parseTime(ent3), sai3: parseTime(sai3),
        ent4: parseTime(ent4), sai4: parseTime(sai4),
        ent5: parseTime(ent5), sai5: parseTime(sai5),
        h_trab: parseHoraInterval(h_trab),
        h_extra: parseHoraInterval(h_extra),
        is_dsr, is_folga, sem_marcacao
      });
    }

    if (registros.length > 0) {
      funcionarios.push({ matricula, cracha: cracha.trim(), nome, cargo, admissao, registros });
    }
  }

  return funcionarios;
}

// ── IMPORTAR HTM ──────────────────────────────────────────────────
router.post('/importar', autenticar, upload.single('arquivo'), async (req, res) => {
  try {
    const { prefixo } = req.body;
    if (!req.file) return res.status(400).json({ erro: 'Arquivo obrigatório' });
    if (!prefixo) return res.status(400).json({ erro: 'Prefixo da loja obrigatório' });

    const html = req.file.buffer.toString('latin1');

    // Extrai período do próprio arquivo; aceita override do body como fallback
    const periodoArquivo = extrairPeriodo(html);
    const periodo_inicio = periodoArquivo.periodo_inicio || req.body.periodo_inicio;
    const periodo_fim    = periodoArquivo.periodo_fim    || req.body.periodo_fim;
    if (!periodo_inicio || !periodo_fim) return res.status(400).json({ erro: 'Período não encontrado no arquivo e não informado' });

    const funcionarios = parsePontoHTM(html, prefixo);

    if (funcionarios.length === 0) {
      return res.status(400).json({ erro: 'Nenhum funcionário encontrado no arquivo' });
    }

    // Calcula ano a partir do periodo_inicio
    const ano = periodo_inicio.split('-')[0];

    // Cria importação
    const loja_id_imp = req.usuario.lojas?.length === 1 ? req.usuario.lojas[0] : null;
    const impRows = await pool.query(`
      INSERT INTO ponto_importacoes (nome_arquivo, prefixo_loja, periodo_inicio, periodo_fim, total_funcionarios, importado_por, loja_id)
      VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id
    `, [req.file.originalname, prefixo, periodo_inicio, periodo_fim, funcionarios.length, req.usuario.nome, loja_id_imp]);
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
    const { perfil, lojas } = req.usuario;
    let q = 'SELECT * FROM ponto_importacoes';
    let params = [];
    if (perfil === 'rh' && lojas?.length) {
      params.push(lojas.map(Number).filter(Boolean));
      q += ` WHERE loja_id = ANY($1)`;
    }
    q += ' ORDER BY importado_em DESC LIMIT 50';
    const rows = await pool.query(q, params);
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
