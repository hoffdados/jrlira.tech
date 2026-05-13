// Auditoria EAN canônico — detecta inconsistências entre mercado e CDs.
// Premissa: EAN do produtos_externo (mercado) é a chave canônica universal.
//
// Tipos detectados:
//   A — Produto vende no mercado mas EAN não existe em nenhum CD (sem fornecedor)
//   B — Produto tem mat_codi em alguns CDs e falta em outros (divergência de cadastro)
//   C — Mesmo EAN apontando pra múltiplos mat_codi no MESMO CD (fardos sem amarração canônica)
//   D — mat_codi de CD não usado por nenhum mercado e fora de produto_canonico_match (órfão)
//
// Filtro padrão: produtos com venda nos últimos 90 dias (reduz ruído).

const express = require('express');
const router = express.Router();
const { query: dbQuery } = require('../db');
const { autenticar } = require('../auth');

// Retorna { totais, amostras: { A, B, C, D } }
async function detectar({ janelaDias = 90, limiteAmostra = 50 } = {}) {
  const t0 = Date.now();

  // Universo: subquery simples reutilizada como SELECT
  const universo = `
    SELECT DISTINCT NULLIF(LTRIM(codigobarra,'0'),'') AS ean
      FROM vendas_historico
     WHERE data_venda >= CURRENT_DATE - INTERVAL '${janelaDias} days'
       AND codigobarra IS NOT NULL
       AND COALESCE(tipo_saida,'venda') = 'venda'`;

  // CDs ativos
  const cdsAtivos = await dbQuery(`SELECT codigo FROM cds WHERE ativo ORDER BY codigo`);
  const totalCds = cdsAtivos.length;

  // ── TIPO A: EAN no mercado, em NENHUM CD ─────────────────────────
  const tipoA = await dbQuery(`
    SELECT v.ean,
           (SELECT pe.descricao FROM produtos_externo pe
             WHERE NULLIF(LTRIM(pe.codigobarra,'0'),'') = v.ean LIMIT 1) AS descricao,
           (SELECT SUM(qtd_vendida)::numeric(14,2) FROM vendas_historico vh
             WHERE NULLIF(LTRIM(vh.codigobarra,'0'),'') = v.ean
               AND data_venda >= CURRENT_DATE - INTERVAL '${janelaDias} days') AS qtd_vendida
      FROM (${universo}) v
     WHERE v.ean IS NOT NULL AND LENGTH(v.ean) >= 6
       AND NOT EXISTS (
         SELECT 1 FROM cd_ean ce
          WHERE NULLIF(LTRIM(ce.ean_codi,'0'),'') = v.ean
       )
     ORDER BY qtd_vendida DESC NULLS LAST
     LIMIT ${limiteAmostra}`);

  const tipoAContagem = await dbQuery(`
    SELECT COUNT(*)::int AS qtd FROM (${universo}) v
     WHERE v.ean IS NOT NULL AND LENGTH(v.ean) >= 6
       AND NOT EXISTS (
         SELECT 1 FROM cd_ean ce
          WHERE NULLIF(LTRIM(ce.ean_codi,'0'),'') = v.ean
       )`);

  // ── TIPO B: em ALGUNS CDs, faltando em outros ────────────────────
  const tipoB = await dbQuery(`
    WITH presenca AS (
      SELECT v.ean,
             ARRAY_AGG(DISTINCT ce.cd_codigo ORDER BY ce.cd_codigo) FILTER (WHERE ce.cd_codigo IS NOT NULL) AS cds_com,
             COUNT(DISTINCT ce.cd_codigo) FILTER (WHERE ce.cd_codigo IS NOT NULL) AS qtd_cds
        FROM (${universo}) v
        LEFT JOIN cd_ean ce ON NULLIF(LTRIM(ce.ean_codi,'0'),'') = v.ean
       WHERE v.ean IS NOT NULL AND LENGTH(v.ean) >= 6
       GROUP BY v.ean
    )
    SELECT p.ean, p.cds_com, p.qtd_cds,
           (SELECT pe.descricao FROM produtos_externo pe
             WHERE NULLIF(LTRIM(pe.codigobarra,'0'),'') = p.ean LIMIT 1) AS descricao,
           (SELECT SUM(qtd_vendida)::numeric(14,2) FROM vendas_historico vh
             WHERE NULLIF(LTRIM(vh.codigobarra,'0'),'') = p.ean
               AND data_venda >= CURRENT_DATE - INTERVAL '${janelaDias} days') AS qtd_vendida
      FROM presenca p
     WHERE p.qtd_cds BETWEEN 1 AND ${totalCds - 1}
     ORDER BY qtd_vendida DESC NULLS LAST
     LIMIT ${limiteAmostra}`);

  const tipoBContagem = await dbQuery(`
    WITH presenca AS (
      SELECT v.ean, COUNT(DISTINCT ce.cd_codigo) FILTER (WHERE ce.cd_codigo IS NOT NULL) AS qtd_cds
        FROM (${universo}) v
        LEFT JOIN cd_ean ce ON NULLIF(LTRIM(ce.ean_codi,'0'),'') = v.ean
       WHERE v.ean IS NOT NULL AND LENGTH(v.ean) >= 6
       GROUP BY v.ean
    )
    SELECT COUNT(*)::int AS qtd FROM presenca WHERE qtd_cds BETWEEN 1 AND ${totalCds - 1}`);

  // ── TIPO C: mesmo EAN com múltiplos mat_codi NO MESMO CD ──────────
  const tipoC = await dbQuery(`
    WITH multi AS (
      SELECT ce.cd_codigo, NULLIF(LTRIM(ce.ean_codi,'0'),'') AS ean,
             COUNT(DISTINCT ce.mat_codi) AS qtd_mat,
             ARRAY_AGG(DISTINCT ce.mat_codi ORDER BY ce.mat_codi) AS mat_codis
        FROM cd_ean ce
       WHERE ce.ean_codi IS NOT NULL AND LTRIM(RTRIM(ce.ean_codi)) <> ''
       GROUP BY ce.cd_codigo, NULLIF(LTRIM(ce.ean_codi,'0'),'')
      HAVING COUNT(DISTINCT ce.mat_codi) > 1
    )
    SELECT m.cd_codigo, m.ean, m.qtd_mat, m.mat_codis,
           (SELECT pe.descricao FROM produtos_externo pe
             WHERE NULLIF(LTRIM(pe.codigobarra,'0'),'') = m.ean LIMIT 1) AS descricao,
           EXISTS (
             SELECT 1 FROM produto_canonico_match pcm
              WHERE pcm.cd_codigo = m.cd_codigo
                AND pcm.mat_codi = ANY(m.mat_codis)
           ) AS tem_canonico
      FROM multi m
     ORDER BY m.qtd_mat DESC
     LIMIT ${limiteAmostra}`);

  const tipoCContagem = await dbQuery(`
    SELECT COUNT(*)::int AS qtd
      FROM (
        SELECT cd_codigo, NULLIF(LTRIM(ean_codi,'0'),'') AS ean
          FROM cd_ean
         WHERE ean_codi IS NOT NULL AND LTRIM(RTRIM(ean_codi)) <> ''
         GROUP BY cd_codigo, NULLIF(LTRIM(ean_codi,'0'),'')
        HAVING COUNT(DISTINCT mat_codi) > 1
      ) x`);

  // ── TIPO D: mat_codi órfão ───────────────────────────────────────
  // mat_codi no CD cujos EANs não aparecem em nenhuma loja e fora de produto_canonico_match
  const tipoD = await dbQuery(`
    WITH mats AS (
      SELECT cm.cd_codigo, cm.mat_codi, cm.mat_desc
        FROM cd_material cm
    ),
    mats_com_ean_no_mercado AS (
      SELECT DISTINCT ce.cd_codigo, ce.mat_codi
        FROM cd_ean ce
        JOIN produtos_externo pe
          ON NULLIF(LTRIM(pe.codigobarra,'0'),'') = NULLIF(LTRIM(ce.ean_codi,'0'),'')
         AND NULLIF(LTRIM(ce.ean_codi,'0'),'') IS NOT NULL
    ),
    mats_canonicos AS (
      SELECT DISTINCT cd_codigo, mat_codi FROM produto_canonico_match
    )
    SELECT m.cd_codigo, m.mat_codi, m.mat_desc,
           (SELECT ARRAY_AGG(DISTINCT NULLIF(LTRIM(ean_codi,'0'),'')) FROM cd_ean ce
             WHERE ce.cd_codigo = m.cd_codigo AND ce.mat_codi = m.mat_codi) AS eans
      FROM mats m
     WHERE NOT EXISTS (SELECT 1 FROM mats_com_ean_no_mercado x
                       WHERE x.cd_codigo = m.cd_codigo AND x.mat_codi = m.mat_codi)
       AND NOT EXISTS (SELECT 1 FROM mats_canonicos x
                       WHERE x.cd_codigo = m.cd_codigo AND x.mat_codi = m.mat_codi)
     ORDER BY m.cd_codigo, m.mat_codi
     LIMIT ${limiteAmostra}`);

  const tipoDContagem = await dbQuery(`
    SELECT COUNT(*)::int AS qtd
      FROM cd_material m
     WHERE NOT EXISTS (
       SELECT 1 FROM cd_ean ce
        JOIN produtos_externo pe
          ON NULLIF(LTRIM(pe.codigobarra,'0'),'') = NULLIF(LTRIM(ce.ean_codi,'0'),'')
       WHERE ce.cd_codigo = m.cd_codigo AND ce.mat_codi = m.mat_codi
     ) AND NOT EXISTS (
       SELECT 1 FROM produto_canonico_match pcm
        WHERE pcm.cd_codigo = m.cd_codigo AND pcm.mat_codi = m.mat_codi
     )`);

  return {
    janela_dias: janelaDias,
    total_cds: totalCds,
    totais: {
      A: tipoAContagem[0]?.qtd || 0,
      B: tipoBContagem[0]?.qtd || 0,
      C: tipoCContagem[0]?.qtd || 0,
      D: tipoDContagem[0]?.qtd || 0,
    },
    amostras: { A: tipoA, B: tipoB, C: tipoC, D: tipoD },
    ms: Date.now() - t0,
  };
}

// JSON endpoint
router.get('/dados', autenticar, async (req, res) => {
  if (!['admin','ceo'].includes(req.usuario.perfil)) return res.status(403).json({ erro: 'apenas admin/ceo' });
  try {
    const r = await detectar({
      janelaDias: parseInt(req.query.dias || '90', 10),
      limiteAmostra: parseInt(req.query.limite || '50', 10),
    });
    res.json(r);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// HTML server-rendered (sem JS, sem auth — debug visual rápido)
router.get('/html', async (req, res) => {
  try {
    const r = await detectar({
      janelaDias: parseInt(req.query.dias || '90', 10),
      limiteAmostra: parseInt(req.query.limite || '30', 10),
    });

    const NOMES_CD = {
      'srv1-itautuba':'Asa Branca ITB', 'srv1-nprogresso':'N. Progresso',
      'srv2-asafrio':'Asa Frios ITB', 'srv2-asasantarem':'Asa Frios STM',
    };
    const cdNome = c => NOMES_CD[c] || c;
    const fmtN = v => Number(v||0).toLocaleString('pt-BR');

    const tabela = (titulo, descr, ths, rows) => `
      <h2 style="font-size:15px;margin:18px 0 6px">${titulo}</h2>
      <div class="aviso">${descr}</div>
      ${rows.length ? `<table>
        <thead><tr>${ths.map(h=>`<th>${h}</th>`).join('')}</tr></thead>
        <tbody>${rows.join('')}</tbody>
      </table>` : '<div class="empty">Nenhum caso amostrado.</div>'}
    `;

    const rowsA = r.amostras.A.map(x => `<tr>
      <td><code>${x.ean}</code></td>
      <td>${x.descricao || '<em>(sem cadastro no mercado)</em>'}</td>
      <td class="n">${fmtN(x.qtd_vendida)}</td>
    </tr>`);

    const rowsB = r.amostras.B.map(x => {
      const cdsCom = (x.cds_com||[]).map(cdNome).join(', ');
      return `<tr>
        <td><code>${x.ean}</code></td>
        <td>${x.descricao || '—'}</td>
        <td class="n">${x.qtd_cds}/${r.total_cds}</td>
        <td>${cdsCom}</td>
        <td class="n">${fmtN(x.qtd_vendida)}</td>
      </tr>`;
    });

    const rowsC = r.amostras.C.map(x => `<tr>
      <td>${cdNome(x.cd_codigo)}</td>
      <td><code>${x.ean}</code></td>
      <td>${x.descricao || '—'}</td>
      <td class="n">${x.qtd_mat}</td>
      <td><small>${(x.mat_codis||[]).join(', ')}</small></td>
      <td>${x.tem_canonico ? '✓ vinculado' : '<span class="alerta">sem vínculo</span>'}</td>
    </tr>`);

    const rowsD = r.amostras.D.map(x => `<tr>
      <td>${cdNome(x.cd_codigo)}</td>
      <td><code>${x.mat_codi}</code></td>
      <td>${(x.mat_desc||'').trim() || '—'}</td>
      <td><small>${(x.eans||[]).filter(Boolean).join(', ') || '<em>sem EAN</em>'}</small></td>
    </tr>`);

    res.set('Cache-Control', 'no-store');
    res.send(`<!doctype html><html><head><meta charset="utf-8"><title>Auditoria EAN canônico</title>
<style>
body{font-family:system-ui;background:#0f172a;color:#e2e8f0;margin:0;padding:20px;font-size:13px}
h1{font-size:20px;margin-bottom:4px}
.sub{color:#94a3b8;font-size:12px;margin-bottom:12px}
.aviso{padding:8px 10px;background:#1e293b;border-left:3px solid #0ea5e9;font-size:12px;color:#94a3b8;margin:8px 0}
.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:12px 0}
.card{background:#1e293b;border:1px solid #334155;border-radius:8px;padding:12px;border-left:4px solid #334155}
.card.a{border-left-color:#ef4444}
.card.b{border-left-color:#fbbf24}
.card.c{border-left-color:#a855f7}
.card.d{border-left-color:#64748b}
.card h3{font-size:11px;color:#94a3b8;text-transform:uppercase;margin-bottom:6px}
.card .big{font-size:26px;font-weight:700;color:#fde047}
.card .desc{font-size:11px;color:#94a3b8;margin-top:4px;line-height:1.35}
table{width:100%;border-collapse:collapse;background:#1e293b;border-radius:6px;overflow:hidden;margin:6px 0 18px;font-size:12px}
th{background:#0c1628;padding:8px 10px;text-align:left;color:#94a3b8;font-size:10px;text-transform:uppercase}
td{padding:7px 10px;border-top:1px solid #334155}
td.n{text-align:right;font-variant-numeric:tabular-nums}
code{font-family:Consolas,monospace;font-size:11px;color:#fde047}
.empty{color:#64748b;padding:8px}
.alerta{color:#fca5a5;font-weight:700}
small{color:#64748b}
a{color:#38bdf8}
</style></head><body>
<h1>Auditoria EAN canônico</h1>
<div class="sub">Premissa: EAN do mercado é a chave. Universo: produtos com venda nos últimos <b>${r.janela_dias} dias</b>. <b>${r.total_cds} CDs ativos</b>. Processado em ${r.ms}ms. <small>${new Date().toLocaleString('pt-BR')}</small></div>

<div class="cards">
  <div class="card a"><h3>🅰 Sem fornecedor</h3><div class="big">${fmtN(r.totais.A)}</div><div class="desc">EAN vende no mercado mas não existe em nenhum CD</div></div>
  <div class="card b"><h3>🅱 Divergente entre CDs</h3><div class="big">${fmtN(r.totais.B)}</div><div class="desc">EAN aparece em alguns CDs, falta em outros</div></div>
  <div class="card c"><h3>🅲 Múltiplos mat_codi</h3><div class="big">${fmtN(r.totais.C)}</div><div class="desc">Mesmo EAN com 2+ mat_codi no MESMO CD (fardos)</div></div>
  <div class="card d"><h3>🅳 Órfãos</h3><div class="big">${fmtN(r.totais.D)}</div><div class="desc">mat_codi no CD não usado por nenhum mercado</div></div>
</div>

${tabela('🅰 Sem fornecedor — top vendidos (amostra ' + r.amostras.A.length + ')',
  'Esses EANs vendem no mercado mas <b>nenhum CD</b> tem cadastro deles. Ação: cadastrar no UltraSyst do CD que vai fornecer.',
  ['EAN','Descrição (mercado)','Qtd vendida 90d'], rowsA)}

${tabela('🅱 Divergente entre CDs — top vendidos (amostra ' + r.amostras.B.length + ')',
  'Esses EANs estão cadastrados em alguns CDs, faltam em outros. Causa comum: digitação errada (zero a mais, dígito trocado). Ação: corrigir o EAN no UltraSyst do CD ausente.',
  ['EAN','Descrição','CDs com','Quais CDs','Qtd vendida 90d'], rowsB)}

${tabela('🅲 Múltiplos mat_codi no mesmo CD (amostra ' + r.amostras.C.length + ')',
  'Mesmo EAN apontando pra 2+ mat_codi no MESMO CD (caso fardo 10× × fardo 30×). Se NÃO tiver vínculo canônico, app trata como produtos distintos. Ação: vincular via /produto-canonico.',
  ['CD','EAN','Descrição','Qtd mat_codi','Mat_codis','Canônico'], rowsC)}

${tabela('🅳 Órfãos — mat_codi sem uso no mercado (amostra ' + r.amostras.D.length + ')',
  'mat_codi cadastrado no CD cujos EANs não batem com nada vendido no mercado e não está em produto_canonico. Pode ser produto novo, descontinuado, ou cadastro errado.',
  ['CD','Mat_codi','Descrição CD','EANs'], rowsD)}

<p style="color:#64748b;font-size:11px;margin-top:20px">Parâmetros: ?dias=90&limite=30 — exemplo: <a href="?dias=30&limite=100">últimos 30 dias, top 100</a></p>
</body></html>`);
  } catch (e) { res.status(500).send('Erro: ' + e.message); }
});

module.exports = router;
