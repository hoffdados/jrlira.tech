const express = require('express');
const multer = require('multer');
const xlsx = require('xlsx');
const router = express.Router();
const { query: dbQuery } = require('../db');
const { autenticar } = require('../auth');

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 20 * 1024 * 1024 } });

function adminOuCeo(req, res, next) {
  const p = req.usuario?.perfil;
  if (p === 'admin' || p === 'ceo') return next();
  return res.status(403).json({ erro: 'Acesso negado' });
}

// Normaliza EAN: strip leading zeros, trim. Retorna null se vazio.
function normEan(v) {
  const s = String(v ?? '').trim().replace(/^0+/, '');
  return s || null;
}

// GET /api/cd-produtos-embalagem?cd_codigo=&busca=&incluir_sem_cadastro=1
// Quando incluir_sem_cadastro=1, retorna TODOS os mat_codi do cd_material (LEFT JOIN), não só os já cadastrados.
router.get('/', autenticar, async (req, res) => {
  try {
    const cdCodigo = String(req.query.cd_codigo || '').trim();
    const busca = String(req.query.busca || '').trim();
    const incluirSem = req.query.incluir_sem_cadastro === '1';
    if (incluirSem && !cdCodigo) return res.status(400).json({ erro: 'cd_codigo obrigatorio com incluir_sem_cadastro' });

    if (incluirSem) {
      const where = [`cm.cd_codigo = $1`, `cm.mat_situ = 'A'`];
      const params = [cdCodigo];
      if (busca) {
        params.push(`%${busca}%`);
        where.push(`(cm.mat_codi ILIKE $${params.length} OR cm.mat_desc ILIKE $${params.length}
                  OR cpe.ean_principal LIKE $${params.length} OR cpe.ean_secundario LIKE $${params.length})`);
      }
      const rows = await dbQuery(
        `SELECT cm.cd_codigo, cm.mat_codi, cm.mat_desc,
                cpe.ean_principal, cpe.ean_secundario, cpe.qtd_embalagem,
                cpe.peso_unidade_kg, cpe.peso_variavel,
                cpe.atualizado_em, cpe.atualizado_por,
                (cpe.mat_codi IS NOT NULL) AS tem_cadastro
           FROM cd_material cm
           LEFT JOIN cd_produtos_embalagem cpe
                  ON cpe.cd_codigo = cm.cd_codigo AND cpe.mat_codi = cm.mat_codi
          WHERE ${where.join(' AND ')}
          ORDER BY cm.mat_codi
          LIMIT 5000`,
        params
      );
      return res.json({ total: rows.length, itens: rows });
    }

    const where = ['1=1'];
    const params = [];
    if (cdCodigo) { params.push(cdCodigo); where.push(`cpe.cd_codigo = $${params.length}`); }
    if (busca) {
      params.push(`%${busca}%`);
      where.push(`(cpe.mat_codi ILIKE $${params.length} OR cpe.ean_principal LIKE $${params.length}
                OR cpe.ean_secundario LIKE $${params.length} OR cm.mat_desc ILIKE $${params.length})`);
    }
    const rows = await dbQuery(
      `SELECT cpe.cd_codigo, cpe.mat_codi, cpe.ean_principal, cpe.ean_secundario,
              cpe.qtd_embalagem, cpe.peso_unidade_kg, cpe.peso_variavel,
              cpe.atualizado_em, cpe.atualizado_por,
              cm.mat_desc, TRUE AS tem_cadastro
         FROM cd_produtos_embalagem cpe
         LEFT JOIN cd_material cm ON cm.cd_codigo = cpe.cd_codigo AND cm.mat_codi = cpe.mat_codi
        WHERE ${where.join(' AND ')}
        ORDER BY cpe.cd_codigo, cpe.mat_codi
        LIMIT 5000`,
      params
    );
    res.json({ total: rows.length, itens: rows });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/cd-produtos-embalagem — upsert manual de 1 linha
// body: { cd_codigo, mat_codi, ean_principal?, ean_secundario?, qtd_embalagem?, peso_unidade_kg?, peso_variavel? }
router.post('/', autenticar, adminOuCeo, async (req, res) => {
  try {
    const b = req.body || {};
    if (!b.cd_codigo || !b.mat_codi) return res.status(400).json({ erro: 'cd_codigo e mat_codi obrigatorios' });
    const por = req.usuario.email || req.usuario.usuario || req.usuario.nome || `id:${req.usuario.id}`;
    const qtd = b.qtd_embalagem != null ? parseFloat(b.qtd_embalagem) : 1;
    const pesoV = !!b.peso_variavel;
    const pesoKg = b.peso_unidade_kg != null ? parseFloat(b.peso_unidade_kg) : null;
    await dbQuery(
      `INSERT INTO cd_produtos_embalagem
         (cd_codigo, mat_codi, ean_principal, ean_secundario, qtd_embalagem,
          peso_unidade_kg, peso_variavel, atualizado_em, atualizado_por)
       VALUES ($1,$2,$3,$4,$5,$6,$7,NOW(),$8)
       ON CONFLICT (cd_codigo, mat_codi) DO UPDATE SET
         ean_principal=EXCLUDED.ean_principal,
         ean_secundario=EXCLUDED.ean_secundario,
         qtd_embalagem=EXCLUDED.qtd_embalagem,
         peso_unidade_kg=EXCLUDED.peso_unidade_kg,
         peso_variavel=EXCLUDED.peso_variavel,
         atualizado_em=NOW(),
         atualizado_por=EXCLUDED.atualizado_por`,
      [b.cd_codigo, String(b.mat_codi), normEan(b.ean_principal), normEan(b.ean_secundario),
       qtd, pesoKg, pesoV, por]
    );
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/cd-produtos-embalagem/importar-excel
// FormData: arquivo (xlsx) + body.mapa_abas (JSON {nome_aba: cd_codigo})
// Se mapa_abas não vier, tenta inferir: "ITAITUBA"→srv2-asafrio, "SANTAREM"→srv2-asasantarem.
router.post('/importar-excel', autenticar, adminOuCeo, upload.single('arquivo'), async (req, res) => {
  try {
    if (!req.file?.buffer) return res.status(400).json({ erro: 'arquivo obrigatorio (campo: arquivo)' });
    const por = req.usuario.email || req.usuario.usuario || req.usuario.nome || `id:${req.usuario.id}`;

    let mapaAbas = {};
    if (req.body.mapa_abas) {
      try { mapaAbas = JSON.parse(req.body.mapa_abas); }
      catch { return res.status(400).json({ erro: 'mapa_abas precisa ser JSON' }); }
    }

    const wb = xlsx.read(req.file.buffer, { type: 'buffer' });
    function inferirCd(nomeAba) {
      if (mapaAbas[nomeAba]) return mapaAbas[nomeAba];
      const upper = nomeAba.toUpperCase();
      if (/ITAITUBA|ITB/.test(upper) && /FRIO/.test(upper)) return 'srv2-asafrio';
      if (/SANTAREM|STM/.test(upper) && /FRIO/.test(upper)) return 'srv2-asasantarem';
      if (/N[_-]?PROGRESSO|NP/.test(upper)) return 'srv1-nprogresso';
      if (/ITAITUBA|ITB/.test(upper)) return 'srv1-itautuba';
      return null;
    }

    const relatorio = [];
    let totalInseridos = 0, totalAtualizados = 0, totalIgnorados = 0;

    for (const nomeAba of wb.SheetNames) {
      const cdCodigo = inferirCd(nomeAba);
      if (!cdCodigo) {
        relatorio.push({ aba: nomeAba, status: 'pulado', motivo: 'cd não identificado (use mapa_abas)' });
        continue;
      }
      const rows = xlsx.utils.sheet_to_json(wb.Sheets[nomeAba], { defval: null });
      let inseridos = 0, atualizados = 0, ignorados = 0;
      const erros = [];

      for (const r of rows) {
        try {
          const matCodi = String(r.MAT_CODI ?? r.mat_codi ?? '').trim();
          if (!matCodi) { ignorados++; continue; }
          const eanPrincipal = normEan(r.EAN ?? r.ean ?? r.ean_principal);
          const eanSecundario = normEan(r.cod_barra ?? r.COD_BARRA ?? r.codigo_barra ?? r.ean_secundario);
          const unidade = parseFloat(r.UNIDADE ?? r.unidade ?? r.qtd_embalagem ?? 1) || 1;
          const fracionario = !Number.isInteger(unidade);
          const qtdEmb = fracionario ? 1 : Math.max(1, Math.round(unidade));
          const pesoKg = fracionario ? unidade : null;

          const existe = await dbQuery(
            `SELECT 1 FROM cd_produtos_embalagem WHERE cd_codigo=$1 AND mat_codi=$2`,
            [cdCodigo, matCodi]
          );
          await dbQuery(
            `INSERT INTO cd_produtos_embalagem
               (cd_codigo, mat_codi, ean_principal, ean_secundario, qtd_embalagem,
                peso_unidade_kg, peso_variavel, atualizado_em, atualizado_por)
             VALUES ($1,$2,$3,$4,$5,$6,$7,NOW(),$8)
             ON CONFLICT (cd_codigo, mat_codi) DO UPDATE SET
               ean_principal=EXCLUDED.ean_principal,
               ean_secundario=EXCLUDED.ean_secundario,
               qtd_embalagem=EXCLUDED.qtd_embalagem,
               peso_unidade_kg=EXCLUDED.peso_unidade_kg,
               peso_variavel=EXCLUDED.peso_variavel,
               atualizado_em=NOW(),
               atualizado_por=EXCLUDED.atualizado_por`,
            [cdCodigo, matCodi, eanPrincipal, eanSecundario, qtdEmb, pesoKg, fracionario, por]
          );
          if (existe.length) atualizados++; else inseridos++;
        } catch (e) {
          erros.push({ mat_codi: r.MAT_CODI, erro: e.message });
          ignorados++;
        }
      }
      relatorio.push({ aba: nomeAba, cd_codigo: cdCodigo, total: rows.length, inseridos, atualizados, ignorados, erros: erros.slice(0, 10) });
      totalInseridos += inseridos;
      totalAtualizados += atualizados;
      totalIgnorados += ignorados;
    }

    res.json({ ok: true, total_inseridos: totalInseridos, total_atualizados: totalAtualizados, total_ignorados: totalIgnorados, relatorio });
  } catch (e) {
    console.error('[cd-produtos-embalagem importar]', e);
    res.status(500).json({ erro: e.message });
  }
});

// DELETE /api/cd-produtos-embalagem/:cd_codigo/:mat_codi
router.delete('/:cd_codigo/:mat_codi', autenticar, adminOuCeo, async (req, res) => {
  try {
    await dbQuery(`DELETE FROM cd_produtos_embalagem WHERE cd_codigo=$1 AND mat_codi=$2`,
      [req.params.cd_codigo, req.params.mat_codi]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

module.exports = router;
