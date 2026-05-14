// Casa Branca — Auditoria de Saídas
// CB vende pras lojas pelo PDV (CAPA + MOVIITEM, sem emitir NFe formal — minuta de transportadora).
// Algumas vendas formais saem como NFe 55 (TBMOVCOMPRA, NOP=024/045) — fallback.
//
// Match primário: cd_capa.cap_sequ ↔ compras_historico.numeronfe (normalizado)
// Match fallback: cd_movcompra.mcp_nnotafis ↔ compras_historico.numeronfe
//
// 3 categorias:
//   ✅ Confirmada       — saída CB (PDV ou NFe) com entrada correspondente em loja
//   ⏳ Em trânsito      — saída CB sem entrada confirmada
//   ⚠️ Origem desconhecida — entrada loja com CNPJ CB sem saída CB sincronizada

const express = require('express');
const router = express.Router();
const { query: dbQuery } = require('../db');
const { autenticar, exigirPerfil } = require('../auth');

const CB_CD_CODIGO = 'srv3-casabranca';
const CB_CNPJ = '07961363000132';
const adminOuCeo = [autenticar, exigirPerfil('admin', 'ceo', 'comprador')];

// Mapeamento CLI_CODI (CB CLIENTE) → loja_id jrlira-tech. Descoberto 2026-05-14.
const CB_CLI_LOJA = {
  '0000001495': 1, // V.C.A LIRA EIRELI — Econômico
  '0000002610': 2, // V.C.A LIRA EIRELI — BR
  '0000003308': 3, // V.C.A LIRA EIRELI — João Pessoa
  '0000004342': 4, // J.R. LIRA LTDA — Floresta
  '0000005153': 5, // J.R. LIRA LTDA — São Jose
  '0000005271': 6, // J.R. LIRA LTDA — Santarém
};
const CB_CLI_CODIS = Object.keys(CB_CLI_LOJA);

// Cláusulas SQL compartilhadas — match capa OR match movcompra
function matchSaidaCb(chAlias = 'ch') {
  return `(
    EXISTS (
      SELECT 1 FROM cd_capa cc
       WHERE cc.cd_codigo = '${CB_CD_CODIGO}' AND cc.cap_tipo IN ('3','4')
         AND COALESCE(cc.cap_stvd, '') <> 'C'
         AND cc.cli_codi = ANY ($CB_CLIS)
         AND REGEXP_REPLACE(COALESCE(cc.cap_sequ, ''), '^0+', '') =
             REGEXP_REPLACE(COALESCE(${chAlias}.numeronfe, ''), '^0+', '')
         AND REGEXP_REPLACE(COALESCE(cc.cap_sequ, ''), '^0+', '') <> ''
    )
    OR EXISTS (
      SELECT 1 FROM cd_movcompra mc
       WHERE mc.cd_codigo = '${CB_CD_CODIGO}' AND mc.mcp_tipomov = 'S'
         AND COALESCE(mc.mcp_status, '') <> 'C'
         AND REGEXP_REPLACE(COALESCE(mc.mcp_nnotafis, ''), '^0+', '') =
             REGEXP_REPLACE(COALESCE(${chAlias}.numeronfe, ''), '^0+', '')
         AND REGEXP_REPLACE(COALESCE(mc.mcp_nnotafis, ''), '^0+', '') <> ''
    )
  )`;
}

// GET /api/cb-auditoria/resumo?mes=YYYY-MM
router.get('/resumo', adminOuCeo, async (req, res) => {
  try {
    const mes = String(req.query.mes || new Date().toISOString().slice(0, 7));
    const matchSQL = matchSaidaCb('ch').replace(/\$CB_CLIS/g, '$3');

    const confirmadas = await dbQuery(`
      SELECT ch.loja_id,
             COUNT(DISTINCT ch.numeronfe)::int AS qtd,
             COALESCE(SUM(ch.custo_total), 0)::numeric(14,2) AS valor
        FROM compras_historico ch
       WHERE REGEXP_REPLACE(ch.fornecedor_cnpj, '\\D', '', 'g') = $1
         AND TO_CHAR(ch.data_entrada, 'YYYY-MM') = $2
         AND ${matchSQL}
       GROUP BY ch.loja_id`, [CB_CNPJ, mes, CB_CLI_CODIS]);

    const desconhecidas = await dbQuery(`
      SELECT ch.loja_id,
             COUNT(DISTINCT ch.numeronfe)::int AS qtd,
             COALESCE(SUM(ch.custo_total), 0)::numeric(14,2) AS valor
        FROM compras_historico ch
       WHERE REGEXP_REPLACE(ch.fornecedor_cnpj, '\\D', '', 'g') = $1
         AND TO_CHAR(ch.data_entrada, 'YYYY-MM') = $2
         AND NOT ${matchSQL}
       GROUP BY ch.loja_id`, [CB_CNPJ, mes, CB_CLI_CODIS]);

    // Em trânsito = saídas CB com cli_codi de loja sem entrada em compras_historico
    // Só PDV (cd_capa) — cd_movcompra não tem identificação de destino confiável
    const transito = await dbQuery(`
      WITH saidas AS (
        SELECT cc.cap_sequ AS doc, cc.cap_dtem AS data, cc.cli_codi AS cli,
               COALESCE((SELECT SUM(mi.ite_quan * mi.ite_valo)
                           FROM cd_moviitem mi
                          WHERE mi.cd_codigo = cc.cd_codigo AND mi.cap_sequ = cc.cap_sequ), 0)::numeric AS valor
          FROM cd_capa cc
         WHERE cc.cd_codigo = $1 AND cc.cap_tipo IN ('3','4')
           AND COALESCE(cc.cap_stvd,'') <> 'C'
           AND cc.cli_codi = ANY($2)
           AND TO_CHAR(cc.cap_dtem, 'YYYY-MM') = $3
      )
      SELECT COUNT(*)::int AS qtd,
             COALESCE(SUM(valor), 0)::numeric(14,2) AS valor
        FROM saidas s
       WHERE NOT EXISTS (
         SELECT 1 FROM compras_historico ch
          WHERE REGEXP_REPLACE(ch.fornecedor_cnpj, '\\D', '', 'g') = $4
            AND REGEXP_REPLACE(COALESCE(ch.numeronfe,''),'^0+','') =
                REGEXP_REPLACE(COALESCE(s.doc,''),'^0+','')
       )`, [CB_CD_CODIGO, CB_CLI_CODIS, mes, CB_CNPJ]);

    // Saídas CB total no mês (só vendas pras lojas — cli_codi mapeado)
    const saidasCb = await dbQuery(`
      SELECT COUNT(*)::int AS qtd,
             COALESCE(SUM(COALESCE((SELECT SUM(mi.ite_quan * mi.ite_valo)
                                      FROM cd_moviitem mi
                                     WHERE mi.cd_codigo = cc.cd_codigo AND mi.cap_sequ = cc.cap_sequ), 0)), 0)::numeric(14,2) AS valor
        FROM cd_capa cc
       WHERE cc.cd_codigo = $1 AND cc.cap_tipo IN ('3','4')
         AND COALESCE(cc.cap_stvd,'') <> 'C'
         AND cc.cli_codi = ANY($2)
         AND TO_CHAR(cc.cap_dtem, 'YYYY-MM') = $3`, [CB_CD_CODIGO, CB_CLI_CODIS, mes]);

    const lojas = await dbQuery(`SELECT id, nome FROM lojas ORDER BY id`);

    const sum = (rows) => rows.reduce((acc, r) => ({ qtd: acc.qtd + r.qtd, valor: acc.valor + parseFloat(r.valor || 0) }), { qtd: 0, valor: 0 });
    const totConf = sum(confirmadas);
    const totDesc = sum(desconhecidas);

    res.json({
      mes,
      cb_cnpj: CB_CNPJ,
      fonte: 'cd_capa (PDV) + cd_movcompra (NFe 55) ↔ compras_historico',
      kpis: {
        saidas_cb_mes: { qtd: saidasCb[0]?.qtd || 0, valor: parseFloat(saidasCb[0]?.valor || 0) },
        confirmadas: totConf,
        em_transito: { qtd: transito[0]?.qtd || 0, valor: parseFloat(transito[0]?.valor || 0) },
        origem_desconhecida: totDesc,
      },
      por_loja: { confirmadas, desconhecidas },
      lojas,
    });
  } catch (e) {
    console.error('[cb-auditoria/resumo]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// GET /api/cb-auditoria/produtos?mes=YYYY-MM&loja_id=&limit=
// Agrega itens via cd_moviitem das saídas confirmadas (cap_sequ que bateu com loja)
router.get('/produtos', adminOuCeo, async (req, res) => {
  try {
    const mes = String(req.query.mes || new Date().toISOString().slice(0, 7));
    const lojaId = req.query.loja_id ? parseInt(req.query.loja_id) : null;
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);

    let lojaCond = '';
    const params = [CB_CD_CODIGO, CB_CLI_CODIS, mes, CB_CNPJ];
    if (lojaId) {
      params.push(lojaId);
      lojaCond = `AND ch.loja_id = $${params.length}`;
    }
    params.push(limit);
    const limitIdx = params.length;

    const rows = await dbQuery(`
      WITH saidas_match AS (
        SELECT cc.cap_sequ AS doc, cc.cd_codigo AS cd_codigo
          FROM cd_capa cc
         WHERE cc.cd_codigo = $1 AND cc.cap_tipo IN ('3','4')
           AND COALESCE(cc.cap_stvd,'') <> 'C'
           AND cc.cli_codi = ANY($2)
           AND TO_CHAR(cc.cap_dtem, 'YYYY-MM') = $3
           AND EXISTS (
             SELECT 1 FROM compras_historico ch
              WHERE REGEXP_REPLACE(ch.fornecedor_cnpj,'\\D','','g') = $4
                AND TO_CHAR(ch.data_entrada,'YYYY-MM') = $3
                ${lojaCond}
                AND REGEXP_REPLACE(COALESCE(ch.numeronfe,''),'^0+','') =
                    REGEXP_REPLACE(COALESCE(cc.cap_sequ,''),'^0+','')
           )
      )
      SELECT mi.pro_codi,
             COALESCE(m.mat_desc, '(sem descrição)') AS descricao,
             COALESCE(m.mat_refe, '') AS referencia,
             SUM(mi.ite_quan)::numeric(14,3) AS qtd_total,
             SUM(mi.ite_quan * mi.ite_valo)::numeric(14,2) AS valor_total,
             COUNT(DISTINCT mi.cap_sequ)::int AS qtd_notas
        FROM cd_moviitem mi
        JOIN saidas_match s ON s.cd_codigo = mi.cd_codigo AND s.doc = mi.cap_sequ
        LEFT JOIN cd_material m ON m.cd_codigo = mi.cd_codigo AND m.mat_codi = mi.pro_codi
       WHERE mi.cd_codigo = $1
       GROUP BY mi.pro_codi, m.mat_desc, m.mat_refe
       ORDER BY valor_total DESC
       LIMIT $${limitIdx}`, params);

    res.json({ mes, loja_id: lojaId, total: rows.length, produtos: rows });
  } catch (e) {
    console.error('[cb-auditoria/produtos]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// GET /api/cb-auditoria/notas?mes=YYYY-MM&loja_id=&categoria=confirmadas|desconhecidas|transito
router.get('/notas', adminOuCeo, async (req, res) => {
  try {
    const mes = String(req.query.mes || new Date().toISOString().slice(0, 7));
    const lojaId = req.query.loja_id ? parseInt(req.query.loja_id) : null;
    const categoria = ['confirmadas', 'desconhecidas', 'transito'].includes(req.query.categoria)
      ? req.query.categoria : 'confirmadas';

    if (categoria === 'transito') {
      // Só PDV (cd_capa) com cli_codi de loja — saídas identificadas sem confirmação
      const rows = await dbQuery(`
        SELECT cc.cap_sequ AS doc, cc.cap_dtem::date AS data, cc.cli_codi AS cli,
               cc.ven_codi AS ven, cc.cap_tipo AS tipo,
               COALESCE((SELECT SUM(mi.ite_quan * mi.ite_valo)
                           FROM cd_moviitem mi
                          WHERE mi.cd_codigo = cc.cd_codigo AND mi.cap_sequ = cc.cap_sequ), 0)::numeric(14,2) AS valor
          FROM cd_capa cc
         WHERE cc.cd_codigo = $1 AND cc.cap_tipo IN ('3','4')
           AND COALESCE(cc.cap_stvd,'') <> 'C'
           AND cc.cli_codi = ANY($2)
           AND TO_CHAR(cc.cap_dtem, 'YYYY-MM') = $3
           AND NOT EXISTS (
             SELECT 1 FROM compras_historico ch
              WHERE REGEXP_REPLACE(ch.fornecedor_cnpj, '\\D', '', 'g') = $4
                AND REGEXP_REPLACE(COALESCE(ch.numeronfe,''),'^0+','') =
                    REGEXP_REPLACE(COALESCE(cc.cap_sequ,''),'^0+','')
           )
         ORDER BY cc.cap_dtem DESC
         LIMIT 500`, [CB_CD_CODIGO, CB_CLI_CODIS, mes, CB_CNPJ]);
      return res.json({ mes, categoria, total: rows.length, notas: rows });
    }

    // confirmadas / desconhecidas — operam em compras_historico
    const params = [CB_CNPJ, mes];
    let lojaCond = '';
    if (lojaId) { params.push(lojaId); lojaCond = `AND ch.loja_id = $${params.length}`; }
    params.push(CB_CLI_CODIS);
    const cliIdx = params.length;

    const matchClause = `(
      EXISTS (
        SELECT 1 FROM cd_capa cc
         WHERE cc.cd_codigo = '${CB_CD_CODIGO}' AND cc.cap_tipo IN ('3','4')
           AND COALESCE(cc.cap_stvd,'') <> 'C'
           AND cc.cli_codi = ANY($${cliIdx})
           AND REGEXP_REPLACE(COALESCE(cc.cap_sequ,''),'^0+','') =
               REGEXP_REPLACE(COALESCE(ch.numeronfe,''),'^0+','')
           AND REGEXP_REPLACE(COALESCE(cc.cap_sequ,''),'^0+','') <> ''
      )
      OR EXISTS (
        SELECT 1 FROM cd_movcompra mc
         WHERE mc.cd_codigo = '${CB_CD_CODIGO}' AND mc.mcp_tipomov = 'S'
           AND COALESCE(mc.mcp_status,'') <> 'C'
           AND REGEXP_REPLACE(COALESCE(mc.mcp_nnotafis,''),'^0+','') =
               REGEXP_REPLACE(COALESCE(ch.numeronfe,''),'^0+','')
           AND REGEXP_REPLACE(COALESCE(mc.mcp_nnotafis,''),'^0+','') <> ''
      )
    )`;
    const filtroMatch = categoria === 'confirmadas' ? matchClause : `NOT ${matchClause}`;

    const rows = await dbQuery(`
      SELECT ch.loja_id, ch.numeronfe, ch.data_entrada::date AS data,
             COALESCE(SUM(ch.custo_total), 0)::numeric(14,2) AS valor,
             COUNT(*)::int AS qtd_itens
        FROM compras_historico ch
       WHERE REGEXP_REPLACE(ch.fornecedor_cnpj,'\\D','','g') = $1
         AND TO_CHAR(ch.data_entrada,'YYYY-MM') = $2
         ${lojaCond}
         AND ${filtroMatch}
       GROUP BY ch.loja_id, ch.numeronfe, ch.data_entrada
       ORDER BY ch.data_entrada DESC
       LIMIT 500`, params);
    res.json({ mes, categoria, loja_id: lojaId, total: rows.length, notas: rows });
  } catch (e) {
    console.error('[cb-auditoria/notas]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

module.exports = router;
