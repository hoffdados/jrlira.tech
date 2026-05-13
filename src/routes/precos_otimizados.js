// Otimizador de preços — replica a planilha OTIMIZADOR DE PRECOS para o CEO.
// Tela única consolidando: dados do CD (cd_estoque/custoprod/vendapro) + L4/L6 (produtos_externo)
// + preços otimizados salvos (precos_otimizados).
//
// Fase 1: read-only. Fase 2 vai adicionar PUT pra editar preco_otimizado.

const express = require('express');
const router = express.Router();
const { query: dbQuery } = require('../db');
const { autenticar, apenasAdmin, adminOuCeo } = require('../auth');
const { criarNotificacao } = require('./notificacoes');
const { enviarWhatsapp } = require('../whatsapp');

// Mapeamento das siglas da planilha → loja_id
// ITB representa o GRUPO Itaituba (L1, L2, L3, L4, L5) que pratica o mesmo preço otimizado.
// Por convenção, gravamos o preço otimizado com loja_id = 4 (L4 = Floresta) representando o grupo.
// STM = L6 Santarém (sozinha + futuras inaugurações similares).
const LOJA_ITB = 4;
const LOJA_STM = 6;
// Lojas que aplicam cada grupo (cadastros recebem a lista do grupo)
const LOJAS_GRUPO = {
  itb: [1, 2, 3, 4, 5],
  stm: [6],
};
const LOJA_LIDER = { itb: LOJA_ITB, stm: LOJA_STM };

// GET /api/precos-otimizados/estudo
// Retorna o extrato completo (todos produtos ativos no CD com dados agregados).
router.get('/estudo', autenticar, async (req, res) => {
  try {
    const linhas = await dbQuery(`
      WITH ult_lote_itb AS (
        SELECT li.mat_codi, li.preco_otimizado AS ultimo_enviado
          FROM precos_otimizados_lote_itens li
          JOIN precos_otimizados_lote l ON l.id = li.lote_id
         WHERE l.grupo = 'itb'
           AND (li.mat_codi, l.id) IN (
             SELECT li2.mat_codi, MAX(l2.id)
               FROM precos_otimizados_lote_itens li2
               JOIN precos_otimizados_lote l2 ON l2.id = li2.lote_id
              WHERE l2.grupo = 'itb' GROUP BY li2.mat_codi
           )
      ),
      ult_lote_stm AS (
        SELECT li.mat_codi, li.preco_otimizado AS ultimo_enviado
          FROM precos_otimizados_lote_itens li
          JOIN precos_otimizados_lote l ON l.id = li.lote_id
         WHERE l.grupo = 'stm'
           AND (li.mat_codi, l.id) IN (
             SELECT li2.mat_codi, MAX(l2.id)
               FROM precos_otimizados_lote_itens li2
               JOIN precos_otimizados_lote l2 ON l2.id = li2.lote_id
              WHERE l2.grupo = 'stm' GROUP BY li2.mat_codi
           )
      )
      SELECT
        pe_emb.mat_codi,
        pe_emb.descricao_atual                         AS descricao,
        cd_m.mat_refe                                  AS referencia,
        pe_emb.ean_principal_cd                        AS ean,
        pe_emb.qtd_embalagem,
        -- CD — preços em CAIXA no ERP, dividir por qtd_embalagem pra unitário.
        COALESCE(cd_e.est_quan, 0)::numeric(14,3)      AS cd_est,
        (cd_c.pro_prcr / NULLIF(pe_emb.qtd_embalagem,0))::numeric(14,4) AS cd_p_compra,
        (cd_c.pro_prad / NULLIF(pe_emb.qtd_embalagem,0))::numeric(14,4) AS cd_p_admin,
        (cd_v.tab_prc4 / NULLIF(pe_emb.qtd_embalagem,0))::numeric(14,4) AS cd_tab4,
        -- Preços brutos (em caixa) pra referência/conferência
        cd_c.pro_prcr                                  AS cd_p_compra_cx,
        cd_c.pro_prad                                  AS cd_p_admin_cx,
        cd_v.tab_prc4                                  AS cd_tab4_cx,
        -- L4 (ITB)
        FLOOR(COALESCE(pe4.estdisponivel,0) / NULLIF(pe_emb.qtd_embalagem,0))::int  AS l4_est_emb,
        pe4.custoorigem                                AS l4_custo_fab,
        pe4.prsugerido                                 AS l4_pv_atual,
        po4.preco_otimizado                            AS l4_pv_otim,
        uli.ultimo_enviado                             AS l4_ult_enviado,
        -- L6 (STM)
        FLOOR(COALESCE(pe6.estdisponivel,0) / NULLIF(pe_emb.qtd_embalagem,0))::int  AS l6_est_emb,
        pe6.custoorigem                                AS l6_custo_fab,
        pe6.prsugerido                                 AS l6_pv_atual,
        po6.preco_otimizado                            AS l6_pv_otim,
        uls.ultimo_enviado                             AS l6_ult_enviado
      FROM produtos_embalagem pe_emb
      LEFT JOIN cd_material cd_m  ON cd_m.cd_codigo = 'srv1-itautuba' AND cd_m.mat_codi = pe_emb.mat_codi
      LEFT JOIN cd_estoque cd_e   ON cd_e.cd_codigo = 'srv1-itautuba' AND cd_e.pro_codi = pe_emb.mat_codi
      LEFT JOIN cd_custoprod cd_c ON cd_c.cd_codigo = 'srv1-itautuba' AND cd_c.pro_codi = pe_emb.mat_codi
      LEFT JOIN cd_vendapro cd_v  ON cd_v.cd_codigo = 'srv1-itautuba' AND cd_v.pro_codi = pe_emb.mat_codi
      LEFT JOIN produtos_externo pe4 ON pe4.loja_id = ${LOJA_ITB}
                                    AND NULLIF(LTRIM(pe4.codigobarra,'0'),'') = NULLIF(LTRIM(pe_emb.ean_principal_cd,'0'),'')
      LEFT JOIN produtos_externo pe6 ON pe6.loja_id = ${LOJA_STM}
                                    AND NULLIF(LTRIM(pe6.codigobarra,'0'),'') = NULLIF(LTRIM(pe_emb.ean_principal_cd,'0'),'')
      LEFT JOIN precos_otimizados po4 ON po4.loja_id = ${LOJA_ITB} AND po4.mat_codi = pe_emb.mat_codi
      LEFT JOIN precos_otimizados po6 ON po6.loja_id = ${LOJA_STM} AND po6.mat_codi = pe_emb.mat_codi
      LEFT JOIN ult_lote_itb uli ON uli.mat_codi = pe_emb.mat_codi
      LEFT JOIN ult_lote_stm uls ON uls.mat_codi = pe_emb.mat_codi
      WHERE pe_emb.ativo_no_cd = TRUE
      ORDER BY pe_emb.descricao_atual
    `);

    // Status de sync (timestamps) pro header da tela
    const status = await dbQuery(`SELECT chave, valor FROM _sync_state WHERE chave LIKE 'cd_%_ultima_sync'`);
    const syncStatus = {};
    for (const s of status) syncStatus[s.chave] = s.valor;

    res.json({ linhas, sync: syncStatus, mapping: { itb: LOJA_ITB, stm: LOJA_STM } });
  } catch (e) {
    console.error('[precos-otimizados/estudo]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// PUT /api/precos-otimizados/:loja_id/:mat_codi — admin grava ou apaga preço otimizado.
// Body: { preco }  → grava (preco > 0) ou remove (preco null/0).
router.put('/:loja_id/:mat_codi', adminOuCeo, async (req, res) => {
  try {
    const lojaId = parseInt(req.params.loja_id);
    if (![LOJA_ITB, LOJA_STM].includes(lojaId)) {
      return res.status(400).json({ erro: `loja_id deve ser ${LOJA_ITB} (ITB) ou ${LOJA_STM} (STM)` });
    }
    const matCodi = String(req.params.mat_codi).trim();
    const preco = req.body?.preco;
    const por = req.usuario.email || req.usuario.usuario || req.usuario.nome || `id:${req.usuario.id}`;
    if (preco == null || preco === '' || parseFloat(preco) <= 0) {
      // Remove o otimizado
      await dbQuery(
        `DELETE FROM precos_otimizados WHERE loja_id=$1 AND mat_codi=$2`,
        [lojaId, matCodi]
      );
      return res.json({ ok: true, acao: 'removido' });
    }
    const valor = parseFloat(preco);
    if (isNaN(valor)) return res.status(400).json({ erro: 'preco inválido' });
    await dbQuery(
      `INSERT INTO precos_otimizados (loja_id, mat_codi, preco_otimizado, atualizado_em, atualizado_por)
       VALUES ($1,$2,$3,NOW(),$4)
       ON CONFLICT (loja_id, mat_codi) DO UPDATE SET
         preco_otimizado=EXCLUDED.preco_otimizado, atualizado_em=NOW(), atualizado_por=EXCLUDED.atualizado_por`,
      [lojaId, matCodi, valor, por]
    );
    res.json({ ok: true, acao: 'salvo', preco_otimizado: valor });
  } catch (e) {
    console.error('[precos-otimizados PUT]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// POST /api/precos-otimizados/sync-now (admin) — força sync imediato
router.post('/sync-now', adminOuCeo, async (req, res) => {
  try {
    const { syncCdAll } = require('../sync_cd');
    const r = await syncCdAll();
    res.json({ ok: true, resultados: r });
  } catch (e) {
    console.error('[precos-otimizados/sync-now]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// ────────── FASE 3: lotes de envio + auto-validação ───────────────────

// Calcula os itens que mudaram desde o último lote do grupo (ou nunca foram enviados).
async function diffPendente(grupo) {
  const lojaId = LOJA_LIDER[grupo];
  return dbQuery(`
    WITH ult_lote AS (
      SELECT i.mat_codi, i.preco_otimizado AS ultimo_enviado
        FROM precos_otimizados_lote_itens i
        JOIN precos_otimizados_lote l ON l.id = i.lote_id
       WHERE l.grupo = $1
         AND (i.mat_codi, l.id) IN (
           SELECT i2.mat_codi, MAX(l2.id)
             FROM precos_otimizados_lote_itens i2
             JOIN precos_otimizados_lote l2 ON l2.id = i2.lote_id
            WHERE l2.grupo = $1
            GROUP BY i2.mat_codi
         )
    )
    SELECT po.mat_codi,
           pe_emb.descricao_atual    AS descricao,
           cd_m.mat_refe              AS referencia,
           pe_emb.ean_principal_cd    AS ean,
           po.preco_otimizado         AS preco_novo,
           ul.ultimo_enviado          AS preco_lote_anterior,
           pe_loja.prsugerido         AS preco_loja_atual
      FROM precos_otimizados po
      JOIN produtos_embalagem pe_emb ON pe_emb.mat_codi = po.mat_codi
      LEFT JOIN cd_material cd_m     ON cd_m.cd_codigo = 'srv1-itautuba' AND cd_m.mat_codi = po.mat_codi
      LEFT JOIN produtos_externo pe_loja
        ON pe_loja.loja_id = po.loja_id
       AND NULLIF(LTRIM(pe_loja.codigobarra,'0'),'') = NULLIF(LTRIM(pe_emb.ean_principal_cd,'0'),'')
      LEFT JOIN ult_lote ul ON ul.mat_codi = po.mat_codi
     WHERE po.loja_id = $2
       AND (ul.ultimo_enviado IS NULL OR ABS(ul.ultimo_enviado - po.preco_otimizado) >= 0.01)
     ORDER BY pe_emb.descricao_atual
  `, [grupo, lojaId]);
}

// GET /api/precos-otimizados/diff?grupo=itb|stm — preview do próximo lote
router.get('/diff', adminOuCeo, async (req, res) => {
  try {
    const grupo = req.query.grupo;
    if (!['itb', 'stm'].includes(grupo)) return res.status(400).json({ erro: 'grupo deve ser itb ou stm' });
    const itens = await diffPendente(grupo);
    res.json({ grupo, total: itens.length, itens });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/precos-otimizados/enviar-lote — body: { grupo: 'itb'|'stm' } (ou ambos se vazio)
// Cria lote(s) com itens diferentes desde último envio. Cria pendências por loja.
router.post('/enviar-lote', adminOuCeo, async (req, res) => {
  try {
    const grupos = req.body?.grupo ? [req.body.grupo] : ['itb', 'stm'];
    const por = req.usuario.email || req.usuario.usuario || req.usuario.nome || `id:${req.usuario.id}`;
    const lotesCriados = [];
    for (const grupo of grupos) {
      if (!LOJAS_GRUPO[grupo]) continue;
      const itens = await diffPendente(grupo);
      if (!itens.length) continue; // não cria lote vazio
      const [lote] = await dbQuery(
        `INSERT INTO precos_otimizados_lote (grupo, gerado_por, total_itens) VALUES ($1,$2,$3) RETURNING id, gerado_em`,
        [grupo, por, itens.length]
      );
      // Itens do lote
      await dbQuery(
        `INSERT INTO precos_otimizados_lote_itens (lote_id, mat_codi, preco_otimizado, preco_anterior)
         SELECT $1, m, p, pa FROM UNNEST($2::text[], $3::numeric[], $4::numeric[]) AS t(m, p, pa)`,
        [
          lote.id,
          itens.map(i => i.mat_codi),
          itens.map(i => i.preco_novo),
          itens.map(i => i.preco_loja_atual),  // PV anterior = PV atual da loja líder
        ]
      );
      // Aplicações pendentes (1 linha por loja do grupo) — header
      for (const lojaId of LOJAS_GRUPO[grupo]) {
        await dbQuery(
          `INSERT INTO precos_otimizados_aplicacao (lote_id, loja_id) VALUES ($1, $2)`,
          [lote.id, lojaId]
        );
      }
      // Aplicações pendentes — granular (loja x item)
      const aplItens = [];
      for (const lojaId of LOJAS_GRUPO[grupo]) {
        for (const it of itens) aplItens.push({ loja: lojaId, mat: it.mat_codi });
      }
      if (aplItens.length) {
        await dbQuery(
          `INSERT INTO precos_otimizados_aplicacao_item (lote_id, loja_id, mat_codi)
           SELECT $1, l, m FROM UNNEST($2::int[], $3::text[]) AS t(l, m)`,
          [lote.id, aplItens.map(x => x.loja), aplItens.map(x => x.mat)]
        );
      }
      lotesCriados.push({ id: lote.id, grupo, gerado_em: lote.gerado_em, total_itens: itens.length });

      // Notifica cadastros das lojas do grupo (e admins) — in-app + WhatsApp.
      const dest = await dbQuery(
        `SELECT id, perfil, loja_id, telefone FROM rh_usuarios
          WHERE ativo=TRUE
            AND ((perfil='cadastro' AND loja_id = ANY($1::int[])) OR perfil='admin')`,
        [LOJAS_GRUPO[grupo]]
      );
      const grupoUp = grupo.toUpperCase();
      const titulo = `📤 Novo lote de preços otimizados — ${grupoUp}`;
      const corpo = `Lote #${lote.id} com ${itens.length} item(ns) pra aplicar no Ecocentauro. Veja em /precos-otimizados-pendentes.`;
      const msgWa = `*${titulo}*\n\n${corpo}\n\nhttps://jrliratech-production.up.railway.app/precos-otimizados-pendentes`;
      for (const u of dest) {
        criarNotificacao({
          destinatario_tipo: 'usuario', destinatario_id: u.id, tipo: 'preco_otimizado_lote',
          titulo, corpo, url: '/precos-otimizados-pendentes',
        }).catch(() => {});
        if (u.telefone) {
          enviarWhatsapp(u.telefone, msgWa).catch(e => console.warn('[wa lote]', e.message));
        }
      }
      // Fallback: admin via ADMIN_WHATSAPP env var (mesmo se não tiver no rh_usuarios.telefone)
      if (process.env.ADMIN_WHATSAPP) {
        enviarWhatsapp(process.env.ADMIN_WHATSAPP, msgWa).catch(() => {});
      }
    }
    res.json({ ok: true, lotes: lotesCriados });
  } catch (e) {
    console.error('[enviar-lote]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// GET /api/precos-otimizados/pendentes?loja_id=
// Cadastro/admin de cada loja vê os lotes com itens pendentes da loja dele.
router.get('/pendentes', autenticar, async (req, res) => {
  try {
    const lojaUsr = req.usuario.loja_id;
    const lojasUsr = Array.isArray(req.usuario.lojas) ? req.usuario.lojas.map(Number) : [];
    const lojaQuery = req.query.loja_id ? parseInt(req.query.loja_id) : null;
    const perfilUsr = req.usuario.perfil;
    let lojaId = lojaUsr || lojaQuery;
    // Cadastro multi-loja: aceita loja_id da query SE estiver nas lojas dele
    if (!lojaId && lojasUsr.length === 1) lojaId = lojasUsr[0];
    if (lojaQuery && perfilUsr === 'cadastro' && lojasUsr.length && !lojasUsr.includes(lojaQuery)) {
      return res.status(403).json({ erro: 'loja fora do escopo do usuário' });
    }
    if (!lojaId) return res.status(400).json({ erro: 'loja_id obrigatório' });

    const lotes = await dbQuery(`
      SELECT l.id, l.grupo, l.gerado_em, l.gerado_por, l.total_itens,
             COUNT(*) FILTER (WHERE ai.aplicado_em IS NOT NULL)::int AS aplicados,
             COUNT(*)::int AS total_loja
        FROM precos_otimizados_lote l
        JOIN precos_otimizados_aplicacao_item ai ON ai.lote_id = l.id AND ai.loja_id = $1
       GROUP BY l.id
       ORDER BY l.gerado_em DESC
       LIMIT 50
    `, [lojaId]);

    // Itens de cada lote (pra render no front)
    const itens = await dbQuery(`
      SELECT ai.lote_id, ai.mat_codi, ai.aplicado_em, ai.aplicado_por,
             li.preco_otimizado, li.preco_anterior,
             pe_emb.descricao_atual AS descricao, pe_emb.ean_principal_cd AS ean,
             cd_m.mat_refe AS referencia,
             pe_loja.prsugerido AS pv_loja_atual
        FROM precos_otimizados_aplicacao_item ai
        JOIN precos_otimizados_lote_itens li ON li.lote_id = ai.lote_id AND li.mat_codi = ai.mat_codi
        LEFT JOIN produtos_embalagem pe_emb ON pe_emb.mat_codi = ai.mat_codi
        LEFT JOIN cd_material cd_m ON cd_m.cd_codigo = 'srv1-itautuba' AND cd_m.mat_codi = ai.mat_codi
        LEFT JOIN produtos_externo pe_loja
          ON pe_loja.loja_id = ai.loja_id
         AND NULLIF(LTRIM(pe_loja.codigobarra,'0'),'') = NULLIF(LTRIM(pe_emb.ean_principal_cd,'0'),'')
       WHERE ai.loja_id = $1
         AND ai.lote_id = ANY($2::int[])
       ORDER BY pe_emb.descricao_atual
    `, [lojaId, lotes.map(l => l.id)]);

    res.json({ loja_id: lojaId, lotes, itens });
  } catch (e) {
    console.error('[pendentes]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// GET /api/precos-otimizados/status — admin: lista lotes + status por loja
router.get('/status', adminOuCeo, async (req, res) => {
  try {
    const lotes = await dbQuery(`
      SELECT l.id, l.grupo, l.gerado_em, l.gerado_por, l.total_itens
        FROM precos_otimizados_lote l
       ORDER BY l.gerado_em DESC LIMIT 50
    `);
    const aplicacoes = await dbQuery(`
      SELECT ai.lote_id, ai.loja_id, lj.nome AS loja_nome,
             COUNT(*)::int AS total_itens,
             COUNT(*) FILTER (WHERE ai.aplicado_em IS NOT NULL)::int AS aplicados,
             MAX(ai.aplicado_em) AS aplicado_em,
             MAX(ai.aplicado_por) AS aplicado_por
        FROM precos_otimizados_aplicacao_item ai
        JOIN lojas lj ON lj.id = ai.loja_id
       WHERE ai.lote_id = ANY($1::int[])
       GROUP BY ai.lote_id, ai.loja_id, lj.nome
       ORDER BY ai.loja_id
    `, [lotes.map(l => l.id)]);
    res.json({ lotes, aplicacoes });
  } catch (e) {
    console.error('[status]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// POST /api/precos-otimizados/aplicar-manual — body: { lote_id, mat_codi (opcional, senão lote inteiro) }
// Fallback: marca aplicado mesmo sem o sync detectar.
router.post('/aplicar-manual', autenticar, async (req, res) => {
  try {
    const lojaUsr = req.usuario.loja_id;
    const lojasUsr = Array.isArray(req.usuario.lojas) ? req.usuario.lojas.map(Number) : [];
    const lojaBody = req.body?.loja_id ? parseInt(req.body.loja_id) : null;
    const perfilUsr = req.usuario.perfil;
    let lojaId = lojaUsr || lojaBody;
    if (!lojaId && lojasUsr.length === 1) lojaId = lojasUsr[0];
    if (lojaBody && perfilUsr === 'cadastro' && lojasUsr.length && !lojasUsr.includes(lojaBody)) {
      return res.status(403).json({ erro: 'loja fora do escopo do usuário' });
    }
    if (!lojaId) return res.status(400).json({ erro: 'loja_id obrigatório' });
    const loteId = parseInt(req.body?.lote_id);
    if (!loteId) return res.status(400).json({ erro: 'lote_id obrigatório' });
    const matCodi = req.body?.mat_codi ? String(req.body.mat_codi).trim() : null;
    const por = req.usuario.email || req.usuario.usuario || req.usuario.nome || `id:${req.usuario.id}`;

    if (matCodi) {
      await dbQuery(
        `UPDATE precos_otimizados_aplicacao_item
            SET aplicado_em = NOW(), aplicado_por = $1
          WHERE lote_id = $2 AND loja_id = $3 AND mat_codi = $4 AND aplicado_em IS NULL`,
        [por + ' (manual)', loteId, lojaId, matCodi]
      );
    } else {
      await dbQuery(
        `UPDATE precos_otimizados_aplicacao_item
            SET aplicado_em = NOW(), aplicado_por = $1
          WHERE lote_id = $2 AND loja_id = $3 AND aplicado_em IS NULL`,
        [por + ' (manual)', loteId, lojaId]
      );
    }
    // Atualiza header se completou
    await dbQuery(`
      UPDATE precos_otimizados_aplicacao a
         SET aplicado_em = NOW(), aplicado_por = $1
       WHERE a.lote_id = $2 AND a.loja_id = $3 AND a.aplicado_em IS NULL
         AND NOT EXISTS (
           SELECT 1 FROM precos_otimizados_aplicacao_item ai
            WHERE ai.lote_id = a.lote_id AND ai.loja_id = a.loja_id AND ai.aplicado_em IS NULL
         )`, [por + ' (manual)', loteId, lojaId]);
    res.json({ ok: true });
  } catch (e) {
    console.error('[aplicar-manual]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// Função de auto-validação (chamada pelo cron e manualmente)
async function autoValidar() {
  // Marca aplicado quando produtos_externo.prsugerido bate com lote_itens.preco_otimizado.
  const r = await dbQuery(`
    UPDATE precos_otimizados_aplicacao_item ai
       SET aplicado_em = NOW(), aplicado_por = 'auto-sync'
      FROM precos_otimizados_lote_itens li,
           produtos_embalagem pe,
           produtos_externo pe_loja
     WHERE ai.aplicado_em IS NULL
       AND li.lote_id = ai.lote_id
       AND li.mat_codi = ai.mat_codi
       AND pe.mat_codi = ai.mat_codi
       AND pe_loja.loja_id = ai.loja_id
       AND NULLIF(LTRIM(pe_loja.codigobarra,'0'),'') = NULLIF(LTRIM(pe.ean_principal_cd,'0'),'')
       AND ABS(pe_loja.prsugerido - li.preco_otimizado) < 0.01
     RETURNING ai.lote_id, ai.loja_id, ai.mat_codi
  `);
  // Atualiza headers das aplicações onde tudo foi aplicado
  await dbQuery(`
    UPDATE precos_otimizados_aplicacao a
       SET aplicado_em = NOW(), aplicado_por = 'auto-sync'
     WHERE a.aplicado_em IS NULL
       AND NOT EXISTS (
         SELECT 1 FROM precos_otimizados_aplicacao_item ai
          WHERE ai.lote_id = a.lote_id AND ai.loja_id = a.loja_id AND ai.aplicado_em IS NULL
       )
  `);
  // Fase 4: pra cada item recém aplicado, promover a etiqueta pendente
  // (criada pelo trigger trim_skip_dup_prod_ext com motivo='preco_alterado') pra 'preco_otimizado'.
  // Repositor vai ver na tela de etiquetas-pendentes que precisa trocar a etiqueta.
  if (r.length) {
    await dbQuery(`
      UPDATE etiquetas_pendentes_check ep
         SET motivo = 'preco_otimizado',
             obs   = COALESCE(NULLIF(ep.obs,''), '') ||
                     CASE WHEN ep.obs IS NULL OR ep.obs = '' THEN '' ELSE ' | ' END ||
                     'Lote #' || aplic.lote_id || ' (CEO)'
        FROM (
          SELECT ai.loja_id, ai.lote_id, ai.mat_codi, pe.ean_principal_cd
            FROM precos_otimizados_aplicacao_item ai
            JOIN produtos_embalagem pe ON pe.mat_codi = ai.mat_codi
           WHERE (ai.lote_id, ai.loja_id, ai.mat_codi) IN (${r.map((_,i) => `($${i*3+1}::int,$${i*3+2}::int,$${i*3+3}::text)`).join(',')})
        ) aplic
       WHERE ep.loja_id = aplic.loja_id
         AND NULLIF(LTRIM(ep.barcode,'0'),'') = NULLIF(LTRIM(aplic.ean_principal_cd,'0'),'')
         AND ep.motivo = 'preco_alterado'
         AND ep.conferida_em IS NULL
         AND ep.gerada_em > NOW() - INTERVAL '6 hours'
    `, r.flatMap(x => [x.lote_id, x.loja_id, x.mat_codi]));
  }
  return r.length || 0;
}

// POST /api/precos-otimizados/auto-validar — admin: dispara validação imediata
router.post('/auto-validar', adminOuCeo, async (req, res) => {
  try {
    const n = await autoValidar();
    res.json({ ok: true, itens_marcados: n });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

router.autoValidar = autoValidar;
module.exports = router;
