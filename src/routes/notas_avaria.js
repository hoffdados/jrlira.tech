// Avarias & Devoluções pós-entrega: detectadas automaticamente, classificadas pelo comprador,
// geram cobrança contra o fornecedor. Fonte: vendas_historico (avaria) + compras_historico (devolução).
//
// Status do evento:
//   pendente            — sistema detectou, aguarda comprador
//   tratado_cadastro    — devolução já resolvida no recebimento (não cobra)
//   cobrar_fornecedor   — vira conta a receber pro fornecedor
//   perda_interna       — culpa da loja, sem cobrança
//   cobrado             — conta a receber foi paga

const express = require('express');
const router = express.Router();
const { pool } = require('../db');
const { autenticar, exigirPerfil } = require('../auth');
const { detectarAvarias } = require('../avaria_detector');

const dbQuery = async (sql, params) => (await pool.query(sql, params)).rows;
const compradorOuAdmin = [autenticar, exigirPerfil('admin', 'comprador', 'cadastro', 'ceo')];

// GET /debug-detector — roda o detector com logs detalhados
router.get('/debug-detector', autenticar, async (req, res) => {
  if (!['admin','ceo'].includes(req.usuario.perfil)) return res.status(403).json({ erro: 'apenas admin' });
  const log = [];
  try {
    const { pool } = require('../db');
    const client = await pool.connect();
    try {
      // 1) Conta avarias candidatas
      const c1 = await client.query(`SELECT COUNT(*)::int AS qtd FROM vendas_historico WHERE tipo_saida='avaria' AND data_venda >= '2026-04-01'`);
      log.push({ etapa: 'count_avarias_candidatas', qtd: c1.rows[0].qtd });
      // 2) Conta já em avaria_eventos
      const c2 = await client.query(`SELECT COUNT(*)::int AS qtd FROM avaria_eventos WHERE fonte='vendas_historico' AND tipo='avaria'`);
      log.push({ etapa: 'count_avaria_eventos_existentes', qtd: c2.rows[0].qtd });
      // 3) Roda detector
      const { detectarAvarias } = require('../avaria_detector');
      const t0 = Date.now();
      const stats = await detectarAvarias();
      log.push({ etapa: 'detectarAvarias', stats, ms: Date.now() - t0 });
      // 4) Conta resultados
      const c3 = await client.query(`SELECT tipo, status, COUNT(*)::int AS qtd FROM avaria_eventos GROUP BY tipo, status`);
      log.push({ etapa: 'avaria_eventos_final', rows: c3.rows });
      // 5) Sample 3 eventos inseridos
      const c4 = await client.query(`SELECT * FROM avaria_eventos ORDER BY id DESC LIMIT 3`);
      log.push({ etapa: 'sample_eventos', rows: c4.rows });
    } finally { client.release(); }
    res.json({ ok: true, log });
  } catch (e) { res.status(500).json({ erro: e.message, stack: e.stack, log }); }
});

// GET /debug-compra?numeronfe= — mostra data_emissao + data_entrada de uma NF na compras_historico
router.get('/debug-compra', autenticar, async (req, res) => {
  try {
    const num = String(req.query.numeronfe || '').trim();
    const lojaId = req.query.loja_id ? parseInt(req.query.loja_id) : null;
    const params = [num];
    let where = `REGEXP_REPLACE(numeronfe,'^0+','') = REGEXP_REPLACE($1,'^0+','')`;
    if (lojaId) { params.push(lojaId); where += ` AND loja_id = $${params.length}`; }
    const rows = await dbQuery(
      `SELECT loja_id, numeronfe, data_emissao, data_entrada,
              codigobarra, fornecedor_cnpj, qtd_comprada, custo_total
         FROM compras_historico
        WHERE ${where}
        ORDER BY data_entrada DESC LIMIT 20`, params);
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /debug-gaps — distribuição de gaps das devoluções pendentes
router.get('/debug-gaps', autenticar, async (req, res) => {
  try {
    const rows = await dbQuery(`
      SELECT
        CASE
          WHEN gap IS NULL THEN 'sem_origem'
          WHEN gap <= 2 THEN '0-2'
          WHEN gap <= 5 THEN '3-5'
          WHEN gap <= 7 THEN '6-7'
          WHEN gap <= 15 THEN '8-15'
          WHEN gap <= 30 THEN '16-30'
          WHEN gap <= 60 THEN '31-60'
          ELSE '60+'
        END AS faixa,
        COUNT(*)::int AS qtd,
        SUM(COALESCE(valor_total,0))::numeric(14,2) AS valor_total
      FROM (
        SELECT id, valor_total,
               CASE WHEN observacao ~ '\\((\\d+)d antes'
                    THEN (REGEXP_MATCH(observacao,'\\((\\d+)d antes'))[1]::int
                    ELSE NULL END AS gap
          FROM avaria_eventos
         WHERE tipo='devolucao' AND status='pendente'
      ) t
      GROUP BY faixa
      ORDER BY MIN(gap) NULLS LAST`);
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /debug-fontes — mostra o que tem em vendas_historico/compras_historico pra debug
router.get('/debug-fontes', compradorOuAdmin, async (req, res) => {
  try {
    const vh = await dbQuery(`
      SELECT COALESCE(tipo_saida,'NULL') AS tipo_saida,
             COUNT(*)::int AS qtd,
             MIN(data_venda) AS data_min,
             MAX(data_venda) AS data_max
        FROM vendas_historico
       WHERE data_venda >= '2026-04-01'
       GROUP BY tipo_saida
       ORDER BY qtd DESC`);
    const ch = await dbQuery(`
      SELECT COALESCE(tipo_entrada,'NULL') AS tipo_entrada,
             COUNT(*)::int AS qtd,
             MIN(data_entrada) AS data_min,
             MAX(data_entrada) AS data_max
        FROM compras_historico
       WHERE data_entrada >= '2026-04-01'
       GROUP BY tipo_entrada
       ORDER BY qtd DESC`);
    const eventos = await dbQuery(`
      SELECT tipo, status, COUNT(*)::int AS qtd FROM avaria_eventos
       GROUP BY tipo, status ORDER BY tipo, status`);
    // Sample 5 de vendas_historico com tipo_saida 'avaria' se houver
    const sampleAvaria = await dbQuery(`
      SELECT vh.loja_id, vh.codigobarra, vh.data_venda, vh.qtd_vendida, vh.tipo_saida
        FROM vendas_historico vh
       WHERE vh.tipo_saida = 'avaria' OR LOWER(COALESCE(vh.tipo_saida,'')) LIKE '%avar%'
       LIMIT 5`);
    res.json({
      vendas_historico_desde_abril: vh,
      compras_historico_desde_abril: ch,
      avaria_eventos_total: eventos,
      sample_avarias: sampleAvaria,
    });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /pendentes?loja_id=&tipo= — lista eventos com status='pendente'
router.get('/pendentes', compradorOuAdmin, async (req, res) => {
  try {
    const lojaQuery = req.query.loja_id ? parseInt(req.query.loja_id) : null;
    const tipo = req.query.tipo || null; // 'avaria' | 'devolucao' | null (ambos)
    const lojaUsr = req.usuario.loja_id;
    const lojasUsr = Array.isArray(req.usuario.lojas) ? req.usuario.lojas.map(Number) : [];
    let lojaId = lojaUsr || lojaQuery;
    if (!lojaId && lojasUsr.length === 1) lojaId = lojasUsr[0];

    const params = [];
    let where = `status = 'pendente'`;
    if (lojaId) { params.push(lojaId); where += ` AND loja_id = $${params.length}`; }
    if (tipo) { params.push(tipo); where += ` AND tipo = $${params.length}`; }

    const rows = await dbQuery(
      `SELECT id, loja_id, tipo, codigobarra, descricao_produto, data_evento, qtd,
              valor_unitario, valor_total,
              fornecedor_cnpj_sugerido, fornecedor_nome_sugerido, fonte_sugestao,
              preco_cobranca, preco_origem, fonte, observacao
         FROM avaria_eventos
        WHERE ${where}
        ORDER BY data_evento DESC, id DESC
        LIMIT 1000`, params);

    // Agrupa por (loja + fornecedor sugerido) — cada loja é cobrança separada
    const porGrupo = {};
    let semFornecedor = [];
    for (const r of rows) {
      const cnpj = r.fornecedor_cnpj_sugerido;
      if (!cnpj) { semFornecedor.push(r); continue; }
      const k = `${r.loja_id}|${cnpj}`;
      if (!porGrupo[k]) {
        porGrupo[k] = {
          loja_id: r.loja_id,
          cnpj,
          nome: r.fornecedor_nome_sugerido,
          fonte: r.fonte_sugestao,
          eventos: [],
          total_valor: 0,
          qtd_eventos: 0,
        };
      }
      porGrupo[k].eventos.push(r);
      porGrupo[k].qtd_eventos++;
      porGrupo[k].total_valor += parseFloat(r.valor_total || 0);
    }
    res.json({
      total: rows.length,
      sem_fornecedor: semFornecedor,
      por_loja_fornecedor: Object.values(porGrupo).sort((a,b) => {
        if (a.loja_id !== b.loja_id) return a.loja_id - b.loja_id;
        return b.total_valor - a.total_valor;
      }),
    });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /classificar — comprador define status + (opcional) muda fornecedor
// Body: { evento_ids: [], status, fornecedor_cnpj?, fornecedor_nome?, observacao? }
router.post('/classificar', compradorOuAdmin, async (req, res) => {
  const { evento_ids, status, fornecedor_cnpj, fornecedor_nome, observacao } = req.body || {};
  if (!Array.isArray(evento_ids) || !evento_ids.length)
    return res.status(400).json({ erro: 'evento_ids[] obrigatório' });
  if (!['tratado_cadastro','cobrar_fornecedor','perda_interna'].includes(status))
    return res.status(400).json({ erro: 'status inválido' });
  const por = req.usuario.email || req.usuario.usuario || req.usuario.nome || `id:${req.usuario.id}`;
  const cnpjN = fornecedor_cnpj ? String(fornecedor_cnpj).replace(/\D/g,'') : null;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Atualiza eventos
    await client.query(`
      UPDATE avaria_eventos
         SET status = $1,
             fornecedor_cnpj_definido = COALESCE($2, fornecedor_cnpj_sugerido),
             fornecedor_nome_definido = COALESCE($3, fornecedor_nome_sugerido),
             classificado_em = NOW(),
             classificado_por = $4,
             observacao = COALESCE($5, observacao)
       WHERE id = ANY($6::int[]) AND status = 'pendente'
    `, [status, cnpjN, fornecedor_nome || null, por, observacao || null, evento_ids]);

    let cobrancasCriadas = [];
    if (status === 'cobrar_fornecedor') {
      // Agrupa por (loja, fornecedor_cnpj_definido) e cria/atualiza cobranças.
      // Pra cobrança "agregada por semana" usamos chave (loja, cnpj, sem semana) — cada loja+forn = 1 cobrança aberta.
      // Estratégia simples: 1 cobrança por loja+forn no status 'pendente'. Se já existe, soma.
      const { rows: agrupado } = await client.query(`
        SELECT loja_id, fornecedor_cnpj_definido AS cnpj,
               MAX(fornecedor_nome_definido) AS nome,
               COUNT(*)::int AS qtd_eventos,
               SUM(COALESCE(valor_total,0))::numeric(14,2) AS valor_total
          FROM avaria_eventos
         WHERE id = ANY($1::int[])
           AND fornecedor_cnpj_definido IS NOT NULL
         GROUP BY loja_id, fornecedor_cnpj_definido
      `, [evento_ids]);

      for (const g of agrupado) {
        // Procura cobrança pendente existente pra esse fornecedor/loja
        const { rows: [existe] } = await client.query(
          `SELECT id, valor_total, qtd_itens, cr_debito_id FROM avarias_cobrancas
            WHERE loja_id=$1 AND fornecedor_cnpj=$2 AND status='pendente'
            ORDER BY id DESC LIMIT 1`, [g.loja_id, g.cnpj]);
        let cobId, crDebitoId;
        if (existe) {
          cobId = existe.id;
          crDebitoId = existe.cr_debito_id;
          await client.query(
            `UPDATE avarias_cobrancas
                SET valor_total = valor_total + $1,
                    qtd_itens = qtd_itens + $2,
                    fornecedor_nome = COALESCE($3, fornecedor_nome)
              WHERE id = $4`,
            [g.valor_total, g.qtd_eventos, g.nome, cobId]);
        } else {
          const { rows: [nv] } = await client.query(
            `INSERT INTO avarias_cobrancas
               (nota_id, loja_id, fornecedor_cnpj, fornecedor_nome, valor_total, qtd_itens, criado_por)
             VALUES (NULL, $1, $2, $3, $4, $5, $6) RETURNING id`,
            [g.loja_id, g.cnpj, g.nome, g.valor_total, g.qtd_eventos, por]);
          cobId = nv.id;
        }
        // Vincula eventos à cobrança
        await client.query(
          `UPDATE avaria_eventos SET avaria_cobranca_id = $1
            WHERE id = ANY($2::int[]) AND loja_id=$3 AND fornecedor_cnpj_definido=$4`,
          [cobId, evento_ids, g.loja_id, g.cnpj]);

        // ── Espelha em cr_debitos (contas a receber) ────────────────────
        // Pega TOTAL atual da cobrança (após soma) pra refletir certinho
        const { rows: [cobAtual] } = await client.query(
          `SELECT valor_total FROM avarias_cobrancas WHERE id=$1`, [cobId]);

        if (!crDebitoId) {
          // Cria novo cr_debitos vinculado (numero_nota será reescrito logo abaixo)
          const { rows: [novoDeb] } = await client.query(
            `INSERT INTO cr_debitos
               (fornecedor_cnpj, fornecedor_nome, loja_id, numero_nota, chave_nfe,
                data_emissao, natureza_operacao, valor_produtos, valor_total, status, importado_por)
             VALUES ($1,$2,$3,$4,$5,CURRENT_DATE,$6,$7,$7,'aberto','sistema-avarias')
             RETURNING id`,
            [g.cnpj, g.nome, g.loja_id, 'AVARIA #'+cobId, 'AVARIA-'+cobId,
             'Cobrança de avarias/devoluções', cobAtual.valor_total]);
          crDebitoId = novoDeb.id;
          await client.query(
            `UPDATE avarias_cobrancas SET cr_debito_id=$1 WHERE id=$2`,
            [crDebitoId, cobId]);
        } else {
          // Atualiza valor total do cr_debitos
          await client.query(
            `UPDATE cr_debitos SET valor_total=$1, valor_produtos=$1 WHERE id=$2`,
            [cobAtual.valor_total, crDebitoId]);
        }

        // Adiciona itens novos no cr_debito_itens (1 linha por evento classificado nesse lote)
        await client.query(`
          INSERT INTO cr_debito_itens (debito_id, codigo_barras, descricao, quantidade, valor_unitario, valor_total)
          SELECT $1, ae.codigobarra, ae.descricao_produto, ae.qtd, ae.valor_unitario, ae.valor_total
            FROM avaria_eventos ae
           WHERE ae.id = ANY($2::int[])
             AND ae.loja_id=$3 AND ae.fornecedor_cnpj_definido=$4`,
          [crDebitoId, evento_ids, g.loja_id, g.cnpj]);

        // Reescreve numero_nota e natureza com base nos tipos+notas dos eventos vinculados
        await atualizarRotuloCrDebito(client, cobId, crDebitoId, g.nome);

        cobrancasCriadas.push({ id: cobId, cr_debito_id: crDebitoId, cnpj: g.cnpj, valor: g.valor_total, qtd: g.qtd_eventos });
      }
    }

    await client.query('COMMIT');
    res.json({ ok: true, status, eventos_atualizados: evento_ids.length, cobrancas: cobrancasCriadas });
  } catch (e) {
    await client.query('ROLLBACK');
    res.status(500).json({ erro: e.message });
  } finally {
    client.release();
  }
});

// GET /debug-devolucao-ref — mostra info de uma devolução pendente pra entender match
router.get('/debug-devolucao-ref', autenticar, async (req, res) => {
  if (!['admin','ceo'].includes(req.usuario.perfil)) return res.status(403).json({ erro: 'apenas admin' });
  try {
    const sample = await dbQuery(`
      SELECT ae.id, ae.loja_id, ae.codigobarra, ae.data_evento, ae.fornecedor_cnpj_sugerido,
             dch.chave_nfe_compra_original,
             dch.chave_nfe AS chave_nfe_devolucao,
             dch.numero_nfe AS numero_devolucao,
             SUBSTRING(dch.chave_nfe_compra_original FROM 26 FOR 9) AS num_extraido,
             dci.devolucao_codigo
        FROM avaria_eventos ae
        LEFT JOIN devolucoes_compra_itens_historico dci ON dci.id = ae.fonte_id
        LEFT JOIN devolucoes_compra_historico dch
          ON dch.loja_id = dci.loja_id AND dch.devolucao_codigo = dci.devolucao_codigo
       WHERE ae.tipo='devolucao' AND ae.fonte='devolucoes_compra_historico'
       LIMIT 5`);
    // Pra cada sample, busca matches potenciais em compras_historico
    for (const s of sample) {
      const numN = s.num_extraido ? s.num_extraido.replace(/^0+/, '') || s.num_extraido : null;
      s.compras_match = numN ? await dbQuery(`
        SELECT numeronfe, fornecedor_cnpj, data_entrada
          FROM compras_historico
         WHERE loja_id = $1 AND REGEXP_REPLACE(numeronfe,'^0+','') = $2
         LIMIT 5`, [s.loja_id, numN]) : [];
    }
    // Contagens gerais
    const counts = await dbQuery(`
      SELECT COUNT(*)::int AS total,
             COUNT(chave_nfe_compra_original)::int AS com_ref,
             COUNT(*) FILTER (WHERE chave_nfe_compra_original IS NULL OR chave_nfe_compra_original='')::int AS sem_ref
        FROM devolucoes_compra_historico
       WHERE data_devolucao >= '2026-04-01'`);
    res.json({ sample, contagens: counts[0] });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /reclassificar-devolucoes — pra eventos de devolução existentes, calcula gap e auto-classifica
router.post('/reclassificar-devolucoes', autenticar, async (req, res) => {
  if (!['admin','ceo'].includes(req.usuario.perfil)) return res.status(403).json({ erro: 'apenas admin' });
  const client = await pool.connect();
  let auto_tratadas = 0, sem_origem = 0, mantidas_pendentes = 0;
  try {
    // Regra nova: classifica baseado no USUÁRIO que emitiu a devolução no Ecocentauro.
    // - Usuário com cargo='comprador' em usuarios_ecocentauro → cobrar fornecedor (pós-entrega)
    // - Outros usuários (cadastro, gerente, etc.) → tratado_cadastro (devolução no ato da entrega)
    // - Usuário NULL (KTR antigo) ou desconhecido → mantém pendente (admin classifica manualmente)
    const { rows: resultados } = await client.query(`
      UPDATE avaria_eventos ae
         SET status = CASE
                        WHEN ue.cargo = 'comprador' THEN ae.status -- mantém pendente p/ cobrança
                        WHEN dch.usuario IS NULL THEN ae.status     -- sem usuario, mantém p/ revisão manual
                        ELSE 'tratado_cadastro'
                      END,
             classificado_em = CASE
                                 WHEN ue.cargo = 'comprador' THEN ae.classificado_em
                                 WHEN dch.usuario IS NULL THEN ae.classificado_em
                                 ELSE NOW()
                               END,
             classificado_por = CASE
                                  WHEN ue.cargo = 'comprador' THEN ae.classificado_por
                                  WHEN dch.usuario IS NULL THEN ae.classificado_por
                                  ELSE 'auto:usuario_nao_comprador'
                                END,
             observacao = REGEXP_REPLACE(COALESCE(ae.observacao,''), ' \\| (Usuario|Origem|Entrada).*$','')
                          || COALESCE(' | Usuario ' || dch.usuario || COALESCE(' ('||ue.cargo||')', ' (?)'),'')
        FROM devolucoes_compra_itens_historico dci
        JOIN devolucoes_compra_historico dch
          ON dch.loja_id = dci.loja_id AND dch.devolucao_codigo = dci.devolucao_codigo
        LEFT JOIN usuarios_ecocentauro ue ON UPPER(ue.usuario) = UPPER(dch.usuario)
       WHERE ae.fonte_id = dci.id
         AND ae.fonte = 'devolucoes_compra_historico'
         AND ae.tipo = 'devolucao'
         AND ae.status = 'pendente'
       RETURNING ae.id, ae.status, dch.usuario AS usuario_emissor, ue.cargo
    `);
    auto_tratadas = resultados.filter(r => r.status === 'tratado_cadastro').length;
    mantidas_pendentes = resultados.filter(r => r.status === 'pendente' && r.cargo === 'comprador').length;
    sem_origem = resultados.filter(r => r.status === 'pendente' && !r.usuario_emissor).length;
    const total = auto_tratadas + mantidas_pendentes + sem_origem;
    res.json({ ok: true, total, auto_tratadas, mantidas_pendentes, sem_origem });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  } finally { client.release(); }
});

// POST /detectar-agora — força detecção (admin)
router.post('/detectar-agora', autenticar, async (req, res) => {
  if (!['admin','ceo'].includes(req.usuario.perfil)) return res.status(403).json({ erro: 'apenas admin' });
  try {
    const stats = await detectarAvarias();
    res.json({ ok: true, ...stats });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// ── Avarias_cobrancas (já existia, mantém) ─────────────────────────────

router.get('/cobrancas', compradorOuAdmin, async (req, res) => {
  try {
    const status = req.query.status ? String(req.query.status) : null;
    const fornCnpj = req.query.fornecedor_cnpj ? String(req.query.fornecedor_cnpj).replace(/\D/g,'') : null;
    const lojaQuery = req.query.loja_id ? parseInt(req.query.loja_id) : null;
    const params = [];
    let where = '1=1';
    if (status) { params.push(status); where += ` AND status = $${params.length}`; }
    if (fornCnpj) { params.push(fornCnpj); where += ` AND fornecedor_cnpj = $${params.length}`; }
    if (lojaQuery) { params.push(lojaQuery); where += ` AND loja_id = $${params.length}`; }
    const rows = await dbQuery(
      `SELECT * FROM avarias_cobrancas WHERE ${where}
       ORDER BY criado_em DESC LIMIT 500`, params);
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

router.get('/cobrancas/por-fornecedor', compradorOuAdmin, async (req, res) => {
  try {
    const rows = await dbQuery(
      `SELECT fornecedor_cnpj, fornecedor_nome,
              SUM(valor_total)::numeric(14,2) AS valor_total,
              SUM(qtd_itens)::int AS qtd_itens,
              COUNT(*)::int AS qtd_cobrancas,
              SUM(CASE WHEN status='pendente' THEN valor_total ELSE 0 END)::numeric(14,2) AS valor_pendente,
              SUM(CASE WHEN status='cobrado' THEN valor_total ELSE 0 END)::numeric(14,2) AS valor_cobrado
         FROM avarias_cobrancas
        GROUP BY fornecedor_cnpj, fornecedor_nome
        ORDER BY valor_pendente DESC NULLS LAST, valor_total DESC`);
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /cobrancas/relatorio-completo — agregado por fornecedor com 3 tipos: avaria, devolução, acordo
router.get('/cobrancas/relatorio-completo', compradorOuAdmin, async (req, res) => {
  try {
    const rows = await dbQuery(`
      WITH avaria AS (
        SELECT ac.fornecedor_cnpj, MAX(ac.fornecedor_nome) AS fornecedor_nome,
               SUM(ae.valor_total) FILTER (WHERE ae.tipo='avaria')::numeric(14,2) AS valor_avaria,
               SUM(ae.qtd) FILTER (WHERE ae.tipo='avaria')::numeric(14,3) AS qtd_avaria,
               SUM(ae.valor_total) FILTER (WHERE ae.tipo='devolucao')::numeric(14,2) AS valor_devolucao,
               SUM(ae.qtd) FILTER (WHERE ae.tipo='devolucao')::numeric(14,3) AS qtd_devolucao
          FROM avarias_cobrancas ac
          JOIN avaria_eventos ae ON ae.avaria_cobranca_id = ac.id
         WHERE ac.status <> 'cancelado'
         GROUP BY ac.fornecedor_cnpj
      ),
      acordo AS (
        SELECT REGEXP_REPLACE(COALESCE(fornecedor_cnpj,''),'\\D','','g') AS fornecedor_cnpj_n,
               MAX(fornecedor_nome) AS fornecedor_nome,
               SUM(valor_debito_atual)::numeric(14,2) AS valor_acordo,
               COUNT(*) FILTER (WHERE status IN ('ativo','fechado'))::int AS qtd_acordos
          FROM acordos_comerciais
         WHERE status IN ('ativo','fechado')
         GROUP BY REGEXP_REPLACE(COALESCE(fornecedor_cnpj,''),'\\D','','g')
      ),
      base AS (
        SELECT REGEXP_REPLACE(COALESCE(fornecedor_cnpj,''),'\\D','','g') AS cnpj, fornecedor_nome,
               valor_avaria, qtd_avaria, valor_devolucao, qtd_devolucao, 0::numeric AS valor_acordo, 0 AS qtd_acordos
          FROM avaria
        UNION ALL
        SELECT a.fornecedor_cnpj_n, a.fornecedor_nome, 0, 0, 0, 0, a.valor_acordo, a.qtd_acordos
          FROM acordo a
      )
      SELECT cnpj AS fornecedor_cnpj,
             MAX(fornecedor_nome) AS fornecedor_nome,
             COALESCE(SUM(valor_avaria),0)::numeric(14,2) AS valor_avaria,
             COALESCE(SUM(qtd_avaria),0)::numeric(14,3) AS qtd_avaria,
             COALESCE(SUM(valor_devolucao),0)::numeric(14,2) AS valor_devolucao,
             COALESCE(SUM(qtd_devolucao),0)::numeric(14,3) AS qtd_devolucao,
             COALESCE(SUM(valor_acordo),0)::numeric(14,2) AS valor_acordo,
             COALESCE(SUM(qtd_acordos),0)::int AS qtd_acordos,
             (COALESCE(SUM(valor_avaria),0)+COALESCE(SUM(valor_devolucao),0)+COALESCE(SUM(valor_acordo),0))::numeric(14,2) AS total_geral
        FROM base
       WHERE cnpj IS NOT NULL AND cnpj <> ''
       GROUP BY cnpj
       ORDER BY total_geral DESC`);
    // Totais gerais
    const totais = rows.reduce((acc, r) => ({
      avaria: acc.avaria + parseFloat(r.valor_avaria || 0),
      devolucao: acc.devolucao + parseFloat(r.valor_devolucao || 0),
      acordo: acc.acordo + parseFloat(r.valor_acordo || 0),
      total: acc.total + parseFloat(r.total_geral || 0),
    }), { avaria:0, devolucao:0, acordo:0, total:0 });
    res.json({ totais, fornecedores: rows });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

router.get('/cobrancas/:id/itens', compradorOuAdmin, async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    const rows = await dbQuery(
      `SELECT id, tipo, codigobarra, descricao_produto, data_evento, qtd,
              valor_unitario, valor_total, fonte, observacao
         FROM avaria_eventos
        WHERE avaria_cobranca_id = $1
        ORDER BY data_evento DESC, id DESC`, [id]);
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /cobrancas/regerar-rotulos — backfill: reescreve numero_nota de TODOS cr_debitos vinculados (admin)
router.post('/cobrancas/regerar-rotulos', autenticar, async (req, res) => {
  if (!['admin','ceo'].includes(req.usuario.perfil)) return res.status(403).json({ erro: 'apenas admin' });
  const client = await pool.connect();
  try {
    const { rows } = await client.query(
      `SELECT id, cr_debito_id, fornecedor_nome FROM avarias_cobrancas WHERE cr_debito_id IS NOT NULL`);
    await client.query('BEGIN');
    for (const c of rows) {
      await atualizarRotuloCrDebito(client, c.id, c.cr_debito_id, c.fornecedor_nome);
    }
    await client.query('COMMIT');
    res.json({ ok: true, atualizados: rows.length });
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch {}
    res.status(500).json({ erro: e.message });
  } finally { client.release(); }
});

router.post('/cobrancas/:id/marcar-cobrado', compradorOuAdmin, async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    const por = req.usuario.email || req.usuario.usuario || `id:${req.usuario.id}`;
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `UPDATE avarias_cobrancas SET status='cobrado', cobrado_em=NOW(), cobrado_por=$2 WHERE id=$1`,
        [id, por]);
      await client.query(
        `UPDATE avaria_eventos SET status='cobrado' WHERE avaria_cobranca_id=$1`, [id]);
      // Baixa também o cr_debitos vinculado
      await client.query(
        `UPDATE cr_debitos SET status='baixado'
          WHERE id = (SELECT cr_debito_id FROM avarias_cobrancas WHERE id=$1)`, [id]);
      await client.query('COMMIT');
    } catch (e) { await client.query('ROLLBACK'); throw e; }
    finally { client.release(); }
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

router.post('/cobrancas/:id/cancelar', compradorOuAdmin, async (req, res) => {
  const { motivo } = req.body || {};
  if (!motivo || motivo.trim().length < 5) return res.status(400).json({ erro: 'motivo obrigatório (≥5 caracteres)' });
  try {
    const id = parseInt(req.params.id);
    const por = req.usuario.email || req.usuario.usuario || `id:${req.usuario.id}`;
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `UPDATE avarias_cobrancas
            SET status='cancelado', cancelado_em=NOW(), cancelado_por=$2, observacao=$3
          WHERE id=$1`, [id, por, motivo.trim()]);
      await client.query(
        `UPDATE avaria_eventos SET status='perda_interna', observacao=COALESCE(observacao,'')||' | cancelado: '||$2 WHERE avaria_cobranca_id=$1`,
        [id, motivo.trim()]);
      // Remove o cr_debitos vinculado (cancelado = não devido)
      await client.query(`
        DELETE FROM cr_debito_itens
         WHERE debito_id = (SELECT cr_debito_id FROM avarias_cobrancas WHERE id=$1)`, [id]);
      await client.query(`
        DELETE FROM cr_debitos
         WHERE id = (SELECT cr_debito_id FROM avarias_cobrancas WHERE id=$1)`, [id]);
      await client.query('COMMIT');
    } catch (e) { await client.query('ROLLBACK'); throw e; }
    finally { client.release(); }
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// Reescreve numero_nota e natureza_operacao do cr_debitos com base nos eventos atuais.
// Regras:
//   - Todos avaria  → "AVARIA #{cobId} - FORN"
//   - Todos devolução, 1 nota  → "DEV Nº {numero_nfe} - FORN"
//   - Todos devolução, N notas → "DEV ({N} notas) - FORN"
//   - Misto → "AVARIA + DEV #{cobId} - FORN"
async function atualizarRotuloCrDebito(client, cobId, crDebitoId, fornecedorNome) {
  const { rows: [info] } = await client.query(`
    SELECT
      ARRAY_AGG(DISTINCT ae.tipo) AS tipos,
      ARRAY_AGG(DISTINCT dch.numero_nfe) FILTER (WHERE dch.numero_nfe IS NOT NULL AND TRIM(dch.numero_nfe) <> '') AS notas_dev
    FROM avaria_eventos ae
    LEFT JOIN devolucoes_compra_itens_historico dci
      ON ae.fonte='devolucoes_compra_historico' AND dci.id = ae.fonte_id
    LEFT JOIN devolucoes_compra_historico dch
      ON dch.loja_id = dci.loja_id AND dch.devolucao_codigo = dci.devolucao_codigo
    WHERE ae.avaria_cobranca_id = $1
  `, [cobId]);
  const tipos = info?.tipos || [];
  const notas = info?.notas_dev || [];
  let prefixo;
  if (tipos.length === 1 && tipos[0] === 'avaria') {
    prefixo = `AVARIA #${cobId}`;
  } else if (tipos.length === 1 && tipos[0] === 'devolucao') {
    if (notas.length === 1) prefixo = `DEV Nº ${notas[0]}`;
    else if (notas.length > 1) prefixo = `DEV (${notas.length} notas)`;
    else prefixo = `DEVOLUÇÃO #${cobId}`;
  } else {
    prefixo = `AVARIA + DEV #${cobId}`;
  }
  const numeroNota = `${prefixo} - ${(fornecedorNome || 'FORNECEDOR').trim()}`;
  const natureza = tipos.length === 1 && tipos[0] === 'devolucao'
    ? 'Cobrança de devoluções pós-entrega'
    : tipos.length === 1 && tipos[0] === 'avaria'
      ? 'Cobrança de avarias'
      : 'Cobrança de avarias e devoluções';
  await client.query(
    `UPDATE cr_debitos SET numero_nota=$1, natureza_operacao=$2 WHERE id=$3`,
    [numeroNota, natureza, crDebitoId]);
}

module.exports = router;
