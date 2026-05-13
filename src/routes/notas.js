const express = require('express');
const router = express.Router();
const multer = require('multer');
const { query, pool } = require('../db');
const { autenticar } = require('../auth');
const { parseNFe, MAX_XML_BYTES } = require('../parsers/nfe');
const { parseEmbalagem } = require('../parser_embalagem');
const { recalcularItensFornecedor } = require('./embalagens_fornecedor');

// Classifica o preço da NF vs custo do CD.
//   diferença <= R$ 0,01           → igual    (azul)
//   variação > 15% (qualquer lado) → auditagem (laranja, exige análise)
//   preco_nota > custo             → maior    (vermelho, vai render menos)
//   preco_nota < custo             → menor    (verde, ganho)
function classificarPreco(precoNota, custoFabrica) {
  if (custoFabrica == null || isNaN(precoNota) || precoNota <= 0) return 'sem_cadastro';
  const diff = precoNota - custoFabrica;
  if (Math.abs(diff) <= 0.01) return 'igual';
  const pctDiff = custoFabrica > 0 ? Math.abs(diff) / custoFabrica : 0;
  if (pctDiff > 0.15) return 'auditagem';
  return diff > 0 ? 'maior' : 'menor';
}

function normalizarDescricao(s) {
  if (!s) return null;
  return String(s)
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .toUpperCase()
    .replace(/\s+/g, ' ')
    .trim() || null;
}

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: MAX_XML_BYTES } });

function n(v) { return parseFloat(v) || 0; }

// Importa NF-e parseada: retorna { nota_id, duplicado }
async function importarNotaParseada(client, header, itens, loja_id, importado_por) {
  await client.query('BEGIN');

  // auto_emitida será decidida APÓS inserir os itens, baseado no CFOP (5927 = avaria).
  // CNPJ emitente continua sendo salvo pra referência.
  const emitCnpjN = (header.fornecedor_cnpj || '').replace(/\D/g, '');

  const ins = await client.query(`
    INSERT INTO notas_entrada
      (chave_nfe, numero_nota, serie, fornecedor_nome, fornecedor_cnpj, data_emissao,
       valor_total, importado_por, loja_id,
       tot_vprod, tot_vbc, tot_vicms, tot_vbcst, tot_vst, tot_vfcp_st,
       tot_vipi, tot_vdesc, tot_vfrete, tot_vseg, tot_voutro,
       cnpj_emitente, natureza_op)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
    ON CONFLICT (chave_nfe) DO NOTHING
    RETURNING id
  `, [header.chave_nfe, header.numero_nota, header.serie, header.fornecedor_nome,
      header.fornecedor_cnpj, header.data_emissao, header.valor_total, importado_por, loja_id,
      header.tot_vprod, header.tot_vbc, header.tot_vicms, header.tot_vbcst, header.tot_vst, header.tot_vfcp_st,
      header.tot_vipi, header.tot_vdesc, header.tot_vfrete, header.tot_vseg, header.tot_voutro,
      emitCnpjN || null, header.natureza_op || null]);

  if (!ins.rows.length) {
    await client.query('ROLLBACK');
    const existente = await client.query('SELECT id FROM notas_entrada WHERE chave_nfe = $1', [header.chave_nfe]);
    return { nota_id: existente.rows[0]?.id || null, duplicado: true };
  }

  const nota_id = ins.rows[0].id;

  // Lookup em batch dos custos no catálogo externo
  const eans = [...new Set(itens.flatMap(it => [it.ean_trib, it.ean_nota].filter(Boolean)))];
  const custoMap = new Map();
  if (eans.length) {
    const { rows: prods } = await client.query(
      'SELECT codigobarra, custoorigem FROM produtos_externo WHERE codigobarra = ANY($1) AND loja_id = $2',
      [eans, loja_id]
    );
    for (const p of prods) custoMap.set(p.codigobarra, parseFloat(p.custoorigem) || 0);
  }

  // Bulk insert dos itens via UNNEST
  if (itens.length) {
    const cols = {
      nota_id: [], numero_item: [], ean_nota: [], ean_trib: [], ean_validado: [], ean_fonte: [],
      cprod_fornecedor: [], cfop: [],
      descricao_nota: [], quantidade: [], preco_unitario_nota: [], preco_total_nota: [],
      custo_fabrica: [], status_preco: [], produto_novo: [],
      vprod: [], vdesc_item: [], vfrete_item: [], vseg_item: [], voutro_item: [],
      vicms_bc: [], vicms: [], vst_bc: [], vst: [], vfcp_st: [], vipi_bc: [], vipi: [],
      qtd_comercial: [], un_comercial: [], qtd_tributavel: [], un_tributavel: [],
      qtd_por_caixa_nfe: [], qtd_por_caixa_confianca: [], preco_unitario_caixa: [],
      qtd_em_unidades: [],
    };

    // Lookup em eans_fornecedor para itens sem match direto via produtos_externo
    const fornCnpjLookup = (header.fornecedor_cnpj || '').replace(/\D/g, '') || null;
    const eansForncMap = new Map(); // chave: cprod ou desc_norm → ean_validado
    if (fornCnpjLookup) {
      const cprods = [...new Set(itens.map(i => i.cprod_fornecedor).filter(Boolean))];
      const descsNorm = [...new Set(itens.map(i => normalizarDescricao(i.descricao_nota)).filter(Boolean))];
      if (cprods.length || descsNorm.length) {
        const { rows: assocs } = await client.query(
          `SELECT cprod_fornecedor, descricao_normalizada, ean_validado
             FROM eans_fornecedor
            WHERE fornecedor_cnpj = $1
              AND (cprod_fornecedor = ANY($2::text[]) OR descricao_normalizada = ANY($3::text[]))`,
          [fornCnpjLookup, cprods, descsNorm]
        );
        for (const a of assocs) {
          if (a.cprod_fornecedor) eansForncMap.set('c:' + a.cprod_fornecedor, a.ean_validado);
          if (a.descricao_normalizada) eansForncMap.set('d:' + a.descricao_normalizada, a.ean_validado);
        }
        // Carrega custos dos EANs encontrados via eans_fornecedor (caso ainda não estejam no custoMap)
        const eansAssoc = [...new Set(assocs.map(a => a.ean_validado).filter(e => !custoMap.has(e)))];
        if (eansAssoc.length) {
          const { rows: extras } = await client.query(
            'SELECT codigobarra, custoorigem FROM produtos_externo WHERE codigobarra = ANY($1) AND loja_id = $2',
            [eansAssoc, loja_id]
          );
          for (const p of extras) custoMap.set(p.codigobarra, parseFloat(p.custoorigem) || 0);
        }
      }
    }

    for (const it of itens) {
      let custo_fabrica = null, status_preco = 'sem_cadastro', produto_novo = true;
      let ean_matched = null, ean_fonte = null;
      const candidatos = [];
      if (it.ean_trib) candidatos.push([it.ean_trib, 'ean_trib']);
      if (it.ean_nota && it.ean_nota !== it.ean_trib) candidatos.push([it.ean_nota, 'ean_nota']);
      for (const [ean, fonte] of candidatos) {
        if (custoMap.has(ean)) {
          custo_fabrica = custoMap.get(ean);
          produto_novo = false;
          status_preco = classificarPreco(it.preco_unitario_nota, custo_fabrica);
          ean_matched = ean;
          ean_fonte = fonte;
          break;
        }
      }
      // Fallback: tenta achar via vínculo prévio em eans_fornecedor (cProd ou descricao)
      if (produto_novo && fornCnpjLookup) {
        const eanAssoc = (it.cprod_fornecedor && eansForncMap.get('c:' + it.cprod_fornecedor))
                      || eansForncMap.get('d:' + normalizarDescricao(it.descricao_nota));
        if (eanAssoc && custoMap.has(eanAssoc)) {
          custo_fabrica = custoMap.get(eanAssoc);
          produto_novo = false;
          status_preco = classificarPreco(it.preco_unitario_nota, custo_fabrica);
          ean_matched = eanAssoc;
          ean_fonte = it.cprod_fornecedor && eansForncMap.has('c:' + it.cprod_fornecedor) ? 'cprod_assoc' : 'desc_assoc';
        }
      }
      cols.nota_id.push(nota_id);
      cols.numero_item.push(it.numero_item);
      cols.ean_nota.push(it.ean_nota);
      cols.ean_trib.push(it.ean_trib);
      cols.ean_validado.push(ean_matched || it.ean_nota || it.ean_trib);
      cols.ean_fonte.push(ean_fonte);
      cols.cprod_fornecedor.push(it.cprod_fornecedor || null);
      cols.cfop.push(it.cfop || null);
      cols.descricao_nota.push(it.descricao_nota);
      cols.quantidade.push(it.quantidade);
      cols.preco_unitario_nota.push(it.preco_unitario_nota);
      cols.preco_total_nota.push(it.preco_total_nota);
      cols.custo_fabrica.push(custo_fabrica);
      cols.status_preco.push(status_preco);
      cols.produto_novo.push(produto_novo);
      cols.vprod.push(it.vprod);
      cols.vdesc_item.push(it.vdesc_item);
      cols.vfrete_item.push(it.vfrete_item);
      cols.vseg_item.push(it.vseg_item);
      cols.voutro_item.push(it.voutro_item);
      cols.vicms_bc.push(it.vicms_bc);
      cols.vicms.push(it.vicms);
      cols.vst_bc.push(it.vst_bc);
      cols.vst.push(it.vst);
      cols.vfcp_st.push(it.vfcp_st);
      cols.vipi_bc.push(it.vipi_bc);
      cols.vipi.push(it.vipi);
      cols.qtd_comercial.push(it.qtd_comercial ?? null);
      cols.un_comercial.push(it.un_comercial ?? null);
      cols.qtd_tributavel.push(it.qtd_tributavel ?? null);
      cols.un_tributavel.push(it.un_tributavel ?? null);
      cols.qtd_por_caixa_nfe.push(it.qtd_por_caixa_nfe ?? null);
      cols.qtd_por_caixa_confianca.push(it.qtd_por_caixa_confianca ?? null);
      cols.preco_unitario_caixa.push(it.preco_unitario_caixa ?? null);
      cols.qtd_em_unidades.push(it.qtd_em_unidades ?? null);
    }

    await client.query(`
      INSERT INTO itens_nota
        (nota_id, numero_item, ean_nota, ean_trib, ean_validado, ean_fonte, cprod_fornecedor, cfop, descricao_nota,
         quantidade, preco_unitario_nota, preco_total_nota, custo_fabrica, status_preco, produto_novo,
         vprod, vdesc_item, vfrete_item, vseg_item, voutro_item,
         vicms_bc, vicms, vst_bc, vst, vfcp_st, vipi_bc, vipi,
         qtd_comercial, un_comercial, qtd_tributavel, un_tributavel,
         qtd_por_caixa_nfe, qtd_por_caixa_confianca, preco_unitario_caixa, qtd_em_unidades)
      SELECT * FROM UNNEST(
        $1::int[], $2::int[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[], $8::text[], $9::text[],
        $10::numeric[], $11::numeric[], $12::numeric[], $13::numeric[], $14::text[], $15::bool[],
        $16::numeric[], $17::numeric[], $18::numeric[], $19::numeric[], $20::numeric[],
        $21::numeric[], $22::numeric[], $23::numeric[], $24::numeric[], $25::numeric[], $26::numeric[], $27::numeric[],
        $28::numeric[], $29::text[], $30::numeric[], $31::text[],
        $32::int[], $33::text[], $34::numeric[], $35::numeric[]
      )
    `, [
      cols.nota_id, cols.numero_item, cols.ean_nota, cols.ean_trib, cols.ean_validado, cols.ean_fonte,
      cols.cprod_fornecedor, cols.cfop, cols.descricao_nota, cols.quantidade, cols.preco_unitario_nota, cols.preco_total_nota,
      cols.custo_fabrica, cols.status_preco, cols.produto_novo,
      cols.vprod, cols.vdesc_item, cols.vfrete_item, cols.vseg_item, cols.voutro_item,
      cols.vicms_bc, cols.vicms, cols.vst_bc, cols.vst, cols.vfcp_st, cols.vipi_bc, cols.vipi,
      cols.qtd_comercial, cols.un_comercial, cols.qtd_tributavel, cols.un_tributavel,
      cols.qtd_por_caixa_nfe, cols.qtd_por_caixa_confianca, cols.preco_unitario_caixa, cols.qtd_em_unidades
    ]);

    // Detecta avaria por CFOP: qualquer item com CFOP 5927/6927/1927/2927/3927 marca a nota como auto_emitida pendente
    const CFOPS_AVARIA = new Set(['5927','6927','1927','2927','3927']);
    const temAvaria = itens.some(it => it.cfop && CFOPS_AVARIA.has(String(it.cfop).trim()));
    if (temAvaria) {
      await client.query(
        `UPDATE notas_entrada SET auto_emitida = TRUE WHERE id = $1 AND auto_emitida = FALSE`, [nota_id]);
    }

    const fornCnpj = (header.fornecedor_cnpj || '').replace(/\D/g, '') || null;
    const fornNome = header.fornecedor_nome || null;

    // Aplica embalagens VALIDADAS já cadastradas (admin/comprador anteriormente).
    // Sobrescreve qtd_por_caixa_nfe e recalcula preço/qtd_un quando achar match por
    // (ean, cnpj) ou (descricao_normalizada, cnpj).
    await client.query(`
      WITH match_emb AS (
        SELECT i.id AS item_id, ef.qtd_por_caixa, i.qtd_comercial, i.preco_total_nota
          FROM itens_nota i
          JOIN embalagens_fornecedor ef
            ON ef.status = 'validado'
           AND ef.qtd_por_caixa IS NOT NULL
           AND (
             (ef.ean IS NOT NULL
              AND NULLIF(LTRIM(ef.ean,'0'),'') = NULLIF(LTRIM(COALESCE(i.ean_validado, i.ean_nota,''),'0'),'')
              AND COALESCE(REGEXP_REPLACE(ef.fornecedor_cnpj,'\\D','','g'),'')
                = COALESCE(REGEXP_REPLACE($2::text,'\\D','','g'),''))
             OR
             (ef.ean IS NULL
              AND ef.descricao_normalizada IS NOT NULL
              AND ef.descricao_normalizada = UPPER(REGEXP_REPLACE(i.descricao_nota,'\\s+',' ','g'))
              AND COALESCE(REGEXP_REPLACE(ef.fornecedor_cnpj,'\\D','','g'),'')
                = COALESCE(REGEXP_REPLACE($2::text,'\\D','','g'),''))
           )
         WHERE i.nota_id = $1
           AND i.qtd_comercial > 0
      )
      UPDATE itens_nota i
         SET qtd_por_caixa_nfe = m.qtd_por_caixa,
             qtd_por_caixa_confianca = 'cadastrada',
             qtd_em_unidades = m.qtd_comercial * m.qtd_por_caixa,
             preco_unitario_nota = ROUND((m.preco_total_nota / (m.qtd_comercial * m.qtd_por_caixa::numeric))::numeric, 4),
             preco_unitario_caixa = CASE WHEN m.qtd_por_caixa > 1
               THEN ROUND((m.preco_total_nota / m.qtd_comercial)::numeric, 4)
               ELSE NULL END
        FROM match_emb m
       WHERE i.id = m.item_id
    `, [nota_id, fornCnpj]);

    // Embalagens vindas de NF-e fornecedor — registra na tabela própria (separada do CD).
    // Chave (ean, fornecedor_cnpj) — mesma EAN pode ter qtd diferente em fornecedores diferentes.
    // Não sobrescreve registros já validados.
    await client.query(`
      INSERT INTO embalagens_fornecedor
        (ean, descricao, fornecedor_cnpj, fornecedor_nome,
         qtd_sugerida_nfe, qtd_sugerida_nfe_em, qtd_sugerida_nfe_nota_id, qtd_sugerida_nfe_confianca,
         status)
      SELECT DISTINCT ON (NULLIF(LTRIM(COALESCE(i.ean_validado, i.ean_nota,''),'0'),''))
             NULLIF(LTRIM(COALESCE(i.ean_validado, i.ean_nota,''),'0'),'') AS ean,
             i.descricao_nota,
             $2, $3,
             i.qtd_por_caixa_nfe, NOW(), $1, i.qtd_por_caixa_confianca,
             'pendente_validacao'
        FROM itens_nota i
       WHERE i.nota_id = $1
         AND i.qtd_por_caixa_nfe IS NOT NULL
         AND NULLIF(LTRIM(COALESCE(i.ean_validado, i.ean_nota,''),'0'),'') IS NOT NULL
       ORDER BY NULLIF(LTRIM(COALESCE(i.ean_validado, i.ean_nota,''),'0'),''), i.id DESC
      ON CONFLICT (ean, COALESCE(fornecedor_cnpj, ''::character varying)) WHERE ean IS NOT NULL DO UPDATE
        SET descricao = COALESCE(EXCLUDED.descricao, embalagens_fornecedor.descricao),
            qtd_sugerida_nfe = EXCLUDED.qtd_sugerida_nfe,
            qtd_sugerida_nfe_em = EXCLUDED.qtd_sugerida_nfe_em,
            qtd_sugerida_nfe_nota_id = EXCLUDED.qtd_sugerida_nfe_nota_id,
            qtd_sugerida_nfe_confianca = EXCLUDED.qtd_sugerida_nfe_confianca,
            atualizado_em = NOW(),
            -- Se já estava validado e a sugestão diverge da qtd validada, volta pra pendente
            status = CASE
              WHEN embalagens_fornecedor.status = 'validado'
                AND embalagens_fornecedor.qtd_por_caixa = EXCLUDED.qtd_sugerida_nfe
                THEN 'validado'
              ELSE 'pendente_validacao'
            END
    `, [nota_id, fornCnpj, fornNome]);
  }

  await client.query('COMMIT');
  return { nota_id, duplicado: false };
}

async function enviarWebhookEntrada(nota_id) {
  const url = process.env.ACOUGUE_WEBHOOK_URL;
  const key = process.env.ACOUGUE_WEBHOOK_KEY;
  if (!url || !key) {
    console.warn(`[webhook] nota=${nota_id} ignorado — ACOUGUE_WEBHOOK_URL/KEY não configurados`);
    return;
  }
  try {
    const [nota] = await query('SELECT id, loja_id, numero_nota FROM notas_entrada WHERE id = $1', [nota_id]);
    if (!nota) return;

    // Coleta lotes da conferência (rodada mais recente) + auditoria
    const itens = await query(`
      SELECT
        COALESCE(i.ean_validado, i.ean_nota) AS barcode,
        i.descricao,
        cl.validade,
        cl.quantidade,
        cl.local_destino
      FROM itens_nota i
      JOIN conferencias_estoque ce ON ce.item_id = i.id
      JOIN conferencia_lotes cl ON cl.conferencia_id = ce.id
      WHERE i.nota_id = $1
        AND cl.validade IS NOT NULL
        AND ce.rodada = (
          SELECT MAX(rodada) FROM conferencias_estoque WHERE item_id = i.id
        )
      UNION ALL
      SELECT
        COALESCE(i.ean_validado, i.ean_nota) AS barcode,
        i.descricao,
        ai.validade,
        ai.qtd_contada AS quantidade,
        'Estoque' AS local_destino
      FROM itens_nota i
      JOIN auditoria_itens ai ON ai.item_id = i.id
      WHERE i.nota_id = $1
        AND ai.validade IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM conferencias_estoque ce
          JOIN conferencia_lotes cl ON cl.conferencia_id = ce.id
          WHERE ce.item_id = i.id AND cl.validade IS NOT NULL
        )
    `, [nota_id]);

    if (!itens.length) return;

    const payload = {
      loja_id: nota.loja_id,
      nota_id: nota.id,
      numero_nota: nota.numero_nota,
      itens: itens.map(i => ({
        barcode: i.barcode,
        descricao: i.descricao,
        validade: i.validade,
        quantidade: parseFloat(i.quantidade) || 0,
        local_destino: i.local_destino || 'Estoque'
      }))
    };

    const r = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-webhook-key': key },
      body: JSON.stringify(payload)
    });
    if (!r.ok) {
      const txt = await r.text().catch(() => '');
      console.error(`[webhook] nota=${nota_id} falhou status=${r.status}: ${txt.slice(0, 200)}`);
    } else {
      console.log(`[webhook] nota=${nota_id} enviada (${payload.itens.length} itens)`);
    }
  } catch (err) {
    console.error(`[webhook] nota=${nota_id} erro:`, err.message);
  }
}


// POST /api/notas/importar
router.post('/importar', autenticar, upload.single('xml'), async (req, res) => {
  const client = await pool.connect();
  try {
    if (!req.file) return res.status(400).json({ erro: 'Arquivo XML obrigatório' });

    const loja_id = parseInt(req.body.loja_id || req.usuario.loja_id) || null;
    if (!loja_id) return res.status(400).json({ erro: 'Selecione a loja de destino antes de importar' });

    const { header, itens } = await parseNFe(req.file.buffer);
    const { nota_id, duplicado } = await importarNotaParseada(client, header, itens, loja_id, req.usuario.nome);
    if (duplicado) return res.status(409).json({ erro: 'Nota já importada', nota_id });

    res.json({ ok: true, nota_id, total_itens: itens.length, fornecedor: header.fornecedor_nome });
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('[notas] importar:', err.message);
    res.status(500).json({ erro: err.message });
  } finally {
    client.release();
  }
});

// POST /api/notas/importar-comprador  — pedido_id opcional
router.post('/importar-comprador', autenticar, upload.single('xml'), async (req, res) => {
  const client = await pool.connect();
  try {
    if (!req.file) return res.status(400).json({ erro: 'Arquivo XML obrigatório' });
    const loja_id = parseInt(req.body.loja_id) || null;
    if (!loja_id) return res.status(400).json({ erro: 'loja_id obrigatório' });

    const { header, itens } = await parseNFe(req.file.buffer);
    const { nota_id, duplicado } = await importarNotaParseada(client, header, itens, loja_id, req.usuario.nome);
    if (duplicado) return res.status(409).json({ erro: 'Nota já importada', nota_id });

    res.json({ ok: true, nota_id, total_itens: itens.length, fornecedor: header.fornecedor_nome });
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    res.status(500).json({ erro: err.message });
  } finally { client.release(); }
});

// PATCH /api/notas/:id/desvincular-pedido
router.patch('/:id/desvincular-pedido', autenticar, async (req, res) => {
  const client = await pool.connect();
  try {
    const [nota] = await query('SELECT status, pedido_id FROM notas_entrada WHERE id=$1', [req.params.id]);
    if (!nota) return res.status(404).json({ erro: 'Nota não encontrada' });
    if (nota.status !== 'importada') return res.status(400).json({ erro: 'Desvínculo apenas em notas com status importada' });
    if (!nota.pedido_id) return res.status(400).json({ erro: 'Nota não possui pedido vinculado' });
    await client.query('BEGIN');
    await client.query(
      `UPDATE itens_nota SET fora_pedido=NULL, preco_pedido=NULL, qtd_pedido=NULL, item_pedido_id=NULL WHERE nota_id=$1`,
      [req.params.id]
    );
    await client.query(`UPDATE pedidos SET nota_id=NULL, status='validado' WHERE id=$1`, [nota.pedido_id]);
    await client.query(`UPDATE notas_entrada SET pedido_id=NULL WHERE id=$1`, [req.params.id]);
    await client.query('COMMIT');
    res.json({ ok: true });
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    res.status(500).json({ erro: err.message });
  } finally { client.release(); }
});

// PATCH /api/notas/:id/vincular-pedido — cruza itens nota × pedido
router.patch('/:id/vincular-pedido', autenticar, async (req, res) => {
  const client = await pool.connect();
  try {
    const { pedido_id } = req.body;
    if (!pedido_id) return res.status(400).json({ erro: 'pedido_id obrigatório' });
    const [nota] = await query('SELECT * FROM notas_entrada WHERE id=$1', [req.params.id]);
    if (!nota) return res.status(404).json({ erro: 'Nota não encontrada' });
    if (nota.status !== 'importada') return res.status(400).json({ erro: 'Nota não está em status importada' });

    const itensPedido = await query('SELECT * FROM itens_pedido WHERE pedido_id=$1', [pedido_id]);
    const itensNota   = await query('SELECT id, ean_nota, ean_trib FROM itens_nota WHERE nota_id=$1', [req.params.id]);

    await client.query('BEGIN');
    for (const in_ of itensNota) {
      let itemPedido = null;
      for (const ean of [in_.ean_trib, in_.ean_nota].filter(Boolean)) {
        itemPedido = itensPedido.find(ip => ip.codigo_barras === ean);
        if (itemPedido) break;
      }
      await client.query(
        `UPDATE itens_nota SET fora_pedido=$1, preco_pedido=$2, qtd_pedido=$3, item_pedido_id=$4 WHERE id=$5`,
        [!itemPedido,
         itemPedido ? parseFloat(itemPedido.preco_validado || itemPedido.preco_unitario) : null,
         itemPedido ? parseFloat(itemPedido.qtd_validada   || itemPedido.quantidade)     : null,
         itemPedido ? itemPedido.id : null,
         in_.id]
      );
    }
    await client.query('UPDATE notas_entrada SET pedido_id=$1 WHERE id=$2', [pedido_id, req.params.id]);
    await client.query('UPDATE pedidos SET nota_id=$1, status=$2 WHERE id=$3', [req.params.id, 'vinculado', pedido_id]);
    await client.query('COMMIT');

    const itensResult = await query('SELECT * FROM itens_nota WHERE nota_id=$1 ORDER BY numero_item', [req.params.id]);
    res.json({ ok: true, itens: itensResult });
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    res.status(500).json({ erro: err.message });
  } finally { client.release(); }
});

// PATCH /api/notas/:id/marcar-emergencial
router.patch('/:id/marcar-emergencial', autenticar, async (req, res) => {
  try {
    const [nota] = await query('SELECT status, pedido_id FROM notas_entrada WHERE id=$1', [req.params.id]);
    if (!nota) return res.status(404).json({ erro: 'Nota não encontrada' });
    if (nota.status !== 'importada') return res.status(400).json({ erro: 'Nota não está em status importada' });
    if (nota.pedido_id) return res.status(400).json({ erro: 'Nota vinculada a pedido — use fluxo normal' });
    await query('UPDATE notas_entrada SET status=$1, emergencial=TRUE WHERE id=$2', ['emergencial_pendente', req.params.id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// PATCH /api/notas/:id/aprovar-emergencial  — admin only
router.patch('/:id/aprovar-emergencial', autenticar, async (req, res) => {
  try {
    if (req.usuario.perfil !== 'admin') return res.status(403).json({ erro: 'Acesso restrito' });
    const [nota] = await query('SELECT status FROM notas_entrada WHERE id=$1', [req.params.id]);
    if (!nota) return res.status(404).json({ erro: 'Nota não encontrada' });
    if (nota.status !== 'emergencial_pendente') return res.status(400).json({ erro: 'Nota não está pendente de aprovação' });
    await query('UPDATE notas_entrada SET status=$1 WHERE id=$2', ['em_validacao_cadastro', req.params.id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// PATCH /api/notas/:id/itens/:itemId/embalagem  body: { qtd_por_caixa }
// Comprador ajusta a qtd por caixa direto na linha do item.
// Atualiza item, recalcula preço/qtd em UN, autocadastra em embalagens_fornecedor + log.
router.patch('/:id/itens/:itemId/embalagem', autenticar, async (req, res) => {
  const client = await pool.connect();
  try {
    const qtdNovo = parseInt(req.body?.qtd_por_caixa);
    if (!Number.isFinite(qtdNovo) || qtdNovo < 1) return res.status(400).json({ erro: 'qtd_por_caixa inválida (>= 1)' });

    const [item] = await query(
      `SELECT i.*, n.fornecedor_cnpj, n.fornecedor_nome
         FROM itens_nota i JOIN notas_entrada n ON n.id = i.nota_id
        WHERE i.id = $1 AND i.nota_id = $2`,
      [req.params.itemId, req.params.id]
    );
    if (!item) return res.status(404).json({ erro: 'Item não encontrado' });

    const qCom = parseFloat(item.qtd_comercial) || parseFloat(item.quantidade) || 0;
    const custoTotal = parseFloat(item.preco_total_nota) || 0;
    if (qCom <= 0) return res.status(400).json({ erro: 'Item sem qtd_comercial — não dá pra recalcular' });

    const qtdAnterior = parseInt(item.qtd_por_caixa_nfe) || 1;
    const totalUn = qCom * qtdNovo;
    const precoUn = parseFloat((custoTotal / totalUn).toFixed(4));
    const precoCx = qtdNovo > 1 ? parseFloat((custoTotal / qCom).toFixed(4)) : null;
    const ean = (item.ean_validado || item.ean_nota || '').replace(/\D/g, '').replace(/^0+/, '') || null;
    const fornCnpj = (item.fornecedor_cnpj || '').replace(/\D/g, '') || null;
    const descNorm = normalizarDescricao(item.descricao_nota);
    const usuario = req.usuario.nome || req.usuario.usuario;

    const novoStatusPreco = classificarPreco(precoUn, parseFloat(item.custo_fabrica));
    await client.query('BEGIN');
    await client.query(
      `UPDATE itens_nota
          SET qtd_por_caixa_nfe = $1, qtd_por_caixa_confianca = 'manual',
              qtd_em_unidades = $2, preco_unitario_nota = $3, preco_unitario_caixa = $4,
              status_preco = $5
        WHERE id = $6`,
      [qtdNovo > 1 ? qtdNovo : null, totalUn, precoUn, precoCx, novoStatusPreco, item.id]
    );

    // Auto-cadastra/atualiza embalagens_fornecedor
    let embId = null;
    if (ean) {
      const r = await client.query(
        `INSERT INTO embalagens_fornecedor
           (ean, descricao, descricao_normalizada, fornecedor_cnpj, fornecedor_nome,
            qtd_por_caixa, status, validado_em, validado_por)
         VALUES ($1::text, $2::text, $3::text, $4::text, $5::text, $6::int, 'validado', NOW(), $7::text)
         ON CONFLICT (ean, COALESCE(fornecedor_cnpj, ''::character varying)) WHERE ean IS NOT NULL DO UPDATE
           SET qtd_por_caixa = EXCLUDED.qtd_por_caixa,
               descricao = COALESCE(EXCLUDED.descricao, embalagens_fornecedor.descricao),
               descricao_normalizada = COALESCE(EXCLUDED.descricao_normalizada, embalagens_fornecedor.descricao_normalizada),
               fornecedor_nome = COALESCE(EXCLUDED.fornecedor_nome, embalagens_fornecedor.fornecedor_nome),
               status = 'validado',
               validado_em = NOW(),
               validado_por = $7::text,
               atualizado_em = NOW()
         RETURNING id`,
        [ean, item.descricao_nota, descNorm, fornCnpj, item.fornecedor_nome, qtdNovo, usuario]
      );
      embId = r.rows[0]?.id;
    } else if (descNorm) {
      const r = await client.query(
        `INSERT INTO embalagens_fornecedor
           (ean, descricao, descricao_normalizada, fornecedor_cnpj, fornecedor_nome,
            qtd_por_caixa, status, validado_em, validado_por)
         VALUES (NULL, $1::text, $2::text, $3::text, $4::text, $5::int, 'validado', NOW(), $6::text)
         ON CONFLICT (descricao_normalizada, COALESCE(fornecedor_cnpj, ''::character varying))
           WHERE ean IS NULL AND descricao_normalizada IS NOT NULL DO UPDATE
           SET qtd_por_caixa = EXCLUDED.qtd_por_caixa,
               descricao = COALESCE(EXCLUDED.descricao, embalagens_fornecedor.descricao),
               fornecedor_nome = COALESCE(EXCLUDED.fornecedor_nome, embalagens_fornecedor.fornecedor_nome),
               status = 'validado',
               validado_em = NOW(),
               validado_por = $6::text,
               atualizado_em = NOW()
         RETURNING id`,
        [item.descricao_nota, descNorm, fornCnpj, item.fornecedor_nome, qtdNovo, usuario]
      );
      embId = r.rows[0]?.id;
    }

    // Log
    await client.query(
      `INSERT INTO embalagens_fornecedor_log
         (embalagem_id, ean, descricao, fornecedor_cnpj, fornecedor_nome,
          qtd_anterior, qtd_novo, origem, nota_id, item_nota_id, alterado_por)
       VALUES ($1,$2,$3,$4,$5,$6,$7,'comprador_nota',$8,$9,$10)`,
      [embId, ean, item.descricao_nota, fornCnpj, item.fornecedor_nome,
       qtdAnterior, qtdNovo, item.nota_id, item.id, usuario]
    );

    await client.query('COMMIT');

    let propagados = 0;
    if (qtdNovo >= 2 && ean) {
      try {
        const r = await recalcularItensFornecedor(ean, fornCnpj, qtdNovo, { excluirItemId: item.id });
        propagados = r.recalculados;
      } catch (e) { console.error('[propagar embalagem]', e.message); }
    }

    res.json({
      ok: true,
      qtd_por_caixa_nfe: qtdNovo > 1 ? qtdNovo : null,
      qtd_em_unidades: totalUn,
      preco_unitario_nota: precoUn,
      preco_unitario_caixa: precoCx,
      embalagem_cadastro_id: embId,
      propagados
    });
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('[PATCH item embalagem]', e.message);
    res.status(500).json({ erro: e.message });
  } finally { client.release(); }
});

// POST /api/notas/reprocessar-qtd-caixa  body: { todas_notas?: bool, nota_id?: int, force?: bool }
// Roda parser de descrição em itens. force=true reprocessa MESMO os já marcados (recalcula preços).
// IMPORTANTE: definir antes de '/:id' pra não cair em router.get('/:id')
router.post('/reprocessar-qtd-caixa', autenticar, async (req, res) => {
  try {
    if (req.usuario.perfil !== 'admin') return res.status(403).json({ erro: 'Apenas admin' });
    const { todas_notas, nota_id, force } = req.body || {};
    if (!todas_notas && !nota_id) return res.status(400).json({ erro: 'Passe nota_id ou todas_notas=true' });
    const condNotaId = todas_notas ? '' : `AND i.nota_id = $1`;
    const condForce = force ? '' : `AND i.qtd_por_caixa_nfe IS NULL`;
    const params = todas_notas ? [] : [nota_id];
    const itens = await query(
      `SELECT i.id, i.nota_id, i.descricao_nota, i.ean_nota, i.ean_trib, i.ean_validado,
              i.qtd_comercial, i.un_comercial, i.qtd_tributavel, i.un_tributavel,
              i.preco_total_nota,
              n.fornecedor_cnpj, n.fornecedor_nome
         FROM itens_nota i
         JOIN notas_entrada n ON n.id = i.nota_id
        WHERE n.origem = 'nfe' ${condNotaId} ${condForce}`,
      params
    );
    let processados = 0, atualizados = 0, recalculados_preco = 0, revertidos = 0;
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      for (const it of itens) {
        processados++;
        const qCom = parseFloat(it.qtd_comercial) || 0;
        const custoTotal = parseFloat(it.preco_total_nota) || 0;
        const uCom = (it.un_comercial || '').toUpperCase();
        const uTrib = (it.un_tributavel || '').toUpperCase();
        // REGRA: cEAN == cEANTrib → emb=1, mesmo se descrição tiver NxN
        const eanDistinto = it.ean_nota && it.ean_trib && it.ean_nota !== it.ean_trib;

        if (!eanDistinto) {
          // Reverte pra emb=1 (caso BAUDISC: vendia individual, parser tinha colocado 30)
          if (qCom > 0) {
            const precoUn = parseFloat((custoTotal / qCom).toFixed(4));
            const itemFull = await client.query('SELECT custo_fabrica FROM itens_nota WHERE id=$1', [it.id]);
            const novoStatus = classificarPreco(precoUn, parseFloat(itemFull.rows[0]?.custo_fabrica));
            await client.query(
              `UPDATE itens_nota
                  SET qtd_por_caixa_nfe = NULL, qtd_por_caixa_confianca = NULL,
                      qtd_em_unidades = $1, preco_unitario_nota = $2, preco_unitario_caixa = NULL,
                      status_preco = $3
                WHERE id = $4 AND qtd_por_caixa_nfe IS NOT NULL`,
              [qCom, precoUn, novoStatus, it.id]
            );
            revertidos++;
          }
          continue;
        }

        const p = parseEmbalagem(it.descricao_nota || '');
        if (!p.qtd || p.qtd < 2) continue;
        const conf = p.confianca === 'alta' ? 'media' : 'baixa';
        // Recalcula preço só quando uCom == uTrib (caso B). Caso A (uCom != uTrib) já está correto.
        if (uCom && uTrib && uCom === uTrib && qCom > 0) {
          const totalUn = qCom * p.qtd;
          const precoUn = parseFloat((custoTotal / totalUn).toFixed(4));
          const precoCx = parseFloat((custoTotal / qCom).toFixed(4));
          const itemFull = await client.query('SELECT custo_fabrica FROM itens_nota WHERE id=$1', [it.id]);
          const novoStatus = classificarPreco(precoUn, parseFloat(itemFull.rows[0]?.custo_fabrica));
          await client.query(
            `UPDATE itens_nota
                SET qtd_por_caixa_nfe = $1, qtd_por_caixa_confianca = $2,
                    qtd_em_unidades = $3, preco_unitario_nota = $4, preco_unitario_caixa = $5,
                    status_preco = $6
              WHERE id = $7`,
            [p.qtd, conf, totalUn, precoUn, precoCx, novoStatus, it.id]
          );
          recalculados_preco++;
        } else {
          await client.query(
            `UPDATE itens_nota SET qtd_por_caixa_nfe = $1, qtd_por_caixa_confianca = $2 WHERE id = $3`,
            [p.qtd, conf, it.id]
          );
        }
        const ean = (it.ean_validado || it.ean_nota || '').replace(/^0+/, '');
        if (!ean) continue;
        const fornCnpj = (it.fornecedor_cnpj || '').replace(/\D/g, '') || null;
        await client.query(
          `INSERT INTO embalagens_fornecedor
             (ean, descricao, fornecedor_cnpj, fornecedor_nome,
              qtd_sugerida_nfe, qtd_sugerida_nfe_em, qtd_sugerida_nfe_nota_id, qtd_sugerida_nfe_confianca,
              status)
           VALUES ($1,$2,$3,$4,$5,NOW(),$6,$7,'pendente_validacao')
           ON CONFLICT (ean, COALESCE(fornecedor_cnpj, ''::character varying)) WHERE ean IS NOT NULL DO UPDATE
             SET descricao = COALESCE(EXCLUDED.descricao, embalagens_fornecedor.descricao),
                 qtd_sugerida_nfe = EXCLUDED.qtd_sugerida_nfe,
                 qtd_sugerida_nfe_em = EXCLUDED.qtd_sugerida_nfe_em,
                 qtd_sugerida_nfe_nota_id = EXCLUDED.qtd_sugerida_nfe_nota_id,
                 qtd_sugerida_nfe_confianca = EXCLUDED.qtd_sugerida_nfe_confianca,
                 atualizado_em = NOW(),
                 status = CASE
                   WHEN embalagens_fornecedor.status = 'validado'
                     AND embalagens_fornecedor.qtd_por_caixa = EXCLUDED.qtd_sugerida_nfe
                     THEN 'validado'
                   ELSE 'pendente_validacao'
                 END`,
          [ean, it.descricao_nota, fornCnpj, it.fornecedor_nome, p.qtd, it.nota_id, conf]
        );
        atualizados++;
      }
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      throw e;
    } finally { client.release(); }
    res.json({ ok: true, processados, atualizados, recalculados_preco, revertidos });
  } catch (e) {
    console.error('[reprocessar-qtd-caixa]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// GET /api/notas/diag-qtd-caixa?numero=N&serie=S — diagnóstico das colunas qCom/qTrib
// IMPORTANTE: definir antes de '/:id' pra não cair em router.get('/:id')
router.get('/diag-qtd-caixa', autenticar, async (req, res) => {
  try {
    const { numero, serie } = req.query;
    if (!numero) return res.status(400).json({ erro: 'numero obrigatório' });
    const params = [String(numero)];
    let where = `n.numero_nota = $1`;
    if (serie) { params.push(String(serie)); where += ` AND n.serie = $${params.length}`; }
    const [nota] = await query(`SELECT id, numero_nota, serie, fornecedor_nome, fornecedor_cnpj, origem, status FROM notas_entrada n WHERE ${where} ORDER BY n.id DESC LIMIT 1`, params);
    if (!nota) return res.status(404).json({ erro: 'Nota não encontrada' });
    const itens = await query(
      `SELECT id, numero_item, ean_nota, ean_validado, descricao_nota,
              qtd_comercial, un_comercial, qtd_tributavel, un_tributavel,
              qtd_por_caixa_nfe, qtd_por_caixa_confianca,
              quantidade, preco_unitario_nota, preco_unitario_caixa
         FROM itens_nota WHERE nota_id = $1 ORDER BY numero_item`,
      [nota.id]
    );
    const stats = {
      total: itens.length,
      com_qcom: itens.filter(i => i.qtd_comercial != null).length,
      com_qtrib: itens.filter(i => i.qtd_tributavel != null).length,
      com_ucom: itens.filter(i => i.un_comercial).length,
      com_utrib: itens.filter(i => i.un_tributavel).length,
      com_qtd_por_caixa: itens.filter(i => i.qtd_por_caixa_nfe).length,
    };
    const eans = itens.map(i => (i.ean_validado || i.ean_nota || '').replace(/^0+/, '')).filter(Boolean);
    const emFornEmb = eans.length
      ? await query(
          `SELECT ean, fornecedor_nome, qtd_sugerida_nfe, qtd_por_caixa, status
             FROM embalagens_fornecedor
            WHERE NULLIF(LTRIM(ean,'0'),'') = ANY($1::text[])`,
          [eans]
        )
      : [];
    res.json({ nota, stats, itens, em_embalagens_fornecedor: emFornEmb });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/notas
router.get('/', autenticar, async (req, res) => {
  try {
    const { perfil, lojas } = req.usuario;
    const needs_filter = perfil !== 'admin' && perfil !== 'rh' && perfil !== 'comprador';
    const params = [];
    const conds = [];

    if (needs_filter && lojas?.length) {
      const ids = lojas.map(Number).filter(n => !isNaN(n));
      if (ids.length) conds.push(`n.loja_id = ANY(ARRAY[${ids.join(',')}]::int[])`);
    }

    // Filtros multi-CD: destinos que NÃO são lojas
    // cd_destino_codigo=srv2-asafrio → mostra só notas onde o CD é destino
    if (req.query.cd_destino_codigo) {
      params.push(String(req.query.cd_destino_codigo).trim());
      conds.push(`n.cd_destino_codigo = $${params.length}`);
    }
    // origem_cd_codigo=srv2-asafrio → mostra só notas enviadas POR esse CD
    if (req.query.origem_cd_codigo) {
      params.push(String(req.query.origem_cd_codigo).trim());
      conds.push(`n.origem_cd_codigo = $${params.length}`);
    }

    if (req.query.status) {
      params.push(req.query.status);
      conds.push(`n.status = $${params.length}`);
    }

    if (req.query.fornecedor_cnpj) {
      params.push(req.query.fornecedor_cnpj.replace(/\D/g, ''));
      conds.push(`REGEXP_REPLACE(n.fornecedor_cnpj, '\\D', '', 'g') = $${params.length}`);
    }

    if (req.query.sem_pedido === '1') {
      conds.push(`n.pedido_id IS NULL`);
    }

    // origem: 'nfe' (fornecedores), 'cd' (transferências CD), 'transferencia_loja' (futuro)
    // pode vir múltiplos via origem=cd,transferencia_loja
    if (req.query.origem) {
      const origens = String(req.query.origem).split(',').map(s => s.trim()).filter(Boolean);
      if (origens.length === 1) {
        params.push(origens[0]);
        conds.push(`n.origem = $${params.length}`);
      } else if (origens.length > 1) {
        params.push(origens);
        conds.push(`n.origem = ANY($${params.length}::text[])`);
      }
    }

    // tratado_comprador=1 → notas que comprador validou (pedido vinculado OU emergencial já aprovada)
    if (req.query.tratado_comprador === '1') {
      conds.push(`(n.pedido_id IS NOT NULL OR (n.emergencial = TRUE AND n.status <> 'emergencial_pendente'))`);
    }

    // Por padrão, esconde transferências CD já validadas (encerradas) — só aparecem
    // quando o usuário solicitar explicitamente via incluir_validadas_cd=1.
    // Se o filtro de status já pediu 'validada' especificamente, não aplica esta regra.
    if (req.query.incluir_validadas_cd !== '1' && req.query.status !== 'validada') {
      conds.push(`NOT (n.origem IN ('cd','transferencia_loja') AND n.status = 'validada')`);
    }

    // finalizada_f e ESCONDIDA de todos (cadastro, estoque, auditor) — só Diretor (admin)
    // ve em /auditoria-finalizada-f via /api/finalizadas-f. Aqui na listagem geral, nunca aparece
    // a menos que o filtro de status seja explicitamente 'finalizada_f'.
    if (req.query.status !== 'finalizada_f') {
      conds.push(`COALESCE(n.status,'') <> 'finalizada_f'`);
    }

    const where = conds.length ? 'WHERE ' + conds.join(' AND ') : '';

    const notas = await query(`
      SELECT n.*,
        COUNT(i.id)::int AS total_itens,
        COUNT(CASE WHEN i.produto_novo THEN 1 END)::int AS total_novos,
        COUNT(CASE WHEN i.status_preco = 'divergente' THEN 1 END)::int AS total_divergentes
      FROM notas_entrada n
      LEFT JOIN itens_nota i ON i.nota_id = n.id
      ${where}
      GROUP BY n.id
      ORDER BY n.importado_em DESC
    `, params);
    res.json(notas);
  } catch (err) {
    res.status(500).json({ erro: err.message });
  }
});

// GET /api/notas/:id
router.get('/:id', autenticar, async (req, res) => {
  try {
    const [nota] = await query('SELECT * FROM notas_entrada WHERE id = $1', [req.params.id]);
    if (!nota) return res.status(404).json({ erro: 'Nota não encontrada' });

    // Pra transferências CD: enriquece com cod_barra loja (produtos_embalagem) + qtd_embalagem + preço última entrada
    const isTransfCD = nota.origem === 'cd';

    const itens = await query(`
      SELECT i.*,
        ${isTransfCD ? `
          pe.ean_principal_jrlira AS cod_barra_loja,
          pe.qtd_embalagem        AS qtd_embalagem_loja,
          pe.descricao_atual      AS descricao_cadastro_cd,
          (SELECT i2.preco_unitario_nota
             FROM itens_nota i2
             JOIN notas_entrada n2 ON n2.id = i2.nota_id
            WHERE n2.origem = 'cd'
              AND i2.cd_pro_codi = i.cd_pro_codi
              AND i2.cd_pro_codi IS NOT NULL
              AND n2.id <> i.nota_id
              AND n2.loja_id = $2
            ORDER BY n2.data_emissao DESC NULLS LAST, n2.id DESC
            LIMIT 1) AS preco_unitario_ultima_entrada_cd,
          (SELECT json_agg(json_build_object(
            'lote_idx', lc.lote_idx, 'qtd_caixas', lc.qtd_caixas,
            'qtd_unidades', lc.qtd_unidades, 'qtd_total', lc.qtd_total,
            'validade', lc.validade, 'conferido_por', lc.conferido_por,
            'conferido_em', lc.conferido_em
          ) ORDER BY lc.lote_idx) FROM lotes_conferidos lc WHERE lc.item_id = i.id) AS lotes_contados,
        ` : ''}
        (SELECT json_agg(json_build_object(
          'id', c.id, 'rodada', c.rodada, 'qtd_contada', c.qtd_contada, 'status', c.status,
          'conferido_por', c.conferido_por, 'conferido_em', c.conferido_em,
          'lotes', (SELECT json_agg(json_build_object('lote', l.lote, 'validade', l.validade, 'quantidade', l.quantidade))
                    FROM conferencia_lotes l WHERE l.conferencia_id = c.id)
        ) ORDER BY c.rodada) FROM conferencias_estoque c WHERE c.item_id = i.id) AS conferencias,
        (SELECT row_to_json(a.*) FROM auditoria_itens a WHERE a.item_id = i.id) AS auditoria,
        (SELECT json_build_object(
            'devolucao_id', di.devolucao_id,
            'qtd_caixas', di.qtd_caixas,
            'qtd_total', di.qtd_total,
            'valor_total', di.valor_total,
            'origem_tipo', di.origem_tipo,
            'devolucao_status', dv.status
          )
           FROM devolucoes_itens di
           JOIN devolucoes dv ON dv.id = di.devolucao_id
          WHERE di.item_nota_id = i.id
            AND dv.status NOT IN ('cancelada','enviada','concluida')
          ORDER BY di.id DESC LIMIT 1) AS devolucao_pendente
      FROM itens_nota i
      ${isTransfCD ? `LEFT JOIN produtos_embalagem pe ON pe.mat_codi = i.cd_pro_codi` : ''}
      WHERE i.nota_id = $1
      ORDER BY i.numero_item
    `, isTransfCD ? [req.params.id, nota.loja_id] : [req.params.id]);
    res.json({ nota, itens });
  } catch (err) {
    res.status(500).json({ erro: err.message });
  }
});

// PATCH /api/notas/:id/itens/:itemId/remap-ean
// Atualiza o EAN do item, salva vínculo (cnpj+cprod ou cnpj+desc) em eans_fornecedor
// e retroaplica em itens compatíveis de notas em status editável.
router.patch('/:id/itens/:itemId/remap-ean', autenticar, async (req, res) => {
  const client = await pool.connect();
  try {
    const { ean_novo } = req.body;
    const eanLimpo = String(ean_novo || '').trim();
    if (!eanLimpo) return res.status(400).json({ erro: 'EAN obrigatório' });
    const [item] = await query(
      `SELECT i.*, n.fornecedor_cnpj, n.fornecedor_nome, n.loja_id
         FROM itens_nota i
         JOIN notas_entrada n ON n.id = i.nota_id
        WHERE i.id = $1 AND i.nota_id = $2`,
      [req.params.itemId, req.params.id]
    );
    if (!item) return res.status(404).json({ erro: 'Item não encontrado' });
    const prods = await query(
      'SELECT custoorigem FROM produtos_externo WHERE codigobarra = $1 AND loja_id = $2 LIMIT 1',
      [eanLimpo, item.loja_id]
    );
    if (!prods.length) {
      // fallback sem filtro de loja (catálogo geral)
      const prodsGeral = await query('SELECT custoorigem FROM produtos_externo WHERE codigobarra = $1 LIMIT 1', [eanLimpo]);
      if (!prodsGeral.length) return res.status(404).json({ erro: 'EAN não encontrado no cadastro' });
      prods.push(prodsGeral[0]);
    }
    const custo_fabrica = parseFloat(prods[0].custoorigem) || 0;

    const fornCnpj = (item.fornecedor_cnpj || '').replace(/\D/g, '') || null;
    const cprod = item.cprod_fornecedor || null;
    const descNorm = normalizarDescricao(item.descricao_nota);

    await client.query('BEGIN');

    // Atualiza o item da nota corrente
    const status_preco = classificarPreco(parseFloat(item.preco_unitario_nota), custo_fabrica);
    await client.query(
      `UPDATE itens_nota SET ean_validado=$1, custo_fabrica=$2, status_preco=$3,
              produto_novo=FALSE, validado_cadastro=TRUE, ean_fonte='manual'
        WHERE id=$4`,
      [eanLimpo, custo_fabrica, status_preco, item.id]
    );

    // Salva vínculo em eans_fornecedor (preferindo cProd; senão descricao_normalizada)
    let assocCount = 0;
    if (fornCnpj && (cprod || descNorm)) {
      if (cprod) {
        await client.query(
          `INSERT INTO eans_fornecedor (fornecedor_cnpj, fornecedor_nome, cprod_fornecedor, descricao_normalizada, ean_validado, associado_por, atualizado_em)
           VALUES ($1,$2,$3,$4,$5,$6,NOW())
           ON CONFLICT (fornecedor_cnpj, cprod_fornecedor) WHERE cprod_fornecedor IS NOT NULL DO UPDATE
             SET ean_validado = EXCLUDED.ean_validado,
                 descricao_normalizada = EXCLUDED.descricao_normalizada,
                 associado_por = EXCLUDED.associado_por,
                 atualizado_em = NOW()`,
          [fornCnpj, item.fornecedor_nome, cprod, descNorm, eanLimpo, req.usuario?.nome || null]
        );
      } else {
        await client.query(
          `INSERT INTO eans_fornecedor (fornecedor_cnpj, fornecedor_nome, cprod_fornecedor, descricao_normalizada, ean_validado, associado_por, atualizado_em)
           VALUES ($1,$2,NULL,$3,$4,$5,NOW())
           ON CONFLICT (fornecedor_cnpj, descricao_normalizada) WHERE cprod_fornecedor IS NULL AND descricao_normalizada IS NOT NULL DO UPDATE
             SET ean_validado = EXCLUDED.ean_validado,
                 associado_por = EXCLUDED.associado_por,
                 atualizado_em = NOW()`,
          [fornCnpj, item.fornecedor_nome, descNorm, eanLimpo, req.usuario?.nome || null]
        );
      }

      // Retroaplica em itens em notas editáveis com mesma chave
      const STATUS_EDITAVEIS = ['importada','emergencial_pendente','em_validacao_cadastro'];
      const upd = await client.query(
        `WITH alvos AS (
           SELECT i.id
             FROM itens_nota i
             JOIN notas_entrada n ON n.id = i.nota_id
            WHERE n.status = ANY($1::text[])
              AND COALESCE(REGEXP_REPLACE(n.fornecedor_cnpj,'\\D','','g'),'') = $2
              AND i.id <> $3
              AND COALESCE(i.produto_novo, FALSE) = TRUE
              AND (
                ($4::text IS NOT NULL AND i.cprod_fornecedor = $4)
                OR ($4::text IS NULL AND $5::text IS NOT NULL
                    AND UPPER(REGEXP_REPLACE(COALESCE(i.descricao_nota,''),'\\s+',' ','g')) = $5)
              )
         )
         UPDATE itens_nota i
            SET ean_validado = $6::text,
                custo_fabrica = COALESCE((SELECT custoorigem FROM produtos_externo WHERE codigobarra=$6::text AND loja_id=n.loja_id LIMIT 1), $7::numeric),
                produto_novo = FALSE,
                ean_fonte = CASE WHEN $4::text IS NOT NULL THEN 'cprod_assoc' ELSE 'desc_assoc' END
           FROM notas_entrada n
          WHERE i.id IN (SELECT id FROM alvos)
            AND n.id = i.nota_id
          RETURNING i.id`,
        [STATUS_EDITAVEIS, fornCnpj, item.id, cprod, descNorm, eanLimpo, custo_fabrica]
      );
      assocCount = upd.rowCount;

      // Reclassifica status_preco dos itens retroaplicados
      if (assocCount > 0) {
        await client.query(
          `UPDATE itens_nota
              SET status_preco = CASE
                WHEN custo_fabrica IS NULL OR custo_fabrica = 0 THEN 'sem_cadastro'
                WHEN ABS(preco_unitario_nota - custo_fabrica) < 0.01 THEN 'igual'
                WHEN preco_unitario_nota < custo_fabrica THEN 'menor'
                WHEN preco_unitario_nota > custo_fabrica * 1.15 THEN 'auditagem'
                ELSE 'maior'
              END
            WHERE ean_validado = $1
              AND nota_id IN (SELECT id FROM notas_entrada WHERE status = ANY($2::text[]))`,
          [eanLimpo, STATUS_EDITAVEIS]
        );
      }
    }

    await client.query('COMMIT');
    res.json({ ok: true, status_preco, custo_fabrica, retroaplicados: assocCount });
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    res.status(500).json({ erro: err.message });
  } finally { client.release(); }
});

// PATCH /api/notas/:id/reprocessar — re-checar EANs no produtos_externo sem reimportar XML
router.patch('/:id/reprocessar', autenticar, async (req, res) => {
  const client = await pool.connect();
  try {
    const [nota] = await query('SELECT * FROM notas_entrada WHERE id=$1', [req.params.id]);
    if (!nota) return res.status(404).json({ erro: 'Nota não encontrada' });
    if (nota.status !== 'importada') return res.status(400).json({ erro: 'Reprocessamento apenas em notas com status importada' });

    const itens = await query('SELECT * FROM itens_nota WHERE nota_id=$1', [req.params.id]);
    await client.query('BEGIN');
    let atualizados = 0;
    for (const item of itens) {
      let custo_fabrica = null, status_preco = 'sem_cadastro', produto_novo = true;
      let ean_matched = null, ean_fonte = null;
      const eansToCheck = [];
      if (item.ean_trib) eansToCheck.push([item.ean_trib, 'ean_trib']);
      if (item.ean_nota && item.ean_nota !== item.ean_trib) eansToCheck.push([item.ean_nota, 'ean_nota']);
      for (const [ean, fonte] of eansToCheck) {
        const { rows: prods } = await client.query(
          'SELECT custoorigem FROM produtos_externo WHERE codigobarra=$1 AND loja_id=$2 LIMIT 1',
          [ean, nota.loja_id]
        );
        if (prods.length) {
          custo_fabrica = parseFloat(prods[0].custoorigem) || 0;
          produto_novo  = false;
          status_preco  = classificarPreco(parseFloat(item.preco_unitario_nota), custo_fabrica);
          ean_matched   = ean;
          ean_fonte     = fonte;
          break;
        }
      }
      await client.query(
        `UPDATE itens_nota SET custo_fabrica=$1, status_preco=$2, produto_novo=$3, ean_validado=$4, ean_fonte=$5 WHERE id=$6`,
        [custo_fabrica, status_preco, produto_novo, ean_matched || item.ean_nota || item.ean_trib, ean_fonte, item.id]
      );
      atualizados++;
    }
    await client.query('COMMIT');
    res.json({ ok: true, atualizados });
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    res.status(500).json({ erro: err.message });
  } finally { client.release(); }
});

// DELETE /api/notas/:id — admin only.
// Sem ?force: apenas status 'importada' (atalho seguro).
// Com ?force=true: qualquer status, mas bloqueia se a nota já tem conferência iniciada/auditoria
// (evita perder trabalho do estoque). Libera pedido vinculado antes do delete.
router.delete('/:id', autenticar, async (req, res) => {
  const client = await pool.connect();
  try {
    if (req.usuario.perfil !== 'admin') return res.status(403).json({ erro: 'Exclusão restrita ao administrador' });
    const force = req.query.force === 'true' || req.query.force === '1';
    const [nota] = await query('SELECT id, status, pedido_id FROM notas_entrada WHERE id = $1', [req.params.id]);
    if (!nota) return res.status(404).json({ erro: 'Nota não encontrada' });

    if (!force && nota.status !== 'importada') {
      return res.status(400).json({ erro: 'Apenas notas com status "Importada" podem ser excluídas. Use force=true se for admin reimportando.' });
    }

    // Bloqueio de segurança: se houver conferência ou auditoria registrada, exige confirmação extra.
    if (force) {
      const [trab] = await query(
        `SELECT
            (SELECT COUNT(*) FROM conferencias_estoque ce JOIN itens_nota i ON i.id=ce.item_id WHERE i.nota_id=$1)::int AS confs,
            (SELECT COUNT(*) FROM auditoria_itens ai JOIN itens_nota i ON i.id=ai.item_id WHERE i.nota_id=$1)::int AS audits,
            (SELECT COUNT(*) FROM devolucoes WHERE nota_id=$1 AND status<>'cancelada')::int AS devs`,
        [req.params.id]
      );
      if (req.query.confirm !== 'true' && (trab.confs > 0 || trab.audits > 0 || trab.devs > 0)) {
        return res.status(409).json({
          erro: 'Nota possui trabalho registrado',
          confs: trab.confs, audits: trab.audits, devs: trab.devs,
          dica: 'Adicione &confirm=true pra forçar a exclusão (perderá conferência/auditoria/devolução).'
        });
      }
    }

    await client.query('BEGIN');
    // Libera pedido (FK sem CASCADE)
    if (nota.pedido_id) {
      await client.query(`UPDATE pedidos SET nota_id=NULL, status='validado' WHERE id=$1`, [nota.pedido_id]);
    }
    await client.query('DELETE FROM notas_entrada WHERE id = $1', [req.params.id]);
    await client.query('COMMIT');
    res.json({ ok: true, status_anterior: nota.status, pedido_liberado: nota.pedido_id || null });
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('[DELETE nota]', err.message);
    res.status(500).json({ erro: err.message });
  } finally {
    client.release();
  }
});

// PATCH /api/notas/:id/liberar-cadastro  — comprador libera para cadastro (requer pedido vinculado)
router.patch('/:id/liberar-cadastro', autenticar, async (req, res) => {
  try {
    const [nota] = await query('SELECT status, pedido_id FROM notas_entrada WHERE id=$1', [req.params.id]);
    if (!nota) return res.status(404).json({ erro: 'Nota não encontrada' });
    if (nota.status !== 'importada') return res.status(400).json({ erro: 'Nota não está em estado válido para liberação' });
    if (!nota.pedido_id) return res.status(400).json({ erro: 'Vincule a nota a um pedido antes de liberar, ou marque como emergencial' });
    await query('UPDATE notas_entrada SET status=$1 WHERE id=$2', ['em_validacao_cadastro', req.params.id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// ── FLUXO TRANSFERÊNCIAS CD ─────────────────────────────────────
// em_transito → recebida → em_conferencia → conferida (ou auditagem) → validada

function _quemSou(req) { return req.usuario.nome || req.usuario.usuario; }

// PATCH /api/notas/:id/marcar-recebida — estoque marca a chegada física
router.patch('/:id/marcar-recebida', autenticar, async (req, res) => {
  try {
    const [nota] = await query('SELECT status, origem FROM notas_entrada WHERE id=$1', [req.params.id]);
    if (!nota) return res.status(404).json({ erro: 'Nota não encontrada' });
    if (nota.origem !== 'cd' && nota.origem !== 'transferencia_loja') {
      return res.status(400).json({ erro: 'Fluxo apenas para transferências (CD ou entre lojas)' });
    }
    if (nota.status !== 'em_transito') return res.status(400).json({ erro: `Status atual ${nota.status} não permite marcar recebida` });
    await query(
      `UPDATE notas_entrada SET status='recebida', recebida_em=NOW(), recebida_por=$2 WHERE id=$1`,
      [req.params.id, _quemSou(req)]
    );
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// PATCH /api/notas/:id/liberar-estoque-transf — cadastro libera para estoque conferir (transferência)
// ANTES de mudar pra em_conferencia, re-sincroniza itens do UltraSyst origem pra pegar
// edições/cancelamentos feitos no CD após a importação inicial.
router.patch('/:id/liberar-estoque-transf', autenticar, async (req, res) => {
  try {
    const [nota] = await query('SELECT status, origem FROM notas_entrada WHERE id=$1', [req.params.id]);
    if (!nota) return res.status(404).json({ erro: 'Nota não encontrada' });
    if (nota.origem !== 'cd' && nota.origem !== 'transferencia_loja') {
      return res.status(400).json({ erro: 'Fluxo apenas para transferências' });
    }
    if (nota.status !== 'recebida') return res.status(400).json({ erro: `Status atual ${nota.status} não permite liberar` });

    // Re-sync itens do CD ANTES da conferência (resolve casos de edição/cancelamento no CD)
    let resync = null;
    try {
      const { resyncItensNotaCd } = require('../resync_itens_nota_cd');
      resync = await resyncItensNotaCd(Number(req.params.id));
      if (resync?.cancelada) {
        return res.status(409).json({
          erro: 'Nota foi CANCELADA no CD após importação. Não pode ser conferida.',
          motivo: resync.motivo,
          cancelada: true,
        });
      }
    } catch (e) {
      console.error('[liberar-estoque-transf resync]', e.message);
      // Não bloqueia se resync falhar — apenas loga e segue (offline-tolerant)
    }

    await query(
      `UPDATE notas_entrada SET status='em_conferencia', liberada_em=NOW(), liberada_por=$2 WHERE id=$1`,
      [req.params.id, _quemSou(req)]
    );
    res.json({ ok: true, resync });
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// POST /api/notas/:id/resync-cd — força re-sync manual dos itens (admin/estoque)
// Útil se loja já está em conferência e precisa re-sincronizar manualmente.
router.post('/:id/resync-cd', autenticar, async (req, res) => {
  try {
    const { resyncItensNotaCd } = require('../resync_itens_nota_cd');
    const r = await resyncItensNotaCd(Number(req.params.id));
    res.json(r);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// PATCH /api/notas/:id/finalizar-conferencia-transf — estoque finaliza
//   body: { itens: [{ item_id, lotes: [{ qtd_caixas, qtd_unidades, validade }] }] }
//   - persiste lotes em lotes_conferidos
//   - calcula divergência (soma vs qtd × emb esperada)
//   - sem divergência → conferida; com divergência → auditagem
router.patch('/:id/finalizar-conferencia-transf', autenticar, async (req, res) => {
  try {
    const itensInput = Array.isArray(req.body?.itens) ? req.body.itens : [];
    const [nota] = await query('SELECT status, origem FROM notas_entrada WHERE id=$1', [req.params.id]);
    if (!nota) return res.status(404).json({ erro: 'Nota não encontrada' });
    if (nota.origem !== 'cd' && nota.origem !== 'transferencia_loja') {
      return res.status(400).json({ erro: 'Fluxo apenas para transferências' });
    }
    if (nota.status !== 'em_conferencia') return res.status(400).json({ erro: `Status atual ${nota.status} não permite finalizar conferência` });

    // Carrega itens da nota com qtd_embalagem + dados pra registrar divergências
    const itensNota = await query(
      `SELECT i.id, i.cd_pro_codi, i.quantidade, i.descricao_nota, i.ean_nota,
              i.preco_unitario_nota, COALESCE(pe.qtd_embalagem,1)::numeric AS emb
         FROM itens_nota i
         LEFT JOIN produtos_embalagem pe ON pe.mat_codi = i.cd_pro_codi
        WHERE i.nota_id=$1`,
      [req.params.id]
    );
    const mapaItens = Object.fromEntries(itensNota.map(i => [i.id, i]));
    const [notaInfo] = await query('SELECT loja_id FROM notas_entrada WHERE id=$1', [req.params.id]);

    let comDivergencia = false;
    const detalhes = [];
    const quem = _quemSou(req);

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      // Apaga lotes/divergências/validades pendentes desta nota (re-conferência)
      await client.query(`DELETE FROM lotes_conferidos WHERE nota_id=$1`, [req.params.id]);
      await client.query(`DELETE FROM auditagem_divergencias WHERE nota_id=$1 AND status='pendente'`, [req.params.id]);
      await client.query(`DELETE FROM validades_em_risco WHERE nota_id=$1 AND status='pendente'`, [req.params.id]);

      let temValidadeEmRisco = false;

      for (const ent of itensInput) {
        const it = mapaItens[ent.item_id];
        if (!it) continue;
        const emb = parseFloat(it.emb) || 1;
        const qtdEsperada = (parseFloat(it.quantidade) || 0) * emb;
        let qtdContada = 0;
        const lotes = Array.isArray(ent.lotes) ? ent.lotes : [];
        const validadesItem = []; // pra calcular risco depois
        for (let idx = 0; idx < lotes.length; idx++) {
          const l = lotes[idx];
          const cx = parseFloat(l.qtd_caixas ?? l.cx) || 0;
          const un = parseFloat(l.qtd_unidades ?? l.un) || 0;
          const total = cx * emb + un;
          qtdContada += total;
          const validade = l.validade || null;
          if (cx > 0 || un > 0 || validade) {
            await client.query(
              `INSERT INTO lotes_conferidos
                  (item_id, nota_id, lote_idx, qtd_caixas, qtd_unidades, qtd_total, validade, conferido_por)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
              [ent.item_id, req.params.id, idx, cx, un, total, validade, quem]
            );
            if (validade && total > 0) validadesItem.push({ validade, qtd: total });
          }
        }

        // ── Análise de validade em risco (por validade do item) ──
        if (validadesItem.length) {
          const eanLojaRow = await client.query(
            `SELECT ean_principal_jrlira FROM produtos_embalagem WHERE mat_codi = $1`,
            [it.cd_pro_codi]
          );
          const eanRef = eanLojaRow.rows[0]?.ean_principal_jrlira || it.ean_nota;
          let estoqueAtual = 0, vendasMedia = 0;
          if (eanRef) {
            const ext = await client.query(
              `SELECT COALESCE(estdisponivel,0)::numeric AS est
                 FROM produtos_externo WHERE loja_id=$1 AND codigobarra=$2 LIMIT 1`,
              [notaInfo.loja_id, eanRef]
            );
            estoqueAtual = parseFloat(ext.rows[0]?.est) || 0;
            const vd = await client.query(
              `SELECT COALESCE(SUM(qtd_vendida),0)::numeric AS total
                 FROM vendas_historico
                WHERE loja_id=$1 AND codigobarra=$2 AND data_venda >= CURRENT_DATE - INTERVAL '60 days'
                  AND COALESCE(tipo_saida, 'venda') = 'venda'`,
              [notaInfo.loja_id, eanRef]
            );
            vendasMedia = (parseFloat(vd.rows[0]?.total) || 0) / 60;
          }
          // Considera worst-case: pra cada validade, calcula isoladamente
          // (assume FEFO — vende primeiro a validade mais curta)
          const validadesOrd = [...validadesItem].sort((a,b) => new Date(a.validade) - new Date(b.validade));
          let estoqueAcum = estoqueAtual;
          for (const v of validadesOrd) {
            estoqueAcum += v.qtd;
            const dias = Math.floor((new Date(v.validade) - Date.now()) / 86400000);
            const consumivel = vendasMedia * Math.max(0, dias);
            const emRisco = Math.max(0, estoqueAcum - consumivel);
            const motivo = vendasMedia <= 0
              ? 'sem_historico_vendas'
              : (dias < 0 ? 'ja_vencido' : 'risco_por_giro');
            if (emRisco > 0 || vendasMedia <= 0 || dias < 0) {
              const precoUnit = (parseFloat(it.preco_unitario_nota) || 0) / (emb || 1);
              const qtdRiscoUn = vendasMedia <= 0 ? v.qtd : emRisco;
              // Arredonda risco PARA CIMA em caixas inteiras (devolução por caixa fechada)
              const qtdRiscoCx = emb > 0 ? Math.ceil(qtdRiscoUn / emb) : Math.ceil(qtdRiscoUn);
              const valorRiscoCx = qtdRiscoCx * emb * precoUnit;
              await client.query(
                `INSERT INTO validades_em_risco
                    (nota_id, item_id, loja_id, cd_pro_codi, descricao, ean,
                     validade, dias_ate_vencer, qtd_recebida_lote,
                     estoque_atual, estoque_pos_recebimento, vendas_media_dia,
                     qtd_consumivel_ate_vencer, qtd_em_risco,
                     valor_unitario, valor_em_risco, motivo_risco,
                     qtd_embalagem, qtd_em_risco_caixas)
                  VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)`,
                [req.params.id, ent.item_id, notaInfo.loja_id, it.cd_pro_codi,
                 it.descricao_nota, eanRef || it.ean_nota,
                 v.validade, dias, v.qtd,
                 estoqueAtual, estoqueAcum, vendasMedia,
                 consumivel, qtdRiscoUn,
                 precoUnit, valorRiscoCx, motivo,
                 emb, qtdRiscoCx]
              );
              temValidadeEmRisco = true;
            }
          }
        }
        // ── fim análise validade ──
        if (Math.abs(qtdContada - qtdEsperada) > 0.0001) {
          comDivergencia = true;
          detalhes.push({ item_id: ent.item_id, esperado: qtdEsperada, contado: qtdContada });
          // Preço unitário em UNIDADE da loja (preço da nota está em embalagem)
          const precoUnit = (parseFloat(it.preco_unitario_nota) || 0) / (emb || 1);
          const valorDif = Math.abs((qtdContada - qtdEsperada) * precoUnit);
          await client.query(
            `INSERT INTO auditagem_divergencias
                (nota_id, item_id, loja_id, cd_pro_codi, descricao, ean_nota,
                 qtd_esperada, qtd_contada, valor_unitario, valor_total_diferenca)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
            [req.params.id, ent.item_id, notaInfo.loja_id, it.cd_pro_codi,
             it.descricao_nota, it.ean_nota,
             qtdEsperada, qtdContada, precoUnit, valorDif]
          );
        }
      }

      // Validade em risco tem precedência: nota fica aguardando admin decidir
      const novoStatus = temValidadeEmRisco
        ? 'aguardando_admin_validade'
        : (comDivergencia ? 'auditagem' : 'conferida');
      await client.query(
        `UPDATE notas_entrada
            SET status=$3, conferida_em=NOW(), conferida_por=$2, conferida_com_divergencia=$4
          WHERE id=$1`,
        [req.params.id, quem, novoStatus, comDivergencia]
      );
      await client.query('COMMIT');
      res.json({ ok: true, novo_status: novoStatus, com_divergencia: comDivergencia, validade_em_risco: temValidadeEmRisco, divergencias: detalhes });
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      throw e;
    } finally { client.release(); }
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// GET /api/notas/:id/historico
// Consolida toda a história da nota: pedido B2B (se houver), sync CD, status timeline,
// itens com lotes conferidos, divergências (com resolução), validades em risco (com decisão)
router.get('/:id/historico', autenticar, async (req, res) => {
  try {
    const [nota] = await query(`SELECT * FROM notas_entrada WHERE id=$1`, [req.params.id]);
    if (!nota) return res.status(404).json({ erro: 'Nota não encontrada' });

    let pedido = null, itensPedido = null;
    if (nota.pedido_id) {
      const [p] = await query(
        `SELECT p.*, v.nome AS vendedor_nome, f.razao_social AS fornecedor_nome, f.cnpj AS fornecedor_cnpj
           FROM pedidos p
           LEFT JOIN vendedores v ON v.id = p.vendedor_id
           LEFT JOIN fornecedores f ON f.id = v.fornecedor_id
          WHERE p.id = $1`, [nota.pedido_id]);
      pedido = p || null;
      itensPedido = await query(`SELECT * FROM itens_pedido WHERE pedido_id=$1 ORDER BY id`, [nota.pedido_id]);
    }

    const isCD = nota.origem === 'cd' || nota.origem === 'transferencia_loja';

    const itens = await query(
      isCD
        ? `SELECT i.*,
                  pe.qtd_embalagem AS qtd_embalagem_loja,
                  pe.ean_principal_jrlira AS cod_barra_loja,
                  (SELECT json_agg(json_build_object(
                    'lote_idx', lc.lote_idx, 'qtd_caixas', lc.qtd_caixas,
                    'qtd_unidades', lc.qtd_unidades, 'qtd_total', lc.qtd_total,
                    'validade', lc.validade, 'conferido_por', lc.conferido_por,
                    'conferido_em', lc.conferido_em
                  ) ORDER BY lc.lote_idx) FROM lotes_conferidos lc WHERE lc.item_id = i.id) AS lotes,
                  (SELECT json_agg(json_build_object(
                    'qtd_esperada', d.qtd_esperada, 'qtd_contada', d.qtd_contada, 'diferenca', d.diferenca,
                    'valor_total_diferenca', d.valor_total_diferenca,
                    'status', d.status, 'observacao', d.observacao,
                    'numero_nf_devolucao', d.numero_nf_devolucao,
                    'resolvido_em', d.resolvido_em, 'resolvido_por', d.resolvido_por
                  )) FROM auditagem_divergencias d WHERE d.item_id = i.id) AS divergencias,
                  (SELECT json_agg(json_build_object(
                    'validade', v.validade, 'dias_ate_vencer', v.dias_ate_vencer,
                    'qtd_em_risco', v.qtd_em_risco, 'qtd_em_risco_caixas', v.qtd_em_risco_caixas,
                    'qtd_embalagem', v.qtd_embalagem, 'valor_em_risco', v.valor_em_risco,
                    'motivo_risco', v.motivo_risco, 'status', v.status,
                    'observacao', v.observacao,
                    'decidido_em', v.decidido_em, 'decidido_por', v.decidido_por
                  )) FROM validades_em_risco v WHERE v.item_id = i.id) AS validades_risco
             FROM itens_nota i
             LEFT JOIN produtos_embalagem pe ON pe.mat_codi = i.cd_pro_codi
            WHERE i.nota_id = $1
            ORDER BY i.numero_item`
        : `SELECT i.*,
                  (SELECT json_agg(json_build_object(
                    'rodada', c.rodada,
                    'qtd_contada', c.qtd_contada,
                    'status', c.status,
                    'conferido_por', c.conferido_por,
                    'conferido_em', c.conferido_em,
                    'lotes', (SELECT json_agg(json_build_object(
                       'lote', cl.lote, 'validade', cl.validade,
                       'quantidade', cl.quantidade, 'local_destino', cl.local_destino
                     )) FROM conferencia_lotes cl WHERE cl.conferencia_id = c.id)
                  ) ORDER BY c.rodada)
                     FROM conferencias_estoque c WHERE c.item_id = i.id) AS conferencias,
                  (SELECT row_to_json(a.*) FROM auditoria_itens a WHERE a.item_id = i.id) AS auditoria,
                  (SELECT json_agg(json_build_object(
                    'validade', v.validade, 'dias_ate_vencer', v.dias_ate_vencer,
                    'qtd_em_risco', v.qtd_em_risco, 'qtd_em_risco_caixas', v.qtd_em_risco_caixas,
                    'qtd_embalagem', v.qtd_embalagem, 'valor_em_risco', v.valor_em_risco,
                    'motivo_risco', v.motivo_risco, 'status', v.status,
                    'observacao', v.observacao,
                    'decidido_em', v.decidido_em, 'decidido_por', v.decidido_por
                  )) FROM validades_em_risco v WHERE v.item_id = i.id) AS validades_risco,
                  (SELECT json_agg(json_build_object(
                    'qtd_anterior', l.qtd_anterior, 'qtd_novo', l.qtd_novo,
                    'origem', l.origem, 'alterado_por', l.alterado_por, 'alterado_em', l.alterado_em
                  ) ORDER BY l.alterado_em) FROM embalagens_fornecedor_log l
                    WHERE l.item_nota_id = i.id) AS log_embalagem
             FROM itens_nota i WHERE i.nota_id = $1 ORDER BY i.numero_item`,
      [req.params.id]
    );

    // Último preço pago do mesmo fornecedor pra cada EAN ANTES desta nota — pra calcular diff de ganho/perda
    if (!isCD) {
      const eans = [...new Set(itens.map(i => (i.ean_validado || i.ean_nota || '').replace(/^0+/, '')).filter(Boolean))];
      if (eans.length) {
        const fornCnpj = (nota.fornecedor_cnpj || '').replace(/\D/g, '');
        const ultPrecos = await query(
          `SELECT codigobarra, preco
             FROM (
               SELECT NULLIF(LTRIM(ch.codigobarra,'0'),'') AS codigobarra,
                      (ch.custo_total / NULLIF(ch.qtd_comprada,0))::float AS preco,
                      ch.data_entrada,
                      ROW_NUMBER() OVER (
                        PARTITION BY NULLIF(LTRIM(ch.codigobarra,'0'),'')
                        ORDER BY ch.data_entrada DESC, ch.id DESC
                      ) AS rn
                 FROM compras_historico ch
                WHERE NULLIF(LTRIM(ch.codigobarra,'0'),'') = ANY($1::text[])
                  AND REGEXP_REPLACE(COALESCE(ch.fornecedor_cnpj,''),'\\D','','g') = $2
                  AND ch.data_entrada < COALESCE($3::date, CURRENT_DATE + 1)
                  AND ch.qtd_comprada > 0
                  AND COALESCE(ch.tipo_entrada, 'compra') = 'compra'
             ) sub
            WHERE rn = 1`,
          [eans, fornCnpj, nota.data_emissao || null]
        );
        const mapPrecos = Object.fromEntries(ultPrecos.map(r => [r.codigobarra, r.preco]));
        for (const it of itens) {
          const ean = (it.ean_validado || it.ean_nota || '').replace(/^0+/, '');
          const precoAnt = ean ? mapPrecos[ean] : null;
          if (precoAnt && it.preco_unitario_nota) {
            it.preco_anterior_fornecedor = precoAnt;
            const qtdUn = parseFloat(it.qtd_em_unidades) || parseFloat(it.quantidade) || 0;
            const diffUn = parseFloat(it.preco_unitario_nota) - precoAnt;
            it.diff_unit = diffUn;
            it.diff_total = diffUn * qtdUn;
            it.diff_pct = precoAnt > 0 ? (diffUn / precoAnt) * 100 : null;
          }
        }
      }
    }

    // Eventos do log de embalagem específicos da nota (todos itens)
    const logsEmbalagem = !isCD ? await query(
      `SELECT l.*, i.descricao_nota
         FROM embalagens_fornecedor_log l
         LEFT JOIN itens_nota i ON i.id = l.item_nota_id
        WHERE l.nota_id = $1
        ORDER BY l.alterado_em`,
      [req.params.id]
    ) : [];

    // Alertas associados (notas_alertas)
    const alertas = await query(
      `SELECT tipo, mensagem, criado_em, lido_em, lido_por
         FROM notas_alertas WHERE nota_id = $1 ORDER BY criado_em`,
      [req.params.id]
    ).catch(() => []);

    // Devoluções vinculadas
    const devolucoes = await query(
      `SELECT d.*, (SELECT json_agg(row_to_json(i.*)) FROM devolucoes_itens i WHERE i.devolucao_id = d.id) AS itens
         FROM devolucoes d WHERE d.nota_id = $1 ORDER BY d.id`,
      [req.params.id]
    );

    res.json({ nota, pedido, itensPedido, itens, devolucoes, logsEmbalagem, alertas });
  } catch (err) {
    console.error('[historico]', err.message);
    res.status(500).json({ erro: err.message });
  }
});

// PATCH /api/notas/:id/itens/:itemId/cadastrar-ean
// Cadastro insere EAN faltante de item de transferência CD durante a validação.
// Atualiza ean_nota do item + ean_principal_jrlira em produtos_embalagem.
router.patch('/:id/itens/:itemId/cadastrar-ean', autenticar, async (req, res) => {
  try {
    const { ean } = req.body || {};
    const eanLimpo = (ean || '').replace(/\D/g, '');
    if (!eanLimpo) return res.status(400).json({ erro: 'EAN inválido' });

    const [item] = await query(
      `SELECT i.*, n.origem, n.loja_id
         FROM itens_nota i JOIN notas_entrada n ON n.id = i.nota_id
        WHERE i.id = $1 AND i.nota_id = $2`,
      [req.params.itemId, req.params.id]
    );
    if (!item) return res.status(404).json({ erro: 'Item não encontrado' });
    if (item.origem !== 'cd' && item.origem !== 'transferencia_loja') {
      return res.status(400).json({ erro: 'Endpoint apenas pra transferências' });
    }

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `UPDATE itens_nota
            SET ean_nota = $2, sem_codigo_barras = FALSE
          WHERE id = $1`,
        [req.params.itemId, eanLimpo]
      );
      if (item.cd_pro_codi) {
        await client.query(
          `UPDATE produtos_embalagem
              SET ean_principal_jrlira = $2,
                  ean_validado_em = NOW(),
                  ean_validado_por = $3,
                  atualizado_em = NOW()
            WHERE mat_codi = $1
              AND (ean_principal_jrlira IS NULL OR ean_principal_jrlira = '')`,
          [item.cd_pro_codi, eanLimpo, req.usuario.nome || req.usuario.usuario]
        );
      }
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      throw e;
    } finally { client.release(); }
    res.json({ ok: true, ean: eanLimpo });
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// PATCH /api/notas/:id/validar-transf — cadastro valida no app
router.patch('/:id/validar-transf', autenticar, async (req, res) => {
  try {
    const [nota] = await query('SELECT status, origem FROM notas_entrada WHERE id=$1', [req.params.id]);
    if (!nota) return res.status(404).json({ erro: 'Nota não encontrada' });
    if (nota.origem !== 'cd' && nota.origem !== 'transferencia_loja') {
      return res.status(400).json({ erro: 'Fluxo apenas para transferências' });
    }
    if (!['conferida', 'auditagem'].includes(nota.status)) {
      return res.status(400).json({ erro: `Status atual ${nota.status} não permite validar` });
    }
    await query(
      `UPDATE notas_entrada
          SET status='validada', validada_em=NOW(), validada_por=$2
        WHERE id=$1`,
      [req.params.id, _quemSou(req)]
    );
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// PATCH /api/notas/:id/liberar  — cadastro libera para estoque
router.patch('/:id/liberar', autenticar, async (req, res) => {
  try {
    const [nota] = await query('SELECT status, loja_id FROM notas_entrada WHERE id=$1', [req.params.id]);
    if (!nota) return res.status(404).json({ erro: 'Nota não encontrada' });
    if (nota.status !== 'em_validacao_cadastro') return res.status(400).json({ erro: 'Nota não está em Validação Cadastro' });
    if (!nota.loja_id) return res.status(400).json({ erro: 'Nota sem loja definida' });
    await query('UPDATE notas_entrada SET status=$1 WHERE id=$2', ['em_conferencia', req.params.id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ erro: err.message }); }
});

// POST /api/notas/:id/conferencia
router.post('/:id/conferencia', autenticar, async (req, res) => {
  const client = await pool.connect();
  try {
    const { itens_conferidos } = req.body;
    if (!Array.isArray(itens_conferidos) || !itens_conferidos.length)
      return res.status(400).json({ erro: 'Itens obrigatórios' });

    const [nota] = await query('SELECT * FROM notas_entrada WHERE id = $1', [req.params.id]);
    if (!nota) return res.status(404).json({ erro: 'Nota não encontrada' });
    if (nota.status !== 'em_conferencia') return res.status(400).json({ erro: 'Nota não está disponível para conferência' });

    const rodada = (nota.conferencia_rodada || 0) + 1;
    if (rodada > 2) return res.status(400).json({ erro: 'Conferências encerradas. Aguarda auditoria.' });

    await client.query('BEGIN');
    let divergentes = 0;
    const resultados = [];

    for (const conf of itens_conferidos) {
      const { rows: [item] } = await client.query(
        'SELECT * FROM itens_nota WHERE id = $1 AND nota_id = $2',
        [conf.item_id, req.params.id]
      );
      if (!item) continue;
      const diff = Math.abs(n(conf.qtd_contada) - n(item.quantidade));
      const status = diff < 0.001 ? 'ok' : 'divergente';
      if (status === 'divergente') divergentes++;

      const { rows: [novaConf] } = await client.query(`
        INSERT INTO conferencias_estoque (item_id, rodada, qtd_contada, status, conferido_por)
        VALUES ($1,$2,$3,$4,$5) RETURNING id
      `, [item.id, rodada, n(conf.qtd_contada), status, req.usuario.nome]);

      for (const lote of (conf.lotes || [])) {
        if (lote.lote || lote.validade) {
          await client.query(`
            INSERT INTO conferencia_lotes (conferencia_id, lote, validade, quantidade, local_destino)
            VALUES ($1,$2,$3,$4,$5)
          `, [novaConf.id, lote.lote || null, lote.validade || null,
              n(lote.quantidade) || n(conf.qtd_contada), lote.local_destino || 'Estoque']);
        }
      }
      resultados.push({ item_id: item.id, status });
    }

    let novoStatus = nota.status;
    if (divergentes === 0) novoStatus = 'fechada';
    else if (rodada === 2) novoStatus = 'em_auditoria';

    await client.query(
      'UPDATE notas_entrada SET conferencia_rodada=$1, status=$2 WHERE id=$3',
      [rodada, novoStatus, req.params.id]
    );
    await client.query('COMMIT');
    if (novoStatus === 'fechada') enviarWebhookEntrada(req.params.id);
    res.json({ ok: true, rodada, divergentes, novo_status: novoStatus, resultados });
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    res.status(500).json({ erro: err.message });
  } finally {
    client.release();
  }
});

// POST /api/notas/:id/auditoria
router.post('/:id/auditoria', autenticar, async (req, res) => {
  const client = await pool.connect();
  try {
    const { itens_auditados } = req.body;
    if (!Array.isArray(itens_auditados) || !itens_auditados.length)
      return res.status(400).json({ erro: 'Itens obrigatórios' });
    const [nota] = await query('SELECT * FROM notas_entrada WHERE id = $1', [req.params.id]);
    if (!nota) return res.status(404).json({ erro: 'Nota não encontrada' });
    if (nota.status !== 'em_auditoria') return res.status(400).json({ erro: 'Nota não está em auditoria' });

    await client.query('BEGIN');
    // Limpa validades + divergências pendentes anteriores desta nota
    await client.query(`DELETE FROM validades_em_risco WHERE nota_id=$1 AND status='pendente'`, [req.params.id]);
    await client.query(`DELETE FROM auditagem_divergencias WHERE nota_id=$1 AND status='pendente'`, [req.params.id]);

    let temValidadeEmRisco = false;
    let temFalta = false;
    let temSobra = false;
    const sobras = []; // pra alerta
    let devolucaoId = null;
    for (const aud of itens_auditados) {
      const { rows: [item] } = await client.query(
        'SELECT i.*, COALESCE(pe.qtd_embalagem,1)::numeric AS emb FROM itens_nota i LEFT JOIN produtos_embalagem pe ON pe.mat_codi = i.cd_pro_codi WHERE i.id = $1 AND i.nota_id = $2',
        [aud.item_id, req.params.id]
      );
      if (!item) continue;
      const qtdEsperada = n(item.quantidade);
      const qtdContada = n(aud.qtd_contada);
      const diferenca = qtdContada - qtdEsperada;
      const diff = Math.abs(diferenca);
      const statusAud = diff < 0.001 ? 'ok' : 'divergente';
      await client.query(`
        INSERT INTO auditoria_itens (item_id, qtd_contada, lote, validade, status, observacao, auditado_por)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT (item_id) DO UPDATE
          SET qtd_contada=$2, lote=$3, validade=$4, status=$5, observacao=$6, auditado_por=$7, auditado_em=NOW()
      `, [aud.item_id, qtdContada, aud.lote || null, aud.validade || null,
          statusAud, aud.observacao || null, req.usuario.nome]);

      // ── Divergência (falta/sobra) ──
      if (diff >= 0.001) {
        // qtd_esperada/qtd_contada estão na MESMA unidade da NF (geralmente caixas);
        // preco_unitario_nota está SEMPRE por UNIDADE INDIVIDUAL (parser converte).
        // qtd_por_caixa_nfe = unidades por caixa (do parser ou cadastro).
        // Precisamos do preço por "unidade da NF" → preco_caixa quando >1, senão preco_unit.
        const embPorCx = parseInt(item.qtd_por_caixa_nfe) || parseFloat(item.emb) || 1;
        const precoUnitInd = parseFloat(item.preco_unitario_nota) || 0;
        const precoPorQtdNota = parseFloat(item.preco_unitario_caixa) || (precoUnitInd * embPorCx);
        const valorDif = diff * precoPorQtdNota;
        await client.query(
          `INSERT INTO auditagem_divergencias
              (nota_id, item_id, loja_id, cd_pro_codi, descricao, ean_nota,
               qtd_esperada, qtd_contada, valor_unitario, valor_total_diferenca, observacao)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
          [req.params.id, aud.item_id, nota.loja_id, item.cd_pro_codi,
           item.descricao_nota, item.ean_nota,
           qtdEsperada, qtdContada, precoPorQtdNota, valorDif, aud.observacao || null]
        );

        if (diferenca < 0) {
          // FALTA — gera item de devolução pro fornecedor
          temFalta = true;
          if (!devolucaoId) {
            const destCnpj = (nota.fornecedor_cnpj || '').replace(/\D/g, '');
            const destNome = nota.fornecedor_nome || 'Fornecedor';
            const { rows: existeDev } = await client.query(
              `SELECT id FROM devolucoes WHERE nota_id=$1 AND destinatario_cnpj=$2 AND status='aguardando' LIMIT 1`,
              [req.params.id, destCnpj]
            );
            if (existeDev.length) {
              devolucaoId = existeDev[0].id;
            } else {
              const { rows: novoDev } = await client.query(
                `INSERT INTO devolucoes
                    (nota_id, loja_id, tipo, destinatario_cnpj, destinatario_nome, motivo, criado_por)
                  VALUES ($1,$2,'fornecedor',$3,$4,'divergencia_quantidade',$5) RETURNING id`,
                [req.params.id, nota.loja_id, destCnpj, destNome, req.usuario.nome]
              );
              devolucaoId = novoDev[0].id;
            }
          }
          const qtdFaltaCx = -diferenca;                     // em caixas (qtd da NF)
          const qtdFaltaUn = qtdFaltaCx * embPorCx;          // em unidades individuais
          const valorFalta = qtdFaltaCx * precoPorQtdNota;   // = qtdFaltaUn × precoUnitInd
          await client.query(
            `INSERT INTO devolucoes_itens
                (devolucao_id, item_nota_id, cd_pro_codi, ean, descricao,
                 qtd_caixas, qtd_unidades, qtd_total, valor_unitario, valor_total,
                 origem_tipo, origem_id)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'auditagem_divergencia',NULL)`,
            [devolucaoId, aud.item_id, item.cd_pro_codi, item.ean_nota, item.descricao_nota,
             qtdFaltaCx, qtdFaltaUn, qtdFaltaUn, precoUnitInd, valorFalta]
          );
        } else {
          // SOBRA — relatório informativo
          temSobra = true;
          sobras.push({
            item_id: aud.item_id,
            descricao: item.descricao_nota,
            ean: item.ean_nota,
            qtd_esperada: qtdEsperada,
            qtd_contada: qtdContada,
            sobra: diferenca,
            valor: valorDif,
          });
        }
      }

      // ── Análise de validade em risco (se auditor preencheu) ──
      if (aud.validade) {
        const emb = parseFloat(item.emb) || 1;
        const eanRef = item.ean_nota;
        let estoqueAtual = 0, vendasMedia = 0;
        if (eanRef) {
          const ext = await client.query(
            `SELECT COALESCE(estdisponivel,0)::numeric AS est FROM produtos_externo
              WHERE loja_id=$1 AND codigobarra=$2 LIMIT 1`,
            [nota.loja_id, eanRef]
          );
          estoqueAtual = parseFloat(ext.rows[0]?.est) || 0;
          const vd = await client.query(
            `SELECT COALESCE(SUM(qtd_vendida),0)::numeric AS total FROM vendas_historico
              WHERE loja_id=$1 AND codigobarra=$2 AND data_venda >= CURRENT_DATE - INTERVAL '60 days'
                AND COALESCE(tipo_saida, 'venda') = 'venda'`,
            [nota.loja_id, eanRef]
          );
          vendasMedia = (parseFloat(vd.rows[0]?.total) || 0) / 60;
        }
        const qtdRecebida = n(aud.qtd_contada);
        const estoquePos = estoqueAtual + qtdRecebida;
        const dias = Math.floor((new Date(aud.validade) - Date.now()) / 86400000);
        const consumivel = vendasMedia * Math.max(0, dias);
        const emRisco = Math.max(0, estoquePos - consumivel);
        if (emRisco > 0 || vendasMedia <= 0 || dias < 0) {
          const motivo = vendasMedia <= 0 ? 'sem_historico_vendas'
                       : dias < 0 ? 'ja_vencido' : 'risco_por_giro';
          const precoUnit = (parseFloat(item.preco_unitario_nota) || 0) / (emb || 1);
          const qtdRiscoUn = vendasMedia <= 0 ? qtdRecebida : emRisco;
          const qtdRiscoCx = emb > 0 ? Math.ceil(qtdRiscoUn / emb) : Math.ceil(qtdRiscoUn);
          const valorRisco = qtdRiscoCx * emb * precoUnit;
          await client.query(
            `INSERT INTO validades_em_risco
                (nota_id, item_id, loja_id, cd_pro_codi, descricao, ean,
                 validade, dias_ate_vencer, qtd_recebida_lote,
                 estoque_atual, estoque_pos_recebimento, vendas_media_dia,
                 qtd_consumivel_ate_vencer, qtd_em_risco,
                 valor_unitario, valor_em_risco, motivo_risco,
                 qtd_embalagem, qtd_em_risco_caixas)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)`,
            [req.params.id, aud.item_id, nota.loja_id, item.cd_pro_codi,
             item.descricao_nota, eanRef,
             aud.validade, dias, qtdRecebida,
             estoqueAtual, estoquePos, vendasMedia,
             consumivel, qtdRiscoUn,
             precoUnit, valorRisco, motivo,
             emb, qtdRiscoCx]
          );
          temValidadeEmRisco = true;
        }
      }
    }

    // Atualiza valor_total da devolução agregada (se houve falta)
    if (devolucaoId) {
      await client.query(
        `UPDATE devolucoes SET valor_total = (SELECT COALESCE(SUM(valor_total),0) FROM devolucoes_itens WHERE devolucao_id=$1) WHERE id=$1`,
        [devolucaoId]
      );
    }

    // Precedência: validade em risco > falta (devolução) > sobra/ok (fecha)
    const novoStatus = temValidadeEmRisco
      ? 'aguardando_admin_validade'
      : temFalta
        ? 'aguardando_devolucao'
        : 'fechada';
    await client.query(
      `UPDATE notas_entrada SET status=$1::text, fechado_em = CASE WHEN $1::text='fechada' THEN NOW() ELSE fechado_em END WHERE id=$2`,
      [novoStatus, req.params.id]
    );

    // Alerta admin/comprador quando há SOBRA (não vai pro estoque — informativo)
    if (temSobra) {
      const totalValor = sobras.reduce((s, x) => s + (x.valor || 0), 0);
      const titulo = `Sobras na NF ${nota.numero_nota || '#'+nota.id} — ${nota.fornecedor_nome || ''}`;
      const linhas = sobras.map(s => `${s.descricao} (+${s.sobra.toFixed(2)} un · R$ ${s.valor.toFixed(2)})`).join(' | ');
      const mensagem = `Auditoria detectou ${sobras.length} item(ns) com sobra (total R$ ${totalValor.toFixed(2)}). Não entra no estoque. Avisar comprador/vendedor — só recebe se fornecedor emitir nova NF complementar. Itens: ${linhas}`;
      await client.query(
        `INSERT INTO alertas_admin (tipo, entidade, entidade_id, titulo, mensagem)
           VALUES ('sobra_auditoria', 'nota', $1, $2, $3)
           ON CONFLICT (tipo, entidade, entidade_id) WHERE resolvido_em IS NULL DO UPDATE
             SET titulo = EXCLUDED.titulo, mensagem = EXCLUDED.mensagem, criado_em = NOW()`,
        [req.params.id, titulo, mensagem]
      );
    }

    await client.query('COMMIT');
    if (novoStatus === 'fechada') enviarWebhookEntrada(req.params.id);
    res.json({
      ok: true,
      novo_status: novoStatus,
      validade_em_risco: temValidadeEmRisco,
      tem_falta: temFalta,
      tem_sobra: temSobra,
      sobras_count: sobras.length,
      devolucao_id: devolucaoId,
    });
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    res.status(500).json({ erro: err.message });
  } finally {
    client.release();
  }
});

// GET /api/notas/:id/relatorio
router.get('/:id/relatorio', autenticar, async (req, res) => {
  try {
    const [nota] = await query('SELECT * FROM notas_entrada WHERE id = $1', [req.params.id]);
    if (!nota) return res.status(404).json({ erro: 'Nota não encontrada' });
    const itens = await query(`
      SELECT i.*,
        (SELECT json_agg(json_build_object(
          'rodada', c.rodada, 'qtd_contada', c.qtd_contada, 'status', c.status,
          'conferido_por', c.conferido_por, 'conferido_em', c.conferido_em,
          'lotes', (SELECT json_agg(json_build_object('lote', l.lote, 'validade', l.validade, 'quantidade', l.quantidade))
                    FROM conferencia_lotes l WHERE l.conferencia_id = c.id)
        ) ORDER BY c.rodada) FROM conferencias_estoque c WHERE c.item_id = i.id) AS conferencias,
        (SELECT row_to_json(a.*) FROM auditoria_itens a WHERE a.item_id = i.id) AS auditoria
      FROM itens_nota i WHERE i.nota_id = $1 ORDER BY i.numero_item
    `, [req.params.id]);
    res.json({ nota, itens });
  } catch (err) {
    res.status(500).json({ erro: err.message });
  }
});

module.exports = router;
