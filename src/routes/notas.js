const express = require('express');
const router = express.Router();
const multer = require('multer');
const xml2js = require('xml2js');
const { query, pool } = require('../db');
const { autenticar } = require('../auth');

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });

function n(v) { return parseFloat(v) || 0; }

async function enviarWebhookEntrada(nota_id) {
  const url = process.env.ACOUGUE_WEBHOOK_URL;
  const key = process.env.ACOUGUE_WEBHOOK_KEY;
  if (!url || !key) return;
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

    await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-webhook-key': key },
      body: JSON.stringify(payload)
    });
  } catch (err) {
    console.error('[webhook] Erro ao enviar entrada de validade:', err.message);
  }
}

function extractICMS(det) {
  const grp = det.imposto?.ICMS || {};
  const e = Object.values(grp)[0] || {};
  return {
    vicms_bc: n(e.vBC),
    vicms:    n(e.vICMS),
    vst_bc:   n(e.vBCST),
    vst:      n(e.vST ?? e.vICMSST),
    vfcp_st:  n(e.vFCPST),
  };
}

function extractIPI(det) {
  const t = det.imposto?.IPI?.IPITrib || {};
  return { vipi_bc: n(t.vBC), vipi: n(t.vIPI) };
}

async function parseNFe(buf) {
  const parser = new xml2js.Parser({
    explicitArray: false,
    ignoreAttrs: false,
    tagNameProcessors: [xml2js.processors.stripPrefix],
  });
  const raw = await parser.parseStringPromise(buf.toString('utf8'));

  const nfeProc = raw.nfeProc || raw;
  const nfe = nfeProc.NFe;
  if (!nfe) throw new Error('Elemento NFe não encontrado no XML');
  const inf = nfe.infNFe;
  const ide = inf.ide;
  const emit = inf.emit;

  let chave = null;
  if (nfeProc.protNFe?.infProt?.chNFe) chave = nfeProc.protNFe.infProt.chNFe;
  else if (inf.$?.Id) chave = inf.$.Id.replace('NFe', '');

  const tot = inf.total?.ICMSTot || {};
  const header = {
    chave_nfe:    chave,
    numero_nota:  String(ide.nNF || ''),
    serie:        String(ide.serie || ''),
    fornecedor_nome: String(emit.xNome || emit.xFant || ''),
    fornecedor_cnpj: String(emit.CNPJ || emit.CPF || ''),
    data_emissao: String(ide.dhEmi || ide.dEmi || '').substring(0, 10) || null,
    valor_total:  n(tot.vNF),
    tot_vprod:    n(tot.vProd),
    tot_vbc:      n(tot.vBC),
    tot_vicms:    n(tot.vICMS),
    tot_vbcst:    n(tot.vBCST),
    tot_vst:      n(tot.vST),
    tot_vfcp_st:  n(tot.vFCPST),
    tot_vipi:     n(tot.vIPI),
    tot_vdesc:    n(tot.vDesc),
    tot_vfrete:   n(tot.vFrete),
    tot_vseg:     n(tot.vSeg),
    tot_voutro:   n(tot.vOutro),
  };

  const detRaw = inf.det;
  const dets = !detRaw ? [] : (Array.isArray(detRaw) ? detRaw : [detRaw]);

  const itens = dets.map((det, idx) => {
    const prod = det.prod || {};
    const qCom        = n(prod.qCom) || n(prod.qTrib) || 1;
    const vprod       = n(prod.vProd);
    const vdesc_item  = n(prod.vDesc);
    const vfrete_item = n(prod.vFrete);
    const vseg_item   = n(prod.vSeg);
    const voutro_item = n(prod.vOutro);

    const { vicms_bc, vicms, vst_bc, vst, vfcp_st } = extractICMS(det);
    const { vipi_bc, vipi } = extractIPI(det);

    const custo_total       = (vprod - vdesc_item) + vfrete_item + vseg_item + voutro_item + vipi + vst + vfcp_st;
    const preco_total_nota  = parseFloat(custo_total.toFixed(2));
    const preco_unitario_nota = parseFloat((custo_total / qCom).toFixed(4));

    const eanRaw    = prod.cEAN;
    const ean       = (!eanRaw || eanRaw === 'SEM GTIN') ? null : String(eanRaw).trim();
    const eanTribRaw = prod.cEANTrib;
    const eanTrib   = (!eanTribRaw || eanTribRaw === 'SEM GTIN') ? null : String(eanTribRaw).trim();

    return {
      numero_item: parseInt(det.$?.nItem || idx + 1) || idx + 1,
      ean_nota: ean,
      ean_trib: eanTrib,
      descricao_nota: String(prod.xProd || '').trim(),
      quantidade: qCom,
      preco_unitario_nota, preco_total_nota,
      vprod, vdesc_item, vfrete_item, vseg_item, voutro_item,
      vicms_bc, vicms, vst_bc, vst, vfcp_st, vipi_bc, vipi,
    };
  });

  return { header, itens };
}

// POST /api/notas/importar
router.post('/importar', autenticar, upload.single('xml'), async (req, res) => {
  const client = await pool.connect();
  try {
    if (!req.file) return res.status(400).json({ erro: 'Arquivo XML obrigatório' });

    const loja_id = parseInt(req.body.loja_id || req.usuario.loja_id) || null;
    if (!loja_id) return res.status(400).json({ erro: 'Selecione a loja de destino antes de importar' });

    const { header, itens } = await parseNFe(req.file.buffer);

    if (header.chave_nfe) {
      const dup = await query('SELECT id FROM notas_entrada WHERE chave_nfe = $1', [header.chave_nfe]);
      if (dup.length) return res.status(409).json({ erro: 'Nota já importada', nota_id: dup[0].id });
    }

    await client.query('BEGIN');
    const { rows: [nova] } = await client.query(`
      INSERT INTO notas_entrada
        (chave_nfe, numero_nota, serie, fornecedor_nome, fornecedor_cnpj, data_emissao,
         valor_total, importado_por, loja_id,
         tot_vprod, tot_vbc, tot_vicms, tot_vbcst, tot_vst, tot_vfcp_st,
         tot_vipi, tot_vdesc, tot_vfrete, tot_vseg, tot_voutro)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
      RETURNING id
    `, [header.chave_nfe, header.numero_nota, header.serie, header.fornecedor_nome,
        header.fornecedor_cnpj, header.data_emissao, header.valor_total, req.usuario.nome, loja_id,
        header.tot_vprod, header.tot_vbc, header.tot_vicms, header.tot_vbcst, header.tot_vst, header.tot_vfcp_st,
        header.tot_vipi, header.tot_vdesc, header.tot_vfrete, header.tot_vseg, header.tot_voutro]);

    const nota_id = nova.id;

    for (const item of itens) {
      let custo_fabrica = null, status_preco = 'sem_cadastro', produto_novo = true;
      let ean_matched = null, ean_fonte = null;
      const eansToCheck = [];
      if (item.ean_trib) eansToCheck.push([item.ean_trib, 'ean_trib']);
      if (item.ean_nota && item.ean_nota !== item.ean_trib) eansToCheck.push([item.ean_nota, 'ean_nota']);
      for (const [ean, fonte] of eansToCheck) {
        const { rows: prods } = await client.query(
          'SELECT custoorigem FROM produtos_externo WHERE codigobarra = $1 AND loja_id = $2 LIMIT 1',
          [ean, loja_id]
        );
        if (prods.length) {
          custo_fabrica = parseFloat(prods[0].custoorigem) || 0;
          produto_novo = false;
          const diff = Math.abs(item.preco_unitario_nota - custo_fabrica);
          status_preco = diff <= 0.01 ? 'igual' : 'divergente';
          ean_matched = ean;
          ean_fonte = fonte;
          break;
        }
      }
      await client.query(`
        INSERT INTO itens_nota
          (nota_id, numero_item, ean_nota, ean_trib, ean_validado, ean_fonte, descricao_nota,
           quantidade, preco_unitario_nota, preco_total_nota, custo_fabrica, status_preco, produto_novo,
           vprod, vdesc_item, vfrete_item, vseg_item, voutro_item,
           vicms_bc, vicms, vst_bc, vst, vfcp_st, vipi_bc, vipi)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)
      `, [nota_id, item.numero_item, item.ean_nota, item.ean_trib, ean_matched || item.ean_nota, ean_fonte,
          item.descricao_nota, item.quantidade, item.preco_unitario_nota, item.preco_total_nota,
          custo_fabrica, status_preco, produto_novo,
          item.vprod, item.vdesc_item, item.vfrete_item, item.vseg_item, item.voutro_item,
          item.vicms_bc, item.vicms, item.vst_bc, item.vst, item.vfcp_st, item.vipi_bc, item.vipi]);
    }

    await client.query('COMMIT');
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

    if (header.chave_nfe) {
      const dup = await query('SELECT id FROM notas_entrada WHERE chave_nfe=$1', [header.chave_nfe]);
      if (dup.length) return res.status(409).json({ erro: 'Nota já importada', nota_id: dup[0].id });
    }

    await client.query('BEGIN');
    const { rows: [nova] } = await client.query(`
      INSERT INTO notas_entrada
        (chave_nfe, numero_nota, serie, fornecedor_nome, fornecedor_cnpj, data_emissao,
         valor_total, importado_por, loja_id,
         tot_vprod, tot_vbc, tot_vicms, tot_vbcst, tot_vst, tot_vfcp_st,
         tot_vipi, tot_vdesc, tot_vfrete, tot_vseg, tot_voutro)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
      RETURNING id`,
      [header.chave_nfe, header.numero_nota, header.serie, header.fornecedor_nome,
       header.fornecedor_cnpj, header.data_emissao, header.valor_total, req.usuario.nome, loja_id,
       header.tot_vprod, header.tot_vbc, header.tot_vicms, header.tot_vbcst, header.tot_vst, header.tot_vfcp_st,
       header.tot_vipi, header.tot_vdesc, header.tot_vfrete, header.tot_vseg, header.tot_voutro]);

    const nota_id = nova.id;

    for (const item of itens) {
      let custo_fabrica = null, status_preco = 'sem_cadastro', produto_novo = true;
      let ean_matched = null, ean_fonte = null;
      const eansToCheck2 = [];
      if (item.ean_trib) eansToCheck2.push([item.ean_trib, 'ean_trib']);
      if (item.ean_nota && item.ean_nota !== item.ean_trib) eansToCheck2.push([item.ean_nota, 'ean_nota']);
      for (const [ean, fonte] of eansToCheck2) {
        const { rows: prods } = await client.query(
          'SELECT custoorigem FROM produtos_externo WHERE codigobarra=$1 AND loja_id=$2 LIMIT 1',
          [ean, loja_id]
        );
        if (prods.length) {
          custo_fabrica = parseFloat(prods[0].custoorigem) || 0;
          produto_novo  = false;
          const diff    = Math.abs(item.preco_unitario_nota - custo_fabrica);
          status_preco  = diff <= 0.01 ? 'igual' : 'divergente';
          ean_matched   = ean;
          ean_fonte     = fonte;
          break;
        }
      }
      await client.query(`
        INSERT INTO itens_nota
          (nota_id, numero_item, ean_nota, ean_trib, ean_validado, ean_fonte, descricao_nota,
           quantidade, preco_unitario_nota, preco_total_nota, custo_fabrica, status_preco, produto_novo,
           vprod, vdesc_item, vfrete_item, vseg_item, voutro_item,
           vicms_bc, vicms, vst_bc, vst, vfcp_st, vipi_bc, vipi)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)`,
        [nota_id, item.numero_item, item.ean_nota, item.ean_trib,
         ean_matched || item.ean_nota || item.ean_trib, ean_fonte,
         item.descricao_nota, item.quantidade, item.preco_unitario_nota, item.preco_total_nota,
         custo_fabrica, status_preco, produto_novo,
         item.vprod, item.vdesc_item, item.vfrete_item, item.vseg_item, item.voutro_item,
         item.vicms_bc, item.vicms, item.vst_bc, item.vst, item.vfcp_st, item.vipi_bc, item.vipi]);
    }
    await client.query('COMMIT');
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
    const itens = await query(`
      SELECT i.*,
        (SELECT json_agg(json_build_object(
          'id', c.id, 'rodada', c.rodada, 'qtd_contada', c.qtd_contada, 'status', c.status,
          'conferido_por', c.conferido_por, 'conferido_em', c.conferido_em,
          'lotes', (SELECT json_agg(json_build_object('lote', l.lote, 'validade', l.validade, 'quantidade', l.quantidade))
                    FROM conferencia_lotes l WHERE l.conferencia_id = c.id)
        ) ORDER BY c.rodada) FROM conferencias_estoque c WHERE c.item_id = i.id) AS conferencias,
        (SELECT row_to_json(a.*) FROM auditoria_itens a WHERE a.item_id = i.id) AS auditoria
      FROM itens_nota i
      WHERE i.nota_id = $1
      ORDER BY i.numero_item
    `, [req.params.id]);
    res.json({ nota, itens });
  } catch (err) {
    res.status(500).json({ erro: err.message });
  }
});

// PATCH /api/notas/:id/itens/:itemId/remap-ean
router.patch('/:id/itens/:itemId/remap-ean', autenticar, async (req, res) => {
  try {
    const { ean_novo } = req.body;
    if (!ean_novo) return res.status(400).json({ erro: 'EAN obrigatório' });
    const [item] = await query('SELECT * FROM itens_nota WHERE id = $1 AND nota_id = $2', [req.params.itemId, req.params.id]);
    if (!item) return res.status(404).json({ erro: 'Item não encontrado' });
    const prods = await query('SELECT custoorigem FROM produtos_externo WHERE codigobarra = $1 LIMIT 1', [ean_novo]);
    if (!prods.length) return res.status(404).json({ erro: 'EAN não encontrado no cadastro' });
    const custo_fabrica = parseFloat(prods[0].custoorigem) || 0;
    const diff = Math.abs(parseFloat(item.preco_unitario_nota) - custo_fabrica);
    const status_preco = diff <= 0.01 ? 'igual' : 'divergente';
    await query(`
      UPDATE itens_nota SET ean_validado=$1, custo_fabrica=$2, status_preco=$3, produto_novo=FALSE, validado_cadastro=TRUE
      WHERE id=$4
    `, [ean_novo, custo_fabrica, status_preco, item.id]);
    res.json({ ok: true, status_preco, custo_fabrica });
  } catch (err) {
    res.status(500).json({ erro: err.message });
  }
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
          const diff    = Math.abs(parseFloat(item.preco_unitario_nota) - custo_fabrica);
          status_preco  = diff <= 0.01 ? 'igual' : 'divergente';
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

// DELETE /api/notas/:id — admin only, apenas status importada
router.delete('/:id', autenticar, async (req, res) => {
  try {
    if (req.usuario.perfil !== 'admin') return res.status(403).json({ erro: 'Exclusão restrita ao administrador' });
    const [nota] = await query('SELECT status FROM notas_entrada WHERE id = $1', [req.params.id]);
    if (!nota) return res.status(404).json({ erro: 'Nota não encontrada' });
    if (nota.status !== 'importada') return res.status(400).json({ erro: 'Apenas notas com status "Importada" podem ser excluídas' });
    await query('DELETE FROM notas_entrada WHERE id = $1', [req.params.id]);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ erro: err.message });
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
    if (novoStatus === 'fechada') enviarWebhookEntrada(req.params.id).catch(() => {});
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
    for (const aud of itens_auditados) {
      const { rows: [item] } = await client.query(
        'SELECT * FROM itens_nota WHERE id = $1 AND nota_id = $2',
        [aud.item_id, req.params.id]
      );
      if (!item) continue;
      const diff = Math.abs(n(aud.qtd_contada) - n(item.quantidade));
      const status = diff < 0.001 ? 'ok' : 'divergente';
      await client.query(`
        INSERT INTO auditoria_itens (item_id, qtd_contada, lote, validade, status, observacao, auditado_por)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT (item_id) DO UPDATE
          SET qtd_contada=$2, lote=$3, validade=$4, status=$5, observacao=$6, auditado_por=$7, auditado_em=NOW()
      `, [aud.item_id, n(aud.qtd_contada), aud.lote || null, aud.validade || null,
          status, aud.observacao || null, req.usuario.nome]);
    }
    await client.query('UPDATE notas_entrada SET status=$1, fechado_em=NOW() WHERE id=$2', ['fechada', req.params.id]);
    await client.query('COMMIT');
    enviarWebhookEntrada(req.params.id).catch(() => {});
    res.json({ ok: true });
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
