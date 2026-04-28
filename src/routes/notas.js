const express = require('express');
const router = express.Router();
const multer = require('multer');
const xml2js = require('xml2js');
const { query, pool } = require('../db');
const { autenticar } = require('../auth');

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });

function n(v) { return parseFloat(v) || 0; }

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

  const header = {
    chave_nfe: chave,
    numero_nota: String(ide.nNF || ''),
    serie: String(ide.serie || ''),
    fornecedor_nome: String(emit.xNome || emit.xFant || ''),
    fornecedor_cnpj: String(emit.CNPJ || emit.CPF || ''),
    data_emissao: String(ide.dhEmi || ide.dEmi || '').substring(0, 10) || null,
    valor_total: n(inf.total?.ICMSTot?.vNF),
  };

  const detRaw = inf.det;
  const dets = !detRaw ? [] : (Array.isArray(detRaw) ? detRaw : [detRaw]);

  const itens = dets.map((det, idx) => {
    const prod = det.prod || {};
    const imp = det.imposto || {};
    const vIPI = n(imp.IPI?.IPITrib?.vIPI) + n(imp.IPI?.IPINT?.vIPI);
    const qCom = n(prod.qCom) || n(prod.qTrib) || 1;
    const vProd = n(prod.vProd);
    const vDesc = n(prod.vDesc);
    const vUnCom = n(prod.vUnCom) || n(prod.vUnTrib);
    const preco_total = parseFloat((vProd - vDesc + vIPI).toFixed(4));
    const eanRaw = prod.cEAN;
    const ean = (!eanRaw || eanRaw === 'SEM GTIN') ? null : String(eanRaw).trim();
    return {
      numero_item: parseInt(det.$?.nItem || idx + 1) || idx + 1,
      ean_nota: ean,
      descricao_nota: String(prod.xProd || '').trim(),
      quantidade: qCom,
      preco_unitario_nota: vUnCom,
      preco_total_nota: preco_total,
    };
  });

  return { header, itens };
}

// POST /api/notas/importar
router.post('/importar', autenticar, upload.single('xml'), async (req, res) => {
  const client = await pool.connect();
  try {
    if (!req.file) return res.status(400).json({ erro: 'Arquivo XML obrigatório' });
    const { header, itens } = await parseNFe(req.file.buffer);

    if (header.chave_nfe) {
      const dup = await query('SELECT id FROM notas_entrada WHERE chave_nfe = $1', [header.chave_nfe]);
      if (dup.length) return res.status(409).json({ erro: 'Nota já importada', nota_id: dup[0].id });
    }

    await client.query('BEGIN');
    const { rows: [nova] } = await client.query(`
      INSERT INTO notas_entrada (chave_nfe, numero_nota, serie, fornecedor_nome, fornecedor_cnpj, data_emissao, valor_total, importado_por)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id
    `, [header.chave_nfe, header.numero_nota, header.serie, header.fornecedor_nome,
        header.fornecedor_cnpj, header.data_emissao, header.valor_total, req.usuario.nome]);

    const nota_id = nova.id;

    for (const item of itens) {
      let produto_id = null, custo_fabrica = null, status_preco = 'sem_cadastro', produto_novo = true;
      if (item.ean_nota) {
        const { rows: prods } = await client.query(
          'SELECT id, custo_fabrica FROM produtos WHERE ean = $1 AND ativo = TRUE LIMIT 1',
          [item.ean_nota]
        );
        if (prods.length) {
          produto_id = prods[0].id;
          custo_fabrica = parseFloat(prods[0].custo_fabrica) || 0;
          produto_novo = false;
          const diff = Math.abs(item.preco_unitario_nota - custo_fabrica);
          status_preco = diff <= 0.01 ? 'igual' : 'divergente';
        }
      }
      await client.query(`
        INSERT INTO itens_nota
          (nota_id, numero_item, ean_nota, ean_validado, produto_id, descricao_nota,
           quantidade, preco_unitario_nota, preco_total_nota, custo_fabrica, status_preco, produto_novo)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
      `, [nota_id, item.numero_item, item.ean_nota, item.ean_nota, produto_id,
          item.descricao_nota, item.quantidade, item.preco_unitario_nota, item.preco_total_nota,
          custo_fabrica, status_preco, produto_novo]);
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

// GET /api/notas
router.get('/', autenticar, async (req, res) => {
  try {
    const notas = await query(`
      SELECT n.*,
        COUNT(i.id)::int AS total_itens,
        COUNT(CASE WHEN i.produto_novo THEN 1 END)::int AS total_novos,
        COUNT(CASE WHEN i.status_preco = 'divergente' THEN 1 END)::int AS total_divergentes
      FROM notas_entrada n
      LEFT JOIN itens_nota i ON i.nota_id = n.id
      GROUP BY n.id
      ORDER BY n.importado_em DESC
    `);
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
    const prods = await query('SELECT id, custo_fabrica FROM produtos WHERE ean = $1 AND ativo = TRUE LIMIT 1', [ean_novo]);
    if (!prods.length) return res.status(404).json({ erro: 'EAN não encontrado no cadastro' });
    const prod = prods[0];
    const custo_fabrica = parseFloat(prod.custo_fabrica) || 0;
    const diff = Math.abs(parseFloat(item.preco_unitario_nota) - custo_fabrica);
    const status_preco = diff <= 0.01 ? 'igual' : 'divergente';
    await query(`
      UPDATE itens_nota SET ean_validado=$1, produto_id=$2, custo_fabrica=$3, status_preco=$4, produto_novo=FALSE, validado_cadastro=TRUE
      WHERE id=$5
    `, [ean_novo, prod.id, custo_fabrica, status_preco, item.id]);
    res.json({ ok: true, status_preco, custo_fabrica });
  } catch (err) {
    res.status(500).json({ erro: err.message });
  }
});

// PATCH /api/notas/:id/liberar
router.patch('/:id/liberar', autenticar, async (req, res) => {
  try {
    const [nota] = await query('SELECT status FROM notas_entrada WHERE id = $1', [req.params.id]);
    if (!nota) return res.status(404).json({ erro: 'Nota não encontrada' });
    if (nota.status !== 'importada') return res.status(400).json({ erro: 'Nota já liberada' });
    await query('UPDATE notas_entrada SET status=$1 WHERE id=$2', ['em_conferencia', req.params.id]);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ erro: err.message });
  }
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
            INSERT INTO conferencia_lotes (conferencia_id, lote, validade, quantidade)
            VALUES ($1,$2,$3,$4)
          `, [novaConf.id, lote.lote || null, lote.validade || null, n(lote.quantidade) || n(conf.qtd_contada)]);
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
