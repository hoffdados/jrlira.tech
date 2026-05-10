// Pedidos pra Distribuidora — Diretor/CEO emite pedido que entra no UltraSyst do CD
// via API REST do APK Ponto de Venda.
//
// Fluxo:
// 1. Operador escolhe loja destino + busca produtos + define qtd
// 2. Backend obtém próximo cod_pedido via /ponto_venda/pedido/codigos
// 3. POST /ponto_venda/pedidos (cabeçalho)
// 4. Para cada item: POST /ponto_venda/envio/{codPedido}/itens
// 5. Salva histórico local em pedidos_distrib_historico

const express = require('express');
const router = express.Router();
const { query: dbQuery } = require('../db');
const { autenticar, apenasAdmin } = require('../auth');
const { exigirPerfil } = require('../auth');
const apk = require('../ultrasyst_apk');

const adminOuCeo = [autenticar, exigirPerfil('admin', 'ceo')];

// ── Configuração padrão (do exemplo de CSVs do CD) ──
const VEN_CODI_PADRAO   = '19';
const EMP_CODI_PADRAO   = '001';
const LOC_CODI_PADRAO   = '001';
const CXA_CODI_PADRAO   = '001';
const TIPO_VENDA_PADRAO = 'V'; // venda
const TABELA_VENDA_PADRAO = 4;  // tab.4 — atacado/revenda (típico do CD)
const SITUACAO_PEDIDO   = 0;   // novo

// ── Lojas destino (mapeamento loja→cli_codi) ──

router.get('/lojas', autenticar, async (req, res) => {
  try {
    const rows = await dbQuery(`
      SELECT plc.loja_id, plc.cli_codi, plc.cli_nome, plc.cli_cpf, l.nome AS loja_nome
        FROM pedidos_distrib_lojas_clientes plc
        LEFT JOIN lojas l ON l.id = plc.loja_id
       ORDER BY plc.loja_id
    `);
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

router.put('/lojas/:loja_id', adminOuCeo, async (req, res) => {
  try {
    const lojaId = parseInt(req.params.loja_id);
    const { cli_codi, cli_nome, cli_cpf } = req.body || {};
    if (!cli_codi) return res.status(400).json({ erro: 'cli_codi obrigatorio' });
    await dbQuery(
      `INSERT INTO pedidos_distrib_lojas_clientes (loja_id, cli_codi, cli_nome, cli_cpf, atualizado_em)
       VALUES ($1, $2, $3, $4, NOW())
       ON CONFLICT (loja_id) DO UPDATE SET
         cli_codi = EXCLUDED.cli_codi,
         cli_nome = EXCLUDED.cli_nome,
         cli_cpf  = EXCLUDED.cli_cpf,
         atualizado_em = NOW()`,
      [lojaId, String(cli_codi).trim(), cli_nome?.trim() || null, cli_cpf?.trim() || null]
    );
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// ── Produtos: catálogo do CD com preço administrativo ──

// GET /api/pedidos-distribuidora/produtos?search=&limit=
router.get('/produtos', adminOuCeo, async (req, res) => {
  try {
    const search = (req.query.search || '').toString().trim();
    const limit  = Math.min(parseInt(req.query.limit) || 50, 200);
    const params = [];
    let where = `cd_m.mat_situ = 'A' OR cd_m.mat_situ IS NULL`; // ativos
    if (search) {
      params.push(`%${search.toLowerCase()}%`);
      params.push(search.replace(/[^0-9]/g, '')); // EAN/codigo numérico
      where = `(LOWER(cd_m.mat_desc) LIKE $1 OR cd_m.mat_codi = $2 OR cd_m.ean_codi = $2)`;
    }
    params.push(limit);
    const rows = await dbQuery(`
      SELECT cd_m.mat_codi,
             cd_m.mat_desc,
             cd_m.mat_refe,
             cd_m.ean_codi,
             cd_c.pro_prad                                 AS preco_admin,
             cd_c.pro_prcr                                 AS preco_compra,
             cd_e.est_quan                                 AS estoque_cd,
             pe.qtd_embalagem,
             pe.descricao_atual                            AS desc_local
        FROM cd_material cd_m
        LEFT JOIN cd_custoprod cd_c ON cd_c.pro_codi = cd_m.mat_codi
        LEFT JOIN cd_estoque   cd_e ON cd_e.pro_codi = cd_m.mat_codi
        LEFT JOIN produtos_embalagem pe ON pe.mat_codi = cd_m.mat_codi
       WHERE ${where}
       ORDER BY cd_m.mat_desc
       LIMIT $${params.length}
    `, params);
    res.json(rows);
  } catch (e) {
    console.error('[pedidos-distrib produtos]', e.message);
    res.status(500).json({ erro: e.message });
  }
});

// ── Health da API do APK ──

router.get('/health-api', adminOuCeo, async (req, res) => {
  const r = await apk.health();
  res.json(r);
});

// ── Criar pedido ──
//
// Body: { loja_id_destino, observacao?, itens: [{ mat_codi, quantidade, valor?, qtd_embalagem? }] }
router.post('/', adminOuCeo, async (req, res) => {
  try {
    const { loja_id_destino, observacao, itens } = req.body || {};
    const lojaId = parseInt(loja_id_destino);
    if (!lojaId) return res.status(400).json({ erro: 'loja_id_destino obrigatorio' });
    if (!Array.isArray(itens) || !itens.length) return res.status(400).json({ erro: 'itens vazio' });
    const por = req.usuario.email || req.usuario.usuario || req.usuario.nome || `id:${req.usuario.id}`;

    // Lookup do cliente destino
    const [cli] = await dbQuery(
      `SELECT cli_codi, cli_nome, cli_cpf FROM pedidos_distrib_lojas_clientes WHERE loja_id = $1`,
      [lojaId]
    );
    if (!cli) return res.status(400).json({ erro: `loja_id ${lojaId} sem cli_codi cadastrado` });

    // Hidrata cada item com preço admin do CD
    const matCodis = itens.map(i => String(i.mat_codi).trim()).filter(Boolean);
    if (!matCodis.length) return res.status(400).json({ erro: 'itens sem mat_codi' });
    const dadosCd = await dbQuery(
      `SELECT cd_m.mat_codi, cd_m.mat_desc, cd_c.pro_prad, pe.qtd_embalagem
         FROM cd_material cd_m
         LEFT JOIN cd_custoprod cd_c ON cd_c.pro_codi = cd_m.mat_codi
         LEFT JOIN produtos_embalagem pe ON pe.mat_codi = cd_m.mat_codi
        WHERE cd_m.mat_codi = ANY($1::text[])`,
      [matCodis]
    );
    const cdMap = new Map(dadosCd.map(x => [x.mat_codi, x]));

    const itensHidratados = itens.map(it => {
      const cd = cdMap.get(String(it.mat_codi).trim());
      if (!cd) throw Object.assign(new Error(`mat_codi ${it.mat_codi} nao encontrado no CD`), { status: 400 });
      const valor = parseFloat(it.valor) > 0 ? parseFloat(it.valor) : parseFloat(cd.pro_prad);
      if (!(valor > 0)) throw Object.assign(new Error(`mat_codi ${it.mat_codi} sem preco admin`), { status: 400 });
      const qtd = parseFloat(it.quantidade);
      if (!(qtd > 0)) throw Object.assign(new Error(`mat_codi ${it.mat_codi} qtd invalida`), { status: 400 });
      return {
        mat_codi: cd.mat_codi,
        mat_desc: cd.mat_desc,
        quantidade: qtd,
        valor,
        qtd_embalagem: parseInt(it.qtd_embalagem) || parseInt(cd.qtd_embalagem) || 1,
      };
    });

    const valorTotal = itensHidratados.reduce((s, x) => s + (x.quantidade * x.valor), 0);

    // 1) Próximo cod_pedido
    const codigos = await apk.getCodigosPedido();
    const codPedido = codigos?.cod_pedido || codigos?.proximo || codigos?.next;
    if (!codPedido) {
      throw new Error('API nao retornou cod_pedido em /ponto_venda/pedido/codigos: ' + JSON.stringify(codigos).slice(0, 200));
    }

    const agora = new Date();
    const dataPedido = agora.toISOString().slice(0, 10);
    const horaImpressao = agora.toTimeString().slice(0, 8);

    const cabecalho = {
      id: '', // gerado pelo backend?
      cod_pedido: codPedido,
      emp_codi: EMP_CODI_PADRAO,
      loc_codi: LOC_CODI_PADRAO,
      cxa_codi: CXA_CODI_PADRAO,
      ven_codi: VEN_CODI_PADRAO,
      us_cd:    String(req.usuario.id || ''),
      cli_codi: cli.cli_codi,
      cli_nome: cli.cli_nome || '',
      cli_cpf:  cli.cli_cpf  || '',
      data_pedido: dataPedido,
      valor_pedido: parseFloat(valorTotal.toFixed(2)),
      obser_pedido: (observacao || `Transferencia ${por} via app`).slice(0, 200),
      hora_impressao: horaImpressao,
      tipo_venda: TIPO_VENDA_PADRAO,
      tabela_venda: TABELA_VENDA_PADRAO,
      situacao_pedido: SITUACAO_PEDIDO,
      valor_descto: 0,
      perct_descto: 0,
      valor_acresc: 0,
      perct_acresc: 0,
      valor_troco:  0,
      valor_bruto:  parseFloat(valorTotal.toFixed(2)),
      android_id: 'jrlira-tech-app',
    };

    // 2) Cria o pedido
    let pedidoCriadoResp;
    try {
      pedidoCriadoResp = await apk.criarPedido(cabecalho);
    } catch (e) {
      // Salva tentativa falha pra debug
      await dbQuery(
        `INSERT INTO pedidos_distrib_historico
           (cod_pedido, loja_id_destino, emitido_por, valor_total, total_itens,
            observacao, payload_pedido, payload_itens, status, erro_msg)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'erro',$9)`,
        [codPedido, lojaId, por, valorTotal, itensHidratados.length,
         cabecalho.obser_pedido, JSON.stringify(cabecalho),
         JSON.stringify(itensHidratados), e.message]
      );
      throw e;
    }

    // 3) Envia cada item
    const itensEnviados = [];
    const itensFalha = [];
    for (const it of itensHidratados) {
      const itemBody = {
        cod_pedido: codPedido,
        pro_codi: it.mat_codi,
        pro_vdtp: 'E', // E=embalagem (do exemplo de CSV)
        quantidade: it.quantidade,
        valor: parseFloat(it.valor.toFixed(4)),
        desconto_valor: 0,
        desconto_perce: 0,
        valor_tabela: parseFloat(it.valor.toFixed(4)),
        nome_embalagem: 'CX',
        qtde_embalagem: it.qtd_embalagem || 1,
        android_id: 'jrlira-tech-app',
        ven_codi: VEN_CODI_PADRAO,
        cor_codi: null,
        cor_desr: null,
        tam_codi: null,
        tam_desr: null,
      };
      try {
        await apk.adicionarItem(codPedido, itemBody);
        itensEnviados.push(itemBody);
      } catch (e) {
        itensFalha.push({ ...itemBody, erro: e.message });
      }
    }

    // 4) Salva histórico
    const status = itensFalha.length === 0 ? 'enviado' : 'parcial';
    const erroMsg = itensFalha.length ? `${itensFalha.length} item(ns) falharam` : null;
    await dbQuery(
      `INSERT INTO pedidos_distrib_historico
         (cod_pedido, loja_id_destino, emitido_por, valor_total, total_itens,
          observacao, payload_pedido, payload_itens, status, erro_msg)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
      [codPedido, lojaId, por, valorTotal, itensHidratados.length,
       cabecalho.obser_pedido, JSON.stringify(cabecalho),
       JSON.stringify({ enviados: itensEnviados, falhas: itensFalha }),
       status, erroMsg]
    );

    res.json({
      ok: itensFalha.length === 0,
      cod_pedido: codPedido,
      total_itens: itensHidratados.length,
      enviados: itensEnviados.length,
      falhas: itensFalha,
      valor_total: valorTotal,
    });
  } catch (e) {
    console.error('[pedidos-distrib POST]', e.message);
    res.status(e.status || 500).json({ erro: e.message });
  }
});

// ── Histórico ──
router.get('/historico', autenticar, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const rows = await dbQuery(`
      SELECT h.id, h.cod_pedido, h.loja_id_destino, l.nome AS loja_nome,
             h.emitido_por, h.emitido_em, h.valor_total, h.total_itens,
             h.observacao, h.status, h.erro_msg
        FROM pedidos_distrib_historico h
        LEFT JOIN lojas l ON l.id = h.loja_id_destino
       ORDER BY h.emitido_em DESC
       LIMIT $1
    `, [limit]);
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

router.get('/historico/:id', autenticar, async (req, res) => {
  try {
    const [row] = await dbQuery(`
      SELECT h.*, l.nome AS loja_nome
        FROM pedidos_distrib_historico h
        LEFT JOIN lojas l ON l.id = h.loja_id_destino
       WHERE h.id = $1
    `, [parseInt(req.params.id)]);
    if (!row) return res.status(404).json({ erro: 'nao encontrado' });
    res.json(row);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

module.exports = router;
