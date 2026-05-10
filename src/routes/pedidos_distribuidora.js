// Pedidos pra Distribuidora — Diretor/CEO emite pedido que vira CSV pro importador do UltraSyst.
//
// Estratégia A (CSV):
// 1. Operador escolhe loja(s) destino + busca produtos + define qtd
// 2. Backend gera 2 CSVs (P_PEDIDOS.csv + P_PEDIDOS_ITENS.csv) no formato oficial
// 3. Salva em pedidos_distrib_geracoes pra histórico/rastreabilidade
// 4. Operador baixa os 2 arquivos e cola na pasta de importação do CD
//
// Formato dos CSVs (do exemplo 06/06/2025):
// - Separador: ';'  ·  Decimal: '.'
// - Data: 'YYYY-MM-DD HH:MM:SS.000'  ·  Hora: 'HH:MM'
// - NOME_CLIENTE com padding de espaços até 60 chars

const express = require('express');
const router = express.Router();
const { query: dbQuery } = require('../db');
const { autenticar, exigirPerfil } = require('../auth');

const adminOuCeo = [autenticar, exigirPerfil('admin', 'ceo')];

// Configuração padrão (do CSV de exemplo)
const EMPRESA_PADRAO        = '1';
const LOCALIZACAO_PADRAO    = '1';
const VEN_CODI_PADRAO       = '19';   // IMPORTADOR
const COD_CONDICAO_PADRAO   = '2';
const COD_PAGAMENTO_PADRAO  = '9';
const COD_TIPOVENDA_PADRAO  = '12';
const COD_TABELA_PADRAO     = '1';
const TIPO_CALCULO_PADRAO   = 'E';    // E=embalagem
const NOME_EMBALAGEM_PADRAO = 'EMB';
const UNIDADE_PADRAO        = 'CX';

// ── Lojas (mapeamento loja_id → cli_codi) ──

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

router.delete('/lojas/:loja_id', adminOuCeo, async (req, res) => {
  try {
    await dbQuery(`DELETE FROM pedidos_distrib_lojas_clientes WHERE loja_id = $1`, [parseInt(req.params.loja_id)]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// Lista clientes do CD pré-cadastrados (pra associar com loja_id)
router.get('/clientes-cd', adminOuCeo, async (req, res) => {
  try {
    const rows = await dbQuery(`SELECT cli_codi, cli_nome, cli_cpf FROM pedidos_distrib_clientes_cd ORDER BY cli_nome`);
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// Lojas do JR Lira (pra UI escolher)
router.get('/lojas-disponiveis', adminOuCeo, async (req, res) => {
  try {
    const rows = await dbQuery(`SELECT id, nome FROM lojas ORDER BY id`);
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// ── Produtos ──

router.get('/produtos', adminOuCeo, async (req, res) => {
  try {
    const search = (req.query.search || '').toString().trim();
    const limit  = Math.min(parseInt(req.query.limit) || 50, 200);
    const params = [];
    let where = `cd_m.mat_situ = 'A' OR cd_m.mat_situ IS NULL`;
    if (search) {
      params.push(`%${search.toLowerCase()}%`);
      params.push(search.replace(/[^0-9]/g, ''));
      where = `(LOWER(cd_m.mat_desc) LIKE $1 OR cd_m.mat_codi = $2 OR cd_m.ean_codi = $2)`;
    }
    params.push(limit);
    const rows = await dbQuery(`
      SELECT cd_m.mat_codi,
             cd_m.mat_desc,
             cd_m.mat_refe,
             cd_m.ean_codi,
             cd_c.pro_prad                AS preco_admin,
             cd_c.pro_prcr                AS preco_compra,
             cd_e.est_quan                AS estoque_cd,
             pe.qtd_embalagem,
             pe.descricao_atual           AS desc_local
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

// ── Helpers de CSV ──

function padNomeCliente(nome) {
  // No exemplo o NOME_CLIENTE tem padding de espaços até 60 chars
  return String(nome || '').padEnd(60, ' ').slice(0, 60);
}

function fmtData(d) {
  // 'YYYY-MM-DD HH:MM:SS.000'
  const pad = n => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.000`;
}
function fmtHora(d) {
  const pad = n => String(n).padStart(2, '0');
  return `${pad(d.getHours())}:${pad(d.getMinutes())}`;
}
function fmtNum(n, casas = 2) {
  return Number(n || 0).toFixed(casas);
}

const HEADER_PEDIDOS = 'COD_PEDIDO;EMPRESA;LOCALIZACAO;COD_VENDEDOR;COD_CLIENTE;COD_CLIENTE_CADASTRO;COD_CONDICAO;COD_PAGAMENTO;COD_TIPOVENDA;COD_TABELA;DATA_EMISSAO;VALOR_PEDIDO;OBSERVACAO;RETORNO;SIT_RETORNO;NUMPEDIDO;HORA_EMISSAO;COD_MOTIVO;ID;LATITUDE;LONGITUDE;CPF_CNPJ;NOME_CLIENTE';
const HEADER_ITENS   = 'COD_PEDIDO;COD_VENDEDOR;COD_PRODUTO;UNIDADE;QUANTIDADE;VALOR;DESCONTO_UNI;DESCONTO_PER;VALOR_TABELA;TIPO_CALCULO;NOME_EMBALAGEM;QTD_EMBALAGEM;ID';

// ── Geração de CSV ──
//
// Body: {
//   observacao?: string,
//   lojas: [
//     { loja_id, itens: [{ mat_codi, quantidade, valor?, qtd_embalagem? }] }
//   ]
// }
//
// Cada loja vira 1 pedido (1 linha no P_PEDIDOS.csv) com seus itens (N linhas em P_PEDIDOS_ITENS.csv).
router.post('/', adminOuCeo, async (req, res) => {
  try {
    const { observacao, lojas } = req.body || {};
    if (!Array.isArray(lojas) || !lojas.length) return res.status(400).json({ erro: 'pedido sem lojas' });
    const por = req.usuario.email || req.usuario.usuario || req.usuario.nome || `id:${req.usuario.id}`;

    // Lookup de lojas → cli_codi
    const lojaIds = lojas.map(l => parseInt(l.loja_id)).filter(Boolean);
    if (!lojaIds.length) return res.status(400).json({ erro: 'lojas sem loja_id' });
    const cliRows = await dbQuery(
      `SELECT loja_id, cli_codi, cli_nome, cli_cpf
         FROM pedidos_distrib_lojas_clientes
        WHERE loja_id = ANY($1::int[])`,
      [lojaIds]
    );
    const cliMap = new Map(cliRows.map(r => [r.loja_id, r]));
    for (const l of lojaIds) {
      if (!cliMap.has(l)) return res.status(400).json({ erro: `loja_id ${l} sem cli_codi cadastrado em /admin-pedidos-distribuidora` });
    }

    // Hidrata todos itens com preço admin do CD
    const todosMatCodi = [...new Set(lojas.flatMap(l => (l.itens || []).map(i => String(i.mat_codi).trim())))].filter(Boolean);
    if (!todosMatCodi.length) return res.status(400).json({ erro: 'nenhum item enviado' });
    const dadosCd = await dbQuery(
      `SELECT cd_m.mat_codi, cd_m.mat_desc, cd_c.pro_prad, pe.qtd_embalagem
         FROM cd_material cd_m
         LEFT JOIN cd_custoprod cd_c ON cd_c.pro_codi = cd_m.mat_codi
         LEFT JOIN produtos_embalagem pe ON pe.mat_codi = cd_m.mat_codi
        WHERE cd_m.mat_codi = ANY($1::text[])`,
      [todosMatCodi]
    );
    const cdMap = new Map(dadosCd.map(x => [x.mat_codi, x]));

    // Determina próximo COD_PEDIDO (sequencial local)
    const [{ proximo }] = await dbQuery(`
      SELECT COALESCE(
        (SELECT MAX(cod_pedido) FROM pedidos_distrib_historico),
        0
      ) + 1 AS proximo
    `);

    const agora = new Date();
    const dataEmissao = fmtData(agora);
    const horaEmissao = fmtHora(agora);
    const obser = String(observacao || `Transferencia ${por}`).slice(0, 120);

    const linhasPedidos = [];
    const linhasItens   = [];
    let codPedido = parseInt(proximo);
    let valorTotalGeral = 0;
    let totalItensGeral = 0;
    const pedidosResumo = [];

    for (const lojaInput of lojas) {
      const lojaId = parseInt(lojaInput.loja_id);
      const cli = cliMap.get(lojaId);
      const itens = lojaInput.itens || [];
      if (!itens.length) continue;

      // Hidrata itens da loja
      const itensHidr = itens.map(it => {
        const cd = cdMap.get(String(it.mat_codi).trim());
        if (!cd) throw Object.assign(new Error(`mat_codi ${it.mat_codi} nao encontrado no CD`), { status: 400 });
        const valor = parseFloat(it.valor) > 0 ? parseFloat(it.valor) : parseFloat(cd.pro_prad);
        if (!(valor > 0)) throw Object.assign(new Error(`mat_codi ${it.mat_codi} sem preco admin`), { status: 400 });
        const qtd = parseFloat(it.quantidade);
        if (!(qtd > 0)) throw Object.assign(new Error(`mat_codi ${it.mat_codi} qtd invalida`), { status: 400 });
        return {
          mat_codi: cd.mat_codi,
          quantidade: qtd,
          valor,
          qtd_embalagem: parseInt(it.qtd_embalagem) || parseInt(cd.qtd_embalagem) || 1,
        };
      });

      const valorPedido = itensHidr.reduce((s, x) => s + x.quantidade * x.valor, 0);

      // Linha do P_PEDIDOS
      linhasPedidos.push([
        codPedido,                    // COD_PEDIDO
        EMPRESA_PADRAO,               // EMPRESA
        LOCALIZACAO_PADRAO,           // LOCALIZACAO
        VEN_CODI_PADRAO,              // COD_VENDEDOR
        cli.cli_codi,                 // COD_CLIENTE
        '0',                          // COD_CLIENTE_CADASTRO (ERP resolve)
        COD_CONDICAO_PADRAO,          // COD_CONDICAO
        COD_PAGAMENTO_PADRAO,         // COD_PAGAMENTO
        COD_TIPOVENDA_PADRAO,         // COD_TIPOVENDA
        COD_TABELA_PADRAO,            // COD_TABELA
        dataEmissao,                  // DATA_EMISSAO
        fmtNum(valorPedido, 2),       // VALOR_PEDIDO
        obser,                        // OBSERVACAO
        '0',                          // RETORNO
        '1',                          // SIT_RETORNO
        '0',                          // NUMPEDIDO
        horaEmissao,                  // HORA_EMISSAO
        '0',                          // COD_MOTIVO
        '0',                          // ID
        '00.00000',                   // LATITUDE
        '00.00000',                   // LONGITUDE
        cli.cli_cpf || '',            // CPF_CNPJ
        padNomeCliente(cli.cli_nome), // NOME_CLIENTE
      ].join(';'));

      // Linhas do P_PEDIDOS_ITENS
      for (const it of itensHidr) {
        linhasItens.push([
          codPedido,                   // COD_PEDIDO
          VEN_CODI_PADRAO,             // COD_VENDEDOR
          it.mat_codi,                 // COD_PRODUTO
          UNIDADE_PADRAO,              // UNIDADE (CX)
          fmtNum(it.quantidade, 0),    // QUANTIDADE (no exemplo é inteiro; ajuste casas se for fracionado)
          fmtNum(it.valor, 2),         // VALOR
          '0',                         // DESCONTO_UNI
          '0',                         // DESCONTO_PER
          fmtNum(it.valor, 2),         // VALOR_TABELA
          TIPO_CALCULO_PADRAO,         // TIPO_CALCULO
          NOME_EMBALAGEM_PADRAO,       // NOME_EMBALAGEM
          it.qtd_embalagem,            // QTD_EMBALAGEM
          '',                          // ID (vazio)
        ].join(';'));
      }

      pedidosResumo.push({
        cod_pedido: codPedido,
        loja_id: lojaId,
        cli_codi: cli.cli_codi,
        valor: valorPedido,
        itens: itensHidr.length,
      });
      valorTotalGeral += valorPedido;
      totalItensGeral += itensHidr.length;
      codPedido += 1;
    }

    if (!linhasPedidos.length) return res.status(400).json({ erro: 'nenhum pedido valido' });

    const csvPedidos = HEADER_PEDIDOS + '\r\n' + linhasPedidos.join('\r\n') + '\r\n';
    const csvItens   = HEADER_ITENS   + '\r\n' + linhasItens.join('\r\n')   + '\r\n';

    // Salva geração
    const [ger] = await dbQuery(
      `INSERT INTO pedidos_distrib_geracoes
         (gerado_por, total_pedidos, total_itens, valor_total, observacao,
          p_pedidos_csv, p_pedidos_itens_csv)
       VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id, gerado_em`,
      [por, pedidosResumo.length, totalItensGeral, valorTotalGeral, obser, csvPedidos, csvItens]
    );

    // Salva também no histórico (1 linha por pedido) — preserva rastreabilidade
    for (const p of pedidosResumo) {
      await dbQuery(
        `INSERT INTO pedidos_distrib_historico
           (cod_pedido, loja_id_destino, emitido_por, valor_total, total_itens,
            observacao, payload_pedido, payload_itens, status)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'csv_gerado')`,
        [p.cod_pedido, p.loja_id, por, p.valor, p.itens, obser, JSON.stringify(p), JSON.stringify({ geracao_id: ger.id })]
      );
    }

    res.json({
      ok: true,
      geracao_id: ger.id,
      gerado_em: ger.gerado_em,
      total_pedidos: pedidosResumo.length,
      total_itens: totalItensGeral,
      valor_total: valorTotalGeral,
      pedidos: pedidosResumo,
      download: {
        p_pedidos: `/api/pedidos-distribuidora/csv/${ger.id}/P_PEDIDOS.csv`,
        p_pedidos_itens: `/api/pedidos-distribuidora/csv/${ger.id}/P_PEDIDOS_ITENS.csv`,
      },
    });
  } catch (e) {
    console.error('[pedidos-distrib POST]', e.message);
    res.status(e.status || 500).json({ erro: e.message });
  }
});

// ── Download dos CSVs gerados ──
// Aceita token via Authorization header OU ?token= (porque <a download> nao pode mandar header).
async function autoricar(req, res, next) {
  let token = (req.headers.authorization || '').replace(/^Bearer\s+/i, '');
  if (!token && req.query.token) token = String(req.query.token);
  if (!token) return res.status(401).json({ erro: 'Token ausente' });
  try {
    const jwt = require('jsonwebtoken');
    const { JWT_SECRET } = require('../auth');
    req.usuario = jwt.verify(token, JWT_SECRET);
    next();
  } catch { res.status(401).json({ erro: 'Token invalido' }); }
}

router.get('/csv/:geracao_id/P_PEDIDOS.csv', autoricar, async (req, res) => {
  await enviarCsv(req, res, 'p_pedidos_csv', 'P_PEDIDOS.csv');
});
router.get('/csv/:geracao_id/P_PEDIDOS_ITENS.csv', autoricar, async (req, res) => {
  await enviarCsv(req, res, 'p_pedidos_itens_csv', 'P_PEDIDOS_ITENS.csv');
});

async function enviarCsv(req, res, coluna, filename) {
  try {
    const [r] = await dbQuery(
      `SELECT ${coluna} AS conteudo FROM pedidos_distrib_geracoes WHERE id = $1`,
      [parseInt(req.params.geracao_id)]
    );
    if (!r) return res.status(404).json({ erro: 'geracao nao encontrada' });
    // Marca baixado
    await dbQuery(`UPDATE pedidos_distrib_geracoes SET baixado_em = COALESCE(baixado_em, NOW()) WHERE id = $1`,
      [parseInt(req.params.geracao_id)]);
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.send(r.conteudo);
  } catch (e) { res.status(500).json({ erro: e.message }); }
}

// ── Histórico ──

router.get('/geracoes', autenticar, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 30, 100);
    const rows = await dbQuery(`
      SELECT id, gerado_em, gerado_por, total_pedidos, total_itens, valor_total,
             observacao, baixado_em
        FROM pedidos_distrib_geracoes
       ORDER BY gerado_em DESC
       LIMIT $1
    `, [limit]);
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

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

module.exports = router;
