// Pedidos pra Distribuidora — Diretor/CEO emite pedido que vira CSV pro importador do UltraSyst.
//
// Modelo:
// - 6 LOJAS (SUPERASA) recebem; 5 CDs (ASA BRANCA, ASA FRIOS, CASA BRANCA) emitem e podem receber entre si.
// - Operador escolhe o CD ORIGEM (de onde sai a mercadoria) + destinos (lojas e/ou outros CDs).
// - CLI_CODI de cada destino é descoberto via relay do CD origem (CLIENTE WHERE CLI_CGC = cnpj).
// - Backend gera 2 CSVs (P_PEDIDOS.csv + P_PEDIDOS_ITENS.csv) no formato oficial.
// - Operador baixa e cola na pasta de importação do UltraSyst do CD origem.

const express = require('express');
const router = express.Router();
const { query: dbQuery } = require('../db');
const { autenticar, exigirPerfil } = require('../auth');
const { listarCds, clientePorCodigo } = require('../cds');

const adminOuCeo = [autenticar, exigirPerfil('admin', 'ceo')];

// Defaults do CSV de exemplo
const EMPRESA_PADRAO        = '1';
const LOCALIZACAO_PADRAO    = '1';
const VEN_CODI_PADRAO       = '19';   // IMPORTADOR
const COD_CONDICAO_PADRAO   = '2';
const COD_PAGAMENTO_PADRAO  = '9';
const COD_TIPOVENDA_PADRAO  = '12';
const COD_TABELA_PADRAO     = '1';
const TIPO_CALCULO_PADRAO   = 'E';
const NOME_EMBALAGEM_PADRAO = 'EMB';
const UNIDADE_PADRAO        = 'CX';

// ── Destinos (lojas + CDs) ──

router.get('/destinos', autenticar, async (req, res) => {
  try {
    const rows = await dbQuery(`
      SELECT id, tipo, codigo, nome, cnpj, loja_id, cd_codigo, ativo
        FROM pedidos_distrib_destinos
       WHERE ativo = TRUE
       ORDER BY tipo, nome
    `);
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// CDs origem: lista CDs cadastrados em /admin-cds que tem destino correspondente em pedidos_distrib_destinos
router.get('/cds-origem', adminOuCeo, async (req, res) => {
  try {
    const cds = await listarCds(true); // só ativos
    // Vincula com destinos pra mostrar nome amigável
    const destinos = await dbQuery(
      `SELECT cd_codigo, cnpj, nome FROM pedidos_distrib_destinos WHERE tipo='CD' AND cd_codigo IS NOT NULL`
    );
    const destMap = new Map(destinos.map(d => [d.cd_codigo, d]));
    const out = cds
      .filter(c => destMap.has(c.codigo))
      .map(c => ({
        codigo: c.codigo,
        nome: destMap.get(c.codigo).nome,
        cnpj: destMap.get(c.codigo).cnpj,
        banco: c.banco,
        url: c.url,
      }));
    res.json(out);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// Diagnóstico: dado um CD origem, busca CLI_CODI de cada destino via relay
router.get('/cli-codi-lookup', adminOuCeo, async (req, res) => {
  try {
    const cdOrigem = String(req.query.cd_origem || '').trim();
    if (!cdOrigem) return res.status(400).json({ erro: 'cd_origem obrigatorio' });
    const destinos = await dbQuery(`SELECT id, tipo, codigo, nome, cnpj FROM pedidos_distrib_destinos WHERE ativo=TRUE`);
    const cli = await clientePorCodigo(cdOrigem);
    const result = [];
    for (const d of destinos) {
      try {
        const r = await cli.query(
          `SELECT TOP 1 CLI_CODI, CLI_RAZS, CLI_CGC FROM CLIENTE WITH (NOLOCK) WHERE REPLACE(REPLACE(REPLACE(CLI_CGC,'.',''),'/',''),'-','') = '${d.cnpj}'`
        );
        const row = r.rows?.[0];
        result.push({
          destino_id: d.id,
          tipo: d.tipo, nome: d.nome, cnpj: d.cnpj,
          cli_codi: row?.CLI_CODI || null,
          cli_razs: row?.CLI_RAZS?.trim() || null,
          ok: !!row,
        });
      } catch (e) {
        result.push({ destino_id: d.id, tipo: d.tipo, nome: d.nome, cnpj: d.cnpj, ok: false, erro: e.message });
      }
    }
    res.json({ cd_origem: cdOrigem, destinos: result });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// ── Catálogo de produtos (do CD legado já sincronizado) ──

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
      SELECT cd_m.mat_codi, cd_m.mat_desc, cd_m.mat_refe, cd_m.ean_codi,
             cd_c.pro_prad   AS preco_admin,
             cd_c.pro_prcr   AS preco_compra,
             cd_e.est_quan   AS estoque_cd,
             pe.qtd_embalagem,
             pe.descricao_atual AS desc_local
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

function padNomeCliente(nome) { return String(nome || '').padEnd(60, ' ').slice(0, 60); }
function fmtData(d) {
  const pad = n => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.000`;
}
function fmtHora(d) {
  const pad = n => String(n).padStart(2, '0');
  return `${pad(d.getHours())}:${pad(d.getMinutes())}`;
}
function fmtNum(n, casas = 2) { return Number(n || 0).toFixed(casas); }

const HEADER_PEDIDOS = 'COD_PEDIDO;EMPRESA;LOCALIZACAO;COD_VENDEDOR;COD_CLIENTE;COD_CLIENTE_CADASTRO;COD_CONDICAO;COD_PAGAMENTO;COD_TIPOVENDA;COD_TABELA;DATA_EMISSAO;VALOR_PEDIDO;OBSERVACAO;RETORNO;SIT_RETORNO;NUMPEDIDO;HORA_EMISSAO;COD_MOTIVO;ID;LATITUDE;LONGITUDE;CPF_CNPJ;NOME_CLIENTE';
const HEADER_ITENS   = 'COD_PEDIDO;COD_VENDEDOR;COD_PRODUTO;UNIDADE;QUANTIDADE;VALOR;DESCONTO_UNI;DESCONTO_PER;VALOR_TABELA;TIPO_CALCULO;NOME_EMBALAGEM;QTD_EMBALAGEM;ID';

// ── Geração de CSV ──
//
// Body: {
//   cd_origem_codigo: "srv1-itautuba",
//   observacao?: string,
//   destinos: [{ destino_id, qtd_unica }],
//   itens:    [{ mat_codi, valor?, qtd_embalagem? }]
// }
// Cesta única replicada pra cada destino (qtd_unica multiplica todas as quantidades).
router.post('/', adminOuCeo, async (req, res) => {
  try {
    const { cd_origem_codigo, observacao, destinos, itens } = req.body || {};
    if (!cd_origem_codigo) return res.status(400).json({ erro: 'cd_origem_codigo obrigatorio' });
    if (!Array.isArray(destinos) || !destinos.length) return res.status(400).json({ erro: 'destinos vazio' });
    if (!Array.isArray(itens) || !itens.length) return res.status(400).json({ erro: 'itens vazio' });
    const por = req.usuario.email || req.usuario.usuario || req.usuario.nome || `id:${req.usuario.id}`;

    // 1) Resolve dados do CD origem (pra excluir do destino e usar relay)
    const [cdOrigem] = await dbQuery(
      `SELECT codigo, nome, cnpj FROM pedidos_distrib_destinos WHERE tipo='CD' AND cd_codigo = $1`,
      [cd_origem_codigo]
    );
    if (!cdOrigem) return res.status(400).json({ erro: `CD origem "${cd_origem_codigo}" nao cadastrado em destinos` });

    // 2) Carrega dados dos destinos
    const destIds = destinos.map(d => parseInt(d.destino_id)).filter(Boolean);
    if (!destIds.length) return res.status(400).json({ erro: 'destinos sem destino_id' });
    const destRows = await dbQuery(
      `SELECT id, tipo, codigo, nome, cnpj, loja_id FROM pedidos_distrib_destinos
        WHERE id = ANY($1::int[]) AND ativo = TRUE`,
      [destIds]
    );
    const destMap = new Map(destRows.map(d => [d.id, d]));
    for (const d of destinos) {
      const id = parseInt(d.destino_id);
      if (!destMap.has(id)) return res.status(400).json({ erro: `destino_id ${id} nao encontrado/ativo` });
      // Não permite enviar pro próprio CD origem
      const dest = destMap.get(id);
      if (dest.tipo === 'CD' && dest.cnpj === cdOrigem.cnpj) {
        return res.status(400).json({ erro: `nao pode enviar pra si mesmo: ${dest.nome}` });
      }
    }

    // 3) Resolve CLI_CODI de cada destino via relay do CD origem
    let cli;
    try { cli = await clientePorCodigo(cd_origem_codigo); }
    catch (e) { return res.status(400).json({ erro: `relay do CD ${cd_origem_codigo} indisponivel: ${e.message}` }); }

    const cliMap = new Map(); // destino_id -> { CLI_CODI }
    for (const d of destRows) {
      try {
        const r = await cli.query(
          `SELECT TOP 1 CLI_CODI FROM CLIENTE WITH (NOLOCK)
            WHERE REPLACE(REPLACE(REPLACE(CLI_CGC,'.',''),'/',''),'-','') = '${d.cnpj}'`
        );
        const cliCodi = r.rows?.[0]?.CLI_CODI;
        if (!cliCodi) {
          return res.status(400).json({ erro: `CLI_CODI nao encontrado pra "${d.nome}" (CNPJ ${d.cnpj}) no banco do CD ${cd_origem_codigo}` });
        }
        cliMap.set(d.id, String(cliCodi).trim());
      } catch (e) {
        return res.status(500).json({ erro: `falha consultando CLIENTE pra "${d.nome}": ${e.message}` });
      }
    }

    // 4) Hidrata itens com preço admin do CD legado (catálogo cd_material/cd_custoprod)
    const matCodis = [...new Set(itens.map(i => String(i.mat_codi).trim()))].filter(Boolean);
    const dadosCd = await dbQuery(
      `SELECT cd_m.mat_codi, cd_m.mat_desc, cd_c.pro_prad, pe.qtd_embalagem
         FROM cd_material cd_m
         LEFT JOIN cd_custoprod cd_c ON cd_c.pro_codi = cd_m.mat_codi
         LEFT JOIN produtos_embalagem pe ON pe.mat_codi = cd_m.mat_codi
        WHERE cd_m.mat_codi = ANY($1::text[])`,
      [matCodis]
    );
    const cdMap = new Map(dadosCd.map(x => [x.mat_codi, x]));

    const itensBase = itens.map(it => {
      const cd = cdMap.get(String(it.mat_codi).trim());
      if (!cd) throw Object.assign(new Error(`mat_codi ${it.mat_codi} nao encontrado no catalogo`), { status: 400 });
      const valor = parseFloat(it.valor) > 0 ? parseFloat(it.valor) : parseFloat(cd.pro_prad);
      if (!(valor > 0)) throw Object.assign(new Error(`mat_codi ${it.mat_codi} sem preco admin`), { status: 400 });
      return {
        mat_codi: cd.mat_codi,
        valor,
        qtd_embalagem: parseInt(it.qtd_embalagem) || parseInt(cd.qtd_embalagem) || 1,
      };
    });

    // 5) Próximo COD_PEDIDO pra esse CD origem
    const [{ proximo }] = await dbQuery(`
      SELECT COALESCE(MAX(cod_pedido), 0) + 1 AS proximo
        FROM pedidos_distrib_historico
       WHERE cd_origem_codigo = $1
    `, [cd_origem_codigo]);

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

    for (const dInput of destinos) {
      const id = parseInt(dInput.destino_id);
      const dest = destMap.get(id);
      const qtdMul = parseFloat(dInput.qtd_unica) || 1;
      if (!(qtdMul > 0)) throw Object.assign(new Error(`destino "${dest.nome}" sem qtd_unica valida`), { status: 400 });
      const cliCodi = cliMap.get(id);

      const valorPedido = itensBase.reduce((s, x) => s + x.valor * qtdMul, 0);

      linhasPedidos.push([
        codPedido,                EMPRESA_PADRAO,         LOCALIZACAO_PADRAO,
        VEN_CODI_PADRAO,          cliCodi,                '0',
        COD_CONDICAO_PADRAO,      COD_PAGAMENTO_PADRAO,   COD_TIPOVENDA_PADRAO,
        COD_TABELA_PADRAO,        dataEmissao,            fmtNum(valorPedido, 2),
        obser,                    '0',                    '1',
        '0',                      horaEmissao,            '0',
        '0',                      '00.00000',             '00.00000',
        dest.cnpj,                padNomeCliente(dest.nome),
      ].join(';'));

      for (const it of itensBase) {
        linhasItens.push([
          codPedido,               VEN_CODI_PADRAO,        it.mat_codi,
          UNIDADE_PADRAO,          fmtNum(qtdMul, 0),      fmtNum(it.valor, 2),
          '0',                     '0',                    fmtNum(it.valor, 2),
          TIPO_CALCULO_PADRAO,     NOME_EMBALAGEM_PADRAO,  it.qtd_embalagem,
          '',
        ].join(';'));
      }

      pedidosResumo.push({
        cod_pedido: codPedido,
        destino_id: dest.id,
        destino_tipo: dest.tipo,
        destino_nome: dest.nome,
        cli_codi: cliCodi,
        valor: valorPedido,
        itens: itensBase.length,
      });
      valorTotalGeral += valorPedido;
      totalItensGeral += itensBase.length;
      codPedido += 1;
    }

    if (!linhasPedidos.length) return res.status(400).json({ erro: 'nenhum pedido valido' });

    const csvPedidos = HEADER_PEDIDOS + '\r\n' + linhasPedidos.join('\r\n') + '\r\n';
    const csvItens   = HEADER_ITENS   + '\r\n' + linhasItens.join('\r\n')   + '\r\n';

    // Salva geração
    const [ger] = await dbQuery(
      `INSERT INTO pedidos_distrib_geracoes
         (gerado_por, cd_origem_codigo, total_pedidos, total_itens, valor_total, observacao,
          p_pedidos_csv, p_pedidos_itens_csv)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id, gerado_em`,
      [por, cd_origem_codigo, pedidosResumo.length, totalItensGeral, valorTotalGeral, obser, csvPedidos, csvItens]
    );

    // Histórico (1 linha por pedido)
    for (const p of pedidosResumo) {
      const dest = destMap.get(p.destino_id);
      await dbQuery(
        `INSERT INTO pedidos_distrib_historico
           (cod_pedido, cd_origem_codigo, loja_id_destino, destino_id, destino_nome, destino_tipo,
            emitido_por, valor_total, total_itens, observacao,
            payload_pedido, payload_itens, status)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'csv_gerado')`,
        [p.cod_pedido, cd_origem_codigo, dest.loja_id, p.destino_id, p.destino_nome, p.destino_tipo,
         por, p.valor, p.itens, obser,
         JSON.stringify(p), JSON.stringify({ geracao_id: ger.id })]
      );
    }

    res.json({
      ok: true,
      geracao_id: ger.id,
      gerado_em: ger.gerado_em,
      cd_origem: cdOrigem,
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

// ── Download dos CSVs ──
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
      SELECT id, gerado_em, gerado_por, cd_origem_codigo, total_pedidos, total_itens,
             valor_total, observacao, baixado_em
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
      SELECT h.id, h.cod_pedido, h.cd_origem_codigo, h.destino_tipo, h.destino_nome,
             h.emitido_por, h.emitido_em, h.valor_total, h.total_itens,
             h.observacao, h.status, h.erro_msg
        FROM pedidos_distrib_historico h
       ORDER BY h.emitido_em DESC
       LIMIT $1
    `, [limit]);
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

module.exports = router;
