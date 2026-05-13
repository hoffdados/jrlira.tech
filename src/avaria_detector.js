// Detector de avarias + devoluções pós-entrega.
// Roda diariamente; cria 1 evento em avaria_eventos por (loja, produto, data, tipo).
//
// Fontes:
// 1. vendas_historico WHERE tipo_saida='avaria' AND data_venda >= DATA_CORTE
// 2. compras_historico WHERE tipo_entrada='devolucao' AND data_entrada >= DATA_CORTE
//    Excluindo as que já foram tratadas no app (existem em devolucoes).
//
// Identificação de fornecedor:
// A) Produto está em cd_material com for_codi → fornecedor do CD (preferencial). Preço = cd_custoprod.pro_prad
// B) Não está no CD → fornecedor da última compra direta na loja (compras_historico 180d). Preço = custo da compra

const { pool } = require('./db');

const DATA_CORTE = '2026-04-01';

async function identificarFornecedor(client, loja_id, codigobarra) {
  // Normaliza o EAN
  const eanN = String(codigobarra || '').replace(/^0+/, '');
  if (!eanN) return null;

  // A) Tenta no CD: cruza ean → cd_material → cd_fornecedor + cd_custoprod
  // Pode ter o produto em vários CDs. Pega o que tem fornecedor cadastrado, prioriza ITB.
  const { rows: cdRows } = await client.query(
    `SELECT cm.cd_codigo, cm.mat_codi, cf.for_cgc, cf.for_nome, cc.pro_prad AS preco_admin
       FROM cd_ean ce
       JOIN cd_material cm ON cm.cd_codigo=ce.cd_codigo AND cm.mat_codi=ce.mat_codi
       LEFT JOIN cd_fornecedor cf ON cf.cd_codigo=cm.cd_codigo AND cf.for_codi=cm.for_codi
       LEFT JOIN cd_custoprod cc ON cc.cd_codigo=cm.cd_codigo AND cc.pro_codi=cm.mat_codi
      WHERE NULLIF(LTRIM(ce.ean_codi,'0'),'') = $1
        AND cm.for_codi IS NOT NULL
      ORDER BY CASE WHEN cm.cd_codigo='srv1-itautuba' THEN 0 ELSE 1 END,
               (cf.for_cgc IS NOT NULL) DESC,
               (cc.pro_prad IS NOT NULL) DESC
      LIMIT 1`, [eanN]);
  if (cdRows.length && cdRows[0].for_cgc) {
    return {
      cnpj: cdRows[0].for_cgc,
      nome: cdRows[0].for_nome,
      fonte: 'cd_fornecedor',
      preco: cdRows[0].preco_admin ? parseFloat(cdRows[0].preco_admin) : null,
      preco_origem: 'cd_custoprod',
    };
  }

  // B) Fornecedor direto da loja: última compra do produto na loja (180d)
  // compras_historico só tem fornecedor_cnpj; cruza com fornecedores pra pegar nome
  const { rows: lojaRows } = await client.query(
    `SELECT ch.fornecedor_cnpj,
            COALESCE(f.razao_social, f.fantasia) AS fornecedor_nome,
            (ch.custo_total / NULLIF(ch.qtd_comprada,0))::numeric(14,4) AS custo_un,
            ch.data_entrada
       FROM compras_historico ch
       LEFT JOIN fornecedores f
         ON REGEXP_REPLACE(COALESCE(f.cnpj,''),'\\D','','g') =
            REGEXP_REPLACE(COALESCE(ch.fornecedor_cnpj,''),'\\D','','g')
      WHERE ch.loja_id = $1
        AND COALESCE(ch.tipo_entrada,'compra') = 'compra'
        AND ch.codigobarra = $2
        AND ch.fornecedor_cnpj IS NOT NULL
        AND ch.data_entrada >= CURRENT_DATE - INTERVAL '180 days'
      ORDER BY ch.data_entrada DESC LIMIT 1`, [loja_id, codigobarra]);
  if (lojaRows.length) {
    return {
      cnpj: lojaRows[0].fornecedor_cnpj,
      nome: lojaRows[0].fornecedor_nome,
      fonte: 'compras_historico',
      preco: lojaRows[0].custo_un ? parseFloat(lojaRows[0].custo_un) : null,
      preco_origem: 'compras_historico',
    };
  }
  return null;
}

async function detectarAvarias() {
  const client = await pool.connect();
  const stats = { avarias_novas: 0, devolucoes_novas: 0 };
  try {
    // 1) Avarias de vendas_historico
    const { rows: avarias } = await client.query(`
      SELECT vh.id, vh.loja_id, vh.codigobarra, vh.data_venda, vh.qtd_vendida,
             pe.descricao
        FROM vendas_historico vh
        LEFT JOIN produtos_externo pe
          ON pe.loja_id = vh.loja_id AND pe.codigobarra = vh.codigobarra
       WHERE vh.tipo_saida = 'avaria'
         AND vh.data_venda >= $1::date
         AND NOT EXISTS (
           SELECT 1 FROM avaria_eventos ae
            WHERE ae.fonte = 'vendas_historico'
              AND ae.loja_id = vh.loja_id
              AND ae.codigobarra = vh.codigobarra
              AND ae.data_evento = vh.data_venda
              AND ae.tipo = 'avaria'
         )
       ORDER BY vh.data_venda DESC
       LIMIT 5000
    `, [DATA_CORTE]);

    for (const a of avarias) {
      const forn = await identificarFornecedor(client, a.loja_id, a.codigobarra);
      const valorTotal = forn?.preco ? (parseFloat(a.qtd_vendida) * forn.preco).toFixed(2) : null;
      await client.query(`
        INSERT INTO avaria_eventos
          (loja_id, tipo, fonte, fonte_id, codigobarra, descricao_produto,
           data_evento, qtd, valor_unitario, valor_total,
           fornecedor_cnpj_sugerido, fornecedor_nome_sugerido, fonte_sugestao,
           preco_cobranca, preco_origem)
        VALUES ($1,'avaria','vendas_historico',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
        ON CONFLICT DO NOTHING
      `, [
        a.loja_id, a.id, a.codigobarra, a.descricao || null,
        a.data_venda, a.qtd_vendida, forn?.preco || null, valorTotal,
        forn?.cnpj || null, forn?.nome || null, forn?.fonte || null,
        forn?.preco || null, forn?.preco_origem || null,
      ]);
      stats.avarias_novas++;
    }

    // 2) Devoluções pós-entrega: vem de devolucoes_compra_historico (TESTDEVOLUCAO via Pentaho)
    //    Excluindo as que JÁ FORAM tratadas no app (devolucoes table)
    //    Pra cada item da devolução, gera 1 evento.
    //    chave_nfe_compra_original referencia a NF de entrada — extraímos o número (chars 26-34) pra cruzar
    const { rows: devs } = await client.query(`
      SELECT dch.id AS dev_id, dch.loja_id, dch.fornecedor_cnpj, dch.fornecedor_nome,
             dch.numero_nfe, dch.chave_nfe, dch.data_devolucao, dch.usuario,
             dch.chave_nfe_compra_original,
             SUBSTRING(dch.chave_nfe_compra_original FROM 26 FOR 9) AS numero_nfe_original,
             dci.id AS item_id, dci.codigobarra, dci.qtd, dci.preco_unitario, dci.valor_total,
             pe.descricao
        FROM devolucoes_compra_historico dch
        JOIN devolucoes_compra_itens_historico dci
          ON dci.loja_id = dch.loja_id AND dci.devolucao_codigo = dch.devolucao_codigo
        LEFT JOIN produtos_externo pe
          ON pe.loja_id = dch.loja_id AND pe.codigobarra = dci.codigobarra
       WHERE dch.data_devolucao >= $1::timestamp
         AND dci.codigobarra IS NOT NULL AND dci.codigobarra <> ''
         AND NOT EXISTS (
           SELECT 1 FROM avaria_eventos ae
            WHERE ae.fonte = 'devolucoes_compra_historico'
              AND ae.fonte_id = dci.id
              AND ae.tipo = 'devolucao'
         )
         AND NOT EXISTS (
           SELECT 1 FROM devolucoes d
            WHERE d.loja_id = dch.loja_id
              AND (d.xml_chave_nfe = dch.chave_nfe OR d.xml_numero_nf = dch.numero_nfe)
         )
       ORDER BY dch.data_devolucao DESC, dci.id
       LIMIT 10000
    `, [DATA_CORTE]);

    for (const d of devs) {
      // Preferir fornecedor da devolução (já vem do TESTDEVOLUCAO);
      // se não, tenta identificar via cd_material/compras_historico
      let fornCnpj = d.fornecedor_cnpj;
      let fornNome = d.fornecedor_nome;
      let fonteSug = 'devolucao_origem';
      let precoCobranca = d.preco_unitario ? parseFloat(d.preco_unitario) : null;
      let precoOrigem = 'devolucao_origem';
      if (!fornCnpj) {
        const forn = await identificarFornecedor(client, d.loja_id, d.codigobarra);
        if (forn) {
          fornCnpj = forn.cnpj; fornNome = forn.nome; fonteSug = forn.fonte;
          if (!precoCobranca) { precoCobranca = forn.preco; precoOrigem = forn.preco_origem; }
        }
      }

      // Regra: classifica baseado no usuário que emitiu a devolução
      //  - Comprador → mantém pendente (cobrar fornecedor)
      //  - Outro cargo conhecido → tratado_cadastro (devolução no ato)
      //  - Sem usuário → pendente p/ revisão manual
      let statusInicial = 'pendente';
      let observacao = d.numero_nfe ? `NF dev ${d.numero_nfe}` : '';
      if (d.usuario) {
        const { rows: [u] } = await client.query(
          `SELECT cargo FROM usuarios_ecocentauro WHERE UPPER(usuario) = UPPER($1)`, [d.usuario]);
        if (u && u.cargo && u.cargo !== 'comprador') statusInicial = 'tratado_cadastro';
        observacao += ` | Usuario ${d.usuario}${u?.cargo ? ' ('+u.cargo+')' : ' (?)'}`;
      }

      await client.query(`
        INSERT INTO avaria_eventos
          (loja_id, tipo, fonte, fonte_id, codigobarra, descricao_produto,
           data_evento, qtd, valor_unitario, valor_total,
           fornecedor_cnpj_sugerido, fornecedor_nome_sugerido, fonte_sugestao,
           preco_cobranca, preco_origem, observacao, status,
           classificado_em, classificado_por)
        VALUES ($1,'devolucao','devolucoes_compra_historico',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,
                CASE WHEN $15<>'pendente' THEN NOW() ELSE NULL END,
                CASE WHEN $15<>'pendente' THEN 'auto:gap_data' ELSE NULL END)
        ON CONFLICT DO NOTHING
      `, [
        d.loja_id, d.item_id, d.codigobarra, d.descricao || null,
        d.data_devolucao, d.qtd, precoCobranca, d.valor_total,
        fornCnpj, fornNome, fonteSug,
        precoCobranca, precoOrigem,
        observacao || null, statusInicial,
      ]);
      stats.devolucoes_novas++;
    }
  } finally {
    client.release();
  }
  return stats;
}

module.exports = { detectarAvarias, identificarFornecedor };
