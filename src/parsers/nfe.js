const xml2js = require('xml2js');
const { parseEmbalagem } = require('../parser_embalagem');

const MAX_XML_BYTES = 5 * 1024 * 1024;

function n(v) { return parseFloat(v) || 0; }

function makeParser() {
  return new xml2js.Parser({
    explicitArray: false,
    ignoreAttrs: false,
    tagNameProcessors: [xml2js.processors.stripPrefix],
    async: true,
  });
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

async function parseRoot(buf) {
  if (!buf || buf.length === 0) throw new Error('XML vazio');
  if (buf.length > MAX_XML_BYTES) throw new Error('XML excede o tamanho máximo permitido');
  const raw = await makeParser().parseStringPromise(buf.toString('utf8'));
  const nfeProc = raw.nfeProc || raw;
  const nfe = nfeProc.NFe;
  if (!nfe) throw new Error('Elemento NFe não encontrado no XML');
  const inf = nfe.infNFe;
  if (!inf) throw new Error('Elemento infNFe não encontrado no XML');

  let chave = null;
  if (nfeProc.protNFe?.infProt?.chNFe) chave = nfeProc.protNFe.infProt.chNFe;
  else if (inf.$?.Id) chave = inf.$.Id.replace('NFe', '');

  return { inf, ide: inf.ide || {}, emit: inf.emit || {}, dest: inf.dest || {}, chave };
}

// Versão completa (notas.js): header + itens com impostos
async function parseNFe(buf) {
  const { inf, ide, emit, dest, chave } = await parseRoot(buf);
  const tot = inf.total?.ICMSTot || {};

  const header = {
    chave_nfe: chave,
    numero_nota: String(ide.nNF || ''),
    serie: String(ide.serie || ''),
    natureza_op: String(ide.natOp || ''),
    fornecedor_nome: String(emit.xNome || emit.xFant || ''),
    fornecedor_cnpj: String(emit.CNPJ || emit.CPF || ''),
    dest_cnpj: String(dest?.CNPJ || dest?.CPF || ''),
    dest_nome: String(dest?.xNome || ''),
    data_emissao: String(ide.dhEmi || ide.dEmi || '').substring(0, 10) || null,
    valor_total: n(tot.vNF),
    tot_vprod: n(tot.vProd),
    tot_vbc: n(tot.vBC),
    tot_vicms: n(tot.vICMS),
    tot_vbcst: n(tot.vBCST),
    tot_vst: n(tot.vST),
    tot_vfcp_st: n(tot.vFCPST),
    tot_vipi: n(tot.vIPI),
    tot_vdesc: n(tot.vDesc),
    tot_vfrete: n(tot.vFrete),
    tot_vseg: n(tot.vSeg),
    tot_voutro: n(tot.vOutro),
  };

  const detRaw = inf.det;
  const dets = !detRaw ? [] : (Array.isArray(detRaw) ? detRaw : [detRaw]);

  const itens = dets.map((det, idx) => {
    const prod = det.prod || {};
    const qCom = n(prod.qCom) || n(prod.qTrib) || 1;
    const qTrib = n(prod.qTrib) || 0;
    const uCom = String(prod.uCom || '').trim().toUpperCase();
    const uTrib = String(prod.uTrib || '').trim().toUpperCase();
    const vprod = n(prod.vProd);
    const vdesc_item = n(prod.vDesc);
    const vfrete_item = n(prod.vFrete);
    const vseg_item = n(prod.vSeg);
    const voutro_item = n(prod.vOutro);

    const { vicms_bc, vicms, vst_bc, vst, vfcp_st } = extractICMS(det);
    const { vipi_bc, vipi } = extractIPI(det);

    const custo_total = (vprod - vdesc_item) + vfrete_item + vseg_item + voutro_item + vipi + vst + vfcp_st;
    const preco_total_nota = parseFloat(custo_total.toFixed(2));

    const eanRaw = prod.cEAN;
    const ean = (!eanRaw || eanRaw === 'SEM GTIN') ? null : String(eanRaw).trim();
    const eanTribRaw = prod.cEANTrib;
    const eanTrib = (!eanTribRaw || eanTribRaw === 'SEM GTIN') ? null : String(eanTribRaw).trim();

    // Qtd por caixa derivada do XML.
    // Caminho A: qCom != qTrib e ratio inteiro >= 2 → derivado direto.
    // Caminho B: uCom == uTrib (fornecedor factura tudo em CX/UN sem distinção)
    //           → fallback via descrição ("18X500 G", "12X1000ML", "27X200ML").
    let qtd_por_caixa_nfe = null;
    let qtd_por_caixa_confianca = 'nula';
    if (qCom > 0 && qTrib > 0 && uCom && uTrib && uCom !== uTrib) {
      const ratio = qTrib / qCom;
      const ratioInt = Math.round(ratio);
      const exato = Math.abs(ratio - ratioInt) < 0.001 && ratioInt >= 2;
      const UCOM_CAIXA = ['CX','CXS','CAIXA','PCT','PC','FD','FDO','FARDO','DP','DZ','DUZIA','BD','BD24','BD12'];
      const UTRIB_UN = ['UN','UND','UNID','UNIDADE','PC','PCT'];
      if (exato) {
        if (UCOM_CAIXA.includes(uCom) && UTRIB_UN.includes(uTrib)) {
          qtd_por_caixa_nfe = ratioInt;
          qtd_por_caixa_confianca = 'alta';
        } else {
          qtd_por_caixa_nfe = ratioInt;
          qtd_por_caixa_confianca = 'media';
        }
      }
    }
    // Fallback pela descrição quando XML não distingue (uCom == uTrib).
    // REGRA: só aplica se cEAN != cEANTrib. EANs iguais = venda unidade-a-unidade,
    // não em caixa (mesmo se descrição tiver "30X150G", que é nome do produto, não embalagem).
    // Admin sobrescreve via cadastro manual em /embalagens-fornecedor pra exceções.
    const eanDistinto = ean && eanTrib && ean !== eanTrib;
    if (qtd_por_caixa_nfe == null && eanDistinto) {
      const descParse = parseEmbalagem(prod.xProd || '');
      if (descParse.qtd && descParse.qtd >= 2) {
        qtd_por_caixa_nfe = descParse.qtd;
        qtd_por_caixa_confianca = descParse.confianca === 'alta' ? 'media' : 'baixa';
      }
    }

    // Cálculo de preço unitário e quantidade em unidades.
    // 3 casos:
    // A) uCom != uTrib (qTrib = total UN) → preço/UN = custo/qTrib, preço/CX = custo/qCom, qtd_un = qTrib
    // B) uCom == uTrib + qtd_por_caixa achada → assume qCom em CAIXAS, multiplica pra UN
    // C) uCom == uTrib sem qtd_por_caixa → assume tudo em UN (tradicional)
    let preco_unitario_nota, preco_unitario_caixa = null, qtd_em_unidades = null;
    if (qCom !== qTrib && qTrib > 0) {
      // Caso A
      preco_unitario_nota = parseFloat((custo_total / qTrib).toFixed(4));
      preco_unitario_caixa = qCom > 0 ? parseFloat((custo_total / qCom).toFixed(4)) : null;
      qtd_em_unidades = qTrib;
    } else if (qtd_por_caixa_nfe && qtd_por_caixa_nfe >= 2) {
      // Caso B: assume qCom em caixas, multiplica
      const totalUn = qCom * qtd_por_caixa_nfe;
      preco_unitario_nota = parseFloat((custo_total / totalUn).toFixed(4));
      preco_unitario_caixa = parseFloat((custo_total / qCom).toFixed(4));
      qtd_em_unidades = totalUn;
    } else {
      // Caso C
      preco_unitario_nota = parseFloat((custo_total / qCom).toFixed(4));
      qtd_em_unidades = qCom;
    }

    const cProdRaw = prod.cProd;
    const cprodFornecedor = cProdRaw ? String(cProdRaw).trim() : null;
    const cfop = String(prod.CFOP || '').trim() || null;
    return {
      numero_item: parseInt(det.$?.nItem || idx + 1) || idx + 1,
      cfop,
      ean_nota: ean,
      ean_trib: eanTrib,
      cprod_fornecedor: cprodFornecedor,
      descricao_nota: String(prod.xProd || '').trim(),
      quantidade: qCom,
      qtd_comercial: qCom,
      un_comercial: uCom || null,
      qtd_tributavel: qTrib || null,
      un_tributavel: uTrib || null,
      qtd_por_caixa_nfe,
      qtd_por_caixa_confianca,
      qtd_em_unidades,
      preco_unitario_nota, preco_total_nota, preco_unitario_caixa,
      vprod, vdesc_item, vfrete_item, vseg_item, voutro_item,
      vicms_bc, vicms, vst_bc, vst, vfcp_st, vipi_bc, vipi,
    };
  });

  return { header, itens };
}

// Versão simplificada (contas-receber.js): cabeçalho enxuto + itens básicos
async function parseNFeSimples(buf) {
  const { inf, ide, emit, dest, chave } = await parseRoot(buf);
  const tot = inf.total?.ICMSTot || {};

  const detRaw = inf.det;
  const dets = !detRaw ? [] : (Array.isArray(detRaw) ? detRaw : [detRaw]);

  const itens = dets.map(det => {
    const prod = det.prod || {};
    const eanRaw = prod.cEAN;
    const eanTribRaw = prod.cEANTrib;
    const ean = (!eanRaw || eanRaw === 'SEM GTIN') ? null : String(eanRaw).trim();
    const eanTrib = (!eanTribRaw || eanTribRaw === 'SEM GTIN') ? null : String(eanTribRaw).trim();
    return {
      codigo_barras: ean || eanTrib,
      descricao: String(prod.xProd || '').trim(),
      ncm: String(prod.NCM || '').trim(),
      quantidade: n(prod.qCom) || n(prod.qTrib) || 1,
      valor_unitario: n(prod.vUnCom),
      valor_total: n(prod.vProd),
    };
  });

  return {
    chave_nfe: chave,
    numero_nota: String(ide.nNF || ''),
    natureza_operacao: String(ide.natOp || ''),
    data_emissao: String(ide.dhEmi || ide.dEmi || '').substring(0, 10) || null,
    emit_cnpj: String(emit?.CNPJ || emit?.CPF || ''),
    emit_nome: String(emit?.xNome || ''),
    dest_cnpj: String(dest?.CNPJ || dest?.CPF || ''),
    dest_nome: String(dest?.xNome || ''),
    valor_produtos: n(tot.vProd),
    valor_total: n(tot.vNF),
    itens,
  };
}

module.exports = { parseNFe, parseNFeSimples, MAX_XML_BYTES };
