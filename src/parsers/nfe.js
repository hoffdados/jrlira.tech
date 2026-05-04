const xml2js = require('xml2js');

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
  const { inf, ide, emit, chave } = await parseRoot(buf);
  const tot = inf.total?.ICMSTot || {};

  const header = {
    chave_nfe: chave,
    numero_nota: String(ide.nNF || ''),
    serie: String(ide.serie || ''),
    fornecedor_nome: String(emit.xNome || emit.xFant || ''),
    fornecedor_cnpj: String(emit.CNPJ || emit.CPF || ''),
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
    // Preço unitário em UN (tributável) — comparável com custo do CD que está em UN.
    // Quando uCom=uTrib (bulk), qTrib==qCom e o resultado é o mesmo.
    // Quando uCom=CX e uTrib=UN, qTrib = total UN → divide certo.
    const qBase = qTrib > 0 ? qTrib : qCom;
    const preco_unitario_nota = parseFloat((custo_total / qBase).toFixed(4));
    const preco_unitario_caixa = (qCom > 0 && qCom !== qBase)
      ? parseFloat((custo_total / qCom).toFixed(4))
      : null;

    const eanRaw = prod.cEAN;
    const ean = (!eanRaw || eanRaw === 'SEM GTIN') ? null : String(eanRaw).trim();
    const eanTribRaw = prod.cEANTrib;
    const eanTrib = (!eanTribRaw || eanTribRaw === 'SEM GTIN') ? null : String(eanTribRaw).trim();

    // Qtd por caixa derivada do XML.
    // qCom = unidade comercial (geralmente CX/PCT/FD), qTrib = unidade tributável (geralmente UN).
    // Se qCom > 0 e qTrib é múltiplo inteiro, ratio = qtd por caixa pra esse fornecedor.
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

    return {
      numero_item: parseInt(det.$?.nItem || idx + 1) || idx + 1,
      ean_nota: ean,
      ean_trib: eanTrib,
      descricao_nota: String(prod.xProd || '').trim(),
      quantidade: qCom,
      qtd_comercial: qCom,
      un_comercial: uCom || null,
      qtd_tributavel: qTrib || null,
      un_tributavel: uTrib || null,
      qtd_por_caixa_nfe,
      qtd_por_caixa_confianca,
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
