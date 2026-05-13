// Re-sincroniza itens de uma nota CD antes da loja iniciar conferência.
// Resolve o caso: CD edita pedido (remove item, muda qtd) após importação inicial.
// Detecta cancelamento (MCP_STATUS='C') e marca como cancelada antes de tudo.

const { pool } = require('./db');
const { clientePorCodigo } = require('./cds');

const CD_LEGADO = 'srv1-itautuba';
const CD_CNPJ_LEGADO = '17764296000209';

// Devolve: { ok, alterou, motivo, cancelada, statusUltra, itens_antes, itens_depois }
async function resyncItensNotaCd(notaId) {
  const client = await pool.connect();
  try {
    const { rows: [nota] } = await client.query(
      `SELECT id, cd_mov_codi, origem_cd_codigo, fornecedor_cnpj, status, loja_id, valor_total
         FROM notas_entrada WHERE id=$1`, [notaId]);
    if (!nota) return { ok: false, motivo: 'nota nao encontrada' };
    if (!nota.cd_mov_codi) return { ok: true, alterou: false, motivo: 'nota sem cd_mov_codi' };

    // Identifica o CD origem: usa origem_cd_codigo, ou fallback pelo CNPJ (ITB legado)
    let cdOrigem = nota.origem_cd_codigo;
    if (!cdOrigem && nota.fornecedor_cnpj === CD_CNPJ_LEGADO) cdOrigem = CD_LEGADO;
    if (!cdOrigem) return { ok: true, alterou: false, motivo: 'origem_cd_codigo nao definido' };

    let cli;
    try { cli = await clientePorCodigo(cdOrigem); }
    catch (e) { return { ok: false, motivo: 'sem relay: ' + e.message }; }

    // 1) Cabeça atual no UltraSyst
    const cab = await cli.query(
      `SELECT TOP 1 MCP_CODI, MCP_STATUS, MCP_VTOT
         FROM TBMOVCOMPRA WITH (NOLOCK)
        WHERE MCP_TIPOMOV='S' AND MCP_CODI='${nota.cd_mov_codi}'`);
    if (!cab.rows.length) {
      // Removida do UltraSyst → cancelar
      await client.query(
        `UPDATE notas_entrada SET status='cancelada', cancelada_em=NOW(),
                                  cancelada_motivo=$2 WHERE id=$1`,
        [notaId, 'Removida do UltraSyst (resync ao iniciar conferência)']);
      return { ok: true, cancelada: true, motivo: 'removida do UltraSyst' };
    }
    const c = cab.rows[0];
    if (c.MCP_STATUS === 'C') {
      await client.query(
        `UPDATE notas_entrada SET status='cancelada', cancelada_em=NOW(),
                                  cancelada_motivo=$2 WHERE id=$1`,
        [notaId, 'Cancelada no UltraSyst (resync ao iniciar conferência)']);
      return { ok: true, cancelada: true, motivo: 'MCP_STATUS=C' };
    }

    // 2) Itens atuais no UltraSyst
    const itensU = await cli.query(
      `SELECT i.MCP_SEQITEM, i.PRO_CODI,
              COALESCE(
                NULLIF(LTRIM(RTRIM(i.EAN_CODI)),''),
                (SELECT TOP 1 LTRIM(RTRIM(EAN_CODI)) FROM EAN WITH (NOLOCK)
                  WHERE MAT_CODI = i.PRO_CODI AND EAN_CODI IS NOT NULL AND LTRIM(RTRIM(EAN_CODI)) <> ''
                  ORDER BY CASE WHEN EAN_NOTA='S' THEN 0 ELSE 1 END, ID),
                NULLIF(LTRIM(RTRIM(mat.EAN_CODI)),'')
              ) AS ean,
              COALESCE(NULLIF(LTRIM(RTRIM(i.PRO_DESCP)),''), LTRIM(RTRIM(mat.MAT_DESC))) AS descricao,
              i.MCP_QUAN, i.MCP_VUNI
         FROM TBITEMCOMPRA i WITH (NOLOCK)
         LEFT JOIN MATERIAL mat WITH (NOLOCK) ON mat.MAT_CODI = i.PRO_CODI
        WHERE i.MCP_TIPOMOV='S' AND i.MCP_CODI='${nota.cd_mov_codi}'
        ORDER BY i.MCP_SEQITEM`);

    // 3) Itens atuais no PG (pra diff)
    const { rows: itensAntes } = await client.query(
      `SELECT cd_pro_codi, quantidade, preco_unitario_nota
         FROM itens_nota WHERE nota_id=$1 ORDER BY numero_item`, [notaId]);

    // Hash simples pra detectar mudança: junta cd_pro_codi+qtd+preco
    const hashItens = arr => arr.map(i => `${i.PRO_CODI || i.cd_pro_codi}|${Number(i.MCP_QUAN || i.quantidade).toFixed(3)}|${Number(i.MCP_VUNI || i.preco_unitario_nota).toFixed(4)}`).sort().join(';');
    const hashAntes = hashItens(itensAntes);
    const hashDepois = hashItens(itensU.rows);
    if (hashAntes === hashDepois) {
      return { ok: true, alterou: false, motivo: 'sem alteracao' };
    }

    // 4) Re-popular itens (transação): DELETE + INSERT + UPDATE valor_total
    await client.query('BEGIN');
    try {
      // Apaga lotes_conferidos atrelados (se houver) — caso re-sincronize depois de iniciar
      await client.query(
        `DELETE FROM lotes_conferidos WHERE item_id IN (SELECT id FROM itens_nota WHERE nota_id=$1)`,
        [notaId]);
      await client.query(`DELETE FROM itens_nota WHERE nota_id=$1`, [notaId]);

      if (itensU.rows.length) {
        const nota_id = itensU.rows.map(() => notaId);
        const numero  = itensU.rows.map(i => Math.floor(i.MCP_SEQITEM || 0));
        const proCodi = itensU.rows.map(i => (i.PRO_CODI || '').trim() || null);
        const ean     = itensU.rows.map(i => ((i.ean || '').trim() || null));
        const desc    = itensU.rows.map(i => ((i.descricao || '').trim() || null));
        const qtd     = itensU.rows.map(i => i.MCP_QUAN || 0);
        const vuni    = itensU.rows.map(i => i.MCP_VUNI || 0);
        const vtot    = itensU.rows.map(i => (i.MCP_QUAN || 0) * (i.MCP_VUNI || 0));
        const semCod  = itensU.rows.map(i => {
          const e = (i.ean || '').replace(/\D/g, '').replace(/^0+/, '');
          return !e;
        });

        await client.query(
          `INSERT INTO itens_nota
              (nota_id, numero_item, cd_pro_codi, ean_nota, descricao_nota,
               quantidade, preco_unitario_nota, preco_total_nota, produto_novo, sem_codigo_barras)
             SELECT * FROM UNNEST(
               $1::int[], $2::int[], $3::text[], $4::text[], $5::text[],
               $6::numeric[], $7::numeric[], $8::numeric[],
               ARRAY_FILL(FALSE, ARRAY[array_length($1,1)]), $9::bool[]
             )`,
          [nota_id, numero, proCodi, ean, desc, qtd, vuni, vtot, semCod]);

        // Cross-cadastro com produtos_externo (mesmo padrão do sync original)
        if (nota.loja_id) {
          await client.query(`
            UPDATE itens_nota i
               SET custo_fabrica = pe.custoorigem,
                   ean_validado = NULLIF(LTRIM(COALESCE(i.ean_nota,''),'0'),''),
                   ean_fonte = 'ean_nota',
                   produto_novo = FALSE,
                   status_preco = CASE
                     WHEN pe.custoorigem IS NULL OR i.preco_unitario_nota IS NULL OR i.preco_unitario_nota <= 0 THEN 'sem_cadastro'
                     WHEN ABS(i.preco_unitario_nota - pe.custoorigem) <= 0.01 THEN 'igual'
                     WHEN ABS(i.preco_unitario_nota - pe.custoorigem) > pe.custoorigem * 0.15 THEN 'auditagem'
                     WHEN i.preco_unitario_nota > pe.custoorigem THEN 'maior'
                     ELSE 'menor'
                   END
              FROM produtos_externo pe
             WHERE i.nota_id = $1
               AND pe.loja_id = $2
               AND NULLIF(LTRIM(pe.codigobarra,'0'),'') = NULLIF(LTRIM(COALESCE(i.ean_nota,''),'0'),'')
               AND NULLIF(LTRIM(COALESCE(i.ean_nota,''),'0'),'') IS NOT NULL`,
            [notaId, nota.loja_id]);
        }
      }

      // Atualiza valor total e timestamp do sync
      await client.query(
        `UPDATE notas_entrada SET valor_total=$2, cd_synced_em=NOW() WHERE id=$1`,
        [notaId, c.MCP_VTOT || 0]);
      await client.query('COMMIT');
    } catch (e) {
      try { await client.query('ROLLBACK'); } catch {}
      return { ok: false, motivo: 'erro ao reescrever: ' + e.message };
    }

    return {
      ok: true, alterou: true,
      itens_antes: itensAntes.length, itens_depois: itensU.rows.length,
      valor_antes: parseFloat(nota.valor_total), valor_depois: c.MCP_VTOT,
    };
  } finally { client.release(); }
}

module.exports = { resyncItensNotaCd };
