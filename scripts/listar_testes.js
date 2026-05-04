const { pool } = require('../src/db');
(async () => {
  try {
    // 1) Pedidos com vendedor + fornecedor
    const pedidos = await pool.query(`
      SELECT p.id, p.numero_pedido, p.status, p.valor_total, p.criado_em, p.validado_em,
             p.loja_id, p.nota_id,
             v.nome AS vendedor_nome, v.cpf AS vendedor_cpf,
             f.razao_social AS fornecedor_nome, f.cnpj AS fornecedor_cnpj
        FROM pedidos p
        LEFT JOIN vendedores v ON v.id = p.vendedor_id
        LEFT JOIN fornecedores f ON f.id = v.fornecedor_id
       ORDER BY p.criado_em DESC
    `);
    console.log('===== PEDIDOS =====');
    console.log(`Total: ${pedidos.rows.length}\n`);
    for (const p of pedidos.rows) {
      const isRodrigo = (p.vendedor_nome || '').toUpperCase().includes('RODRIGO HOFF');
      const isAtacadao = (p.fornecedor_nome || '').toUpperCase().includes('ATACAD') &&
                         (p.fornecedor_nome || '').toUpperCase().includes('ASA BRANCA');
      const tag = (isRodrigo && isAtacadao) ? '[MANTER]' : '[CANDIDATO APAGAR]';
      console.log(`${tag} #${p.id} ${p.numero_pedido || '(s/num)'} | ${p.status} | R$ ${p.valor_total || 0}`);
      console.log(`  Vend: ${p.vendedor_nome || '?'} (${p.vendedor_cpf || '?'})`);
      console.log(`  Forn: ${p.fornecedor_nome || '?'}`);
      console.log(`  Loja: ${p.loja_id || '?'} | nota_id: ${p.nota_id || '—'} | criado: ${p.criado_em ? p.criado_em.toISOString().slice(0,10) : '?'}`);
      console.log('');
    }

    // 2) Notas de entrada NÃO-CD (NF-e XML — testes vieram daí)
    const notas = await pool.query(`
      SELECT id, numero_nota, serie, fornecedor_nome, fornecedor_cnpj,
             status, valor_total, importado_por, importado_em, loja_id, pedido_id, origem
        FROM notas_entrada
       WHERE origem <> 'cd' OR origem IS NULL
       ORDER BY importado_em DESC
    `);
    console.log('\n===== NOTAS DE ENTRADA (não-CD) =====');
    console.log(`Total: ${notas.rows.length}\n`);
    for (const n of notas.rows) {
      console.log(`#${n.id} NF ${n.numero_nota}/${n.serie || '-'} | ${n.status} | R$ ${n.valor_total || 0}`);
      console.log(`  Forn: ${n.fornecedor_nome} (${n.fornecedor_cnpj})`);
      console.log(`  Loja: ${n.loja_id} | pedido_id: ${n.pedido_id || '—'} | origem: ${n.origem || '?'}`);
      console.log(`  Importado: ${n.importado_por} em ${n.importado_em ? n.importado_em.toISOString().slice(0,10) : '?'}`);
      console.log('');
    }
  } catch (e) {
    console.error('ERRO:', e.message);
    process.exit(1);
  } finally {
    await pool.end();
  }
})();
