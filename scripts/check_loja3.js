const { pool } = require('../src/db');
(async () => {
  try {
    // vendas
    const v = await pool.query(`
      SELECT loja_id, COUNT(*)::int qtd,
             MAX(data_venda) ultimo_venda,
             MAX(sincronizado_em) ultimo_sync
        FROM vendas_historico WHERE loja_id = 3 GROUP BY loja_id
    `);
    console.log('vendas_historico L3:', v.rows[0] || 'VAZIO');

    // compras
    const c = await pool.query(`
      SELECT loja_id, COUNT(*)::int qtd,
             MAX(data_entrada) ultimo_entrada,
             MAX(sincronizado_em) ultimo_sync
        FROM compras_historico WHERE loja_id = 3 GROUP BY loja_id
    `);
    console.log('compras_historico L3:', c.rows[0] || 'VAZIO');

    // produtos_externo
    const p = await pool.query(`
      SELECT loja_id, COUNT(*)::int qtd, MAX(sincronizado_em) ultimo_sync
        FROM produtos_externo WHERE loja_id = 3 GROUP BY loja_id
    `);
    console.log('produtos_externo L3:', p.rows[0] || 'VAZIO');

    // fornecedores (se tem loja_id)
    try {
      const f = await pool.query(`
        SELECT loja_id, COUNT(*)::int qtd
          FROM fornecedores WHERE loja_id = 3 GROUP BY loja_id
      `);
      console.log('fornecedores L3:', f.rows[0] || 'VAZIO');
    } catch (e) {
      console.log('fornecedores: sem loja_id ou tabela diferente');
    }

    // precos_promocionais
    try {
      const pp = await pool.query(`
        SELECT loja_id, COUNT(*)::int qtd
          FROM precos_promocionais WHERE loja_id = 3 GROUP BY loja_id
      `);
      console.log('precos_promocionais L3:', pp.rows[0] || 'VAZIO');
    } catch (e) {
      console.log('precos_promocionais: erro', e.message);
    }
  } catch (e) { console.error(e.message); } finally { await pool.end(); }
})();
