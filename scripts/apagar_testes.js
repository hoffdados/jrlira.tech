const { pool } = require('../src/db');

const PEDIDOS_APAGAR = [1, 14, 15, 16, 17, 18, 19, 20, 21];
const NOTAS_APAGAR = [2];

(async () => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // 1) Apagar itens dos pedidos
    const r1 = await client.query(
      `DELETE FROM itens_pedido WHERE pedido_id = ANY($1::int[])`,
      [PEDIDOS_APAGAR]
    );
    console.log(`itens_pedido apagados: ${r1.rowCount}`);

    // 2) Apagar pedidos
    const r2 = await client.query(
      `DELETE FROM pedidos WHERE id = ANY($1::int[])`,
      [PEDIDOS_APAGAR]
    );
    console.log(`pedidos apagados: ${r2.rowCount}`);

    // 3) Apagar dependentes pelos itens da nota (ordem: filhos antes do pai)
    const r3a = await client.query(
      `DELETE FROM conferencias_estoque
        WHERE item_id IN (SELECT id FROM itens_nota WHERE nota_id = ANY($1::int[]))`,
      [NOTAS_APAGAR]
    );
    console.log(`conferencias_estoque apagados: ${r3a.rowCount}`);

    const r3b = await client.query(
      `DELETE FROM auditoria_itens
        WHERE item_id IN (SELECT id FROM itens_nota WHERE nota_id = ANY($1::int[]))`,
      [NOTAS_APAGAR]
    );
    console.log(`auditoria_itens apagados: ${r3b.rowCount}`);

    const r3 = await client.query(
      `DELETE FROM itens_nota WHERE nota_id = ANY($1::int[])`,
      [NOTAS_APAGAR]
    );
    console.log(`itens_nota apagados: ${r3.rowCount}`);

    // 4) Apagar a nota
    const r4 = await client.query(
      `DELETE FROM notas_entrada WHERE id = ANY($1::int[])`,
      [NOTAS_APAGAR]
    );
    console.log(`notas_entrada apagadas: ${r4.rowCount}`);

    await client.query('COMMIT');
    console.log('OK — commit feito.');
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('ERRO (rollback):', e.message);
    process.exit(1);
  } finally {
    client.release();
    await pool.end();
  }
})();
