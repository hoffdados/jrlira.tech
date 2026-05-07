const { Pool } = require('pg');
const { enviarEmail } = require('./src/mailer');
const { gerarPDF, templatePedidoEmail } = require('./src/routes/pedidos');

const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
const DISCLAIMER = 'Caso este pedido já tenha sido faturado, favor desconsiderar este email.';

(async () => {
  const { rows } = await pool.query(`
    SELECT p.*,
           f.razao_social, f.fantasia, f.cnpj as fornecedor_cnpj,
           v.nome as vendedor_nome, v.email as vendedor_email, v.telefone as vendedor_tel,
           l.nome as loja_nome, l.cnpj as loja_cnpj
      FROM pedidos p
      LEFT JOIN fornecedores f ON f.id = p.fornecedor_id
      LEFT JOIN vendedores v ON v.id = p.vendedor_id
      LEFT JOIN lojas l ON l.id = p.loja_id
     WHERE p.status = 'validado'
       AND p.validado_em >= CURRENT_DATE
       AND v.email IS NOT NULL AND v.email <> ''
     ORDER BY p.validado_em ASC
  `);

  console.log(`Pedidos a reenviar: ${rows.length}`);
  let ok = 0, fail = 0;
  for (const ped of rows) {
    try {
      const itens = (await pool.query('SELECT * FROM itens_pedido WHERE pedido_id=$1 ORDER BY id', [ped.id])).rows;
      const pdf = await gerarPDF(ped, itens);
      await enviarEmail(
        ped.vendedor_email,
        `Pedido ${ped.numero_pedido} validado — JR Lira (reenvio)`,
        templatePedidoEmail({ pedido: ped, disclaimer: DISCLAIMER }),
        [{ filename: `pedido-${ped.numero_pedido}.pdf`, content: pdf }]
      );
      console.log(`OK ${ped.numero_pedido} -> ${ped.vendedor_email}`);
      ok++;
    } catch (e) {
      console.error(`FAIL ${ped.numero_pedido}: ${e.message}`);
      fail++;
    }
  }
  console.log(`\nResumo: ${ok} enviados, ${fail} falhas`);
  await pool.end();
})().catch(e => { console.error(e); process.exit(1); });
