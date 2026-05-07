require('dotenv').config();
const { Pool } = require('pg');
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

const LOJAS = { 1:'Econômico', 2:'BR', 3:'João Pessoa', 4:'Floresta', 5:'São José', 6:'Santarém' };

async function main() {
  const r = await pool.query(`
    SELECT v.loja_id,
      (SELECT COUNT(*) FROM devolucoes_compra_historico WHERE loja_id=v.loja_id) AS dev,
      (SELECT MAX(data_devolucao) FROM devolucoes_compra_historico WHERE loja_id=v.loja_id) AS dev_ult,
      (SELECT COUNT(DISTINCT tipo_saida) FROM vendas_historico WHERE loja_id=v.loja_id) AS tipos_saida,
      (SELECT COUNT(*) FILTER (WHERE COALESCE(tipo_saida,'venda')<>'venda') FROM vendas_historico WHERE loja_id=v.loja_id) AS saidas_neq,
      (SELECT COUNT(DISTINCT tipo_entrada) FROM compras_historico WHERE loja_id=v.loja_id) AS tipos_entr,
      (SELECT COUNT(*) FILTER (WHERE COALESCE(tipo_entrada,'compra')<>'compra') FROM compras_historico WHERE loja_id=v.loja_id) AS entradas_neq,
      (SELECT MAX(data_venda) FROM vendas_historico WHERE loja_id=v.loja_id) AS venda_ult,
      (SELECT MAX(data_entrada) FROM compras_historico WHERE loja_id=v.loja_id) AS compra_ult,
      (SELECT MAX(sincronizado_em) FROM compras_historico WHERE loja_id=v.loja_id) AS sync_ult
    FROM (SELECT generate_series(1,6) AS loja_id) v
  `);
  console.log('LOJA | nome         | dev | últ-dev    | tipos-saída | saídas≠venda | tipos-entrada | entr≠compra | últ-venda  | últ-compra | últ-sync');
  console.log('-----+--------------+-----+------------+-------------+--------------+---------------+-------------+------------+------------+--------------------');
  for (const row of r.rows) {
    const nome = LOJAS[row.loja_id].padEnd(12);
    const fmt = (d) => d ? new Date(d).toLocaleString('pt-BR',{dateStyle:'short'}).padEnd(10) : '—'.padEnd(10);
    const fmtS = (d) => d ? new Date(d).toLocaleString('pt-BR') : '—';
    console.log(`  ${row.loja_id}  | ${nome} | ${String(row.dev).padStart(3)} | ${fmt(row.dev_ult)} |       ${row.tipos_saida}     |   ${String(row.saidas_neq).padStart(8)}   |       ${row.tipos_entr}       |  ${String(row.entradas_neq).padStart(8)}   | ${fmt(row.venda_ult)} | ${fmt(row.compra_ult)} | ${fmtS(row.sync_ult)}`);
  }
  console.log();

  console.log('=== Distribuição tipo_entrada por loja ===');
  const r2 = await pool.query(`SELECT loja_id, tipo_entrada, COUNT(*) qt FROM compras_historico GROUP BY 1,2 ORDER BY 1,2`);
  console.table(r2.rows);
  await pool.end();
}
main().catch(e=>{console.error(e);process.exit(1);});
