// Conecta no UltraSyst (SQL Server) e lista tabelas que parecem de pedido/transferência/CD
require('dotenv').config();
const sql = require('mssql');

const config = {
  server: process.env.ULTRASYST_HOST || '147.93.177.172',
  port: parseInt(process.env.ULTRASYST_PORT || '1433'),
  database: process.env.ULTRASYST_DB || 'ITAUTUBA',
  user: process.env.ULTRASYST_USER || 'ASAB',
  password: process.env.ULTRASYST_PASS || 'u6G%J.+UD2?jPff[Pv"\'nrPz&-',
  options: { encrypt: false, trustServerCertificate: true, enableArithAbort: true },
  connectionTimeout: 15000,
  requestTimeout: 30000,
};

(async () => {
  try {
    console.log('[UltraSyst] Conectando…');
    await sql.connect(config);
    console.log('[UltraSyst] OK');

    // 1) Lista todas as tabelas
    const tabs = await sql.query`
      SELECT TABLE_SCHEMA, TABLE_NAME
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_TYPE='BASE TABLE'
      ORDER BY TABLE_NAME`;
    console.log(`\n[Total tabelas] ${tabs.recordset.length}`);

    // 2) Filtra candidatas por palavra-chave
    const kws = ['ped', 'transf', 'transit', 'distrib', 'cd', 'remess', 'expedi', 'sai', 'item', 'produto', 'loja', 'estab', 'empres'];
    const candidatas = tabs.recordset.filter(t =>
      kws.some(k => t.TABLE_NAME.toLowerCase().includes(k))
    );
    console.log(`\n[Candidatas]`);
    candidatas.forEach(t => console.log(`  ${t.TABLE_SCHEMA}.${t.TABLE_NAME}`));

    await sql.close();
  } catch (e) {
    console.error('[ERRO]', e.message);
    process.exit(1);
  }
})();
