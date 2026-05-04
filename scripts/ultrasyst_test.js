require('dotenv').config();
console.log('USER:', JSON.stringify(process.env.ULTRASYST_USER));
console.log('PASS:', JSON.stringify(process.env.ULTRASYST_PASS));
console.log('PASS len:', (process.env.ULTRASYST_PASS||'').length);
console.log('PASS expected len:', 'u6G%J.+UD2?jPff[Pv"\'nrPz&-'.length);
