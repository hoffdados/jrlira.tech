const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

module.exports = {
  query: async (text, params) => {
    const result = await pool.query(text, params);
    return result.rows;
  },
  pool
};
