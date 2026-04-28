require('dotenv').config();
const express = require('express');
const cors = require('cors');
const path = require('path');
const { pool } = require('./src/db');

const app = express();
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));
app.use(express.static(path.join(__dirname, 'public')));

// ── ROTAS ─────────────────────────────────────────────────────────
app.use('/api/auth', require('./src/routes/auth'));
app.use('/api/funcionarios', require('./src/routes/funcionarios'));
app.use('/api/ponto', require('./src/routes/ponto'));
app.use('/api/importacao', require('./src/routes/importacao'));

// ── PÁGINAS ───────────────────────────────────────────────────────
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public/index.html')));
app.get('/rh', (req, res) => res.sendFile(path.join(__dirname, 'public/rh.html')));
app.get('/ponto', (req, res) => res.sendFile(path.join(__dirname, 'public/ponto.html')));
app.get('/importar-qlp', (req, res) => res.sendFile(path.join(__dirname, 'public/importar-qlp.html')));

// ── INIT DB ───────────────────────────────────────────────────────
async function initDB() {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS rh_usuarios (
        id SERIAL PRIMARY KEY,
        usuario VARCHAR(50) UNIQUE NOT NULL,
        senha_hash VARCHAR(200) NOT NULL,
        nome VARCHAR(150) NOT NULL,
        perfil VARCHAR(20) DEFAULT 'usuario',
        ativo BOOLEAN DEFAULT TRUE,
        criado_em TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS funcionarios (
        id SERIAL PRIMARY KEY,
        matricula VARCHAR(30) UNIQUE NOT NULL,
        nome VARCHAR(200) NOT NULL,
        cpf VARCHAR(14),
        pis VARCHAR(20),
        data_nascimento DATE,
        sexo VARCHAR(20),
        escolaridade VARCHAR(100),
        raca VARCHAR(50),
        estado_civil VARCHAR(50),
        cargo VARCHAR(100),
        grupo_cargo VARCHAR(50),
        nivel VARCHAR(50),
        loja_id INTEGER,
        salario DECIMAL(10,2),
        status VARCHAR(30) DEFAULT 'ATIVO',
        data_admissao DATE,
        data_demissao DATE,
        causa_afastamento VARCHAR(100),
        motivo_afastamento VARCHAR(200),
        foto_path VARCHAR(300),
        cep VARCHAR(10),
        logradouro VARCHAR(200),
        numero VARCHAR(20),
        complemento VARCHAR(100),
        bairro VARCHAR(100),
        cidade VARCHAR(100),
        uf VARCHAR(10),
        telefone VARCHAR(20),
        email VARCHAR(150),
        criado_em TIMESTAMPTZ DEFAULT NOW(),
        atualizado_em TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS funcionario_eventos (
        id SERIAL PRIMARY KEY,
        funcionario_id INTEGER REFERENCES funcionarios(id) ON DELETE CASCADE,
        tipo VARCHAR(30) NOT NULL,
        data_inicio DATE NOT NULL,
        data_fim DATE,
        descricao TEXT,
        documento_path VARCHAR(300),
        criado_por VARCHAR(150),
        criado_em TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS ponto_importacoes (
        id SERIAL PRIMARY KEY,
        nome_arquivo VARCHAR(200),
        prefixo_loja VARCHAR(10),
        periodo_inicio DATE,
        periodo_fim DATE,
        total_funcionarios INTEGER DEFAULT 0,
        total_registros INTEGER DEFAULT 0,
        importado_por VARCHAR(150),
        importado_em TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS ponto_registros (
        id SERIAL PRIMARY KEY,
        importacao_id INTEGER REFERENCES ponto_importacoes(id) ON DELETE CASCADE,
        matricula VARCHAR(30) NOT NULL,
        funcionario_id INTEGER,
        data DATE NOT NULL,
        tab VARCHAR(10),
        ent1 TIME, sai1 TIME,
        ent2 TIME, sai2 TIME,
        ent3 TIME, sai3 TIME,
        ent4 TIME, sai4 TIME,
        ent5 TIME, sai5 TIME,
        h_trab INTERVAL,
        h_extra INTERVAL,
        is_dsr BOOLEAN DEFAULT FALSE,
        is_folga BOOLEAN DEFAULT FALSE,
        sem_marcacao BOOLEAN DEFAULT FALSE,
        criado_em TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (matricula, data, importacao_id)
      );
    `);

    // Migrations
    await client.query(`
      ALTER TABLE funcionarios ALTER COLUMN sexo TYPE VARCHAR(20);
      ALTER TABLE funcionarios ALTER COLUMN uf TYPE VARCHAR(10);
    `).catch(() => {});

    // Admin padrão
    const { rows } = await client.query("SELECT id FROM rh_usuarios WHERE usuario = 'admin'");
    if (!rows.length) {
      const bcrypt = require('bcryptjs');
      const hash = await bcrypt.hash('admin123', 10);
      await client.query(
        "INSERT INTO rh_usuarios (usuario, senha_hash, nome, perfil) VALUES ('admin', $1, 'Administrador', 'admin')",
        [hash]
      );
      console.log('[DB] Usuário admin criado (senha: admin123)');
    }

    console.log('[DB] Tabelas inicializadas');
  } finally {
    client.release();
  }
}

const PORT = process.env.PORT || 3001;
initDB().then(() => {
  app.listen(PORT, () => console.log(`jrlira.tech rodando na porta ${PORT}`));
}).catch(err => {
  console.error('[DB] Erro init:', err.message);
  process.exit(1);
});
