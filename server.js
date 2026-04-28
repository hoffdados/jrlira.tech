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
app.use('/api/notas', require('./src/routes/notas'));

// ── PÁGINAS ───────────────────────────────────────────────────────
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public/index.html')));
app.get('/rh', (req, res) => res.sendFile(path.join(__dirname, 'public/rh.html')));
app.get('/ponto', (req, res) => res.sendFile(path.join(__dirname, 'public/ponto.html')));
app.get('/importar-qlp', (req, res) => res.sendFile(path.join(__dirname, 'public/importar-qlp.html')));
app.get('/notas-cadastro', (req, res) => res.sendFile(path.join(__dirname, 'public/notas-cadastro.html')));
app.get('/notas-estoque', (req, res) => res.sendFile(path.join(__dirname, 'public/notas-estoque.html')));
app.get('/notas-auditoria', (req, res) => res.sendFile(path.join(__dirname, 'public/notas-auditoria.html')));
app.get('/usuarios', (req, res) => res.sendFile(path.join(__dirname, 'public/usuarios.html')));

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

    // ── NOTAS DE ENTRADA ──────────────────────────────────────────
    await client.query(`
      CREATE TABLE IF NOT EXISTS notas_entrada (
        id SERIAL PRIMARY KEY,
        chave_nfe VARCHAR(44) UNIQUE,
        numero_nota VARCHAR(20),
        serie VARCHAR(5),
        fornecedor_nome VARCHAR(300),
        fornecedor_cnpj VARCHAR(18),
        data_emissao DATE,
        valor_total DECIMAL(12,2),
        status VARCHAR(30) DEFAULT 'importada',
        conferencia_rodada INTEGER DEFAULT 0,
        importado_por VARCHAR(150),
        importado_em TIMESTAMPTZ DEFAULT NOW(),
        fechado_em TIMESTAMPTZ
      );

      CREATE TABLE IF NOT EXISTS itens_nota (
        id SERIAL PRIMARY KEY,
        nota_id INTEGER REFERENCES notas_entrada(id) ON DELETE CASCADE,
        numero_item INTEGER,
        ean_nota VARCHAR(20),
        ean_validado VARCHAR(20),
        descricao_nota VARCHAR(300),
        quantidade DECIMAL(12,4),
        preco_unitario_nota DECIMAL(12,4),
        preco_total_nota DECIMAL(12,2),
        custo_fabrica DECIMAL(12,4),
        status_preco VARCHAR(20) DEFAULT 'sem_cadastro',
        produto_novo BOOLEAN DEFAULT TRUE,
        validado_cadastro BOOLEAN DEFAULT FALSE,
        criado_em TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS conferencias_estoque (
        id SERIAL PRIMARY KEY,
        item_id INTEGER REFERENCES itens_nota(id) ON DELETE CASCADE,
        rodada INTEGER NOT NULL,
        qtd_contada DECIMAL(12,4),
        status VARCHAR(20),
        conferido_por VARCHAR(150),
        conferido_em TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (item_id, rodada)
      );

      CREATE TABLE IF NOT EXISTS conferencia_lotes (
        id SERIAL PRIMARY KEY,
        conferencia_id INTEGER REFERENCES conferencias_estoque(id) ON DELETE CASCADE,
        lote VARCHAR(100),
        validade DATE,
        quantidade DECIMAL(12,4)
      );

      CREATE TABLE IF NOT EXISTS auditoria_itens (
        id SERIAL PRIMARY KEY,
        item_id INTEGER REFERENCES itens_nota(id) ON DELETE CASCADE UNIQUE,
        qtd_contada DECIMAL(12,4),
        lote VARCHAR(100),
        validade DATE,
        status VARCHAR(20),
        observacao TEXT,
        auditado_por VARCHAR(150),
        auditado_em TIMESTAMPTZ DEFAULT NOW()
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
