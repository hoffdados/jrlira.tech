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
app.use('/api/foto-upload', require('./src/routes/fotoUpload'));
app.use('/api/fornecedores', require('./src/routes/fornecedores'));
app.use('/api/vendedores', require('./src/routes/vendedores'));
app.use('/api/pedidos', require('./src/routes/pedidos'));
app.use('/api/cr', require('./src/routes/contas-receber'));

// ── PÁGINAS ───────────────────────────────────────────────────────
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public/index.html')));
app.get('/rh', (req, res) => res.sendFile(path.join(__dirname, 'public/rh.html')));
app.get('/ponto', (req, res) => res.sendFile(path.join(__dirname, 'public/ponto.html')));
app.get('/importar-qlp', (req, res) => res.sendFile(path.join(__dirname, 'public/importar-qlp.html')));
app.get('/notas-cadastro', (req, res) => res.sendFile(path.join(__dirname, 'public/notas-cadastro.html')));
app.get('/notas-estoque', (req, res) => res.sendFile(path.join(__dirname, 'public/notas-estoque.html')));
app.get('/notas-auditoria', (req, res) => res.sendFile(path.join(__dirname, 'public/notas-auditoria.html')));
app.get('/usuarios', (req, res) => res.sendFile(path.join(__dirname, 'public/usuarios.html')));
app.get('/foto-upload', (req, res) => res.sendFile(path.join(__dirname, 'public/foto-upload.html')));
app.get('/fornecedores', (req, res) => res.sendFile(path.join(__dirname, 'public/fornecedores.html')));
app.get('/pedidos-comprador', (req, res) => res.sendFile(path.join(__dirname, 'public/pedidos-comprador.html')));
app.get('/vendedor-cadastro', (req, res) => res.sendFile(path.join(__dirname, 'public/vendedor-cadastro.html')));
app.get('/vendedor', (req, res) => res.sendFile(path.join(__dirname, 'public/vendedor.html')));
app.get('/notas-comprador', (req, res) => res.sendFile(path.join(__dirname, 'public/notas-comprador.html')));
app.get('/sugestao-compras', (req, res) => res.sendFile(path.join(__dirname, 'public/sugestao-compras.html')));
app.get('/contas-receber', (req, res) => res.sendFile(path.join(__dirname, 'public/contas-receber.html')));
app.get('/preview-icons', (req, res) => res.sendFile(path.join(__dirname, 'public/preview-icons.html')));

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
        quantidade DECIMAL(12,4),
        local_destino VARCHAR(50) DEFAULT 'Estoque'
      );
      ALTER TABLE conferencia_lotes ADD COLUMN IF NOT EXISTS local_destino VARCHAR(50) DEFAULT 'Estoque';

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
    await client.query(`ALTER TABLE rh_usuarios ADD COLUMN IF NOT EXISTS loja_id INTEGER`).catch(() => {});
    await client.query(`ALTER TABLE notas_entrada ADD COLUMN IF NOT EXISTS loja_id INTEGER`).catch(() => {});
    await client.query(`ALTER TABLE itens_nota ADD COLUMN IF NOT EXISTS ean_trib VARCHAR(20)`).catch(() => {});
    await client.query(`ALTER TABLE rh_usuarios ADD COLUMN IF NOT EXISTS lojas_ids INTEGER[]`).catch(() => {});
    await client.query(`ALTER TABLE rh_usuarios ADD COLUMN IF NOT EXISTS email VARCHAR(200)`).catch(() => {});
    await client.query(`ALTER TABLE ponto_importacoes ADD COLUMN IF NOT EXISTS loja_id INTEGER`).catch(() => {});
    await client.query(`ALTER TABLE funcionarios ADD COLUMN IF NOT EXISTS foto_data BYTEA`).catch(() => {});
    await client.query(`ALTER TABLE funcionarios ADD COLUMN IF NOT EXISTS foto_mime VARCHAR(20)`).catch(() => {});
    await client.query(`ALTER TABLE lojas ADD COLUMN IF NOT EXISTS cnpj VARCHAR(18)`).catch(() => {});
    await client.query(`ALTER TABLE lojas ADD COLUMN IF NOT EXISTS ativo BOOLEAN DEFAULT TRUE`).catch(() => {});

    // ── PEDIDOS / FORNECEDORES ────────────────────────────────────
    await client.query(`
      CREATE TABLE IF NOT EXISTS configuracoes (
        chave VARCHAR(100) PRIMARY KEY,
        valor TEXT
      );
      CREATE TABLE IF NOT EXISTS lojas (
        id SERIAL PRIMARY KEY,
        nome VARCHAR(100) NOT NULL,
        cnpj VARCHAR(18),
        ativo BOOLEAN DEFAULT TRUE
      );
      CREATE TABLE IF NOT EXISTS fornecedores (
        id SERIAL PRIMARY KEY,
        razao_social VARCHAR(300) NOT NULL,
        fantasia VARCHAR(200),
        cnpj VARCHAR(18),
        ativo BOOLEAN DEFAULT TRUE,
        foto_data BYTEA,
        foto_mime VARCHAR(20),
        criado_em TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS vendedores (
        id SERIAL PRIMARY KEY,
        fornecedor_id INTEGER REFERENCES fornecedores(id) ON DELETE CASCADE,
        nome VARCHAR(200) NOT NULL,
        cpf VARCHAR(14),
        email VARCHAR(150) UNIQUE,
        telefone VARCHAR(20),
        nome_gerente VARCHAR(200),
        telefone_gerente VARCHAR(20),
        foto_data BYTEA,
        foto_mime VARCHAR(20),
        status VARCHAR(20) DEFAULT 'pendente',
        senha_hash VARCHAR(200),
        token_cadastro VARCHAR(64) UNIQUE,
        acesso_expira_em TIMESTAMPTZ,
        criado_em TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS pedidos (
        id SERIAL PRIMARY KEY,
        numero_pedido VARCHAR(30) UNIQUE,
        fornecedor_id INTEGER REFERENCES fornecedores(id),
        vendedor_id INTEGER REFERENCES vendedores(id),
        loja_id INTEGER REFERENCES lojas(id),
        status VARCHAR(30) DEFAULT 'rascunho',
        condicao_pagamento INTEGER,
        valor_total DECIMAL(12,2) DEFAULT 0,
        observacoes TEXT,
        nota_id INTEGER REFERENCES notas_entrada(id),
        criado_em TIMESTAMPTZ DEFAULT NOW(),
        enviado_em TIMESTAMPTZ,
        validado_em TIMESTAMPTZ,
        validado_por VARCHAR(150)
      );
      CREATE TABLE IF NOT EXISTS itens_pedido (
        id SERIAL PRIMARY KEY,
        pedido_id INTEGER REFERENCES pedidos(id) ON DELETE CASCADE,
        codigo_barras VARCHAR(20),
        descricao VARCHAR(300) NOT NULL,
        quantidade DECIMAL(12,4) NOT NULL,
        preco_unitario DECIMAL(12,4) NOT NULL,
        valor_total DECIMAL(12,2) NOT NULL,
        produto_novo BOOLEAN DEFAULT FALSE,
        qtd_validada DECIMAL(12,4),
        preco_validado DECIMAL(12,4)
      );
    `).catch(() => {});

    await client.query(`ALTER TABLE notas_entrada ADD COLUMN IF NOT EXISTS pedido_id INTEGER REFERENCES pedidos(id)`).catch(() => {});
    // totais XML por nota
    await client.query(`ALTER TABLE notas_entrada ADD COLUMN IF NOT EXISTS tot_vprod   DECIMAL(14,2)`).catch(() => {});
    await client.query(`ALTER TABLE notas_entrada ADD COLUMN IF NOT EXISTS tot_vbc     DECIMAL(14,2)`).catch(() => {});
    await client.query(`ALTER TABLE notas_entrada ADD COLUMN IF NOT EXISTS tot_vicms   DECIMAL(14,2)`).catch(() => {});
    await client.query(`ALTER TABLE notas_entrada ADD COLUMN IF NOT EXISTS tot_vbcst   DECIMAL(14,2)`).catch(() => {});
    await client.query(`ALTER TABLE notas_entrada ADD COLUMN IF NOT EXISTS tot_vst     DECIMAL(14,2)`).catch(() => {});
    await client.query(`ALTER TABLE notas_entrada ADD COLUMN IF NOT EXISTS tot_vfcp_st DECIMAL(14,2)`).catch(() => {});
    await client.query(`ALTER TABLE notas_entrada ADD COLUMN IF NOT EXISTS tot_vipi    DECIMAL(14,2)`).catch(() => {});
    await client.query(`ALTER TABLE notas_entrada ADD COLUMN IF NOT EXISTS tot_vdesc   DECIMAL(14,2)`).catch(() => {});
    await client.query(`ALTER TABLE notas_entrada ADD COLUMN IF NOT EXISTS tot_vfrete  DECIMAL(14,2)`).catch(() => {});
    await client.query(`ALTER TABLE notas_entrada ADD COLUMN IF NOT EXISTS tot_vseg    DECIMAL(14,2)`).catch(() => {});
    await client.query(`ALTER TABLE notas_entrada ADD COLUMN IF NOT EXISTS tot_voutro  DECIMAL(14,2)`).catch(() => {});
    // breakdown fiscal por item
    await client.query(`ALTER TABLE itens_nota ADD COLUMN IF NOT EXISTS ean_fonte    VARCHAR(10)`).catch(() => {});
    await client.query(`ALTER TABLE itens_nota ADD COLUMN IF NOT EXISTS vprod        DECIMAL(14,4)`).catch(() => {});
    await client.query(`ALTER TABLE itens_nota ADD COLUMN IF NOT EXISTS vdesc_item   DECIMAL(14,4)`).catch(() => {});
    await client.query(`ALTER TABLE itens_nota ADD COLUMN IF NOT EXISTS vfrete_item  DECIMAL(14,4)`).catch(() => {});
    await client.query(`ALTER TABLE itens_nota ADD COLUMN IF NOT EXISTS vseg_item    DECIMAL(14,4)`).catch(() => {});
    await client.query(`ALTER TABLE itens_nota ADD COLUMN IF NOT EXISTS voutro_item  DECIMAL(14,4)`).catch(() => {});
    await client.query(`ALTER TABLE itens_nota ADD COLUMN IF NOT EXISTS vicms_bc     DECIMAL(14,4)`).catch(() => {});
    await client.query(`ALTER TABLE itens_nota ADD COLUMN IF NOT EXISTS vicms        DECIMAL(14,4)`).catch(() => {});
    await client.query(`ALTER TABLE itens_nota ADD COLUMN IF NOT EXISTS vst_bc       DECIMAL(14,4)`).catch(() => {});
    await client.query(`ALTER TABLE itens_nota ADD COLUMN IF NOT EXISTS vst          DECIMAL(14,4)`).catch(() => {});
    await client.query(`ALTER TABLE itens_nota ADD COLUMN IF NOT EXISTS vfcp_st      DECIMAL(14,4)`).catch(() => {});
    await client.query(`ALTER TABLE itens_nota ADD COLUMN IF NOT EXISTS vipi_bc      DECIMAL(14,4)`).catch(() => {});
    await client.query(`ALTER TABLE itens_nota ADD COLUMN IF NOT EXISTS vipi         DECIMAL(14,4)`).catch(() => {});
    // comparativo nota vs pedido
    await client.query(`ALTER TABLE itens_nota ADD COLUMN IF NOT EXISTS fora_pedido    BOOLEAN DEFAULT FALSE`).catch(() => {});
    await client.query(`ALTER TABLE itens_nota ADD COLUMN IF NOT EXISTS preco_pedido   DECIMAL(14,4)`).catch(() => {});
    await client.query(`ALTER TABLE itens_nota ADD COLUMN IF NOT EXISTS qtd_pedido     DECIMAL(12,4)`).catch(() => {});
    await client.query(`ALTER TABLE itens_nota ADD COLUMN IF NOT EXISTS item_pedido_id INTEGER`).catch(() => {});
    await client.query(`ALTER TABLE notas_entrada ADD COLUMN IF NOT EXISTS emergencial BOOLEAN DEFAULT FALSE`).catch(() => {});
    await client.query(`INSERT INTO configuracoes (chave, valor) VALUES ('validade_acesso_vendedor_dias','90') ON CONFLICT DO NOTHING`).catch(() => {});
    // análise de compra
    await client.query(`
      CREATE TABLE IF NOT EXISTS vendas_historico (
        id SERIAL PRIMARY KEY,
        loja_id INTEGER NOT NULL,
        codigobarra VARCHAR(30) NOT NULL,
        data_venda DATE NOT NULL,
        qtd_vendida NUMERIC(12,3) NOT NULL DEFAULT 0,
        sincronizado_em TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(loja_id, codigobarra, data_venda)
      )
    `).catch(() => {});
    await client.query(`CREATE INDEX IF NOT EXISTS idx_vendas_hist ON vendas_historico(loja_id, codigobarra, data_venda)`).catch(() => {});
    await client.query(`ALTER TABLE fornecedores ADD COLUMN IF NOT EXISTS leadtime_dias INTEGER DEFAULT 7`).catch(() => {});
    await client.query(`
      CREATE TABLE IF NOT EXISTS compras_historico (
        id SERIAL PRIMARY KEY,
        loja_id INTEGER NOT NULL,
        numeronfe VARCHAR(20) NOT NULL,
        codigobarra VARCHAR(30) NOT NULL,
        data_emissao DATE,
        data_entrada DATE NOT NULL,
        qtd_comprada NUMERIC(12,3) NOT NULL DEFAULT 0,
        custo_total NUMERIC(14,4),
        fornecedor_cnpj VARCHAR(20),
        sincronizado_em TIMESTAMPTZ DEFAULT NOW()
      )
    `).catch(() => {});
    await client.query(`CREATE INDEX IF NOT EXISTS idx_compras_hist ON compras_historico(loja_id, codigobarra, data_entrada)`).catch(() => {});

    await client.query(`
      CREATE TABLE IF NOT EXISTS cr_debitos (
        id SERIAL PRIMARY KEY,
        fornecedor_id INTEGER REFERENCES fornecedores(id) ON DELETE SET NULL,
        fornecedor_cnpj TEXT,
        fornecedor_nome TEXT,
        loja_id INTEGER NOT NULL,
        numero_nota TEXT NOT NULL,
        chave_nfe TEXT UNIQUE,
        data_emissao DATE,
        natureza_operacao TEXT,
        valor_produtos NUMERIC(14,2) DEFAULT 0,
        valor_total NUMERIC(14,2) NOT NULL DEFAULT 0,
        valor_creditos NUMERIC(14,2) NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'aberto',
        observacoes TEXT,
        importado_em TIMESTAMPTZ DEFAULT NOW(),
        importado_por TEXT
      )
    `).catch(() => {});
    await client.query(`
      CREATE TABLE IF NOT EXISTS cr_debito_itens (
        id SERIAL PRIMARY KEY,
        debito_id INTEGER NOT NULL REFERENCES cr_debitos(id) ON DELETE CASCADE,
        codigo_barras TEXT,
        descricao TEXT,
        ncm TEXT,
        quantidade NUMERIC(14,4),
        valor_unitario NUMERIC(14,4),
        valor_total NUMERIC(14,2)
      )
    `).catch(() => {});
    await client.query(`
      CREATE TABLE IF NOT EXISTS cr_creditos (
        id SERIAL PRIMARY KEY,
        debito_id INTEGER REFERENCES cr_debitos(id) ON DELETE SET NULL,
        fornecedor_id INTEGER REFERENCES fornecedores(id) ON DELETE SET NULL,
        fornecedor_cnpj TEXT,
        loja_id INTEGER NOT NULL,
        tipo TEXT NOT NULL,
        numero_nota TEXT,
        chave_nfe TEXT UNIQUE,
        data_credito DATE,
        valor NUMERIC(14,2) NOT NULL,
        nr_nf_boleto TEXT,
        valor_nf_boleto NUMERIC(14,2),
        valor_boleto NUMERIC(14,2),
        valor_desconto NUMERIC(14,2),
        observacoes TEXT,
        registrado_em TIMESTAMPTZ DEFAULT NOW(),
        registrado_por TEXT
      )
    `).catch(() => {});

    // Lojas iniciais (se tabela vazia)
    const { rows: lojaRows } = await client.query('SELECT COUNT(*) FROM lojas');
    if (parseInt(lojaRows[0].count) === 0) {
      await client.query(`INSERT INTO lojas (id, nome) VALUES (1,'ECONOMICO'),(2,'BR'),(3,'JOAO PAULO'),(4,'FLORESTA'),(5,'SAO JOSE'),(6,'SANTAREM') ON CONFLICT DO NOTHING`);
    }

    await client.query(`
      CREATE TABLE IF NOT EXISTS foto_tokens (
        id SERIAL PRIMARY KEY,
        token VARCHAR(64) UNIQUE NOT NULL,
        funcionario_id INTEGER REFERENCES funcionarios(id) ON DELETE CASCADE,
        usado BOOLEAN DEFAULT FALSE,
        criado_em TIMESTAMPTZ DEFAULT NOW(),
        expira_em TIMESTAMPTZ DEFAULT (NOW() + INTERVAL '72 hours')
      )
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
