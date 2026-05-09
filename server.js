require('dotenv').config();
const express = require('express');
const cors = require('cors');
const path = require('path');
const { pool, query: dbQuery } = require('./src/db');

const app = express();

const corsOrigins = (process.env.CORS_ORIGIN || '')
  .split(',').map(s => s.trim()).filter(Boolean);
app.use(cors({
  origin: corsOrigins.length
    ? (origin, cb) => {
        if (!origin || corsOrigins.includes(origin)) return cb(null, true);
        return cb(new Error('Origem não permitida pelo CORS'));
      }
    : false,
  credentials: true,
}));
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
app.use('/api/acordos', require('./src/routes/acordos'));
app.use('/api/ultrasyst', require('./src/routes/ultrasyst'));
app.use('/api/produtos-embalagem', require('./src/routes/produtos_embalagem'));
app.use('/api/embalagens-fornecedor', require('./src/routes/embalagens_fornecedor'));
app.use('/api/auditagem-divergencias', require('./src/routes/auditagem_divergencias'));
app.use('/api/validades-em-risco', require('./src/routes/validades_em_risco'));
app.use('/api/devolucoes', require('./src/routes/devolucoes'));
app.use('/api/perdas', require('./src/routes/perdas'));
app.use('/api/sync-status', require('./src/routes/sync_status'));
app.use('/api/notificacoes', require('./src/routes/notificacoes'));
app.use('/api/dashboard', require('./src/routes/dashboard'));

// ── PÁGINAS ───────────────────────────────────────────────────────
app.get('/favicon.ico', (req, res) => res.redirect(301, '/favicon.svg'));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public/index.html')));
app.get('/rh', (req, res) => res.sendFile(path.join(__dirname, 'public/rh.html')));
app.get('/rh-painel', (req, res) => res.sendFile(path.join(__dirname, 'public/rh-painel.html')));
app.get('/ponto', (req, res) => res.sendFile(path.join(__dirname, 'public/ponto.html')));
app.get('/importar-qlp', (req, res) => res.sendFile(path.join(__dirname, 'public/importar-qlp.html')));
app.get('/notas-cadastro', (req, res) => res.sendFile(path.join(__dirname, 'public/notas-cadastro.html')));
app.get('/notas-transferencias-cadastro', (req, res) => res.sendFile(path.join(__dirname, 'public/notas-transferencias-cadastro.html')));
app.get('/notas-estoque', (req, res) => res.sendFile(path.join(__dirname, 'public/notas-estoque.html')));
app.get('/notas-auditoria', (req, res) => res.sendFile(path.join(__dirname, 'public/notas-auditoria.html')));
app.get('/usuarios', (req, res) => res.sendFile(path.join(__dirname, 'public/usuarios.html')));
app.get('/foto-upload', (req, res) => res.sendFile(path.join(__dirname, 'public/foto-upload.html')));
app.get('/fornecedores', (req, res) => res.sendFile(path.join(__dirname, 'public/fornecedores.html')));
app.get('/pedidos-comprador', (req, res) => res.sendFile(path.join(__dirname, 'public/pedidos-comprador.html')));
app.get('/auditoria-pedidos', (req, res) => res.sendFile(path.join(__dirname, 'public/auditoria-pedidos.html')));
app.get('/vendedor-cadastro', (req, res) => res.sendFile(path.join(__dirname, 'public/vendedor-cadastro.html')));
app.get('/vendedor', (req, res) => res.sendFile(path.join(__dirname, 'public/vendedor.html')));
app.get('/notas-comprador', (req, res) => res.sendFile(path.join(__dirname, 'public/notas-comprador.html')));
app.get('/notas-cd', (req, res) => res.sendFile(path.join(__dirname, 'public/notas-cd.html')));
app.get('/notas-distribuidora', (req, res) => res.sendFile(path.join(__dirname, 'public/notas-distribuidora.html')));
app.get('/sugestao-compras', (req, res) => res.sendFile(path.join(__dirname, 'public/sugestao-compras.html')));
app.get('/contas-receber', (req, res) => res.sendFile(path.join(__dirname, 'public/contas-receber.html')));
app.get('/produtos-embalagem', (req, res) => res.sendFile(path.join(__dirname, 'public/produtos-embalagem.html')));
app.get('/embalagens-fornecedor', (req, res) => res.sendFile(path.join(__dirname, 'public/embalagens-fornecedor.html')));
app.get('/acordo-extrato', (req, res) => res.sendFile(path.join(__dirname, 'public/acordo-extrato.html')));
app.get('/auditagem-divergencias', (req, res) => res.sendFile(path.join(__dirname, 'public/auditagem-divergencias.html')));
app.get('/validades-em-risco', (req, res) => res.sendFile(path.join(__dirname, 'public/validades-em-risco.html')));
app.get('/nota-historico', (req, res) => res.sendFile(path.join(__dirname, 'public/nota-historico.html')));
app.get('/aguardando-devolucao', (req, res) => res.sendFile(path.join(__dirname, 'public/aguardando-devolucao.html')));
app.get('/preview-icons', (req, res) => res.sendFile(path.join(__dirname, 'public/preview-icons.html')));
app.get('/auditoria-acordos', (req, res) => res.sendFile(path.join(__dirname, 'public/auditoria-acordos.html')));
app.get('/devolucoes-divergencias', (req, res) => res.sendFile(path.join(__dirname, 'public/devolucoes-divergencias.html')));
app.get('/dashboard-notas', (req, res) => res.sendFile(path.join(__dirname, 'public/dashboard-notas.html')));
app.get('/divergencias-preco', (req, res) => res.sendFile(path.join(__dirname, 'public/divergencias-preco.html')));
app.get('/produtos-novos', (req, res) => res.sendFile(path.join(__dirname, 'public/produtos-novos.html')));
app.get('/emergenciais', (req, res) => res.sendFile(path.join(__dirname, 'public/emergenciais.html')));
app.get('/sla-notas', (req, res) => res.sendFile(path.join(__dirname, 'public/sla-notas.html')));
app.get('/divergencias-estoque', (req, res) => res.sendFile(path.join(__dirname, 'public/divergencias-estoque.html')));
app.get('/pedidos-fornecedor', (req, res) => res.sendFile(path.join(__dirname, 'public/pedidos-fornecedor.html')));
app.get('/index-novo', (req, res) => res.sendFile(path.join(__dirname, 'public/index-novo.html')));
app.get('/index-novo-perfis', (req, res) => res.sendFile(path.join(__dirname, 'public/index-novo-perfis.html')));

// ── INIT DB ───────────────────────────────────────────────────────
async function runMigration(client, name, sql) {
  const { rows } = await client.query('SELECT 1 FROM _migrations WHERE name = $1', [name]);
  if (rows.length) return;
  try {
    await client.query(sql);
    await client.query('INSERT INTO _migrations (name) VALUES ($1)', [name]);
    console.log(`[DB] Migration aplicada: ${name}`);
  } catch (err) {
    console.error(`[DB] Migration falhou (${name}):`, err.message);
  }
}

async function initDB() {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS _migrations (
        name TEXT PRIMARY KEY,
        aplicada_em TIMESTAMPTZ DEFAULT NOW()
      )
    `);

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
    // 'cprod_assoc' tem 11 chars — extender pra 30 (era 10 originalmente)
    await runMigration(client, '20260506_ean_fonte_alter_30',
      `ALTER TABLE itens_nota ALTER COLUMN ean_fonte TYPE VARCHAR(30)`);
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
      await client.query(`INSERT INTO lojas (id, nome) VALUES (1,'ECONOMICO'),(2,'BR'),(3,'JOAO PESSOA'),(4,'FLORESTA'),(5,'SAO JOSE'),(6,'SANTAREM') ON CONFLICT DO NOTHING`);
    }
    await runMigration(client, '20260502_loja3_joao_pessoa',
      `UPDATE lojas SET nome = 'JOAO PESSOA' WHERE id = 3 AND nome = 'JOAO PAULO'`);

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

    // ── Índices e constraints adicionais ──────────────────────────
    await runMigration(client, '20260501_idx_itens_nota_id',
      'CREATE INDEX IF NOT EXISTS idx_itens_nota_id ON itens_nota(nota_id)');
    await runMigration(client, '20260501_idx_conferencias_item_id',
      'CREATE INDEX IF NOT EXISTS idx_conferencias_item_id ON conferencias_estoque(item_id)');
    await runMigration(client, '20260501_idx_conferencia_lotes_conf',
      'CREATE INDEX IF NOT EXISTS idx_conferencia_lotes_conf ON conferencia_lotes(conferencia_id)');
    await runMigration(client, '20260501_idx_funcionarios_matricula',
      'CREATE INDEX IF NOT EXISTS idx_funcionarios_matricula ON funcionarios(matricula)');
    await runMigration(client, '20260501_idx_compras_loja_data',
      'CREATE INDEX IF NOT EXISTS idx_compras_loja_data ON compras_historico(loja_id, data_entrada)');
    await runMigration(client, '20260501_idx_compras_codbarra',
      'CREATE INDEX IF NOT EXISTS idx_compras_codbarra ON compras_historico(codigobarra)');
    await runMigration(client, '20260501_idx_vendas_loja_data',
      'CREATE INDEX IF NOT EXISTS idx_vendas_loja_data ON vendas_historico(loja_id, data_venda)');
    await runMigration(client, '20260501_idx_vendas_codbarra',
      'CREATE INDEX IF NOT EXISTS idx_vendas_codbarra ON vendas_historico(codigobarra)');
    await runMigration(client, '20260501_idx_notas_loja',
      'CREATE INDEX IF NOT EXISTS idx_notas_loja ON notas_entrada(loja_id)');
    await runMigration(client, '20260501_idx_notas_status',
      'CREATE INDEX IF NOT EXISTS idx_notas_status ON notas_entrada(status)');
    await runMigration(client, '20260501_idx_cr_debitos_forn',
      'CREATE INDEX IF NOT EXISTS idx_cr_debitos_forn ON cr_debitos(fornecedor_id)');
    await runMigration(client, '20260501_idx_cr_creditos_debito',
      'CREATE INDEX IF NOT EXISTS idx_cr_creditos_debito ON cr_creditos(debito_id)');
    await runMigration(client, '20260501_idx_cr_debito_itens_debito',
      'CREATE INDEX IF NOT EXISTS idx_cr_debito_itens_debito ON cr_debito_itens(debito_id)');
    await runMigration(client, '20260501_idx_pedidos_fornecedor',
      'CREATE INDEX IF NOT EXISTS idx_pedidos_fornecedor ON pedidos(fornecedor_id)');
    await runMigration(client, '20260501_idx_pedidos_status',
      'CREATE INDEX IF NOT EXISTS idx_pedidos_status ON pedidos(status)');
    await runMigration(client, '20260501_funcionarios_terceirizada',
      'ALTER TABLE funcionarios ADD COLUMN IF NOT EXISTS terceirizada VARCHAR(100)');

    // Vendedores resilientes a deleções de fornecedor (Açougue compartilha tabela)
    await runMigration(client, '20260501_vendedores_fornecedor_cnpj',
      `ALTER TABLE vendedores ADD COLUMN IF NOT EXISTS fornecedor_cnpj VARCHAR(18)`);
    await runMigration(client, '20260501_idx_vendedores_cnpj',
      `CREATE INDEX IF NOT EXISTS idx_vendedores_cnpj ON vendedores(fornecedor_cnpj)`);
    await runMigration(client, '20260501_pedidos_preco_valido_ate',
      `ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS preco_valido_ate DATE`);
    await runMigration(client, '20260502_pedidos_auditoria',
      `ALTER TABLE pedidos
         ADD COLUMN IF NOT EXISTS auditado_por VARCHAR(150),
         ADD COLUMN IF NOT EXISTS auditado_em TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS rejeitado_por VARCHAR(150),
         ADD COLUMN IF NOT EXISTS rejeitado_em TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS motivo_rejeicao TEXT`);
    await runMigration(client, '20260502_pedidos_editado_auditoria',
      `ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS editado_na_auditoria BOOLEAN DEFAULT FALSE`);
    await runMigration(client, '20260502_itens_marcadores_alteracao',
      `ALTER TABLE itens_pedido
         ADD COLUMN IF NOT EXISTS adicionado_pelo_comprador BOOLEAN DEFAULT FALSE,
         ADD COLUMN IF NOT EXISTS excluido_pelo_comprador BOOLEAN DEFAULT FALSE`);
    await runMigration(client, '20260502_pedidos_faturamento',
      `ALTER TABLE pedidos
         ADD COLUMN IF NOT EXISTS faturado_em TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS faturado_por VARCHAR(150),
         ADD COLUMN IF NOT EXISTS numero_nf_faturada VARCHAR(50),
         ADD COLUMN IF NOT EXISTS atrasado_em TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS ultimo_lembrete_em TIMESTAMPTZ`);
    await runMigration(client, '20260502_itens_pedido_justificativa',
      `ALTER TABLE itens_pedido
         ADD COLUMN IF NOT EXISTS justificativa_excesso TEXT,
         ADD COLUMN IF NOT EXISTS sugestao_sistema NUMERIC(14,3)`);
    await runMigration(client, '20260502_pedidos_cancelamento_vendedor',
      `ALTER TABLE pedidos
         ADD COLUMN IF NOT EXISTS cancelado_em TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS cancelado_por VARCHAR(150),
         ADD COLUMN IF NOT EXISTS motivo_cancelamento TEXT`);
    await runMigration(client, '20260503_notas_origem_cd',
      `ALTER TABLE notas_entrada
         ADD COLUMN IF NOT EXISTS origem VARCHAR(20) DEFAULT 'nfe',
         ADD COLUMN IF NOT EXISTS cd_mov_codi VARCHAR(7),
         ADD COLUMN IF NOT EXISTS cd_loja_cli_codi VARCHAR(10),
         ADD COLUMN IF NOT EXISTS cd_synced_em TIMESTAMPTZ`);
    await runMigration(client, '20260503_notas_cd_unique',
      `CREATE UNIQUE INDEX IF NOT EXISTS uniq_notas_cd_mov ON notas_entrada(cd_mov_codi) WHERE cd_mov_codi IS NOT NULL`);
    await runMigration(client, '20260503_sync_state',
      `CREATE TABLE IF NOT EXISTS _sync_state (
         chave VARCHAR(100) PRIMARY KEY,
         valor TEXT,
         atualizado_em TIMESTAMPTZ DEFAULT NOW()
       )`);
    await runMigration(client, '20260503_notas_data_recebimento',
      `ALTER TABLE notas_entrada ADD COLUMN IF NOT EXISTS data_recebimento DATE`);
    await runMigration(client, '20260503_idx_compras_match_cd',
      `CREATE INDEX IF NOT EXISTS idx_compras_match_cd ON compras_historico(loja_id, numeronfe) WHERE fornecedor_cnpj = '17764296000209'`);
    await runMigration(client, '20260503_idx_compras_match_cd_norm',
      `CREATE INDEX IF NOT EXISTS idx_compras_match_cd_norm
         ON compras_historico(loja_id, REGEXP_REPLACE(numeronfe, '^0+', ''))
         WHERE fornecedor_cnpj = '17764296000209'`);
    await runMigration(client, '20260503_cd_status_inicial',
      `UPDATE notas_entrada SET status='em_transito' WHERE origem='cd' AND status='aguardando_estoque'`);

    // Fluxo CD novo: em_transito → recebida → em_conferencia → (auditagem) → conferida → validada
    await runMigration(client, '20260503_notas_validada_em',
      `ALTER TABLE notas_entrada ADD COLUMN IF NOT EXISTS validada_em TIMESTAMPTZ`);
    await runMigration(client, '20260503_notas_alertas',
      `CREATE TABLE IF NOT EXISTS notas_alertas (
         id SERIAL PRIMARY KEY,
         nota_id INTEGER NOT NULL REFERENCES notas_entrada(id) ON DELETE CASCADE,
         tipo VARCHAR(50) NOT NULL,
         mensagem TEXT,
         criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         lido_em TIMESTAMPTZ,
         lido_por VARCHAR(150)
       )`);
    await runMigration(client, '20260503_idx_notas_alertas_nota',
      `CREATE INDEX IF NOT EXISTS idx_notas_alertas_nota ON notas_alertas(nota_id)`);
    await runMigration(client, '20260503_uniq_notas_alertas_tipo_nota',
      `CREATE UNIQUE INDEX IF NOT EXISTS uniq_notas_alertas_tipo_nota
         ON notas_alertas(nota_id, tipo) WHERE lido_em IS NULL`);

    // Backfill fluxo CD: fechada→validada; em_conferencia (com match) → validada; em_conferencia (sem match) → em_transito
    await runMigration(client, '20260503_cd_backfill_validada_fechada',
      `UPDATE notas_entrada SET status='validada' WHERE origem='cd' AND status='fechada'`);
    await runMigration(client, '20260503_cd_backfill_validada_match',
      `UPDATE notas_entrada n SET status='validada'
         FROM compras_historico c
        WHERE n.origem='cd' AND n.status='em_conferencia'
          AND c.loja_id=n.loja_id
          AND c.fornecedor_cnpj='17764296000209'
          AND c.numeronfe=REGEXP_REPLACE(n.cd_mov_codi,'^0+','')`);
    await runMigration(client, '20260503_cd_backfill_em_transito_resto',
      `UPDATE notas_entrada SET status='em_transito' WHERE origem='cd' AND status='em_conferencia'`);
    // Pentaho compras_historico só pega NF de 01/07/2025 pra cá → notas CD antes disso nunca casam.
    // Assumir como histórico já tratado: marcar como validada.
    await runMigration(client, '20260503_cd_zumbis_pre_corte_pentaho',
      `UPDATE notas_entrada
          SET status='validada', validada_em=COALESCE(validada_em, NOW())
        WHERE origem='cd' AND status='em_transito'
          AND data_emissao < DATE '2025-07-01'`);
    // Filtro do Pentaho é estrito (DATAENTRADA > 01/07), então 01/07 também não casa. Cortar igual.
    await runMigration(client, '20260503_cd_zumbis_01_07_2025',
      `UPDATE notas_entrada
          SET status='validada', validada_em=COALESCE(validada_em, NOW())
        WHERE origem='cd' AND status='em_transito'
          AND data_emissao < DATE '2025-07-02'`);

    await runMigration(client, '20260503_itens_nota_cd_pro_codi',
      `ALTER TABLE itens_nota ADD COLUMN IF NOT EXISTS cd_pro_codi VARCHAR(10)`);
    await runMigration(client, '20260503_itens_nota_sem_cod_barras',
      `ALTER TABLE itens_nota ADD COLUMN IF NOT EXISTS sem_codigo_barras BOOLEAN DEFAULT FALSE`);
    await runMigration(client, '20260504_itens_nota_qtd_por_caixa',
      `ALTER TABLE itens_nota
         ADD COLUMN IF NOT EXISTS qtd_comercial NUMERIC(12,4),
         ADD COLUMN IF NOT EXISTS un_comercial VARCHAR(10),
         ADD COLUMN IF NOT EXISTS qtd_tributavel NUMERIC(12,4),
         ADD COLUMN IF NOT EXISTS un_tributavel VARCHAR(10),
         ADD COLUMN IF NOT EXISTS qtd_por_caixa_nfe INTEGER,
         ADD COLUMN IF NOT EXISTS qtd_por_caixa_confianca VARCHAR(10)`);
    await runMigration(client, '20260504_itens_nota_preco_unitario_caixa',
      `ALTER TABLE itens_nota
         ADD COLUMN IF NOT EXISTS preco_unitario_caixa NUMERIC(12,4)`);
    await runMigration(client, '20260504_itens_nota_qtd_em_unidades',
      `ALTER TABLE itens_nota
         ADD COLUMN IF NOT EXISTS qtd_em_unidades NUMERIC(14,4)`);
    await runMigration(client, '20260505_itens_nota_cprod_fornecedor',
      `ALTER TABLE itens_nota
         ADD COLUMN IF NOT EXISTS cprod_fornecedor VARCHAR(60)`);
    await runMigration(client, '20260505_eans_fornecedor',
      `CREATE TABLE IF NOT EXISTS eans_fornecedor (
         id SERIAL PRIMARY KEY,
         fornecedor_cnpj VARCHAR(20) NOT NULL,
         fornecedor_nome VARCHAR(200),
         cprod_fornecedor VARCHAR(60),
         descricao_normalizada TEXT,
         ean_validado VARCHAR(20) NOT NULL,
         associado_em TIMESTAMP DEFAULT NOW(),
         associado_por VARCHAR(150),
         atualizado_em TIMESTAMP DEFAULT NOW()
       )`);
    await runMigration(client, '20260505_eans_fornecedor_uniq_cprod',
      `CREATE UNIQUE INDEX IF NOT EXISTS eans_fornecedor_cprod_uniq
         ON eans_fornecedor (fornecedor_cnpj, cprod_fornecedor)
         WHERE cprod_fornecedor IS NOT NULL`);
    await runMigration(client, '20260505_eans_fornecedor_uniq_desc',
      `CREATE UNIQUE INDEX IF NOT EXISTS eans_fornecedor_desc_uniq
         ON eans_fornecedor (fornecedor_cnpj, descricao_normalizada)
         WHERE cprod_fornecedor IS NULL AND descricao_normalizada IS NOT NULL`);

    // Índices funcionais pra queries que usam NULLIF(LTRIM(codigobarra,'0'),'')
    // Beneficia: jrlira-tech (acordos.js, notas.js, sync_ultrasyst.js) e acougue-senhas (validade-dashboard).
    // Sem CONCURRENTLY porque migrations podem rodar em transação implícita do driver.
    await runMigration(client, '20260505_idx_vh_barcode_norm',
      `CREATE INDEX IF NOT EXISTS idx_vh_barcode_norm_loja_data
         ON vendas_historico ((NULLIF(LTRIM(codigobarra,'0'),'')), loja_id, data_venda DESC)`);
    await runMigration(client, '20260505_idx_ch_barcode_norm',
      `CREATE INDEX IF NOT EXISTS idx_ch_barcode_norm_loja_data
         ON compras_historico ((NULLIF(LTRIM(codigobarra,'0'),'')), loja_id, data_entrada DESC)`);

    // Devolução com valor real do XML (pode divergir do esperado calculado pelo sistema)
    await runMigration(client, '20260505_devolucoes_valor_xml',
      `ALTER TABLE devolucoes
         ADD COLUMN IF NOT EXISTS valor_xml NUMERIC(14,2),
         ADD COLUMN IF NOT EXISTS valor_xml_vprod NUMERIC(14,2),
         ADD COLUMN IF NOT EXISTS valor_xml_vst NUMERIC(14,2)`);
    await runMigration(client, '20260505_devolucoes_diferenca_valor',
      `ALTER TABLE devolucoes
         ADD COLUMN IF NOT EXISTS diferenca_valor NUMERIC(14,2)
           GENERATED ALWAYS AS (COALESCE(valor_xml,0) - COALESCE(valor_total,0)) STORED`);

    // Saídas com classificação (venda/avaria/transferencia/producao/bonificacao/consumo_interno).
    // Pentaho TVENPEDIDO traz tipo_saida; backend filtra 'venda' por padrão pra média_dia.
    await runMigration(client, '20260506_vendas_historico_tipo_saida',
      `ALTER TABLE vendas_historico
         ADD COLUMN IF NOT EXISTS tipo_saida VARCHAR(30)`);
    // Backfill desabilitado (UPDATE em ~5M linhas estoura statement_timeout do Supabase).
    // Filtros usam COALESCE(tipo_saida,'venda')='venda' — NULL é tratado como venda automaticamente.
    // Pra reativar, rodar manual em chunks de 100k em janela de baixo uso.
    await runMigration(client, '20260506_backfill_tipo_saida_venda',
      `SELECT 1`);
    // Índice em tabela grande gera carga pesada na criação — adiar.
    // Os filtros já usam idx_vh_barcode_norm_loja_data + COALESCE; performance aceitável sem este índice.
    await runMigration(client, '20260506_idx_vh_tipo_saida',
      `SELECT 1`);
    // Trigger UPSERT atualizado pra incluir tipo_saida na chave de dedup
    // (avaria + venda do mesmo dia/produto não devem colidir)
    await runMigration(client, '20260506_trim_skip_dup_vendas_tipo',
      `CREATE OR REPLACE FUNCTION trim_skip_dup_vendas() RETURNS TRIGGER AS $trg$
       BEGIN
         NEW.codigobarra := NULLIF(LTRIM(COALESCE(NEW.codigobarra,''),'0'),'');
         NEW.tipo_saida  := COALESCE(NEW.tipo_saida, 'venda');
         IF EXISTS (
           SELECT 1 FROM vendas_historico
            WHERE loja_id = NEW.loja_id
              AND codigobarra = NEW.codigobarra
              AND data_venda = NEW.data_venda
              AND COALESCE(tipo_saida,'venda') = NEW.tipo_saida
         ) THEN
           UPDATE vendas_historico
              SET qtd_vendida = NEW.qtd_vendida, sincronizado_em = NOW()
            WHERE loja_id = NEW.loja_id
              AND codigobarra = NEW.codigobarra
              AND data_venda = NEW.data_venda
              AND COALESCE(tipo_saida,'venda') = NEW.tipo_saida;
           RETURN NULL;
         END IF;
         RETURN NEW;
       END;
       $trg$ LANGUAGE plpgsql;`);

    // Devolução de compra emitida pela loja — sync via Pentaho (TESTDEVOLUCAO + TESTDEVOLUCAOPRODUTO)
    await runMigration(client, '20260506_devolucoes_compra_historico',
      `CREATE TABLE IF NOT EXISTS devolucoes_compra_historico (
         id SERIAL PRIMARY KEY,
         loja_id INTEGER NOT NULL,
         devolucao_codigo INTEGER NOT NULL,
         fornecedor_codigo VARCHAR(20),
         fornecedor_cnpj VARCHAR(20),
         fornecedor_nome VARCHAR(200),
         natureza_codigo VARCHAR(10),
         data_devolucao TIMESTAMP,
         data_nfe DATE,
         chave_nfe VARCHAR(44),
         numero_nfe VARCHAR(20),
         serie_nfe VARCHAR(10),
         valor_total NUMERIC(14,2),
         chave_nfe_compra_original VARCHAR(44),
         sincronizado_em TIMESTAMPTZ DEFAULT NOW(),
         UNIQUE (loja_id, devolucao_codigo)
       )`);
    await runMigration(client, '20260506_devolucoes_compra_itens_historico',
      `CREATE TABLE IF NOT EXISTS devolucoes_compra_itens_historico (
         id SERIAL PRIMARY KEY,
         loja_id INTEGER NOT NULL,
         devolucao_codigo INTEGER NOT NULL,
         produto_codigo VARCHAR(20),
         codigobarra VARCHAR(20),
         qtd NUMERIC(14,3),
         preco_unitario NUMERIC(14,4),
         valor_total NUMERIC(14,2),
         sincronizado_em TIMESTAMPTZ DEFAULT NOW()
       )`);
    await runMigration(client, '20260506_idx_dch_chave_nfe',
      `CREATE INDEX IF NOT EXISTS idx_dch_chave_nfe ON devolucoes_compra_historico (chave_nfe)`);
    await runMigration(client, '20260506_idx_dch_fornecedor',
      `CREATE INDEX IF NOT EXISTS idx_dch_fornecedor_data
         ON devolucoes_compra_historico (fornecedor_cnpj, data_devolucao DESC)`);
    await runMigration(client, '20260506_idx_dch_loja_data',
      `CREATE INDEX IF NOT EXISTS idx_dch_loja_data
         ON devolucoes_compra_historico (loja_id, data_devolucao DESC)`);
    await runMigration(client, '20260506_idx_dchi_loja_codigo',
      `CREATE INDEX IF NOT EXISTS idx_dchi_loja_codigo
         ON devolucoes_compra_itens_historico (loja_id, devolucao_codigo)`);
    await runMigration(client, '20260506_idx_dchi_codigobarra',
      `CREATE INDEX IF NOT EXISTS idx_dchi_codigobarra
         ON devolucoes_compra_itens_historico ((NULLIF(LTRIM(codigobarra,'0'),'')))`);

    // UNIQUE em itens (suporta dedup quando Pentaho roda 2x)
    await runMigration(client, '20260506_dchi_uniq',
      `CREATE UNIQUE INDEX IF NOT EXISTS uniq_dchi_loja_dev_prod
         ON devolucoes_compra_itens_historico (loja_id, devolucao_codigo, produto_codigo)`);

    // Triggers UPSERT silenciosos pras 2 novas tabelas (mesmo padrão de vendas/compras_historico)
    await runMigration(client, '20260506_trg_dch_upsert',
      `CREATE OR REPLACE FUNCTION dch_upsert() RETURNS TRIGGER AS $trg$
       BEGIN
         IF EXISTS (
           SELECT 1 FROM devolucoes_compra_historico
            WHERE loja_id = NEW.loja_id AND devolucao_codigo = NEW.devolucao_codigo
         ) THEN
           UPDATE devolucoes_compra_historico
              SET fornecedor_codigo = NEW.fornecedor_codigo,
                  fornecedor_cnpj   = NEW.fornecedor_cnpj,
                  fornecedor_nome   = NEW.fornecedor_nome,
                  natureza_codigo   = NEW.natureza_codigo,
                  data_devolucao    = NEW.data_devolucao,
                  data_nfe          = NEW.data_nfe,
                  chave_nfe         = NEW.chave_nfe,
                  numero_nfe        = NEW.numero_nfe,
                  serie_nfe         = NEW.serie_nfe,
                  valor_total       = NEW.valor_total,
                  chave_nfe_compra_original = NEW.chave_nfe_compra_original,
                  sincronizado_em   = NOW()
            WHERE loja_id = NEW.loja_id AND devolucao_codigo = NEW.devolucao_codigo;
           RETURN NULL;
         END IF;
         RETURN NEW;
       END;
       $trg$ LANGUAGE plpgsql;
       DROP TRIGGER IF EXISTS trg_dch_upsert ON devolucoes_compra_historico;
       CREATE TRIGGER trg_dch_upsert BEFORE INSERT ON devolucoes_compra_historico
         FOR EACH ROW EXECUTE FUNCTION dch_upsert();`);

    await runMigration(client, '20260506_trg_dchi_upsert',
      `CREATE OR REPLACE FUNCTION dchi_upsert() RETURNS TRIGGER AS $trg$
       BEGIN
         NEW.codigobarra := NULLIF(LTRIM(COALESCE(NEW.codigobarra,''),'0'),'');
         IF EXISTS (
           SELECT 1 FROM devolucoes_compra_itens_historico
            WHERE loja_id = NEW.loja_id
              AND devolucao_codigo = NEW.devolucao_codigo
              AND produto_codigo = NEW.produto_codigo
         ) THEN
           UPDATE devolucoes_compra_itens_historico
              SET codigobarra     = NEW.codigobarra,
                  qtd             = NEW.qtd,
                  preco_unitario  = NEW.preco_unitario,
                  valor_total     = NEW.valor_total,
                  sincronizado_em = NOW()
            WHERE loja_id = NEW.loja_id
              AND devolucao_codigo = NEW.devolucao_codigo
              AND produto_codigo = NEW.produto_codigo;
           RETURN NULL;
         END IF;
         RETURN NEW;
       END;
       $trg$ LANGUAGE plpgsql;
       DROP TRIGGER IF EXISTS trg_dchi_upsert ON devolucoes_compra_itens_historico;
       CREATE TRIGGER trg_dchi_upsert BEFORE INSERT ON devolucoes_compra_itens_historico
         FOR EACH ROW EXECUTE FUNCTION dchi_upsert();`);

    // Entradas com classificação (compra/transferencia_entrada/bonificacao_recebida/devolucao_venda).
    // Backend filtra 'compra' por padrão pra não duplicar transferência interna como compra real.
    await runMigration(client, '20260506_compras_historico_tipo_entrada',
      `ALTER TABLE compras_historico
         ADD COLUMN IF NOT EXISTS tipo_entrada VARCHAR(30)`);
    await runMigration(client, '20260506_backfill_tipo_entrada_compra',
      `UPDATE compras_historico SET tipo_entrada='compra' WHERE tipo_entrada IS NULL`);
    await runMigration(client, '20260506_idx_ch_tipo_entrada',
      `CREATE INDEX IF NOT EXISTS idx_ch_tipo_entrada_loja_data
         ON compras_historico (tipo_entrada, loja_id, data_entrada DESC)`);
    // Trigger UPSERT atualizado pra considerar tipo_entrada na chave de dedup
    // (compra de fornecedor + transferência interna podem ter mesmo numeronfe entre lojas)
    await runMigration(client, '20260506_trim_skip_dup_compras_tipo',
      `CREATE OR REPLACE FUNCTION trim_skip_dup_compras() RETURNS TRIGGER AS $trg$
       BEGIN
         NEW.codigobarra := NULLIF(LTRIM(COALESCE(NEW.codigobarra,''),'0'),'');
         NEW.tipo_entrada := COALESCE(NEW.tipo_entrada, 'compra');
         IF NEW.codigobarra IS NOT NULL AND EXISTS (
           SELECT 1 FROM compras_historico
            WHERE loja_id = NEW.loja_id
              AND codigobarra = NEW.codigobarra
              AND numeronfe = NEW.numeronfe
              AND COALESCE(tipo_entrada,'compra') = NEW.tipo_entrada
         ) THEN
           RETURN NULL;
         END IF;
         RETURN NEW;
       END;
       $trg$ LANGUAGE plpgsql;`);

    // KTRs Firebird emitiam tipo_xxx com padding (CASE → CHAR(N) right-padded). Triggers comparavam AS IS,
    // então 'compra' vs 'compra               ' não dedupavam → 130k duplicatas em L1/L2. Fix: TRIM no NEW.
    await runMigration(client, '20260506_trim_skip_dup_vendas_v3_trim_tipo',
      `CREATE OR REPLACE FUNCTION trim_skip_dup_vendas() RETURNS TRIGGER AS $trg$
       BEGIN
         NEW.codigobarra := NULLIF(LTRIM(COALESCE(NEW.codigobarra,''),'0'),'');
         NEW.tipo_saida  := TRIM(COALESCE(NEW.tipo_saida, 'venda'));
         IF EXISTS (
           SELECT 1 FROM vendas_historico
            WHERE loja_id = NEW.loja_id
              AND codigobarra = NEW.codigobarra
              AND data_venda = NEW.data_venda
              AND COALESCE(tipo_saida,'venda') = NEW.tipo_saida
         ) THEN
           UPDATE vendas_historico
              SET qtd_vendida = NEW.qtd_vendida, sincronizado_em = NOW()
            WHERE loja_id = NEW.loja_id
              AND codigobarra = NEW.codigobarra
              AND data_venda = NEW.data_venda
              AND COALESCE(tipo_saida,'venda') = NEW.tipo_saida;
           RETURN NULL;
         END IF;
         RETURN NEW;
       END;
       $trg$ LANGUAGE plpgsql;`);

    await runMigration(client, '20260506_trim_skip_dup_compras_v3_trim_tipo',
      `CREATE OR REPLACE FUNCTION trim_skip_dup_compras() RETURNS TRIGGER AS $trg$
       BEGIN
         NEW.codigobarra := NULLIF(LTRIM(COALESCE(NEW.codigobarra,''),'0'),'');
         NEW.tipo_entrada := TRIM(COALESCE(NEW.tipo_entrada, 'compra'));
         IF NEW.codigobarra IS NOT NULL AND EXISTS (
           SELECT 1 FROM compras_historico
            WHERE loja_id = NEW.loja_id
              AND codigobarra = NEW.codigobarra
              AND numeronfe = NEW.numeronfe
              AND COALESCE(tipo_entrada,'compra') = NEW.tipo_entrada
         ) THEN
           RETURN NULL;
         END IF;
         RETURN NEW;
       END;
       $trg$ LANGUAGE plpgsql;`);

    // Índice único pra prevenir duplicação física em compras_historico.
    // (vendas já tem uniq em loja_id+codigobarra+data_venda, mas compras só tinha pkey em id)
    await runMigration(client, '20260506_compras_historico_dedup_unique',
      `CREATE UNIQUE INDEX IF NOT EXISTS compras_historico_dedup_unique
         ON compras_historico (loja_id, numeronfe, codigobarra, data_entrada, fornecedor_cnpj)`);

    // V4: triggers fazem UPSERT real (UPDATE quando chave física já existe). Necessário pra
    // reclassificar tipo_xxx historico — KTR antigo deixou tudo como 'compra'/'venda', e o KTR
    // novo só inseriria movs novos. Com UPSERT, o sync re-extrai tudo desde 2025-01-01 e
    // reclassifica linhas existentes automaticamente.
    await runMigration(client, '20260506_trim_skip_dup_vendas_v4_upsert',
      `CREATE OR REPLACE FUNCTION trim_skip_dup_vendas() RETURNS TRIGGER AS $trg$
       BEGIN
         NEW.codigobarra := NULLIF(LTRIM(COALESCE(NEW.codigobarra,''),'0'),'');
         NEW.tipo_saida  := TRIM(COALESCE(NEW.tipo_saida, 'venda'));
         IF NEW.codigobarra IS NULL THEN RETURN NEW; END IF;
         UPDATE vendas_historico
            SET qtd_vendida = NEW.qtd_vendida,
                tipo_saida  = NEW.tipo_saida,
                sincronizado_em = NOW()
          WHERE loja_id = NEW.loja_id
            AND codigobarra = NEW.codigobarra
            AND data_venda = NEW.data_venda;
         IF FOUND THEN RETURN NULL; END IF;
         RETURN NEW;
       END;
       $trg$ LANGUAGE plpgsql;`);

    // V5: id_tabela_preco entra na chave (necessário pra calcular consumido por tabela do ERP)
    await runMigration(client, '20260507_vendas_historico_id_tabela_preco',
      `ALTER TABLE vendas_historico ADD COLUMN IF NOT EXISTS id_tabela_preco INT`);
    await runMigration(client, '20260507_vendas_historico_drop_unique_v4',
      `ALTER TABLE vendas_historico DROP CONSTRAINT IF EXISTS vendas_historico_loja_id_codigobarra_data_venda_key`);
    await runMigration(client, '20260507_vendas_historico_unique_v5',
      `CREATE UNIQUE INDEX IF NOT EXISTS vendas_historico_uniq_v5
         ON vendas_historico (loja_id, codigobarra, data_venda, COALESCE(id_tabela_preco, 0))`);
    await runMigration(client, '20260507_idx_vendas_id_tabela_preco',
      `CREATE INDEX IF NOT EXISTS idx_vendas_id_tabela_preco
         ON vendas_historico (id_tabela_preco) WHERE id_tabela_preco IS NOT NULL`);
    await runMigration(client, '20260507_trim_skip_dup_vendas_v5_idtabela',
      `CREATE OR REPLACE FUNCTION trim_skip_dup_vendas() RETURNS TRIGGER AS $trg$
       BEGIN
         NEW.codigobarra := NULLIF(LTRIM(COALESCE(NEW.codigobarra,''),'0'),'');
         NEW.tipo_saida  := TRIM(COALESCE(NEW.tipo_saida, 'venda'));
         IF NEW.codigobarra IS NULL THEN RETURN NEW; END IF;
         UPDATE vendas_historico
            SET qtd_vendida = NEW.qtd_vendida, tipo_saida  = NEW.tipo_saida, sincronizado_em = NOW()
          WHERE loja_id = NEW.loja_id AND codigobarra = NEW.codigobarra AND data_venda = NEW.data_venda
            AND COALESCE(id_tabela_preco, 0) = COALESCE(NEW.id_tabela_preco, 0);
         IF FOUND THEN RETURN NULL; END IF;
         RETURN NEW;
       END;
       $trg$ LANGUAGE plpgsql;`);

    // V6: aproveita linha existente com id_tabela_preco=NULL e "promove" pra preencher,
    // evitando duplicar quando a coluna nova começa a ser populada pelo KTR.
    await runMigration(client, '20260507_trim_skip_dup_vendas_v6_promote_null',
      `CREATE OR REPLACE FUNCTION trim_skip_dup_vendas() RETURNS TRIGGER AS $trg$
       BEGIN
         NEW.codigobarra := NULLIF(LTRIM(COALESCE(NEW.codigobarra,''),'0'),'');
         NEW.tipo_saida  := TRIM(COALESCE(NEW.tipo_saida, 'venda'));
         IF NEW.codigobarra IS NULL THEN RETURN NEW; END IF;
         -- 1) Match exato (mesmo id_tabela_preco)
         UPDATE vendas_historico
            SET qtd_vendida = NEW.qtd_vendida, tipo_saida = NEW.tipo_saida, sincronizado_em = NOW()
          WHERE loja_id = NEW.loja_id AND codigobarra = NEW.codigobarra AND data_venda = NEW.data_venda
            AND COALESCE(id_tabela_preco, 0) = COALESCE(NEW.id_tabela_preco, 0);
         IF FOUND THEN RETURN NULL; END IF;
         -- 2) Se vem id_tabela_preco real e existe linha legada NULL, promove ela
         IF NEW.id_tabela_preco IS NOT NULL THEN
           WITH alvo AS (
             SELECT id FROM vendas_historico
              WHERE loja_id = NEW.loja_id AND codigobarra = NEW.codigobarra AND data_venda = NEW.data_venda
                AND id_tabela_preco IS NULL
              LIMIT 1
           )
           UPDATE vendas_historico v
              SET id_tabela_preco = NEW.id_tabela_preco, qtd_vendida = NEW.qtd_vendida,
                  tipo_saida = NEW.tipo_saida, sincronizado_em = NOW()
             FROM alvo WHERE v.id = alvo.id;
           IF FOUND THEN RETURN NULL; END IF;
         END IF;
         RETURN NEW;
       END;
       $trg$ LANGUAGE plpgsql;`);

    // Limpeza one-shot: remove linhas legadas com id_tabela_preco=NULL onde já existe outra preenchida
    await runMigration(client, '20260507_dedup_vendas_id_tabela_null',
      `DELETE FROM vendas_historico v_old
        WHERE v_old.id_tabela_preco IS NULL
          AND EXISTS (
            SELECT 1 FROM vendas_historico v_new
             WHERE v_new.loja_id = v_old.loja_id
               AND v_new.codigobarra = v_old.codigobarra
               AND v_new.data_venda = v_old.data_venda
               AND v_new.id_tabela_preco IS NOT NULL
          )`);

    await runMigration(client, '20260506_trim_skip_dup_compras_v4_upsert',
      `CREATE OR REPLACE FUNCTION trim_skip_dup_compras() RETURNS TRIGGER AS $trg$
       BEGIN
         NEW.codigobarra := NULLIF(LTRIM(COALESCE(NEW.codigobarra,''),'0'),'');
         NEW.tipo_entrada := TRIM(COALESCE(NEW.tipo_entrada, 'compra'));
         IF NEW.codigobarra IS NULL THEN RETURN NEW; END IF;
         UPDATE compras_historico
            SET tipo_entrada = NEW.tipo_entrada,
                qtd_comprada = NEW.qtd_comprada,
                custo_total  = NEW.custo_total,
                data_emissao = NEW.data_emissao,
                sincronizado_em = NOW()
          WHERE loja_id = NEW.loja_id
            AND numeronfe = NEW.numeronfe
            AND codigobarra = NEW.codigobarra
            AND data_entrada = NEW.data_entrada
            AND COALESCE(fornecedor_cnpj,'') = COALESCE(NEW.fornecedor_cnpj,'');
         IF FOUND THEN RETURN NULL; END IF;
         RETURN NEW;
       END;
       $trg$ LANGUAGE plpgsql;`);

    // View consolidada: avarias internas + devoluções (Pentaho histórico) + devoluções (jrlira-tech recentes)
    // Deduplicação por chave_nfe quando ambas fontes têm o mesmo registro.
    await runMigration(client, '20260506_vw_perdas_consolidadas',
      `CREATE OR REPLACE VIEW vw_perdas_consolidadas AS
         SELECT 'avaria'::text AS origem,
                vh.loja_id,
                vh.codigobarra,
                NULL::varchar AS fornecedor_cnpj,
                NULL::varchar AS fornecedor_nome,
                vh.data_venda::date AS data,
                vh.qtd_vendida AS qtd,
                NULL::numeric AS valor_total,
                NULL::varchar AS chave_nfe
           FROM vendas_historico vh
          WHERE vh.tipo_saida = 'avaria'
         UNION ALL
         SELECT 'devolucao_compra'::text AS origem,
                d.loja_id,
                NULL::varchar AS codigobarra,
                d.fornecedor_cnpj,
                d.fornecedor_nome,
                d.data_devolucao::date AS data,
                NULL::numeric AS qtd,
                d.valor_total,
                d.chave_nfe
           FROM devolucoes_compra_historico d`);
    await runMigration(client, '20260504_pedidos_criado_por_comprador',
      `ALTER TABLE pedidos
         ADD COLUMN IF NOT EXISTS criado_por_comprador VARCHAR(150)`);
    await runMigration(client, '20260504_backfill_criado_por_sug',
      `UPDATE pedidos SET criado_por_comprador = 'sugestao'
         WHERE status='rascunho' AND numero_pedido LIKE 'SUG-%' AND criado_por_comprador IS NULL`);

    // Embalagens vindas de NF-e fornecedor — separadas das embalagens CD (produtos_embalagem)
    await runMigration(client, '20260504_embalagens_fornecedor',
      `CREATE TABLE IF NOT EXISTS embalagens_fornecedor (
         id SERIAL PRIMARY KEY,
         ean VARCHAR(20) NOT NULL,
         descricao VARCHAR(300),
         fornecedor_cnpj VARCHAR(20),
         fornecedor_nome VARCHAR(200),
         qtd_por_caixa INTEGER,
         qtd_sugerida_nfe INTEGER,
         qtd_sugerida_nfe_em TIMESTAMPTZ,
         qtd_sugerida_nfe_nota_id INTEGER,
         qtd_sugerida_nfe_confianca VARCHAR(10),
         status VARCHAR(20) NOT NULL DEFAULT 'pendente_validacao',
         validado_em TIMESTAMPTZ,
         validado_por VARCHAR(150),
         observacao TEXT,
         criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         CHECK (status IN ('pendente_validacao','validado','ignorado'))
       )`);
    await runMigration(client, '20260504_embalagens_fornecedor_uniq',
      `CREATE UNIQUE INDEX IF NOT EXISTS uniq_embalagens_fornecedor
         ON embalagens_fornecedor(ean, COALESCE(fornecedor_cnpj,''))`);
    await runMigration(client, '20260504_idx_embalagens_fornecedor_pendentes',
      `CREATE INDEX IF NOT EXISTS idx_embalagens_fornecedor_pendentes
         ON embalagens_fornecedor(criado_em DESC) WHERE status = 'pendente_validacao'`);
    // Aceita registro sem EAN — chave alternativa por (descricao_normalizada, fornecedor_cnpj)
    await runMigration(client, '20260504_emb_fornecedor_ean_nullable',
      `ALTER TABLE embalagens_fornecedor ALTER COLUMN ean DROP NOT NULL;
       ALTER TABLE embalagens_fornecedor ADD COLUMN IF NOT EXISTS descricao_normalizada VARCHAR(300);
       DROP INDEX IF EXISTS uniq_embalagens_fornecedor;
       CREATE UNIQUE INDEX IF NOT EXISTS uniq_emb_fornecedor_ean
         ON embalagens_fornecedor(ean, COALESCE(fornecedor_cnpj,'')) WHERE ean IS NOT NULL;
       CREATE UNIQUE INDEX IF NOT EXISTS uniq_emb_fornecedor_desc
         ON embalagens_fornecedor(descricao_normalizada, COALESCE(fornecedor_cnpj,''))
         WHERE ean IS NULL AND descricao_normalizada IS NOT NULL`);
    // Log de alterações de embalagem (auditoria)
    await runMigration(client, '20260504_emb_fornecedor_log',
      `CREATE TABLE IF NOT EXISTS embalagens_fornecedor_log (
         id SERIAL PRIMARY KEY,
         embalagem_id INTEGER REFERENCES embalagens_fornecedor(id) ON DELETE SET NULL,
         ean VARCHAR(20),
         descricao TEXT,
         fornecedor_cnpj VARCHAR(20),
         fornecedor_nome VARCHAR(200),
         qtd_anterior INTEGER,
         qtd_novo INTEGER NOT NULL,
         origem VARCHAR(30) NOT NULL,
         nota_id INTEGER,
         item_nota_id INTEGER,
         alterado_por VARCHAR(150),
         alterado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`);
    await runMigration(client, '20260504_idx_emb_fornecedor_log_nota',
      `CREATE INDEX IF NOT EXISTS idx_emb_fornecedor_log_nota
         ON embalagens_fornecedor_log(nota_id)`);

    // Re-classifica status_preco com a nova taxonomia (maior/menor/auditagem) — backfill 1x
    await runMigration(client, '20260504_status_preco_taxonomia',
      `UPDATE itens_nota
          SET status_preco = CASE
            WHEN custo_fabrica IS NULL THEN 'sem_cadastro'
            WHEN ABS(preco_unitario_nota - custo_fabrica) <= 0.01 THEN 'igual'
            WHEN custo_fabrica > 0
              AND ABS(preco_unitario_nota - custo_fabrica) / custo_fabrica > 0.15
              THEN 'auditagem'
            WHEN preco_unitario_nota > custo_fabrica THEN 'maior'
            ELSE 'menor'
          END
        WHERE status_preco IN ('divergente','igual')
          AND preco_unitario_nota IS NOT NULL`);
    await runMigration(client, '20260504_produtos_embalagem_sugestao_nfe',
      `ALTER TABLE produtos_embalagem
         ADD COLUMN IF NOT EXISTS qtd_sugerida_nfe INTEGER,
         ADD COLUMN IF NOT EXISTS qtd_sugerida_nfe_fornecedor VARCHAR(200),
         ADD COLUMN IF NOT EXISTS qtd_sugerida_nfe_em TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS qtd_sugerida_nfe_nota_id INTEGER,
         ADD COLUMN IF NOT EXISTS qtd_sugerida_nfe_confianca VARCHAR(10)`);
    await runMigration(client, '20260503_notas_alertas_notificado',
      `ALTER TABLE notas_alertas
         ADD COLUMN IF NOT EXISTS notificado_em TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS notificacoes_enviadas INTEGER DEFAULT 0`);
    // Tabela de alertas independentes de nota (ex: produto novo no CD)
    await runMigration(client, '20260503_alertas_admin',
      `CREATE TABLE IF NOT EXISTS alertas_admin (
         id SERIAL PRIMARY KEY,
         tipo VARCHAR(50) NOT NULL,
         entidade VARCHAR(50),
         entidade_id VARCHAR(50),
         titulo VARCHAR(200),
         mensagem TEXT,
         criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         resolvido_em TIMESTAMPTZ,
         resolvido_por VARCHAR(150),
         notificado_em TIMESTAMPTZ,
         notificacoes_enviadas INTEGER DEFAULT 0
       )`);
    await runMigration(client, '20260503_idx_alertas_admin_pendentes',
      `CREATE INDEX IF NOT EXISTS idx_alertas_admin_pendentes
         ON alertas_admin(tipo, criado_em) WHERE resolvido_em IS NULL`);
    await runMigration(client, '20260503_uniq_alertas_admin_entidade',
      `CREATE UNIQUE INDEX IF NOT EXISTS uniq_alertas_admin_entidade
         ON alertas_admin(tipo, entidade, entidade_id)
        WHERE resolvido_em IS NULL`);

    // Backfill sem_codigo_barras pra itens existentes (CD com EAN nulo/placeholder = só zeros)
    await runMigration(client, '20260503_backfill_sem_cod_barras',
      `UPDATE itens_nota SET sem_codigo_barras = TRUE
         WHERE (ean_nota IS NULL OR ean_nota = '' OR LTRIM(ean_nota,'0') = '')
           AND nota_id IN (SELECT id FROM notas_entrada WHERE origem = 'cd')`);

    // Normalizar EANs: remove zeros à esquerda em todas as tabelas e cria trigger
    // pra manter normalizado em qualquer INSERT/UPDATE futuro (inclusive via Pentaho).
    await runMigration(client, '20260503_eans_normalizar_dados',
      `UPDATE produtos_embalagem SET ean_principal_cd = NULLIF(LTRIM(ean_principal_cd,'0'),'') WHERE ean_principal_cd ~ '^0';
       UPDATE produtos_embalagem SET ean_principal_jrlira = NULLIF(LTRIM(ean_principal_jrlira,'0'),'') WHERE ean_principal_jrlira ~ '^0';
       UPDATE produtos_embalagem SET ean_sugerido_eco = NULLIF(LTRIM(ean_sugerido_eco,'0'),'') WHERE ean_sugerido_eco ~ '^0';
       UPDATE produtos_externo  SET codigobarra = NULLIF(LTRIM(codigobarra,'0'),'') WHERE codigobarra ~ '^0';
       UPDATE itens_nota        SET ean_nota = NULLIF(LTRIM(ean_nota,'0'),'') WHERE ean_nota ~ '^0';
       UPDATE vendas_historico  SET codigobarra = NULLIF(LTRIM(codigobarra,'0'),'') WHERE codigobarra ~ '^0';
       UPDATE compras_historico SET codigobarra = NULLIF(LTRIM(codigobarra,'0'),'') WHERE codigobarra ~ '^0';`);

    await runMigration(client, '20260503_eans_trigger_func',
      `CREATE OR REPLACE FUNCTION trim_zeros_ean() RETURNS TRIGGER AS $trg$
       BEGIN
         IF TG_TABLE_NAME = 'produtos_embalagem' THEN
           NEW.ean_principal_cd     := NULLIF(LTRIM(COALESCE(NEW.ean_principal_cd,''),'0'),'');
           NEW.ean_principal_jrlira := NULLIF(LTRIM(COALESCE(NEW.ean_principal_jrlira,''),'0'),'');
           NEW.ean_sugerido_eco     := NULLIF(LTRIM(COALESCE(NEW.ean_sugerido_eco,''),'0'),'');
         ELSIF TG_TABLE_NAME = 'produtos_externo' THEN
           NEW.codigobarra := NULLIF(LTRIM(COALESCE(NEW.codigobarra,''),'0'),'');
         ELSIF TG_TABLE_NAME = 'itens_nota' THEN
           NEW.ean_nota := NULLIF(LTRIM(COALESCE(NEW.ean_nota,''),'0'),'');
         ELSIF TG_TABLE_NAME = 'vendas_historico' THEN
           NEW.codigobarra := NULLIF(LTRIM(COALESCE(NEW.codigobarra,''),'0'),'');
         ELSIF TG_TABLE_NAME = 'compras_historico' THEN
           NEW.codigobarra := NULLIF(LTRIM(COALESCE(NEW.codigobarra,''),'0'),'');
         END IF;
         RETURN NEW;
       END;
       $trg$ LANGUAGE plpgsql;`);

    await runMigration(client, '20260503_eans_triggers_tabelas',
      `DROP TRIGGER IF EXISTS trg_norm_ean ON produtos_embalagem;
       CREATE TRIGGER trg_norm_ean BEFORE INSERT OR UPDATE ON produtos_embalagem FOR EACH ROW EXECUTE FUNCTION trim_zeros_ean();
       DROP TRIGGER IF EXISTS trg_norm_ean ON produtos_externo;
       CREATE TRIGGER trg_norm_ean BEFORE INSERT OR UPDATE ON produtos_externo FOR EACH ROW EXECUTE FUNCTION trim_zeros_ean();
       DROP TRIGGER IF EXISTS trg_norm_ean ON itens_nota;
       CREATE TRIGGER trg_norm_ean BEFORE INSERT OR UPDATE ON itens_nota FOR EACH ROW EXECUTE FUNCTION trim_zeros_ean();
       DROP TRIGGER IF EXISTS trg_norm_ean ON vendas_historico;
       CREATE TRIGGER trg_norm_ean BEFORE INSERT OR UPDATE ON vendas_historico FOR EACH ROW EXECUTE FUNCTION trim_zeros_ean();
       DROP TRIGGER IF EXISTS trg_norm_ean ON compras_historico;
       CREATE TRIGGER trg_norm_ean BEFORE INSERT OR UPDATE ON compras_historico FOR EACH ROW EXECUTE FUNCTION trim_zeros_ean();`);

    // Trigger especial pra vendas/compras que faz UPSERT silencioso ao normalizar.
    // Resolve o caso onde Firebird envia 2 variantes do mesmo EAN (com/sem zero) que
    // viram iguais após LTRIM e batem na unique constraint.
    await runMigration(client, '20260504_trim_skip_dup_vendas',
      `CREATE OR REPLACE FUNCTION trim_skip_dup_vendas() RETURNS TRIGGER AS $trg$
       BEGIN
         NEW.codigobarra := NULLIF(LTRIM(COALESCE(NEW.codigobarra,''),'0'),'');
         IF EXISTS (
           SELECT 1 FROM vendas_historico
            WHERE loja_id = NEW.loja_id
              AND codigobarra = NEW.codigobarra
              AND data_venda = NEW.data_venda
         ) THEN
           UPDATE vendas_historico
              SET qtd_vendida = NEW.qtd_vendida, sincronizado_em = NOW()
            WHERE loja_id = NEW.loja_id
              AND codigobarra = NEW.codigobarra
              AND data_venda = NEW.data_venda;
           RETURN NULL;
         END IF;
         RETURN NEW;
       END;
       $trg$ LANGUAGE plpgsql;
       DROP TRIGGER IF EXISTS trg_norm_ean ON vendas_historico;
       CREATE TRIGGER trg_norm_ean BEFORE INSERT ON vendas_historico FOR EACH ROW EXECUTE FUNCTION trim_skip_dup_vendas();`);
    // Re-aplica versão corrigida em ambientes que já tinham a versão buggada (somava qtd)
    await runMigration(client, '20260504_trim_skip_dup_vendas_fix',
      `CREATE OR REPLACE FUNCTION trim_skip_dup_vendas() RETURNS TRIGGER AS $trg$
       BEGIN
         NEW.codigobarra := NULLIF(LTRIM(COALESCE(NEW.codigobarra,''),'0'),'');
         IF EXISTS (
           SELECT 1 FROM vendas_historico
            WHERE loja_id = NEW.loja_id
              AND codigobarra = NEW.codigobarra
              AND data_venda = NEW.data_venda
         ) THEN
           UPDATE vendas_historico
              SET qtd_vendida = NEW.qtd_vendida, sincronizado_em = NOW()
            WHERE loja_id = NEW.loja_id
              AND codigobarra = NEW.codigobarra
              AND data_venda = NEW.data_venda;
           RETURN NULL;
         END IF;
         RETURN NEW;
       END;
       $trg$ LANGUAGE plpgsql;`);

    // produtos_externo: igual — duplicata por (loja, codigobarra) após LTRIM. Faz UPDATE last-wins.
    await runMigration(client, '20260504_trim_skip_dup_prod_ext',
      `CREATE OR REPLACE FUNCTION trim_skip_dup_prod_ext() RETURNS TRIGGER AS $trg$
       BEGIN
         NEW.codigobarra := NULLIF(LTRIM(COALESCE(NEW.codigobarra,''),'0'),'');
         IF NEW.codigobarra IS NOT NULL AND EXISTS (
           SELECT 1 FROM produtos_externo
            WHERE loja_id = NEW.loja_id
              AND codigobarra = NEW.codigobarra
         ) THEN
           UPDATE produtos_externo
              SET descricao = COALESCE(NEW.descricao, descricao),
                  referencia = COALESCE(NEW.referencia, referencia),
                  custoorigem = COALESCE(NEW.custoorigem, custoorigem),
                  custofinal = COALESCE(NEW.custofinal, custofinal),
                  prsugerido = COALESCE(NEW.prsugerido, prsugerido),
                  estdisponivel = COALESCE(NEW.estdisponivel, estdisponivel),
                  margemlucro = COALESCE(NEW.margemlucro, margemlucro),
                  qtdeembalagem = COALESCE(NEW.qtdeembalagem, qtdeembalagem),
                  ultima_venda = COALESCE(NEW.ultima_venda, ultima_venda),
                  produtoprincipal = COALESCE(NEW.produtoprincipal, produtoprincipal),
                  codigo_interno = COALESCE(NEW.codigo_interno, codigo_interno),
                  sincronizado_em = NOW()
            WHERE loja_id = NEW.loja_id AND codigobarra = NEW.codigobarra;
           RETURN NULL;
         END IF;
         RETURN NEW;
       END;
       $trg$ LANGUAGE plpgsql;
       DROP TRIGGER IF EXISTS trg_norm_ean ON produtos_externo;
       CREATE TRIGGER trg_norm_ean BEFORE INSERT ON produtos_externo FOR EACH ROW EXECUTE FUNCTION trim_skip_dup_prod_ext();`);

    await runMigration(client, '20260504_trim_skip_dup_compras',
      `CREATE OR REPLACE FUNCTION trim_skip_dup_compras() RETURNS TRIGGER AS $trg$
       BEGIN
         NEW.codigobarra := NULLIF(LTRIM(COALESCE(NEW.codigobarra,''),'0'),'');
         IF NEW.codigobarra IS NOT NULL AND EXISTS (
           SELECT 1 FROM compras_historico
            WHERE loja_id = NEW.loja_id
              AND codigobarra = NEW.codigobarra
              AND numeronfe = NEW.numeronfe
         ) THEN
           RETURN NULL;
         END IF;
         RETURN NEW;
       END;
       $trg$ LANGUAGE plpgsql;
       DROP TRIGGER IF EXISTS trg_norm_ean ON compras_historico;
       CREATE TRIGGER trg_norm_ean BEFORE INSERT ON compras_historico FOR EACH ROW EXECUTE FUNCTION trim_skip_dup_compras();`);

    // Lotes contados na conferência de transferência (cx + un + validade por item)
    await runMigration(client, '20260503_lotes_conferidos_transf',
      `CREATE TABLE IF NOT EXISTS lotes_conferidos (
         id SERIAL PRIMARY KEY,
         item_id INTEGER NOT NULL REFERENCES itens_nota(id) ON DELETE CASCADE,
         nota_id INTEGER NOT NULL REFERENCES notas_entrada(id) ON DELETE CASCADE,
         lote_idx INTEGER NOT NULL DEFAULT 0,
         qtd_caixas NUMERIC(14,3) DEFAULT 0,
         qtd_unidades NUMERIC(14,3) DEFAULT 0,
         qtd_total NUMERIC(14,3) DEFAULT 0,
         validade DATE,
         conferido_por VARCHAR(150),
         conferido_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`);
    await runMigration(client, '20260503_idx_lotes_conferidos_item',
      `CREATE INDEX IF NOT EXISTS idx_lotes_conferidos_item ON lotes_conferidos(item_id)`);
    await runMigration(client, '20260503_idx_lotes_conferidos_nota',
      `CREATE INDEX IF NOT EXISTS idx_lotes_conferidos_nota ON lotes_conferidos(nota_id)`);

    // Divergências detectadas na conferência de transferência (falta/sobra)
    await runMigration(client, '20260503_auditagem_divergencias',
      `CREATE TABLE IF NOT EXISTS auditagem_divergencias (
         id SERIAL PRIMARY KEY,
         nota_id INTEGER NOT NULL REFERENCES notas_entrada(id) ON DELETE CASCADE,
         item_id INTEGER NOT NULL REFERENCES itens_nota(id) ON DELETE CASCADE,
         loja_id INTEGER NOT NULL,
         cd_pro_codi VARCHAR(10),
         descricao TEXT,
         ean_nota VARCHAR(20),
         qtd_esperada NUMERIC(14,3) NOT NULL,
         qtd_contada NUMERIC(14,3) NOT NULL,
         diferenca NUMERIC(14,3) GENERATED ALWAYS AS (qtd_contada - qtd_esperada) STORED,
         valor_unitario NUMERIC(14,4),
         valor_total_diferenca NUMERIC(14,2),
         status VARCHAR(30) NOT NULL DEFAULT 'pendente',
         observacao TEXT,
         resolvido_em TIMESTAMPTZ,
         resolvido_por VARCHAR(150),
         numero_nf_devolucao VARCHAR(20),
         criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         CHECK (status IN ('pendente','resolvida_cd','devolvida_loja','baixada'))
       )`);
    await runMigration(client, '20260503_idx_auditagem_divergencias_pendentes',
      `CREATE INDEX IF NOT EXISTS idx_auditagem_divergencias_pendentes
         ON auditagem_divergencias(loja_id, criado_em) WHERE status = 'pendente'`);
    await runMigration(client, '20260503_idx_auditagem_divergencias_nota',
      `CREATE INDEX IF NOT EXISTS idx_auditagem_divergencias_nota ON auditagem_divergencias(nota_id)`);

    // Validades em risco — calculadas no momento da conferência
    await runMigration(client, '20260503_validades_em_risco',
      `CREATE TABLE IF NOT EXISTS validades_em_risco (
         id SERIAL PRIMARY KEY,
         nota_id INTEGER NOT NULL REFERENCES notas_entrada(id) ON DELETE CASCADE,
         item_id INTEGER NOT NULL REFERENCES itens_nota(id) ON DELETE CASCADE,
         loja_id INTEGER NOT NULL,
         cd_pro_codi VARCHAR(10),
         descricao TEXT,
         ean VARCHAR(20),
         validade DATE NOT NULL,
         dias_ate_vencer INTEGER,
         qtd_recebida_lote NUMERIC(14,3),
         estoque_atual NUMERIC(14,3),
         estoque_pos_recebimento NUMERIC(14,3),
         vendas_media_dia NUMERIC(14,4),
         qtd_consumivel_ate_vencer NUMERIC(14,3),
         qtd_em_risco NUMERIC(14,3),
         valor_unitario NUMERIC(14,4),
         valor_em_risco NUMERIC(14,2),
         motivo_risco VARCHAR(50),
         status VARCHAR(30) NOT NULL DEFAULT 'pendente',
         decidido_em TIMESTAMPTZ,
         decidido_por VARCHAR(150),
         observacao TEXT,
         criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         CHECK (status IN ('pendente','liberado','devolucao'))
       )`);
    await runMigration(client, '20260503_idx_validades_pendentes',
      `CREATE INDEX IF NOT EXISTS idx_validades_pendentes
         ON validades_em_risco(loja_id, criado_em) WHERE status = 'pendente'`);
    await runMigration(client, '20260503_idx_validades_nota',
      `CREATE INDEX IF NOT EXISTS idx_validades_nota ON validades_em_risco(nota_id)`);
    await runMigration(client, '20260503_validades_em_caixas',
      `ALTER TABLE validades_em_risco
         ADD COLUMN IF NOT EXISTS qtd_embalagem NUMERIC(10,3),
         ADD COLUMN IF NOT EXISTS qtd_em_risco_caixas INTEGER`);

    // Devoluções (CD ou Fornecedor) — geradas a partir de validades em risco ou divergências
    await runMigration(client, '20260503_devolucoes',
      `CREATE TABLE IF NOT EXISTS devolucoes (
         id SERIAL PRIMARY KEY,
         nota_id INTEGER NOT NULL REFERENCES notas_entrada(id) ON DELETE CASCADE,
         loja_id INTEGER NOT NULL,
         tipo VARCHAR(20) NOT NULL,
         destinatario_cnpj VARCHAR(20),
         destinatario_nome VARCHAR(200),
         motivo VARCHAR(50),
         valor_total NUMERIC(14,2) DEFAULT 0,
         status VARCHAR(20) NOT NULL DEFAULT 'aguardando',
         observacao TEXT,
         xml_chave_nfe VARCHAR(50),
         xml_numero_nf VARCHAR(20),
         xml_data_emissao DATE,
         xml_anexado_em TIMESTAMPTZ,
         xml_anexado_por VARCHAR(150),
         xml_content TEXT,
         enviada_em TIMESTAMPTZ,
         enviada_por VARCHAR(150),
         criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         criado_por VARCHAR(150),
         CHECK (status IN ('aguardando','enviada','concluida','cancelada'))
       )`);
    await runMigration(client, '20260503_devolucoes_itens',
      `CREATE TABLE IF NOT EXISTS devolucoes_itens (
         id SERIAL PRIMARY KEY,
         devolucao_id INTEGER NOT NULL REFERENCES devolucoes(id) ON DELETE CASCADE,
         item_nota_id INTEGER REFERENCES itens_nota(id) ON DELETE SET NULL,
         cd_pro_codi VARCHAR(10),
         ean VARCHAR(20),
         descricao TEXT,
         qtd_caixas INTEGER DEFAULT 0,
         qtd_unidades NUMERIC(14,3) DEFAULT 0,
         qtd_total NUMERIC(14,3) DEFAULT 0,
         valor_unitario NUMERIC(14,4),
         valor_total NUMERIC(14,2),
         origem_tipo VARCHAR(30),
         origem_id INTEGER
       )`);
    await runMigration(client, '20260503_idx_devolucoes_pendentes',
      `CREATE INDEX IF NOT EXISTS idx_devolucoes_pendentes
         ON devolucoes(loja_id, criado_em) WHERE status = 'aguardando'`);
    await runMigration(client, '20260503_idx_devolucoes_nota',
      `CREATE INDEX IF NOT EXISTS idx_devolucoes_nota ON devolucoes(nota_id)`);
    await runMigration(client, '20260503_idx_devolucoes_itens_dev',
      `CREATE INDEX IF NOT EXISTS idx_devolucoes_itens_dev ON devolucoes_itens(devolucao_id)`);
    await runMigration(client, '20260503_devolucoes_pdf_notif',
      `ALTER TABLE devolucoes
         ADD COLUMN IF NOT EXISTS pdf_content BYTEA,
         ADD COLUMN IF NOT EXISTS pdf_mime VARCHAR(50),
         ADD COLUMN IF NOT EXISTS pdf_origem VARCHAR(20),
         ADD COLUMN IF NOT EXISTS vendedor_notificado_em TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS vendedor_notificacao_canais TEXT,
         ADD COLUMN IF NOT EXISTS vendedor_notificacao_status VARCHAR(20)`);

    // Fase 2 fluxo CD: timestamps e responsáveis por etapa
    await runMigration(client, '20260503_cd_fluxo_etapas',
      `ALTER TABLE notas_entrada
         ADD COLUMN IF NOT EXISTS recebida_em TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS recebida_por VARCHAR(150),
         ADD COLUMN IF NOT EXISTS liberada_em TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS liberada_por VARCHAR(150),
         ADD COLUMN IF NOT EXISTS conferida_em TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS conferida_por VARCHAR(150),
         ADD COLUMN IF NOT EXISTS conferida_com_divergencia BOOLEAN DEFAULT FALSE,
         ADD COLUMN IF NOT EXISTS validada_por VARCHAR(150)`);

    // Embalagens: cadastro do CD + auditoria via app
    await runMigration(client, '20260503_produtos_embalagem',
      `CREATE TABLE IF NOT EXISTS produtos_embalagem (
         mat_codi VARCHAR(10) PRIMARY KEY,
         descricao_atual TEXT,
         descricao_anterior TEXT,
         unidade VARCHAR(10),
         qtd_embalagem INTEGER,
         qtd_sugerida INTEGER,
         confianca_parser VARCHAR(10),
         status VARCHAR(40) NOT NULL DEFAULT 'pendente_validacao',
         ativo_no_cd BOOLEAN DEFAULT TRUE,
         observacao TEXT,
         validado_em TIMESTAMPTZ,
         validado_por VARCHAR(150),
         criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`);
    await runMigration(client, '20260503_produtos_embalagem_idx_status',
      `CREATE INDEX IF NOT EXISTS idx_produtos_embalagem_status ON produtos_embalagem(status)`);
    await runMigration(client, '20260509_produtos_embalagem_idx_ean_cd',
      `CREATE INDEX IF NOT EXISTS idx_produtos_embalagem_ean_cd ON produtos_embalagem(ean_principal_cd) WHERE ean_principal_cd IS NOT NULL`);
    await runMigration(client, '20260509_itens_nota_idx_cd_pro_codi',
      `CREATE INDEX IF NOT EXISTS idx_itens_nota_cd_pro_codi ON itens_nota(cd_pro_codi) WHERE cd_pro_codi IS NOT NULL`);
    await runMigration(client, '20260503_produtos_embalagem_eans',
      `ALTER TABLE produtos_embalagem
         ADD COLUMN IF NOT EXISTS ean_principal_jrlira VARCHAR(20),
         ADD COLUMN IF NOT EXISTS ean_principal_cd VARCHAR(20),
         ADD COLUMN IF NOT EXISTS ean_cd_synced_em TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS ean_status VARCHAR(20),
         ADD COLUMN IF NOT EXISTS ean_validado_em TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS ean_validado_por VARCHAR(150)`);
    await runMigration(client, '20260503_produtos_embalagem_idx_ean',
      `CREATE INDEX IF NOT EXISTS idx_produtos_embalagem_ean_status ON produtos_embalagem(ean_status)`);
    // Busca de sugestões por similaridade de descrição (pg_trgm)
    await runMigration(client, '20260503_pg_trgm',
      `CREATE EXTENSION IF NOT EXISTS pg_trgm`);
    await runMigration(client, '20260503_idx_produtos_externo_desc_trgm',
      `CREATE INDEX IF NOT EXISTS idx_produtos_externo_desc_trgm
         ON produtos_externo USING gin(descricao gin_trgm_ops)`);
    await runMigration(client, '20260503_produtos_embalagem_sugestao',
      `ALTER TABLE produtos_embalagem
         ADD COLUMN IF NOT EXISTS ean_sugerido_eco VARCHAR(20),
         ADD COLUMN IF NOT EXISTS ean_sugerido_score NUMERIC(5,3),
         ADD COLUMN IF NOT EXISTS ean_sugerido_em TIMESTAMPTZ`);
    await runMigration(client, '20260503_produtos_embalagem_aponta_para',
      `ALTER TABLE produtos_embalagem
         ADD COLUMN IF NOT EXISTS ean_aponta_para VARCHAR(20),
         ADD COLUMN IF NOT EXISTS ean_duplicado_count INTEGER DEFAULT 0`);
    await runMigration(client, '20260503_produtos_externo_codigo_interno',
      `ALTER TABLE produtos_externo ADD COLUMN IF NOT EXISTS codigo_interno VARCHAR(20)`);
    await runMigration(client, '20260503_idx_produtos_externo_codigo_interno',
      `CREATE INDEX IF NOT EXISTS idx_produtos_externo_codigo_interno ON produtos_externo(codigo_interno)`);

    // Permite mesmo email em fornecedores diferentes (vendedor multi-fornecedor)
    await runMigration(client, '20260501_vendedores_email_drop_unique',
      `ALTER TABLE vendedores DROP CONSTRAINT IF EXISTS vendedores_email_key`);
    // Bloqueia mesmo CPF + CNPJ ativo (vendedor não pode ter 2 cadastros no mesmo fornecedor)
    await runMigration(client, '20260501_vendedores_uniq_cpf_cnpj',
      `CREATE UNIQUE INDEX IF NOT EXISTS uniq_vend_cpf_cnpj
       ON vendedores (
         REGEXP_REPLACE(cpf,'\\D','','g'),
         REGEXP_REPLACE(fornecedor_cnpj,'\\D','','g')
       ) WHERE status IN ('pendente','aprovado','aguardando_cadastro')`);
    await runMigration(client, '20260501_vendedores_fk_set_null',
      `DO $mig$
       BEGIN
         IF EXISTS (
           SELECT 1 FROM pg_constraint
           WHERE conrelid = 'vendedores'::regclass AND contype = 'f'
             AND confrelid = 'fornecedores'::regclass
             AND confdeltype = 'c'
         ) THEN
           ALTER TABLE vendedores DROP CONSTRAINT IF EXISTS vendedores_fornecedor_id_fkey;
           ALTER TABLE vendedores ADD CONSTRAINT vendedores_fornecedor_id_fkey
             FOREIGN KEY (fornecedor_id) REFERENCES fornecedores(id) ON DELETE SET NULL;
         END IF;
       END $mig$`);
    // Backfill: vendedores existentes sem CNPJ snapshot
    await client.query(`
      UPDATE vendedores v SET fornecedor_cnpj = f.cnpj
      FROM fornecedores f
      WHERE v.fornecedor_id = f.id AND v.fornecedor_cnpj IS NULL
    `).catch(err => console.error('[backfill cnpj]', err.message));

    // Eventos enriquecidos (timeline única com colunas opcionais por tipo)
    await runMigration(client, '20260501_eventos_loja_origem_destino',
      `ALTER TABLE funcionario_eventos
         ADD COLUMN IF NOT EXISTS loja_origem_id INTEGER,
         ADD COLUMN IF NOT EXISTS loja_destino_id INTEGER`);
    await runMigration(client, '20260501_eventos_atestado',
      `ALTER TABLE funcionario_eventos
         ADD COLUMN IF NOT EXISTS cid VARCHAR(20),
         ADD COLUMN IF NOT EXISTS cid_descricao TEXT,
         ADD COLUMN IF NOT EXISTS medico_nome VARCHAR(200),
         ADD COLUMN IF NOT EXISTS medico_crm VARCHAR(50),
         ADD COLUMN IF NOT EXISTS clinica VARCHAR(200),
         ADD COLUMN IF NOT EXISTS dias_afastado INTEGER`);
    await runMigration(client, '20260501_eventos_advertencia_atraso',
      `ALTER TABLE funcionario_eventos
         ADD COLUMN IF NOT EXISTS gravidade INTEGER,
         ADD COLUMN IF NOT EXISTS motivo_categoria VARCHAR(50),
         ADD COLUMN IF NOT EXISTS minutos INTEGER`);
    await runMigration(client, '20260501_eventos_anexo',
      `ALTER TABLE funcionario_eventos
         ADD COLUMN IF NOT EXISTS anexo BYTEA,
         ADD COLUMN IF NOT EXISTS anexo_mime VARCHAR(100),
         ADD COLUMN IF NOT EXISTS anexo_nome VARCHAR(200)`);
    await runMigration(client, '20260501_eventos_auditoria_cargo',
      `ALTER TABLE funcionario_eventos
         ADD COLUMN IF NOT EXISTS editado_por VARCHAR(150),
         ADD COLUMN IF NOT EXISTS editado_em TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS cargo_anterior VARCHAR(100),
         ADD COLUMN IF NOT EXISTS cargo_novo VARCHAR(100),
         ADD COLUMN IF NOT EXISTS salario_anterior NUMERIC(12,2),
         ADD COLUMN IF NOT EXISTS salario_novo NUMERIC(12,2)`);
    await runMigration(client, '20260501_idx_eventos_funcionario',
      'CREATE INDEX IF NOT EXISTS idx_eventos_funcionario ON funcionario_eventos(funcionario_id)');
    await runMigration(client, '20260501_idx_eventos_tipo_data',
      'CREATE INDEX IF NOT EXISTS idx_eventos_tipo_data ON funcionario_eventos(tipo, data_inicio)');

    // Períodos de ponto (fechamento mensal)
    await runMigration(client, '20260501_ponto_periodos',
      `CREATE TABLE IF NOT EXISTS ponto_periodos (
         id SERIAL PRIMARY KEY,
         data_inicio DATE NOT NULL,
         data_fim DATE NOT NULL,
         fechado BOOLEAN DEFAULT FALSE,
         fechado_por VARCHAR(150),
         fechado_em TIMESTAMPTZ,
         UNIQUE(data_inicio, data_fim)
       )`);

    // Controle de alertas RH enviados (anti-spam)
    await runMigration(client, '20260501_rh_alertas_enviados',
      `CREATE TABLE IF NOT EXISTS rh_alertas_enviados (
         id SERIAL PRIMARY KEY,
         funcionario_id INTEGER REFERENCES funcionarios(id) ON DELETE CASCADE,
         tipo VARCHAR(50) NOT NULL,
         destinatario VARCHAR(200),
         enviado_em TIMESTAMPTZ DEFAULT NOW()
       )`);
    await runMigration(client, '20260501_idx_rh_alertas',
      'CREATE INDEX IF NOT EXISTS idx_rh_alertas ON rh_alertas_enviados(funcionario_id, tipo, enviado_em)');

    // Catálogo de CID-10 (subset)
    await runMigration(client, '20260501_cid_codigos',
      `CREATE TABLE IF NOT EXISTS cid_codigos (
         codigo VARCHAR(20) PRIMARY KEY,
         descricao TEXT NOT NULL
       )`);
    // Popula CID-10 se vazio
    const { rows: cidCount } = await client.query('SELECT COUNT(*) FROM cid_codigos');
    if (parseInt(cidCount[0].count) === 0) {
      const cids = require('./src/data/cid10');
      const codigos = cids.map(c => c[0]);
      const descricoes = cids.map(c => c[1]);
      await client.query(
        `INSERT INTO cid_codigos (codigo, descricao)
         SELECT * FROM UNNEST($1::text[], $2::text[])
         ON CONFLICT (codigo) DO NOTHING`,
        [codigos, descricoes]
      );
      console.log(`[DB] CID-10: ${cids.length} códigos populados`);
    }

    // Catálogo de tipos de licença / afastamento (CLT, INSS, convenções)
    await runMigration(client, '20260502_tipos_licenca',
      `CREATE TABLE IF NOT EXISTS tipos_licenca (
         codigo VARCHAR(50) PRIMARY KEY,
         nome VARCHAR(150) NOT NULL,
         dias_padrao INTEGER,
         dias_max INTEGER,
         exige_atestado BOOLEAN DEFAULT FALSE,
         remunerada BOOLEAN DEFAULT TRUE,
         status_funcionario VARCHAR(40),
         base_legal VARCHAR(300),
         descricao TEXT,
         ordem INTEGER DEFAULT 0
       )`);
    // Seed/upsert idempotente
    await client.query(`
      INSERT INTO tipos_licenca
        (codigo, nome, dias_padrao, dias_max, exige_atestado, remunerada, status_funcionario, base_legal, descricao, ordem)
      VALUES
        ('licenca_maternidade','Licença-maternidade',120,180,TRUE,TRUE,'LICENSA MATERNIDADE',
         'CF art. 7º XVIII / CLT art. 392 / Lei 11.770 (Empresa Cidadã)',
         '120 dias após o parto, podendo ser estendida para 180 dias em empresas do Programa Empresa Cidadã. Estabilidade no emprego e remuneração integral.',1),
        ('licenca_paternidade','Licença-paternidade',5,20,FALSE,TRUE,'ATIVO',
         'CF art. 7º XIX / Lei 13.257 (Empresa Cidadã)',
         '5 dias após o nascimento. Aplicável também a adoção e guarda compartilhada. Pode ser estendida em empresas do Programa Empresa Cidadã.',2),
        ('atestado_medico','Atestado médico (até 15 dias — empresa)',NULL,15,TRUE,TRUE,'ATESTADO',
         'CLT art. 473 / Lei 8.213 art. 60',
         'Primeiros 15 dias pagos pela empresa. A partir do 16º dia o trabalhador é encaminhado ao INSS (auxílio-doença).',3),
        ('afastamento_inss','Afastamento INSS (a partir do 16º dia)',NULL,NULL,TRUE,FALSE,'INSS',
         'Lei 8.213 art. 60',
         'A partir do 16º dia de afastamento por doença, o pagamento passa a ser do INSS, mediante perícia.',4),
        ('acidente_trabalho','Acidente de trabalho (B91)',NULL,NULL,TRUE,TRUE,'INSS',
         'Lei 8.213 art. 19 / CLT art. 118',
         'Pagamento integral pela empresa nos primeiros 15 dias; INSS a partir do 16º. Estabilidade de 12 meses após o retorno.',5),
        ('licenca_doenca_familia','Licença por doença na família',NULL,NULL,TRUE,TRUE,'AFASTADO',
         'Convenção coletiva / política interna',
         'Cuidado de familiar doente. Não prevista diretamente pela CLT — prazo e remuneração dependem de acordo coletivo ou política da empresa.',6),
        ('licenca_casamento','Licença-casamento (gala)',3,3,FALSE,TRUE,'ATIVO',
         'CLT art. 473 II',
         '3 dias consecutivos após o casamento civil, contados a partir do primeiro dia útil. Empresa pode exigir comprovação.',7),
        ('licenca_luto','Licença-luto (nojo)',2,2,TRUE,TRUE,'ATIVO',
         'CLT art. 473 I',
         '2 dias consecutivos por falecimento de cônjuge/companheiro, pais/padrasto/madrasta, filhos ou irmãos. Comprovação por atestado de óbito.',8),
        ('licenca_doacao_sangue','Doação de sangue',1,1,TRUE,TRUE,'ATIVO',
         'CLT art. 473 IV',
         '1 dia de licença remunerada por ano, com comprovação de doação voluntária.',9),
        ('licenca_servico_eleitoral_juri','Serviço eleitoral / júri',NULL,NULL,TRUE,TRUE,'ATIVO',
         'CLT art. 473 VI / Lei 9.504 art. 98',
         'Mesário, trabalho eleitoral ou júri — licença remunerada durante o período de serviço. Empresa não pode descontar. Comprovante oficial obrigatório.',10)
      ON CONFLICT (codigo) DO UPDATE SET
        nome = EXCLUDED.nome,
        dias_padrao = EXCLUDED.dias_padrao,
        dias_max = EXCLUDED.dias_max,
        exige_atestado = EXCLUDED.exige_atestado,
        remunerada = EXCLUDED.remunerada,
        status_funcionario = EXCLUDED.status_funcionario,
        base_legal = EXCLUDED.base_legal,
        descricao = EXCLUDED.descricao,
        ordem = EXCLUDED.ordem
    `).catch(err => console.error('[seed tipos_licenca]', err.message));

    // Notificações in-app (substitui WhatsApp em vários fluxos)
    await runMigration(client, '20260506_notificacoes',
      `CREATE TABLE IF NOT EXISTS notificacoes (
         id SERIAL PRIMARY KEY,
         destinatario_tipo VARCHAR(20) NOT NULL,
         destinatario_id INTEGER NOT NULL,
         tipo VARCHAR(40) NOT NULL,
         titulo VARCHAR(120) NOT NULL,
         corpo TEXT,
         url VARCHAR(200),
         lida_em TIMESTAMPTZ,
         criado_em TIMESTAMPTZ DEFAULT NOW()
       )`);
    await runMigration(client, '20260506_idx_notificacoes_dest',
      `CREATE INDEX IF NOT EXISTS idx_notificacoes_dest
         ON notificacoes (destinatario_tipo, destinatario_id, lida_em NULLS FIRST, criado_em DESC)`);

    console.log('[DB] Tabelas inicializadas');
  } finally {
    client.release();
  }
}

// Middleware global de erro — captura exceções não tratadas das rotas
app.use((err, req, res, next) => {
  console.error(`[erro] ${req.method} ${req.path}:`, err.message);
  if (res.headersSent) return next(err);
  res.status(err.status || 500).json({ erro: err.message || 'Erro interno' });
});

const PORT = process.env.PORT || 3001;
let server;

initDB().then(() => {
  server = app.listen(PORT, () => console.log(`jrlira.tech rodando na porta ${PORT}`));

  // Job diário: limpa anexos de eventos de funcionários demitidos há 30+ dias
  const limparAnexos = async () => {
    try {
      const { rows } = await pool.query(
        `UPDATE funcionario_eventos
           SET anexo = NULL, anexo_mime = NULL, anexo_nome = NULL
         WHERE anexo IS NOT NULL
           AND funcionario_id IN (
             SELECT id FROM funcionarios
             WHERE status = 'DESLIGADO'
               AND data_demissao IS NOT NULL
               AND data_demissao < CURRENT_DATE - INTERVAL '30 days'
           )
         RETURNING id`
      );
      if (rows.length) console.log(`[cleanup] ${rows.length} anexos removidos (demitidos há 30+ dias)`);
    } catch (err) { console.error('[cleanup] erro:', err.message); }
  };
  setTimeout(limparAnexos, 60 * 1000); // 1 min após startup
  setInterval(limparAnexos, 24 * 60 * 60 * 1000); // diário

  // Job diário: gera eventos de aniversário e tempo de casa
  const reconhecimento = async () => {
    try {
      const a = await pool.query(
        `INSERT INTO funcionario_eventos (funcionario_id, tipo, data_inicio, descricao, criado_por)
         SELECT id, 'aniversario', CURRENT_DATE, 'Feliz aniversário!', 'sistema'
         FROM funcionarios
         WHERE status = 'ATIVO' AND data_nascimento IS NOT NULL
           AND EXTRACT(MONTH FROM data_nascimento) = EXTRACT(MONTH FROM CURRENT_DATE)
           AND EXTRACT(DAY FROM data_nascimento) = EXTRACT(DAY FROM CURRENT_DATE)
           AND NOT EXISTS (
             SELECT 1 FROM funcionario_eventos
             WHERE funcionario_id = funcionarios.id AND tipo = 'aniversario'
               AND data_inicio = CURRENT_DATE
           )
         RETURNING id`
      );
      const t = await pool.query(
        `INSERT INTO funcionario_eventos (funcionario_id, tipo, data_inicio, descricao, criado_por)
         SELECT id, 'tempo_de_casa', CURRENT_DATE,
                EXTRACT(YEAR FROM AGE(CURRENT_DATE, data_admissao))::int || ' anos de casa', 'sistema'
         FROM funcionarios
         WHERE status = 'ATIVO' AND data_admissao IS NOT NULL
           AND EXTRACT(MONTH FROM data_admissao) = EXTRACT(MONTH FROM CURRENT_DATE)
           AND EXTRACT(DAY FROM data_admissao) = EXTRACT(DAY FROM CURRENT_DATE)
           AND EXTRACT(YEAR FROM AGE(CURRENT_DATE, data_admissao))::int IN (1, 5, 10, 15, 20, 25)
           AND NOT EXISTS (
             SELECT 1 FROM funcionario_eventos
             WHERE funcionario_id = funcionarios.id AND tipo = 'tempo_de_casa'
               AND data_inicio = CURRENT_DATE
           )
         RETURNING id`
      );
      if (a.rows.length || t.rows.length) {
        console.log(`[reconhecimento] aniversários: ${a.rows.length} | tempo de casa: ${t.rows.length}`);
      }
    } catch (err) { console.error('[reconhecimento] erro:', err.message); }
  };
  setTimeout(reconhecimento, 90 * 1000); // 1.5 min após startup
  setInterval(reconhecimento, 24 * 60 * 60 * 1000); // diário

  // Job semanal: envia alerta por email aos RH/admin sobre funcionários em risco e alerta INSS
  const alertaSemanal = async () => {
    try {
      const { enviarEmail } = require('./src/mailer');

      // Alerta INSS — 14+ dias de atestado em 60d
      const { rows: inss } = await pool.query(
        `SELECT f.id, f.matricula, f.nome, f.loja_id,
                COALESCE(SUM(GREATEST(1, COALESCE(e.dias_afastado, (e.data_fim - e.data_inicio + 1)))), 0)::int AS dias
         FROM funcionarios f
         JOIN funcionario_eventos e ON e.funcionario_id = f.id
         WHERE f.status NOT IN ('DESLIGADO')
           AND e.tipo IN ('atestado_medico','licenca_maternidade','afastamento_inss','acidente_trabalho')
           AND e.data_inicio >= CURRENT_DATE - INTERVAL '60 days'
         GROUP BY f.id, f.matricula, f.nome, f.loja_id
         HAVING COALESCE(SUM(GREATEST(1, COALESCE(e.dias_afastado, (e.data_fim - e.data_inicio + 1)))), 0) >= 14
         ORDER BY dias DESC`
      );

      if (!inss.length) return;

      // Destinatários: usuários admin + rh com email cadastrado
      const { rows: dests } = await pool.query(
        `SELECT u.email, u.nome, u.perfil, u.lojas_ids
         FROM rh_usuarios u
         WHERE u.ativo = TRUE AND u.email IS NOT NULL AND u.email <> ''
           AND u.perfil IN ('admin','rh')`
      );

      const LOJAS_NOMES = { 1:'ECONOMICO',2:'BR',3:'JOAO PESSOA',4:'FLORESTA',5:'SAO JOSE',6:'SANTAREM' };

      for (const d of dests) {
        // Filtra funcionários por lojas do destinatário (se RH)
        const filtrarLoja = (f) => {
          if (d.perfil === 'admin') return true;
          if (!d.lojas_ids?.length) return false;
          return d.lojas_ids.includes(f.loja_id);
        };
        const inssVis = inss.filter(filtrarLoja);
        if (!inssVis.length) continue;

        // Anti-spam: não envia se já enviou nos últimos 7 dias para os mesmos funcionários
        const idsInss = inssVis.map(f => f.id);
        const { rows: jaEnv } = await pool.query(
          `SELECT funcionario_id FROM rh_alertas_enviados
           WHERE tipo = 'inss' AND destinatario = $1 AND funcionario_id = ANY($2)
             AND enviado_em >= CURRENT_DATE - INTERVAL '7 days'`,
          [d.email, idsInss]
        );
        const jaSet = new Set(jaEnv.map(j => j.funcionario_id));
        const novos = inssVis.filter(f => !jaSet.has(f.id));
        if (!novos.length) continue;

        const linhas = novos.map(f =>
          `<tr><td>${f.matricula}</td><td>${f.nome}</td><td>${LOJAS_NOMES[f.loja_id] || f.loja_id}</td><td style="text-align:right;color:#dc2626;font-weight:700">${f.dias} dias</td></tr>`
        ).join('');
        const html = `
          <h2 style="color:#dc2626">⚠ Alerta INSS — encaminhamento recomendado</h2>
          <p>Os seguintes funcionários atingiram ≥14 dias de atestado nos últimos 60 dias:</p>
          <table border="1" cellpadding="8" style="border-collapse:collapse;font-family:Arial">
            <thead style="background:#f1f5f9"><tr><th>Matrícula</th><th>Nome</th><th>Loja</th><th>Dias</th></tr></thead>
            <tbody>${linhas}</tbody>
          </table>
          <p style="font-size:12px;color:#64748b">Acesse o painel: <a href="https://jrliratech-production.up.railway.app/rh-painel">jrliratech-production.up.railway.app/rh-painel</a></p>
        `;
        await enviarEmail(d.email, '[JR Lira RH] Alerta INSS — funcionários para encaminhamento', html);

        // Marca enviado
        for (const f of novos) {
          await pool.query(
            `INSERT INTO rh_alertas_enviados (funcionario_id, tipo, destinatario) VALUES ($1, 'inss', $2)`,
            [f.id, d.email]
          );
        }
        console.log(`[alerta-rh] enviado para ${d.email}: ${novos.length} func. INSS`);
      }
    } catch (err) { console.error('[alerta-rh] erro:', err.message); }
  };
  setTimeout(alertaSemanal, 120 * 1000); // 2 min após startup
  setInterval(alertaSemanal, 7 * 24 * 60 * 60 * 1000); // semanal

  // Lembretes de faturamento (seg-sex 08, 12 e 16h) — via email
  const HORARIOS_LEMBRETE = [8, 12, 16];
  let ultimaHoraExecutada = null;

  async function lembretesFaturamento() {
    const agora = new Date();
    const dow = agora.getDay();
    const hora = agora.getHours();
    if (dow === 0 || dow === 6) return; // sab/dom
    if (!HORARIOS_LEMBRETE.includes(hora)) return;
    const chave = `${agora.toDateString()}-${hora}`;
    if (ultimaHoraExecutada === chave) return;
    ultimaHoraExecutada = chave;

    try {
      const { enviarEmail } = require('./src/mailer');
      const { rows } = await pool.query(`
        SELECT v.id AS vendedor_id, v.nome AS vendedor_nome, v.email,
               array_agg(p.numero_pedido ORDER BY p.validado_em) AS pedidos,
               array_agg(EXTRACT(EPOCH FROM (NOW() - p.validado_em))/3600 ORDER BY p.validado_em) AS horas_decorridas
        FROM pedidos p
        JOIN vendedores v ON v.id = p.vendedor_id
        WHERE p.status = 'validado'
          AND p.faturado_em IS NULL
          AND v.email IS NOT NULL AND v.email <> ''
        GROUP BY v.id, v.nome, v.email
      `);
      for (const r of rows) {
        const pedidos = r.pedidos.slice(0, 10);
        const linhas = pedidos.map((p, i) => `<li><b>${p}</b> — há ${Math.round(r.horas_decorridas[i])}h</li>`);
        const extras = r.pedidos.length > 10 ? `<p>... e mais ${r.pedidos.length - 10} pedido(s)</p>` : '';
        const html = `<p>Olá, <b>${r.vendedor_nome}</b>!</p>
<p>Você tem pedido(s) validado(s) aguardando faturamento:</p>
<ul>${linhas.join('')}</ul>${extras}
<p>⏰ Prazo: <b>48h após validação</b>. Após esse tempo o pedido vira <b>ATRASADO</b> e fica sujeito a nova validação.</p>
<p>⚠ <b>Não fature sem o pedido validado</b> — risco de recusa da NF na SEFA. Evite transtornos e prejuízos.</p>`;
        enviarEmail(r.email, `[JR Lira] Lembrete de faturamento — ${r.pedidos.length} pedido(s) pendente(s)`, html)
          .catch(e => console.error('[cron-faturamento] email falhou', r.email, e.message));
        await pool.query(`UPDATE pedidos SET ultimo_lembrete_em=NOW() WHERE vendedor_id=$1 AND status='validado' AND faturado_em IS NULL`, [r.vendedor_id]);
      }
      if (rows.length) console.log(`[cron-faturamento] lembretes ${hora}h por email: ${rows.length} vendedor(es)`);
    } catch (err) { console.error('[cron-faturamento] erro:', err.message); }
  }

  // Marca pedidos atrasados (>48h validado sem faturar) e notifica
  async function marcarPedidosAtrasados() {
    try {
      const { rows } = await pool.query(`
        UPDATE pedidos p SET status='atrasado', atrasado_em=NOW()
        WHERE p.status='validado'
          AND p.faturado_em IS NULL
          AND p.validado_em < NOW() - INTERVAL '48 hours'
        RETURNING p.id, p.numero_pedido, p.vendedor_id, p.loja_id, p.validado_por
      `);
      if (!rows.length) return;
      console.log(`[cron-atraso] ${rows.length} pedido(s) marcado(s) como atrasado`);
      for (const p of rows) {
        // Notifica vendedor (WhatsApp)
        const v = await pool.query(`SELECT nome, telefone FROM vendedores WHERE id=$1`, [p.vendedor_id]);
        if (v.rows[0]?.telefone) {
          enviarWhatsapp(v.rows[0].telefone,
            `🚨 *PEDIDO ATRASADO*\n\nPedido: *${p.numero_pedido}*\n\nO prazo de 48h para faturamento expirou.\nO pedido foi marcado como ATRASADO e está sujeito a nova validação pelo comprador.\n\n⚠ Entre em contato com o comprador antes de tentar faturar.`);
        }
        // Notifica usuários comprador/admin com a loja do pedido
        const us = await pool.query(`
          SELECT email, nome FROM rh_usuarios
          WHERE ativo=TRUE AND email IS NOT NULL AND email<>''
            AND perfil IN ('admin','comprador')
            AND (perfil='admin' OR loja_id=$1 OR $1 = ANY(lojas_ids))
        `, [p.loja_id]);
        const { enviarEmail } = require('./src/mailer');
        for (const u of us.rows) {
          await enviarEmail(u.email,
            `[JR Lira] Pedido ${p.numero_pedido} atrasado`,
            `<p>Olá, ${u.nome}.</p><p>O pedido <strong>${p.numero_pedido}</strong> foi marcado como ATRASADO (passou 48h após a validação sem faturamento pelo vendedor).</p><p>Entre em contato com o vendedor ou cancele o pedido se necessário.</p>`);
        }
      }
    } catch (err) { console.error('[cron-atraso] erro:', err.message); }
  }

  // Roda a cada 5 minutos
  setTimeout(lembretesFaturamento, 60 * 1000);
  setInterval(lembretesFaturamento, 5 * 60 * 1000);
  setTimeout(marcarPedidosAtrasados, 90 * 1000);
  setInterval(marcarPedidosAtrasados, 5 * 60 * 1000);

  // Sync UltraSyst CD→loja (a cada 10 min, se relay configurado).
  // Sync + match + detecção de produtos novos rodam juntos.
  if (process.env.ULTRASYST_RELAY_URL && process.env.ULTRASYST_RELAY_TOKEN) {
    const { syncTransferenciasCD, matchTransferenciasRecebidas, detectarProdutosNovosCD } = require('./src/sync_ultrasyst');
    const rodarSyncEMatch = async () => {
      try { await syncTransferenciasCD(); }
      catch (e) { console.error('[ultrasyst sync] falha:', e.message); }
      try { await matchTransferenciasRecebidas(); }
      catch (e) { console.error('[ultrasyst match] falha:', e.message); }
      try { await detectarProdutosNovosCD(); }
      catch (e) { console.error('[ultrasyst novos] falha:', e.message); }
    };
    setTimeout(rodarSyncEMatch, 2 * 60 * 1000);
    setInterval(rodarSyncEMatch, 10 * 60 * 1000);
  } else {
    console.log('[ultrasyst] sync desabilitado — sem ULTRASYST_RELAY_URL/TOKEN');
  }

  // Cron horário: notifica admin via WhatsApp dos alertas pendentes (agregado).
  // Reenvia a cada hora até resolver. enviarWhatsapp já foi importado acima.
  const ADMIN_WHATSAPP = process.env.ADMIN_WHATSAPP || '93991001102';
  // Admin pra receber emails de alerta. Fallback: primeiro admin com email no rh_usuarios.
  async function emailsAdmin() {
    if (process.env.ADMIN_EMAIL) return [process.env.ADMIN_EMAIL];
    const r = await dbQuery(`SELECT email FROM rh_usuarios WHERE perfil='admin' AND ativo=TRUE AND email IS NOT NULL AND email<>'' ORDER BY id`);
    return r.map(x => x.email);
  }
  // Monitor SLA: verifica se sync de cada loja×tabela está atrasado.
  // Cadência real (watchdog v2):
  //   VENDAS  — 30min
  //   SLOW    — 3x/dia (08:00 / 14:00 / 20:00) → cobre compras + produtos
  // Janelas inativas: 22:30-06:00 (backup) e 12:00-13:30 (PowerBI).
  async function verificarSlaSync() {
    const agora = new Date();
    const minDia = agora.getHours() * 60 + agora.getMinutes();
    if (minDia < 7 * 60 || minDia >= 22 * 60) return [];      // fora do horário comercial
    if (minDia >= 12 * 60 && minDia < 13 * 60 + 30) return []; // janela PowerBI
    const TABELAS = [
      // VENDAS 30min → tolerância 90min (3 ciclos perdidos). Disponível desde 07:30.
      { nome: 'vendas_historico',  col: 'sincronizado_em', max_atraso:  90 * 60 * 1000, inicio_min:  7 * 60 + 30 },
      // SLOW 08/14/20 → maior gap diurno = 6h (entre 14→20). Tolerância 7h. Só checar após 09:30
      // (último sync da noite anterior foi 20h → gap natural >11h até 08h da manhã).
      { nome: 'compras_historico', col: 'sincronizado_em', max_atraso:   7 * 60 * 60 * 1000, inicio_min: 9 * 60 + 30 },
      { nome: 'produtos_externo',  col: 'sincronizado_em', max_atraso:   7 * 60 * 60 * 1000, inicio_min: 9 * 60 + 30 },
    ];
    const lojas = await dbQuery(`SELECT id FROM lojas WHERE ativo = TRUE ORDER BY id`);
    const lojasIds = lojas.map(l => l.id);
    const atrasos = [];
    for (const t of TABELAS) {
      if (minDia < t.inicio_min) continue;
      const r = await dbQuery(
        `SELECT l.id AS loja_id,
                MAX(x.${t.col}) AS ult,
                EXTRACT(EPOCH FROM (NOW()-MAX(x.${t.col})))::int AS seg
           FROM lojas l
           LEFT JOIN ${t.nome} x ON x.loja_id = l.id
          WHERE l.id = ANY($1::int[])
          GROUP BY l.id ORDER BY l.id`,
        [lojasIds]
      );
      for (const row of r) {
        if (!row.ult) {
          atrasos.push(`L${row.loja_id} ${t.nome}: SEM REGISTROS (sync gravando em outra loja?)`);
          continue;
        }
        const atrasoMs = (row.seg || 0) * 1000;
        if (atrasoMs > t.max_atraso) {
          const horas = Math.floor(atrasoMs / 3600000);
          const mins = Math.floor((atrasoMs % 3600000) / 60000);
          atrasos.push(`L${row.loja_id} ${t.nome}: ${horas}h${mins}m`);
        }
      }
    }
    return atrasos;
  }

  async function notificarAlertasPendentes() {
    try {
      const stats = await dbQuery(`
        SELECT tipo, COUNT(*)::int AS qtd
          FROM alertas_admin
         WHERE resolvido_em IS NULL
         GROUP BY tipo
         ORDER BY qtd DESC
      `);
      const divs = await dbQuery(`
        SELECT COUNT(*)::int AS qtd,
               COALESCE(SUM(valor_total_diferenca) FILTER (WHERE diferenca < 0),0)::numeric(14,2) AS valor_falta
          FROM auditagem_divergencias WHERE status = 'pendente'
      `);
      const vals = await dbQuery(`
        SELECT COUNT(*)::int AS qtd,
               COALESCE(SUM(valor_em_risco),0)::numeric(14,2) AS valor_risco
          FROM validades_em_risco WHERE status = 'pendente'
      `);
      const devs = await dbQuery(`
        SELECT COUNT(*)::int AS qtd,
               COALESCE(SUM(valor_total),0)::numeric(14,2) AS valor_dev
          FROM devolucoes WHERE status = 'aguardando'
      `);
      const embs = await dbQuery(`
        SELECT
          (SELECT COUNT(*) FROM embalagens_fornecedor WHERE status = 'pendente_validacao')::int AS sugestao_nfe,
          (SELECT COUNT(*) FROM produtos_embalagem WHERE ativo_no_cd = TRUE AND status <> 'validado')::int AS pendentes_validacao
      `);
      const totalDiv = divs[0]?.qtd || 0;
      const totalVal = vals[0]?.qtd || 0;
      const totalDev = devs[0]?.qtd || 0;
      const totalEmbNfe = embs[0]?.sugestao_nfe || 0;
      const totalEmbPend = embs[0]?.pendentes_validacao || 0;
      const atrasosSync = await verificarSlaSync();
      if (!stats.length && !totalDiv && !totalVal && !totalDev && !totalEmbNfe && !totalEmbPend && !atrasosSync.length) { console.log('[alertas] nenhum pendente'); return; }
      const total = stats.reduce((s, a) => s + a.qtd, 0);
      const linhas = stats.map(a => `• ${a.qtd} ${a.tipo.replace(/_/g, ' ')}`).join('\n');
      const linhaDivs = totalDiv
        ? `\n• ${totalDiv} divergência(s) CD pendente(s) — R$ ${Number(divs[0].valor_falta).toLocaleString('pt-BR',{minimumFractionDigits:2})} em falta`
        : '';
      const linhaVals = totalVal
        ? `\n• ${totalVal} validade(s) em risco — R$ ${Number(vals[0].valor_risco).toLocaleString('pt-BR',{minimumFractionDigits:2})} em risco`
        : '';
      const linhaDevs = totalDev
        ? `\n• ${totalDev} devolução(ões) aguardando XML — R$ ${Number(devs[0].valor_dev).toLocaleString('pt-BR',{minimumFractionDigits:2})}`
        : '';
      const linhaEmbNfe = totalEmbNfe
        ? `\n• ${totalEmbNfe} embalagem(ns) FORNECEDOR pendente(s) — /embalagens-fornecedor`
        : '';
      const linhaEmbPend = totalEmbPend
        ? `\n• ${totalEmbPend} embalagem(ns) CD ativa(s) sem qtd validada`
        : '';
      const linhaSync = atrasosSync.length
        ? `\n\n⚠️ *SYNC ATRASADO:*\n${atrasosSync.map(a => '• ' + a).join('\n')}`
        : '';
      const corpo = `${total} pendência(s):\n${linhas}${linhaDivs}${linhaVals}${linhaDevs}${linhaEmbNfe}${linhaEmbPend}${linhaSync}`.replace(/\*/g, '');
      // Notifica todos os admins via in-app
      const { criarNotificacao } = require('./src/routes/notificacoes');
      const adminsRows = await dbQuery(`SELECT id FROM rh_usuarios WHERE perfil='admin' AND ativo=TRUE`);
      for (const a of adminsRows) {
        await criarNotificacao({
          destinatario_tipo: 'usuario', destinatario_id: a.id,
          tipo: 'alerta_pendente',
          titulo: `🔔 ${total} pendência(s)`,
          corpo,
          url: '/'
        });
      }
      // marca como notificado (incrementa contador)
      await dbQuery(`
        UPDATE alertas_admin
           SET notificado_em = NOW(), notificacoes_enviadas = COALESCE(notificacoes_enviadas,0) + 1
         WHERE resolvido_em IS NULL
      `);
      console.log(`[alertas] in-app: ${total} pendência(s) → ${adminsRows.length} admin(s)`);
    } catch (e) {
      console.error('[alertas notificar] falha:', e.message);
    }
  }
  // Primeiro tick em 5min após boot, depois a cada 1h
  setTimeout(notificarAlertasPendentes, 5 * 60 * 1000);
  setInterval(notificarAlertasPendentes, 60 * 60 * 1000);

  // Top da semana de embalagens — segunda-feira de manhã (toda hora checa, dispara só uma vez)
  let topSemanaEnviado = null;
  async function notificarTopSemanaEmbalagens() {
    try {
      const agora = new Date();
      const dia = agora.getDay(); // 0=domingo, 1=segunda
      const hora = agora.getHours();
      if (dia !== 1 || hora < 8 || hora >= 12) return;
      const chave = agora.toISOString().slice(0,10);
      if (topSemanaEnviado === chave) return;
      topSemanaEnviado = chave;
      const top = await dbQuery(`
        SELECT validado_por, COUNT(*)::int AS qtd
          FROM embalagens_fornecedor
         WHERE status = 'validado'
           AND validado_por IS NOT NULL
           AND validado_em >= CURRENT_DATE - INTERVAL '7 days'
         GROUP BY validado_por
         ORDER BY qtd DESC
         LIMIT 5
      `);
      if (!top.length) return;
      const { enviarEmail } = require('./src/mailer');
      const lista = top.map((u, i) => `<li>${i+1}º <b>${u.validado_por}</b> — ${u.qtd}</li>`).join('');
      const html = `<h2>🏆 Top semana — Embalagens Fornecedor</h2>
<p><i>(últimos 7 dias)</i></p>
<ol>${lista}</ol>
<p>👑 Parabéns ao primeiro! Continuem firmes — cada validação evita erro de custo.</p>
<p><a href="https://jrliratech-production.up.railway.app/embalagens-fornecedor">Abrir painel</a></p>`;
      const destinos = await emailsAdmin();
      for (const e of destinos) {
        enviarEmail(e, '[JR Lira] 🏆 Top semana — Embalagens Fornecedor', html)
          .catch(err => console.error('[gamificacao] email falhou', e, err.message));
      }
      console.log(`[gamificacao] top semana enviado (email): ${destinos.length} admin(s)`);
    } catch (e) { console.error('[gamificacao]', e.message); }
  }
  setInterval(notificarTopSemanaEmbalagens, 60 * 60 * 1000);

  // Limpeza diária de órfãos em produtos_externo (registros sem update há mais de 7 dias).
  // Como sync é incremental (UPSERT, sem DELETE prévio), produtos removidos no Firebird
  // ficariam pra sempre. Aqui removemos os que pararam de ser sincronizados.
  async function limparProdutosExternoOrfaos() {
    try {
      const r = await dbQuery(
        `DELETE FROM produtos_externo WHERE sincronizado_em < NOW() - INTERVAL '7 days' RETURNING loja_id`
      );
      const porLoja = {};
      for (const row of r) porLoja[row.loja_id] = (porLoja[row.loja_id] || 0) + 1;
      const linhas = Object.entries(porLoja).map(([l, n]) => `L${l}: ${n}`).join(', ');
      console.log(`[limpeza orfaos] ${r.length} produtos_externo removidos${linhas ? ' ('+linhas+')' : ''}`);
    } catch (e) {
      console.error('[limpeza orfaos] falha:', e.message);
    }
  }
  // Roda 1x por dia (a cada 24h) — primeiro tick 30min após boot
  setTimeout(limparProdutosExternoOrfaos, 30 * 60 * 1000);
  setInterval(limparProdutosExternoOrfaos, 24 * 60 * 60 * 1000);
}).catch(err => {
  console.error('[DB] Erro init:', err.message);
  process.exit(1);
});

// Graceful shutdown — fecha conexões ativas antes de encerrar
async function shutdown(signal) {
  console.log(`[${signal}] iniciando shutdown...`);
  const timeoutId = setTimeout(() => {
    console.error('[shutdown] timeout — encerrando à força');
    process.exit(1);
  }, 15000);
  try {
    if (server) await new Promise(r => server.close(r));
    await pool.end();
    clearTimeout(timeoutId);
    console.log('[shutdown] concluído');
    process.exit(0);
  } catch (err) {
    console.error('[shutdown] erro:', err.message);
    process.exit(1);
  }
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('unhandledRejection', err => console.error('[unhandledRejection]', err));
process.on('uncaughtException', err => { console.error('[uncaughtException]', err); shutdown('uncaughtException'); });
