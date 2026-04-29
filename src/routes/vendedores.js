const express = require('express');
const router = express.Router();
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const multer = require('multer');
const pool = require('../db');
const { JWT_SECRET } = require('../auth');

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });

function autVendedor(req, res, next) {
  const token = req.headers.authorization?.replace('Bearer ', '');
  if (!token) return res.status(401).json({ erro: 'Não autenticado' });
  try {
    req.vendedor = jwt.verify(token, JWT_SECRET + '_vendedor');
    next();
  } catch { res.status(401).json({ erro: 'Token inválido' }); }
}

// ── CADASTRO PÚBLICO ──────────────────────────────────────────────

// GET /api/vendedores/buscar-fornecedor?cnpj=XX — público, sem autenticação
router.get('/buscar-fornecedor', async (req, res) => {
  try {
    const cnpj = (req.query.cnpj || '').replace(/\D/g, '');
    if (cnpj.length < 11) return res.status(400).json({ erro: 'CNPJ inválido' });
    const rows = await pool.query(
      `SELECT id, razao_social, fantasia FROM fornecedores
       WHERE REGEXP_REPLACE(cnpj, '\\D', '', 'g') = $1
       ORDER BY id LIMIT 1`,
      [cnpj]
    );
    if (!rows.length) return res.status(404).json({ erro: 'Fornecedor não encontrado' });
    res.json({ id: rows[0].id, fantasia: rows[0].fantasia, razao_social: rows[0].razao_social });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/vendedores/cadastro — público, sem token
router.post('/cadastro', upload.single('foto'), async (req, res) => {
  try {
    const { fornecedor_id, nome, cpf, email, telefone, nome_gerente, telefone_gerente } = req.body;
    if (!fornecedor_id || !nome || !email || !telefone)
      return res.status(400).json({ erro: 'Nome, e-mail e telefone são obrigatórios' });
    const fRows = await pool.query('SELECT id FROM fornecedores WHERE id=$1', [fornecedor_id]);
    if (!fRows.length) return res.status(404).json({ erro: 'Fornecedor não encontrado' });
    const dup = await pool.query(
      `SELECT id FROM vendedores WHERE email=$1 AND fornecedor_id=$2 AND status NOT IN ('rejeitado','inativo')`,
      [email, fornecedor_id]
    );
    if (dup.length) return res.status(409).json({ erro: 'Cadastro já enviado para este fornecedor. Aguarde aprovação.' });
    await pool.query(
      `INSERT INTO vendedores (fornecedor_id, nome, cpf, email, telefone, nome_gerente, telefone_gerente, status, foto_data, foto_mime)
       VALUES ($1,$2,$3,$4,$5,$6,$7,'pendente',$8,$9)`,
      [fornecedor_id, nome, cpf || null, email, telefone, nome_gerente || null, telefone_gerente || null,
       req.file ? req.file.buffer : null, req.file ? req.file.mimetype : null]
    );
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/vendedores/cadastro/:token — valida token e retorna fornecedor
router.get('/cadastro/:token', async (req, res) => {
  try {
    const rows = await pool.query(
      `SELECT v.id, v.status, f.razao_social, f.fantasia
       FROM vendedores v JOIN fornecedores f ON f.id = v.fornecedor_id
       WHERE v.token_cadastro = $1`,
      [req.params.token]
    );
    if (!rows.length) return res.status(404).json({ erro: 'Link inválido' });
    const v = rows[0];
    if (v.status === 'aprovado') return res.status(400).json({ erro: 'Este link já foi utilizado' });
    if (!['pendente', 'aguardando_cadastro'].includes(v.status))
      return res.status(400).json({ erro: 'Link inválido ou expirado' });
    res.json({ valido: true, fornecedor: v.fantasia || v.razao_social });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/vendedores/cadastro/:token — envia dados do vendedor
router.post('/cadastro/:token', upload.single('foto'), async (req, res) => {
  try {
    const rows = await pool.query(
      'SELECT id, status, fornecedor_id FROM vendedores WHERE token_cadastro=$1',
      [req.params.token]
    );
    if (!rows.length) return res.status(404).json({ erro: 'Link inválido' });
    const v = rows[0];
    if (!['pendente', 'aguardando_cadastro'].includes(v.status))
      return res.status(400).json({ erro: 'Link inválido ou já utilizado' });

    const { nome, cpf, email, telefone, nome_gerente, telefone_gerente } = req.body;
    if (!nome || !email || !telefone) return res.status(400).json({ erro: 'Nome, e-mail e telefone são obrigatórios' });

    // Verifica se e-mail já existe em outro vendedor
    const dup = await pool.query('SELECT id FROM vendedores WHERE email=$1 AND id!=$2', [email, v.id]);
    if (dup.length) return res.status(409).json({ erro: 'E-mail já cadastrado' });

    await pool.query(
      `UPDATE vendedores SET nome=$1, cpf=$2, email=$3, telefone=$4,
       nome_gerente=$5, telefone_gerente=$6, status='pendente',
       foto_data=$7, foto_mime=$8, token_cadastro=NULL
       WHERE id=$9`,
      [nome, cpf || null, email, telefone, nome_gerente || null, telefone_gerente || null,
       req.file ? req.file.buffer : null, req.file ? req.file.mimetype : null, v.id]
    );
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// ── LOGIN DO VENDEDOR ─────────────────────────────────────────────

router.post('/login', async (req, res) => {
  try {
    const { email, senha } = req.body;
    if (!email || !senha) return res.status(400).json({ erro: 'Email e senha obrigatórios' });
    const rows = await pool.query(
      `SELECT v.*, f.razao_social, f.fantasia FROM vendedores v
       JOIN fornecedores f ON f.id = v.fornecedor_id
       WHERE LOWER(v.email)=LOWER($1)`,
      [email]
    );
    if (!rows.length) return res.status(401).json({ erro: 'Credenciais inválidas' });
    const v = rows[0];
    if (v.status !== 'aprovado') return res.status(403).json({ erro: 'Acesso não aprovado ou desativado' });
    if (v.acesso_expira_em && new Date(v.acesso_expira_em) < new Date())
      return res.status(403).json({ erro: 'Acesso expirado. Solicite revalidação ao comprador.' });
    if (!v.senha_hash || !await bcrypt.compare(senha, v.senha_hash))
      return res.status(401).json({ erro: 'Credenciais inválidas' });

    const token = jwt.sign(
      { id: v.id, nome: v.nome, email: v.email, fornecedor_id: v.fornecedor_id,
        fornecedor: v.fantasia || v.razao_social },
      JWT_SECRET + '_vendedor',
      { expiresIn: '12h' }
    );
    res.json({ token, nome: v.nome, fornecedor: v.fantasia || v.razao_social });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// ── APP DO VENDEDOR ───────────────────────────────────────────────

// GET /api/vendedores/lojas
router.get('/lojas', autVendedor, async (req, res) => {
  try {
    const rows = await pool.query('SELECT id, nome, cnpj FROM lojas WHERE ativo=true ORDER BY nome');
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/vendedores/produto/:barcode
router.get('/produto/:barcode', autVendedor, async (req, res) => {
  try {
    const bc = req.params.barcode.trim();
    const rows = await pool.query(
      `SELECT codigobarra as codigo_barras, descricao, custofabrica as preco
       FROM produtos_externo WHERE codigobarra=$1 LIMIT 1`,
      [bc]
    );
    if (!rows.length) return res.status(404).json({ encontrado: false });
    res.json({ encontrado: true, ...rows[0] });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/vendedores/meus-pedidos
router.get('/meus-pedidos', autVendedor, async (req, res) => {
  try {
    const rows = await pool.query(
      `SELECT p.id, p.numero_pedido, p.status, p.valor_total, p.condicao_pagamento,
              p.criado_em, p.enviado_em, l.nome as loja_nome
       FROM pedidos p LEFT JOIN lojas l ON l.id=p.loja_id
       WHERE p.vendedor_id=$1 ORDER BY p.criado_em DESC`,
      [req.vendedor.id]
    );
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/vendedores/pedido/:id
router.get('/pedido/:id', autVendedor, async (req, res) => {
  try {
    const rows = await pool.query(
      `SELECT p.*, l.nome as loja_nome FROM pedidos p LEFT JOIN lojas l ON l.id=p.loja_id
       WHERE p.id=$1 AND p.vendedor_id=$2`,
      [req.params.id, req.vendedor.id]
    );
    if (!rows.length) return res.status(404).json({ erro: 'Pedido não encontrado' });
    const itens = await pool.query('SELECT * FROM itens_pedido WHERE pedido_id=$1 ORDER BY id', [req.params.id]);
    res.json({ ...rows[0], itens });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/vendedores/pedido — cria rascunho
router.post('/pedido', autVendedor, async (req, res) => {
  try {
    const { loja_id, condicao_pagamento, observacoes } = req.body;
    if (!loja_id) return res.status(400).json({ erro: 'loja_id obrigatório' });
    const rows = await pool.query(
      `INSERT INTO pedidos (fornecedor_id, vendedor_id, loja_id, condicao_pagamento, observacoes, status)
       VALUES ($1,$2,$3,$4,$5,'rascunho') RETURNING id`,
      [req.vendedor.fornecedor_id, req.vendedor.id, loja_id, condicao_pagamento || null, observacoes || null]
    );
    res.json({ ok: true, id: rows[0].id });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// PUT /api/vendedores/pedido/:id — atualiza cabeçalho
router.put('/pedido/:id', autVendedor, async (req, res) => {
  try {
    const { loja_id, condicao_pagamento, observacoes } = req.body;
    await pool.query(
      `UPDATE pedidos SET loja_id=$1, condicao_pagamento=$2, observacoes=$3
       WHERE id=$4 AND vendedor_id=$5 AND status='rascunho'`,
      [loja_id, condicao_pagamento || null, observacoes || null, req.params.id, req.vendedor.id]
    );
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/vendedores/pedido/:id/item
router.post('/pedido/:id/item', autVendedor, async (req, res) => {
  try {
    const { codigo_barras, descricao, quantidade, preco_unitario, produto_novo } = req.body;
    if (!descricao || !quantidade || !preco_unitario)
      return res.status(400).json({ erro: 'descricao, quantidade e preco obrigatórios' });
    const valor_total = parseFloat(quantidade) * parseFloat(preco_unitario);
    const rows = await pool.query(
      `INSERT INTO itens_pedido (pedido_id, codigo_barras, descricao, quantidade, preco_unitario, valor_total, produto_novo)
       VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id`,
      [req.params.id, codigo_barras || null, descricao, quantidade, preco_unitario, valor_total, produto_novo || false]
    );
    await recalcTotal(req.params.id);
    res.json({ ok: true, id: rows[0].id });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// DELETE /api/vendedores/pedido/:id/item/:itemId
router.delete('/pedido/:id/item/:itemId', autVendedor, async (req, res) => {
  try {
    await pool.query('DELETE FROM itens_pedido WHERE id=$1 AND pedido_id=$2', [req.params.itemId, req.params.id]);
    await recalcTotal(req.params.id);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/vendedores/pedido/:id/enviar
router.post('/pedido/:id/enviar', autVendedor, async (req, res) => {
  try {
    const rows = await pool.query(
      `SELECT p.*, l.cnpj as loja_cnpj FROM pedidos p LEFT JOIN lojas l ON l.id=p.loja_id
       WHERE p.id=$1 AND p.vendedor_id=$2 AND p.status='rascunho'`,
      [req.params.id, req.vendedor.id]
    );
    if (!rows.length) return res.status(404).json({ erro: 'Pedido não encontrado ou já enviado' });
    const p = rows[0];
    const itens = await pool.query('SELECT id FROM itens_pedido WHERE pedido_id=$1', [p.id]);
    if (!itens.length) return res.status(400).json({ erro: 'Adicione ao menos um item antes de enviar' });
    if (!p.condicao_pagamento) return res.status(400).json({ erro: 'Informe a condição de pagamento' });

    const numero_pedido = await gerarNumeroPedido(p.loja_id, p.loja_cnpj);
    await pool.query(
      `UPDATE pedidos SET status='aguardando_validacao', numero_pedido=$1, enviado_em=NOW() WHERE id=$2`,
      [numero_pedido, p.id]
    );
    res.json({ ok: true, numero_pedido });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

async function recalcTotal(pedido_id) {
  await pool.query(
    `UPDATE pedidos SET valor_total=(SELECT COALESCE(SUM(valor_total),0) FROM itens_pedido WHERE pedido_id=$1) WHERE id=$1`,
    [pedido_id]
  );
}

async function gerarNumeroPedido(loja_id, cnpj) {
  const cnpjLimpo = (cnpj || '').replace(/\D/g, '').padEnd(14, '0');
  const hoje = new Date().toISOString().slice(0, 10).replace(/-/g, '');
  const seqRows = await pool.query(
    `SELECT COUNT(*)+1 AS prox FROM pedidos WHERE loja_id=$1 AND DATE(enviado_em)=CURRENT_DATE`,
    [loja_id]
  );
  const seq = String(seqRows[0].prox).padStart(4, '0');
  return `${cnpjLimpo}${hoje}${seq}`;
}

module.exports = router;
