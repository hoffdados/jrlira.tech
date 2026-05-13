const express = require('express');
const router = express.Router();
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const multer = require('multer');
const { pool, query: dbQuery } = require('../db');
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
    const rows = await dbQuery(
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
    const faltam = [];
    if (!fornecedor_id) faltam.push('fornecedor');
    if (!nome?.trim()) faltam.push('nome');
    if (!cpf?.trim()) faltam.push('CPF');
    if (!email?.trim()) faltam.push('e-mail');
    if (!telefone?.trim()) faltam.push('telefone');
    if (!nome_gerente?.trim()) faltam.push('nome do gerente');
    if (!telefone_gerente?.trim()) faltam.push('telefone do gerente');
    if (!req.file) faltam.push('foto');
    if (faltam.length) return res.status(400).json({ erro: 'Campos obrigatórios faltando: ' + faltam.join(', ') });
    const cpfNorm = (cpf || '').replace(/\D/g, '');
    if (cpfNorm.length !== 11)
      return res.status(400).json({ erro: 'CPF deve ter 11 dígitos' });
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email))
      return res.status(400).json({ erro: 'E-mail inválido' });
    const fRows = await dbQuery('SELECT id, cnpj FROM fornecedores WHERE id=$1', [fornecedor_id]);
    if (!fRows.length) return res.status(404).json({ erro: 'Fornecedor não encontrado' });
    const fornecedor_cnpj = fRows[0].cnpj;
    const cnpjNorm = (fornecedor_cnpj || '').replace(/\D/g, '');
    // Bloqueia mesmo CPF para o mesmo CNPJ (com status ativo)
    const dup = await dbQuery(
      `SELECT id, status FROM vendedores
       WHERE REGEXP_REPLACE(COALESCE(cpf,''),'\\D','','g') = $1
         AND REGEXP_REPLACE(COALESCE(fornecedor_cnpj,''),'\\D','','g') = $2
         AND status NOT IN ('rejeitado','inativo')`,
      [cpfNorm, cnpjNorm]
    );
    if (dup.length) {
      const st = dup[0].status;
      const msg = st === 'aprovado'
        ? 'Este CPF já está cadastrado e aprovado para este fornecedor.'
        : 'Este CPF já tem solicitação pendente para este fornecedor. Aguarde aprovação.';
      return res.status(409).json({ erro: msg });
    }
    await dbQuery(
      `INSERT INTO vendedores (fornecedor_id, fornecedor_cnpj, nome, cpf, email, telefone, nome_gerente, telefone_gerente, status, foto_data, foto_mime)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'pendente',$9,$10)`,
      [fornecedor_id, fornecedor_cnpj, nome, cpf || null, email, telefone, nome_gerente || null, telefone_gerente || null,
       req.file ? req.file.buffer : null, req.file ? req.file.mimetype : null]
    );
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/vendedores/cadastro/:token — valida token e retorna fornecedor
router.get('/cadastro/:token', async (req, res) => {
  try {
    const rows = await dbQuery(
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
    const rows = await dbQuery(
      'SELECT id, status, fornecedor_id FROM vendedores WHERE token_cadastro=$1',
      [req.params.token]
    );
    if (!rows.length) return res.status(404).json({ erro: 'Link inválido' });
    const v = rows[0];
    if (!['pendente', 'aguardando_cadastro'].includes(v.status))
      return res.status(400).json({ erro: 'Link inválido ou já utilizado' });

    const { nome, cpf, email, telefone, nome_gerente, telefone_gerente } = req.body;
    if (!nome || !email || !telefone) return res.status(400).json({ erro: 'Nome, e-mail e telefone são obrigatórios' });

    // Mesmo CPF não pode ter 2 cadastros ativos no mesmo fornecedor (UNIQUE parcial garante)

    await dbQuery(
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
    // LEFT JOIN + fallback por CNPJ — sobrevive se Açougue apagar fornecedores
    const rows = await dbQuery(
      `SELECT v.*,
              COALESCE(f.razao_social, f2.razao_social) AS razao_social,
              COALESCE(f.fantasia, f2.fantasia) AS fantasia,
              COALESCE(v.fornecedor_id, f2.id) AS fornecedor_id_efetivo
       FROM vendedores v
       LEFT JOIN fornecedores f ON f.id = v.fornecedor_id
       LEFT JOIN LATERAL (
         SELECT id, razao_social, fantasia FROM fornecedores
         WHERE v.fornecedor_id IS NULL AND v.fornecedor_cnpj IS NOT NULL
           AND REGEXP_REPLACE(cnpj,'\\D','','g') = REGEXP_REPLACE(v.fornecedor_cnpj,'\\D','','g')
         LIMIT 1
       ) f2 ON TRUE
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

    // Auto-heal: se fornecedor_id está null mas encontramos por CNPJ, religar
    if (!v.fornecedor_id && v.fornecedor_id_efetivo) {
      await dbQuery('UPDATE vendedores SET fornecedor_id = $1 WHERE id = $2',
        [v.fornecedor_id_efetivo, v.id]);
    }

    const token = jwt.sign(
      { id: v.id, nome: v.nome, email: v.email,
        fornecedor_id: v.fornecedor_id_efetivo || v.fornecedor_id,
        fornecedor_cnpj: v.fornecedor_cnpj,
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
    const rows = await dbQuery('SELECT id, nome, cnpj FROM lojas WHERE ativo=true ORDER BY nome');
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/vendedores/produto/:barcode
router.get('/produto/:barcode', autVendedor, async (req, res) => {
  try {
    const bc = req.params.barcode.trim();
    const rows = await dbQuery(
      `SELECT codigobarra as codigo_barras, descricao, custoorigem as preco
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
    const rows = await dbQuery(
      `SELECT p.id, p.numero_pedido, p.status, p.valor_total, p.condicao_pagamento,
              p.preco_valido_ate, p.criado_em, p.enviado_em, p.validado_em,
              p.motivo_rejeicao, p.rejeitado_em,
              p.faturado_em, p.faturado_por, p.numero_nf_faturada,
              p.atrasado_em,
              p.cancelado_em, p.motivo_cancelamento,
              l.nome as loja_nome
       FROM pedidos p LEFT JOIN lojas l ON l.id=p.loja_id
       WHERE p.vendedor_id=$1
       ORDER BY (p.status = 'rascunho') DESC,
                (p.status = 'atrasado') DESC,
                (p.status = 'validado') DESC,
                p.criado_em DESC`,
      [req.vendedor.id]
    );
    res.json(rows);
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/vendedores/estatisticas — KPIs do historico do vendedor
router.get('/estatisticas', autVendedor, async (req, res) => {
  try {
    const r = await dbQuery(
      `SELECT
         COUNT(*)::int                                             AS qtd_total,
         COALESCE(SUM(valor_total),0)::numeric(14,2)               AS valor_total,
         COUNT(*) FILTER (WHERE status = 'rascunho')::int          AS qtd_rascunho,
         COUNT(*) FILTER (WHERE status IN ('aguardando_validacao','aguardando_auditoria'))::int AS qtd_aguardando,
         COUNT(*) FILTER (WHERE status = 'validado')::int          AS qtd_validado,
         COUNT(*) FILTER (WHERE status = 'faturado')::int          AS qtd_faturado,
         COUNT(*) FILTER (WHERE status = 'atrasado')::int          AS qtd_atrasado,
         COUNT(*) FILTER (WHERE status = 'rejeitado')::int         AS qtd_rejeitado,
         COUNT(*) FILTER (WHERE status = 'cancelado_pelo_vendedor')::int AS qtd_cancelado,
         COUNT(*) FILTER (WHERE status = 'vinculado')::int         AS qtd_vinculado,
         COALESCE(SUM(valor_total) FILTER (WHERE status IN ('faturado','vinculado')),0)::numeric(14,2) AS valor_concretizado,
         COALESCE(AVG(condicao_pagamento) FILTER (WHERE condicao_pagamento > 0),0)::numeric(10,1) AS prazo_medio
       FROM pedidos
       WHERE vendedor_id = $1`,
      [req.vendedor.id]
    );
    res.json(r[0] || {});
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// GET /api/vendedores/pedido/:id/pdf — vendedor baixa PDF do proprio pedido.
// Aceita token na query (link <a href>) ou no header.
router.get('/pedido/:id/pdf', async (req, res) => {
  try {
    const token = req.headers.authorization?.replace('Bearer ', '') || req.query.token;
    if (!token) return res.status(401).send('Token nao fornecido');
    let vendedorId;
    try { vendedorId = jwt.verify(token, JWT_SECRET + '_vendedor').id; }
    catch { return res.status(401).send('Token invalido'); }

    const rows = await dbQuery(
      `SELECT p.*,
              COALESCE(f.razao_social, f2.razao_social) AS razao_social,
              COALESCE(f.fantasia, f2.fantasia) AS fantasia,
              COALESCE(f.cnpj, p.fornecedor_cnpj_snapshot, f2.cnpj) AS fornecedor_cnpj,
              v.nome as vendedor_nome, v.email as vendedor_email, v.telefone as vendedor_tel,
              l.nome as loja_nome, l.cnpj as loja_cnpj
         FROM pedidos p
         LEFT JOIN fornecedores f ON f.id=p.fornecedor_id
         LEFT JOIN vendedores v ON v.id=p.vendedor_id
         LEFT JOIN lojas l ON l.id=p.loja_id
         LEFT JOIN LATERAL (
           SELECT razao_social, fantasia, cnpj FROM fornecedores
           WHERE p.fornecedor_id IS NULL AND v.fornecedor_cnpj IS NOT NULL
             AND REGEXP_REPLACE(cnpj,'\\D','','g') = REGEXP_REPLACE(v.fornecedor_cnpj,'\\D','','g')
           LIMIT 1
         ) f2 ON TRUE
        WHERE p.id = $1 AND p.vendedor_id = $2`,
      [req.params.id, vendedorId]
    );
    if (!rows.length) return res.status(404).send('Pedido nao encontrado');
    const itens = await dbQuery('SELECT * FROM itens_pedido WHERE pedido_id = $1 ORDER BY id', [req.params.id]);
    const { gerarPDF } = require('./pedidos');
    const buf = await gerarPDF(rows[0], itens);
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="pedido-${rows[0].numero_pedido || req.params.id}.pdf"`);
    res.send(buf);
  } catch (e) { res.status(500).send('Erro: ' + e.message); }
});

// GET /api/vendedores/pedido/:id
router.get('/pedido/:id', autVendedor, async (req, res) => {
  try {
    const rows = await dbQuery(
      `SELECT p.*, l.nome as loja_nome FROM pedidos p LEFT JOIN lojas l ON l.id=p.loja_id
       WHERE p.id=$1 AND p.vendedor_id=$2`,
      [req.params.id, req.vendedor.id]
    );
    if (!rows.length) return res.status(404).json({ erro: 'Pedido não encontrado' });
    const itens = await dbQuery('SELECT * FROM itens_pedido WHERE pedido_id=$1 ORDER BY id', [req.params.id]);
    res.json({ ...rows[0], itens });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/vendedores/pedido — cria rascunho
router.post('/pedido', autVendedor, async (req, res) => {
  try {
    const { loja_id, condicao_pagamento, observacoes, preco_valido_ate } = req.body;
    if (!loja_id) return res.status(400).json({ erro: 'loja_id obrigatório' });
    const rows = await dbQuery(
      `INSERT INTO pedidos (fornecedor_id, vendedor_id, loja_id, condicao_pagamento, observacoes, preco_valido_ate, status)
       VALUES ($1,$2,$3,$4,$5,$6,'rascunho') RETURNING id`,
      [req.vendedor.fornecedor_id, req.vendedor.id, loja_id, condicao_pagamento || null, observacoes || null, preco_valido_ate || null]
    );
    res.json({ ok: true, id: rows[0].id });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// PUT /api/vendedores/pedido/:id — atualiza cabeçalho
router.put('/pedido/:id', autVendedor, async (req, res) => {
  try {
    const { loja_id, condicao_pagamento, observacoes, preco_valido_ate } = req.body;
    await dbQuery(
      `UPDATE pedidos SET loja_id=$1, condicao_pagamento=$2, observacoes=$3, preco_valido_ate=$4
       WHERE id=$5 AND vendedor_id=$6 AND status='rascunho'`,
      [loja_id, condicao_pagamento || null, observacoes || null, preco_valido_ate || null, req.params.id, req.vendedor.id]
    );
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/vendedores/pedido/:id/duplicar — cria novo rascunho com os itens deste pedido
router.post('/pedido/:id/duplicar', autVendedor, async (req, res) => {
  const client = await pool.connect();
  try {
    const { rows: orig } = await client.query(
      `SELECT * FROM pedidos WHERE id=$1 AND vendedor_id=$2`,
      [req.params.id, req.vendedor.id]
    );
    if (!orig.length) return res.status(404).json({ erro: 'Pedido não encontrado' });

    await client.query('BEGIN');
    const { rows: novo } = await client.query(
      `INSERT INTO pedidos (fornecedor_id, vendedor_id, status)
       VALUES ($1, $2, 'rascunho') RETURNING id`,
      [req.vendedor.fornecedor_id, req.vendedor.id]
    );
    const novoId = novo[0].id;
    // Copia itens (qtd e preço, mantém)
    await client.query(
      `INSERT INTO itens_pedido (pedido_id, codigo_barras, descricao, quantidade, preco_unitario, valor_total, produto_novo)
       SELECT $1, codigo_barras, descricao, quantidade, preco_unitario, valor_total, produto_novo
       FROM itens_pedido WHERE pedido_id=$2`,
      [novoId, req.params.id]
    );
    await client.query('COMMIT');
    await recalcTotal(novoId);
    res.json({ ok: true, id: novoId });
  } catch (e) {
    await client.query('ROLLBACK').catch(()=>{});
    res.status(500).json({ erro: e.message });
  } finally { client.release(); }
});

// POST /api/vendedores/pedido/:id/item
router.post('/pedido/:id/item', autVendedor, async (req, res) => {
  try {
    const { codigo_barras, descricao, quantidade, preco_unitario, produto_novo } = req.body;
    if (!descricao || !quantidade || !preco_unitario)
      return res.status(400).json({ erro: 'descricao, quantidade e preco obrigatórios' });
    const valor_total = parseFloat(quantidade) * parseFloat(preco_unitario);
    const rows = await dbQuery(
      `INSERT INTO itens_pedido (pedido_id, codigo_barras, descricao, quantidade, preco_unitario, valor_total, produto_novo)
       VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id`,
      [req.params.id, codigo_barras || null, descricao, quantidade, preco_unitario, valor_total, produto_novo || false]
    );
    await recalcTotal(req.params.id);
    res.json({ ok: true, id: rows[0].id });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/vendedores/pedido/:id/faturar — vendedor confirma o faturamento com nº da NF
router.post('/pedido/:id/faturar', autVendedor, async (req, res) => {
  try {
    const { numero_nf } = req.body;
    if (!numero_nf?.trim()) return res.status(400).json({ erro: 'Informe o número da NF' });
    const ped = await dbQuery(
      `SELECT id, status FROM pedidos WHERE id=$1 AND vendedor_id=$2`,
      [req.params.id, req.vendedor.id]
    );
    if (!ped.length) return res.status(404).json({ erro: 'Pedido não encontrado' });
    const st = ped[0].status;
    if (!['validado', 'atrasado'].includes(st))
      return res.status(400).json({ erro: `Pedido com status "${st}" não pode ser faturado` });

    await dbQuery(
      `UPDATE pedidos SET status='faturado', faturado_em=NOW(), faturado_por=$1, numero_nf_faturada=$2 WHERE id=$3`,
      [req.vendedor.nome, numero_nf.trim(), req.params.id]
    );

    // Notifica o(s) comprador(es) responsável(eis) pela loja do pedido por email
    try {
      const info = await dbQuery(
        `SELECT p.numero_pedido, p.loja_id, l.nome AS loja_nome, f.razao_social AS fornecedor_nome
         FROM pedidos p LEFT JOIN lojas l ON l.id=p.loja_id LEFT JOIN fornecedores f ON f.id=p.fornecedor_id
         WHERE p.id=$1`, [req.params.id]
      );
      const us = await dbQuery(
        `SELECT email, nome FROM rh_usuarios
         WHERE ativo=TRUE AND email IS NOT NULL AND email<>''
           AND perfil IN ('admin','comprador')
           AND (perfil='admin' OR loja_id=$1 OR $1 = ANY(lojas_ids))`,
        [info[0]?.loja_id]
      );
      const { enviarEmail } = require('../mailer');
      for (const u of us) {
        await enviarEmail(u.email,
          `[JR Lira] Pedido ${info[0]?.numero_pedido} foi faturado`,
          `<p>Olá, ${u.nome}.</p><p>O fornecedor <strong>${info[0]?.fornecedor_nome || ''}</strong> faturou o pedido <strong>${info[0]?.numero_pedido}</strong> da loja <strong>${info[0]?.loja_nome || ''}</strong>.</p><p>NF: <strong>${numero_nf}</strong></p>`);
      }
    } catch (err) { console.error('[fatura email]', err.message); }

    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/vendedores/pedido/:id/cancelar — vendedor cancela pedido com motivo
router.post('/pedido/:id/cancelar', autVendedor, async (req, res) => {
  try {
    const motivo = (req.body?.motivo || '').trim();
    if (motivo.length < 5) return res.status(400).json({ erro: 'Informe o motivo do cancelamento (mínimo 5 caracteres)' });

    const ped = await dbQuery(
      `SELECT id, status, numero_pedido, loja_id, fornecedor_id
         FROM pedidos WHERE id=$1 AND vendedor_id=$2`,
      [req.params.id, req.vendedor.id]
    );
    if (!ped.length) return res.status(404).json({ erro: 'Pedido não encontrado' });
    const st = ped[0].status;
    const cancelaveis = ['aguardando_validacao', 'aguardando_auditoria', 'validado', 'atrasado'];
    if (!cancelaveis.includes(st))
      return res.status(400).json({ erro: `Pedido com status "${st}" não pode ser cancelado` });

    await dbQuery(
      `UPDATE pedidos
          SET status='cancelado_pelo_vendedor',
              cancelado_em=NOW(), cancelado_por=$1, motivo_cancelamento=$2
        WHERE id=$3`,
      [req.vendedor.nome, motivo, req.params.id]
    );

    // Notifica compradores da loja por email + WhatsApp
    try {
      const info = await dbQuery(
        `SELECT p.numero_pedido, p.loja_id, l.nome AS loja_nome,
                f.razao_social AS fornecedor_nome,
                v.nome AS vendedor_nome
           FROM pedidos p
           LEFT JOIN lojas l ON l.id=p.loja_id
           LEFT JOIN fornecedores f ON f.id=p.fornecedor_id
           LEFT JOIN vendedores v ON v.id=p.vendedor_id
          WHERE p.id=$1`, [req.params.id]
      );
      const us = await dbQuery(
        `SELECT email, nome, telefone FROM rh_usuarios
          WHERE ativo=TRUE AND perfil IN ('admin','comprador')
            AND (perfil='admin' OR loja_id=$1 OR $1 = ANY(lojas_ids))`,
        [info[0]?.loja_id]
      );
      const i = info[0] || {};
      const { enviarEmail } = require('../mailer');
      const { enviarWhatsapp } = require('../whatsapp');
      const assunto = `[JR Lira] Pedido ${i.numero_pedido} foi CANCELADO pelo fornecedor`;
      const corpoHtml = `<p>Olá.</p>
        <p>O fornecedor <strong>${i.fornecedor_nome || ''}</strong> (vendedor ${i.vendedor_nome || ''}) <strong>cancelou</strong> o pedido <strong>${i.numero_pedido}</strong> da loja <strong>${i.loja_nome || ''}</strong>.</p>
        <p><strong>Motivo:</strong> ${motivo}</p>
        <p>O vendedor enviará um novo pedido em substituição.</p>`;
      const wmsg = `🚫 *Pedido cancelado pelo fornecedor*\n\nPedido: *${i.numero_pedido}*\nLoja: ${i.loja_nome || ''}\nFornecedor: ${i.fornecedor_nome || ''}\nVendedor: ${i.vendedor_nome || ''}\nMotivo: ${motivo}\n\nO vendedor enviará um novo pedido em substituição.`;
      for (const u of us) {
        if (u.email) await enviarEmail(u.email, assunto, corpoHtml).catch(()=>{});
        if (u.telefone) enviarWhatsapp(u.telefone, wmsg).catch(()=>{});
      }
    } catch (err) { console.error('[cancelar pedido notif]', err.message); }

    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// DELETE /api/vendedores/pedido/:id — exclui rascunho do próprio vendedor
router.delete('/pedido/:id', autVendedor, async (req, res) => {
  try {
    const r = await dbQuery(
      `DELETE FROM pedidos WHERE id=$1 AND vendedor_id=$2 AND status='rascunho' RETURNING id`,
      [req.params.id, req.vendedor.id]
    );
    if (!r.length) return res.status(404).json({ erro: 'Rascunho não encontrado ou não pertence a você' });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// PUT /api/vendedores/pedido/:id/item/:itemId — edita qtd/preço de um item
router.put('/pedido/:id/item/:itemId', autVendedor, async (req, res) => {
  try {
    const { quantidade, preco_unitario } = req.body;
    if (!Number.isFinite(parseFloat(quantidade)) || !Number.isFinite(parseFloat(preco_unitario)))
      return res.status(400).json({ erro: 'quantidade e preco_unitario obrigatórios' });
    // Só permite editar se o pedido ainda for rascunho do próprio vendedor
    const ok = await dbQuery(
      `SELECT 1 FROM pedidos p WHERE p.id=$1 AND p.vendedor_id=$2 AND p.status='rascunho'`,
      [req.params.id, req.vendedor.id]
    );
    if (!ok.length) return res.status(403).json({ erro: 'Pedido não editável' });
    const valor_total = parseFloat(quantidade) * parseFloat(preco_unitario);
    await dbQuery(
      `UPDATE itens_pedido SET quantidade=$1, preco_unitario=$2, valor_total=$3
       WHERE id=$4 AND pedido_id=$5`,
      [quantidade, preco_unitario, valor_total, req.params.itemId, req.params.id]
    );
    await recalcTotal(req.params.id);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// DELETE /api/vendedores/pedido/:id/item/:itemId
router.delete('/pedido/:id/item/:itemId', autVendedor, async (req, res) => {
  try {
    await dbQuery('DELETE FROM itens_pedido WHERE id=$1 AND pedido_id=$2', [req.params.itemId, req.params.id]);
    await recalcTotal(req.params.id);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

// POST /api/vendedores/pedido/:id/enviar
router.post('/pedido/:id/enviar', autVendedor, async (req, res) => {
  try {
    const rows = await dbQuery(
      `SELECT p.*, l.cnpj as loja_cnpj FROM pedidos p LEFT JOIN lojas l ON l.id=p.loja_id
       WHERE p.id=$1 AND p.vendedor_id=$2 AND p.status='rascunho'`,
      [req.params.id, req.vendedor.id]
    );
    if (!rows.length) return res.status(404).json({ erro: 'Pedido não encontrado ou já enviado' });
    const p = rows[0];
    const itens = await dbQuery('SELECT id FROM itens_pedido WHERE pedido_id=$1', [p.id]);
    if (!itens.length) return res.status(400).json({ erro: 'Adicione ao menos um item antes de enviar' });
    if (p.condicao_pagamento == null) return res.status(400).json({ erro: 'Informe a condição de pagamento' });

    // Serializa geração de numero_pedido por loja_id pra evitar race condition (2 vendedores enviarem ao mesmo tempo)
    const { pool } = require('../db');
    const client = await pool.connect();
    let numero_pedido;
    try {
      await client.query('BEGIN');
      await client.query('SELECT pg_advisory_xact_lock($1)', [p.loja_id]);
      // Sequência dentro do lock — usa MAX(seq)+1 do prefixo (cnpj+data) pra evitar
      // colisão quando há gap (pedido deletado/cancelado). COUNT+1 colide quando MAX > COUNT.
      const cnpjLimpo = (p.loja_cnpj || '').replace(/\D/g, '').padEnd(14, '0');
      const hoje = new Date().toISOString().slice(0, 10).replace(/-/g, '');
      const prefixo = `${cnpjLimpo}${hoje}`;
      const { rows: seqRows } = await client.query(
        `SELECT COALESCE(MAX(NULLIF(REGEXP_REPLACE(SUBSTRING(numero_pedido FROM 23 FOR 4),'\\D','','g'),'')::int), 0) + 1 AS prox
           FROM pedidos WHERE numero_pedido LIKE $1 || '%'`,
        [prefixo]);
      const seq = String(seqRows[0].prox).padStart(4, '0');
      numero_pedido = `${prefixo}${seq}`;
      await client.query(
        `UPDATE pedidos SET status='aguardando_validacao', numero_pedido=$1, enviado_em=NOW() WHERE id=$2`,
        [numero_pedido, p.id]);
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      throw e;
    } finally {
      client.release();
    }
    res.json({ ok: true, numero_pedido });
  } catch (e) { res.status(500).json({ erro: e.message }); }
});

async function recalcTotal(pedido_id) {
  await dbQuery(
    `UPDATE pedidos SET valor_total=(SELECT COALESCE(SUM(valor_total),0) FROM itens_pedido WHERE pedido_id=$1) WHERE id=$1`,
    [pedido_id]
  );
}

async function gerarNumeroPedido(loja_id, cnpj) {
  const cnpjLimpo = (cnpj || '').replace(/\D/g, '').padEnd(14, '0');
  const hoje = new Date().toISOString().slice(0, 10).replace(/-/g, '');
  const prefixo = `${cnpjLimpo}${hoje}`;
  const seqRows = await dbQuery(
    `SELECT COALESCE(MAX(NULLIF(REGEXP_REPLACE(SUBSTRING(numero_pedido FROM 23 FOR 4),'\\D','','g'),'')::int), 0) + 1 AS prox
       FROM pedidos WHERE numero_pedido LIKE $1 || '%'`,
    [prefixo]
  );
  const seq = String(seqRows[0].prox).padStart(4, '0');
  return `${prefixo}${seq}`;
}

module.exports = router;
