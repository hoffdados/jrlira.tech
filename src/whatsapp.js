// Integração WhatsApp via Evolution API (mesma instância usada pelo açougue-senhas)
//
// Variáveis:
//   EVOLUTION_API_URL          ex: https://evo.jrlira.tech
//   EVOLUTION_API_KEY
//   EVOLUTION_INSTANCE_INTERNO instância p/ comunicação com vendedores/funcionários (default: superasa-interno)
//   EVOLUTION_INSTANCE         instância de fallback

function normalizarFone(numeroBR) {
  let f = String(numeroBR || '').replace(/\D/g, '');
  if (!f) return null;
  if (!f.startsWith('55')) f = '55' + f;
  return f;
}

// Gera variantes para celular: com e sem o 9° dígito (formato antigo)
function variantesCelular(fone) {
  // fone esperado: 55 + DDD(2) + número(8 ou 9)
  if (fone.length === 13 && fone[4] === '9') {
    // Tem o 9° dígito → variante sem: 55 + DDD + 8 dígitos
    return [fone, fone.slice(0, 4) + fone.slice(5)];
  }
  if (fone.length === 12) {
    // Sem 9° → variante com: 55 + DDD + 9 + 8 dígitos
    return [fone, fone.slice(0, 4) + '9' + fone.slice(4)];
  }
  return [fone];
}

async function postEvolution(instancia, fone, mensagem) {
  const url = `${process.env.EVOLUTION_API_URL}/message/sendText/${instancia}`;
  const r = await fetch(url, {
    method: 'POST',
    headers: { apikey: process.env.EVOLUTION_API_KEY, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      number: fone + '@s.whatsapp.net',
      textMessage: { text: mensagem },
    }),
  });
  const txt = await r.text().catch(() => '');
  return { ok: r.ok, status: r.status, body: txt, naoExiste: !r.ok && /"exists":\s*false/i.test(txt) };
}

async function enviarViaInstancia(telefone, mensagem, instancia) {
  const fone = normalizarFone(telefone);
  if (!fone) { console.warn('[whatsapp] número inválido:', telefone); return false; }

  const variantes = variantesCelular(fone);
  for (const f of variantes) {
    const res = await postEvolution(instancia, f, mensagem);
    if (res.ok) {
      console.log(`[whatsapp ${instancia}] enviado para ${f}`);
      return true;
    }
    if (!res.naoExiste) {
      console.error(`[whatsapp ${instancia}] falhou ${res.status} para ${f}: ${res.body.slice(0, 200)}`);
      return false;
    }
    // naoExiste → tenta próxima variante
  }
  console.warn(`[whatsapp ${instancia}] número ${fone} não existe no WhatsApp (tentou ${variantes.length} variantes)`);
  return false;
}

// Envia ao vendedor/funcionário via instância interna SOMENTE.
// A instância 'acougue' é EXCLUSIVA para clientes — nunca usar como fallback aqui.
async function enviarWhatsapp(telefone, mensagem) {
  if (!process.env.EVOLUTION_API_URL || !process.env.EVOLUTION_API_KEY) {
    console.warn('[whatsapp] EVOLUTION_API_URL/KEY ausentes — mensagem não enviada');
    return;
  }
  const fone = normalizarFone(telefone);
  if (!fone) return;

  const instInterno = process.env.EVOLUTION_INSTANCE_INTERNO || 'superasa-interno';

  try {
    await enviarViaInstancia(fone, mensagem, instInterno);
  } catch (err) {
    console.error(`[whatsapp ${instInterno}] erro:`, err.message);
  }
}

module.exports = { enviarWhatsapp, normalizarFone };
