// Parser de descrição de produto → quantidade por embalagem.
// Retorna { qtd, confianca: 'alta'|'baixa'|'nula' }.
//
// Regras (ordem importa, primeira que casa vence):
//  1. NPC x N           → N pacotes (composição embalagem)
//  2. N x N UN/UND      → N pacotes
//  3. N x N (ML|L|G|KG) → N pacotes
//  4. N x ?1 DZ         → 12 (uma dúzia, multiplicador antes ignorado)
//  5. N x N             → N (genérico, baixa confiança)
//  6. N UN final        → N (pacotes avulsos rotulados em UN)
//  7. termina em ML/L/G/KG/UN sem multiplicador → 1 (unidade simples)
//  8. fallback         → 1 baixa

function parseEmbalagem(descricao) {
  if (!descricao) return { qtd: null, confianca: 'nula' };
  const d = String(descricao).trim();

  let m;
  if ((m = d.match(/(\d+)\s*PC\s*[Xx]\s*\d+/i))) return { qtd: +m[1], confianca: 'alta' };
  if ((m = d.match(/(\d+)\s*[Xx]\s*\d+\s*UN(D)?\b/i))) return { qtd: +m[1], confianca: 'alta' };
  if ((m = d.match(/(\d+)\s*[Xx]\s*\d+\s*(ML|L|G|KG|MG|CM|MM)\b/i))) return { qtd: +m[1], confianca: 'alta' };
  if ((m = d.match(/(\d+)\s*[Xx]\s*0?\d+\s*DZ\b/i))) return { qtd: 12, confianca: 'alta' };
  if ((m = d.match(/(\d+)\s*[Xx]\s*\d+/))) return { qtd: +m[1], confianca: 'baixa' };
  if ((m = d.match(/\b(\d+)\s*UN(D)?\s*$/i))) return { qtd: +m[1], confianca: 'baixa' };
  if (/\d+\s*(ML|L|G|KG|MG|UN|UND|CM|MM)\s*$/i.test(d)) return { qtd: 1, confianca: 'alta' };
  return { qtd: 1, confianca: 'baixa' };
}

module.exports = { parseEmbalagem };
