/**
 * ╔══════════════════════════════════════════════════════════════════════════════╗
 * ║  AÇAÍTERIA SaaS — BACK-END Node.js + Express + Supabase/PostgreSQL          ║
 * ║  Arquitetura Multi-tenant  |  Cobrança via Asaas                            ║
 * ║  Schema: acaiteria                                                           ║
 * ║                                                                              ║
 * ║  Tabelas (schema acaiteria):                                                 ║
 * ║   lojas, usuarios, pedidos, cardapio, acai_categorias                        ║
 * ║   acai_ingredientes, acai_modelos, bairros, cupons, taras                    ║
 * ║   clientes, configuracoes                                                    ║
 * ╚══════════════════════════════════════════════════════════════════════════════╝
 *
 * Variáveis de ambiente (.env):
 *   PORT=3000
 *   TZ=America/Fortaleza
 *   JWT_SECRET=<string longa e aleatória>
 *   SUPABASE_URL=https://<projeto>.supabase.co
 *   SUPABASE_SERVICE_KEY=<service_role key>
 *   ASAAS_WEBHOOK_TOKEN=<token que você define no painel Asaas>
 *   ONESIGNAL_REST_API_KEY=<chave REST do OneSignal>
 */

'use strict';

const express        = require('express');
const cors           = require('cors');
const jwt            = require('jsonwebtoken');
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

// ─── VALIDAÇÃO DE AMBIENTE ────────────────────────────────────────────────────

const JWT_SECRET          = process.env.JWT_SECRET;
const ASAAS_WEBHOOK_TOKEN = process.env.ASAAS_WEBHOOK_TOKEN || '';
const PORT                = process.env.PORT || 3000;
const TZ                  = process.env.TZ   || 'America/Fortaleza';
const SUPABASE_URL        = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY= process.env.SUPABASE_SERVICE_KEY;

if (!JWT_SECRET)           throw new Error('❌  JWT_SECRET não definido no .env');
if (!SUPABASE_URL)         throw new Error('❌  SUPABASE_URL não definido no .env');
if (!SUPABASE_SERVICE_KEY) throw new Error('❌  SUPABASE_SERVICE_KEY não definido no .env');
if (!ASAAS_WEBHOOK_TOKEN) {
  console.warn('⚠️  ASAAS_WEBHOOK_TOKEN não definido — webhook Asaas estará desprotegido!');
}

// ─── SUPABASE CLIENT (schema: acaiteria) ─────────────────────────────────────

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  db: { schema: 'acaiteria' },
  auth: { persistSession: false },
});

// ─── EXPRESS ─────────────────────────────────────────────────────────────────

const app = express();

const ORIGENS_PERMITIDAS = [
  'https://alyxsoftwares.vercel.app',
  /\.vercel\.app$/,
];

app.use(cors({
  origin:         ORIGENS_PERMITIDAS,
  methods:        ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  credentials:    true,
}));

app.use(express.json());


// ══════════════════════════════════════════════════════════
// CACHE DE SLUG → UUID (Regra 3: Interceptador de Slug)
// Evita consultas repetidas ao banco para tradução de slug
// ══════════════════════════════════════════════════════════

const _slugCache = new Map(); // slug → { uuid, ts }
const SLUG_CACHE_TTL_MS = 60 * 60 * 1000; // 1 hora

async function slugParaUUID(slug) {
  if (!slug) return null;

  // Verifica se o slug já é um UUID válido (36 chars com hifens)
  const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  if (UUID_REGEX.test(slug)) return slug;

  const cached = _slugCache.get(slug);
  if (cached && (Date.now() - cached.ts) < SLUG_CACHE_TTL_MS) {
    return cached.uuid;
  }

  // Busca no schema public (tabela lojas está no schema padrão)
  const { data, error } = await supabase
    .from('lojas')
    .select('id')
    .eq('slug', slug)
    .single();

  if (error || !data) {
    _slugCache.set(slug, { uuid: null, ts: Date.now() }); // Guarda o erro na memória para não sobrecarregar o banco
    return null;
  }

  _slugCache.set(slug, { uuid: data.id, ts: Date.now() });
  return data.id;
}

/**
 * Middleware que intercepta :loja_id na rota, traduz slug → UUID
 * e injeta req.lojaUUID para uso nas funções de negócio.
 */
async function resolverLojaId(req, res, next) {
  const slug = req.params.loja_id;
  if (!slug) return next();

  const uuid = await slugParaUUID(slug);
  if (!uuid) {
    return res.status(404).json({ sucesso: false, mensagem: 'Loja não encontrada.' });
  }

  req.lojaUUID = uuid;
  next();
}


// ══════════════════════════════════════════════════════════
// CACHE DE CARDÁPIO (in-memory, TTL 15 min)
// ══════════════════════════════════════════════════════════

const CACHE_TTL_MS   = 15 * 60 * 1000;
const _cacheCardapio = new Map(); // loja_uuid → { data, ts }

function invalidarCacheCardapio(lojaId) {
  _cacheCardapio.delete(lojaId);
}

function _getCacheCardapio(lojaId) {
  const entry = _cacheCardapio.get(lojaId);
  if (!entry) return null;
  if (Date.now() - entry.ts > CACHE_TTL_MS) {
    _cacheCardapio.delete(lojaId);
    return null;
  }
  return entry.data;
}

function _setCacheCardapio(lojaId, data) {
  _cacheCardapio.set(lojaId, { data, ts: Date.now() });
}


// ══════════════════════════════════════════════════════════
// UTILITÁRIOS INTERNOS
// ══════════════════════════════════════════════════════════

/** Retorna timestamp ISO para agora (Supabase aceita ISO 8601). */
function _agora() {
  return new Date().toISOString();
}

/** Retorna "dd/MM/yyyy" para hoje no fuso configurado. */
function _hojeString() {
  return new Date().toLocaleDateString('pt-BR', { timeZone: TZ });
}

/** Converte string "dd/MM/yyyy" em Date (meia-noite UTC). */
function _parseDataDDMMYYYY(str) {
  if (!str || typeof str !== 'string') return null;
  const partes = str.trim().split('/');
  if (partes.length !== 3) return null;
  const [d, m, a] = partes.map(Number);
  if (isNaN(d) || isNaN(m) || isNaN(a)) return null;
  return new Date(a, m - 1, d);
}

function _resumoVazio() {
  return {
    totalVendas: 0, totalDescontos: 0, ticketMedio: 0,
    qtdPedidos: 0, porOrigem: {}, porPagamento: {}, porStatus: {}, periodo: {},
  };
}

/** Lança erro caso o Supabase retorne erro. */
function _assertOk({ error }, contexto) {
  if (error) {
    console.error(`[Supabase/${contexto}]`, error);
    throw new Error(error.message || 'Erro no banco de dados.');
  }
}


// ══════════════════════════════════════════════════════════
// PERMISSÕES POR CARGO
// ══════════════════════════════════════════════════════════

const _PERMISSOES = {
  Dono:       ['abaPedidos', 'abaDelivery', 'abaRetirada', 'abaRelatorios', 'abaItens', 'abaMonteAcai', 'abaConfig'],
  Gerente:    ['abaPedidos', 'abaDelivery', 'abaRetirada', 'abaRelatorios', 'abaItens', 'abaMonteAcai'],
  Supervisor: ['abaPedidos', 'abaDelivery', 'abaRetirada', 'abaItens', 'abaMonteAcai'],
  Operador:   ['abaPedidos', 'abaDelivery', 'abaRetirada'],
  Entregador: ['abaDelivery'],
};

function getPermissoesCargo(cargo) {
  return _PERMISSOES[(cargo || '').toString().trim()] || ['abaDelivery'];
}


// ══════════════════════════════════════════════════════════
// MIDDLEWARES
// ══════════════════════════════════════════════════════════

function autenticar(req, res, next) {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.startsWith('Bearer ')
    ? authHeader.slice(7) : null;

  if (!token) {
    return res.status(401).json({ sucesso: false, mensagem: 'Token não fornecido.' });
  }

  try {
    req.usuario = jwt.verify(token, JWT_SECRET);
    next();
  } catch (e) {
    return res.status(401).json({
      sucesso: false,
      mensagem: e.name === 'TokenExpiredError'
        ? 'Sessão expirada. Faça login novamente.'
        : 'Token inválido.',
    });
  }
}

function exigirCargo(...cargosPermitidos) {
  return (req, res, next) => {
    if (!req.usuario) {
      return res.status(401).json({ sucesso: false, mensagem: 'Não autenticado.' });
    }
    if (!cargosPermitidos.includes(req.usuario.cargo)) {
      return res.status(403).json({
        sucesso: false,
        mensagem: `Acesso negado. Requer: ${cargosPermitidos.join(' ou ')}.`,
      });
    }
    next();
  };
}

function verificarLojaAcesso(req, res, next) {
  const lojaUUID = req.lojaUUID;
  if (!lojaUUID) return next();

  if (req.usuario.loja_id !== lojaUUID) {
    return res.status(403).json({
      sucesso: false,
      mensagem: 'Acesso negado. Esta loja não pertence à sua conta.',
    });
  }
  next();
}

async function verificarStatusLoja(req, res, next) {
  const lojaId = req.lojaUUID || (req.usuario && req.usuario.loja_id);
  if (!lojaId) return next();

  try {
    const { data, error } = await supabase
      .from('lojas')
      .select('status')
      .eq('id', lojaId)
      .single();

    if (error || !data) {
      return res.status(404).json({ sucesso: false, mensagem: 'Loja não encontrada.' });
    }

    if ((data.status || 'ativo').toLowerCase() === 'bloqueado') {
      return res.status(403).json({
        sucesso: false,
        mensagem: 'Acesso bloqueado. Verifique o pagamento da sua assinatura ou contate o suporte.',
      });
    }
    next();
  } catch (err) {
    console.error('[verificarStatusLoja]', err);
    return res.status(500).json({ sucesso: false, mensagem: 'Erro interno ao verificar status da loja.' });
  }
}

const guardLoja  = [resolverLojaId, autenticar, verificarLojaAcesso, verificarStatusLoja];
const guardCargo = (...cargos) => [...guardLoja, exigirCargo(...cargos)];

const handler = fn => async (req, res) => {
  try {
    const resultado = await fn(req, res);
    if (!res.headersSent) res.json(resultado);
  } catch (err) {
    console.error('[ERRO]', err);
    if (!res.headersSent)
      res.status(500).json({ sucesso: false, mensagem: err.message });
  }
};


// ══════════════════════════════════════════════════════════
// AUTENTICAÇÃO — LOGIN
// ══════════════════════════════════════════════════════════

async function validarLogin(lojaId, usuarioDigitado, senhaDigitada) {
  if (!lojaId || !usuarioDigitado || !senhaDigitada) {
    return { sucesso: false, mensagem: 'Loja, usuário e senha são obrigatórios.' };
  }

  const loginNorm = usuarioDigitado.toString().trim().toLowerCase();

  const { data: usrs, error: errUsr } = await supabase
    .from('usuarios')
    .select('*')
    .eq('loja_id', lojaId)
    .ilike('login', loginNorm)
    .limit(1);

  if (errUsr) throw new Error(errUsr.message);
  if (!usrs || usrs.length === 0)
    return { sucesso: false, mensagem: 'Usuário ou senha inválidos.' };

  const d = usrs[0];

  if ((d.senha || '').toString() !== senhaDigitada.toString()) {
    return { sucesso: false, mensagem: 'Usuário ou senha inválidos.' };
  }
  if ((d.status || '').toString().toLowerCase() !== 'ativo') {
    return { sucesso: false, mensagem: 'Acesso bloqueado. Contate a gerência.' };
  }

  // Verifica status da loja
  const { data: loja, error: errLoja } = await supabase
    .from('lojas')
    .select('status')
    .eq('id', lojaId)
    .single();

  if (errLoja || !loja) return { sucesso: false, mensagem: 'Loja não encontrada.' };
  if ((loja.status || 'ativo').toLowerCase() === 'bloqueado') {
    return { sucesso: false, mensagem: 'Esta loja está bloqueada. Verifique o pagamento da assinatura.' };
  }

  const cargo = (d.cargo || '').toString();
  return {
    sucesso:    true,
    cargo,
    nome:       (d.nome || '').toString(),
    fotoPerfil: (d.foto_perfil || '').toString(),
    permissoes: getPermissoesCargo(cargo),
  };
}

async function validarSupervisorOuAcima(lojaId, login, senha) {
  const auth = await validarLogin(lojaId, login, senha);
  if (!auth.sucesso) return { autorizado: false, mensagem: auth.mensagem };

  const cargosAutorizados = ['Dono', 'Gerente', 'Supervisor'];
  if (!cargosAutorizados.includes(auth.cargo)) {
    return { autorizado: false, mensagem: `Cargo "${auth.cargo}" não tem autoridade para esta ação.` };
  }
  return { autorizado: true, nome: auth.nome, cargo: auth.cargo, mensagem: 'Autorizado.' };
}


// ══════════════════════════════════════════════════════════
// CONFIGURAÇÕES
// ══════════════════════════════════════════════════════════

async function getConfiguracoes(lojaId) {
  const { data, error } = await supabase
    .from('configuracoes')
    .select('*')
    .eq('loja_id', lojaId)
    .single();

  if (error && error.code !== 'PGRST116') throw new Error(error.message);
  return data || {};
}

async function salvarConfiguracoesLote(lojaId, configObj) {
  // Garante que loja_id está presente e remove campos inválidos
  const payload = { ...configObj, loja_id: lojaId };

  const { error } = await supabase
    .from('configuracoes')
    .upsert(payload, { onConflict: 'loja_id' });

  if (error) throw new Error(error.message);
  invalidarCacheCardapio(lojaId);
  return { sucesso: true, mensagem: 'Configurações salvas.' };
}


// ══════════════════════════════════════════════════════════
// HELPERS SUPABASE — CRUD GENÉRICO
// ══════════════════════════════════════════════════════════

/**
 * Lê todos os registros de uma tabela filtrando por loja_id.
 */
async function lerTabela(lojaId, tabela) {
  let query = supabase.from(tabela).select('*').eq('loja_id', lojaId);
  
  // Aplica ordenação apenas nas tabelas que realmente possuem a coluna "ordem"
  const tabelasComOrdem = ['cardapio', 'acai_categorias', 'acai_ingredientes', 'acai_modelos', 'bairros'];
  
  if (tabelasComOrdem.includes(tabela)) {
    query = query.order('ordem', { ascending: true, nullsFirst: false });
  } else if (tabela === 'usuarios') {
    query = query.order('nome', { ascending: true }); // Organiza os usuários por ordem alfabética
  }
  
  const { data, error } = await query;
  if (error) {
    console.error(`[Erro na tabela ${tabela}]:`, error.message);
    throw new Error(error.message);
  }
  return data || [];
}

const _CACHE_TABELAS = new Set([
  'cardapio', 'acai_modelos', 'acai_categorias',
  'acai_ingredientes', 'bairros', 'configuracoes', 'cupons',
]);

/**
 * Upsert genérico. Se campoId estiver preenchido faz UPDATE, senão INSERT.
 * O Supabase gera o UUID automaticamente no INSERT.
 */
async function salvarRegistro(lojaId, tabela, dadosObj, campoId) {
  const id     = (dadosObj[campoId] || '').toString().trim();
  const ehNovo = !id;
  const payload = { ...dadosObj, loja_id: lojaId };

  let resultData;
  if (ehNovo) {
    // Remove o campo de ID para deixar o Postgres gerar
    delete payload[campoId];
    const { data, error } = await supabase
      .from(tabela)
      .insert(payload)
      .select()
      .single();
    if (error) throw new Error(error.message);
    resultData = data;
  } else {
    const { data, error } = await supabase
      .from(tabela)
      .upsert(payload, { onConflict: campoId })
      .select()
      .single();
    if (error) throw new Error(error.message);
    resultData = data;
  }

  if (_CACHE_TABELAS.has(tabela)) invalidarCacheCardapio(lojaId);
  return {
    sucesso:  true,
    mensagem: ehNovo ? 'Salvo!' : 'Atualizado!',
    id:       resultData[campoId],
    data:     resultData,
  };
}

async function deletarRegistro(lojaId, tabela, campoId, valorId) {
  const { error, count } = await supabase
    .from(tabela)
    .delete({ count: 'exact' })
    .eq('loja_id', lojaId)
    .eq(campoId, valorId);

  if (error) throw new Error(error.message);
  if (count === 0) return { sucesso: false, mensagem: 'Registro não encontrado.' };

  if (_CACHE_TABELAS.has(tabela)) invalidarCacheCardapio(lojaId);
  return { sucesso: true, mensagem: 'Excluído com sucesso!' };
}


// ══════════════════════════════════════════════════════════
// CARGAS DE DADOS — PAINEL, CONFIG, CARDÁPIO
// ══════════════════════════════════════════════════════════

async function getDadosPainelGeral(lojaId) {
  const [
    usuarios, itensFixos, acaiCategorias, acaiIngredientes,
    acaiModelos, bairros, cupons, taras, configuracoes,
    pedidosDia, deliveryAtivo,
  ] = await Promise.all([
    lerTabela(lojaId, 'usuarios'),
    lerTabela(lojaId, 'cardapio'),
    lerTabela(lojaId, 'acai_categorias'),
    lerTabela(lojaId, 'acai_ingredientes'),
    lerTabela(lojaId, 'acai_modelos'),
    lerTabela(lojaId, 'bairros'),
    lerTabela(lojaId, 'cupons'),
    lerTabela(lojaId, 'taras'),
    getConfiguracoes(lojaId),
    getPedidosDoDia(lojaId),
    getDeliveryEmAndamento(lojaId),
  ]);

  return {
    usuarios, itensFixos, acaiCategorias, acaiIngredientes,
    acaiModelos, bairros, cupons, taras, configuracoes,
    pedidosDia, deliveryAtivo,
  };
}

async function getDadosConfig(lojaId) {
  const [configuracoes, usuarios, bairros, cupons, taras] = await Promise.all([
    getConfiguracoes(lojaId),
    lerTabela(lojaId, 'usuarios'),
    lerTabela(lojaId, 'bairros'),
    lerTabela(lojaId, 'cupons'),
    lerTabela(lojaId, 'taras'),
  ]);
  return { configuracoes, usuarios, bairros, cupons, taras };
}

async function getDadosMonteAcai(lojaId) {
  const [acaiModelos, acaiCategorias, acaiIngredientes] = await Promise.all([
    lerTabela(lojaId, 'acai_modelos'),
    lerTabela(lojaId, 'acai_categorias'),
    lerTabela(lojaId, 'acai_ingredientes'),
  ]);
  return { acaiModelos, acaiCategorias, acaiIngredientes };
}

async function getCardapioClienteCache(lojaId) {
  const cached = _getCacheCardapio(lojaId);
  if (cached) return cached;

  const [config, prontos, tamanhos, categorias, ingredientes, bairros] = await Promise.all([
    getConfiguracoes(lojaId),
    lerTabela(lojaId, 'cardapio'),
    lerTabela(lojaId, 'acai_modelos'),
    lerTabela(lojaId, 'acai_categorias'),
    lerTabela(lojaId, 'acai_ingredientes'),
    lerTabela(lojaId, 'bairros'),
  ]);

  // Regra 5: booleanos nativos — disponivel === true (sem tradução SIM/NÃO)
  const pacote = {
    configuracoes: config,
    prontos:      prontos.filter(i => i.disponivel === true && i.mostrar_online !== false),
    tamanhos:     tamanhos.filter(m => m.disponivel === true),
    categorias,
    ingredientes: ingredientes.filter(i => i.disponivel === true),
    bairros:      bairros.filter(b => b.disponivel === true),
  };

  _setCacheCardapio(lojaId, pacote);
  return pacote;
}

async function getConfigPix(lojaId) {
  const config = await getConfiguracoes(lojaId);
  const token  = (config.mp_access_token || '').toString().trim();
  return {
    modoMP:        (config.pix_modo || 'MANUAL').toUpperCase(),
    mpConfigurado: token.length > 10,
  };
}


// ══════════════════════════════════════════════════════════
// PEDIDOS — CONSULTAS
// ══════════════════════════════════════════════════════════

async function getPedidosDoDia(lojaId) {
  return getPedidosPorPeriodo(lojaId, _hojeString(), _hojeString());
}

async function getPedidosPorPeriodo(lojaId, dataInicio, dataFim) {
  const dtIni = _parseDataDDMMYYYY(dataInicio);
  const dtFim = _parseDataDDMMYYYY(dataFim);
  if (!dtIni || !dtFim) return { pedidos: [], resumo: _resumoVazio() };

  // Força o fuso horário de Fortaleza (-03:00) para garantir o "hoje" correto
  const isoIni = `${dtIni.getFullYear()}-${String(dtIni.getMonth() + 1).padStart(2, '0')}-${String(dtIni.getDate()).padStart(2, '0')}T00:00:00.000-03:00`;
  const isoFim = `${dtFim.getFullYear()}-${String(dtFim.getMonth() + 1).padStart(2, '0')}-${String(dtFim.getDate()).padStart(2, '0')}T23:59:59.999-03:00`;

  const { data: pedidos, error } = await supabase
    .from('pedidos')
    .select('*')
    .eq('loja_id', lojaId)
    .gte('data_hora', isoIni)
    .lte('data_hora', isoFim)
    .order('data_hora', { ascending: false });

  if (error) throw new Error(error.message);
  const lista = pedidos || [];

  let totalDia = 0, totalDesc = 0;
  const contOrigem  = { BALCAO: 0, DELIVERY: 0, ONLINE: 0 };
  const contPgto    = {};
  const contStatus  = {};

  for (const p of lista) {
    const tot    = parseFloat(p.total_final   || 0);
    const desc   = parseFloat(p.desconto      || 0);
    const origem = (p.origem           || '').toUpperCase();
    const pgto   = (p.metodo_pagamento || '').toUpperCase();
    const status = (p.status           || '').toUpperCase();

    if (status !== 'CANCELADO') { totalDia += tot; totalDesc += desc; }
    contOrigem[origem] = (contOrigem[origem] || 0) + 1;
    contPgto[pgto]     = (contPgto[pgto]     || 0) + 1;
    contStatus[status] = (contStatus[status]  || 0) + 1;
  }

  const qtdAtivos = lista.filter(p => (p.status || '').toUpperCase() !== 'CANCELADO').length;

  return {
    pedidos: lista,
    resumo: {
      totalVendas:    Math.round(totalDia  * 100) / 100,
      totalDescontos: Math.round(totalDesc * 100) / 100,
      ticketMedio:    qtdAtivos > 0 ? Math.round((totalDia / qtdAtivos) * 100) / 100 : 0,
      qtdPedidos:     lista.length,
      porOrigem: contOrigem, porPagamento: contPgto, porStatus: contStatus,
      periodo: { inicio: dataInicio, fim: dataFim },
    },
  };
}

async function getRelatorioAvancado(lojaId, params) {
  if (!params?.dataInicio || !params?.dataFim) {
    const hoje = _hojeString();
    params = { ...params, dataInicio: hoje, dataFim: hoje };
  }

  const base = await getPedidosPorPeriodo(lojaId, params.dataInicio, params.dataFim);
  let pedidos = base.pedidos;

  if (params.pagamento?.trim()) pedidos = pedidos.filter(p => (p.metodo_pagamento || '').toUpperCase() === params.pagamento.toUpperCase());
  if (params.operador?.trim())  pedidos = pedidos.filter(p => (p.operador || '').toLowerCase().includes(params.operador.toLowerCase()));
  if (params.origem?.trim())    pedidos = pedidos.filter(p => (p.origem || '').toUpperCase() === params.origem.toUpperCase());
  if (params.status?.trim())    pedidos = pedidos.filter(p => (p.status || '').toUpperCase() === params.status.toUpperCase());

  let totalVendas = 0, totalDescontos = 0;
  const porOrigem = {}, porPagamento = {}, porStatus = {}, porOperador = {};

  for (const p of pedidos) {
    const st = (p.status           || '').toUpperCase();
    const og = (p.origem           || '').toUpperCase();
    const pg = (p.metodo_pagamento || '').toUpperCase();
    const op = (p.operador         || '').toString();
    if (st !== 'CANCELADO') {
      totalVendas    += parseFloat(p.total_final || 0);
      totalDescontos += parseFloat(p.desconto    || 0);
    }
    porOrigem[og]    = (porOrigem[og]    || 0) + 1;
    porPagamento[pg] = (porPagamento[pg] || 0) + 1;
    porStatus[st]    = (porStatus[st]    || 0) + 1;
    porOperador[op]  = (porOperador[op]  || 0) + 1;
  }

  const qtdAtivos = pedidos.filter(p => (p.status || '').toUpperCase() !== 'CANCELADO').length;

  return {
    pedidos,
    resumo: {
      totalVendas:    Math.round(totalVendas    * 100) / 100,
      totalDescontos: Math.round(totalDescontos * 100) / 100,
      ticketMedio:    qtdAtivos > 0 ? Math.round((totalVendas / qtdAtivos) * 100) / 100 : 0,
      qtdPedidos:     pedidos.length,
      porOrigem, porPagamento, porStatus, porOperador,
      periodo: { inicio: params.dataInicio, fim: params.dataFim },
    },
  };
}

async function buscarPedidos(lojaId, query) {
  if (!query?.toString().trim()) return [];
  const q = query.toString().trim().toLowerCase();

  const limite60dias = new Date();
  limite60dias.setDate(limite60dias.getDate() - 60);

  const { data: pedidos, error } = await supabase
    .from('pedidos')
    .select('*')
    .eq('loja_id', lojaId)
    .gte('data_hora', limite60dias.toISOString())
    .order('data_hora', { ascending: false })
    .limit(500);

  if (error) throw new Error(error.message);

  const resultado = [];
  for (const p of (pedidos || [])) {
    if (resultado.length >= 50) break;

    // Regra 6: cliente_info e itens_comprados são jsonb — já vêm como objeto/array
    const cliStr   = JSON.stringify(p.cliente_info    || '').toLowerCase();
    const itensStr = JSON.stringify(p.itens_comprados || '').toLowerCase();
    const idStr    = (p.id_venda || '').toLowerCase();

    if (idStr.includes(q) || cliStr.includes(q) || itensStr.includes(q)) {
      resultado.push(p);
    }
  }

  return resultado;
}

async function getAcompanhamentoPedido(lojaId, query) {
  if (!query || query.toString().trim().length < 5)
    return { erro: 'Digite pelo menos 5 caracteres.' };

  const q       = query.toString().trim();
  const qNorm   = q.replace(/\s+/g, '').toLowerCase();
  const qDigits = q.replace(/\D/g, '');

  const limiteHoje = new Date();
  limiteHoje.setHours(0, 0, 0, 0);

  const { data: pedidos, error } = await supabase
    .from('pedidos')
    .select('*')
    .eq('loja_id', lojaId)
    .order('data_hora', { ascending: false })
    .limit(500);

  if (error) throw new Error(error.message);

  let encontrados = [];

  for (const p of (pedidos || [])) {
    const idVenda  = (p.id_venda || '').replace(/\s+/g, '').toLowerCase();
    let isMatchID  = idVenda && (idVenda.includes(qNorm) || (qNorm.includes(idVenda) && idVenda.length > 4));

    let isMatchPhone = false;
    if (qDigits.length >= 7 && p.cliente_info) {
      // Regra 6: jsonb já é objeto
      const cli = (p.cliente_info || {});
      const tel  = (cli.telefone || '').replace(/\D/g, '');
      if (tel.length >= 7 && tel.includes(qDigits)) isMatchPhone = true;
    }

    if (isMatchID || isMatchPhone) {
      const dtPedido = p.data_hora ? new Date(p.data_hora) : null;
      if (isMatchPhone && !isMatchID) {
        if (dtPedido && dtPedido >= limiteHoje) encontrados.push(p);
      } else {
        encontrados.push(p);
      }
    }
  }

  if (encontrados.length === 0)
    return { erro: 'Pedido não encontrado de hoje. Verifique o número e tente novamente.' };

  encontrados.sort((a, b) => new Date(b.data_hora) - new Date(a.data_hora));
  encontrados = encontrados.slice(0, 3);

  const statusLabels = {
    NOVO: 'Recebido', PREPARANDO: 'Preparando', PRONTO: 'Pronto para retirada',
    EM_MONTAGEM: 'Em montagem', A_CAMINHO: 'Saiu para entrega',
    ENTREGUE: 'Entregue ✅', CANCELADO: 'Cancelado ❌',
  };

  return encontrados.map(p => {
    // Regra 6: jsonb já é objeto/array nativamente
    const arr        = Array.isArray(p.itens_comprados) ? p.itens_comprados
                     : (p.itens_comprados || []);
    const itensResumo = arr.map(it =>
      (it.descricao || '') + (it.preco ? ' — R$' + parseFloat(it.preco).toFixed(2).replace('.', ',') : '')
    ).join('\n');

    const cli         = (p.cliente_info || {});
    const statusVal   = (p.status || '').toUpperCase();

    return {
      id_venda:        p.id_venda        || '',
      origem:          (p.origem         || '').toUpperCase(),
      status:          statusVal,
      statusLabel:     statusLabels[statusVal] || statusVal,
      data_hora:       p.data_hora || '',
      total_final:     parseFloat(p.total_final || 0),
      entregador_nome: (p.entregador_nome || '').toString(),
      nomeCliente:     cli.nome     || '',
      endereco:        cli.endereco || '',
      itensResumo,
    };
  });
}

async function getDeliveryEmAndamento(lojaId) {
  const limite7dias = new Date();
  limite7dias.setDate(limite7dias.getDate() - 7);
  limite7dias.setHours(0, 0, 0, 0);

  const { data, error } = await supabase
    .from('pedidos')
    .select('*')
    .eq('loja_id', lojaId)
    .in('origem', ['DELIVERY', 'ONLINE'])
    .neq('status', 'CANCELADO')
    .gte('data_hora', limite7dias.toISOString())
    .order('data_hora', { ascending: false });

  if (error) throw new Error(error.message);

  const limiteHoje = new Date();
  limiteHoje.setHours(0, 0, 0, 0);

  return (data || []).filter(p => {
    const st = (p.status || '').toUpperCase();
    if (st === 'ENTREGUE') {
      return p.data_hora && new Date(p.data_hora) >= limiteHoje;
    }
    return true;
  });
}

async function getDadosPolling(lojaId) {
  const [pedidosDia, deliveryAtivo] = await Promise.all([
    getPedidosDoDia(lojaId),
    getDeliveryEmAndamento(lojaId),
  ]);
  return { pedidosDia, deliveryAtivo };
}


// ══════════════════════════════════════════════════════════
// PEDIDOS — ESCRITA
// ══════════════════════════════════════════════════════════

const ONESIGNAL_REST_API_KEY = process.env.ONESIGNAL_REST_API_KEY || '';
const ONESIGNAL_APP_ID       = 'c7565fa8-cd8c-4264-85b4-1a9d9cf150af';

async function dispararPushOneSignal(lojaId, titulo, mensagem, ignorarEntregador = false) {
  if (!ONESIGNAL_REST_API_KEY) return;
  try {
    const filtros = [{ field: 'tag', key: 'loja_id', relation: '=', value: lojaId }];
    if (ignorarEntregador) {
      filtros.push({ field: 'tag', key: 'cargo', relation: '!=', value: 'Entregador' });
    }
    await fetch('https://onesignal.com/api/v1/notifications', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Key ${ONESIGNAL_REST_API_KEY}` },
      body:    JSON.stringify({ app_id: ONESIGNAL_APP_ID, filters: filtros, headings: { en: titulo }, contents: { en: mensagem } }),
    });
  } catch (e) { console.error('[OneSignal] Erro:', e); }
}

/**
 * Registra um pedido na tabela pedidos.
 * Regra 6: cliente_info e itens_comprados devem ser objetos/arrays (jsonb).
 */
async function registrarVendaPDV(lojaId, pedido) {
  // Garante que jsonb são objetos, não strings
  const payload = {
    loja_id:          lojaId,
    origem:           pedido.origem           || '',
    data_hora:        pedido.data_hora        || _agora(),
    operador:         pedido.operador         || 'CAIXA',
    cliente_info:     (pedido.cliente_info || {}),
    itens_comprados:  (pedido.itens_comprados || []),
    subtotal:         pedido.subtotal         || 0,
    desconto:         pedido.desconto         || 0,
    taxa_entrega:     pedido.taxa_entrega      || 0,
    total_final:      pedido.total_final       || 0,
    metodo_pagamento: pedido.metodo_pagamento  || '',
    status:           pedido.status            || 'NOVO',
    peso_bruto_g:     pedido.peso_bruto_g      || 0,
    id_tara:          pedido.id_tara           || null,
    peso_tara_g:      pedido.peso_tara_g       || 0,
    peso_liquido_g:   pedido.peso_liquido_g    || 0,
    preco_kg:         pedido.preco_kg          || 0,
    troco:            pedido.troco             || 0,
    entregador_nome:  pedido.entregador_nome   || '',
    cancelado_por:    pedido.cancelado_por     || '',
  };

  const { data, error } = await supabase
    .from('pedidos')
    .insert(payload)
    .select('id_venda')
    .single();

  if (error) throw new Error(error.message);

  const origemUp = (pedido.origem || '').toUpperCase();
  if (origemUp === 'DELIVERY' || origemUp === 'ONLINE') {
    const cli = payload.cliente_info;
    const isRetirada = (cli.endereco || '') === 'Retirada na loja';
    dispararPushOneSignal(
      lojaId,
      isRetirada ? '🛍️ Nova Retirada!'            : '🔔 Novo Pedido de Delivery!',
      isRetirada ? 'Cliente vem buscar na loja.' : 'Uma nova entrega caiu no painel.',
      isRetirada
    );
  }

  return { sucesso: true, mensagem: 'Pedido registrado!', id: data.id_venda };
}

async function registrarVendaBalcao(lojaId, dados) {
  const pesoBruto = parseFloat(dados.pesoBruto_g || 0);
  const pesoTara  = parseFloat(dados.pesoTara_g  || 0);
  const precoKG   = parseFloat(dados.precoKG     || 0);
  const pesoLiq   = Math.max(0, pesoBruto - pesoTara);
  const valorAcai = Math.round((pesoLiq / 1000) * precoKG * 100) / 100;

  const itensExtras = dados.itensExtras || [];
  const somaExtras  = itensExtras.reduce((s, i) => s + parseFloat(i.preco || 0), 0);
  const subtotal    = Math.round((valorAcai + somaExtras) * 100) / 100;
  const desconto    = parseFloat(dados.desconto  || 0);
  const total       = Math.max(0, Math.round((subtotal - desconto) * 100) / 100);
  const valorPago   = parseFloat(dados.valorPago || 0);
  const troco       = dados.pagamento === 'DINHEIRO'
    ? Math.max(0, Math.round((valorPago - total) * 100) / 100) : 0;

  const itemAcai   = pesoBruto > 0
    ? [{ tipo: 'acai_balanca', descricao: `Açaí — ${pesoLiq}g`, detalhes: `${pesoBruto}g bruto − ${pesoTara}g tara`, preco: valorAcai }]
    : [];
  const todosItens = itemAcai.concat(itensExtras);

  if (dados.nomeCliente?.trim()) {
    const cpfLimpo = dados.cpfCliente ? dados.cpfCliente.toString().replace(/\D/g, '') : '';
    const jaExiste = cpfLimpo ? await buscarClientePorCPF(lojaId, cpfLimpo) : null;
    if (!jaExiste) {
      await salvarCliente(lojaId, {
        nome:     dados.nomeCliente.trim(),
        cpf:      cpfLimpo || '',
        telefone: dados.telefone ? dados.telefone.replace(/\D/g, '') : '',
      });
    }
  }

  const pedido = {
    origem: 'BALCAO', data_hora: _agora(),
    operador: dados.operador || 'CAIXA',
    cliente_info: { nome: dados.nomeCliente || '', cpf: dados.cpfCliente || '', telefone: dados.telefone || '' },
    itens_comprados: todosItens,
    subtotal, desconto, taxa_entrega: 0, total_final: total,
    metodo_pagamento: dados.pagamento || '', status: 'ENTREGUE',
    peso_bruto_g: pesoBruto, id_tara: dados.idTara || null, peso_tara_g: pesoTara,
    peso_liquido_g: pesoLiq, preco_kg: precoKG, troco,
    entregador_nome: '', cancelado_por: '',
  };

  const resultado = await registrarVendaPDV(lojaId, pedido);
  if (resultado.sucesso) Object.assign(resultado, { troco, total, pesoLiq });
  return resultado;
}

async function registrarVendaDelivery(lojaId, dados) {
  const itens    = dados.itens || [];
  const subtotal = Math.round(itens.reduce((s, i) => s + parseFloat(i.preco || 0), 0) * 100) / 100;
  const desconto = parseFloat(dados.desconto    || 0);
  const taxaEnt  = parseFloat(dados.taxaEntrega || 0);
  const total    = Math.max(0, Math.round((subtotal - desconto + taxaEnt) * 100) / 100);
  const valorPago = parseFloat(dados.valorPago  || 0);
  const troco    = dados.pagamento === 'DINHEIRO'
    ? Math.max(0, Math.round((valorPago - total) * 100) / 100) : 0;

  const pedido = {
    origem: 'DELIVERY', data_hora: _agora(),
    operador: dados.operador || 'CAIXA',
    cliente_info: { nome: dados.nomeCliente || '', cpf: dados.cpfCliente || '', telefone: dados.telefone || '', endereco: dados.endereco || '' },
    itens_comprados: itens,
    subtotal, desconto, taxa_entrega: taxaEnt, total_final: total,
    metodo_pagamento: dados.pagamento || '', status: 'NOVO',
    peso_bruto_g: 0, id_tara: null, peso_tara_g: 0, peso_liquido_g: 0,
    preco_kg: 0, troco, entregador_nome: '', cancelado_por: '',
  };

  const resultado = await registrarVendaPDV(lojaId, pedido);
  if (resultado.sucesso) Object.assign(resultado, { troco, total });
  return resultado;
}

async function finalizarPedidoOnline(lojaId, dadosPedido) {
  if (!dadosPedido?.itens?.length) throw new Error('Pedido vazio ou formato inválido.');

  const config = await getConfiguracoes(lojaId);
  if ((config.status_loja || 'AUTOMATICO') === 'FORCAR_FECHADO')
    throw new Error('A loja está fechada no momento. Tente mais tarde.');

  const subtotalReal    = Math.round(dadosPedido.itens.reduce((s, i) => s + parseFloat(i.preco || 0), 0) * 100) / 100;
  const subtotalEnviado = parseFloat(dadosPedido.subtotal || 0);
  if (Math.abs(subtotalReal - subtotalEnviado) > 0.05)
    throw new Error('Divergência financeira detectada. Pedido rejeitado por segurança.');

  const desconto = parseFloat(dadosPedido.desconto    || 0);
  const taxaEnt  = parseFloat(dadosPedido.taxaEntrega || 0);
  const total    = Math.max(0, Math.round((subtotalReal - desconto + taxaEnt) * 100) / 100);

  const pedido = {
    origem: 'ONLINE', data_hora: _agora(), operador: 'APP',
    cliente_info:    { nome: dadosPedido.nomeCliente || '', cpf: '', telefone: dadosPedido.telefone || '', endereco: dadosPedido.endereco || '' },
    itens_comprados: dadosPedido.itens,
    subtotal: subtotalReal, desconto, taxa_entrega: taxaEnt, total_final: total,
    metodo_pagamento: dadosPedido.pagamento || '', status: 'NOVO',
    peso_bruto_g: 0, id_tara: null, peso_tara_g: 0, peso_liquido_g: 0,
    preco_kg: 0, troco: 0, entregador_nome: '', cancelado_por: '',
  };

  return registrarVendaPDV(lojaId, pedido);
}

async function atualizarStatusPedido(lojaId, idVenda, novoStatus) {
  const statusValidos = ['NOVO','PREPARANDO','PRONTO','ENTREGUE','CANCELADO','EM_MONTAGEM','A_CAMINHO','AGUARDANDO_PIX'];
  if (!statusValidos.includes(novoStatus))
    return { sucesso: false, mensagem: `Status inválido: ${novoStatus}` };

  const { error, count } = await supabase
    .from('pedidos')
    .update({ status: novoStatus })
    .eq('loja_id', lojaId)
    .eq('id_venda', idVenda)
    .select('id_venda', { count: 'exact' });

  if (error) throw new Error(error.message);
  if (count === 0) return { sucesso: false, mensagem: `Pedido "${idVenda}" não encontrado.` };
  return { sucesso: true };
}

async function atualizarStatusEntrega(lojaId, idVenda, novoStatus) {
  const permitidos = ['EM_MONTAGEM', 'A_CAMINHO', 'ENTREGUE'];
  if (!permitidos.includes(novoStatus))
    return { sucesso: false, mensagem: `Status inválido para entrega: ${novoStatus}` };
  return atualizarStatusPedido(lojaId, idVenda, novoStatus);
}

async function cancelarPedidoAutorizado(lojaId, idVenda, loginAuth, senhaAuth) {
  const auth = await validarSupervisorOuAcima(lojaId, loginAuth, senhaAuth);
  if (!auth.autorizado) return { sucesso: false, mensagem: auth.mensagem };

  // Busca o pedido para verificar status atual
  const { data: pedido, error: errBusca } = await supabase
    .from('pedidos')
    .select('status, origem')
    .eq('loja_id', lojaId)
    .eq('id_venda', idVenda)
    .single();

  if (errBusca || !pedido) return { sucesso: false, mensagem: `Pedido "${idVenda}" não encontrado.` };

  const statusAtual = (pedido.status || '').toUpperCase();
  const origemAtual = (pedido.origem || '').toUpperCase();

  if (statusAtual === 'CANCELADO') return { sucesso: false, mensagem: 'Pedido já cancelado.' };
  if (statusAtual === 'ENTREGUE' && origemAtual !== 'BALCAO') return { sucesso: false, mensagem: 'Pedido já entregue.' };

  const { error } = await supabase
    .from('pedidos')
    .update({
      status:        'CANCELADO',
      cancelado_por: `${auth.nome} (${auth.cargo}) — ${new Date().toLocaleString('pt-BR', { timeZone: TZ })}`,
    })
    .eq('loja_id', lojaId)
    .eq('id_venda', idVenda);

  if (error) throw new Error(error.message);
  return { sucesso: true, mensagem: `Pedido cancelado por ${auth.nome} (${auth.cargo}).` };
}

async function pegarPedidoDelivery(lojaId, idVenda, nomeEntregador) {
  if (!idVenda || !nomeEntregador?.toString().trim())
    return { sucesso: false, mensagem: 'ID do pedido e nome do entregador são obrigatórios.' };

  const nomeTrimmed = nomeEntregador.toString().trim();

  const { data: pedido, error: errBusca } = await supabase
    .from('pedidos')
    .select('status, entregador_nome')
    .eq('loja_id', lojaId)
    .eq('id_venda', idVenda)
    .single();

  if (errBusca || !pedido) return { sucesso: false, mensagem: 'Pedido não encontrado.' };

  const status      = (pedido.status           || '').toUpperCase();
  const entregAtual = (pedido.entregador_nome   || '').toString().trim();

  if (status === 'ENTREGUE')  return { sucesso: false, mensagem: 'Pedido já entregue.' };
  if (status === 'CANCELADO') return { sucesso: false, mensagem: 'Pedido cancelado.' };
  if (entregAtual && entregAtual.toLowerCase() !== nomeTrimmed.toLowerCase()) {
    return { sucesso: false, mensagem: `Este pedido já foi pego por ${entregAtual}.`, entregadorAtual: entregAtual };
  }

  const { error } = await supabase
    .from('pedidos')
    .update({ entregador_nome: nomeTrimmed, status: 'EM_MONTAGEM' })
    .eq('loja_id', lojaId)
    .eq('id_venda', idVenda);

  if (error) throw new Error(error.message);
  return { sucesso: true, mensagem: `Pedido atribuído a ${nomeTrimmed}.` };
}


// ══════════════════════════════════════════════════════════
// CADASTROS — CRUD POR TABELA
// ══════════════════════════════════════════════════════════

const salvarUsuario         = (lojaId, d) => salvarRegistro(lojaId, 'usuarios',          d, 'id_usuario');
const excluirUsuario        = (lojaId, id) => deletarRegistro(lojaId, 'usuarios',          'id_usuario',     id);
const salvarAcaiCategoria   = (lojaId, d) => salvarRegistro(lojaId, 'acai_categorias',   d, 'id_categoria');
const excluirAcaiCategoria  = (lojaId, id) => deletarRegistro(lojaId, 'acai_categorias',  'id_categoria',   id);
const salvarAcaiIngrediente = (lojaId, d) => salvarRegistro(lojaId, 'acai_ingredientes', d, 'id_ingrediente');
const excluirAcaiIngrediente= (lojaId, id) => deletarRegistro(lojaId, 'acai_ingredientes','id_ingrediente', id);
const salvarAcaiModelo      = (lojaId, d) => salvarRegistro(lojaId, 'acai_modelos',      d, 'id_modelo');
const excluirAcaiModelo     = (lojaId, id) => deletarRegistro(lojaId, 'acai_modelos',      'id_modelo',      id);
const salvarItemFixo        = (lojaId, d) => salvarRegistro(lojaId, 'cardapio',          d, 'id_item');
const excluirItemFixo       = (lojaId, id) => deletarRegistro(lojaId, 'cardapio',          'id_item',        id);
const salvarBairro          = (lojaId, d) => salvarRegistro(lojaId, 'bairros',           d, 'id_bairro');
const excluirBairro         = (lojaId, id) => deletarRegistro(lojaId, 'bairros',           'id_bairro',      id);
const salvarCupom           = (lojaId, d) => salvarRegistro(lojaId, 'cupons',            d, 'codigo_cupom');
const excluirCupom          = (lojaId, id) => deletarRegistro(lojaId, 'cupons',            'codigo_cupom',   id);
const salvarTara            = (lojaId, d) => salvarRegistro(lojaId, 'taras',             d, 'id_tara');
const excluirTara           = (lojaId, id) => deletarRegistro(lojaId, 'taras',             'id_tara',        id);

async function salvarCliente(lojaId, dados) {
  if (!dados.data_cadastro) dados.data_cadastro = _agora();
  return salvarRegistro(lojaId, 'clientes', dados, 'id_cliente');
}

async function buscarClientePorCPF(lojaId, cpf) {
  const cpfLimpo = cpf.toString().replace(/\D/g, '');
  if (cpfLimpo.length !== 11) return null;

  const { data, error } = await supabase
    .from('clientes')
    .select('*')
    .eq('loja_id', lojaId)
    .eq('cpf', cpfLimpo)
    .limit(1);

  if (error) throw new Error(error.message);
  return data && data.length > 0 ? data[0] : null;
}

async function buscarClientePorTelefone(lojaId, telefone) {
  const telLimpo = telefone.toString().replace(/\D/g, '');

  const { data, error } = await supabase
    .from('clientes')
    .select('*')
    .eq('loja_id', lojaId)
    .eq('telefone', telLimpo)
    .limit(1);

  if (error) throw new Error(error.message);
  return data && data.length > 0 ? data[0] : null;
}


// ══════════════════════════════════════════════════════════
// VALIDAÇÃO DE CUPOM
// ══════════════════════════════════════════════════════════

async function validarCupom(lojaId, codigo) {
  if (!codigo?.toString().trim()) return { valido: false, mensagem: 'Digite um código de cupom.' };

  const codigoUpper = codigo.toString().trim().toUpperCase();

  const { data, error } = await supabase
    .from('cupons')
    .select('*')
    .eq('loja_id', lojaId)
    .eq('codigo_cupom', codigoUpper)
    .limit(1);

  if (error) throw new Error(error.message);
  if (!data || data.length === 0) return { valido: false, mensagem: 'Cupom não encontrado.' };

  const d = data[0];

  // Regra 5: booleano nativo — ativo é boolean true/false
  if (d.ativo !== true) return { valido: false, mensagem: 'Cupom inativo ou esgotado.' };

  if (d.validade) {
    const validade = new Date(d.validade);
    if (!isNaN(validade.getTime())) {
      validade.setHours(23, 59, 59, 999);
      if (validade < new Date()) return { valido: false, mensagem: 'Cupom expirado.' };
    }
  }

  const tipo         = (d.tipo_desconto || 'VALOR').toUpperCase();
  // Regra 5: usar_cardapio é boolean nativo
  const usarCardapio = d.usar_cardapio !== false;
  const valorBruto   = parseFloat(d.valor_desconto) || 0;

  const msgDesc = tipo === 'PERCENTUAL'
    ? `Desconto de ${valorBruto.toFixed(0)}% aplicado 🎉`
    : `Desconto de R$ ${valorBruto.toFixed(2).replace('.', ',')} aplicado 🎉`;

  return {
    valido:       true,
    codigo:       d.codigo_cupom || '',
    tipo,
    valor:        valorBruto,
    desconto:     tipo === 'VALOR' ? valorBruto : 0,
    usarCardapio,
    mensagem:     msgDesc,
  };
}


// ══════════════════════════════════════════════════════════
// MERCADO PAGO — PIX AUTOMÁTICO
// ══════════════════════════════════════════════════════════

async function gerarPixMP(lojaId, total, idVenda, emailPagador) {
  const config = await getConfiguracoes(lojaId);
  const token  = (config.mp_access_token || '').toString().trim();
  if (!token) return { sucesso: false, mensagem: 'Token do Mercado Pago não configurado.' };

  const idempKey = 'PIX-' + Date.now() + '-' + Math.floor(Math.random() * 1000);
  const resp = await fetch('https://api.mercadopago.com/v1/payments', {
    method:  'POST',
    headers: {
      'Content-Type':      'application/json',
      'Authorization':     `Bearer ${token}`,
      'X-Idempotency-Key': idempKey,
    },
    body: JSON.stringify({
      transaction_amount: Math.round(parseFloat(total) * 100) / 100,
      description:        `Pedido ${idVenda}`,
      payment_method_id:  'pix',
      payer: { email: emailPagador || 'contato@acaiteria.com.br' },
    }),
    signal: AbortSignal.timeout(15000),
  });
  const r = await resp.json();

  if (r?.id && r?.point_of_interaction?.transaction_data) {
    const td = r.point_of_interaction.transaction_data;
    return {
      sucesso:       true,
      idPagamento:   r.id.toString(),
      pixCopiaECola: td.qr_code        || '',
      qrCodeBase64:  td.qr_code_base64 || '',
      expiracao:     r.date_of_expiration || '',
    };
  }

  return { sucesso: false, mensagem: (r?.message) || 'Resposta inesperada do Mercado Pago.' };
}

async function verificarPagamentoMP(lojaId, idPagamento) {
  const config = await getConfiguracoes(lojaId);
  const token  = (config.mp_access_token || '').toString().trim();
  if (!token || !idPagamento) return { status: 'erro', detalhe: 'Dados inválidos.' };

  const resp = await fetch(`https://api.mercadopago.com/v1/payments/${idPagamento}`, {
    headers: { 'Authorization': `Bearer ${token}` },
    signal:  AbortSignal.timeout(10000),
  });
  const r = await resp.json();
  return { status: r.status || 'erro', detalhe: r.status_detail || '' };
}

async function finalizarPedidoOnlineComPix(lojaId, dadosPedido) {
  const config  = await getConfiguracoes(lojaId);
  const modoMP  = (config.pix_modo || 'MANUAL').toUpperCase();

  if (modoMP !== 'AUTO' && modoMP !== 'AUTOMATICO' && modoMP !== 'AUTOMÁTICO') {
    return finalizarPedidoOnline(lojaId, dadosPedido);
  }

  if (!dadosPedido?.itens?.length) throw new Error('Pedido vazio ou formato inválido.');
  if ((config.status_loja || 'AUTOMATICO') === 'FORCAR_FECHADO')
    throw new Error('A loja está fechada no momento.');

  const subtotalReal    = Math.round(dadosPedido.itens.reduce((s, i) => s + parseFloat(i.preco || 0), 0) * 100) / 100;
  const subtotalEnviado = parseFloat(dadosPedido.subtotal || 0);
  if (Math.abs(subtotalReal - subtotalEnviado) > 0.05)
    throw new Error('Divergência financeira detectada. Pedido rejeitado por segurança.');

  const desconto    = parseFloat(dadosPedido.desconto    || 0);
  const taxaEnt     = parseFloat(dadosPedido.taxaEntrega || 0);
  const total       = Math.max(0, Math.round((subtotalReal - desconto + taxaEnt) * 100) / 100);
  const idVendaTemp = 'WEB-' + Date.now().toString().substring(5);

  const pix = await gerarPixMP(
    lojaId, total, idVendaTemp,
    dadosPedido.telefone ? `${dadosPedido.telefone.replace(/\D/g, '')}@mp.br` : 'contato@acaiteria.com.br'
  );

  if (!pix.sucesso) {
    return { sucesso: false, mensagem: `PIX não gerado: ${pix.mensagem}. Tente outra forma de pagamento.` };
  }

  const pedido = {
    origem: 'ONLINE', data_hora: _agora(), operador: 'APP',
    cliente_info:    { nome: dadosPedido.nomeCliente || '', cpf: '', telefone: dadosPedido.telefone || '', endereco: dadosPedido.endereco || '' },
    itens_comprados: dadosPedido.itens,
    subtotal: subtotalReal, desconto, taxa_entrega: taxaEnt, total_final: total,
    metodo_pagamento: 'PIX', status: 'AGUARDANDO_PIX',
    peso_bruto_g: 0, id_tara: null, peso_tara_g: 0, peso_liquido_g: 0,
    preco_kg: 0, troco: 0, entregador_nome: '', cancelado_por: '',
  };

  const res = await registrarVendaPDV(lojaId, pedido);
  if (!res.sucesso) return res;

  return {
    sucesso: true, id: res.id, total,
    pixCopiaECola: pix.pixCopiaECola, qrCodeBase64: pix.qrCodeBase64, idPagamento: pix.idPagamento,
  };
}

async function confirmarPagamentoELiberarPedido(lojaId, idVenda, idPagamento) {
  if (!idVenda || !idPagamento) return { status: 'erro', mensagem: 'Parâmetros inválidos.' };

  const verif = await verificarPagamentoMP(lojaId, idPagamento);

  if (verif.status === 'approved') {
    const upd = await atualizarStatusPedido(lojaId, idVenda, 'NOVO');
    return { status: 'approved', liberado: upd.sucesso, mensagem: upd.sucesso ? 'Pagamento confirmado! Pedido enviado para preparo.' : upd.mensagem };
  }
  if (verif.status === 'cancelled' || verif.status === 'rejected') {
    await atualizarStatusPedido(lojaId, idVenda, 'CANCELADO');
    return { status: verif.status, mensagem: 'PIX cancelado ou rejeitado. Tente novamente.' };
  }

  return { status: 'pending', mensagem: 'Aguardando pagamento...' };
}


// ══════════════════════════════════════════════════════════
// ROTAS — AUTENTICAÇÃO
// ══════════════════════════════════════════════════════════

app.post('/api/lojas/:loja_id/auth/login', resolverLojaId, handler(async req => {
  const lojaId    = req.lojaUUID;
  const resultado = await validarLogin(lojaId, req.body.usuario, req.body.senha);

  if (resultado.sucesso) {
    resultado.token = jwt.sign(
      { loja_id: lojaId, id: req.body.usuario, nome: resultado.nome, cargo: resultado.cargo },
      JWT_SECRET,
      { expiresIn: '8h' }
    );
  }
  return resultado;
}));

app.post('/api/lojas/:loja_id/auth/validar-supervisor', resolverLojaId, handler(req =>
  validarSupervisorOuAcima(req.lojaUUID, req.body.login, req.body.senha)
));


// ══════════════════════════════════════════════════════════
// ROTAS — WEBHOOK ASAAS
// ══════════════════════════════════════════════════════════

app.post('/api/webhooks/asaas', async (req, res) => {
  try {
    const tokenRecebido = req.headers['asaas-access-token'] || req.headers['x-access-token'] || '';
    if (ASAAS_WEBHOOK_TOKEN && tokenRecebido !== ASAAS_WEBHOOK_TOKEN) {
      console.warn('[Webhook Asaas] Token inválido:', tokenRecebido);
      return res.status(401).json({ sucesso: false, mensagem: 'Token inválido.' });
    }

    const { event, payment } = req.body;
    if (!event || !payment?.customer) {
      return res.status(400).json({ sucesso: false, mensagem: 'Payload inválido.' });
    }

    const customerId        = payment.customer;
    const eventosMonitorados = ['PAYMENT_OVERDUE', 'PAYMENT_RECEIVED', 'PAYMENT_RESTORED'];

    if (!eventosMonitorados.includes(event)) {
      return res.json({ sucesso: true, mensagem: `Evento "${event}" ignorado.` });
    }

    // Busca loja pelo asaas_customer_id (tabela lojas está no schema public)
    const { data: lojas, error } = await supabase
      .from('lojas')
      .select('id')
      .eq('asaas_customer_id', customerId)
      .limit(1);

    if (error || !lojas || lojas.length === 0) {
      console.warn(`[Webhook Asaas] Nenhuma loja para customer: ${customerId}`);
      return res.status(404).json({ sucesso: false, mensagem: 'Loja não encontrada para este customer.' });
    }

    const lojaId    = lojas[0].id;
    const novoStatus = event === 'PAYMENT_OVERDUE' ? 'bloqueado' : 'ativo';

    await supabase
      .from('lojas')
      .update({ status: novoStatus, ultimo_evento_asaas: event, atualizado_em: _agora() })
      .eq('id', lojaId);

    if (novoStatus === 'bloqueado') invalidarCacheCardapio(lojaId);

    console.log(`[Webhook Asaas] Loja "${lojaId}" → status "${novoStatus}" (evento: ${event})`);
    return res.json({ sucesso: true, lojaId, novoStatus });

  } catch (err) {
    console.error('[Webhook Asaas] Erro:', err);
    return res.status(500).json({ sucesso: false, mensagem: err.message });
  }
});


// ══════════════════════════════════════════════════════════
// ROTAS — CARGAS DE DADOS
// ══════════════════════════════════════════════════════════

app.get('/api/lojas/:loja_id/dados-painel',
  ...guardCargo('Dono', 'Gerente'),
  handler(req => getDadosPainelGeral(req.lojaUUID)));

app.get('/api/lojas/:loja_id/dados-config',
  ...guardCargo('Dono'),
  handler(req => getDadosConfig(req.lojaUUID)));

app.get('/api/lojas/:loja_id/dados-monte-acai',
  resolverLojaId,
  handler(req => getDadosMonteAcai(req.lojaUUID)));

app.get('/api/lojas/:loja_id/cardapio',
  resolverLojaId,
  verificarStatusLoja,
  handler(req => getCardapioClienteCache(req.lojaUUID)));

app.get('/api/lojas/:loja_id/polling',
  ...guardLoja,
  handler(req => getDadosPolling(req.lojaUUID)));

app.get('/api/lojas/:loja_id/config-pix',
  ...guardCargo('Dono'),
  handler(req => getConfigPix(req.lojaUUID)));

app.get('/api/lojas/:loja_id/delivery-ativo',
  ...guardLoja,
  handler(req => getDeliveryEmAndamento(req.lojaUUID)));


// ══════════════════════════════════════════════════════════
// ROTAS — CONFIGURAÇÕES
// ══════════════════════════════════════════════════════════

app.post('/api/lojas/:loja_id/configuracoes',
  ...guardCargo('Dono'),
  handler(req => salvarConfiguracoesLote(req.lojaUUID, req.body)));


// ══════════════════════════════════════════════════════════
// ROTAS — USUÁRIOS
// ══════════════════════════════════════════════════════════

app.post('/api/lojas/:loja_id/usuarios',
  ...guardCargo('Dono'),
  handler(req => salvarUsuario(req.lojaUUID, req.body)));

app.delete('/api/lojas/:loja_id/usuarios/:id',
  ...guardCargo('Dono'),
  handler(req => excluirUsuario(req.lojaUUID, req.params.id)));


// ══════════════════════════════════════════════════════════
// ROTAS — CARDÁPIO / CADASTROS
// ══════════════════════════════════════════════════════════

app.post('/api/lojas/:loja_id/acai-categorias',
  ...guardCargo('Dono'),
  handler(req => salvarAcaiCategoria(req.lojaUUID, req.body)));

app.delete('/api/lojas/:loja_id/acai-categorias/:id',
  ...guardCargo('Dono'),
  handler(req => excluirAcaiCategoria(req.lojaUUID, req.params.id)));

app.post('/api/lojas/:loja_id/acai-ingredientes',
  ...guardCargo('Dono'),
  handler(req => salvarAcaiIngrediente(req.lojaUUID, req.body)));

app.delete('/api/lojas/:loja_id/acai-ingredientes/:id',
  ...guardCargo('Dono'),
  handler(req => excluirAcaiIngrediente(req.lojaUUID, req.params.id)));

app.post('/api/lojas/:loja_id/acai-modelos',
  ...guardCargo('Dono'),
  handler(req => salvarAcaiModelo(req.lojaUUID, req.body)));

app.delete('/api/lojas/:loja_id/acai-modelos/:id',
  ...guardCargo('Dono'),
  handler(req => excluirAcaiModelo(req.lojaUUID, req.params.id)));

app.post('/api/lojas/:loja_id/itens-fixos',
  ...guardCargo('Dono'),
  handler(req => salvarItemFixo(req.lojaUUID, req.body)));

app.delete('/api/lojas/:loja_id/itens-fixos/:id',
  ...guardCargo('Dono'),
  handler(req => excluirItemFixo(req.lojaUUID, req.params.id)));

app.post('/api/lojas/:loja_id/bairros',
  ...guardCargo('Dono'),
  handler(req => salvarBairro(req.lojaUUID, req.body)));

app.delete('/api/lojas/:loja_id/bairros/:id',
  ...guardCargo('Dono'),
  handler(req => excluirBairro(req.lojaUUID, req.params.id)));

app.post('/api/lojas/:loja_id/cupons',
  ...guardCargo('Dono'),
  handler(req => salvarCupom(req.lojaUUID, req.body)));

app.delete('/api/lojas/:loja_id/cupons/:id',
  ...guardCargo('Dono'),
  handler(req => excluirCupom(req.lojaUUID, req.params.id)));

// Rota pública de validação de cupom
app.get('/api/lojas/:loja_id/cupons/validar',
  resolverLojaId,
  handler(req => validarCupom(req.lojaUUID, req.query.codigo)));

app.post('/api/lojas/:loja_id/taras',
  ...guardCargo('Dono', 'Gerente'),
  handler(req => salvarTara(req.lojaUUID, req.body)));

app.delete('/api/lojas/:loja_id/taras/:id',
  ...guardCargo('Dono', 'Gerente'),
  handler(req => excluirTara(req.lojaUUID, req.params.id)));


// ══════════════════════════════════════════════════════════
// ROTAS — CLIENTES
// ══════════════════════════════════════════════════════════

app.post('/api/lojas/:loja_id/clientes',
  ...guardCargo('Dono', 'Gerente'),
  handler(req => salvarCliente(req.lojaUUID, req.body)));

app.get('/api/lojas/:loja_id/clientes/cpf/:cpf',
  ...guardCargo('Dono', 'Gerente'),
  handler(req => buscarClientePorCPF(req.lojaUUID, req.params.cpf)));

app.get('/api/lojas/:loja_id/clientes/telefone/:telefone',
  ...guardCargo('Dono', 'Gerente'),
  handler(req => buscarClientePorTelefone(req.lojaUUID, req.params.telefone)));


// ══════════════════════════════════════════════════════════
// ROTAS — PEDIDOS
// ══════════════════════════════════════════════════════════

app.post('/api/lojas/:loja_id/pedidos/balcao',
  ...guardCargo('Dono', 'Gerente', 'Supervisor', 'Operador'),
  handler(req => registrarVendaBalcao(req.lojaUUID, req.body)));

app.post('/api/lojas/:loja_id/pedidos/delivery',
  ...guardCargo('Dono', 'Gerente', 'Supervisor', 'Operador'),
  handler(req => registrarVendaDelivery(req.lojaUUID, req.body)));

app.post('/api/lojas/:loja_id/pedidos/online',
  resolverLojaId,
  verificarStatusLoja,
  handler(async req => {
    try { return await finalizarPedidoOnline(req.lojaUUID, req.body); }
    catch (e) { return { sucesso: false, mensagem: e.message }; }
  }));

app.post('/api/lojas/:loja_id/pedidos/online-pix',
  resolverLojaId,
  verificarStatusLoja,
  handler(async req => {
    try { return await finalizarPedidoOnlineComPix(req.lojaUUID, req.body); }
    catch (e) { return { sucesso: false, mensagem: e.message }; }
  }));

app.patch('/api/lojas/:loja_id/pedidos/:id/status',
  ...guardLoja,
  handler(req => atualizarStatusPedido(req.lojaUUID, req.params.id, req.body.status)));

app.patch('/api/lojas/:loja_id/pedidos/:id/status-entrega',
  ...guardLoja,
  handler(req => atualizarStatusEntrega(req.lojaUUID, req.params.id, req.body.status)));

app.post('/api/lojas/:loja_id/pedidos/:id/cancelar',
  resolverLojaId,
  handler(req => cancelarPedidoAutorizado(req.lojaUUID, req.params.id, req.body.login, req.body.senha)));

app.post('/api/lojas/:loja_id/pedidos/:id/pegar-delivery',
  ...guardLoja,
  handler(req => pegarPedidoDelivery(req.lojaUUID, req.params.id, req.body.nomeEntregador)));

app.delete('/api/lojas/:loja_id/pedidos/:id',
  ...guardCargo('Dono'),
  handler(req => deletarRegistro(req.lojaUUID, 'pedidos', 'id_venda', req.params.id)));

app.get('/api/lojas/:loja_id/pedidos/dia',
  ...guardLoja,
  handler(req => getPedidosDoDia(req.lojaUUID)));

app.get('/api/lojas/:loja_id/pedidos/buscar',
  ...guardLoja,
  handler(req => buscarPedidos(req.lojaUUID, req.query.q)));

app.get('/api/lojas/:loja_id/pedidos/acompanhamento',
  resolverLojaId,
  handler(req => getAcompanhamentoPedido(req.lojaUUID, req.query.q)));

app.get('/api/lojas/:loja_id/pedidos/periodo',
  ...guardLoja,
  handler(req => getPedidosPorPeriodo(req.lojaUUID, req.query.inicio, req.query.fim)));

app.get('/api/lojas/:loja_id/pedidos/relatorio',
  ...guardCargo('Dono', 'Gerente'),
  handler(req => getRelatorioAvancado(req.lojaUUID, {
    dataInicio: req.query.inicio,
    dataFim:    req.query.fim,
    pagamento:  req.query.pagamento,
    operador:   req.query.operador,
    origem:     req.query.origem,
    status:     req.query.status,
  })));


// ══════════════════════════════════════════════════════════
// ROTAS — MERCADO PAGO / PIX
// ══════════════════════════════════════════════════════════

app.post('/api/lojas/:loja_id/pix/gerar',
  resolverLojaId,
  handler(req => gerarPixMP(req.lojaUUID, req.body.total, req.body.idVenda, req.body.emailPagador)));

app.get('/api/lojas/:loja_id/pix/verificar/:idPagamento',
  resolverLojaId,
  handler(req => verificarPagamentoMP(req.lojaUUID, req.params.idPagamento)));

app.post('/api/lojas/:loja_id/pix/confirmar',
  resolverLojaId,
  handler(req => confirmarPagamentoELiberarPedido(req.lojaUUID, req.body.idVenda, req.body.idPagamento)));


// ══════════════════════════════════════════════════════════
// HEALTH CHECK
// ══════════════════════════════════════════════════════════

app.get('/api/health', (_, res) => res.json({ status: 'ok', ts: new Date().toISOString() }));


// ══════════════════════════════════════════════════════════
// START
// ══════════════════════════════════════════════════════════

app.listen(PORT, () => {
  console.log(`✅  API SaaS Açaíteria (Supabase) rodando na porta ${PORT}`);
  console.log(`
  ╔══════════════════════════════════════════════════════════════════════╗
  ║  ⚠️  CHECKLIST OBRIGATÓRIO ANTES DE COMEÇAR A VENDER               ║
  ╠══════════════════════════════════════════════════════════════════════╣
  ║                                                                      ║
  ║  SUPABASE:                                                           ║
  ║  [1] Criar o schema "acaiteria" no Supabase SQL Editor.             ║
  ║  [2] Criar todas as tabelas no schema acaiteria conforme o          ║
  ║      mapeamento oficial (ver documentação do projeto).              ║
  ║  [3] Tabela "lojas" deve estar no schema PUBLIC com as colunas:     ║
  ║      id (uuid PK), nome_loja, status, slug, asaas_customer_id,      ║
  ║      ultimo_evento_asaas, plano, criado_em, atualizado_em.          ║
  ║  [4] Habilitar RLS nas tabelas e criar policies adequadas ou        ║
  ║      usar a service_role key (recomendado para backend).            ║
  ║  [5] SUPABASE_SERVICE_KEY no .env é a chave service_role.           ║
  ║      NUNCA exponha esta chave no frontend.                          ║
  ║                                                                      ║
  ║  ASAAS:                                                              ║
  ║  [6] Em Configurações → Integrações → Webhooks, cadastrar:          ║
  ║      https://SEU_BACKEND/api/webhooks/asaas                         ║
  ║  [7] ASAAS_WEBHOOK_TOKEN no .env deve bater com o token do Asaas.  ║
  ║                                                                      ║
  ║  SEGURANÇA:                                                          ║
  ║  [8] O .env NUNCA deve ser commitado no Git (.gitignore).           ║
  ║  [9] JWT_SECRET deve ter mínimo 64 chars aleatórios.               ║
  ║      node -e "console.log(require('crypto')                         ║
  ║             .randomBytes(64).toString('hex'))"                      ║
  ║                                                                      ║
  ╚══════════════════════════════════════════════════════════════════════╝
  `);
});