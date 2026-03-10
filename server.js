/**
 * ╔══════════════════════════════════════════════════════════════════════════════╗
 * ║  AÇAÍTERIA SaaS — BACK-END Node.js + Express + Supabase/PostgreSQL          ║
 * ║  Arquitetura Multi-tenant  |  Cobrança via Asaas                            ║
 * ║                                                                              ║
 * ║  Tabelas Supabase (Multi-tenant via loja_id):                               ║
 * ║   lojas              → { id, status, asaas_customer_id, slug, ... }         ║
 * ║   usuarios           → { id_usuario, loja_id, login, senha, cargo, ... }    ║
 * ║   pedidos            → { id_venda, loja_id, origem, cliente_info(jsonb)...} ║
 * ║   cardapio           → itens fixos do cardápio                              ║
 * ║   acai_categorias    → categorias do monte-açaí                             ║
 * ║   acai_ingredientes  → ingredientes do monte-açaí                           ║
 * ║   acai_modelos       → tamanhos/modelos do açaí                             ║
 * ║   bairros            → bairros de entrega                                   ║
 * ║   cupons             → cupons de desconto                                   ║
 * ║   taras              → pesos de tara para balança                           ║
 * ║   clientes           → cadastro de clientes                                 ║
 * ║   configuracoes      → configurações por loja (1 linha por loja)            ║
 * ╚══════════════════════════════════════════════════════════════════════════════╝
 *
 * Variáveis de ambiente (.env):
 *   PORT=3000
 *   TZ=America/Fortaleza
 *   JWT_SECRET=<string longa e aleatória>
 *   SUPABASE_URL=https://<seu-projeto>.supabase.co
 *   SUPABASE_SERVICE_KEY=<service_role key — NUNCA exponha no frontend>
 *   ASAAS_WEBHOOK_TOKEN=<token definido no painel Asaas>
 *   ONESIGNAL_REST_API_KEY=<chave REST do OneSignal>
 */

'use strict';

const express              = require('express');
const cors                 = require('cors');
const jwt                  = require('jsonwebtoken');
const { createClient }     = require('@supabase/supabase-js');
require('dotenv').config();

// ─── VALIDAÇÃO DE AMBIENTE ────────────────────────────────────────────────────

const JWT_SECRET          = process.env.JWT_SECRET;
const SUPABASE_URL        = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY= process.env.SUPABASE_SERVICE_KEY;
const ASAAS_WEBHOOK_TOKEN = process.env.ASAAS_WEBHOOK_TOKEN || '';
const ONESIGNAL_REST_API_KEY = process.env.ONESIGNAL_REST_API_KEY || '';
const ONESIGNAL_APP_ID    = 'c7565fa8-cd8c-4264-85b4-1a9d9cf150af';
const PORT                = process.env.PORT || 3000;
const TZ                  = process.env.TZ   || 'America/Fortaleza';

if (!JWT_SECRET)           throw new Error('❌  JWT_SECRET não definido no .env');
if (!SUPABASE_URL)         throw new Error('❌  SUPABASE_URL não definido no .env');
if (!SUPABASE_SERVICE_KEY) throw new Error('❌  SUPABASE_SERVICE_KEY não definido no .env');
if (!ASAAS_WEBHOOK_TOKEN)  console.warn('⚠️  ASAAS_WEBHOOK_TOKEN não definido — webhook Asaas desprotegido!');

// ─── SUPABASE ─────────────────────────────────────────────────────────────────
// Usa a service_role key no back-end: ignora RLS e tem acesso total.
// JAMAIS exponha esta chave no frontend.

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
  db: { schema: 'acaiteria' } // <-- O Segredo: Aponta para o "bairro" correto
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
// UTILITÁRIOS
// ══════════════════════════════════════════════════════════

/** Retorna "dd/MM/yyyy HH:mm:ss" no fuso configurado (uso em textos, ex: cancelado_por). */
function _agora() {
  return new Date()
    .toLocaleString('pt-BR', { timeZone: TZ, hour12: false })
    .replace(',', '');
}

/** Retorna "dd/MM/yyyy" para hoje. */
function _hojeString() {
  return new Date().toLocaleDateString('pt-BR', { timeZone: TZ });
}

/** Converte string "dd/MM/yyyy" em Date (meia-noite local). */
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


// ══════════════════════════════════════════════════════════
// CACHE EM MEMÓRIA (TTL configurável)
// Estratégia senior para 500+ lojas: evita esgotar conexões
// ══════════════════════════════════════════════════════════

const CACHE_CARDAPIO_TTL_MS = 15 * 60 * 1000; //  15 min
const CACHE_STATUS_TTL_MS   =  5 * 60 * 1000; //   5 min
const CACHE_SLUG_TTL_MS     = 30 * 60 * 1000; //  30 min

/** Cache do pacote de cardápio público. Invalidado ao salvar qualquer dado relacionado. */
const _cacheCardapio = new Map();   // loja_id → { data, ts }

/** Cache do status da loja (ativo/bloqueado). Invalidado pelo webhook Asaas. */
const _cacheLojaStatus = new Map(); // loja_id → { status, ts }

/** Cache de tradução slug → uuid. Raramente muda; TTL longo. */
const _cacheSlugParaId = new Map(); // slug → { id, ts }

// --- Cardápio ---
function _getCacheCardapio(lojaId) {
  const e = _cacheCardapio.get(lojaId);
  if (!e || Date.now() - e.ts > CACHE_CARDAPIO_TTL_MS) { _cacheCardapio.delete(lojaId); return null; }
  return e.data;
}
function _setCacheCardapio(lojaId, data) { _cacheCardapio.set(lojaId, { data, ts: Date.now() }); }
function invalidarCacheCardapio(lojaId) { _cacheCardapio.delete(lojaId); }

// --- Status da loja ---
function _getStatusCache(lojaId) {
  const e = _cacheLojaStatus.get(lojaId);
  if (!e || Date.now() - e.ts > CACHE_STATUS_TTL_MS) { _cacheLojaStatus.delete(lojaId); return undefined; }
  return e.status;
}
function _setStatusCache(lojaId, status) { _cacheLojaStatus.set(lojaId, { status, ts: Date.now() }); }
function invalidarStatusCache(lojaId)    { _cacheLojaStatus.delete(lojaId); }

// --- Slug → ID ---
async function resolverSlugParaId(slug) {
  const entry = _cacheSlugParaId.get(slug);
  if (entry && Date.now() - entry.ts < CACHE_SLUG_TTL_MS) return entry.id;

  const { data, error } = await supabase
    .from('lojas').select('id').eq('slug', slug).maybeSingle();
  if (error || !data) return null;
  _cacheSlugParaId.set(slug, { id: data.id, ts: Date.now() });
  return data.id;
}


// ══════════════════════════════════════════════════════════
// PERMISSÕES POR CARGO
// ══════════════════════════════════════════════════════════

const _PERMISSOES = {
  Dono:       ['abaPedidos','abaDelivery','abaRetirada','abaRelatorios','abaItens','abaMonteAcai','abaConfig'],
  Gerente:    ['abaPedidos','abaDelivery','abaRetirada','abaRelatorios','abaItens','abaMonteAcai'],
  Supervisor: ['abaPedidos','abaDelivery','abaRetirada','abaItens','abaMonteAcai'],
  Operador:   ['abaPedidos','abaDelivery','abaRetirada'],
  Entregador: ['abaDelivery'],
};

function getPermissoesCargo(cargo) {
  return _PERMISSOES[(cargo || '').trim()] || ['abaDelivery'];
}


// ══════════════════════════════════════════════════════════
// MAPEAMENTO: coleção (legado) → tabela Supabase + PK
// ══════════════════════════════════════════════════════════

const _TABLE_MAP = {
  'usuarios':          { tabela: 'usuarios',          pk: 'id_usuario'     },
  'cardapio':          { tabela: 'cardapio',           pk: 'id_item'        },
  'acai-categorias':   { tabela: 'acai_categorias',    pk: 'id_categoria'   },
  'acai-ingredientes': { tabela: 'acai_ingredientes',  pk: 'id_ingrediente' },
  'acai-modelos':      { tabela: 'acai_modelos',       pk: 'id_modelo'      },
  'bairros':           { tabela: 'bairros',            pk: 'id_bairro'      },
  'cupons':            { tabela: 'cupons',             pk: 'codigo_cupom'   },
  'taras':             { tabela: 'taras',              pk: 'id_tara'        },
  'clientes':          { tabela: 'clientes',           pk: 'id_cliente'     },
  'pedidos':           { tabela: 'pedidos',            pk: 'id_venda'       },
};

/** Conjunto de tabelas que impactam o cache do cardápio. */
const _CACHE_COLS = new Set([
  'cardapio', 'acai_modelos', 'acai_categorias',
  'acai_ingredientes', 'bairros', 'configuracoes', 'cupons',
]);


// ══════════════════════════════════════════════════════════
// HELPERS SUPABASE — OPERAÇÕES GENÉRICAS
// ══════════════════════════════════════════════════════════

/**
 * Lê todos os registros de uma tabela filtrando por loja_id.
 * Equivale ao antigo lerSubcolecao().
 */
async function lerTabela(lojaId, nomeColecao) {
  const mapa = _TABLE_MAP[nomeColecao];
  if (!mapa) throw new Error(`Coleção desconhecida: ${nomeColecao}`);
  const { data, error } = await supabase
    .from(mapa.tabela).select('*').eq('loja_id', lojaId);
  if (error) throw new Error(`[lerTabela:${mapa.tabela}] ${error.message}`);
  return data || [];
}

/** Lê as configurações da loja (1 linha por loja). */
async function getConfiguracoes(lojaId) {
  const { data, error } = await supabase
    .from('configuracoes').select('*').eq('loja_id', lojaId).maybeSingle();
  if (error) throw new Error(`[getConfiguracoes] ${error.message}`);
  return data || {};
}

/**
 * Salva (INSERT ou UPDATE) um registro em uma tabela.
 * Se o campo PK estiver vazio/ausente → INSERT (UUID gerado pelo banco).
 * Se o campo PK estiver preenchido    → UPDATE via upsert.
 */
async function salvarRegistro(lojaId, nomeColecao, dadosObj) {
  const mapa = _TABLE_MAP[nomeColecao];
  if (!mapa) throw new Error(`Coleção desconhecida: ${nomeColecao}`);
  const { tabela, pk } = mapa;

  const id    = dadosObj[pk] ? dadosObj[pk].toString().trim() : '';
  const ehNovo = !id;
  const dados  = { ...dadosObj, loja_id: lojaId };

  if (ehNovo) {
    // Remove PK para deixar o banco gerar o UUID
    delete dados[pk];
    const { data, error } = await supabase.from(tabela).insert(dados).select(pk).single();
    if (error) throw new Error(`[salvarRegistro:insert:${tabela}] ${error.message}`);
    if (_CACHE_COLS.has(tabela)) invalidarCacheCardapio(lojaId);
    return { sucesso: true, mensagem: 'Salvo!', id: data[pk] };
  } else {
    const { error } = await supabase
      .from(tabela).update(dados).eq(pk, id).eq('loja_id', lojaId);
    if (error) throw new Error(`[salvarRegistro:update:${tabela}] ${error.message}`);
    if (_CACHE_COLS.has(tabela)) invalidarCacheCardapio(lojaId);
    return { sucesso: true, mensagem: 'Atualizado!', id };
  }
}

/**
 * Exclui um registro de uma tabela.
 */
async function deletarRegistro(lojaId, nomeColecao, idValor) {
  const mapa = _TABLE_MAP[nomeColecao];
  if (!mapa) throw new Error(`Coleção desconhecida: ${nomeColecao}`);
  const { tabela, pk } = mapa;

  const { error, count } = await supabase
    .from(tabela).delete({ count: 'exact' })
    .eq(pk, idValor).eq('loja_id', lojaId);

  if (error) throw new Error(`[deletarRegistro:${tabela}] ${error.message}`);
  if (count === 0) return { sucesso: false, mensagem: 'Registro não encontrado.' };
  if (_CACHE_COLS.has(tabela)) invalidarCacheCardapio(lojaId);
  return { sucesso: true, mensagem: 'Excluído com sucesso!' };
}


// ══════════════════════════════════════════════════════════
// MIDDLEWARES
// ══════════════════════════════════════════════════════════

/** MW 1 — Autenticação JWT. Injeta req.usuario = { loja_id, id, nome, cargo }. */
function autenticar(req, res, next) {
  const authHeader = req.headers['authorization'];
  const token = authHeader?.startsWith('Bearer ') ? authHeader.slice(7) : null;
  if (!token)
    return res.status(401).json({ sucesso: false, mensagem: 'Token não fornecido.' });
  try {
    req.usuario = jwt.verify(token, JWT_SECRET);
    next();
  } catch (e) {
    return res.status(401).json({
      sucesso: false,
      mensagem: e.name === 'TokenExpiredError'
        ? 'Sessão expirada. Faça login novamente.' : 'Token inválido.',
    });
  }
}

/** MW 2 — Verificação de cargo. Deve ser usado APÓS autenticar(). */
function exigirCargo(...cargosPermitidos) {
  return (req, res, next) => {
    if (!req.usuario)
      return res.status(401).json({ sucesso: false, mensagem: 'Não autenticado.' });
    if (!cargosPermitidos.includes(req.usuario.cargo))
      return res.status(403).json({
        sucesso: false,
        mensagem: `Acesso negado. Requer: ${cargosPermitidos.join(' ou ')}.`,
      });
    next();
  };
}

/** MW 3 — Isolamento multi-tenant. Garante que o usuário só acessa sua loja. */
function verificarLojaAcesso(req, res, next) {
  const lojaIdRota = req.params.loja_id;
  if (!lojaIdRota) return next();
  if (req.usuario.loja_id !== lojaIdRota)
    return res.status(403).json({
      sucesso: false,
      mensagem: 'Acesso negado. Esta loja não pertence à sua conta.',
    });
  next();
}

/**
 * MW 4 — SaaS Gate: verifica se a loja está ativa.
 * Usa cache de 5 min para não consultar o banco em cada requisição.
 */
async function verificarStatusLoja(req, res, next) {
  const lojaId = req.params.loja_id || (req.usuario && req.usuario.loja_id);
  if (!lojaId) return next();

  try {
    let status = _getStatusCache(lojaId);

    if (status === undefined) {
      const { data, error } = await supabase
        .from('lojas').select('status').eq('id', lojaId).maybeSingle();
      if (error) throw error;
      if (!data) return res.status(404).json({ sucesso: false, mensagem: 'Loja não encontrada.' });
      status = (data.status || 'ativo').toLowerCase();
      _setStatusCache(lojaId, status);
    }

    if (status === 'bloqueado')
      return res.status(403).json({
        sucesso: false,
        mensagem: 'Acesso bloqueado. Verifique o pagamento da sua assinatura ou contate o suporte.',
      });

    next();
  } catch (err) {
    console.error('[verificarStatusLoja]', err);
    return res.status(500).json({ sucesso: false, mensagem: 'Erro ao verificar status da loja.' });
  }
}

/** Pilha padrão de MWs para rotas protegidas da loja. */
const guardLoja  = [autenticar, verificarLojaAcesso, verificarStatusLoja];
const guardCargo = (...cargos) => [...guardLoja, exigirCargo(...cargos)];

/** Wrapper async: executa handler e retorna JSON. */
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
  if (!lojaId || !usuarioDigitado || !senhaDigitada)
    return { sucesso: false, mensagem: 'Loja, usuário e senha são obrigatórios.' };

  const loginNorm = usuarioDigitado.toString().trim().toLowerCase();

  const { data: usuario, error } = await supabase
    .from('usuarios')
    .select('*')
    .eq('loja_id', lojaId)
    .eq('login', loginNorm)
    .maybeSingle();

  if (error || !usuario)
    return { sucesso: false, mensagem: 'Usuário ou senha inválidos.' };

  if ((usuario.senha || '') !== senhaDigitada.toString())
    return { sucesso: false, mensagem: 'Usuário ou senha inválidos.' };

  if ((usuario.status || '').toLowerCase() !== 'ativo')
    return { sucesso: false, mensagem: 'Acesso bloqueado. Contate a gerência.' };

  // Verifica se a loja em si está ativa (usa cache)
  let statusLoja = _getStatusCache(lojaId);
  if (statusLoja === undefined) {
    const { data: loja } = await supabase
      .from('lojas').select('status').eq('id', lojaId).maybeSingle();
    if (!loja) return { sucesso: false, mensagem: 'Loja não encontrada.' };
    statusLoja = (loja.status || 'ativo').toLowerCase();
    _setStatusCache(lojaId, statusLoja);
  }

  if (statusLoja === 'bloqueado')
    return { sucesso: false, mensagem: 'Esta loja está bloqueada. Verifique o pagamento da assinatura.' };

  const cargo = usuario.cargo || '';
  return {
    sucesso:    true,
    cargo,
    nome:       usuario.nome       || '',
    fotoPerfil: usuario.foto_perfil || '',
    permissoes: getPermissoesCargo(cargo),
  };
}

async function validarSupervisorOuAcima(lojaId, login, senha) {
  const auth = await validarLogin(lojaId, login, senha);
  if (!auth.sucesso) return { autorizado: false, mensagem: auth.mensagem };
  if (!['Dono','Gerente','Supervisor'].includes(auth.cargo))
    return { autorizado: false, mensagem: `Cargo "${auth.cargo}" não tem autoridade para esta ação.` };
  return { autorizado: true, nome: auth.nome, cargo: auth.cargo, mensagem: 'Autorizado.' };
}


// ══════════════════════════════════════════════════════════
// CARGAS DE DADOS (carregamento inicial do painel)
// ══════════════════════════════════════════════════════════

async function getDadosPainelGeral(lojaId) {
  const [
    usuarios, cardapio, acai_categorias, acai_ingredientes,
    acai_modelos, bairros, cupons, taras, configuracoes,
    pedidosDia, deliveryAtivo,
  ] = await Promise.all([
    lerTabela(lojaId, 'usuarios'),
    lerTabela(lojaId, 'cardapio'),
    lerTabela(lojaId, 'acai-categorias'),
    lerTabela(lojaId, 'acai-ingredientes'),
    lerTabela(lojaId, 'acai-modelos'),
    lerTabela(lojaId, 'bairros'),
    lerTabela(lojaId, 'cupons'),
    lerTabela(lojaId, 'taras'),
    getConfiguracoes(lojaId),
    getPedidosDoDia(lojaId),
    getDeliveryEmAndamento(lojaId),
  ]);

  return {
    usuarios, cardapio, acai_categorias, acai_ingredientes,
    acai_modelos, bairros, cupons, taras, configuracoes,
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
  const [acai_modelos, acai_categorias, acai_ingredientes] = await Promise.all([
    lerTabela(lojaId, 'acai-modelos'),
    lerTabela(lojaId, 'acai-categorias'),
    lerTabela(lojaId, 'acai-ingredientes'),
  ]);
  return { acai_modelos, acai_categorias, acai_ingredientes };
}

/** Retorna o pacote público do cardápio, usando cache em memória. */
async function getCardapioClienteCache(lojaId) {
  const cached = _getCacheCardapio(lojaId);
  if (cached) return cached;

  const [config, prontos, tamanhos, categorias, ingredientes, bairros] = await Promise.all([
    getConfiguracoes(lojaId),
    lerTabela(lojaId, 'cardapio'),
    lerTabela(lojaId, 'acai-modelos'),
    lerTabela(lojaId, 'acai-categorias'),
    lerTabela(lojaId, 'acai-ingredientes'),
    lerTabela(lojaId, 'bairros'),
  ]);

  const pacote = {
    configuracoes: {
      url_logo:               config.url_logo               || '',
      hora_abre:              config.hora_abre              || '',
      hora_fecha:             config.hora_fecha             || '',
      status_loja:            config.status_loja            || '',
      nome_loja:              config.nome_loja              || '',
      whatsapp_numero:        config.whatsapp_numero        || '',
      retirada_loja_ativo:    config.retirada_loja_ativo    ?? false,
      endereco_loja:          config.endereco_loja          || '',
      instagram_url:          config.instagram_url          || '',
      whatsapp_link:          config.whatsapp_link          || '',
      facebook_url:           config.facebook_url           || '',
      mostrar_acai_rapido_pdv:config.mostrar_acai_rapido_pdv ?? true,
      preco_kg:               parseFloat(config.preco_kg    || 39.90),
      autoimprimir_balcao:    config.autoimprimir_balcao    ?? false,
      autoimprimir_delivery:  config.autoimprimir_delivery  ?? false,
      frete_gratis:           parseFloat(config.frete_gratis || 0),
      tempo_entrega:          config.tempo_entrega          || '',
    },
    // Filtra com booleans nativos — zero tradução "SIM"/"NÃO"
    prontos:      prontos.filter(i => i.disponivel === true && i.mostrar_online !== false),
    tamanhos:     tamanhos.filter(m => m.disponivel === true),
    categorias,
    ingredientes: ingredientes.filter(i => i.disponivel === true),
    bairros:      bairros.filter(b => b.disponivel === true),
  };

  _setCacheCardapio(lojaId, pacote);
  return pacote;
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

  dtIni.setHours(0,  0,  0,   0);
  dtFim.setHours(23, 59, 59, 999);

  // Filtragem por intervalo diretamente no banco — mais eficiente que carregar tudo
  const { data: pedidos, error } = await supabase
    .from('pedidos').select('*')
    .eq('loja_id', lojaId)
    .gte('data_hora', dtIni.toISOString())
    .lte('data_hora', dtFim.toISOString())
    .order('data_hora', { ascending: false });

  if (error) throw new Error(`[getPedidosPorPeriodo] ${error.message}`);

  const lista = pedidos || [];
  let totalDia = 0, totalDesc = 0;
  const contOrigem  = { BALCAO: 0, DELIVERY: 0, ONLINE: 0 };
  const contPgto    = {}, contStatus = {};

  for (const p of lista) {
    const status = (p.status || '').toUpperCase();
    const origem = (p.origem || '').toUpperCase();
    const pgto   = (p.metodo_pagamento || '').toUpperCase();

    if (status !== 'CANCELADO') {
      totalDia  += parseFloat(p.total_final || 0);
      totalDesc += parseFloat(p.desconto    || 0);
    }
    contOrigem[origem] = (contOrigem[origem] || 0) + 1;
    contPgto[pgto]     = (contPgto[pgto]     || 0) + 1;
    contStatus[status] = (contStatus[status]  || 0) + 1;
  }

  const qtdAtivos = lista.filter(p => (p.status||'').toUpperCase() !== 'CANCELADO').length;

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

  if (params.pagamento?.trim()) pedidos = pedidos.filter(p => (p.metodo_pagamento||'').toUpperCase() === params.pagamento.toUpperCase());
  if (params.operador?.trim())  pedidos = pedidos.filter(p => (p.operador||'').toLowerCase().includes(params.operador.toLowerCase()));
  if (params.origem?.trim())    pedidos = pedidos.filter(p => (p.origem||'').toUpperCase() === params.origem.toUpperCase());
  if (params.status?.trim())    pedidos = pedidos.filter(p => (p.status||'').toUpperCase() === params.status.toUpperCase());

  let totalVendas = 0, totalDescontos = 0;
  const porOrigem = {}, porPagamento = {}, porStatus = {}, porOperador = {};

  for (const p of pedidos) {
    const st = (p.status||'').toUpperCase();
    const og = (p.origem||'').toUpperCase();
    const pg = (p.metodo_pagamento||'').toUpperCase();
    const op = (p.operador||'').toString();
    if (st !== 'CANCELADO') {
      totalVendas    += parseFloat(p.total_final || 0);
      totalDescontos += parseFloat(p.desconto    || 0);
    }
    porOrigem[og]    = (porOrigem[og]    || 0) + 1;
    porPagamento[pg] = (porPagamento[pg] || 0) + 1;
    porStatus[st]    = (porStatus[st]    || 0) + 1;
    porOperador[op]  = (porOperador[op]  || 0) + 1;
  }

  const qtdAtivos = pedidos.filter(p => (p.status||'').toUpperCase() !== 'CANCELADO').length;

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

  const limite = new Date();
  limite.setDate(limite.getDate() - 60);

  const { data, error } = await supabase
    .from('pedidos').select('*')
    .eq('loja_id', lojaId)
    .gte('data_hora', limite.toISOString())
    .order('data_hora', { ascending: false })
    .limit(200);

  if (error) throw new Error(`[buscarPedidos] ${error.message}`);

  const resultado = [];
  for (const p of (data || [])) {
    if (resultado.length >= 50) break;

    const idStr    = (p.id_venda || '').toLowerCase();
    // cliente_info é JSONB — Supabase já retorna como objeto
    const cliStr   = JSON.stringify(p.cliente_info  || {}).toLowerCase();
    const itensStr = JSON.stringify(p.itens_comprados|| []).toLowerCase();

    if (idStr.includes(q) || cliStr.includes(q) || itensStr.includes(q))
      resultado.push(p);
  }
  return resultado;
}

async function getAcompanhamentoPedido(lojaId, query) {
  if (!query || query.toString().trim().length < 5)
    return { erro: 'Digite pelo menos 5 caracteres.' };

  const q       = query.toString().trim();
  const qNorm   = q.replace(/\s+/g, '').toLowerCase();
  const qDigits = q.replace(/\D/g, '');

  const limiteHoje = new Date(); limiteHoje.setHours(0, 0, 0, 0);
  const limite7d   = new Date(); limite7d.setDate(limite7d.getDate() - 7);

  // Busca por id_venda (últimos 7 dias) ou por telefone no JSONB (apenas hoje)
  const { data, error } = await supabase
    .from('pedidos').select('*')
    .eq('loja_id', lojaId)
    .gte('data_hora', limite7d.toISOString())
    .order('data_hora', { ascending: false });

  if (error) throw new Error(`[getAcompanhamentoPedido] ${error.message}`);

  let encontrados = [];
  for (const p of (data || [])) {
    const idVenda    = (p.id_venda || '').replace(/\s+/g,'').toLowerCase();
    const isMatchID  = idVenda && (idVenda.includes(qNorm) || (qNorm.includes(idVenda) && idVenda.length > 4));

    let isMatchPhone = false;
    if (qDigits.length >= 7 && p.cliente_info) {
      const tel = ((p.cliente_info.telefone || '')).replace(/\D/g, '');
      if (tel.length >= 7 && tel.includes(qDigits)) isMatchPhone = true;
    }

    if (isMatchID) {
      encontrados.push(p);
    } else if (isMatchPhone) {
      const dt = new Date(p.data_hora);
      if (dt >= limiteHoje) encontrados.push(p);
    }
  }

  if (encontrados.length === 0)
    return { erro: 'Pedido não encontrado. Verifique o número e tente novamente.' };

  encontrados = encontrados.slice(0, 3); // máximo 3 pedidos

  const statusLabels = {
    NOVO: 'Recebido', PREPARANDO: 'Preparando', PRONTO: 'Pronto para retirada',
    EM_MONTAGEM: 'Em montagem', A_CAMINHO: 'Saiu para entrega',
    ENTREGUE: 'Entregue ✅', CANCELADO: 'Cancelado ❌',
  };

  return encontrados.map(p => {
    const statusVal = (p.status || '').toUpperCase();
    const cli       = p.cliente_info   || {};
    const itens     = p.itens_comprados || [];

    const itensResumo = itens.map(it =>
      it.descricao + (it.preco ? ' — R$' + parseFloat(it.preco).toFixed(2).replace('.', ',') : '')
    ).join('\n');

    return {
      id_venda:        p.id_venda    || '',
      origem:          (p.origem     || '').toUpperCase(),
      status:          statusVal,
      status_label:    statusLabels[statusVal] || statusVal,
      data_hora:       p.data_hora   || '',
      total_final:     parseFloat(p.total_final || 0),
      entregador_nome: p.entregador_nome || '',
      nome_cliente:    cli.nome      || '',
      endereco:        cli.endereco  || '',
      itens_resumo:    itensResumo,
    };
  });
}

async function getDeliveryEmAndamento(lojaId) {
  const limite = new Date();
  limite.setDate(limite.getDate() - 7);
  limite.setHours(0, 0, 0, 0);

  const { data, error } = await supabase
    .from('pedidos').select('*')
    .eq('loja_id', lojaId)
    .in('origem', ['DELIVERY', 'ONLINE'])
    .gte('data_hora', limite.toISOString());

  if (error) throw new Error(`[getDeliveryEmAndamento] ${error.message}`);

  const limiteHoje = new Date(); limiteHoje.setHours(0, 0, 0, 0);

  return (data || []).filter(p => {
    const st = (p.status || '').toUpperCase();
    if (st === 'CANCELADO') return false;
    if (st === 'ENTREGUE') {
      const dt = new Date(p.data_hora);
      return dt >= limiteHoje; // mantém entregues de hoje para a carteira do entregador
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
// PEDIDOS — ESCRITA (Balcão, Delivery, Online)
// ══════════════════════════════════════════════════════════

async function dispararPushOneSignal(lojaId, titulo, mensagem, ignorarEntregador = false) {
  if (!ONESIGNAL_REST_API_KEY) return;
  try {
    const filtros = [{ field: 'tag', key: 'loja_id', relation: '=', value: lojaId }];
    if (ignorarEntregador)
      filtros.push({ field: 'tag', key: 'cargo', relation: '!=', value: 'Entregador' });

    await fetch('https://onesignal.com/api/v1/notifications', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Key ${ONESIGNAL_REST_API_KEY}` },
      body: JSON.stringify({
        app_id: ONESIGNAL_APP_ID,
        filters:  filtros,
        headings: { en: titulo },
        contents: { en: mensagem },
      }),
    });
  } catch (e) { console.error('[OneSignal] Erro:', e); }
}

/**
 * Persiste um pedido na tabela `pedidos`.
 * cliente_info e itens_comprados são enviados como objetos JSON nativos (JSONB).
 */
async function registrarVenda(lojaId, pedido) {
  const dados = {
    loja_id:          lojaId,
    origem:           pedido.origem,
    data_hora:        new Date().toISOString(),
    operador:         pedido.operador         || 'CAIXA',
    // JSONB: envia como objeto/array — NÃO usar JSON.stringify aqui
    cliente_info:     pedido.cliente_info     || {},
    itens_comprados:  pedido.itens_comprados  || [],
    subtotal:         pedido.subtotal         || 0,
    desconto:         pedido.desconto         || 0,
    taxa_entrega:     pedido.taxa_entrega     || 0,
    total_final:      pedido.total_final      || 0,
    metodo_pagamento: pedido.metodo_pagamento || '',
    status:           pedido.status           || 'NOVO',
    peso_bruto_g:     pedido.peso_bruto_g     || 0,
    id_tara:          pedido.id_tara          || null,
    peso_tara_g:      pedido.peso_tara_g      || 0,
    peso_liquido_g:   pedido.peso_liquido_g   || 0,
    preco_kg:         pedido.preco_kg         || 0,
    troco:            pedido.troco            || 0,
    entregador_nome:  pedido.entregador_nome  || '',
    cancelado_por:    pedido.cancelado_por    || '',
  };

  const { data, error } = await supabase
    .from('pedidos').insert(dados).select('id_venda').single();
  if (error) throw new Error(`[registrarVenda] ${error.message}`);

  // Push OneSignal para delivery/online
  if (['DELIVERY', 'ONLINE'].includes(pedido.origem)) {
    const isRetirada = pedido.cliente_info?.endereco === 'Retirada na loja';
    dispararPushOneSignal(
      lojaId,
      isRetirada ? '🛍️ Nova Retirada!' : '🔔 Novo Pedido de Delivery!',
      isRetirada ? 'Cliente vem buscar na loja. Verifique o painel.' : 'Uma nova entrega caiu no painel. Verifique!',
      isRetirada,
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

  // Auto-cadastro de cliente
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
    origem:           'BALCAO',
    operador:         dados.operador || 'CAIXA',
    cliente_info:     { nome: dados.nomeCliente || '', cpf: dados.cpfCliente || '', telefone: dados.telefone || '' },
    itens_comprados:  todosItens,
    subtotal, desconto,
    taxa_entrega:     0,
    total_final:      total,
    metodo_pagamento: dados.pagamento || '',
    status:           'ENTREGUE',
    peso_bruto_g:     pesoBruto,
    id_tara:          dados.idTara || null,
    peso_tara_g:      pesoTara,
    peso_liquido_g:   pesoLiq,
    preco_kg:         precoKG,
    troco,
  };

  const resultado = await registrarVenda(lojaId, pedido);
  if (resultado.sucesso) Object.assign(resultado, { troco, total, pesoLiq });
  return resultado;
}

async function registrarVendaDelivery(lojaId, dados) {
  const itens     = dados.itens || [];
  const subtotal  = Math.round(itens.reduce((s, i) => s + parseFloat(i.preco || 0), 0) * 100) / 100;
  const desconto  = parseFloat(dados.desconto    || 0);
  const taxaEnt   = parseFloat(dados.taxaEntrega || 0);
  const total     = Math.max(0, Math.round((subtotal - desconto + taxaEnt) * 100) / 100);
  const valorPago = parseFloat(dados.valorPago   || 0);
  const troco     = dados.pagamento === 'DINHEIRO'
    ? Math.max(0, Math.round((valorPago - total) * 100) / 100) : 0;

  const pedido = {
    origem:           'DELIVERY',
    operador:         dados.operador || 'CAIXA',
    cliente_info:     { nome: dados.nomeCliente || '', cpf: dados.cpfCliente || '', telefone: dados.telefone || '', endereco: dados.endereco || '' },
    itens_comprados:  itens,
    subtotal, desconto,
    taxa_entrega:     taxaEnt,
    total_final:      total,
    metodo_pagamento: dados.pagamento || '',
    status:           'NOVO',
    troco,
  };

  const resultado = await registrarVenda(lojaId, pedido);
  if (resultado.sucesso) Object.assign(resultado, { troco, total });
  return resultado;
}

/** Finaliza pedido do cardápio online (rota pública — recalcula total no servidor). */
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
    origem:           'ONLINE',
    operador:         'APP',
    cliente_info:     { nome: dadosPedido.nomeCliente || '', cpf: '', telefone: dadosPedido.telefone || '', endereco: dadosPedido.endereco || '' },
    itens_comprados:  dadosPedido.itens,
    subtotal:         subtotalReal, desconto,
    taxa_entrega:     taxaEnt,
    total_final:      total,
    metodo_pagamento: dadosPedido.pagamento || '',
    status:           'NOVO',
  };

  return registrarVenda(lojaId, pedido);
}

async function atualizarStatusPedido(lojaId, idVenda, novoStatus) {
  const statusValidos = ['NOVO','PREPARANDO','PRONTO','ENTREGUE','CANCELADO','EM_MONTAGEM','A_CAMINHO','AGUARDANDO_PIX'];
  if (!statusValidos.includes(novoStatus))
    return { sucesso: false, mensagem: `Status inválido: ${novoStatus}` };

  const { error, count } = await supabase
    .from('pedidos').update({ status: novoStatus }, { count: 'exact' })
    .eq('id_venda', idVenda).eq('loja_id', lojaId);

  if (error) throw new Error(`[atualizarStatusPedido] ${error.message}`);
  if (count === 0) return { sucesso: false, mensagem: `Pedido "${idVenda}" não encontrado.` };
  return { sucesso: true };
}

async function atualizarStatusEntrega(lojaId, idVenda, novoStatus) {
  if (!['EM_MONTAGEM','A_CAMINHO','ENTREGUE'].includes(novoStatus))
    return { sucesso: false, mensagem: `Status inválido para entrega: ${novoStatus}` };
  return atualizarStatusPedido(lojaId, idVenda, novoStatus);
}

async function cancelarPedidoAutorizado(lojaId, idVenda, loginAuth, senhaAuth) {
  const auth = await validarSupervisorOuAcima(lojaId, loginAuth, senhaAuth);
  if (!auth.autorizado) return { sucesso: false, mensagem: auth.mensagem };

  // Lê o pedido atual para validações
  const { data: pedido, error: errGet } = await supabase
    .from('pedidos').select('status, origem')
    .eq('id_venda', idVenda).eq('loja_id', lojaId).maybeSingle();

  if (errGet) throw new Error(errGet.message);
  if (!pedido) throw new Error(`Pedido "${idVenda}" não encontrado.`);

  const statusAtual = (pedido.status || '').toUpperCase();
  const origemAtual = (pedido.origem || '').toUpperCase();

  if (statusAtual === 'CANCELADO') throw new Error('Pedido já cancelado.');
  if (statusAtual === 'ENTREGUE' && origemAtual !== 'BALCAO') throw new Error('Pedido já entregue.');

  const { error: errUpd } = await supabase
    .from('pedidos')
    .update({ status: 'CANCELADO', cancelado_por: `${auth.nome} (${auth.cargo}) — ${_agora()}` })
    .eq('id_venda', idVenda).eq('loja_id', lojaId);

  if (errUpd) throw new Error(errUpd.message);
  return { sucesso: true, mensagem: `Pedido cancelado por ${auth.nome} (${auth.cargo}).` };
}

async function pegarPedidoDelivery(lojaId, idVenda, nomeEntregador) {
  if (!idVenda || !nomeEntregador?.toString().trim())
    return { sucesso: false, mensagem: 'ID do pedido e nome do entregador são obrigatórios.' };

  const nomeTrimmed = nomeEntregador.toString().trim();

  const { data: pedido, error } = await supabase
    .from('pedidos').select('status, entregador_nome')
    .eq('id_venda', idVenda).eq('loja_id', lojaId).maybeSingle();

  if (error) throw new Error(error.message);
  if (!pedido) throw new Error('Pedido não encontrado.');

  const statusAtual  = (pedido.status          || '').toUpperCase();
  const entregAtual  = (pedido.entregador_nome || '').trim();

  if (statusAtual === 'ENTREGUE')  throw new Error('Pedido já entregue.');
  if (statusAtual === 'CANCELADO') throw new Error('Pedido cancelado.');

  if (entregAtual && entregAtual.toLowerCase() !== nomeTrimmed.toLowerCase())
    return { sucesso: false, mensagem: `Este pedido já foi pego por ${entregAtual}.`, entregadorAtual: entregAtual };

  const { error: errUpd } = await supabase
    .from('pedidos')
    .update({ entregador_nome: nomeTrimmed, status: 'EM_MONTAGEM' })
    .eq('id_venda', idVenda).eq('loja_id', lojaId);

  if (errUpd) throw new Error(errUpd.message);
  return { sucesso: true, mensagem: `Pedido atribuído a ${nomeTrimmed}.` };
}


// ══════════════════════════════════════════════════════════
// CADASTROS (CRUD POR LOJA)
// ══════════════════════════════════════════════════════════

// Aliases semânticos que delegam para o helper genérico
const salvarUsuario         = (l, d) => salvarRegistro(l, 'usuarios',          d);
const excluirUsuario        = (l, id) => deletarRegistro(l, 'usuarios', id);
const salvarAcaiCategoria   = (l, d) => salvarRegistro(l, 'acai-categorias',   d);
const excluirAcaiCategoria  = (l, id) => deletarRegistro(l, 'acai-categorias', id);
const salvarAcaiIngrediente = (l, d) => salvarRegistro(l, 'acai-ingredientes', d);
const excluirAcaiIngrediente= (l, id) => deletarRegistro(l, 'acai-ingredientes', id);
const salvarAcaiModelo      = (l, d) => salvarRegistro(l, 'acai-modelos',      d);
const excluirAcaiModelo     = (l, id) => deletarRegistro(l, 'acai-modelos', id);
const salvarItemFixo        = (l, d) => salvarRegistro(l, 'cardapio',          d);
const excluirItemFixo       = (l, id) => deletarRegistro(l, 'cardapio', id);
const salvarBairro          = (l, d) => salvarRegistro(l, 'bairros',           d);
const excluirBairro         = (l, id) => deletarRegistro(l, 'bairros', id);
const salvarCupom           = (l, d) => salvarRegistro(l, 'cupons',            d);
const excluirCupom          = (l, id) => deletarRegistro(l, 'cupons', id);
const salvarTara            = (l, d) => salvarRegistro(l, 'taras',             d);
const excluirTara           = (l, id) => deletarRegistro(l, 'taras', id);

async function salvarCliente(lojaId, dados) {
  const registro = {
    loja_id:       lojaId,
    nome:          dados.nome      || '',
    cpf:           dados.cpf       || '',
    telefone:      dados.telefone  || '',
    data_cadastro: dados.data_cadastro || new Date().toISOString(),
  };
  if (dados.id_cliente) registro.id_cliente = dados.id_cliente;

  const { data, error } = dados.id_cliente
    ? await supabase.from('clientes').update(registro).eq('id_cliente', dados.id_cliente).eq('loja_id', lojaId).select('id_cliente').single()
    : await supabase.from('clientes').insert(registro).select('id_cliente').single();

  if (error) throw new Error(`[salvarCliente] ${error.message}`);
  return { sucesso: true, mensagem: dados.id_cliente ? 'Atualizado!' : 'Salvo!', id: data.id_cliente };
}

async function buscarClientePorCPF(lojaId, cpf) {
  const cpfLimpo = cpf.toString().replace(/\D/g, '');
  if (cpfLimpo.length !== 11) return null;
  const { data } = await supabase
    .from('clientes').select('*').eq('loja_id', lojaId).eq('cpf', cpfLimpo).maybeSingle();
  return data;
}

async function buscarClientePorTelefone(lojaId, telefone) {
  const telLimpo = telefone.toString().replace(/\D/g, '');
  const { data } = await supabase
    .from('clientes').select('*').eq('loja_id', lojaId).eq('telefone', telLimpo).maybeSingle();
  return data;
}

async function salvarConfiguracoesLote(lojaId, configObj) {
  // Garante que loja_id sempre está presente no upsert
  const { error } = await supabase
    .from('configuracoes')
    .upsert({ ...configObj, loja_id: lojaId }, { onConflict: 'loja_id' });
  if (error) throw new Error(`[salvarConfiguracoesLote] ${error.message}`);
  invalidarCacheCardapio(lojaId);
  return { sucesso: true, mensagem: 'Configurações salvas.' };
}


// ══════════════════════════════════════════════════════════
// VALIDAÇÃO DE CUPOM
// ══════════════════════════════════════════════════════════

async function validarCupom(lojaId, codigo) {
  if (!codigo?.toString().trim()) return { valido: false, mensagem: 'Digite um código de cupom.' };

  const codigoUpper = codigo.toString().trim().toUpperCase();

  const { data: cupom, error } = await supabase
    .from('cupons').select('*')
    .eq('loja_id', lojaId).eq('codigo_cupom', codigoUpper)
    .maybeSingle();

  if (error) throw new Error(error.message);
  if (!cupom) return { valido: false, mensagem: 'Cupom não encontrado.' };

  // ativo é boolean nativo — sem tradução "SIM"/"NÃO"
  if (!cupom.ativo) return { valido: false, mensagem: 'Cupom inativo ou esgotado.' };

  if (cupom.validade) {
    const validade = new Date(cupom.validade);
    validade.setHours(23, 59, 59, 999);
    if (validade < new Date()) return { valido: false, mensagem: 'Cupom expirado.' };
  }

  const tipo       = (cupom.tipo_desconto || 'VALOR').toUpperCase();
  const valorBruto = parseFloat(cupom.valor_desconto) || 0;

  return {
    valido:       true,
    codigo:       cupom.codigo_cupom,
    tipo,
    valor:        valorBruto,
    desconto:     tipo === 'VALOR' ? valorBruto : 0,
    // usar_cardapio é boolean nativo
    usar_cardapio: cupom.usar_cardapio,
    mensagem:     tipo === 'PERCENTUAL'
      ? `Desconto de ${valorBruto.toFixed(0)}% aplicado 🎉`
      : `Desconto de R$ ${valorBruto.toFixed(2).replace('.', ',')} aplicado 🎉`,
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
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`,
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
  return { sucesso: false, mensagem: r?.message || 'Resposta inesperada do Mercado Pago.' };
}

async function verificarPagamentoMP(lojaId, idPagamento) {
  const config = await getConfiguracoes(lojaId);
  const token  = (config.mp_access_token || '').toString().trim();
  if (!token || !idPagamento) return { status: 'erro', detalhe: 'Dados inválidos.' };

  const resp = await fetch(`https://api.mercadopago.com/v1/payments/${idPagamento}`, {
    headers: { 'Authorization': `Bearer ${token}` },
    signal: AbortSignal.timeout(10000),
  });
  const r = await resp.json();
  return { status: r.status || 'erro', detalhe: r.status_detail || '' };
}

async function finalizarPedidoOnlineComPix(lojaId, dadosPedido) {
  const config = await getConfiguracoes(lojaId);
  const modoMP = (config.pix_modo || 'MANUAL').toUpperCase();

  if (modoMP !== 'AUTO' && modoMP !== 'AUTOMATICO' && modoMP !== 'AUTOMÁTICO')
    return finalizarPedidoOnline(lojaId, dadosPedido);

  if (!dadosPedido?.itens?.length) throw new Error('Pedido vazio ou formato inválido.');
  if ((config.status_loja || 'AUTOMATICO') === 'FORCAR_FECHADO')
    throw new Error('A loja está fechada no momento.');

  const subtotalReal    = Math.round(dadosPedido.itens.reduce((s, i) => s + parseFloat(i.preco || 0), 0) * 100) / 100;
  const subtotalEnviado = parseFloat(dadosPedido.subtotal || 0);
  if (Math.abs(subtotalReal - subtotalEnviado) > 0.05)
    throw new Error('Divergência financeira detectada. Pedido rejeitado por segurança.');

  const desconto = parseFloat(dadosPedido.desconto    || 0);
  const taxaEnt  = parseFloat(dadosPedido.taxaEntrega || 0);
  const total    = Math.max(0, Math.round((subtotalReal - desconto + taxaEnt) * 100) / 100);

  const idVendaTemp = 'WEB-' + Date.now().toString().substring(5);
  const pix = await gerarPixMP(
    lojaId, total, idVendaTemp,
    dadosPedido.telefone ? `${dadosPedido.telefone.replace(/\D/g,'')}@mp.br` : 'contato@acaiteria.com.br'
  );
  if (!pix.sucesso)
    return { sucesso: false, mensagem: `PIX não gerado: ${pix.mensagem}. Tente outra forma de pagamento.` };

  const pedido = {
    origem:           'ONLINE',
    operador:         'APP',
    cliente_info:     { nome: dadosPedido.nomeCliente || '', cpf: '', telefone: dadosPedido.telefone || '', endereco: dadosPedido.endereco || '' },
    itens_comprados:  dadosPedido.itens,
    subtotal:         subtotalReal, desconto,
    taxa_entrega:     taxaEnt,
    total_final:      total,
    metodo_pagamento: 'PIX',
    status:           'AGUARDANDO_PIX',
  };

  const res = await registrarVenda(lojaId, pedido);
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

async function getConfigPix(lojaId) {
  const config = await getConfiguracoes(lojaId);
  const token  = (config.mp_access_token || '').toString().trim();
  return {
    modoMP:        (config.pix_modo || 'MANUAL').toUpperCase(),
    mpConfigurado: token.length > 10,
  };
}


// ══════════════════════════════════════════════════════════
// ROTAS — AUTENTICAÇÃO
// ══════════════════════════════════════════════════════════

/** POST /api/lojas/:loja_id/auth/login — Body: { usuario, senha } */
app.post('/api/lojas/:loja_id/auth/login', handler(async req => {
  const lojaId    = req.params.loja_id;
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

/** POST /api/lojas/:loja_id/auth/validar-supervisor — Body: { login, senha } */
app.post('/api/lojas/:loja_id/auth/validar-supervisor', handler(req =>
  validarSupervisorOuAcima(req.params.loja_id, req.body.login, req.body.senha)
));

/** GET /api/lojas/slug/:slug — Resolve slug → uuid (útil para o frontend) */
app.get('/api/lojas/slug/:slug', handler(async req => {
  const id = await resolverSlugParaId(req.params.slug);
  if (!id) return { sucesso: false, mensagem: 'Loja não encontrada.' };
  return { sucesso: true, id };
}));


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
    if (!event || !payment?.customer)
      return res.status(400).json({ sucesso: false, mensagem: 'Payload inválido.' });

    const customerId = payment.customer;
    const eventosMonitorados = ['PAYMENT_OVERDUE','PAYMENT_RECEIVED','PAYMENT_RESTORED'];

    if (!eventosMonitorados.includes(event))
      return res.json({ sucesso: true, mensagem: `Evento "${event}" ignorado.` });

    // Busca a loja pelo asaas_customer_id na tabela lojas
    const { data: lojas, error } = await supabase
      .from('lojas').select('id')
      .eq('asaas_customer_id', customerId).limit(1);

    if (error) throw error;
    if (!lojas || lojas.length === 0) {
      console.warn(`[Webhook Asaas] Nenhuma loja para customer: ${customerId}`);
      return res.status(404).json({ sucesso: false, mensagem: 'Loja não encontrada para este customer.' });
    }

    const lojaId    = lojas[0].id;
    const novoStatus = event === 'PAYMENT_OVERDUE' ? 'bloqueado' : 'ativo';

    const { error: errUpd } = await supabase
      .from('lojas')
      .update({ status: novoStatus, ultimo_evento_asaas: event, atualizado_em: new Date().toISOString() })
      .eq('id', lojaId);

    if (errUpd) throw errUpd;

    // Invalida caches para forçar revalidação imediata
    invalidarCacheCardapio(lojaId);
    invalidarStatusCache(lojaId);

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
  handler(req => getDadosPainelGeral(req.params.loja_id)));

app.get('/api/lojas/:loja_id/dados-config',
  ...guardCargo('Dono'),
  handler(req => getDadosConfig(req.params.loja_id)));

app.get('/api/lojas/:loja_id/dados-monte-acai',
  handler(req => getDadosMonteAcai(req.params.loja_id)));

// Rota pública do cardápio (sem autenticação JWT)
app.get('/api/lojas/:loja_id/cardapio',
  verificarStatusLoja,
  handler(req => getCardapioClienteCache(req.params.loja_id)));

app.get('/api/lojas/:loja_id/polling',
  ...guardLoja,
  handler(req => getDadosPolling(req.params.loja_id)));

app.get('/api/lojas/:loja_id/config-pix',
  ...guardCargo('Dono'),
  handler(req => getConfigPix(req.params.loja_id)));

app.get('/api/lojas/:loja_id/delivery-ativo',
  ...guardLoja,
  handler(req => getDeliveryEmAndamento(req.params.loja_id)));


// ══════════════════════════════════════════════════════════
// ROTAS — CONFIGURAÇÕES
// ══════════════════════════════════════════════════════════

app.post('/api/lojas/:loja_id/configuracoes',
  ...guardCargo('Dono'),
  handler(req => salvarConfiguracoesLote(req.params.loja_id, req.body)));


// ══════════════════════════════════════════════════════════
// ROTAS — USUÁRIOS
// ══════════════════════════════════════════════════════════

app.post('/api/lojas/:loja_id/usuarios',
  ...guardCargo('Dono'),
  handler(req => salvarUsuario(req.params.loja_id, req.body)));

app.delete('/api/lojas/:loja_id/usuarios/:id',
  ...guardCargo('Dono'),
  handler(req => excluirUsuario(req.params.loja_id, req.params.id)));


// ══════════════════════════════════════════════════════════
// ROTAS — CARDÁPIO / CADASTROS
// ══════════════════════════════════════════════════════════

app.post('/api/lojas/:loja_id/acai-categorias',
  ...guardCargo('Dono'),
  handler(req => salvarAcaiCategoria(req.params.loja_id, req.body)));

app.delete('/api/lojas/:loja_id/acai-categorias/:id',
  ...guardCargo('Dono'),
  handler(req => excluirAcaiCategoria(req.params.loja_id, req.params.id)));

app.post('/api/lojas/:loja_id/acai-ingredientes',
  ...guardCargo('Dono'),
  handler(req => salvarAcaiIngrediente(req.params.loja_id, req.body)));

app.delete('/api/lojas/:loja_id/acai-ingredientes/:id',
  ...guardCargo('Dono'),
  handler(req => excluirAcaiIngrediente(req.params.loja_id, req.params.id)));

app.post('/api/lojas/:loja_id/acai-modelos',
  ...guardCargo('Dono'),
  handler(req => salvarAcaiModelo(req.params.loja_id, req.body)));

app.delete('/api/lojas/:loja_id/acai-modelos/:id',
  ...guardCargo('Dono'),
  handler(req => excluirAcaiModelo(req.params.loja_id, req.params.id)));

app.post('/api/lojas/:loja_id/itens-fixos',
  ...guardCargo('Dono'),
  handler(req => salvarItemFixo(req.params.loja_id, req.body)));

app.delete('/api/lojas/:loja_id/itens-fixos/:id',
  ...guardCargo('Dono'),
  handler(req => excluirItemFixo(req.params.loja_id, req.params.id)));

app.post('/api/lojas/:loja_id/bairros',
  ...guardCargo('Dono'),
  handler(req => salvarBairro(req.params.loja_id, req.body)));

app.delete('/api/lojas/:loja_id/bairros/:id',
  ...guardCargo('Dono'),
  handler(req => excluirBairro(req.params.loja_id, req.params.id)));

app.post('/api/lojas/:loja_id/cupons',
  ...guardCargo('Dono'),
  handler(req => salvarCupom(req.params.loja_id, req.body)));

app.delete('/api/lojas/:loja_id/cupons/:id',
  ...guardCargo('Dono'),
  handler(req => excluirCupom(req.params.loja_id, req.params.id)));

// Rota pública de validação de cupom (usada pelo cardápio online)
app.get('/api/lojas/:loja_id/cupons/validar',
  handler(req => validarCupom(req.params.loja_id, req.query.codigo)));

app.post('/api/lojas/:loja_id/taras',
  ...guardCargo('Dono', 'Gerente'),
  handler(req => salvarTara(req.params.loja_id, req.body)));

app.delete('/api/lojas/:loja_id/taras/:id',
  ...guardCargo('Dono', 'Gerente'),
  handler(req => excluirTara(req.params.loja_id, req.params.id)));


// ══════════════════════════════════════════════════════════
// ROTAS — CLIENTES
// ══════════════════════════════════════════════════════════

app.post('/api/lojas/:loja_id/clientes',
  ...guardCargo('Dono', 'Gerente'),
  handler(req => salvarCliente(req.params.loja_id, req.body)));

app.get('/api/lojas/:loja_id/clientes/cpf/:cpf',
  ...guardCargo('Dono', 'Gerente'),
  handler(req => buscarClientePorCPF(req.params.loja_id, req.params.cpf)));

app.get('/api/lojas/:loja_id/clientes/telefone/:telefone',
  ...guardCargo('Dono', 'Gerente'),
  handler(req => buscarClientePorTelefone(req.params.loja_id, req.params.telefone)));


// ══════════════════════════════════════════════════════════
// ROTAS — PEDIDOS
// ══════════════════════════════════════════════════════════

app.post('/api/lojas/:loja_id/pedidos/balcao',
  ...guardCargo('Dono','Gerente','Supervisor','Operador'),
  handler(req => registrarVendaBalcao(req.params.loja_id, req.body)));

app.post('/api/lojas/:loja_id/pedidos/delivery',
  ...guardCargo('Dono','Gerente','Supervisor','Operador'),
  handler(req => registrarVendaDelivery(req.params.loja_id, req.body)));

// Pedido online: rota PÚBLICA — verifica apenas status da loja
app.post('/api/lojas/:loja_id/pedidos/online',
  verificarStatusLoja,
  handler(async req => {
    try { return await finalizarPedidoOnline(req.params.loja_id, req.body); }
    catch (e) { return { sucesso: false, mensagem: e.message }; }
  }));

// PIX online: público
app.post('/api/lojas/:loja_id/pedidos/online-pix',
  verificarStatusLoja,
  handler(async req => {
    try { return await finalizarPedidoOnlineComPix(req.params.loja_id, req.body); }
    catch (e) { return { sucesso: false, mensagem: e.message }; }
  }));

app.patch('/api/lojas/:loja_id/pedidos/:id/status',
  ...guardLoja,
  handler(req => atualizarStatusPedido(req.params.loja_id, req.params.id, req.body.status)));

app.patch('/api/lojas/:loja_id/pedidos/:id/status-entrega',
  ...guardLoja,
  handler(req => atualizarStatusEntrega(req.params.loja_id, req.params.id, req.body.status)));

// Cancelamento: público — usa validação interna de supervisor
app.post('/api/lojas/:loja_id/pedidos/:id/cancelar',
  handler(req => cancelarPedidoAutorizado(req.params.loja_id, req.params.id, req.body.login, req.body.senha)));

app.post('/api/lojas/:loja_id/pedidos/:id/pegar-delivery',
  ...guardLoja,
  handler(req => pegarPedidoDelivery(req.params.loja_id, req.params.id, req.body.nomeEntregador)));

// Exclusão definitiva (apenas Dono)
app.delete('/api/lojas/:loja_id/pedidos/:id',
  ...guardCargo('Dono'),
  handler(async req => {
    const { error, count } = await supabase
      .from('pedidos').delete({ count: 'exact' })
      .eq('id_venda', req.params.id).eq('loja_id', req.params.loja_id);
    if (error) throw new Error(error.message);
    if (count === 0) return { sucesso: false, mensagem: 'Pedido não encontrado.' };
    return { sucesso: true, mensagem: 'Pedido excluído.' };
  }));

app.get('/api/lojas/:loja_id/pedidos/dia',
  ...guardLoja,
  handler(req => getPedidosDoDia(req.params.loja_id)));

app.get('/api/lojas/:loja_id/pedidos/buscar',
  ...guardLoja,
  handler(req => buscarPedidos(req.params.loja_id, req.query.q)));

// Acompanhamento público (rastreio pelo cliente)
app.get('/api/lojas/:loja_id/pedidos/acompanhamento',
  handler(req => getAcompanhamentoPedido(req.params.loja_id, req.query.q)));

app.get('/api/lojas/:loja_id/pedidos/periodo',
  ...guardLoja,
  handler(req => getPedidosPorPeriodo(req.params.loja_id, req.query.inicio, req.query.fim)));

app.get('/api/lojas/:loja_id/pedidos/relatorio',
  ...guardCargo('Dono','Gerente'),
  handler(req => getRelatorioAvancado(req.params.loja_id, {
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
  handler(req => gerarPixMP(req.params.loja_id, req.body.total, req.body.idVenda, req.body.emailPagador)));

app.get('/api/lojas/:loja_id/pix/verificar/:idPagamento',
  handler(req => verificarPagamentoMP(req.params.loja_id, req.params.idPagamento)));

app.post('/api/lojas/:loja_id/pix/confirmar',
  handler(req => confirmarPagamentoELiberarPedido(req.params.loja_id, req.body.idVenda, req.body.idPagamento)));


// ══════════════════════════════════════════════════════════
// HEALTH CHECK
// ══════════════════════════════════════════════════════════

app.get('/api/health', (_, res) => res.json({ status: 'ok', ts: _agora(), engine: 'supabase' }));


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
  ║  [1] Criar as tabelas via SQL Editor conforme o schema acordado.     ║
  ║      Tabelas obrigatórias: lojas, configuracoes, usuarios, pedidos,  ║
  ║      cardapio, acai_categorias, acai_ingredientes, acai_modelos,     ║
  ║      bairros, cupons, taras, clientes.                               ║
  ║  [2] Habilitar RLS em todas as tabelas (segurança multi-tenant).     ║
  ║      O backend usa service_role key → bypassa RLS no servidor.       ║
  ║  [3] Garantir que as colunas boolean usam tipo BOOLEAN nativo        ║
  ║      (disponivel, ativo, mostrar_online, retirada_loja_ativo, etc.)  ║
  ║      Sem "SIM"/"NÃO" — somente true/false.                          ║
  ║  [4] Colunas cliente_info e itens_comprados devem ser tipo JSONB.    ║
  ║                                                                      ║
  ║  VARIÁVEIS DE AMBIENTE (.env):                                       ║
  ║  [5] SUPABASE_URL=https://<projeto>.supabase.co                      ║
  ║  [6] SUPABASE_SERVICE_KEY=<service_role key do projeto>              ║
  ║  [7] JWT_SECRET=<mínimo 64 chars aleatórios>                        ║
  ║      node -e "console.log(require('crypto')                          ║
  ║      .randomBytes(64).toString('hex'))"                              ║
  ║                                                                      ║
  ║  ASAAS:                                                              ║
  ║  [8] Cadastrar webhook: https://SEU_BACKEND/api/webhooks/asaas       ║
  ║  [9] ASAAS_WEBHOOK_TOKEN=<token gerado no painel Asaas>              ║
  ║                                                                      ║
  ║  SEGURANÇA:                                                          ║
  ║  [10] JAMAIS commite o .env no Git. Adicione ao .gitignore AGORA.   ║
  ║  [11] JAMAIS exponha SUPABASE_SERVICE_KEY no frontend.              ║
  ║                                                                      ║
  ╚══════════════════════════════════════════════════════════════════════╝
  `);
});