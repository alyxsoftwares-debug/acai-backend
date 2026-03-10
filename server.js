/**
 * ╔══════════════════════════════════════════════════════════════════════════════╗
 * ║  AÇAÍTERIA SaaS — BACK-END Node.js + Express + Firebase Firestore          ║
 * ║  Arquitetura Multi-tenant  |  Cobrança via Asaas                           ║
 * ║                                                                             ║
 * ║  Estrutura Firestore (Multi-tenant):                                        ║
 * ║   /lojas/{loja_id}                → { status, asaas_customer_id, ... }     ║
 * ║   /lojas/{loja_id}/usuarios       → usuários da loja                       ║
 * ║   /lojas/{loja_id}/pedidos        → pedidos (era pedidos_pdv)               ║
 * ║   /lojas/{loja_id}/cardapio       → itens fixos (era itens_fixos)          ║
 * ║   /lojas/{loja_id}/acai-categorias                                          ║
 * ║   /lojas/{loja_id}/acai-ingredientes                                        ║
 * ║   /lojas/{loja_id}/acai-modelos                                             ║
 * ║   /lojas/{loja_id}/bairros                                                  ║
 * ║   /lojas/{loja_id}/cupons                                                   ║
 * ║   /lojas/{loja_id}/taras                                                    ║
 * ║   /lojas/{loja_id}/clientes                                                 ║
 * ║   /lojas/{loja_id}/configuracoes  → doc único "main"                       ║
 * ╚══════════════════════════════════════════════════════════════════════════════╝
 *
 * ⚠️  CHECKLIST ANTES DE VENDER — leia os alertas no final deste arquivo.
 *
 * Variáveis de ambiente (.env):
 *   PORT=3000
 *   TZ=America/Fortaleza
 *   JWT_SECRET=<string longa e aleatória>
 *   FIREBASE_SERVICE_ACCOUNT={"type":"service_account",...}   ← JSON em string
 *   ASAAS_WEBHOOK_TOKEN=<token que você define no painel Asaas>
 */

'use strict';

const express = require('express');
const cors    = require('cors');
const admin   = require('firebase-admin');
const jwt     = require('jsonwebtoken');
require('dotenv').config();

// ─── VALIDAÇÃO DE AMBIENTE ────────────────────────────────────────────────────

const JWT_SECRET            = process.env.JWT_SECRET;
const ASAAS_WEBHOOK_TOKEN   = process.env.ASAAS_WEBHOOK_TOKEN || '';
const PORT                  = process.env.PORT || 3000;
const TZ                    = process.env.TZ   || 'America/Fortaleza';

if (!JWT_SECRET) throw new Error('❌  JWT_SECRET não definido no .env');
if (!ASAAS_WEBHOOK_TOKEN) {
  console.warn('⚠️  ASAAS_WEBHOOK_TOKEN não definido — webhook Asaas estará desprotegido!');
}

// ─── FIREBASE ────────────────────────────────────────────────────────────────

admin.initializeApp({
  credential: admin.credential.cert(
    JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)
  ),
});
const db = admin.firestore();

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
// UTILITÁRIOS INTERNOS
// ══════════════════════════════════════════════════════════

/** Gera ID único: prefixo + 8 dígitos do timestamp + 4 dígitos aleatórios. */
function _gerarId(prefixo) {
  const ts   = Date.now().toString().slice(-8);
  const rand = Math.floor(Math.random() * 9000 + 1000).toString();
  return `${prefixo}-${ts}${rand}`;
}

/** Retorna "dd/MM/yyyy HH:mm:ss" no fuso configurado. */
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

/** Converte Timestamp Firestore, Date ou string em Date. */
function _toDate(valor) {
  if (!valor) return null;
  if (typeof valor.toDate === 'function') return valor.toDate();
  if (valor instanceof Date) return isNaN(valor.getTime()) ? null : valor;
  const str = valor.toString().trim();
  if (!str) return null;
  return _parseDataDDMMYYYY(str.split(' ')[0]);
}

/** Converte campos Timestamp do Firestore para string legível dentro de um objeto. */
function _normalizarTimestamps(obj) {
  for (const k of Object.keys(obj)) {
    if (obj[k] && typeof obj[k].toDate === 'function') {
      obj[k] = obj[k].toDate()
        .toLocaleString('pt-BR', { timeZone: TZ, hour12: false })
        .replace(',', '');
    }
  }
  return obj;
}

function _resumoVazio() {
  return {
    totalVendas: 0, totalDescontos: 0, ticketMedio: 0,
    qtdPedidos: 0, porOrigem: {}, porPagamento: {}, porStatus: {}, periodo: {},
  };
}


// ══════════════════════════════════════════════════════════
// CACHE POR LOJA (in-memory, TTL 15 min)
// ══════════════════════════════════════════════════════════

const CACHE_TTL_MS = 15 * 60 * 1000;
const _cacheCardapio = new Map(); // loja_id → { data, ts }

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
// HELPERS FIRESTORE — SUBCOLEÇÕES
// ══════════════════════════════════════════════════════════

/** Referência da subcoleção de uma loja. */
function _col(lojaId, nomeColecao) {
  return db.collection('lojas').doc(lojaId).collection(nomeColecao);
}

/** Referência do documento principal da loja. */
function _lojaRef(lojaId) {
  return db.collection('lojas').doc(lojaId);
}

/** Lê todos os documentos de uma subcoleção da loja. */
async function lerSubcolecao(lojaId, nomeColecao) {
  const snap = await _col(lojaId, nomeColecao).get();
  return snap.docs.map(doc => _normalizarTimestamps(doc.data()));
}

/** Lê o documento de configurações da loja (doc único "main"). */
async function getConfiguracoes(lojaId) {
  const doc = await _col(lojaId, 'configuracoes').doc('main').get();
  return doc.exists ? doc.data() : {};
}

/**
 * Salva (INSERT ou UPDATE) um registro em uma subcoleção.
 * Invalida o cache de cardápio se necessário.
 */
const _CACHE_COLS = new Set([
  'cardapio', 'acai-modelos', 'acai-categorias',
  'acai-ingredientes', 'bairros', 'configuracoes', 'cupons',
]);

async function salvarRegistro(lojaId, nomeColecao, idPrefixo, dadosObj, campoId) {
  let id = (dadosObj[campoId] || '').toString().trim();
  const ehNovo = !id;
  if (ehNovo) {
    id = _gerarId(idPrefixo);
    dadosObj[campoId] = id;
  }
  await _col(lojaId, nomeColecao).doc(id).set(dadosObj, { merge: true });
  if (_CACHE_COLS.has(nomeColecao)) invalidarCacheCardapio(lojaId);
  return { sucesso: true, mensagem: ehNovo ? 'Salvo!' : 'Atualizado!', id };
}

/** Exclui um registro de uma subcoleção. */
async function deletarRegistro(lojaId, nomeColecao, idValor) {
  const ref = _col(lojaId, nomeColecao).doc(idValor.toString());
  const doc = await ref.get();
  if (!doc.exists) return { sucesso: false, mensagem: 'Registro não encontrado.' };
  await ref.delete();
  if (_CACHE_COLS.has(nomeColecao)) invalidarCacheCardapio(lojaId);
  return { sucesso: true, mensagem: 'Excluído com sucesso!' };
}


// ══════════════════════════════════════════════════════════
// MIDDLEWARES
// ══════════════════════════════════════════════════════════

/**
 * MW 1 — Autenticação JWT.
 * Injeta req.usuario = { loja_id, id, nome, cargo }
 */
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

/**
 * MW 2 — Verificação de cargo.
 * Deve ser usado APÓS autenticar().
 */
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

/**
 * MW 3 — Isolamento de loja.
 * Garante que o usuário logado só acessa sua própria loja.
 * Deve ser usado APÓS autenticar().
 */
function verificarLojaAcesso(req, res, next) {
  const lojaIdRota = req.params.loja_id;
  if (!lojaIdRota) return next(); // rota sem :loja_id não precisa deste check

  if (req.usuario.loja_id !== lojaIdRota) {
    return res.status(403).json({
      sucesso: false,
      mensagem: 'Acesso negado. Esta loja não pertence à sua conta.',
    });
  }
  next();
}

/**
 * MW 4 — Status da loja (SaaS gate).
 * Bloqueia qualquer requisição se a loja estiver com status 'bloqueado'.
 * Deve ser usado APÓS autenticar() + verificarLojaAcesso().
 */
async function verificarStatusLoja(req, res, next) {
  const lojaId = req.params.loja_id || (req.usuario && req.usuario.loja_id);
  if (!lojaId) return next();

  try {
    const lojaDoc = await _lojaRef(lojaId).get();
    if (!lojaDoc.exists) {
      return res.status(404).json({ sucesso: false, mensagem: 'Loja não encontrada.' });
    }
    const status = (lojaDoc.data().status || 'ativo').toString().toLowerCase();
    if (status === 'bloqueado') {
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

/**
 * Conjunto de middlewares padrão para rotas protegidas da loja:
 * autenticar + verificar acesso à loja + verificar status.
 */
const guardLoja = [autenticar, verificarLojaAcesso, verificarStatusLoja];

/**
 * Conjunto de middlewares para rotas que exigem cargo específico:
 * uso: [...guardLoja, ...guardCargo('Dono', 'Gerente')]
 */
const guardCargo = (...cargos) => [...guardLoja, exigirCargo(...cargos)];

/** Wrapper padrão: executa handler async e retorna JSON. */
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

  const snap = await _col(lojaId, 'usuarios')
    .where('Login', '==', loginNorm)
    .limit(1)
    .get();

  if (snap.empty) return { sucesso: false, mensagem: 'Usuário ou senha inválidos.' };

  const d = snap.docs[0].data();

  if ((d.Senha || '').toString() !== senhaDigitada.toString()) {
    return { sucesso: false, mensagem: 'Usuário ou senha inválidos.' };
  }
  if ((d.Status || '').toString() !== 'Ativo') {
    return { sucesso: false, mensagem: 'Acesso bloqueado. Contate a gerência.' };
  }

  // Verifica se a loja em si está ativa antes de emitir o token
  const lojaDoc = await _lojaRef(lojaId).get();
  if (!lojaDoc.exists) return { sucesso: false, mensagem: 'Loja não encontrada.' };
  const statusLoja = (lojaDoc.data().status || 'Ativo').toLowerCase();
  if (statusLoja === 'bloqueado') {
    return { sucesso: false, mensagem: 'Esta loja está bloqueada. Verifique o pagamento da assinatura.' };
  }

  const cargo = (d.Cargo || '').toString();
  return {
    sucesso:    true,
    cargo,
    nome:       (d.Nome || '').toString(),
    fotoPerfil: (d.Foto_Perfil || '').toString(),
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
// CARGAS DE DADOS
// ══════════════════════════════════════════════════════════

async function getDadosPainelGeral(lojaId) {
  const [
    usuarios, itensFixos, acaiCategorias, acaiIngredientes,
    acaiModelos, bairros, cupons, taras, configuracoes,
    pedidosDia, deliveryAtivo,
  ] = await Promise.all([
    lerSubcolecao(lojaId, 'usuarios'),
    lerSubcolecao(lojaId, 'cardapio'),
    lerSubcolecao(lojaId, 'acai-categorias'),
    lerSubcolecao(lojaId, 'acai-ingredientes'),
    lerSubcolecao(lojaId, 'acai-modelos'),
    lerSubcolecao(lojaId, 'bairros'),
    lerSubcolecao(lojaId, 'cupons'),
    lerSubcolecao(lojaId, 'taras'),
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
    lerSubcolecao(lojaId, 'usuarios'),
    lerSubcolecao(lojaId, 'bairros'),
    lerSubcolecao(lojaId, 'cupons'),
    lerSubcolecao(lojaId, 'taras'),
  ]);
  return { configuracoes, usuarios, bairros, cupons, taras };
}

async function getDadosMonteAcai(lojaId) {
  const [acaiModelos, acaiCategorias, acaiIngredientes] = await Promise.all([
    lerSubcolecao(lojaId, 'acai-modelos'),
    lerSubcolecao(lojaId, 'acai-categorias'),
    lerSubcolecao(lojaId, 'acai-ingredientes'),
  ]);
  return { acaiModelos, acaiCategorias, acaiIngredientes };
}

async function getCardapioClienteCache(lojaId) {
  const cached = _getCacheCardapio(lojaId);
  if (cached) return cached;

  const [config, prontos, tamanhos, categorias, ingredientes, bairros] = await Promise.all([
    getConfiguracoes(lojaId),
    lerSubcolecao(lojaId, 'cardapio'),
    lerSubcolecao(lojaId, 'acai-modelos'),
    lerSubcolecao(lojaId, 'acai-categorias'),
    lerSubcolecao(lojaId, 'acai-ingredientes'),
    lerSubcolecao(lojaId, 'bairros'),
  ]);

  const pacote = {
    configuracoes: {
      URL_Logo:               config['URL_Logo']               || '',
      Hora_Abre:              config['Hora_Abre']              || '',
      Hora_Fecha:             config['Hora_Fecha']             || '',
      Status_Loja:            config['Status_Loja']            || '',
      Nome_Loja:              config['Nome_Loja']              || '',
      WhatsApp_Numero:        config['WhatsApp_Numero']        || '',
      Retirada_Loja_Ativo:    config['Retirada_Loja_Ativo']    || '',
      Endereco_Loja:          config['Endereco_Loja']          || '',
      Instagram_URL:          config['Instagram_URL']          || '',
      WhatsApp_Link:          config['WhatsApp_Link']          || '',
      Facebook_URL:           config['Facebook_URL']           || '',
      Mostrar_AcaiRapido_PDV: config['Mostrar_AcaiRapido_PDV'] || 'SIM',
      Preco_KG:               config['Preco_KG']               || '39.90',
      AutoImprimir_Balcao:    config['AutoImprimir_Balcao']    || 'NAO',
      AutoImprimir_Delivery:  config['AutoImprimir_Delivery']  || 'NAO',
      Frete_Gratis:           config['Frete_Gratis']           || '0',
      Tempo_Entrega:          config['Tempo_Entrega']          || '',
    },
    prontos:      prontos.filter(i => i.Disponivel === 'SIM' && (i.Mostrar_Online || 'SIM') !== 'NÃO'),
    tamanhos:     tamanhos.filter(m => m.Disponivel === 'SIM'),
    categorias,
    ingredientes: ingredientes.filter(i => i.Disponivel === 'SIM'),
    bairros:      bairros.filter(b => b.Disponivel === 'SIM'),
  };

  _setCacheCardapio(lojaId, pacote);
  return pacote;
}

async function getDeliveryEmAndamento(lojaId) {
  const snap = await _col(lojaId, 'pedidos')
    .where('Origem', 'in', ['DELIVERY', 'ONLINE'])
    .get();

  const limite = new Date();
  limite.setDate(limite.getDate() - 7);
  limite.setHours(0, 0, 0, 0);
  
  const limiteHoje = new Date();
  limiteHoje.setHours(0, 0, 0, 0);

  return snap.docs
    .map(d => _normalizarTimestamps(d.data()))
    .filter(p => {
      const st = (p.Status || '').toUpperCase();
      const dt = _toDate(p.Data_Hora);
      
      if (st === 'CANCELADO') return false;
      if (st === 'ENTREGUE') {
          // Mantém os entregues de HOJE para somar na carteira do entregador
          return dt && dt >= limiteHoje;
      }
      return !dt || dt >= limite;
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

  const snap   = await _col(lojaId, 'pedidos').get();
  const pedidos = [];
  let totalDia  = 0, totalDesc = 0;
  const contOrigem = { BALCAO: 0, DELIVERY: 0, ONLINE: 0 };
  const contPgto = {}, contStatus = {};

  for (const doc of snap.docs) {
    const p = _normalizarTimestamps(doc.data());

    const dt = _toDate(p.Data_Hora);
    if (!dt) continue;
    const dtCmp = new Date(dt.getTime());
    dtCmp.setHours(12, 0, 0, 0);
    if (dtCmp < dtIni || dtCmp > dtFim) continue;

    pedidos.push(p);

    const tot    = parseFloat(p.Total_Final || 0);
    const desc   = parseFloat(p.Desconto    || 0);
    const origem = (p.Origem           || '').toUpperCase();
    const pgto   = (p.Metodo_Pagamento || '').toUpperCase();
    const status = (p.Status           || '').toUpperCase();

    if (status !== 'CANCELADO') { totalDia += tot; totalDesc += desc; }
    contOrigem[origem] = (contOrigem[origem] || 0) + 1;
    contPgto[pgto]     = (contPgto[pgto]     || 0) + 1;
    contStatus[status] = (contStatus[status]  || 0) + 1;
  }

  const qtdAtivos = pedidos.filter(p => (p.Status || '').toUpperCase() !== 'CANCELADO').length;

  return {
    pedidos,
    resumo: {
      totalVendas:    Math.round(totalDia  * 100) / 100,
      totalDescontos: Math.round(totalDesc * 100) / 100,
      ticketMedio:    qtdAtivos > 0 ? Math.round((totalDia / qtdAtivos) * 100) / 100 : 0,
      qtdPedidos:     pedidos.length,
      porOrigem: contOrigem, porPagamento: contPgto, porStatus: contStatus,
      periodo:   { inicio: dataInicio, fim: dataFim },
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

  if (params.pagamento?.trim()) pedidos = pedidos.filter(p => (p.Metodo_Pagamento || '').toUpperCase() === params.pagamento.toUpperCase());
  if (params.operador?.trim())  pedidos = pedidos.filter(p => (p.Operador || '').toLowerCase().includes(params.operador.toLowerCase()));
  if (params.origem?.trim())    pedidos = pedidos.filter(p => (p.Origem || '').toUpperCase() === params.origem.toUpperCase());
  if (params.status?.trim())    pedidos = pedidos.filter(p => (p.Status || '').toUpperCase() === params.status.toUpperCase());

  let totalVendas = 0, totalDescontos = 0;
  const porOrigem = {}, porPagamento = {}, porStatus = {}, porOperador = {};

  for (const p of pedidos) {
    const st = (p.Status           || '').toUpperCase();
    const og = (p.Origem           || '').toUpperCase();
    const pg = (p.Metodo_Pagamento || '').toUpperCase();
    const op = (p.Operador         || '').toString();
    if (st !== 'CANCELADO') {
      totalVendas    += parseFloat(p.Total_Final || 0);
      totalDescontos += parseFloat(p.Desconto    || 0);
    }
    porOrigem[og]    = (porOrigem[og]    || 0) + 1;
    porPagamento[pg] = (porPagamento[pg] || 0) + 1;
    porStatus[st]    = (porStatus[st]    || 0) + 1;
    porOperador[op]  = (porOperador[op]  || 0) + 1;
  }

  const qtdAtivos = pedidos.filter(p => (p.Status || '').toUpperCase() !== 'CANCELADO').length;

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
  limite.setHours(0, 0, 0, 0);

  const snap = await _col(lojaId, 'pedidos').get();
  const resultado = [];

  for (const doc of snap.docs) {
    if (resultado.length >= 50) break;
    const p = doc.data();
    const dt = _toDate(p.Data_Hora);
    if (dt && dt < limite) continue;

    const idStr    = (p.ID_Venda        || '').toLowerCase();
    const cliStr   = (p.Cliente_Info    || '').toLowerCase();
    const itensStr = (p.Itens_Comprados || '').toLowerCase();

    if (idStr.includes(q) || cliStr.includes(q) || itensStr.includes(q)) {
      resultado.push(_normalizarTimestamps(p));
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

  const snap = await _col(lojaId, 'pedidos').get();
  let encontrados = [];

  for (const doc of snap.docs) {
    const p  = doc.data();
    const dt = _toDate(p.Data_Hora);

    const idVenda = (p.ID_Venda || '').replace(/\s+/g, '').toLowerCase();
    let isMatchID = idVenda && (idVenda.includes(qNorm) || (qNorm.includes(idVenda) && idVenda.length > 4));

    let isMatchPhone = false;
    if (qDigits.length >= 7 && p.Cliente_Info) {
      try {
        const cli = typeof p.Cliente_Info === 'string' ? JSON.parse(p.Cliente_Info) : p.Cliente_Info;
        const tel = (cli.telefone || '').replace(/\D/g, '');
        if (tel.length >= 7 && tel.includes(qDigits)) { isMatchPhone = true; }
      } catch { /* ignora */ }
    }

    if (isMatchID || isMatchPhone) {
      // Filtra os de hoje se a busca for por telefone
      if (isMatchPhone && !isMatchID) {
          if (dt && dt >= limiteHoje) encontrados.push(p);
      } else {
          encontrados.push(p); // Se for pelo ID exato, traz mesmo se for de outro dia
      }
    }
  }

  if (encontrados.length === 0) return { erro: 'Pedido não encontrado de hoje. Verifique o número e tente novamente.' };

  // Ordena matematicamente desmembrando a data BR para achar o pedido mais novo (topo da lista)
  encontrados.sort((a, b) => {
     function parseDt(str) {
         if(!str) return 0;
         if(str.toDate) return str.toDate().getTime();
         const pts = str.split(' ');
         if(pts.length < 2) return 0;
         const d = pts[0].split('/');
         const t = pts[1].split(':');
         return new Date(d[2], d[1]-1, d[0], t[0], t[1], t[2]||0).getTime();
     }
     return parseDt(b.Data_Hora) - parseDt(a.Data_Hora);
  });

  // Mostra apenas os 3 pedidos mais recentes do dia
  encontrados = encontrados.slice(0, 3);

  const statusLabels = {
    NOVO: 'Recebido', PREPARANDO: 'Preparando', PRONTO: 'Pronto para retirada',
    EM_MONTAGEM: 'Em montagem', A_CAMINHO: 'Saiu para entrega',
    ENTREGUE: 'Entregue ✅', CANCELADO: 'Cancelado ❌',
  };

  const resultados = encontrados.map(encontrado => {
      let itensResumo = '';
      try {
        const arr = typeof encontrado.Itens_Comprados === 'string'
          ? JSON.parse(encontrado.Itens_Comprados) : (encontrado.Itens_Comprados || []);
        itensResumo = arr.map(it =>
          it.descricao + (it.preco ? ' — R$' + parseFloat(it.preco).toFixed(2).replace('.', ',') : '')
        ).join('\n');
      } catch { /* ignora */ }

      let nomeCliente = '';
      let enderecoCli = '';
      try {
        const cli = typeof encontrado.Cliente_Info === 'string'
          ? JSON.parse(encontrado.Cliente_Info) : (encontrado.Cliente_Info || {});
        nomeCliente = cli.nome || '';
        enderecoCli = cli.endereco || '';
      } catch { /* ignora */ }

      let dataHoraV = encontrado.Data_Hora || '';
      if (dataHoraV && typeof dataHoraV.toDate === 'function')
        dataHoraV = dataHoraV.toDate().toLocaleString('pt-BR', { timeZone: TZ, hour12: false }).replace(',', '');

      const statusVal = (encontrado.Status || '').toUpperCase();

      return {
        ID_Venda:        encontrado.ID_Venda    || '',
        Origem:          (encontrado.Origem     || '').toUpperCase(),
        Status:          statusVal,
        StatusLabel:     statusLabels[statusVal] || statusVal,
        Data_Hora:       dataHoraV.toString(),
        Total_Final:     parseFloat(encontrado.Total_Final || 0),
        Entregador_Nome: (encontrado.Entregador_Nome || '').toString(),
        nomeCliente,
        endereco:        enderecoCli,
        itensResumo,
      };
  });

  return resultados;
}


// ══════════════════════════════════════════════════════════
// PEDIDOS — ESCRITA (PDV, Balcão, Delivery, Online)
// ══════════════════════════════════════════════════════════

const ONESIGNAL_REST_API_KEY = process.env.ONESIGNAL_REST_API_KEY || '';
const ONESIGNAL_APP_ID = 'c7565fa8-cd8c-4264-85b4-1a9d9cf150af';

async function dispararPushOneSignal(lojaId, titulo, mensagem, ignorarEntregador = false) {
  if (!ONESIGNAL_REST_API_KEY) return;
  try {
    const filtros = [{ field: "tag", key: "loja_id", relation: "=", value: lojaId }];
    // Se for Retirada, manda um filtro extra bloqueando o sinal para os Entregadores
    if (ignorarEntregador) {
        filtros.push({ field: "tag", key: "cargo", relation: "!=", value: "Entregador" });
    }
    await fetch('https://onesignal.com/api/v1/notifications', {
      method: 'POST',
      headers: { 
        'Content-Type': 'application/json', 
        'Authorization': `Key ${ONESIGNAL_REST_API_KEY}` 
      },
      body: JSON.stringify({
        app_id: ONESIGNAL_APP_ID,
        filters: filtros,
        headings: { "en": titulo },
        contents: { "en": mensagem }
      })
    });
  } catch (e) { console.error('[OneSignal] Erro:', e); }
}

const _COLUNAS_PEDIDO_DEFAULT = {
  ID_Venda: '', Origem: '', Data_Hora: '', Operador: '', Cliente_Info: '',
  Itens_Comprados: '', Subtotal: 0, Desconto: 0, Taxa_Entrega: 0,
  Total_Final: 0, Metodo_Pagamento: '', Status: '',
  Peso_Bruto_g: 0, ID_Tara: '', Peso_Tara_g: 0, Peso_Liquido_g: 0,
  Preco_KG: 0, Troco: 0, Entregador_Nome: '', Cancelado_Por: '',
};

async function registrarVendaPDV(lojaId, pedido) {
  const doc = Object.assign({}, _COLUNAS_PEDIDO_DEFAULT, pedido);
  const res = await salvarRegistro(lojaId, 'pedidos', 'VND', doc, 'ID_Venda');
  
  if (res.sucesso && (pedido.Origem === 'DELIVERY' || pedido.Origem === 'ONLINE')) {
     let isRetirada = false;
     try {
         let cli = typeof pedido.Cliente_Info === 'string' ? JSON.parse(pedido.Cliente_Info) : (pedido.Cliente_Info || {});
         if (cli.endereco === 'Retirada na loja') isRetirada = true;
     } catch(e){}

     // Dispara o alerta blindando o motoboy se for Retirada
     if (isRetirada) {
         dispararPushOneSignal(lojaId, '🛍️ Nova Retirada!', 'Cliente vem buscar na loja. Verifique o painel.', true);
     } else {
         dispararPushOneSignal(lojaId, '🔔 Novo Pedido de Delivery!', 'Uma nova entrega caiu no painel. Verifique!', false);
     }
  }
  return res;
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
        Nome: dados.nomeCliente.trim(),
        CPF: cpfLimpo || '',
        Telefone: dados.telefone ? dados.telefone.replace(/\D/g, '') : '',
      });
    }
  }

  const pedido = {
    ID_Venda: '', Origem: 'BALCAO', Data_Hora: _agora(),
    Operador: dados.operador || 'CAIXA',
    Cliente_Info: JSON.stringify({ nome: dados.nomeCliente || '', cpf: dados.cpfCliente || '', telefone: dados.telefone || '' }),
    Itens_Comprados: JSON.stringify(todosItens),
    Subtotal: subtotal, Desconto: desconto, Taxa_Entrega: 0, Total_Final: total,
    Metodo_Pagamento: dados.pagamento || '', Status: 'ENTREGUE',
    Peso_Bruto_g: pesoBruto, ID_Tara: dados.idTara || '', Peso_Tara_g: pesoTara,
    Peso_Liquido_g: pesoLiq, Preco_KG: precoKG, Troco: troco,
    Entregador_Nome: '', Cancelado_Por: '',
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
    ID_Venda: '', Origem: 'DELIVERY', Data_Hora: _agora(),
    Operador: dados.operador || 'CAIXA',
    Cliente_Info: JSON.stringify({ nome: dados.nomeCliente || '', cpf: dados.cpfCliente || '', telefone: dados.telefone || '', endereco: dados.endereco || '' }),
    Itens_Comprados: JSON.stringify(itens),
    Subtotal: subtotal, Desconto: desconto, Taxa_Entrega: taxaEnt, Total_Final: total,
    Metodo_Pagamento: dados.pagamento || '', Status: 'NOVO',
    Peso_Bruto_g: 0, ID_Tara: '', Peso_Tara_g: 0, Peso_Liquido_g: 0,
    Preco_KG: 0, Troco: troco, Entregador_Nome: '', Cancelado_Por: '',
  };

  const resultado = await registrarVendaPDV(lojaId, pedido);
  if (resultado.sucesso) Object.assign(resultado, { troco, total });
  return resultado;
}

/** Finaliza pedido do cardápio online (rota pública — recalcula total no servidor). */
async function finalizarPedidoOnline(lojaId, dadosPedido) {
  if (!dadosPedido?.itens?.length) throw new Error('Pedido vazio ou formato inválido.');

  const config = await getConfiguracoes(lojaId);
  if ((config['Status_Loja'] || 'AUTOMATICO') === 'FORCAR_FECHADO')
    throw new Error('A loja está fechada no momento. Tente mais tarde.');

  const subtotalReal    = Math.round(dadosPedido.itens.reduce((s, i) => s + parseFloat(i.preco || 0), 0) * 100) / 100;
  const subtotalEnviado = parseFloat(dadosPedido.subtotal || 0);
  if (Math.abs(subtotalReal - subtotalEnviado) > 0.05)
    throw new Error('Divergência financeira detectada. Pedido rejeitado por segurança.');

  const desconto = parseFloat(dadosPedido.desconto    || 0);
  const taxaEnt  = parseFloat(dadosPedido.taxaEntrega || 0);
  const total    = Math.max(0, Math.round((subtotalReal - desconto + taxaEnt) * 100) / 100);

  const pedido = {
    ID_Venda: '', Origem: 'ONLINE', Data_Hora: _agora(), Operador: 'APP',
    Cliente_Info: JSON.stringify({ nome: dadosPedido.nomeCliente || '', cpf: '', telefone: dadosPedido.telefone || '', endereco: dadosPedido.endereco || '' }),
    Itens_Comprados: JSON.stringify(dadosPedido.itens),
    Subtotal: subtotalReal, Desconto: desconto, Taxa_Entrega: taxaEnt, Total_Final: total,
    Metodo_Pagamento: dadosPedido.pagamento || '', Status: 'NOVO',
    Peso_Bruto_g: 0, ID_Tara: '', Peso_Tara_g: 0, Peso_Liquido_g: 0,
    Preco_KG: 0, Troco: 0, Entregador_Nome: '', Cancelado_Por: '',
  };

  return registrarVendaPDV(lojaId, pedido);
}

async function atualizarStatusPedido(lojaId, idVenda, novoStatus) {
  const statusValidos = ['NOVO','PREPARANDO','PRONTO','ENTREGUE','CANCELADO','EM_MONTAGEM','A_CAMINHO','AGUARDANDO_PIX'];
  if (!statusValidos.includes(novoStatus))
    return { sucesso: false, mensagem: `Status inválido: ${novoStatus}` };

  const ref = _col(lojaId, 'pedidos').doc(idVenda.toString());
  const doc = await ref.get();
  if (!doc.exists) return { sucesso: false, mensagem: `Pedido "${idVenda}" não encontrado.` };

  await ref.update({ Status: novoStatus });
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

  const ref = _col(lojaId, 'pedidos').doc(idVenda.toString());

  return db.runTransaction(async tx => {
    const doc = await tx.get(ref);
    if (!doc.exists) throw new Error(`Pedido "${idVenda}" não encontrado.`);

    const p           = doc.data();
    const statusAtual = (p.Status || '').toUpperCase();
    const origemAtual = (p.Origem || '').toUpperCase();

    if (statusAtual === 'CANCELADO') throw new Error('Pedido já cancelado.');
    if (statusAtual === 'ENTREGUE' && origemAtual !== 'BALCAO') throw new Error('Pedido já entregue.');

    tx.update(ref, {
      Status:       'CANCELADO',
      Cancelado_Por: `${auth.nome} (${auth.cargo}) — ${_agora()}`,
    });

    return { sucesso: true, mensagem: `Pedido cancelado por ${auth.nome} (${auth.cargo}).` };
  });
}

async function pegarPedidoDelivery(lojaId, idVenda, nomeEntregador) {
  if (!idVenda || !nomeEntregador?.toString().trim())
    return { sucesso: false, mensagem: 'ID do pedido e nome do entregador são obrigatórios.' };

  const nomeTrimmed = nomeEntregador.toString().trim();
  const ref = _col(lojaId, 'pedidos').doc(idVenda.toString());

  return db.runTransaction(async tx => {
    const doc = await tx.get(ref);
    if (!doc.exists) throw new Error('Pedido não encontrado.');

    const p           = doc.data();
    const status      = (p.Status          || '').toUpperCase();
    const entregAtual = (p.Entregador_Nome || '').toString().trim();

    if (status === 'ENTREGUE')  throw new Error('Pedido já entregue.');
    if (status === 'CANCELADO') throw new Error('Pedido cancelado.');

    if (entregAtual && entregAtual.toLowerCase() !== nomeTrimmed.toLowerCase())
      return { sucesso: false, mensagem: `Este pedido já foi pego por ${entregAtual}.`, entregadorAtual: entregAtual };

    tx.update(ref, { Entregador_Nome: nomeTrimmed, Status: 'EM_MONTAGEM' });
    return { sucesso: true, mensagem: `Pedido atribuído a ${nomeTrimmed}.` };
  });
}


// ══════════════════════════════════════════════════════════
// CADASTROS (CRUD genérico por loja)
// ══════════════════════════════════════════════════════════

const salvarUsuario         = (lojaId, d) => salvarRegistro(lojaId, 'usuarios',          'USR', d, 'ID_Usuario');
const excluirUsuario        = (lojaId, id) => deletarRegistro(lojaId, 'usuarios', id);
const salvarAcaiCategoria   = (lojaId, d) => salvarRegistro(lojaId, 'acai-categorias',   'CAT', d, 'ID_Categoria');
const excluirAcaiCategoria  = (lojaId, id) => deletarRegistro(lojaId, 'acai-categorias', id);
const salvarAcaiIngrediente = (lojaId, d) => salvarRegistro(lojaId, 'acai-ingredientes', 'ING', d, 'ID_Ingrediente');
const excluirAcaiIngrediente= (lojaId, id) => deletarRegistro(lojaId, 'acai-ingredientes', id);
const salvarAcaiModelo      = (lojaId, d) => salvarRegistro(lojaId, 'acai-modelos',      'MOD', d, 'ID_Modelo');
const excluirAcaiModelo     = (lojaId, id) => deletarRegistro(lojaId, 'acai-modelos', id);
const salvarItemFixo        = (lojaId, d) => salvarRegistro(lojaId, 'cardapio',          'IT',  d, 'ID_Item');
const excluirItemFixo       = (lojaId, id) => deletarRegistro(lojaId, 'cardapio', id);
const salvarBairro          = (lojaId, d) => salvarRegistro(lojaId, 'bairros',           'BRR', d, 'ID_Bairro');
const excluirBairro         = (lojaId, id) => deletarRegistro(lojaId, 'bairros', id);
const salvarCupom           = (lojaId, d) => salvarRegistro(lojaId, 'cupons',            'CUP', d, 'Codigo_Cupom');
const excluirCupom          = (lojaId, id) => deletarRegistro(lojaId, 'cupons', id);
const salvarTara            = (lojaId, d) => salvarRegistro(lojaId, 'taras',             'TRA', d, 'ID_Tara');
const excluirTara           = (lojaId, id) => deletarRegistro(lojaId, 'taras', id);

async function salvarCliente(lojaId, dados) {
  if (!dados.Data_Cadastro) dados.Data_Cadastro = _agora();
  return salvarRegistro(lojaId, 'clientes', 'CLI', dados, 'ID_Cliente');
}

async function buscarClientePorCPF(lojaId, cpf) {
  const cpfLimpo = cpf.toString().replace(/\D/g, '');
  if (cpfLimpo.length !== 11) return null;
  const snap = await _col(lojaId, 'clientes').where('CPF', '==', cpfLimpo).limit(1).get();
  return snap.empty ? null : snap.docs[0].data();
}

async function buscarClientePorTelefone(lojaId, telefone) {
  const telLimpo = telefone.toString().replace(/\D/g, '');
  const snap = await _col(lojaId, 'clientes').where('Telefone', '==', telLimpo).limit(1).get();
  return snap.empty ? null : snap.docs[0].data();
}

async function salvarConfiguracoesLote(lojaId, configObj) {
  await _col(lojaId, 'configuracoes').doc('main').set(configObj, { merge: true });
  invalidarCacheCardapio(lojaId);
  return { sucesso: true, mensagem: 'Configurações salvas.' };
}


// ══════════════════════════════════════════════════════════
// VALIDAÇÃO DE CUPOM
// ══════════════════════════════════════════════════════════

async function validarCupom(lojaId, codigo) {
  if (!codigo?.toString().trim()) return { valido: false, mensagem: 'Digite um código de cupom.' };

  const codigoUpper = codigo.toString().trim().toUpperCase();
  const docRef = await _col(lojaId, 'cupons').doc(codigoUpper).get();

  let d = docRef.exists ? docRef.data() : null;
  if (!d) {
    const snap = await _col(lojaId, 'cupons')
      .where('Codigo_Cupom', '==', codigoUpper).limit(1).get();
    if (snap.empty) return { valido: false, mensagem: 'Cupom não encontrado.' };
    d = snap.docs[0].data();
  }

  if ((d.Ativo || '').toUpperCase() !== 'SIM')
    return { valido: false, mensagem: 'Cupom inativo ou esgotado.' };

  if (d.Validade) {
    const validade = new Date(
      d.Validade instanceof admin.firestore.Timestamp ? d.Validade.toDate() : d.Validade
    );
    if (!isNaN(validade.getTime())) {
      validade.setHours(23, 59, 59, 999);
      if (validade < new Date()) return { valido: false, mensagem: 'Cupom expirado.' };
    }
  }

  const tipo         = (d.Tipo_Desconto  || 'VALOR').toUpperCase();
  const usarCardapio = (d.Usar_Cardapio  || 'SIM').toUpperCase();
  const valorBruto   = parseFloat(d.Valor_Desconto) || 0;

  const msgDesc = tipo === 'PERCENTUAL'
    ? `Desconto de ${valorBruto.toFixed(0)}% aplicado 🎉`
    : `Desconto de R$ ${valorBruto.toFixed(2).replace('.', ',')} aplicado 🎉`;

  return {
    valido: true,
    codigo: (d.Codigo_Cupom || '').toString(),
    tipo,
    valor:        valorBruto,
    desconto:     tipo === 'VALOR' ? valorBruto : 0,
    usarCardapio: usarCardapio === 'SIM',
    mensagem:     msgDesc,
  };
}


// ══════════════════════════════════════════════════════════
// MERCADO PAGO — PIX AUTOMÁTICO
// ══════════════════════════════════════════════════════════

async function gerarPixMP(lojaId, total, idVenda, emailPagador) {
  const config = await getConfiguracoes(lojaId);
  const token  = (config['MP_AccessToken'] || '').toString().trim();
  if (!token) return { sucesso: false, mensagem: 'Token do Mercado Pago não configurado.' };

  const idempKey = 'PIX-' + Date.now() + '-' + Math.floor(Math.random() * 1000);
  const resp = await fetch('https://api.mercadopago.com/v1/payments', {
    method:  'POST',
    headers: { 
      'Content-Type': 'application/json', 
      'Authorization': `Bearer ${token}`,
      'X-Idempotency-Key': idempKey
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
  const token  = (config['MP_AccessToken'] || '').toString().trim();
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
  const modoMP = (config['PIX_Modo'] || 'MANUAL').toUpperCase();

  // Agora ele aceita AUTO, AUTOMATICO ou AUTOMÁTICO
  if (modoMP !== 'AUTO' && modoMP !== 'AUTOMATICO' && modoMP !== 'AUTOMÁTICO') {
    return finalizarPedidoOnline(lojaId, dadosPedido);
  }

  if (!dadosPedido?.itens?.length) throw new Error('Pedido vazio ou formato inválido.');
  if ((config['Status_Loja'] || 'AUTOMATICO') === 'FORCAR_FECHADO')
    throw new Error('A loja está fechada no momento.');

  const subtotalReal    = Math.round(dadosPedido.itens.reduce((s, i) => s + parseFloat(i.preco || 0), 0) * 100) / 100;
  const subtotalEnviado = parseFloat(dadosPedido.subtotal || 0);
  if (Math.abs(subtotalReal - subtotalEnviado) > 0.05)
    throw new Error('Divergência financeira detectada. Pedido rejeitado por segurança.');

  const desconto = parseFloat(dadosPedido.desconto    || 0);
  const taxaEnt  = parseFloat(dadosPedido.taxaEntrega || 0);
  const total    = Math.max(0, Math.round((subtotalReal - desconto + taxaEnt) * 100) / 100);

  // 1. Tenta gerar o PIX no Mercado Pago com um ID provisório (escondido)
  const idVendaTemp = 'WEB-' + Date.now().toString().substring(5);
  const pix = await gerarPixMP(
    lojaId, total, idVendaTemp,
    dadosPedido.telefone ? `${dadosPedido.telefone.replace(/\D/g,'')}@mp.br` : 'contato@acaiteria.com.br'
  );

  // 2. Se der erro no PIX, ele para aqui e NEM salva no painel da loja
  if (!pix.sucesso) {
    return { sucesso: false, mensagem: `PIX não gerado: ${pix.mensagem}. Tente outra forma de pagamento.` };
  }

  // 3. Se gerou o PIX com sucesso, aí sim criamos o pedido oficial no banco de dados
  const pedido = {
    ID_Venda: '', Origem: 'ONLINE', Data_Hora: _agora(), Operador: 'APP',
    Cliente_Info: JSON.stringify({ nome: dadosPedido.nomeCliente || '', cpf: '', telefone: dadosPedido.telefone || '', endereco: dadosPedido.endereco || '' }),
    Itens_Comprados: JSON.stringify(dadosPedido.itens),
    Subtotal: subtotalReal, Desconto: desconto, Taxa_Entrega: taxaEnt, Total_Final: total,
    Metodo_Pagamento: 'PIX', Status: 'AGUARDANDO_PIX',
    Peso_Bruto_g: 0, ID_Tara: '', Peso_Tara_g: 0, Peso_Liquido_g: 0,
    Preco_KG: 0, Troco: 0, Entregador_Nome: '', Cancelado_Por: '',
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

async function getConfigPix(lojaId) {
  const config = await getConfiguracoes(lojaId);
  const token  = (config['MP_AccessToken'] || '').toString().trim();
  return {
    modoMP:        (config['PIX_Modo'] || 'MANUAL').toUpperCase(),
    mpConfigurado: token.length > 10,
  };
}


// ══════════════════════════════════════════════════════════
// ROTAS — AUTENTICAÇÃO
// ══════════════════════════════════════════════════════════

/**
 * POST /api/lojas/:loja_id/auth/login
 * Body: { usuario, senha }
 * Retorna token JWT com { loja_id, id, nome, cargo }
 */
app.post('/api/lojas/:loja_id/auth/login', handler(async req => {
  const lojaId   = req.params.loja_id;
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

/**
 * POST /api/lojas/:loja_id/auth/validar-supervisor
 * Body: { login, senha }
 */
app.post('/api/lojas/:loja_id/auth/validar-supervisor', handler(req =>
  validarSupervisorOuAcima(req.params.loja_id, req.body.login, req.body.senha)
));


// ══════════════════════════════════════════════════════════
// ROTAS — WEBHOOK ASAAS
// Não requer autenticação JWT, mas valida token do Asaas.
// ══════════════════════════════════════════════════════════

/**
 * POST /api/webhooks/asaas
 *
 * Eventos tratados:
 *   PAYMENT_OVERDUE  → status da loja = 'bloqueado'
 *   PAYMENT_RECEIVED → status da loja = 'ativo'
 */
app.post('/api/webhooks/asaas', async (req, res) => {
  try {
    // Verifica token de segurança do Asaas (configurado no painel Asaas)
    const tokenRecebido = req.headers['asaas-access-token'] || req.headers['x-access-token'] || '';
    if (ASAAS_WEBHOOK_TOKEN && tokenRecebido !== ASAAS_WEBHOOK_TOKEN) {
      console.warn('[Webhook Asaas] Token inválido recebido:', tokenRecebido);
      return res.status(401).json({ sucesso: false, mensagem: 'Token inválido.' });
    }

    const { event, payment } = req.body;

    if (!event || !payment?.customer) {
      return res.status(400).json({ sucesso: false, mensagem: 'Payload inválido.' });
    }

    const customerId = payment.customer;
    const eventosMonitorados = ['PAYMENT_OVERDUE', 'PAYMENT_RECEIVED', 'PAYMENT_RESTORED'];

    if (!eventosMonitorados.includes(event)) {
      // Evento não relevante — aceita sem processar (boa prática)
      return res.json({ sucesso: true, mensagem: `Evento "${event}" ignorado.` });
    }

    // Busca a loja pelo asaas_customer_id
    const snap = await db.collection('lojas')
      .where('asaas_customer_id', '==', customerId)
      .limit(1)
      .get();

    if (snap.empty) {
      console.warn(`[Webhook Asaas] Nenhuma loja encontrada para customer: ${customerId}`);
      return res.status(404).json({ sucesso: false, mensagem: 'Loja não encontrada para este customer.' });
    }

    const lojaRef  = snap.docs[0].ref;
    const lojaId   = snap.docs[0].id;
    let novoStatus;

    if (event === 'PAYMENT_OVERDUE') {
      novoStatus = 'bloqueado';
    } else {
      // PAYMENT_RECEIVED ou PAYMENT_RESTORED
      novoStatus = 'ativo';
    }

    await lojaRef.update({ status: novoStatus, ultimo_evento_asaas: event, atualizado_em: _agora() });

    // Se bloqueou, invalida o cache da loja para forçar revalidação
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
  handler(req => getDadosPainelGeral(req.params.loja_id)));

app.get('/api/lojas/:loja_id/dados-config',
  ...guardCargo('Dono'),
  handler(req => getDadosConfig(req.params.loja_id)));

app.get('/api/lojas/:loja_id/dados-monte-acai',
  handler(req => getDadosMonteAcai(req.params.loja_id)));

// Rota pública do cardápio (sem autenticação — verifica status via middleware próprio)
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

// ATENÇÃO: balcão e delivery são rotas internas (painel), exigem cargo
app.post('/api/lojas/:loja_id/pedidos/balcao',
  ...guardCargo('Dono', 'Gerente', 'Supervisor', 'Operador'),
  handler(req => registrarVendaBalcao(req.params.loja_id, req.body)));

app.post('/api/lojas/:loja_id/pedidos/delivery',
  ...guardCargo('Dono', 'Gerente', 'Supervisor', 'Operador'),
  handler(req => registrarVendaDelivery(req.params.loja_id, req.body)));

// Pedido online: rota PÚBLICA (cliente do cardápio) — verifica apenas status da loja
app.post('/api/lojas/:loja_id/pedidos/online',
  verificarStatusLoja,
  handler(async req => {
    try { return await finalizarPedidoOnline(req.params.loja_id, req.body); }
    catch (e) { return { sucesso: false, mensagem: e.message }; }
  }));

// PIX online: também público
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

// Cancelamento: público apenas na assinatura (usa validação interna de supervisor)
app.post('/api/lojas/:loja_id/pedidos/:id/cancelar',
  handler(req => cancelarPedidoAutorizado(req.params.loja_id, req.params.id, req.body.login, req.body.senha)));

app.post('/api/lojas/:loja_id/pedidos/:id/pegar-delivery',
  ...guardLoja,
  handler(req => pegarPedidoDelivery(req.params.loja_id, req.params.id, req.body.nomeEntregador)));

// Exclusão definitiva de um pedido (Apenas o Dono tem permissão)
app.delete('/api/lojas/:loja_id/pedidos/:id',
  ...guardCargo('Dono'),
  handler(req => deletarRegistro(req.params.loja_id, 'pedidos', req.params.id)));

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
  ...guardCargo('Dono', 'Gerente'),
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

app.get('/api/health', (_, res) => res.json({ status: 'ok', ts: _agora() }));


// ══════════════════════════════════════════════════════════
// START
// ══════════════════════════════════════════════════════════

app.listen(PORT, () => {
  console.log(`✅  API SaaS Açaíteria rodando na porta ${PORT}`);

  // ════════════════════════════════════════════════════════
  // ⚠️  CHECKLIST DE PRÉ-VENDA — LEIA ANTES DE ATIVAR
  // ════════════════════════════════════════════════════════
  console.log(`
  ╔══════════════════════════════════════════════════════════════════════╗
  ║  ⚠️  CHECKLIST OBRIGATÓRIO ANTES DE COMEÇAR A VENDER               ║
  ╠══════════════════════════════════════════════════════════════════════╣
  ║                                                                      ║
  ║  FIREBASE:                                                           ║
  ║  [1] Criar documento em /lojas/{loja_id} com os campos:             ║
  ║      - status: "ativo"                                               ║
  ║      - asaas_customer_id: "<ID do cliente no Asaas>"                ║
  ║      - nome_loja, criado_em, plano, etc.                            ║
  ║  [2] Migrar dados existentes para a nova estrutura de subcoleções.   ║
  ║      ATENÇÃO: Não existe migração automática.                        ║
  ║  [3] Criar índice composto no Firestore:                             ║
  ║      Coleção: lojas/{id}/pedidos                                     ║
  ║      Campos: Origem (ASC) + Data_Hora (DESC)                        ║
  ║                                                                      ║
  ║  ASAAS:                                                              ║
  ║  [4] Em Configurações → Integrações → Webhooks, cadastrar a URL:    ║
  ║      https://SEU_BACKEND/api/webhooks/asaas                         ║
  ║  [5] Copiar o "Token de Acesso" gerado pelo Asaas e colocar         ║
  ║      no .env como: ASAAS_WEBHOOK_TOKEN=<token>                      ║
  ║  [6] Criar o cliente no Asaas para cada loja e salvar o             ║
  ║      ID do customer no campo asaas_customer_id do documento Firestore║
  ║                                                                      ║
  ║  SEGURANÇA:                                                          ║
  ║  [7] O arquivo .env NUNCA deve ser commitado no Git.                 ║
  ║      Adicione .env ao .gitignore AGORA.                             ║
  ║  [8] Use um JWT_SECRET forte (mínimo 64 chars aleatórios).          ║
  ║      Gere um em: node -e "console.log(require('crypto')             ║
  ║      .randomBytes(64).toString('hex'))"                             ║
  ║                                                                      ║
  ╚══════════════════════════════════════════════════════════════════════╝
  `);
});
