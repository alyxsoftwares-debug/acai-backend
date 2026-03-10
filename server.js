/**
 * ╔══════════════════════════════════════════════════════════════════════════════╗
 * ║  AÇAÍTERIA SaaS — BACK-END Node.js + Express + Supabase (PostgreSQL)        ║
 * ║  Arquitetura Multi-tenant  |  Cobrança via Asaas                            ║
 * ║                                                                              ║
 * ║  Estrutura PostgreSQL (schema: acaiteria):                                  ║
 * ║   acaiteria.lojas          → { status, asaas_customer_id, ... }             ║
 * ║   acaiteria.usuarios       → usuários da loja (loja_id FK)                  ║
 * ║   acaiteria.pedidos        → pedidos/vendas  (loja_id FK)                   ║
 * ║   acaiteria.cardapio       → itens fixos     (loja_id FK)                   ║
 * ║   acaiteria.acai_categorias                  (loja_id FK)                   ║
 * ║   acaiteria.acai_ingredientes                (loja_id FK)                   ║
 * ║   acaiteria.acai_modelos                     (loja_id FK)                   ║
 * ║   acaiteria.bairros                          (loja_id FK)                   ║
 * ║   acaiteria.cupons                           (loja_id FK)                   ║
 * ║   acaiteria.taras                            (loja_id FK)                   ║
 * ║   acaiteria.clientes                         (loja_id FK)                   ║
 * ║   acaiteria.configuracoes  → 1:1 com lojas   (loja_id PK+FK)               ║
 * ╚══════════════════════════════════════════════════════════════════════════════╝
 *
 * Variáveis de ambiente (.env):
 *   PORT=3000
 *   TZ=America/Fortaleza
 *   JWT_SECRET=<string longa e aleatória>
 *   SUPABASE_URL=https://<seu-projeto>.supabase.co
 *   SUPABASE_SERVICE_ROLE_KEY=<sua-service-role-key>
 *   ASAAS_WEBHOOK_TOKEN=<token que você define no painel Asaas>
 *   ONESIGNAL_REST_API_KEY=<chave da API OneSignal>
 *
 * ⚠️  PRÉ-REQUISITO SUPABASE:
 *   Em Settings → API → "Extra Search Path", adicione o schema: acaiteria
 */

'use strict';

const express         = require('express');
const cors            = require('cors');
const { createClient } = require('@supabase/supabase-js');
const jwt             = require('jsonwebtoken');
require('dotenv').config();


// ══════════════════════════════════════════════════════════
// VALIDAÇÃO DE AMBIENTE
// ══════════════════════════════════════════════════════════

const JWT_SECRET                = process.env.JWT_SECRET;
const ASAAS_WEBHOOK_TOKEN       = process.env.ASAAS_WEBHOOK_TOKEN || '';
const PORT                      = process.env.PORT || 3000;
const TZ                        = process.env.TZ   || 'America/Fortaleza';
const SUPABASE_URL              = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!JWT_SECRET)                throw new Error('❌  JWT_SECRET não definido no .env');
if (!SUPABASE_URL)              throw new Error('❌  SUPABASE_URL não definido no .env');
if (!SUPABASE_SERVICE_ROLE_KEY) throw new Error('❌  SUPABASE_SERVICE_ROLE_KEY não definido no .env');
if (!ASAAS_WEBHOOK_TOKEN) {
  console.warn('⚠️  ASAAS_WEBHOOK_TOKEN não definido — webhook Asaas estará desprotegido!');
}


// ══════════════════════════════════════════════════════════
// SUPABASE CLIENT
// Service Role Key: bypassa RLS → todo isolamento de loja_id
// é garantido pelo código (middlewares + queries).
// ══════════════════════════════════════════════════════════

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  db:   { schema: 'acaiteria' },          // todas as queries são em acaiteria.*
  auth: { persistSession: false, autoRefreshToken: false },
});

// ══════════════════════════════════════════════════════════
// EXPRESS
// ══════════════════════════════════════════════════════════

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
// TRADUTOR DE SLUG PARA UUID (URL BONITA)
// Intercepta e reescreve a URL antes do servidor processar
// ══════════════════════════════════════════════════════════
app.use(async (req, res, next) => {
  if (req.url.startsWith('/api/lojas/')) {
    // Isola apenas o nome da loja (ignorando coisas como ?q=busca)
    const urlSemQuery = req.url.split('?')[0]; 
    const partes = urlSemQuery.split('/');
    const idOuSlug = partes[3]; // Pega a palavra que está na posição da loja
    
    if (idOuSlug && idOuSlug !== 'undefined') {
      const isUUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(idOuSlug);
      
      if (!isUUID) {
        // Se não for UUID, busca no banco de dados qual é o UUID dessa palavra
        const { data } = await supabase
          .from('lojas')
          .select('id')
          .eq('slug', idOuSlug.toLowerCase())
          .maybeSingle();
          
        if (data && data.id) {
          // Engana o servidor trocando a palavra pelo UUID real na URL
          req.url = req.url.replace(`/api/lojas/${idOuSlug}`, `/api/lojas/${data.id}`);
        } else {
          return res.status(404).json({ sucesso: false, mensagem: 'Loja não encontrada pelo link bonito.' });
        }
      }
    }
  }
  next();
});


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

/**
 * Converte valor de data para objeto Date.
 * Suporta: ISO string do Postgres, string "dd/MM/yyyy HH:mm:ss", Date, e
 * Firestore Timestamp legado (toDate()).
 */
function _toDate(valor) {
  if (!valor) return null;
  if (typeof valor.toDate === 'function') return valor.toDate(); // Firestore legado
  if (valor instanceof Date) return isNaN(valor.getTime()) ? null : valor;
  const str = valor.toString().trim();
  if (!str) return null;
  // Formato ISO do PostgreSQL: "2024-11-08T17:30:45.000Z"
  if (/^\d{4}-\d{2}-\d{2}T/.test(str)) return new Date(str);
  // Formato já normalizado pelo app: "08/11/2024 14:30:45"
  const partes = str.split(' ');
  if (partes.length >= 2 && partes[0].includes('/')) {
    const d = partes[0].split('/');
    const t = partes[1].split(':');
    return new Date(+d[2], +d[1] - 1, +d[0], +t[0], +t[1], +(t[2] || 0));
  }
  return _parseDataDDMMYYYY(partes[0]);
}

/**
 * Converte timestamps ISO do Postgres para "dd/MM/yyyy HH:mm:ss" dentro de um
 * objeto (compatibilidade retroativa com o formato esperado pelo frontend).
 */
function _normalizarTimestamps(obj) {
  for (const k of Object.keys(obj)) {
    const v = obj[k];
    if (v && typeof v === 'string' && /^\d{4}-\d{2}-\d{2}T/.test(v)) {
      obj[k] = new Date(v)
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

const CACHE_TTL_MS   = 15 * 60 * 1000;
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
// MAPEAMENTO: Coleção Firestore → Tabela Postgres
// ══════════════════════════════════════════════════════════

/** Firestore collection name → Postgres table name (dentro do schema acaiteria). */
const _TABLE_MAP = {
  'lojas':             'lojas',
  'usuarios':          'usuarios',
  'pedidos':           'pedidos',
  'cardapio':          'cardapio',
  'acai-categorias':   'acai_categorias',
  'acai-ingredientes': 'acai_ingredientes',
  'acai-modelos':      'acai_modelos',
  'bairros':           'bairros',
  'cupons':            'cupons',
  'taras':             'taras',
  'clientes':          'clientes',
  'configuracoes':     'configuracoes',
};

/** Coluna de chave primária por coleção. */
const _PK_MAP = {
  'usuarios':          'ID_Usuario',
  'pedidos':           'ID_Venda',
  'cardapio':          'ID_Item',
  'acai-categorias':   'ID_Categoria',
  'acai-ingredientes': 'ID_Ingrediente',
  'acai-modelos':      'ID_Modelo',
  'bairros':           'ID_Bairro',
  'cupons':            'Codigo_Cupom',
  'taras':             'ID_Tara',
  'clientes':          'ID_Cliente',
};

/**
 * Coleções cujos campos booleanos no Postgres devem ser expostos como
 * 'SIM' / 'NAO' para o frontend (retrocompatibilidade com Firestore).
 */
const _BOOL_SIM_NAO = {
  'acai-ingredientes': ['Disponivel'],
  'acai-modelos':      ['Disponivel'],
  'cardapio':          ['Disponivel', 'Mostrar_Online'],
  'bairros':           ['Disponivel'],
  'taras':             ['Ativo'],
  'cupons':            ['Ativo', 'Usar_Cardapio'],
  'acai-categorias':   ['Ativo'],
  'configuracoes':     ['Retirada_Loja_Ativo', 'AutoImprimir_Balcao', 'AutoImprimir_Delivery', 'Mostrar_AcaiRapido_PDV'],
};

/** Coleções cujo cache de cardápio deve ser invalidado após escrita. */
const _CACHE_COLS = new Set([
  'cardapio', 'acai-modelos', 'acai-categorias',
  'acai-ingredientes', 'bairros', 'configuracoes', 'cupons',
]);

/**
 * Mapeamento bidirecional entre nomes de campo do frontend (Firestore legado)
 * e nomes de coluna reais no Postgres.
 *
 * toApp  → renomear coluna DB para o nome que o frontend espera (leitura)
 * toDB   → renomear campo do frontend para o nome da coluna DB (escrita)
 * dbCols → whitelist de colunas válidas da tabela (evita erro de coluna desconhecida)
 */
const _FIELD_MAP = {
  'acai-categorias': {
    toApp:  { Nome: 'Nome_Categoria' },
    toDB:   { Nome_Categoria: 'Nome' },
    dbCols: ['ID_Categoria', 'loja_id', 'Nome', 'Ordem', 'Ativo', 'criado_em'],
  },
  'acai-ingredientes': {
    toApp:  { categoria_id: 'ID_Categoria', Foto_URL: 'Foto' },
    toDB:   { ID_Categoria: 'categoria_id', Foto: 'Foto_URL' },
    dbCols: ['ID_Ingrediente', 'loja_id', 'categoria_id', 'Nome', 'Disponivel',
             'Ordem', 'Foto_URL', 'Descricao', 'Preco', 'criado_em', 'atualizado_em'],
  },
  'acai-modelos': {
    toApp:  { Preco: 'Preco_Base', Foto_URL: 'Foto' },
    toDB:   { Preco_Base: 'Preco', Foto: 'Foto_URL' },
    dbCols: ['ID_Modelo', 'loja_id', 'Nome', 'Descricao', 'Capacidade_ml',
             'Preco', 'Disponivel', 'Ordem', 'Foto_URL', 'criado_em', 'atualizado_em'],
  },
  'cardapio': {
    toApp:  { Foto_URL: 'Foto' },
    toDB:   { Foto: 'Foto_URL' },
    dbCols: ['ID_Item', 'loja_id', 'Nome', 'Descricao', 'Preco', 'Categoria',
             'Disponivel', 'Mostrar_Online', 'Foto_URL', 'Ordem', 'criado_em', 'atualizado_em'],
  },
  'bairros': {
    toApp:  { Nome: 'Nome_Bairro', Taxa_Entrega: 'Taxa_R$' },
    toDB:   { Nome_Bairro: 'Nome', 'Taxa_R$': 'Taxa_Entrega' },
    dbCols: ['ID_Bairro', 'loja_id', 'Nome', 'Taxa_Entrega', 'Disponivel',
             'Tempo_Entrega', 'Ordem', 'criado_em'],
  },
  'taras': {
    toApp:  { Nome: 'Nome_Tara', Peso_g: 'Gramas', Foto_URL: 'Foto' },
    toDB:   { Nome_Tara: 'Nome', Gramas: 'Peso_g', Foto: 'Foto_URL' },
    dbCols: ['ID_Tara', 'loja_id', 'Nome', 'Peso_g', 'Ativo', 'Foto_URL', 'criado_em'],
  },
};


// ══════════════════════════════════════════════════════════
// NORMALIZAÇÃO DE DADOS (DB ↔ APP)
// ══════════════════════════════════════════════════════════

/**
 * Normaliza uma linha do Postgres para o formato esperado pelo frontend:
 * - TIMESTAMPTZ ISO → "dd/MM/yyyy HH:mm:ss"
 * - BOOLEAN → 'SIM' / 'NAO' para campos mapeados em _BOOL_SIM_NAO
 * - usuarios.Status: 'ativo' → 'Ativo' (capitalização original do Firestore)
 */
function _normalizarRow(nomeColecao, row) {
  if (!row) return row;
  const result = { ...row };

  // 1. Timestamps ISO → string legível
  for (const k of Object.keys(result)) {
    const v = result[k];
    if (v && typeof v === 'string' && /^\d{4}-\d{2}-\d{2}T/.test(v)) {
      result[k] = new Date(v)
        .toLocaleString('pt-BR', { timeZone: TZ, hour12: false })
        .replace(',', '');
    }
  }

  // 2. Boolean → 'SIM' / 'NAO'
  const boolFields = _BOOL_SIM_NAO[nomeColecao] || [];
  for (const field of boolFields) {
    if (typeof result[field] === 'boolean') {
      result[field] = result[field] ? 'SIM' : 'NAO';
    }
  }

  // 3. usuarios.Status: 'ativo' → 'Ativo' (formato original)
  if (nomeColecao === 'usuarios' && typeof result['Status'] === 'string') {
    const s = result['Status'].toLowerCase();
    result['Status'] = s === 'ativo' ? 'Ativo' : 'inativo';
  }

  // 4. Renomear colunas DB → nomes que o frontend espera (retrocompatibilidade)
  const map = _FIELD_MAP[nomeColecao];
  if (map?.toApp) {
    for (const [dbCol, appField] of Object.entries(map.toApp)) {
      if (dbCol in result) {
        result[appField] = result[dbCol];
        delete result[dbCol];
      }
    }
  }

  return result;
}

/**
 * Prepara dados vindos do frontend/app para inserção no Postgres:
 * - 'SIM'/'NAO' → boolean
 * - String JSON → objeto para colunas JSONB
 * - '' → null para colunas UUID (FK)
 * - usuarios.Status: normaliza para lowercase (CHECK constraint)
 */

function _prepararParaDB(nomeColecao, dados) {
  const result = { ...dados };

  // 🧹 FAXINEIRO UNIVERSAL (Limpa campos vazios)
  for (const key of Object.keys(result)) {
    if (typeof result[key] === 'string' && result[key].trim() === '') {
      result[key] = null;
    }
  }

  // 1. Normalização de Status e Origem
  if (result['Status']) result['Status'] = result['Status'].toString().toUpperCase();
  if (result['Origem']) result['Origem'] = result['Origem'].toString().toUpperCase();

  // 2. 'SIM'/'NAO' → boolean
  const boolFields = _BOOL_SIM_NAO[nomeColecao] || [];
  for (const field of boolFields) {
    if (field in result && typeof result[field] !== 'boolean') {
      const s = (result[field] || '').toString().toUpperCase().trim();
      result[field] = (s === 'SIM' || s === 'S' || s === 'TRUE' || s === '1');
    }
  }

  // 3. JSONB: parse strings JSON
  for (const field of ['Cliente_Info', 'Itens_Comprados']) {
    if (field in result && typeof result[field] === 'string') {
      if (!result[field]) {
        result[field] = field === 'Itens_Comprados' ? [] : null;
      } else {
        try { result[field] = JSON.parse(result[field]); } catch { }
      }
    }
  }

  // 4. UUID FK vazio → null
  for (const field of ['ID_Tara', 'categoria_id', 'ID_Categoria']) {
    if (field in result && (result[field] === '' || result[field] === undefined)) {
      result[field] = null;
    }
  }

  // 5. TRADUTOR DE DATA (Converte 30/04/2026 para 2026-04-30 automaticamente)
  for (const field of ['Validade', 'Data_Hora', 'Data_Cadastro']) {
    if (result[field] && typeof result[field] === 'string' && result[field].includes('/')) {
      const partes = result[field].split('/');
      if (partes.length === 3) {
        result[field] = `${partes[2]}-${partes[1]}-${partes[0]}`;
      }
    }
  }

  // 6. Renomear campos do frontend para nomes de coluna reais
  const map = _FIELD_MAP[nomeColecao];
  if (map?.toDB) {
    for (const [appField, dbCol] of Object.entries(map.toDB)) {
      if (appField in result) {
        result[dbCol] = result[appField];
        delete result[appField];
      }
    }
  }

  // 7. Remover campos desconhecidos (Whitelist)
  if (map?.dbCols) {
    const permitidos = new Set([...map.dbCols, 'loja_id']);
    for (const k of Object.keys(result)) {
      if (!permitidos.has(k)) delete result[k];
    }
  }

  return result;
}

// ══════════════════════════════════════════════════════════
// HELPERS SUPABASE — SUBSTITUEM OS HELPERS DO FIRESTORE
// ══════════════════════════════════════════════════════════

/**
 * Lê todos os registros de uma tabela filtrados por loja_id.
 * Equivalente ao lerSubcolecao() do Firestore.
 */
async function lerSubcolecao(lojaId, nomeColecao) {
  const tabela = _TABLE_MAP[nomeColecao];
  if (!tabela) throw new Error(`Coleção desconhecida: ${nomeColecao}`);

  const { data, error } = await supabase
    .from(tabela)
    .select('*')
    .eq('loja_id', lojaId);

  if (error) throw new Error(`[lerSubcolecao:${nomeColecao}] ${error.message}`);
  return (data || []).map(row => _normalizarRow(nomeColecao, row));
}

/**
 * Lê o documento de configurações da loja (relação 1:1, loja_id é a PK).
 * Equivalente ao getConfiguracoes() do Firestore.
 */
async function getConfiguracoes(lojaId) {
  const { data, error } = await supabase
    .from('configuracoes')
    .select('*')
    .eq('loja_id', lojaId)
    .maybeSingle();

  if (error) throw new Error(`[getConfiguracoes] ${error.message}`);
  return data ? _normalizarRow('configuracoes', data) : {};
}

/**
 * Salva (INSERT ou UPDATE) um registro em uma tabela do schema acaiteria.
 * - Se não tiver ID, insere e deixa o Postgres gerar o UUID.
 * - Se já tiver ID, atualiza o registro filtrando também por loja_id.
 * - Para cupons (PK composta), usa upsert.
 * Equivalente ao salvarRegistro() do Firestore.
 */
async function salvarRegistro(lojaId, nomeColecao, _idPrefixo, dadosObj, campoId) {
  const tabela = _TABLE_MAP[nomeColecao];
  if (!tabela) throw new Error(`Coleção desconhecida: ${nomeColecao}`);

  const id     = (dadosObj[campoId] || '').toString().trim();
  const ehNovo = !id;

  // Prepara payload com loja_id e conversões de tipos
  const dbData = _prepararParaDB(nomeColecao, { ...dadosObj, loja_id: lojaId });

  // Cupons têm PK composta (Codigo_Cupom, loja_id) → sempre upsert
  if (nomeColecao === 'cupons') {
    if (!dbData['Codigo_Cupom']?.toString().trim())
      return { sucesso: false, mensagem: 'Código do cupom é obrigatório.' };

    const { error } = await supabase
      .from(tabela)
      .upsert(dbData, { onConflict: 'Codigo_Cupom,loja_id' });

    if (error) throw new Error(`[salvarCupom] ${error.message}`);
    invalidarCacheCardapio(lojaId);
    return { sucesso: true, mensagem: 'Cupom salvo!', id: dbData['Codigo_Cupom'] };
  }

  if (ehNovo) {
    // Remove campo PK vazio → Postgres gera UUID automaticamente
    delete dbData[campoId];

    const { data, error } = await supabase
      .from(tabela)
      .insert(dbData)
      .select(campoId)
      .single();

    if (error) throw new Error(`[salvarRegistro:insert:${nomeColecao}] ${error.message}`);

    if (_CACHE_COLS.has(nomeColecao)) invalidarCacheCardapio(lojaId);
    return { sucesso: true, mensagem: 'Salvo!', id: data[campoId] };

  } else {
    // Remove PK e loja_id do payload de update (são usados como filtro)
    const updateData = { ...dbData };
    delete updateData[campoId];
    delete updateData['loja_id'];

    const { error } = await supabase
      .from(tabela)
      .update(updateData)
      .eq(campoId, id)
      .eq('loja_id', lojaId);

    if (error) throw new Error(`[salvarRegistro:update:${nomeColecao}] ${error.message}`);

    if (_CACHE_COLS.has(nomeColecao)) invalidarCacheCardapio(lojaId);
    return { sucesso: true, mensagem: 'Atualizado!', id };
  }
}

/**
 * Exclui um registro de uma tabela filtrando por ID e loja_id.
 * Equivalente ao deletarRegistro() do Firestore.
 */
async function deletarRegistro(lojaId, nomeColecao, idValor) {
  const tabela  = _TABLE_MAP[nomeColecao];
  const campoId = _PK_MAP[nomeColecao];
  if (!tabela || !campoId) throw new Error(`Coleção desconhecida: ${nomeColecao}`);

  // Verifica existência antes de deletar
  const { data: existente, error: errBusca } = await supabase
    .from(tabela)
    .select(campoId)
    .eq(campoId, idValor.toString())
    .eq('loja_id', lojaId)
    .maybeSingle();

  if (errBusca) throw new Error(`[deletarRegistro:check:${nomeColecao}] ${errBusca.message}`);
  if (!existente) return { sucesso: false, mensagem: 'Registro não encontrado.' };

  const { error } = await supabase
    .from(tabela)
    .delete()
    .eq(campoId, idValor.toString())
    .eq('loja_id', lojaId);

  if (error) throw new Error(`[deletarRegistro:delete:${nomeColecao}] ${error.message}`);
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
  if (!lojaIdRota) return next();

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
 * Funciona em rotas autenticadas e públicas (cardápio online, pedidos online).
 */
async function verificarStatusLoja(req, res, next) {
  const lojaId = req.params.loja_id || (req.usuario && req.usuario.loja_id);
  if (!lojaId) return next();

  try {
    const { data: loja, error } = await supabase
      .from('lojas')
      .select('status')
      .eq('id', lojaId)
      .maybeSingle();

    if (error) throw error;

    if (!loja) {
      return res.status(404).json({ sucesso: false, mensagem: 'Loja não encontrada.' });
    }

    const status = (loja.status || 'ativo').toLowerCase();
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

/** Conjunto de middlewares padrão para rotas protegidas da loja. */
const guardLoja = [autenticar, verificarLojaAcesso, verificarStatusLoja];

/** Conjunto de middlewares para rotas que exigem cargo específico. */
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

  // Busca usuario por login (case-insensitive) dentro da loja
  const { data: rows, error } = await supabase
    .from('usuarios')
    .select('*')
    .eq('loja_id', lojaId)
    .ilike('Login', loginNorm)
    .limit(1);

  if (error) throw new Error(`[validarLogin] ${error.message}`);
  if (!rows || rows.length === 0)
    return { sucesso: false, mensagem: 'Usuário ou senha inválidos.' };

  const d = rows[0];

  // Verificação de senha em texto puro (fase 1 da migração)
  if ((d.Senha || '').toString() !== senhaDigitada.toString()) {
    return { sucesso: false, mensagem: 'Usuário ou senha inválidos.' };
  }

  // Status do usuário (Postgres armazena 'ativo'/'inativo')
  if ((d.Status || '').toString().toLowerCase() !== 'ativo') {
    return { sucesso: false, mensagem: 'Acesso bloqueado. Contate a gerência.' };
  }

  // Verifica se a loja em si está ativa
  const { data: loja } = await supabase
    .from('lojas')
    .select('status')
    .eq('id', lojaId)
    .maybeSingle();

  if (!loja) return { sucesso: false, mensagem: 'Loja não encontrada.' };
  const statusLoja = (loja.status || 'ativo').toLowerCase();
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
  // Pré-filtra 7 dias no banco para evitar varredura total
  const limite7d = new Date();
  limite7d.setDate(limite7d.getDate() - 7);
  limite7d.setHours(0, 0, 0, 0);

  const { data, error } = await supabase
    .from('pedidos')
    .select('*')
    .eq('loja_id', lojaId)
    .in('Origem', ['DELIVERY', 'ONLINE'])
    .gte('Data_Hora', limite7d.toISOString());

  if (error) throw new Error(`[getDeliveryEmAndamento] ${error.message}`);

  const limiteHoje = new Date();
  limiteHoje.setHours(0, 0, 0, 0);

  return (data || [])
    .map(p => _normalizarTimestamps({ ...p }))
    .filter(p => {
      const st = (p.Status || '').toUpperCase();
      const dt = _toDate(p.Data_Hora);

      if (st === 'CANCELADO') return false;
      if (st === 'ENTREGUE') {
        // Mantém os entregues de HOJE para somar na carteira do entregador
        return dt && dt >= limiteHoje;
      }
      return true; // Já filtrado pelos 7 dias no banco
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

  // Filtra por intervalo de datas diretamente no Postgres (usa o índice)
  const { data, error } = await supabase
    .from('pedidos')
    .select('*')
    .eq('loja_id', lojaId)
    .gte('Data_Hora', dtIni.toISOString())
    .lte('Data_Hora', dtFim.toISOString())
    .order('Data_Hora', { ascending: false });

  if (error) throw new Error(`[getPedidosPorPeriodo] ${error.message}`);

  const pedidos  = [];
  let totalDia   = 0, totalDesc = 0;
  const contOrigem = { BALCAO: 0, DELIVERY: 0, ONLINE: 0 };
  const contPgto = {}, contStatus = {};

  for (const p of (data || [])) {
    // Normaliza timestamps para o formato esperado pelo frontend
    const row = _normalizarTimestamps({ ...p });
    pedidos.push(row);

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

  const limite60 = new Date();
  limite60.setDate(limite60.getDate() - 60);
  limite60.setHours(0, 0, 0, 0);

  const { data, error } = await supabase
    .from('pedidos')
    .select('*')
    .eq('loja_id', lojaId)
    .gte('Data_Hora', limite60.toISOString())
    .order('Data_Hora', { ascending: false })
    .limit(200); // Pré-limita no banco para não sobrecarregar

  if (error) throw new Error(`[buscarPedidos] ${error.message}`);

  const resultado = [];
  for (const p of (data || [])) {
    if (resultado.length >= 50) break;

    const idStr    = (p.ID_Venda || '').toString().toLowerCase();
    // JSONB vem como objeto do Postgres → serializa para busca textual
    const cliStr   = JSON.stringify(p.Cliente_Info    || {}).toLowerCase();
    const itensStr = JSON.stringify(p.Itens_Comprados || []).toLowerCase();

    if (idStr.includes(q) || cliStr.includes(q) || itensStr.includes(q)) {
      resultado.push(_normalizarTimestamps({ ...p }));
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

  // Busca por ID do pedido (parcial, últimos 30 dias)
  const limite30 = new Date();
  limite30.setDate(limite30.getDate() - 30);

  const { data: porId } = await supabase
    .from('pedidos')
    .select('*')
    .eq('loja_id', lojaId)
    .ilike('ID_Venda', `%${q}%`)
    .gte('Data_Hora', limite30.toISOString())
    .limit(5);

  // Busca por telefone no campo JSONB (somente pedidos de hoje)
  let porTelefone = [];
  if (qDigits.length >= 7) {
    const { data: telData } = await supabase
      .from('pedidos')
      .select('*')
      .eq('loja_id', lojaId)
      .gte('Data_Hora', limiteHoje.toISOString())
      .filter('Cliente_Info->>telefone', 'ilike', `%${qDigits}%`)
      .limit(5);
    porTelefone = telData || [];
  }

  // Combina e deduplica resultados
  const vistos = new Set();
  let encontrados = [...(porId || []), ...porTelefone]
    .filter(p => {
      if (vistos.has(p.ID_Venda)) return false;
      vistos.add(p.ID_Venda);
      return true;
    });

  if (encontrados.length === 0)
    return { erro: 'Pedido não encontrado de hoje. Verifique o número e tente novamente.' };

  // Ordena pelo mais recente
  encontrados.sort((a, b) => {
    const tA = a.Data_Hora ? new Date(a.Data_Hora).getTime() : 0;
    const tB = b.Data_Hora ? new Date(b.Data_Hora).getTime() : 0;
    return tB - tA;
  });

  encontrados = encontrados.slice(0, 3);

  const statusLabels = {
    NOVO: 'Recebido', PREPARANDO: 'Preparando', PRONTO: 'Pronto para retirada',
    EM_MONTAGEM: 'Em montagem', A_CAMINHO: 'Saiu para entrega',
    ENTREGUE: 'Entregue ✅', CANCELADO: 'Cancelado ❌',
  };

  const resultados = encontrados.map(encontrado => {
    let itensResumo = '';
    try {
      const arr = Array.isArray(encontrado.Itens_Comprados)
        ? encontrado.Itens_Comprados
        : (typeof encontrado.Itens_Comprados === 'string'
            ? JSON.parse(encontrado.Itens_Comprados) : []);
      itensResumo = arr.map(it =>
        it.descricao + (it.preco ? ' — R$' + parseFloat(it.preco).toFixed(2).replace('.', ',') : '')
      ).join('\n');
    } catch { /* ignora */ }

    let nomeCliente = '';
    let enderecoCli = '';
    try {
      const cli = typeof encontrado.Cliente_Info === 'object'
        ? (encontrado.Cliente_Info || {})
        : JSON.parse(encontrado.Cliente_Info || '{}');
      nomeCliente = cli.nome    || '';
      enderecoCli = cli.endereco || '';
    } catch { /* ignora */ }

    // Normaliza Data_Hora: ISO → string BR
    let dataHoraV = encontrado.Data_Hora || '';
    if (dataHoraV && /^\d{4}-\d{2}-\d{2}T/.test(dataHoraV.toString())) {
      dataHoraV = new Date(dataHoraV)
        .toLocaleString('pt-BR', { timeZone: TZ, hour12: false })
        .replace(',', '');
    }

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
    const filtros = [{ field: 'tag', key: 'loja_id', relation: '=', value: lojaId }];
    if (ignorarEntregador) {
      filtros.push({ field: 'tag', key: 'cargo', relation: '!=', value: 'Entregador' });
    }
    await fetch('https://onesignal.com/api/v1/notifications', {
      method: 'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': `Key ${ONESIGNAL_REST_API_KEY}`,
      },
      body: JSON.stringify({
        app_id:   ONESIGNAL_APP_ID,
        filters:  filtros,
        headings: { en: titulo },
        contents: { en: mensagem },
      }),
    });
  } catch (e) { console.error('[OneSignal] Erro:', e); }
}

const _COLUNAS_PEDIDO_DEFAULT = {
  ID_Venda: '', Origem: '', Data_Hora: '', Operador: '', Cliente_Info: null,
  Itens_Comprados: [], Subtotal: 0, Desconto: 0, Taxa_Entrega: 0,
  Total_Final: 0, Metodo_Pagamento: '', Status: '',
  Peso_Bruto_g: 0, ID_Tara: null, Peso_Tara_g: 0, Peso_Liquido_g: 0,
  Preco_KG: 0, Troco: 0, Entregador_Nome: '', Cancelado_Por: '',
};

async function registrarVendaPDV(lojaId, pedido) {
  const doc = Object.assign({}, _COLUNAS_PEDIDO_DEFAULT, pedido);

  // Garante timestamp ISO para o campo Data_Hora
  if (!doc.Data_Hora) doc.Data_Hora = new Date().toISOString();

  const res = await salvarRegistro(lojaId, 'pedidos', 'VND', doc, 'ID_Venda');

  if (res.sucesso && (pedido.Origem === 'DELIVERY' || pedido.Origem === 'ONLINE')) {
    let isRetirada = false;
    try {
      const cli = typeof pedido.Cliente_Info === 'string'
        ? JSON.parse(pedido.Cliente_Info)
        : (pedido.Cliente_Info || {});
      if (cli.endereco === 'Retirada na loja') isRetirada = true;
    } catch { /* ignora */ }

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
        Nome:     dados.nomeCliente.trim(),
        CPF:      cpfLimpo || '',
        Telefone: dados.telefone ? dados.telefone.replace(/\D/g, '') : '',
      });
    }
  }

  const pedido = {
    ID_Venda: '', Origem: 'BALCAO', Data_Hora: new Date().toISOString(),
    Operador: dados.operador || 'CAIXA',
    // JSONB: passa objeto diretamente (sem JSON.stringify)
    Cliente_Info:    { nome: dados.nomeCliente || '', cpf: dados.cpfCliente || '', telefone: dados.telefone || '' },
    Itens_Comprados: todosItens,
    Subtotal: subtotal, Desconto: desconto, Taxa_Entrega: 0, Total_Final: total,
    Metodo_Pagamento: dados.pagamento || '', Status: 'ENTREGUE',
    Peso_Bruto_g: pesoBruto, ID_Tara: dados.idTara || null, Peso_Tara_g: pesoTara,
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
    ID_Venda: '', Origem: 'DELIVERY', Data_Hora: new Date().toISOString(),
    Operador: dados.operador || 'CAIXA',
    Cliente_Info:    { nome: dados.nomeCliente || '', cpf: dados.cpfCliente || '', telefone: dados.telefone || '', endereco: dados.endereco || '' },
    Itens_Comprados: itens,
    Subtotal: subtotal, Desconto: desconto, Taxa_Entrega: taxaEnt, Total_Final: total,
    Metodo_Pagamento: dados.pagamento || '', Status: 'NOVO',
    Peso_Bruto_g: 0, ID_Tara: null, Peso_Tara_g: 0, Peso_Liquido_g: 0,
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
    ID_Venda: '', Origem: 'ONLINE', Data_Hora: new Date().toISOString(), Operador: 'APP',
    Cliente_Info:    { nome: dadosPedido.nomeCliente || '', cpf: '', telefone: dadosPedido.telefone || '', endereco: dadosPedido.endereco || '' },
    Itens_Comprados: dadosPedido.itens,
    Subtotal: subtotalReal, Desconto: desconto, Taxa_Entrega: taxaEnt, Total_Final: total,
    Metodo_Pagamento: dadosPedido.pagamento || '', Status: 'NOVO',
    Peso_Bruto_g: 0, ID_Tara: null, Peso_Tara_g: 0, Peso_Liquido_g: 0,
    Preco_KG: 0, Troco: 0, Entregador_Nome: '', Cancelado_Por: '',
  };

  return registrarVendaPDV(lojaId, pedido);
}

async function atualizarStatusPedido(lojaId, idVenda, novoStatus) {
  const statusValidos = ['NOVO','PREPARANDO','PRONTO','ENTREGUE','CANCELADO','EM_MONTAGEM','A_CAMINHO','AGUARDANDO_PIX'];
  if (!statusValidos.includes(novoStatus))
    return { sucesso: false, mensagem: `Status inválido: ${novoStatus}` };

  // Verifica existência (e garante loja_id correto)
  const { data: existente, error: errBusca } = await supabase
    .from('pedidos')
    .select('ID_Venda')
    .eq('ID_Venda', idVenda.toString())
    .eq('loja_id', lojaId)
    .maybeSingle();

  if (errBusca) throw new Error(`[atualizarStatusPedido:check] ${errBusca.message}`);
  if (!existente) return { sucesso: false, mensagem: `Pedido "${idVenda}" não encontrado.` };

  const { error } = await supabase
    .from('pedidos')
    .update({ Status: novoStatus })
    .eq('ID_Venda', idVenda.toString())
    .eq('loja_id', lojaId);

  if (error) throw new Error(`[atualizarStatusPedido] ${error.message}`);
  return { sucesso: true };
}

async function atualizarStatusEntrega(lojaId, idVenda, novoStatus) {
  const permitidos = ['EM_MONTAGEM', 'A_CAMINHO', 'ENTREGUE'];
  if (!permitidos.includes(novoStatus))
    return { sucesso: false, mensagem: `Status inválido para entrega: ${novoStatus}` };
  return atualizarStatusPedido(lojaId, idVenda, novoStatus);
}

/**
 * Cancela um pedido mediante autenticação de supervisor ou acima.
 * Nota: substituído db.runTransaction() por leitura + escrita sequencial
 * (aceitável para este caso de uso — cancelamentos não são concorrentes).
 */
async function cancelarPedidoAutorizado(lojaId, idVenda, loginAuth, senhaAuth) {
  const auth = await validarSupervisorOuAcima(lojaId, loginAuth, senhaAuth);
  if (!auth.autorizado) return { sucesso: false, mensagem: auth.mensagem };

  const { data: pedido, error: errBusca } = await supabase
    .from('pedidos')
    .select('Status, Origem')
    .eq('ID_Venda', idVenda.toString())
    .eq('loja_id', lojaId)
    .maybeSingle();

  if (errBusca) throw new Error(`[cancelarPedido:check] ${errBusca.message}`);
  if (!pedido) throw new Error(`Pedido "${idVenda}" não encontrado.`);

  const statusAtual = (pedido.Status || '').toUpperCase();
  const origemAtual = (pedido.Origem || '').toUpperCase();

  if (statusAtual === 'CANCELADO') throw new Error('Pedido já cancelado.');
  if (statusAtual === 'ENTREGUE' && origemAtual !== 'BALCAO') throw new Error('Pedido já entregue.');

  const { error } = await supabase
    .from('pedidos')
    .update({
      Status:        'CANCELADO',
      Cancelado_Por: `${auth.nome} (${auth.cargo}) — ${_agora()}`,
    })
    .eq('ID_Venda', idVenda.toString())
    .eq('loja_id', lojaId);

  if (error) throw new Error(`[cancelarPedido:update] ${error.message}`);
  return { sucesso: true, mensagem: `Pedido cancelado por ${auth.nome} (${auth.cargo}).` };
}

/**
 * Atribui entregador a um pedido delivery.
 * Nota: substituído db.runTransaction() por leitura + escrita sequencial.
 */
async function pegarPedidoDelivery(lojaId, idVenda, nomeEntregador) {
  if (!idVenda || !nomeEntregador?.toString().trim())
    return { sucesso: false, mensagem: 'ID do pedido e nome do entregador são obrigatórios.' };

  const nomeTrimmed = nomeEntregador.toString().trim();

  const { data: pedido, error: errBusca } = await supabase
    .from('pedidos')
    .select('Status, Entregador_Nome')
    .eq('ID_Venda', idVenda.toString())
    .eq('loja_id', lojaId)
    .maybeSingle();

  if (errBusca) throw new Error(`[pegarDelivery:check] ${errBusca.message}`);
  if (!pedido) throw new Error('Pedido não encontrado.');

  const status      = (pedido.Status          || '').toUpperCase();
  const entregAtual = (pedido.Entregador_Nome || '').toString().trim();

  if (status === 'ENTREGUE')  throw new Error('Pedido já entregue.');
  if (status === 'CANCELADO') throw new Error('Pedido cancelado.');

  if (entregAtual && entregAtual.toLowerCase() !== nomeTrimmed.toLowerCase())
    return { sucesso: false, mensagem: `Este pedido já foi pego por ${entregAtual}.`, entregadorAtual: entregAtual };

  const { error } = await supabase
    .from('pedidos')
    .update({ Entregador_Nome: nomeTrimmed, Status: 'EM_MONTAGEM' })
    .eq('ID_Venda', idVenda.toString())
    .eq('loja_id', lojaId);

  if (error) throw new Error(`[pegarDelivery:update] ${error.message}`);
  return { sucesso: true, mensagem: `Pedido atribuído a ${nomeTrimmed}.` };
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
  // Data_Cadastro em ISO para armazenar como TIMESTAMPTZ
  if (!dados.Data_Cadastro) dados.Data_Cadastro = new Date().toISOString();
  return salvarRegistro(lojaId, 'clientes', 'CLI', dados, 'ID_Cliente');
}

async function buscarClientePorCPF(lojaId, cpf) {
  const cpfLimpo = cpf.toString().replace(/\D/g, '');
  if (cpfLimpo.length !== 11) return null;

  const { data, error } = await supabase
    .from('clientes')
    .select('*')
    .eq('loja_id', lojaId)
    .eq('CPF', cpfLimpo)
    .limit(1);

  if (error) throw new Error(`[buscarClientePorCPF] ${error.message}`);
  return data?.length ? _normalizarRow('clientes', data[0]) : null;
}

async function buscarClientePorTelefone(lojaId, telefone) {
  const telLimpo = telefone.toString().replace(/\D/g, '');

  const { data, error } = await supabase
    .from('clientes')
    .select('*')
    .eq('loja_id', lojaId)
    .eq('Telefone', telLimpo)
    .limit(1);

  if (error) throw new Error(`[buscarClientePorTelefone] ${error.message}`);
  return data?.length ? _normalizarRow('clientes', data[0]) : null;
}

async function salvarConfiguracoesLote(lojaId, configObj) {
  // Normaliza PIX_Modo: 'AUTO'/'AUTOMATICO'/'auto' → sempre 'AUTO' ou 'MANUAL'
  if (configObj['PIX_Modo'] !== undefined) {
    const m = (configObj['PIX_Modo'] || 'MANUAL').toString().toUpperCase().trim();
    configObj['PIX_Modo'] = (m === 'AUTO' || m === 'AUTOMATICO' || m === 'AUTOMÁTICO')
      ? 'AUTO' : 'MANUAL';
  }

  // configuracoes usa loja_id como PK → upsert direto
  const dbData = _prepararParaDB('configuracoes', { ...configObj, loja_id: lojaId });

  // Remove campos de controle que não devem ser sobrescritos manualmente
  delete dbData['atualizado_em'];

  const { error } = await supabase
    .from('configuracoes')
    .upsert(dbData, { onConflict: 'loja_id' });

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

  const { data, error } = await supabase
    .from('cupons')
    .select('*')
    .eq('loja_id', lojaId)
    .eq('Codigo_Cupom', codigoUpper)
    .limit(1);

  if (error) throw new Error(`[validarCupom] ${error.message}`);
  if (!data?.length) return { valido: false, mensagem: 'Cupom não encontrado.' };

  const d = data[0];

  // Ativo: BOOLEAN no Postgres → compatibiliza com lógica legada
  const ativoRaw = d.Ativo;
  const ativo    = typeof ativoRaw === 'boolean'
    ? ativoRaw
    : (ativoRaw || '').toString().toUpperCase() === 'SIM';

  if (!ativo) return { valido: false, mensagem: 'Cupom inativo ou esgotado.' };

  // Validade: TIMESTAMPTZ (string ISO) do Postgres
  if (d.Validade) {
    const validade = new Date(d.Validade); // ISO → Date funciona diretamente
    if (!isNaN(validade.getTime())) {
      validade.setHours(23, 59, 59, 999);
      if (validade < new Date()) return { valido: false, mensagem: 'Cupom expirado.' };
    }
  }

  // Usar_Cardapio: BOOLEAN no Postgres
  const usarCardapioRaw = d.Usar_Cardapio;
  const usarCardapio    = typeof usarCardapioRaw === 'boolean'
    ? usarCardapioRaw
    : (usarCardapioRaw || '').toString().toUpperCase() === 'SIM';

  const tipo       = (d.Tipo_Desconto  || 'VALOR').toUpperCase();
  const valorBruto = parseFloat(d.Valor_Desconto) || 0;

  const msgDesc = tipo === 'PERCENTUAL'
    ? `Desconto de ${valorBruto.toFixed(0)}% aplicado 🎉`
    : `Desconto de R$ ${valorBruto.toFixed(2).replace('.', ',')} aplicado 🎉`;

  return {
    valido:       true,
    codigo:       (d.Codigo_Cupom || '').toString(),
    tipo,
    valor:        valorBruto,
    desconto:     tipo === 'VALOR' ? valorBruto : 0,
    usarCardapio,
    mensagem:     msgDesc,
  };
}


// ══════════════════════════════════════════════════════════
// MERCADO PAGO — PIX AUTOMÁTICO
// (Código 100% intacto — apenas lê configurações via getConfiguracoes)
// ══════════════════════════════════════════════════════════

async function gerarPixMP(lojaId, total, idVenda, emailPagador) {
  const config = await getConfiguracoes(lojaId);
  const token  = (config['MP_AccessToken'] || '').toString().trim();
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

  // Aceita AUTO, AUTOMATICO ou AUTOMÁTICO
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

  // 1. Tenta gerar o PIX no Mercado Pago com um ID provisório
  const idVendaTemp = 'WEB-' + Date.now().toString().substring(5);
  const pix = await gerarPixMP(
    lojaId, total, idVendaTemp,
    dadosPedido.telefone ? `${dadosPedido.telefone.replace(/\D/g,'')}@mp.br` : 'contato@acaiteria.com.br'
  );

  // 2. Se der erro no PIX, para aqui sem salvar no banco
  if (!pix.sucesso) {
    return { sucesso: false, mensagem: `PIX não gerado: ${pix.mensagem}. Tente outra forma de pagamento.` };
  }

  // 3. PIX gerado com sucesso → cria o pedido no banco
  const pedido = {
    ID_Venda: '', Origem: 'ONLINE', Data_Hora: new Date().toISOString(), Operador: 'APP',
    Cliente_Info:    { nome: dadosPedido.nomeCliente || '', cpf: '', telefone: dadosPedido.telefone || '', endereco: dadosPedido.endereco || '' },
    Itens_Comprados: dadosPedido.itens,
    Subtotal: subtotalReal, Desconto: desconto, Taxa_Entrega: taxaEnt, Total_Final: total,
    Metodo_Pagamento: 'PIX', Status: 'AGUARDANDO_PIX',
    Peso_Bruto_g: 0, ID_Tara: null, Peso_Tara_g: 0, Peso_Liquido_g: 0,
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
    const tokenRecebido = req.headers['asaas-access-token'] || req.headers['x-access-token'] || '';
    if (ASAAS_WEBHOOK_TOKEN && tokenRecebido !== ASAAS_WEBHOOK_TOKEN) {
      console.warn('[Webhook Asaas] Token inválido recebido:', tokenRecebido);
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

    // Busca loja pelo asaas_customer_id
    const { data: lojas, error: errBusca } = await supabase
      .from('lojas')
      .select('id')
      .eq('asaas_customer_id', customerId)
      .limit(1);

    if (errBusca) throw errBusca;

    if (!lojas?.length) {
      console.warn(`[Webhook Asaas] Nenhuma loja encontrada para customer: ${customerId}`);
      return res.status(404).json({ sucesso: false, mensagem: 'Loja não encontrada para este customer.' });
    }

    const lojaId    = lojas[0].id;
    const novoStatus = event === 'PAYMENT_OVERDUE' ? 'bloqueado' : 'ativo';

    const { error: errUpdate } = await supabase
      .from('lojas')
      .update({ status: novoStatus, ultimo_evento_asaas: event })
      .eq('id', lojaId);

    if (errUpdate) throw errUpdate;

    // Invalida cache se bloqueou
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
  console.log(`✅  API SaaS Açaíteria rodando na porta ${PORT} — Supabase (acaiteria schema)`);

  console.log(`
  ╔══════════════════════════════════════════════════════════════════════╗
  ║  ⚠️  CHECKLIST OBRIGATÓRIO — SUPABASE                               ║
  ╠══════════════════════════════════════════════════════════════════════╣
  ║                                                                      ║
  ║  SUPABASE — CONFIGURAÇÃO:                                            ║
  ║  [1] Em Settings → API → "Extra Search Path":                       ║
  ║      Adicione o schema: acaiteria                                    ║
  ║  [2] Rodar no SQL Editor (correção do campo Status_Loja):           ║
  ║      ALTER TABLE acaiteria.configuracoes DROP COLUMN "Status_Loja"; ║
  ║      ALTER TABLE acaiteria.configuracoes                             ║
  ║        ADD COLUMN "Status_Loja" TEXT NOT NULL DEFAULT 'AUTOMATICO'; ║
  ║  [3] Criar o primeiro registro em acaiteria.lojas:                  ║
  ║      INSERT INTO acaiteria.lojas (nome_loja, status, plano)         ║
  ║      VALUES ('Nome da Loja', 'ativo', 'pro') RETURNING id;          ║
  ║  [4] Salvar o UUID retornado como loja_id no .env do frontend.      ║
  ║                                                                      ║
  ║  VARIÁVEIS DE AMBIENTE (.env):                                       ║
  ║  [5] SUPABASE_URL=https://<projeto>.supabase.co                     ║
  ║  [6] SUPABASE_SERVICE_ROLE_KEY=<sua-service-role-key>               ║
  ║      (NÃO usar a anon key — service_role bypassa RLS)               ║
  ║  [7] JWT_SECRET=<mínimo 64 chars aleatórios>                        ║
  ║                                                                      ║
  ║  ASAAS:                                                              ║
  ║  [8] Em Configurações → Integrações → Webhooks, cadastrar:          ║
  ║      https://SEU_BACKEND/api/webhooks/asaas                         ║
  ║  [9] Salvar o asaas_customer_id de cada loja na tabela lojas.       ║
  ║                                                                      ║
  ║  SEGURANÇA:                                                          ║
  ║  [10] Nunca commitar o .env no Git. Adicione ao .gitignore.         ║
  ║  [11] A SUPABASE_SERVICE_ROLE_KEY dá acesso total — proteja-a.      ║
  ║                                                                      ║
  ╚══════════════════════════════════════════════════════════════════════╝
  `);
});