const PORTFOLIO_ENDPOINT = "/api/dashboard/portfolio";
const RADAR_ENDPOINT = "/api/dashboard/radar";
const API_FALLBACK_ORIGIN = "https://genesisbot-production.up.railway.app";
const GENESIS_LOGO_SRC = "./assets/genesis-logo-green.png?v=genesis-g-clean";

function initialChatMessage() {
  return {
    id: `welcome-${Date.now()}`,
    role: "assistant",
    text: "Hola. ¿Qué quieres revisar hoy?",
  };
}

const appState = {
  activeScreen: "genesis",
  trackingItems: [],
  paperPositions: [],
  portfolioTotals: {
    totalValue: 0,
    dailyPnl: null,
    dailyPnlPct: null,
    totalPnl: null,
    totalPnlPct: null,
    positionCount: 0,
    watchlistCount: 0,
  },
  selectedAsset: "",
  selectedAssetPreviousScreen: "genesis",
  marketSearchResults: {
    tracking: [],
    portfolio: [],
  },
  lastUpdated: "",
  loading: false,
  error: "",
  allItems: [],
  portfolioSnapshot: null,
  radarSnapshot: null,
  trackingSearchQuery: "",
  portfolioSearchQuery: "",
  trackingFilter: "all",
  whalesSnapshot: null,
  alertsSnapshot: null,
  alertSubtab: "alerts",
  newsSnapshot: null,
  newsLoading: false,
  newsFilter: "important",
  selectedNewsId: "",
  newsItemsById: {},
  selectedAlertId: "",
  alertItemsById: {},
  selectedWhaleId: "",
  whaleItemsById: {},
  marketPulse: {},
  marketPulseLoadedAt: 0,
  marketPulseLoading: false,
  opportunityQuotes: {},
  opportunityQuotesLoadedAt: 0,
  opportunityQuotesLoading: false,
  searchOpen: {
    tracking: false,
    portfolio: false,
    news: false,
    alerts: false,
    whales: false,
  },
  chartCache: {},
  assetChartRanges: {},
  refreshTimer: null,
  marketRefreshTimer: null,
  refreshInFlight: false,
  refreshPromise: null,
  chatHistoryOpen: false,
  chatMessages: [initialChatMessage()],
  chatConversations: [],
  currentConversationId: `chat-${Date.now()}`,
  voiceMode: false,
  voiceListening: false,
  voiceSpeaking: false,
  voiceStatus: "",
  voiceRecognition: null,
};

const REFRESH_MS = 15000;
const MARKET_FEED_REFRESH_MS = 60000;
const MARKET_PULSE_TICKERS = ["SPY", "QQQ", "BTC-USD", "NVDA", "BZ=F"];
const OPPORTUNITY_TICKERS = ["NVDA", "MSFT", "NFLX", "META", "TSLA", "SPY", "QQQ", "BTC-USD"];
const CHART_RANGES = ["1D", "1W", "1M", "1Y", "5Y", "MAX"];
const CHART_CACHE_TTL_MS = 2 * 60 * 1000;
const CHART_FAILURE_RETRY_MS = 8000;
const MONEY_COLORS = ["#7be0ad", "#91a7ff", "#efbd6f", "#ec7f77", "#7fd9df", "#d7c27f", "#b7c5d9"];
const NEWS_FALLBACK_IMAGES = {
  market: "https://images.unsplash.com/photo-1611974789855-9c2a0a7236a3?auto=format&fit=crop&w=640&q=80",
  macro: "https://images.unsplash.com/photo-1526304640581-d334cdbbf45e?auto=format&fit=crop&w=640&q=80",
  crypto: "https://images.unsplash.com/photo-1518546305927-5a555bb7020d?auto=format&fit=crop&w=640&q=80",
  commodity: "https://images.unsplash.com/photo-1473341304170-971dccb5ac1e?auto=format&fit=crop&w=640&q=80",
  geopolitics: "https://images.unsplash.com/photo-1529107386315-e1a2ed48a620?auto=format&fit=crop&w=640&q=80",
  earnings: "https://images.unsplash.com/photo-1554224155-6726b3ff858f?auto=format&fit=crop&w=640&q=80",
  tech: "https://images.unsplash.com/photo-1518770660439-4636190af475?auto=format&fit=crop&w=640&q=80",
  gold: "https://images.unsplash.com/photo-1610375461246-83df859d849d?auto=format&fit=crop&w=640&q=80",
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function numberOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function positiveOrNull(value) {
  const numeric = numberOrNull(value);
  return numeric !== null && numeric > 0 ? numeric : null;
}

function normalizeTicker(value) {
  return String(value || "").trim().toUpperCase();
}

function simpleHash(value) {
  let hash = 5381;
  const text = String(value || "");
  for (let index = 0; index < text.length; index += 1) {
    hash = ((hash << 5) + hash) + text.charCodeAt(index);
    hash >>>= 0;
  }
  return hash.toString(36);
}

function nextMessageId() {
  return `msg-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function itemTicker(item) {
  return normalizeTicker(item?.ticker || item?.symbol);
}

const FRIENDLY_ASSET_NAMES = {
  "BZ=F": { displayName: "Brent Crude Oil", subtitle: "Brent Front Month" },
  BNO: { displayName: "United States Brent Oil Fund", subtitle: "ETF petrolero" },
  "BTC-USD": { displayName: "Bitcoin", subtitle: "Cripto" },
  "ETH-USD": { displayName: "Ethereum", subtitle: "Cripto" },
  "SOL-USD": { displayName: "Solana", subtitle: "Cripto" },
  IAU: { displayName: "iShares Gold Trust", subtitle: "Oro ETF" },
  SLV: { displayName: "iShares Silver Trust", subtitle: "Plata ETF" },
  IXC: { displayName: "iShares Global Energy ETF", subtitle: "Energia ETF" },
  SPY: { displayName: "S&P 500 ETF", subtitle: "Indice EEUU" },
  QQQ: { displayName: "Nasdaq 100 ETF", subtitle: "Indice tecnologia" },
  DIA: { displayName: "Dow Jones ETF", subtitle: "Indice EEUU" },
};

function getAssetDisplayName(itemOrTicker) {
  const ticker = typeof itemOrTicker === "string" ? normalizeTicker(itemOrTicker) : itemTicker(itemOrTicker);
  const raw = typeof itemOrTicker === "string"
    ? ""
    : String(itemOrTicker?.name || itemOrTicker?.display_name || itemOrTicker?.companyName || "").trim();
  const friendly = FRIENDLY_ASSET_NAMES[ticker];
  if (friendly) {
    const displayName = friendly.displayName || String(friendly);
    return {
      displayName,
      subtitle: friendly.subtitle || (ticker ? `${ticker}` : ""),
      ticker,
    };
  }
  const displayName = raw || ticker;
  return {
    displayName,
    subtitle: ticker && displayName !== ticker ? ticker : "",
    ticker,
  };
}

function assetDisplayName(itemOrTicker) {
  return getAssetDisplayName(itemOrTicker).displayName;
}

function displayAssetLabel(itemOrTicker) {
  const display = getAssetDisplayName(itemOrTicker);
  return display.displayName || display.ticker || "";
}

function humanizeInternalTickerText(value) {
  let text = String(value || "");
  Object.entries(FRIENDLY_ASSET_NAMES).forEach(([ticker, friendly]) => {
    const label = friendly?.displayName || ticker;
    const escapedTicker = ticker.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    text = text.replace(new RegExp(`\\b${escapedTicker}\\b`, "gi"), label);
  });
  return text;
}

function assetSubtitle(itemOrTicker) {
  return getAssetDisplayName(itemOrTicker).subtitle;
}

function itemUnits(item) {
  return positiveOrNull(item?.units);
}

function itemMode(item) {
  return String(item?.mode || item?.position_mode || "").trim().toLowerCase();
}

function itemPrice(item) {
  return positiveOrNull(item?.current_price)
    ?? positiveOrNull(item?.price)
    ?? positiveOrNull(item?.reference_price)
    ?? positiveOrNull(item?.entry_price);
}

function itemValue(item) {
  const explicit = numberOrNull(item?.market_value ?? item?.current_value);
  if (explicit !== null && explicit > 0) return explicit;
  const units = itemUnits(item);
  const price = itemPrice(item);
  return units !== null && price !== null ? units * price : null;
}

function itemDailyPct(item) {
  const explicit = numberOrNull(item?.daily_change_pct ?? item?.change_pct ?? item?.percent_change ?? item?.changesPercentage);
  const price = itemPrice(item);
  const previous = positiveOrNull(item?.previous_close ?? item?.previousClose ?? item?.prev_close);
  if (explicit !== null && explicit !== 0) return explicit;
  if (price !== null && previous !== null && previous > 0) return ((price - previous) / previous) * 100;
  if (explicit !== null) return explicit;
  return null;
}

function itemDailyUsd(item) {
  const explicit = numberOrNull(item?.daily_change ?? item?.change ?? item?.change_usd);
  const price = itemPrice(item);
  const previous = positiveOrNull(item?.previous_close ?? item?.previousClose ?? item?.prev_close);
  if (explicit !== null && explicit !== 0) return explicit;
  if (price !== null && previous !== null) return price - previous;
  if (explicit !== null) return explicit;
  return null;
}

function itemInWatchlist(item) {
  if (!itemTicker(item)) return false;
  if (item?.removed_watchlist === true) return false;
  if (item?.watchlist === true) return true;
  if (item?.watchlist === false) return false;
  return !itemIsPaper(item);
}

function itemIsPaper(item) {
  return Boolean(itemTicker(item)) && (itemMode(item) === "paper" || itemUnits(item) !== null);
}

function positionPnl(item) {
  const explicit = numberOrNull(item?.unrealized_pnl ?? item?.pnl_usd);
  if (explicit !== null) return explicit;
  const value = itemValue(item);
  const cost = positiveOrNull(item?.cost_basis) ?? positiveOrNull(item?.amount_usd);
  return value !== null && cost !== null ? value - cost : null;
}

function money(value, empty = "Sin precio") {
  const numeric = numberOrNull(value);
  if (numeric === null) return empty;
  return `$${numeric.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function signedMoney(value, empty = "Sin dato") {
  const numeric = numberOrNull(value);
  if (numeric === null) return empty;
  const sign = numeric >= 0 ? "+" : "-";
  return `${sign}$${Math.abs(numeric).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function percent(value, empty = "Sin dato") {
  const numeric = numberOrNull(value);
  if (numeric === null) return empty;
  return `${numeric >= 0 ? "+" : ""}${numeric.toFixed(2)}%`;
}

function positiveClass(value) {
  const numeric = numberOrNull(value);
  if (numeric === null || numeric === 0) return "flat";
  return numeric > 0 ? "up" : "down";
}

function marketTone(value) {
  const numeric = numberOrNull(value);
  if (numeric === null || numeric === 0) return "neutral";
  return numeric > 0 ? "positive" : "negative";
}

function marketClass(value) {
  return positiveClass(value);
}

function formatSignedMoney(value, empty = "Sin dato") {
  return signedMoney(value, empty);
}

function formatSignedPercent(value, empty = "Sin dato") {
  return percent(value, empty);
}

function formatMarketNumber(value, empty = "Sin precio") {
  return money(value, empty);
}

function formatChange(value, empty = "Sin cambio") {
  return formatSignedMoney(value, empty);
}

function formatPercent(value, empty = "Sin dato") {
  return formatSignedPercent(value, empty);
}

function compactPercent(value, empty = "Sin peso") {
  const numeric = numberOrNull(value);
  if (numeric === null) return empty;
  return `${numeric.toFixed(numeric >= 10 ? 0 : 1)}%`;
}

function formatDate(value) {
  if (!value) return "Sin fecha";
  const raw = String(value).trim();
  const numeric = Number(raw);
  const parsed = Number.isFinite(numeric) && /^\d+(\.\d+)?$/.test(raw)
    ? new Date(numeric > 10000000000 ? numeric : numeric * 1000)
    : new Date(raw);
  if (Number.isNaN(parsed.getTime())) return raw;
  return parsed.toLocaleString("es-MX", {
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function cleanCopy(value) {
  return String(value ?? "")
    .replace(/\bsenales\b/gi, "señales")
    .replace(/\bsenal\b/gi, "señal")
    .replace(/\bdireccion\b/gi, "dirección")
    .replace(/\bdistribucion\b/gi, "distribución")
    .replace(/\bacumulacion\b/gi, "acumulación")
    .replace(/\bconfirmacion\b/gi, "confirmación")
    .replace(/\bpresion\b/gi, "presión")
    .replace(/\batencion\b/gi, "atención")
    .replace(/\bsesion\b/gi, "sesión")
    .replace(/\binstitucion\b/gi, "institución")
    .replace(/\bperdida\b/gi, "pérdida")
    .replace(/\bminimo\b/gi, "mínimo")
    .replace(/\bintradia\b/gi, "intradía")
    .replace(/\bcaida\b/gi, "caída")
    .replace(/\brecuperacion\b/gi, "recuperación")
    .replace(/\bautomatica\b/gi, "automática")
    .replace(/\bautomatico\b/gi, "automático")
    .replace(/\bcontinua\b/gi, "continúa")
    .replace(/\btecnicas\b/gi, "técnicas")
    .replace(/\bhistorico\b/gi, "histórico")
    .replace(/\baun\b/gi, "aún")
    .replace(/\bFMP_API_KEY\b/gi, "fuente privada")
    .replace(/\bapikey\b/gi, "credencial")
    .replace(/\bendpoint\b/gi, "consulta")
    .replace(/\bruntime\b/gi, "sistema")
    .replace(/\bsnapshot\b/gi, "lectura")
    .replace(/\bdegraded\b/gi, "datos parciales")
    .replace(/\bunavailable\b/gi, "sin datos")
    .replace(/\binsufficient_confirmation\b/gi, "no concluyente")
    .replace(/probability\s+(ready|disabled)/gi, "sin confirmacion suficiente")
    .replace(/causalidad\s+probabilidad/gi, "lectura causal no confirmada")
    .replace(/money\s*flow\s*ready/gi, "flujo en vigilancia")
    .replace(/detection\s+ready/gi, "deteccion en vigilancia")
    .replace(/\bTelegram\b/gi, "Genesis")
    .replace(/\blegacy\b/gi, "local")
    .replace(/\bdatos_directos\b/gi, "datos directos")
    .replace(/\bfmp_opportunity_scan\b/gi, "FMP oportunidad");
}

function stripMarkdownCopy(value) {
  return cleanCopy(value)
    .replace(/```[\s\S]*?```/g, (match) => match.replace(/```/g, ""))
    .replace(/^\s{0,3}#{1,6}\s+/gm, "")
    .replace(/\*\*([^*]+)\*\*/g, "$1")
    .replace(/\*([^*]+)\*/g, "$1")
    .replace(/`([^`]+)`/g, "$1")
    .replace(/^\s*[-*]\s+/gm, "")
    .replace(/^\s*\d+\.\s+/gm, "")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function cleanSentenceList(value, limit = 4) {
  const copy = stripMarkdownCopy(value);
  if (!copy) return [];
  const lines = copy
    .split(/\n+/)
    .map((line) => line.replace(/^\s*[:;|,-]+\s*/, "").trim())
    .filter(Boolean);
  if (lines.length > 1) return lines.slice(0, limit);
  return copy
    .split(/[.!?]\s+/)
    .map((line) => line.trim())
    .filter(Boolean)
    .slice(0, limit);
}

function quotePrice(quote) {
  return numberOrNull(quote?.current_price ?? quote?.price ?? quote?.quote?.price);
}

function quoteChange(quote) {
  return numberOrNull(quote?.daily_change ?? quote?.change ?? quote?.quote?.change);
}

function quoteChangePct(quote) {
  return numberOrNull(quote?.daily_change_pct ?? quote?.changesPercentage ?? quote?.change_pct ?? quote?.quote?.changesPercentage);
}

function quoteSourceLabel(quote) {
  if (!quote) return "Sin fuente confirmada";
  if (quotePrice(quote) === null) return "Sin fuente confirmada";
  return cleanCopy(quote.source_label || quote.source || "FMP / fuente verificada");
}

function biasFromMove(value) {
  const numeric = numberOrNull(value);
  if (numeric === null || numeric === 0) return "neutral";
  return numeric > 0 ? "bullish" : "bearish";
}

function biasLabel(value) {
  if (value === "bullish") return "Alcista";
  if (value === "bearish") return "Bajista";
  return "Neutral";
}

function confidenceFromQuote(quote) {
  const price = quotePrice(quote);
  const source = String(quote?.source || quote?.source_label || "").toLowerCase();
  if (price === null) return 0.35;
  if (source.includes("fmp") || quote?.is_live) return 0.78;
  return 0.58;
}

function compactNumber(value, empty = "Sin dato") {
  const numeric = numberOrNull(value);
  if (numeric === null) return empty;
  return Math.abs(numeric) >= 1000
    ? numeric.toLocaleString("en-US", { maximumFractionDigits: 0 })
    : numeric.toLocaleString("en-US", { maximumFractionDigits: 2 });
}

function compactSuffixNumber(value, empty = "Sin dato", decimals = 1) {
  const numeric = numberOrNull(value);
  if (numeric === null) return empty;
  const sign = numeric < 0 ? "-" : "";
  const abs = Math.abs(numeric);
  if (abs >= 1_000_000_000_000) return `${sign}${(abs / 1_000_000_000_000).toFixed(decimals)}T`;
  if (abs >= 1_000_000_000) return `${sign}${(abs / 1_000_000_000).toFixed(decimals)}B`;
  if (abs >= 1_000_000) return `${sign}${(abs / 1_000_000).toFixed(decimals)}M`;
  if (abs >= 1_000) return `${sign}${(abs / 1_000).toFixed(decimals)}K`;
  return `${sign}${abs.toLocaleString("en-US", { maximumFractionDigits: abs < 10 ? 2 : 0 })}`;
}

function formatMoneyCompact(value, empty = "Pendiente") {
  const numeric = numberOrNull(value);
  if (numeric === null) return empty;
  return `${numeric < 0 ? "-" : ""}$${compactSuffixNumber(Math.abs(numeric), empty)}`;
}

function formatVolumeCompact(value, empty = "Volumen pendiente") {
  return compactSuffixNumber(value, empty);
}

function formatPercentSigned(value, empty = "0.00%") {
  return formatPercent(value, empty);
}

function confidenceTone(value) {
  const text = String(value ?? "").toLowerCase();
  const numeric = numberOrNull(value);
  if (numeric !== null) {
    if (numeric >= 0.7 || numeric >= 70) return "up";
    if (numeric <= 0.35 || numeric <= 35) return "down";
    return "flat";
  }
  if (text.includes("alta") || text.includes("high") || text.includes("fuerte")) return "up";
  if (text.includes("baja") || text.includes("low") || text.includes("debil")) return "down";
  return "flat";
}

function assetMoveValues(item = {}) {
  return [
    itemDailyPct(item),
    itemDailyUsd(item),
    numberOrNull(item?.relative_volume ?? item?.relativeVolume),
    numberOrNull(item?.volume),
  ].filter((value) => value !== null);
}

function renderSparkline(values = [], tone = "flat") {
  return `<span class="render-sparkline ${escapeHtml(tone)}" aria-hidden="true">${miniSeriesBars(values, 24)}</span>`;
}

function renderMiniBars(values = [], maxHeight = 30) {
  return `<span class="render-mini-bars" aria-hidden="true">${miniSeriesBars(values, maxHeight)}</span>`;
}

function renderMetricCard(label, value, tone = "flat") {
  return `
    <span class="metric-card ${escapeHtml(tone)}">
      <small>${escapeHtml(label)}</small>
      <strong>${escapeHtml(value)}</strong>
    </span>
  `;
}

function renderImpactBadge(label, tone = "flat") {
  return `<span class="impact-badge ${escapeHtml(tone)}">${escapeHtml(cleanCopy(label || "Neutral"))}</span>`;
}

function renderConfidenceBar(confidence) {
  const numeric = numberOrNull(confidence);
  const pct = numeric === null
    ? (confidenceTone(confidence) === "up" ? 78 : confidenceTone(confidence) === "down" ? 34 : 56)
    : Math.max(8, Math.min(100, numeric <= 1 ? numeric * 100 : numeric));
  return `<span class="confidence-bar ${confidenceTone(confidence)}"><i><b style="width:${pct}%"></b></i><small>${escapeHtml(cleanCopy(confidence || `${Math.round(pct)}%`))}</small></span>`;
}

function renderAssetIcon(itemOrTicker) {
  const display = getAssetDisplayName(itemOrTicker);
  const ticker = display.ticker || "";
  const initials = ticker.includes("-USD")
    ? ticker.split("-")[0].slice(0, 2)
    : (display.displayName || ticker || "G").split(/\s+/).map((word) => word[0]).join("").slice(0, 2);
  const category = assetCategory({ ticker });
  return `<span class="asset-icon asset-icon-${escapeHtml(category)}" aria-hidden="true">${escapeHtml(initials || "G")}</span>`;
}

function firstKnownValue(...values) {
  for (const value of values) {
    if (value !== null && value !== undefined && value !== "") return value;
  }
  return null;
}

function normalizeToastTone(tone) {
  if (tone === "bad" || tone === "error") return "error";
  if (tone === "ok" || tone === "success") return "success";
  return "info";
}

function hideToast() {
  const node = document.getElementById("app-toast");
  if (!node) return;
  node.hidden = true;
  clearTimeout(toast._timer);
}

function toast(message, tone = "info") {
  const node = document.getElementById("app-toast");
  if (!node) return;
  const normalizedTone = normalizeToastTone(tone);
  const title = normalizedTone === "success" ? "Listo" : normalizedTone === "error" ? "Atencion" : "Genesis";
  node.dataset.tone = normalizedTone;
  node.innerHTML = `
    <div class="toast-copy">
      <strong>${escapeHtml(title)}</strong>
      <span>${escapeHtml(cleanCopy(message))}</span>
    </div>
    <button class="toast-close" type="button" data-toast-close aria-label="Cerrar aviso">x</button>
  `;
  node.hidden = false;
  clearTimeout(toast._timer);
  toast._timer = setTimeout(() => {
    node.hidden = true;
  }, 3600);
}

async function getJson(url) {
  return requestJson(url);
}

async function postJson(url, body, config = {}) {
  const payload = await requestJson(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  }, config);
  if (payload.ok === false) throw new Error(payload.message || "Genesis no confirmo el cambio.");
  return payload;
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function networkErrorMessage(error) {
  const text = String(error?.message || error || "").trim();
  if (error?.name === "AbortError" || text.toLowerCase().includes("abort")) {
    return "La API tardo demasiado. Genesis mantiene la pantalla viva y reintenta con datos en cache.";
  }
  if (text.toLowerCase().includes("failed to fetch")) {
    return "No pude conectar con Genesis API. Revisa que el backend local o Railway siga respondiendo.";
  }
  return cleanCopy(text || "No pude conectar con la fuente activa.");
}

async function requestJson(url, options = {}, config = {}) {
  const candidates = apiUrlCandidates(url);
  const targets = config.localOnly ? candidates.slice(0, 1) : candidates;
  const attempts = config.attempts || 2;
  const timeoutMs = config.timeoutMs || 14000;
  let lastError = null;
  for (const targetUrl of targets) {
    for (let attempt = 1; attempt <= attempts; attempt += 1) {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeoutMs);
      try {
        const response = await fetch(targetUrl, {
          cache: "no-store",
          ...options,
          signal: controller.signal,
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error(payload.message || `HTTP ${response.status}`);
        return payload;
      } catch (error) {
        lastError = error;
        if (attempt >= attempts) break;
        await delay(320);
      } finally {
        clearTimeout(timer);
      }
    }
  }
  throw new Error(networkErrorMessage(lastError));
}

function apiUrlCandidates(url) {
  const text = String(url || "");
  if (!text.startsWith("/api/")) return [text];
  const local = typeof window !== "undefined" && /^(localhost|127\.0\.0\.1|0\.0\.0\.0)$/i.test(window.location.hostname || "");
  if (!local) return [text];
  return [text, `${API_FALLBACK_ORIGIN}${text}`];
}

function priceLabel(item) {
  const price = itemPrice(item);
  return price === null ? "Sin precio" : money(price);
}

function priceSourceLabel(item) {
  if (positiveOrNull(item?.current_price) !== null) return "Precio live";
  if (positiveOrNull(item?.reference_price) !== null) return "Referencia";
  if (positiveOrNull(item?.entry_price) !== null) return "Referencia de entrada";
  return "Sin precio confirmado";
}

function dailyMoveLabel(item) {
  const usd = itemDailyUsd(item);
  const pct = itemDailyPct(item);
  if (usd === null && pct === null) return "Sin cambio";
  if (usd === null) return formatPercent(pct);
  if (pct === null) return formatChange(usd);
  return `${formatChange(usd)} ${formatPercent(pct)}`;
}

function firstDirectionalValue(...values) {
  let fallback = null;
  for (const value of values) {
    const numeric = numberOrNull(value);
    if (numeric === null) continue;
    if (fallback === null) fallback = numeric;
    if (numeric !== 0) return numeric;
  }
  return fallback;
}

function movementTone(itemOrValue) {
  const value = typeof itemOrValue === "object"
    ? firstDirectionalValue(itemDailyPct(itemOrValue), itemDailyUsd(itemOrValue), positionPnl(itemOrValue))
    : numberOrNull(itemOrValue);
  return positiveClass(value);
}

function marketToneClass(itemOrValue) {
  return `market-number ${movementTone(itemOrValue)}`;
}

function dailyMoveMarkup(item) {
  const usd = itemDailyUsd(item);
  const pct = itemDailyPct(item);
  const usdTone = positiveClass(usd);
  const pctTone = positiveClass(pct);
  return `
    <span class="change-line ${usdTone}">${escapeHtml(formatChange(usd, "Sin cambio"))}</span>
    <span class="change-line ${pctTone}">${escapeHtml(formatPercent(pct, "Sin dato"))}</span>
  `;
}

function iconSvg(name) {
  const icons = {
    add: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 5v14M5 12h14"/></svg>`,
    cart: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 6h15l-2 8H8L6 3H3"/><path d="M9 20h.01M18 20h.01"/></svg>`,
    remove: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 12h14"/></svg>`,
    send: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 12h13"/><path d="M13 6l6 6-6 6"/></svg>`,
    check: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M20 6 9 17l-5-5"/></svg>`,
    search: `<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg>`,
    upload: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 17V5"/><path d="M7 10l5-5 5 5"/><path d="M5 19h14"/></svg>`,
    mic: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3a3 3 0 0 0-3 3v6a3 3 0 0 0 6 0V6a3 3 0 0 0-3-3z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2"/><path d="M12 19v3"/><path d="M8 22h8"/></svg>`,
    menu: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 12h.01M19 12h.01M5 12h.01"/></svg>`,
    history: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 12a9 9 0 1 0 3-6.7"/><path d="M3 4v6h6"/><path d="M12 7v5l3 2"/></svg>`,
    new: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 5v14M5 12h14"/></svg>`,
    clear: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 7h16"/><path d="M10 11v6M14 11v6"/><path d="M6 7l1 13h10l1-13"/><path d="M9 7V4h6v3"/></svg>`,
    back: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M15 6l-6 6 6 6"/></svg>`,
    refresh: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M20 12a8 8 0 0 1-13.6 5.7"/><path d="M4 12A8 8 0 0 1 17.6 6.3"/><path d="M17 2v5h5"/><path d="M7 22v-5H2"/></svg>`,
  };
  return icons[name] || "";
}

function genesisLogoMarkup(className = "", alt = "") {
  const classes = ["genesis-logo-img", className].filter(Boolean).join(" ");
  return `<img class="${escapeHtml(classes)}" src="${GENESIS_LOGO_SRC}" alt="${escapeHtml(alt)}" decoding="async" loading="eager">`;
}

function chartCacheKey(ticker, range) {
  return `${normalizeTicker(ticker)}:${String(range || "1Y").toUpperCase()}`;
}

function chartIntentFromText(text) {
  const raw = String(text || "");
  const normalized = raw
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase();
  if (!/(GRAFICA|GRAFICO|CHART)/.test(normalized)) return null;
  const ticker = tickerFromText(normalized);
  return ticker ? { ticker: normalizeTicker(ticker), range: "1Y" } : null;
}

function plainQuestionText(text) {
  return String(text || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function isMarketOverviewQuestion(text) {
  const normalized = ` ${plainQuestionText(text)} `;
  if (normalized.includes(" mercado libre ")) return false;
  if (normalized.includes(" mercado ") && !/( seguimiento | cartera | watchlist | portfolio | paper )/.test(normalized)) return true;
  return [
    " como esta el mercado ",
    " como va el mercado ",
    " mercado el dia de hoy ",
    " mercado hoy ",
    " que esta pasando hoy ",
    " viernes pasado ",
  ].some((needle) => normalized.includes(needle));
}

function isWhaleQuestion(text) {
  const normalized = ` ${plainQuestionText(text)} `;
  return [
    " ballena ",
    " ballenas ",
    " smart money ",
    " dinero grande ",
    " flujo institucional ",
    " flujos institucionales ",
  ].some((needle) => normalized.includes(needle));
}

function isNewsQuestion(text) {
  const normalized = ` ${plainQuestionText(text)} `;
  return [
    " noticia ",
    " noticias ",
    " titulares ",
    " catalizador ",
    " catalizadores ",
    " que esta pasando en noticias ",
    " que paso en noticias ",
    " noticias importantes ",
    " ultimas noticias ",
  ].some((needle) => normalized.includes(needle));
}

function tickerFromText(text) {
  const normalized = String(text || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase();
  if (isMarketOverviewQuestion(normalized)) return "";
  if (isNewsQuestion(normalized)) return "";
  if (isWhaleQuestion(normalized)) {
    const whaleTicker = normalized.match(/\b(BTC|BITCOIN|ETH|SOL|NVDA|MSFT|META|NFLX|TSLA|SPY|QQQ|BNO|BZ=F|IAU|SLV|MARA)\b/);
    if (!whaleTicker) return "";
  }
  const stop = new Set(["ANALIZA", "ANALIZAR", "OPINAS", "OPINA", "COMPRAR", "COMPRA", "DEBERIA", "QUIERO", "REVISA", "REVISAR", "VER", "HAZME", "UNA", "UN", "GRAFICA", "GRAFICAS", "GRAFICO", "GRAFICOS", "CHART", "MUESTRAME", "MOSTRAME", "MUESTRA", "DE", "DEL", "EN", "LA", "EL", "LAS", "LOS", "POR", "FAVOR", "CON", "VELAS", "VELA", "HORA", "FECHA", "QUE", "RESUMEN", "DIA", "HOY", "OYE", "GENESIS", "MERCADO", "ACCION", "ACCIONES", "NOTICIA", "NOTICIAS", "TITULAR", "TITULARES", "CATALIZADOR", "CATALIZADORES", "SI", "BAJA", "SUBE", "ENTRA", "ENTRAR", "VENDER", "PUEDE", "PUEDO", "COMO", "ESTA", "ESTAN", "ESTAS", "ESTOY", "ESTAMOS", "PASANDO", "PASA", "PASO", "DIME", "BIEN", "TAL", "TODO", "LISTO", "GRACIAS", "VA", "VAS", "VOY", "HOLA", "BUENAS", "BALLENA", "BALLENAS", "SMART", "MONEY", "DINERO", "GRANDE", "FLUJO", "FLUJOS", "INSTITUCIONAL", "INSTITUCIONALES"]);
  const aliases = { BTC: "BTC-USD", BITCOIN: "BTC-USD", ETH: "ETH-USD", SOL: "SOL-USD", BRENT: "BZ=F", PETROLEO: "BZ=F", ORO: "IAU", PLATA: "SLV" };
  const tokens = normalized.match(/\b[A-Z0-9]{1,12}(?:[.\-=][A-Z0-9]{1,8})?\b/g) || [];
  const rawTicker = tokens.find((token) => !stop.has(token) && /[A-Z0-9]/.test(token));
  return aliases[rawTicker] || rawTicker || "";
}

async function loadChartSeries(ticker, range = "1Y") {
  const normalizedTicker = normalizeTicker(ticker);
  const normalizedRange = CHART_RANGES.includes(String(range).toUpperCase()) ? String(range).toUpperCase() : "1Y";
  const key = chartCacheKey(normalizedTicker, normalizedRange);
  const cached = appState.chartCache[key] || {};
  const now = Date.now();
  const age = now - (cached.loadedAt || 0);
  if (cached.loading && cached.promise) return cached.promise;
  if (cached.payload?.ok !== false && cached.loadedAt && age <= CHART_CACHE_TTL_MS) return cached.payload;
  if (cached.payload?.ok === false && cached.loadedAt && age <= CHART_FAILURE_RETRY_MS) return cached.payload;
  const request = (async () => {
    try {
      const payload = await getJson(`/api/dashboard/asset/chart?ticker=${encodeURIComponent(normalizedTicker)}&range=${encodeURIComponent(normalizedRange)}`);
      appState.chartCache[key] = { loading: false, payload, promise: null, loadedAt: Date.now() };
      return payload;
    } catch (error) {
      const payload = {
        ok: false,
        ticker: normalizedTicker,
        timeframe: normalizedRange,
        points: [],
        ohlc: [],
        returns: {},
        summary: {},
        message: error.message || "No pude cargar la grafica.",
      };
      appState.chartCache[key] = { loading: false, payload, promise: null, loadedAt: Date.now() };
      return payload;
    }
  })();
  appState.chartCache[key] = {
    loading: true,
    payload: cached.payload?.ok !== false ? cached.payload : null,
    promise: request,
    loadedAt: cached.loadedAt || 0,
  };
  return request;
}

function chartReading(payload) {
  const ticker = normalizeTicker(payload?.ticker);
  const range = String(payload?.range || payload?.timeframe || "1Y").toUpperCase();
  const changePct = numberOrNull(payload?.returns?.[range] ?? payload?.summary?.change_pct);
  const candles = Array.isArray(payload?.ohlc) ? payload.ohlc : [];
  if (!payload?.ok || candles.length < 2) {
    return `${ticker || "El activo"} no tiene datos OHLC suficientes para esta temporalidad.`;
  }
  if (changePct === null || changePct === 0) {
    return `${ticker} se mantiene neutral en ${range}; conviene esperar confirmacion antes de subir conviccion.`;
  }
  return changePct > 0
    ? `${ticker} mantiene rendimiento positivo en ${range}; puede tener correcciones internas, pero no lo trato como perdida en esa ventana.`
    : `${ticker} esta negativo en ${range}; Genesis prioriza cautela hasta ver recuperacion.`;
}

function chartScale(candles, height, padding) {
  const values = candles.flatMap((point) => [numberOrNull(point.high), numberOrNull(point.low)]).filter((value) => value !== null);
  if (!values.length) return () => height / 2;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || Math.max(max, 1);
  return (value) => padding + ((max - value) / range) * (height - padding * 2);
}

function candleMarkup(point, index, candles, width, height, padding) {
  const open = numberOrNull(point.open);
  const high = numberOrNull(point.high);
  const low = numberOrNull(point.low);
  const close = numberOrNull(point.close);
  if ([open, high, low, close].some((value) => value === null)) return "";
  const y = chartScale(candles, height, padding);
  const slot = (width - padding * 2) / Math.max(candles.length, 1);
  const bodyWidth = Math.max(2, Math.min(9, slot * 0.58));
  const x = padding + slot * index + slot / 2;
  const openY = y(open);
  const closeY = y(close);
  const highY = y(high);
  const lowY = y(low);
  const top = Math.min(openY, closeY);
  const bodyHeight = Math.max(1.5, Math.abs(closeY - openY));
  const tone = close > open ? "up" : close < open ? "down" : "flat";
  return `
    <g class="candle ${tone}">
      <line class="candle-wick" x1="${x.toFixed(2)}" x2="${x.toFixed(2)}" y1="${highY.toFixed(2)}" y2="${lowY.toFixed(2)}"></line>
      <rect class="candle-body" x="${(x - bodyWidth / 2).toFixed(2)}" y="${top.toFixed(2)}" width="${bodyWidth.toFixed(2)}" height="${bodyHeight.toFixed(2)}" rx="1.2"></rect>
    </g>
  `;
}

function chartSvgMarkup(payload) {
  const candles = Array.isArray(payload?.ohlc) ? payload.ohlc : Array.isArray(payload?.points) ? payload.points : [];
  if (candles.length < 2) return `<div class="chart-empty">${escapeHtml(payload?.message || "No hay datos OHLC suficientes para esta temporalidad.")}</div>`;
  const width = 320;
  const height = 150;
  const padding = 12;
  const tone = positiveClass(payload?.summary?.change_pct ?? payload?.summary?.change);
  const first = candles[0];
  const last = candles[candles.length - 1];
  return `
    <svg class="asset-chart-svg candle-chart ${tone}" viewBox="0 0 ${width} ${height}" role="img" aria-label="Velas japonesas de ${escapeHtml(payload.ticker || "activo")}">
      <path class="chart-grid" d="M${padding} 38 H${width - padding} M${padding} 75 H${width - padding} M${padding} 112 H${width - padding}"></path>
      ${candles.map((point, index) => candleMarkup(point, index, candles, width, height, padding)).join("")}
      <text x="${padding}" y="${height - 5}">${escapeHtml(String(first?.time || first?.date || "").slice(0, 10))}</text>
      <text x="${width - padding}" y="${height - 5}" text-anchor="end">${escapeHtml(String(last?.time || last?.date || "").slice(0, 10))}</text>
    </svg>
  `;
}

function chartReturnsMarkup(payload) {
  const returns = payload?.returns || {};
  return `
    <div class="chart-returns">
      ${CHART_RANGES.map((range) => `<span class="${marketClass(returns[range])}"><small>${range}</small>${escapeHtml(formatSignedPercent(returns[range], "Sin dato"))}</span>`).join("")}
    </div>
  `;
}

function indicatorStripMarkup(payload) {
  const indicators = payload?.indicators || payload?.technical?.indicators || {};
  if (!indicators || indicators.ok === false) return "";
  const macd = indicators.macd || {};
  const ema = indicators.ema || {};
  const fibonacci = indicators.fibonacci || {};
  const golden = indicators.golden_pocket || {};
  const items = [
    ["Volumen", payload?.quote?.volume ?? payload?.summary?.volume ?? indicators.relative_volume],
    ["RSI", indicators.rsi],
    ["MACD", macd.line],
    ["EMA 50", ema["50"] ?? indicators.ema_50],
    ["Fib 0.618", fibonacci["0.618"]],
    ["Golden", firstKnownValue(golden.from, golden.to)],
  ].filter(([, value]) => numberOrNull(value) !== null);
  if (!items.length) return "";
  return `
    <div class="indicator-strip" aria-label="Indicadores tecnicos">
      ${items.slice(0, 6).map(([label, value]) => `
        <span>
          <small>${escapeHtml(label)}</small>
          <strong>${escapeHtml(compactNumber(value))}</strong>
        </span>
      `).join("")}
    </div>
  `;
}

function chartMaxNote(payload) {
  if (!payload) return "";
  if (payload.max_history_note) return String(payload.max_history_note);
  if (payload.source?.max_history_note) return String(payload.source.max_history_note);
  const years = numberOrNull(payload.max_history_years ?? payload.source?.max_history_years);
  if (years === null) return "";
  if (years > 5.05) return `MAX usa ${years.toFixed(2)} anos reales de historico FMP.`;
  if (years > 0) return `MAX disponible: ${years.toFixed(2)} anos. FMP no entrego mas historico confirmado para este activo.`;
  return payload.ok === false ? "MAX sin datos historicos en este entorno." : "";
}

function chartBlockMarkup(ticker, range = "1Y", target = "asset") {
  const normalizedTicker = normalizeTicker(ticker);
  const normalizedRange = CHART_RANGES.includes(String(range).toUpperCase()) ? String(range).toUpperCase() : "1Y";
  const state = appState.chartCache[chartCacheKey(normalizedTicker, normalizedRange)] || {};
  const payload = state.payload;
  const fallbackAsset = findAsset(normalizedTicker) || {};
  const fallbackPrice = itemPrice(fallbackAsset);
  const fallbackChange = itemDailyUsd(fallbackAsset);
  const fallbackPct = itemDailyPct(fallbackAsset);
  const quotePrice = positiveOrNull(payload?.quote?.price ?? payload?.summary?.end_price) ?? fallbackPrice;
  const change = numberOrNull(payload?.quote?.change ?? payload?.summary?.change) ?? fallbackChange;
  const rawChangePct = numberOrNull(payload?.quote?.changesPercentage ?? payload?.quote?.change_pct ?? payload?.summary?.change_pct);
  const previousClose = positiveOrNull(payload?.quote?.previousClose ?? payload?.quote?.previous_close ?? fallbackAsset.previousClose ?? fallbackAsset.previous_close);
  const derivedChangePct = quotePrice !== null && previousClose !== null && previousClose > 0
    ? ((quotePrice - previousClose) / previousClose) * 100
    : null;
  const changePct = rawChangePct !== null && rawChangePct !== 0 ? rawChangePct : (derivedChangePct ?? fallbackPct);
  const tone = positiveClass(changePct ?? change);
  const display = getAssetDisplayName({ ticker: normalizedTicker, name: payload?.name || fallbackAsset.name });
  return `
    <section class="chart-card" data-chart-card="${escapeHtml(normalizedTicker)}">
      <div class="chart-card-header">
        <div>
          <strong>${escapeHtml(display.displayName)}</strong>
          <small>${escapeHtml(display.subtitle || normalizedTicker)}</small>
        </div>
        <div class="chart-price">
          <strong class="market-number ${tone}">${escapeHtml(money(quotePrice, "Sin precio"))}</strong>
          <span class="${positiveClass(changePct ?? change)}">${escapeHtml(formatChange(change, "Sin cambio"))} ${escapeHtml(formatPercent(changePct, "Sin dato"))}</span>
        </div>
      </div>
      <div class="chart-ranges">
        ${CHART_RANGES.map((item) => `<button type="button" class="${item === normalizedRange ? "is-active" : ""}" data-chart-range="${item}" data-chart-ticker="${escapeHtml(normalizedTicker)}" data-chart-target="${escapeHtml(target)}">${item}</button>`).join("")}
      </div>
      <div class="chart-canvas">
        ${state.loading ? `<div class="chart-empty">Cargando grafica...</div>` : chartSvgMarkup(payload)}
      </div>
      ${payload ? chartReturnsMarkup(payload) : ""}
      ${payload ? indicatorStripMarkup(payload) : ""}
      ${payload ? `<p class="chart-source-note">${escapeHtml(chartMaxNote(payload))}</p>` : ""}
      <p class="chart-read">${escapeHtml(chartReading(payload))}</p>
    </section>
  `;
}

function previousCloseLabel(item) {
  const value = positiveOrNull(item?.previous_close ?? item?.previousClose);
  return value === null ? "Sesion anterior no disponible" : `Anterior ${money(value)}`;
}

function extendedLabel(item) {
  const price = positiveOrNull(item?.extended_hours_price);
  if (price === null) return "Sin nocturno";
  const change = numberOrNull(item?.extended_hours_change);
  const pct = numberOrNull(item?.extended_hours_change_pct);
  const move = change !== null || pct !== null ? ` ${dailyMoveLabel({ daily_change: change, daily_change_pct: pct })}` : "";
  return `Nocturno ${money(price)}${move}`;
}

function marketSessionLabel(item) {
  const raw = String(item?.market_session || "").toLowerCase();
  if (raw.includes("after") || raw.includes("post")) return "After-hours";
  if (raw.includes("pre")) return "Premarket";
  if (raw.includes("open")) return "Mercado abierto";
  if (raw.includes("close")) return "Mercado cerrado";
  if (positiveOrNull(item?.extended_hours_price) !== null) return "Nocturno";
  return "Sesion regular";
}

function findAsset(ticker) {
  const normalized = normalizeTicker(ticker);
  return appState.allItems.find((item) => itemTicker(item) === normalized)
    || appState.marketSearchResults.tracking.find((item) => itemTicker(item) === normalized)
    || appState.marketSearchResults.portfolio.find((item) => itemTicker(item) === normalized)
    || null;
}

function chartQuoteAsset(ticker, range = "1Y") {
  const normalized = normalizeTicker(ticker);
  const payload = appState.chartCache[chartCacheKey(normalized, range)]?.payload
    || appState.chartCache[chartCacheKey(normalized, "1Y")]?.payload
    || appState.chartCache[chartCacheKey(normalized, "1D")]?.payload;
  if (!payload || payload.ok === false) return null;
  const candles = Array.isArray(payload.ohlc) ? payload.ohlc : Array.isArray(payload.points) ? payload.points : [];
  const lastCandle = candles.length ? candles[candles.length - 1] : {};
  const quote = payload.quote || {};
  const summary = payload.summary || {};
  const indicators = payload.indicators || {};
  const price = positiveOrNull(quote.price ?? quote.current_price ?? summary.end_price ?? lastCandle.close);
  if (price === null) return null;
  const previousClose = positiveOrNull(quote.previousClose ?? quote.previous_close);
  const rawChangePct = numberOrNull(quote.changesPercentage ?? quote.change_pct ?? summary.change_pct);
  const derivedChangePct = previousClose !== null && previousClose > 0 ? ((price - previousClose) / previousClose) * 100 : null;
  return {
    ticker: normalized,
    name: payload.name || normalized,
    current_price: price,
    price,
    daily_change: numberOrNull(quote.change ?? summary.change),
    daily_change_pct: rawChangePct !== null && rawChangePct !== 0 ? rawChangePct : derivedChangePct,
    previous_close: previousClose,
    day_high: positiveOrNull(quote.dayHigh ?? quote.day_high ?? lastCandle.high),
    day_low: positiveOrNull(quote.dayLow ?? quote.day_low ?? lastCandle.low),
    volume: positiveOrNull(quote.volume ?? lastCandle.volume ?? indicators.volume),
    relative_volume: numberOrNull(indicators.relative_volume),
    support: numberOrNull(indicators.support),
    resistance: numberOrNull(indicators.resistance),
    quote_timestamp: quote.timestamp || lastCandle.time || lastCandle.date || payload.last_date,
    source: "FMP / chart live",
  };
}

function assetDetailItem(ticker, range = "1Y") {
  const normalized = normalizeTicker(ticker);
  return {
    ticker: normalized,
    ...(chartQuoteAsset(normalized, range) || {}),
    ...(findAsset(normalized) || {}),
    ...(chartQuoteAsset(normalized, range) || {}),
  };
}

function normalizeScreen(view) {
  if (view === "watchlist") return "tracking";
  if (view === "radar") return "portfolio";
  if (view === "money-flow") return "news";
  return view || "genesis";
}

function screenId(screen) {
  if (screen === "tracking") return "view-watchlist";
  if (screen === "portfolio") return "view-radar";
  if (screen === "news") return "view-news";
  return `view-${screen}`;
}

function updateNav() {
  const visibleScreen = appState.activeScreen === "asset-detail" ? appState.selectedAssetPreviousScreen : appState.activeScreen;
  document.querySelectorAll(".nav-link").forEach((button) => {
    button.classList.toggle("is-active", normalizeScreen(button.dataset.view) === visibleScreen);
  });
}

function extractPortfolioItems(snapshot) {
  if (Array.isArray(snapshot?.items)) return snapshot.items;
  if (Array.isArray(snapshot?.positions)) return snapshot.positions;
  if (Array.isArray(snapshot?.portfolio?.items)) return snapshot.portfolio.items;
  if (Array.isArray(snapshot?.portfolio?.positions)) return snapshot.portfolio.positions;

  const positions = snapshot?.positions || snapshot?.portfolio?.positions;
  if (positions && typeof positions === "object") {
    return Object.entries(positions).map(([ticker, value]) => (
      value && typeof value === "object"
        ? { ticker, ...value }
        : { ticker, reference_price: value, watchlist: true }
    ));
  }
  return [];
}

function extractPortfolioSummary(snapshot) {
  const summary = snapshot?.summary || {};
  return summary.portfolio && typeof summary.portfolio === "object"
    ? { ...summary.portfolio, ...summary }
    : summary;
}

function findPaperPosition(ticker) {
  const normalized = normalizeTicker(ticker);
  return appState.paperPositions.find((item) => itemTicker(item) === normalized && itemIsPaper(item)) || null;
}

function findTrackingItem(ticker) {
  const normalized = normalizeTicker(ticker);
  return appState.trackingItems.find((item) => itemTicker(item) === normalized && itemInWatchlist(item)) || null;
}

function splitPortfolioSnapshot(snapshot) {
  const rows = extractPortfolioItems(snapshot).filter((item) => itemTicker(item));
  appState.allItems = rows;
  appState.paperPositions = rows.filter((item) => itemIsPaper(item));
  appState.trackingItems = rows.filter((item) => itemInWatchlist(item));
  appState.lastUpdated = snapshot?.summary?.last_update || snapshot?.generated_at || new Date().toISOString();

  const computedValue = appState.paperPositions.reduce((sum, item) => sum + (itemValue(item) || 0), 0);
  const computedDaily = appState.paperPositions.reduce((sum, item) => sum + (numberOrNull(item.daily_pnl) || 0), 0);
  const computedPnl = appState.paperPositions.reduce((sum, item) => sum + (numberOrNull(positionPnl(item)) || 0), 0);
  const summary = extractPortfolioSummary(snapshot);
  appState.portfolioTotals = {
    totalValue: computedValue || numberOrNull(summary.total_value) || 0,
    dailyPnl: computedDaily || numberOrNull(summary.daily_pnl),
    dailyPnlPct: numberOrNull(summary.daily_pnl_pct),
    totalPnl: computedPnl || numberOrNull(summary.total_unrealized_pnl),
    totalPnlPct: numberOrNull(summary.total_unrealized_pnl_pct),
    positionCount: appState.paperPositions.length,
    watchlistCount: appState.trackingItems.length,
  };
}

async function refreshPortfolio(options = {}) {
  const shouldRender = options.render !== false;
  if (appState.refreshInFlight && appState.refreshPromise) {
    if (!options.force) return appState.refreshPromise;
    await appState.refreshPromise.catch(() => null);
  }
  appState.refreshInFlight = true;
  setLiveRefreshIndicator(true);
  if (shouldRender) renderActiveScreen();
  appState.refreshPromise = (async () => {
    const snapshot = await getJson(PORTFOLIO_ENDPOINT);
    appState.portfolioSnapshot = snapshot;
    appState.radarSnapshot = snapshot;
    splitPortfolioSnapshot(snapshot);
    appState.error = "";
    if (shouldRender) renderActiveScreen();
    return snapshot;
  })();
  try {
    return await appState.refreshPromise;
  } finally {
    appState.refreshInFlight = false;
    appState.refreshPromise = null;
    setLiveRefreshIndicator(false);
    if (shouldRender) renderActiveScreen();
  }
}

function startPortfolioAutoRefresh() {
  if (appState.refreshTimer) return;
  appState.refreshTimer = setInterval(() => {
    if (document.hidden) return;
    if (appState.activeScreen !== "tracking" && appState.activeScreen !== "portfolio" && appState.activeScreen !== "asset-detail") return;
    refreshPortfolio({ render: true }).catch(() => {});
  }, REFRESH_MS);
}

function stopPortfolioAutoRefresh() {
  if (!appState.refreshTimer) return;
  clearInterval(appState.refreshTimer);
  appState.refreshTimer = null;
}

function startMarketAutoRefresh() {
  if (appState.marketRefreshTimer) return;
  appState.marketRefreshTimer = setInterval(() => {
    if (document.hidden) return;
    if (appState.activeScreen === "news") {
      loadNews({ silent: true }).catch(() => {});
      return;
    }
    if (appState.activeScreen === "alerts") {
      Promise.allSettled([
        refreshPortfolio({ render: false, force: true }),
        loadMarketPulse({ force: true }),
        loadOpportunityQuotes({ force: true }),
        loadAlerts(),
        loadWhalesData(),
      ]).then(() => renderAlertsScreen()).catch(() => {});
      return;
    }
    if (appState.activeScreen === "tracking" || appState.activeScreen === "portfolio") {
      Promise.allSettled([
        refreshPortfolio({ render: false, force: true }),
        loadMarketPulse({ force: true }),
      ]).then(() => renderActiveScreen()).catch(() => {});
    }
  }, MARKET_FEED_REFRESH_MS);
}

function stopMarketAutoRefresh() {
  if (!appState.marketRefreshTimer) return;
  clearInterval(appState.marketRefreshTimer);
  appState.marketRefreshTimer = null;
}

function marketPulseRows() {
  return MARKET_PULSE_TICKERS.map((ticker) => findAsset(ticker) || appState.marketPulse[ticker] || { ticker });
}

async function loadMarketPulse(options = {}) {
  const now = Date.now();
  if (appState.marketPulseLoading) return appState.marketPulse;
  if (!options.force && appState.marketPulseLoadedAt && now - appState.marketPulseLoadedAt < MARKET_FEED_REFRESH_MS) {
    return appState.marketPulse;
  }
  appState.marketPulseLoading = true;
  setLiveRefreshIndicator(true);
  try {
    const results = await Promise.allSettled(MARKET_PULSE_TICKERS.map(async (ticker) => {
      const payload = await getJson(`/api/dashboard/market/search?q=${encodeURIComponent(ticker)}`);
      const rows = Array.isArray(payload?.results) ? payload.results : Array.isArray(payload?.items) ? payload.items : [];
      const first = rows.find((row) => itemTicker(row) === ticker) || rows[0] || payload;
      return { ticker, item: { ...first, ticker: itemTicker(first) || ticker } };
    }));
    results.forEach((result) => {
      if (result.status !== "fulfilled" || !result.value?.ticker) return;
      appState.marketPulse[result.value.ticker] = result.value.item;
    });
    appState.marketPulseLoadedAt = Date.now();
  } finally {
    appState.marketPulseLoading = false;
    setLiveRefreshIndicator(false);
  }
  return appState.marketPulse;
}

async function loadOpportunityQuotes(options = {}) {
  const now = Date.now();
  if (appState.opportunityQuotesLoading) return appState.opportunityQuotes;
  if (!options.force && appState.opportunityQuotesLoadedAt && now - appState.opportunityQuotesLoadedAt < MARKET_FEED_REFRESH_MS) {
    return appState.opportunityQuotes;
  }
  appState.opportunityQuotesLoading = true;
  setLiveRefreshIndicator(true);
  try {
    const results = await Promise.allSettled(OPPORTUNITY_TICKERS.map(async (ticker) => {
      const payload = await getJson(`/api/dashboard/market/search?q=${encodeURIComponent(ticker)}`);
      const rows = Array.isArray(payload?.results) ? payload.results : [];
      const first = rows.find((row) => itemTicker(row) === ticker) || rows[0];
      return first ? { ticker, item: { ...first, ticker: itemTicker(first) || ticker } } : null;
    }));
    results.forEach((result) => {
      if (result.status !== "fulfilled" || !result.value?.ticker) return;
      appState.opportunityQuotes[result.value.ticker] = result.value.item;
    });
    appState.opportunityQuotesLoadedAt = Date.now();
  } finally {
    appState.opportunityQuotesLoading = false;
    setLiveRefreshIndicator(false);
  }
  return appState.opportunityQuotes;
}

function ensureOpportunityQuotes() {
  const stale = !appState.opportunityQuotesLoadedAt || Date.now() - appState.opportunityQuotesLoadedAt > MARKET_FEED_REFRESH_MS;
  if (!stale || appState.opportunityQuotesLoading) return;
  loadOpportunityQuotes().then(() => {
    if (appState.activeScreen === "alerts") renderAlertsScreen();
  }).catch(() => {});
}

function ensureMarketPulse() {
  const stale = !appState.marketPulseLoadedAt || Date.now() - appState.marketPulseLoadedAt > MARKET_FEED_REFRESH_MS;
  if (!stale || appState.marketPulseLoading) return;
  loadMarketPulse().then(() => {
    if (["tracking", "portfolio", "alerts"].includes(appState.activeScreen)) renderActiveScreen();
  }).catch(() => {});
}

function setActiveScreen(screen, options = {}) {
  appState.activeScreen = screen;
  document.querySelectorAll(".app-screen").forEach((node) => {
    node.classList.toggle("is-active", node.id === screenId(screen));
  });
  updateNav();
  renderActiveScreen();
  if (options.scrollTop !== false) {
    scrollActiveScreenToTop(screen);
  }

  if (screen === "tracking" || screen === "portfolio" || screen === "asset-detail") {
    startPortfolioAutoRefresh();
    refreshPortfolio({ render: true }).catch((error) => toast(error.message, "error"));
  } else {
    stopPortfolioAutoRefresh();
  }

  if (screen === "news") {
    startMarketAutoRefresh();
    loadNews().catch((error) => toast(error.message, "error"));
  } else if (screen === "alerts") {
    startMarketAutoRefresh();
    Promise.allSettled([
      refreshPortfolio({ render: false, force: true }),
      loadOpportunityQuotes(),
      loadAlerts(),
      loadWhalesData(),
    ]).then((results) => {
      const failed = results.find((result) => result.status === "rejected");
      if (failed) toast(networkErrorMessage(failed.reason), "info");
      renderAlertsScreen();
    });
  } else {
    stopMarketAutoRefresh();
  }
}

async function searchMarket(query, mode) {
  const value = String(query || "").trim();
  if (!value) throw new Error("Escribe un ticker o empresa.");
  const payload = await getJson(`/api/dashboard/market/search?q=${encodeURIComponent(value)}`);
  const results = Array.isArray(payload.results) ? payload.results : [];
  appState.marketSearchResults[mode] = results;
  if (mode === "tracking") appState.trackingSearchQuery = value;
  if (mode === "portfolio") appState.portfolioSearchQuery = value;
  renderActiveScreen();
  if (!payload.ok || !results.length) {
    throw new Error(payload.message || "No encontre ese ticker en mercado.");
  }
  return results;
}

async function searchAndAddPortfolioTicker() {
  const input = document.getElementById("portfolio-search-input");
  const query = input?.value || appState.trackingSearchQuery || "";
  try {
    const results = await searchMarket(query, "tracking");
    await addTickerToWatchlist(results[0].ticker);
  } catch (error) {
    toast(error.message || "No pude agregar el activo.", "error");
  }
}

async function searchTrackingOnly() {
  const input = document.getElementById("portfolio-search-input");
  try {
    await searchMarket(input?.value || appState.trackingSearchQuery || "", "tracking");
  } catch (error) {
    toast(error.message, "error");
  }
}

async function searchPortfolioBuyTicker() {
  const input = document.getElementById("portfolio-buy-search-input");
  try {
    await searchMarket(input?.value || appState.portfolioSearchQuery || "", "portfolio");
  } catch (error) {
    toast(error.message, "error");
  }
}

async function addTickerToWatchlist(ticker) {
  const normalized = normalizeTicker(ticker);
  if (!normalized) throw new Error("Ticker no valido.");
  const result = await postJson("/api/dashboard/portfolio/watchlist/add", { ticker: normalized });
  await refreshPortfolio({ render: false, force: true });
  const exists = Boolean(findTrackingItem(normalized));
  if (!exists) {
    renderActiveScreen();
    throw new Error("No se agrego a seguimiento. El snapshot no cambio.");
  }
  renderActiveScreen();
  toast(result.status === "exists" ? "Ya esta en seguimiento." : `${normalized} agregado a seguimiento.`, "success");
  return result;
}

async function removeTickerFromWatchlist(ticker) {
  const normalized = normalizeTicker(ticker);
  if (!normalized) return;
  const result = await postJson("/api/dashboard/portfolio/watchlist/remove", { ticker: normalized });
  await refreshPortfolio({ render: false, force: true });
  const stillThere = Boolean(findTrackingItem(normalized));
  if (stillThere) {
    renderActiveScreen();
    throw new Error("No se quito de seguimiento. El snapshot no cambio.");
  }
  renderActiveScreen();
  toast(result.message || `${normalized} quitado de seguimiento.`, "success");
  return result;
}

async function savePaperBuy(ticker, units, entryPrice) {
  const normalized = normalizeTicker(ticker);
  if (!normalized) throw new Error("Ticker no valido.");
  if (!numberOrNull(units) || numberOrNull(units) <= 0) throw new Error("Necesito unidades mayores a cero.");
  if (!numberOrNull(entryPrice) || numberOrNull(entryPrice) <= 0) throw new Error("Necesito precio de entrada mayor a cero.");

  const result = await postJson("/api/dashboard/portfolio/paper-buy", {
    ticker: normalized,
    units,
    entry_price: entryPrice,
    mode: "paper",
  });
  await refreshPortfolio({ render: false, force: true });
  const position = findPaperPosition(normalized);
  if (!position) {
    renderActiveScreen();
    throw new Error("No se guardo la compra simulada. La lectura actual no trae la posicion paper.");
  }
  renderActiveScreen();
  toast(result.message || `Compra simulada de ${normalized} guardada.`, "success");
  return result;
}

async function removePaperTicker(ticker) {
  const normalized = normalizeTicker(ticker);
  if (!normalized) return;
  const result = await postJson("/api/dashboard/portfolio/paper-remove", { ticker: normalized });
  await refreshPortfolio({ render: false, force: true });
  const stillThere = appState.paperPositions.some((item) => itemTicker(item) === normalized);
  if (stillThere) {
    renderActiveScreen();
    throw new Error("No se cerro la posicion. El snapshot no cambio.");
  }
  renderActiveScreen();
  toast(result.message || `Paper de ${normalized} cerrado.`, "success");
  return result;
}

function render() {
  renderGenesisScreen();
  renderAssetDetailScreen();
  renderNewsScreen();
  renderTrackingScreen();
  renderPortfolioScreen();
  renderAlertsScreen();
  updateNav();
}

function renderActiveScreen() {
  if (appState.activeScreen === "genesis") renderGenesisScreen();
  if (appState.activeScreen === "news") renderNewsScreen();
  if (appState.activeScreen === "tracking") renderTrackingScreen();
  if (appState.activeScreen === "portfolio") renderPortfolioScreen();
  if (appState.activeScreen === "alerts") renderAlertsScreen();
  if (appState.activeScreen === "asset-detail") renderAssetDetailScreen();
  updateNav();
}

function renderGenesisScreen() {
  const root = document.getElementById("view-genesis");
  if (!root) return;
  root.innerHTML = `
    <section class="genesis-stage">
      <div class="genesis-conversation">
        <div class="genesis-chat-head">
          <div class="genesis-brand-lockup">
            <span class="genesis-header-logo" aria-hidden="true">${genesisLogoMarkup("genesis-header-logo-img")}</span>
            <div>
              <strong>Genesis</strong>
              <span>Tu copiloto financiero con IA</span>
            </div>
          </div>
          ${renderConfidenceBar("activa")}
        </div>
        <div class="chat-toolbar" aria-label="Controles de conversacion">
          <button type="button" data-chat-new aria-label="Nuevo chat" title="Nuevo chat">${iconSvg("new")}</button>
          <button type="button" data-chat-history aria-label="Historial" title="Historial">${iconSvg("history")}</button>
          <button type="button" data-chat-clear aria-label="Limpiar chat actual" title="Limpiar chat actual">${iconSvg("clear")}</button>
        </div>
        ${appState.chatHistoryOpen ? chatHistoryPanelMarkup() : ""}
        <div class="chat-thread" id="genesis-thread">
          ${appState.chatMessages.map(chatBubbleMarkup).join("")}
        </div>
        <div class="chat-attachment-name" id="genesis-attachment-name" hidden></div>
        <form class="chat-form" id="genesis-chat-form">
          <div class="chat-composer-pill">
            <label class="chat-attach" title="Adjuntar grafica" aria-label="Adjuntar imagen de grafica">
              ${iconSvg("upload")}
              <input id="genesis-image-input" type="file" accept="image/*">
            </label>
            <input id="genesis-chat-input" placeholder="Pregunta a Genesis..." autocomplete="off">
            <button type="button" class="voice-button ${appState.voiceListening ? "is-listening" : ""} ${appState.voiceSpeaking ? "is-speaking" : ""}" data-voice-toggle aria-label="${appState.voiceListening ? "Detener voz" : "Hablar con Genesis"}" title="${appState.voiceListening ? "Escuchando..." : "Hablar con Genesis"}">${iconSvg("mic")}</button>
            <button type="submit" aria-label="Mandar mensaje">${iconSvg("send")}</button>
          </div>
        </form>
        <div class="chat-voice-status" id="genesis-voice-status" ${appState.voiceStatus ? "" : "hidden"}>${escapeHtml(appState.voiceStatus)}</div>
      </div>
    </section>
  `;
  const form = document.getElementById("genesis-chat-form");
  form.addEventListener("submit", submitGenesisQuestion);
  document.getElementById("genesis-image-input").addEventListener("change", (event) => {
    const file = event.target.files?.[0];
    const label = document.getElementById("genesis-attachment-name");
    if (!label) return;
    label.hidden = !file;
    label.textContent = file ? `Imagen lista: ${file.name}` : "";
  });
  const thread = document.getElementById("genesis-thread");
  if (thread) thread.scrollTop = thread.scrollHeight;
}

function chatHistoryPanelMarkup() {
  const conversations = Array.isArray(appState.chatConversations) ? appState.chatConversations : [];
  const history = conversations.length
    ? conversations
    : appState.chatMessages
      .filter((message) => {
        const text = cleanCopy(message.text);
        return String(text || "").trim() && !["Hola. Que quieres revisar hoy?", "Hola. ¿Qué quieres revisar hoy?"].includes(text);
      })
      .slice(-8)
      .reverse()
      .map((message) => ({
        conversation_id: appState.currentConversationId,
        summary: cleanCopy(message.text),
        updated_at: "",
      }));
  return `
    <div class="chat-history-panel" data-chat-history-panel>
      ${history.length ? history.map((conversation) => `
        <button type="button" data-chat-history-pick="${escapeHtml(conversation.conversation_id || appState.currentConversationId)}">
          <small>${escapeHtml(formatDate(conversation.updated_at))}</small>
          <span>${escapeHtml(cleanCopy(conversation.summary || conversation.conversation_id || "Chat Genesis").slice(0, 96))}</span>
        </button>
      `).join("") : `<span>Sin historial visible todavia.</span>`}
    </div>
  `;
}

function genesisAssistantMessageFromPayload(payload, fallbackText = "") {
  const answer = stripMarkdownCopy(payload?.assistant_narrative || payload?.answer || fallbackText || "No tengo lectura suficiente.");
  const visual = genesisVisualFromPayload(payload, answer);
  const chart = genesisChartFromPayload(payload, fallbackText);
  return {
    id: nextMessageId(),
    role: "assistant",
    text: answer,
    visual,
    chart,
  };
}

function genesisChartFromPayload(payload, prompt = "") {
  const intent = String(payload?.intent || "");
  const chart = payload?.chart || {};
  const chartTicker = normalizeTicker(chart.ticker || chart.symbol);
  if (chartTicker) return { ticker: chartTicker, range: String(chart.range || "1Y").toUpperCase() };
  const ticker = normalizeTicker(payload?.quote?.ticker || payload?.tickers?.[0]);
  if ((intent === "ticker_analysis" || intent === "technical_indicators") && ticker) {
    return { ticker, range: "1Y" };
  }
  const detected = chartIntentFromText(prompt);
  return detected ? { ticker: detected.ticker, range: detected.range } : null;
}

function genesisVisualFromPayload(payload, answer = "") {
  const intent = String(payload?.intent || "");
  const responseType = String(payload?.response_type || "");
  if ((["asset_analysis", "chart_analysis"].includes(responseType) || ["ticker_analysis", "technical_indicators", "chart_request"].includes(intent)) && (payload?.quote || payload?.chart || payload?.technical)) {
    return assetAnalysisVisual(payload, answer);
  }
  if (responseType === "comparison" || intent === "comparison") return comparisonVisual(payload, answer);
  if (responseType === "news_brief" || intent === "macro_news") return newsBriefVisual(payload, answer);
  if (responseType === "market_summary" || intent === "daily_briefing" || intent === "market_overview") return briefingVisual(payload, answer);
  if (responseType === "weather" || intent === "weather") return weatherVisual(payload, answer);
  if (responseType === "alerts_digest" || intent === "alerts") return alertsDigestVisual(payload, answer);
  if (responseType === "whale_flow" || intent === "whale_activity" || intent === "money_flow") {
    return whaleFlowVisual(payload, answer);
  }
  if (intent === "memory_query" || payload?.structured?.kind === "memory_digest") return memoryDigestVisual(payload, answer);
  if (responseType === "general_assistant" && payload?.structured?.kind === "general_assistant") return generalAssistantVisual(payload, answer);
  if (intent === "portfolio_summary") return summaryVisual("Cartera", answer);
  if (intent === "tracking_summary") return summaryVisual("Seguimiento", answer);
  if (intent === "image_chart_analysis" || payload?.structured?.kind === "chart_image_analysis") return imageChartVisual(payload, answer);
  if (responseType === "general_assistant" && answer) return summaryVisual("Genesis", answer);
  return null;
}

function isWhalePayload(payload = {}) {
  const intent = String(payload?.intent || "");
  const responseType = String(payload?.response_type || "");
  const kind = String(payload?.kind || payload?.structured?.kind || "");
  return responseType === "whale_flow" || intent === "whale_activity" || intent === "money_flow" || kind === "whale_flow";
}

function isNewsPayload(payload = {}) {
  const intent = String(payload?.intent || "");
  const responseType = String(payload?.response_type || "");
  const kind = String(payload?.kind || payload?.structured?.kind || "");
  return responseType === "news_brief" || intent === "macro_news" || kind === "news_brief";
}

function isTickerLikeGenesisPayload(payload = {}) {
  const intent = String(payload?.intent || "");
  const responseType = String(payload?.response_type || "");
  return ["asset_analysis", "chart_analysis"].includes(responseType)
    || ["ticker_analysis", "technical_indicators", "chart_request"].includes(intent)
    || Boolean(payload?.quote?.ticker || payload?.chart?.ticker || payload?.technical?.ticker);
}

function forcedWhalePayloadFromState(question = "", sourcePayload = {}) {
  const requestedTicker = tickerFromText(question);
  const rows = currentWhaleRows();
  const filteredRows = requestedTicker ? rows.filter((row) => itemTicker(row) === requestedTicker) : rows;
  const selectedRows = (filteredRows.length ? filteredRows : rows).slice(0, 5);
  const watchedVolume = selectedRows.reduce((sum, row) => {
    const value = numberOrNull(row?.monitoredDollarVolume ?? row?.monitored_dollar_volume ?? row?.dollarVolume ?? row?.dollar_volume);
    return sum + (value || 0);
  }, 0);
  const confirmedValue = selectedRows.reduce((sum, row) => {
    const confirmed = Boolean(row?.confirmed || row?.event_type === "whale_confirmed" || row?.type === "whale_confirmed");
    const value = confirmed ? numberOrNull(row?.amountUsd ?? row?.amount_usd ?? row?.confirmedAmountUsd ?? row?.confirmed_amount_usd) : null;
    return sum + (value || 0);
  }, 0);
  const focusNames = selectedRows
    .map((row) => getAssetDisplayName(row?.ticker || row?.symbol).displayName)
    .filter(Boolean)
    .slice(0, 3)
    .join(", ");
  const answer = selectedRows.length
    ? `En claro: esta es una lectura de ballenas y smart money, no un ticker. Genesis ve ${selectedRows.length} flujos vigilados${focusNames ? ` en ${focusNames}` : ""}; separo volumen vigilado de ballena confirmada y no invento comprador.`
    : "En claro: no hay ballena confirmada con entidad y monto en la lectura actual; Genesis vigila volumen, precio y flujo institucional sin convertirlo en compra confirmada.";
  return {
    ...sourcePayload,
    ok: true,
    status: "genesis_intelligence_ready",
    intent: "whale_activity",
    response_type: "whale_flow",
    kind: "whale_flow",
    answer,
    assistant_narrative: answer,
    tickers: requestedTicker ? [requestedTicker] : [],
    whales: {
      answer,
      events: selectedRows,
      items: selectedRows,
      summary: {
        estimated_count: selectedRows.length,
        watched_volume: watchedVolume || null,
        confirmed_value: confirmedValue || null,
      },
    },
    structured: {
      kind: "whale_flow",
      title: "Ballenas / Smart money",
      summary: answer,
      events: selectedRows,
      metrics: {
        estimated_count: selectedRows.length,
        watched_volume: watchedVolume || null,
        confirmed_value: confirmedValue || null,
      },
      sections: [
        { title: "Que significa", bullets: ["Volumen vigilado no es compra confirmada.", "Solo sube a ballena confirmada cuando hay entidad, monto y fuente."] },
        { title: "Que vigilar", bullets: ["Direccion del precio despues del flujo.", "Volumen relativo, soporte/resistencia y noticias relacionadas."] },
      ],
    },
  };
}

function forcedNewsPayloadFromState(question = "", sourcePayload = {}) {
  const items = newsFeedItems(appState.newsSnapshot || {});
  const filtered = filteredNewsItems(items);
  const important = importantNewsItems(filtered);
  const latest = latestNewsItems(filtered);
  const selected = [...important, ...latest.filter((item) => !important.some((candidate) => newsItemId(candidate) === newsItemId(item)))].slice(0, 5);
  const focus = selected
    .flatMap((item) => item?.assets || item?.tickers || item?.tickersAffected || [])
    .map(normalizeTicker)
    .filter(Boolean)
    .slice(0, 4);
  const uniqueFocus = Array.from(new Set(focus));
  const answer = selected.length
    ? `En noticias: Genesis esta leyendo ${selected.length} titulares reales del feed cargado. Lo importante ahora es impacto, recencia y si toca ${uniqueFocus.length ? uniqueFocus.join(", ") : "mercado general"}.`
    : "En noticias: no tengo titulares confirmados en el snapshot local todavia. Genesis debe mostrar feed real FMP/RSS cuando llegue la fuente, sin convertir esta pregunta en ticker.";
  return {
    ...sourcePayload,
    ok: true,
    status: "genesis_intelligence_ready",
    intent: "macro_news",
    response_type: "news_brief",
    kind: "news_brief",
    answer,
    assistant_narrative: answer,
    tickers: [],
    quote: null,
    chart: null,
    technical: null,
    overview: {
      answer,
      summary: answer,
      news: selected,
      important_news: important,
      latest_news: latest,
      source_status: appState.newsSnapshot?.source_status || {},
    },
    structured: {
      kind: "news_brief",
      title: "Noticias",
      summary: answer,
      important_news: important,
      latest_news: latest,
      news: selected,
      metrics: {
        important: important.length,
        latest: latest.length,
        total: items.length,
      },
      sections: [
        { title: "Lectura rapida", bullets: [answer] },
        { title: "Que vigilar", bullets: ["Titulares que mueven precio.", "Volumen despues de la noticia.", "Impacto en tus activos y cartera."] },
      ],
    },
  };
}

async function correctGenesisIntentPayload(payload = {}, question = "") {
  if (isWhaleQuestion(question)) {
    if (isWhalePayload(payload)) {
      return {
        ...payload,
        tickers: Array.isArray(payload.tickers) ? payload.tickers.filter((item) => item !== "ESTA" && item !== "ESTAS" && item !== "DIME") : [],
      };
    }
    if (isTickerLikeGenesisPayload(payload)) return forcedWhalePayloadFromState(question, payload);
    return forcedWhalePayloadFromState(question, payload);
  }
  if (isNewsQuestion(question)) {
    if (isNewsPayload(payload)) {
      return {
        ...payload,
        tickers: [],
        quote: null,
        chart: null,
        technical: null,
      };
    }
    if (isTickerLikeGenesisPayload(payload)) return forcedNewsPayloadFromState(question, payload);
    return forcedNewsPayloadFromState(question, payload);
  }
  return payload;
}

function setLiveRefreshIndicator(active) {
  const enabled = Boolean(active || appState.refreshInFlight || appState.marketPulseLoading || appState.newsLoading || appState.opportunityQuotesLoading);
  document.body.classList.toggle("is-live-refreshing", enabled);
}

function liveRefreshBadgeMarkup(label = "Actualizando precios") {
  return `
    <span class="live-refresh-badge" data-live-refresh-indicator>
      ${iconSvg("refresh")}
      <em>${escapeHtml(label)}</em>
    </span>
  `;
}

function assetAnalysisVisual(payload, answer = "") {
  const quote = payload?.quote || {};
  const structured = payload?.structured || {};
  const ticker = normalizeTicker(quote.ticker || structured.ticker || payload?.chart?.ticker || payload?.technical?.ticker || payload?.tickers?.[0]);
  const changePct = quoteChangePct(quote);
  const bias = biasFromMove(changePct ?? quoteChange(quote));
  const technical = payload?.technical?.indicators || structured.indicators || {};
  const sections = cleanSentenceList(answer, 5);
  const price = quotePrice(quote);
  const support = firstKnownValue(technical.support, quote.day_low, quote.dayLow);
  const resistance = firstKnownValue(technical.resistance, quote.day_high, quote.dayHigh);
  const macd = technical.macd || {};
  const ema = technical.ema || {};
  const sma = technical.sma || {};
  const fib = technical.fibonacci || {};
  const golden = technical.golden_pocket || {};
  const scenario = structured.scenario || {};
  const catalystLines = (structured.sections || [])
    .flatMap((section) => Array.isArray(section?.bullets) ? section.bullets : [])
    .map(stripMarkdownCopy)
    .filter(Boolean)
    .slice(0, 5);
  const hasConfirmedPrice = price !== null && price !== undefined;
  let thesis = structured.thesis || sections[0] || `${ticker || "El activo"} queda en vigilancia con datos confirmados por backend.`;
  if (hasConfirmedPrice && /no (tiene|tengo) precio confirmado/i.test(String(thesis))) {
    thesis = `${ticker || "El activo"} tiene precio confirmado en ${money(price)}. Genesis evalúa volumen, niveles, noticias y riesgo antes de convertirlo en señal operativa.`;
  }
  const confidence = Math.max(numberOrNull(structured.confidence) ?? confidenceFromQuote(quote), hasConfirmedPrice ? 0.82 : 0);
  const volatility = firstKnownValue(technical.volatility?.annualized_pct, technical.volatility?.pct, technical.volatility);
  const convictionScore = Math.round(Math.max(0, Math.min(1, confidence || 0)) * 100);
  return {
    kind: "asset_analysis",
    ticker,
    name: assetDisplayName({ ticker, name: quote.name || payload?.name }),
    subtitle: assetSubtitle({ ticker, name: quote.name || payload?.name }) || ticker,
    thesis,
    bias,
    confidence,
    price: {
      value: price,
      change: quoteChange(quote),
      changePct,
      source: quoteSourceLabel(quote),
    },
    levels: {
      support,
      resistance,
      previousClose: firstKnownValue(quote.previous_close, quote.previousClose),
    },
    indicators: {
      rsi: technical.rsi,
      macd: macd.line,
      macdSignal: macd.signal,
      macdHistogram: macd.histogram,
      volume: firstKnownValue(quote.volume, technical.volume),
      avgVolume: firstKnownValue(quote.avgVolume, technical.avg_volume_20, technical.avgVolume),
      relativeVolume: technical.relative_volume,
      ema20: ema["20"] ?? technical.ema_20,
      ema50: ema["50"] ?? technical.ema_50,
      ema200: ema["200"] ?? technical.ema_200,
      sma20: sma["20"] ?? technical.sma_20,
      sma50: sma["50"] ?? technical.sma_50,
      sma200: sma["200"] ?? technical.sma_200,
      fib382: fib["0.382"],
      fib50: fib["0.5"],
      fib618: fib["0.618"],
      goldenFrom: golden.from,
      goldenTo: golden.to,
      volatility,
      momentum: technical.momentum,
      trend: technical.trend,
      risk: technical.risk,
      conviction: convictionScore,
      moneyFlow: payload?.whales?.answer || "Sin flujo institucional confirmado.",
    },
    scenario: {
      probable: scenario.probable || "Esperar confirmacion de precio, volumen y contexto.",
      invalidation: scenario.invalidacion || scenario.invalidation || "Perder soporte o deteriorar volumen baja conviccion.",
    },
    sections: catalystLines.length ? catalystLines : sections.slice(1),
  };
}

function briefingVisual(payload, answer = "") {
  const source = payload?.briefing || payload?.overview || {};
  const sections = cleanSentenceList(source.answer || answer, 6);
  const movers = Array.isArray(source.movers) ? source.movers : [];
  const risks = Array.isArray(source.risks) ? source.risks : [];
  const alerts = Array.isArray(source.alerts) ? source.alerts : [];
  const news = Array.isArray(source.news) ? source.news : [];
  const watch = Array.isArray(source.watch) ? source.watch : [];
  return {
    kind: "briefing",
    title: payload?.intent === "market_overview" ? "Pulso de mercado" : "Resumen del dia",
    thesis: sections[0] || "Genesis sintetiza mercado, cartera, alertas y fuentes disponibles sin inventar datos.",
    sections: sections.slice(1),
    tone: source.tone || source.summary?.tone || "neutral",
    movers,
    risks,
    alerts,
    news,
    watch,
    sourceStatus: source.source_status || {},
    confidence: 0.62,
  };
}

function newsBriefVisual(payload, answer = "") {
  const structured = payload?.structured || {};
  const overview = payload?.overview || payload?.briefing || {};
  const rows = [
    ...(Array.isArray(structured.important_news) ? structured.important_news : []),
    ...(Array.isArray(structured.latest_news) ? structured.latest_news : []),
    ...(Array.isArray(overview.news) ? overview.news : []),
  ]
    .map(normalizeNewsItemForUi)
    .filter((item) => !isInternalNewsPlaceholder(item));
  const seen = new Set();
  const cleanRows = [];
  rows.forEach((item) => {
    const id = newsItemId(item);
    if (!id || seen.has(id)) return;
    seen.add(id);
    cleanRows.push({ ...item, id });
  });
  const cachedRows = cleanRows.length ? cleanRows : newsFeedItems(appState.newsSnapshot || {});
  const thesis = cleanSentenceList(structured.summary || overview.answer || answer, 1)[0] || "";
  return {
    kind: "news_brief",
    title: "Noticias que importan",
    thesis: isWeakNewsThesis(thesis)
      ? "Estas son las noticias reales que Genesis esta vigilando por impacto, recencia y relacion con tus activos."
      : thesis || "Genesis resume titulares reales, impacto, activos afectados y que vigilar.",
    rows: cachedRows.slice(0, 5),
    metrics: {
      important: Array.isArray(structured.important_news) ? structured.important_news.length : cachedRows.length,
      latest: Array.isArray(structured.latest_news) ? structured.latest_news.length : 0,
      source: overview.source || "FMP / RSS",
    },
  };
}

function isWeakNewsThesis(value) {
  const text = cleanCopy(value || "").toLowerCase();
  return !text
    || text.includes("mercado sin confirmacion")
    || text.includes("sin titulares externos")
    || text.includes("contexto relevante")
    || text.includes("sin contexto");
}

function weatherVisual(payload, answer = "") {
  const weather = payload?.weather || {};
  const data = weather.data && typeof weather.data === "object" ? weather.data : {};
  return {
    kind: "weather",
    title: weather.city ? `Clima en ${weather.city}` : "Clima",
    thesis: stripMarkdownCopy(weather.answer || answer || "No pude confirmar clima con la fuente activa."),
    icon: weather.icon || data.icon || "☁️",
    temperature: firstKnownValue(weather.temperature, weather.temp, data.temperature, data.temp),
    feelsLike: firstKnownValue(weather.feels_like, data.feels_like),
    minTemp: firstKnownValue(weather.min_temp, data.min_temp),
    maxTemp: firstKnownValue(weather.max_temp, data.max_temp),
    condition: weather.condition || data.condition || weather.description || data.description || "",
    wind: firstKnownValue(weather.wind_speed, data.wind_speed),
    rain: firstKnownValue(weather.precipitation_probability, weather.rain_probability, data.precipitation_probability, data.rain_probability),
    updatedAt: weather.updated_at || data.updated_at || data.timestamp,
    source: weather.source || "Open-Meteo",
  };
}

function comparisonVisual(payload, answer = "") {
  const quotes = Array.isArray(payload?.quotes) ? payload.quotes : [];
  return {
    kind: "comparison",
    title: "Comparacion",
    thesis: cleanSentenceList(answer, 1)[0] || "Comparo solo datos confirmados.",
    quotes,
  };
}

function summaryVisual(title, answer = "") {
  const sections = cleanSentenceList(answer, 5);
  return {
    kind: "summary",
    title,
    thesis: sections[0] || "Genesis mantiene la lectura limpia y sin datos inventados.",
    sections: sections.slice(1),
  };
}

function imageChartVisual(payload, answer = "") {
  const structured = payload?.structured || {};
  const sections = Array.isArray(structured.sections) ? structured.sections : [];
  const lines = cleanSentenceList(structured.summary || answer, 7);
  const ticker = normalizeTicker(structured.ticker || payload?.tickers?.[0] || tickerFromText(answer) || "");
  const status = cleanCopy(payload?.status || structured.status || "");
  const confidence = numberOrNull(structured.confidence) ?? (status === "vision_ready" ? 0.78 : 0.42);
  const sectionCards = sections.length
    ? sections.map((section) => ({
        title: cleanCopy(section?.title || "Lectura"),
        bullets: Array.isArray(section?.bullets) ? section.bullets.map(stripMarkdownCopy).filter(Boolean).slice(0, 3) : [],
      })).filter((section) => section.bullets.length)
    : [
        { title: "Lectura rapida", bullets: lines.slice(0, 2) },
        { title: "Que vigilar", bullets: lines.slice(2, 5) },
      ];
  return {
    kind: "chart_image_analysis",
    title: structured.title || "Grafica analizada",
    ticker,
    thesis: lines[0] || "Genesis recibio la grafica y la separa de los datos duros del mercado.",
    status,
    confidence,
    source: payload?.source_status?.provider || "Vision Genesis",
    model: payload?.source_status?.model || "",
    policy: payload?.vision_policy || "La imagen se interpreta visualmente; precios y retornos se reconfirman con FMP.",
    sections: sectionCards,
  };
}

function generalAssistantVisual(payload, answer = "") {
  const structured = payload?.structured || {};
  const sections = Array.isArray(structured.sections) ? structured.sections : [];
  const lines = cleanSentenceList(structured.summary || answer, 5);
  return {
    kind: "general_assistant",
    title: structured.title || "Genesis",
    mode: structured.mode || "Asistente completo",
    thesis: lines[0] || "Genesis razona primero la intencion: vida diaria, memoria, mercado o activo.",
    confidence: numberOrNull(structured.confidence) ?? 0.72,
    sections: sections.length
      ? sections.map((section) => ({
          title: cleanCopy(section?.title || "Lectura"),
          bullets: Array.isArray(section?.bullets) ? section.bullets.map(stripMarkdownCopy).filter(Boolean).slice(0, 4) : [],
        })).filter((section) => section.bullets.length)
      : [
          { title: "Siguiente paso", bullets: lines.slice(1, 4) },
        ],
  };
}

function memoryDigestVisual(payload, answer = "") {
  const structured = payload?.structured || {};
  const memory = payload?.memory_summary || {};
  const counts = memory.counts || structured.metrics || {};
  const sections = Array.isArray(structured.sections) ? structured.sections : [];
  const bullets = sections.flatMap((section) => Array.isArray(section?.bullets) ? section.bullets : []);
  const summaryLines = Array.isArray(memory.summary_lines) ? memory.summary_lines : [];
  return {
    kind: "memory_digest",
    title: structured.title || (memory.ticker ? `Memoria de ${memory.ticker}` : "Memoria Genesis"),
    ticker: memory.ticker || structured.ticker || "",
    thesis: cleanSentenceList(structured.summary || answer, 1)[0] || "Genesis guarda contexto util sin exponer secretos.",
    metrics: {
      decisions: numberOrNull(counts.decisions) || 0,
      signals: numberOrNull(counts.signals) || 0,
      news: numberOrNull(counts.news) || 0,
      whales: numberOrNull(counts.whales) || 0,
      outcomes: numberOrNull(counts.outcomes) || 0,
    },
    lines: (summaryLines.length ? summaryLines : bullets).map(stripMarkdownCopy).filter(Boolean).slice(0, 6),
  };
}

function feedVisual(title, rows, answer = "") {
  return {
    kind: "feed",
    title,
    thesis: cleanSentenceList(answer, 1)[0] || "Genesis muestra solo eventos confirmados por la fuente activa.",
    rows: Array.isArray(rows) ? rows.slice(0, 4) : [],
  };
}

function alertsDigestVisual(payload, answer = "") {
  const structured = payload?.structured || {};
  const rows = structured.alerts || payload?.alerts?.items || [];
  return {
    kind: "alerts_digest",
    title: structured.title || "Alertas Genesis",
    thesis: cleanSentenceList(structured.summary || answer, 1)[0] || "Genesis prioriza alertas con precio, volumen, niveles y contexto.",
    rows: Array.isArray(rows) ? rows.slice(0, 5) : [],
    metrics: structured.metrics || {},
    sections: structured.sections || [],
  };
}

function whaleFlowVisual(payload, answer = "") {
  const structured = payload?.structured || {};
  const whales = payload?.whales || {};
  const directRows = structured.events || whales.events || whales.items || (whales.unconfirmed_watch || []).map((ticker) => ({
    ticker,
    event_type: "smart_money_estimate",
    genesis_reading: "Flujo en vigilancia sin entidad ni monto confirmado.",
  }));
  const localRows = currentWhaleRows();
  const rows = Array.isArray(directRows) && directRows.length ? directRows : localRows;
  const watchedVolume = (Array.isArray(rows) ? rows : []).reduce((sum, row) => {
    const value = numberOrNull(row?.monitored_dollar_volume ?? row?.monitoredDollarVolume ?? row?.dollar_volume ?? row?.dollarVolume);
    return sum + (value || 0);
  }, 0);
  const confirmedValue = (Array.isArray(rows) ? rows : []).reduce((sum, row) => {
    const confirmed = row?.event_type === "whale_confirmed" || row?.type === "whale_confirmed" || row?.confirmed;
    const value = confirmed ? numberOrNull(row?.confirmed_amount_usd ?? row?.confirmedAmountUsd ?? row?.amount_usd ?? row?.amountUsd) : null;
    return sum + (value || 0);
  }, 0);
  const metrics = {
    ...(whales.summary || {}),
    ...(structured.metrics || {}),
  };
  if (!numberOrNull(metrics.watched_volume) && watchedVolume) metrics.watched_volume = watchedVolume;
  if (!numberOrNull(metrics.confirmed_value) && confirmedValue) metrics.confirmed_value = confirmedValue;
  if (!numberOrNull(metrics.estimated_count) && rows.length) metrics.estimated_count = rows.length;
  return {
    kind: "whale_flow",
    title: structured.title || "Ballenas / Smart money",
    thesis: cleanSentenceList(structured.summary || whales.answer || answer, 1)[0]
      || (rows.length
        ? "Genesis detecta flujo vigilado: muestra volumen y dirección, sin venderlo como compra confirmada."
        : "No hay ballena confirmada con entidad y monto; Genesis vigila volumen y precio sin inventar comprador."),
    rows: Array.isArray(rows) ? rows.slice(0, 5) : [],
    metrics,
    sections: structured.sections || [],
  };
}

function visualResponseMarkup(visual) {
  if (!visual) return "";
  if (visual.kind === "asset_analysis") return assetAnalysisVisualMarkup(visual);
  if (visual.kind === "briefing") return briefingVisualMarkup(visual);
  if (visual.kind === "weather") return weatherVisualMarkup(visual);
  if (visual.kind === "comparison") return comparisonVisualMarkup(visual);
  if (visual.kind === "news_brief") return newsBriefVisualMarkup(visual);
  if (visual.kind === "alerts_digest") return alertsDigestVisualMarkup(visual);
  if (visual.kind === "whale_flow") return whaleFlowVisualMarkup(visual);
  if (visual.kind === "memory_digest") return memoryDigestVisualMarkup(visual);
  if (visual.kind === "general_assistant") return generalAssistantVisualMarkup(visual);
  if (visual.kind === "chart_image_analysis") return imageChartVisualMarkup(visual);
  if (visual.kind === "feed") return feedVisualMarkup(visual);
  return summaryVisualMarkup(visual);
}

function assetAnalysisVisualMarkup(visual) {
  const confidencePct = Math.round(Math.max(0, Math.min(1, visual.confidence || 0)) * 100);
  const changeTone = positiveClass(visual.price?.changePct ?? visual.price?.change);
  const rsi = numberOrNull(visual.indicators?.rsi);
  const rsiPct = rsi === null ? 50 : Math.max(0, Math.min(100, rsi));
  const relVol = numberOrNull(visual.indicators?.relativeVolume);
  const volumePressure = relVol !== null ? Math.max(8, Math.min(100, relVol * 42)) : (visual.indicators?.volume ? 42 : 18);
  const momentumScore = Math.max(12, Math.min(100, Math.abs(numberOrNull(visual.price?.changePct) || 0) * 18 + (relVol || 0) * 18 + 18));
  const directionLabel = changeTone === "up" ? "Presion compradora" : changeTone === "down" ? "Presion vendedora" : "Rango / espera";
  return `
    <section class="visual-response asset-visual tone-${visual.bias}">
      <div class="visual-hero">
        <div>
          <span class="visual-kicker">${escapeHtml(visual.subtitle || visual.ticker || "Activo")}</span>
          <strong>${escapeHtml(visual.name || visual.ticker || "Analisis")}</strong>
        </div>
        <span class="conviction-pill ${visual.bias}">${escapeHtml(biasLabel(visual.bias))}</span>
      </div>
      <p class="visual-thesis">${escapeHtml(visual.thesis)}</p>
      <div class="visual-market-strip">
        <span>
          <small>Precio confirmado</small>
          <strong class="market-number ${changeTone}">${escapeHtml(money(visual.price?.value, "Sin precio"))}</strong>
        </span>
        <span>
          <small>Cambio</small>
          <strong class="${changeTone}">${escapeHtml(formatChange(visual.price?.change, "Sin cambio"))} ${escapeHtml(formatPercent(visual.price?.changePct, "Sin dato"))}</strong>
        </span>
        <span>
          <small>Fuente</small>
          <strong>${escapeHtml(visual.price?.source || "FMP")}</strong>
        </span>
      </div>
      <div class="jarvis-asset-core ${changeTone}">
        <div class="jarvis-orb" aria-hidden="true">
          <span style="--meter:${confidencePct}%"></span>
          <b>${escapeHtml(String(confidencePct))}</b>
          <small>score</small>
        </div>
        <div class="holo-metrics">
          ${holoMeterMarkup("Dirección", directionLabel, Math.max(12, Math.min(100, Math.abs(numberOrNull(visual.price?.changePct) || 0) * 20 + 18)), changeTone)}
          ${holoMeterMarkup("Volumen", visual.indicators?.volume ? compactNumber(visual.indicators.volume) : "Sin volumen", volumePressure, relVol !== null && relVol >= 1.2 ? "up" : "flat")}
          ${holoMeterMarkup("RSI", rsi === null ? "Sin dato" : compactNumber(rsi), rsiPct, rsiPct >= 70 ? "down" : rsiPct <= 35 ? "up" : "flat")}
          ${holoMeterMarkup("Momentum", cleanCopy(visual.indicators?.momentum || "Confirmacion pendiente"), momentumScore, changeTone)}
        </div>
      </div>
      ${supportResistanceRailMarkup(visual)}
      <div class="confidence-row">
        <span>Confianza ${confidencePct}%</span>
        <i><b style="width:${confidencePct}%"></b></i>
      </div>
      <div class="visual-grid">
        ${visualMetricMarkup("Soporte", visual.levels?.support, money)}
        ${visualMetricMarkup("Resistencia", visual.levels?.resistance, money)}
        ${visualMetricMarkup("EMA 20", visual.indicators?.ema20, money)}
        ${visualMetricMarkup("EMA 50", visual.indicators?.ema50, money)}
        ${visualMetricMarkup("EMA 200", visual.indicators?.ema200, money)}
        ${visualMetricMarkup("RSI", visual.indicators?.rsi, compactNumber)}
        ${visualMetricMarkup("MACD", visual.indicators?.macd, compactNumber)}
        ${visualMetricMarkup("Volumen", visual.indicators?.volume, compactNumber)}
        ${visualMetricMarkup("Vol. rel", visual.indicators?.relativeVolume, compactNumber)}
        ${visualMetricMarkup("Fib 0.618", visual.indicators?.fib618, money)}
        ${visualMetricMarkup("Golden", firstKnownValue(visual.indicators?.goldenFrom, visual.indicators?.goldenTo), money)}
        ${visualMetricMarkup("Volatilidad", visual.indicators?.volatility, compactNumber)}
        ${visualTextMetricMarkup("Tendencia", visual.indicators?.trend)}
        ${visualTextMetricMarkup("Momentum", visual.indicators?.momentum)}
        ${visualTextMetricMarkup("Riesgo", visual.indicators?.risk)}
        ${visualTextMetricMarkup("Conviccion", `${visual.indicators?.conviction || 0}%`)}
        ${visualTextMetricMarkup("Money flow", stripMarkdownCopy(visual.indicators?.moneyFlow || "Sin dato").slice(0, 42))}
      </div>
      <div class="scenario-card">
        <strong>Escenario probable</strong>
        <p>${escapeHtml(visual.scenario?.probable || "Esperar confirmacion antes de operar.")}</p>
        <small>Invalidacion: ${escapeHtml(visual.scenario?.invalidation || "Perder soporte o deteriorar volumen.")}</small>
      </div>
      <div class="visual-sections">
        ${(visual.sections?.length ? visual.sections : ["Vigilar confirmacion de precio, volumen y riesgo antes de subir conviccion."]).slice(0, 4).map((line) => `<p>${escapeHtml(line)}</p>`).join("")}
      </div>
      ${assetVisualRelatedMarkup(visual.ticker)}
    </section>
  `;
}

function assetVisualRelatedMarkup(ticker) {
  const normalized = normalizeTicker(ticker);
  if (!normalized) return "";
  const news = assetRelatedNews(normalized).slice(0, 2);
  const alerts = assetRelatedAlerts(normalized).map(normalizeAlertRowForUi).slice(0, 2);
  const whales = assetRelatedWhales(normalized).slice(0, 2);
  if (!news.length && !alerts.length && !whales.length) return "";
  return `
    <div class="asset-jarvis-panels">
      ${news.length ? `
        <section>
          <strong>Noticias relevantes</strong>
          ${news.map((item) => `
            <button type="button" data-news-open="${escapeHtml(newsItemId(item))}">
              ${newsImageTag(item, "mini-news-image")}
              <span>${escapeHtml(newsDisplayTitle(item))}</span>
            </button>
          `).join("")}
        </section>
      ` : ""}
      ${alerts.length ? `
        <section>
          <strong>Alertas clave</strong>
          ${alerts.map((item) => `
            <button type="button" data-alert-id="${escapeHtml(alertItemId(item))}">
              <span>${escapeHtml(itemTicker(item) || normalized)}</span>
              <b>${escapeHtml(alertVisualDigest(item))}</b>
            </button>
          `).join("")}
        </section>
      ` : ""}
      ${whales.length ? `
        <section>
          <strong>Flujo smart money</strong>
          ${whales.map((item) => `
            <button type="button" data-whale-id="${escapeHtml(whaleItemId(item))}">
              <span>${escapeHtml(itemTicker(item) || normalized)}</span>
              <b>${escapeHtml(item.dollarVolume ? formatMoneyCompact(item.dollarVolume) : cleanCopy(item.event || "Flujo vigilado"))}</b>
            </button>
          `).join("")}
        </section>
      ` : ""}
    </div>
  `;
}

function holoMeterMarkup(label, value, width, tone = "flat") {
  return `
    <span class="holo-meter ${tone}">
      <small>${escapeHtml(label)}</small>
      <strong>${escapeHtml(value || "Sin dato")}</strong>
      <i><b style="width:${Math.max(6, Math.min(100, numberOrNull(width) || 12))}%"></b></i>
    </span>
  `;
}

function supportResistanceRailMarkup(visual) {
  const price = numberOrNull(visual.price?.value);
  const support = numberOrNull(visual.levels?.support);
  const resistance = numberOrNull(visual.levels?.resistance);
  if (price === null || support === null || resistance === null || resistance <= support) return "";
  const pct = Math.max(3, Math.min(97, ((price - support) / (resistance - support)) * 100));
  return `
    <div class="price-rail">
      <span>${escapeHtml(money(support, "Soporte"))}</span>
      <i><b style="left:${pct}%"></b></i>
      <span>${escapeHtml(money(resistance, "Resistencia"))}</span>
    </div>
  `;
}

function visualTextMetricMarkup(label, value) {
  return `
    <span>
      <small>${escapeHtml(label)}</small>
      <strong>${escapeHtml(cleanCopy(value || "Sin dato"))}</strong>
    </span>
  `;
}

function visualMetricMarkup(label, value, formatter) {
  return `
    <span>
      <small>${escapeHtml(label)}</small>
      <strong>${escapeHtml(numberOrNull(value) === null ? "Sin dato" : formatter(value))}</strong>
    </span>
  `;
}

function briefingVisualMarkup(visual) {
  const leaders = (visual.movers || []).filter((item) => (numberOrNull(item.daily_change_pct) || 0) > 0).slice(0, 3);
  const pressure = (visual.movers || []).filter((item) => (numberOrNull(item.daily_change_pct) || 0) < 0).slice(0, 3);
  return `
    <section class="visual-response briefing-visual">
      <div class="visual-hero">
        <div>
          <span class="visual-kicker">Genesis</span>
          <strong>${escapeHtml(visual.title)}</strong>
        </div>
        <span class="conviction-pill neutral">Lectura</span>
      </div>
      <p class="visual-thesis">${escapeHtml(visual.thesis)}</p>
      <div class="visual-grid briefing-grid">
        ${visualTextMetricMarkup("Tono", visual.tone || "Neutral")}
        ${visualTextMetricMarkup("Noticias", `${(visual.news || []).length} titulares`)}
        ${visualTextMetricMarkup("Alertas", `${(visual.alerts || []).length} eventos`)}
        ${visualTextMetricMarkup("Ballenas", (visual.sourceStatus?.whales_confirmed || 0) > 0 ? `${visual.sourceStatus.whales_confirmed} confirmadas` : "Sin entidad confirmada")}
      </div>
      <div class="market-mini-lists">
        <div>
          <strong>Lideres</strong>
          ${(leaders.length ? leaders : (visual.watch || []).slice(0, 3)).map((item) => `<span>${escapeHtml(item.ticker || item.symbol || item.reason || "Activo")} ${escapeHtml(formatPercent(item.daily_change_pct, ""))}</span>`).join("") || "<span>Sin liderazgo confirmado</span>"}
        </div>
        <div>
          <strong>Riesgos</strong>
          ${(visual.risks || []).slice(0, 3).map((item) => `<span>${escapeHtml(cleanCopy(item.text || item.title || item.reason || item))}</span>`).join("") || (pressure.length ? pressure.map((item) => `<span>${escapeHtml(item.ticker || item.symbol)} ${escapeHtml(formatPercent(item.daily_change_pct, ""))}</span>`).join("") : "<span>Sin riesgo destacado</span>")}
        </div>
      </div>
      <div class="visual-sections">
        ${(visual.sections?.length ? visual.sections : ["Sin fuente adicional confirmada; Genesis conserva cautela y usa precio, volumen y alertas disponibles."]).slice(0, 5).map((line) => `<p>${escapeHtml(line)}</p>`).join("")}
      </div>
    </section>
  `;
}

function weatherVisualMarkup(visual) {
  const range = visual.minTemp !== null && visual.minTemp !== undefined && visual.maxTemp !== null && visual.maxTemp !== undefined
    ? `${compactNumber(visual.minTemp)}-${compactNumber(visual.maxTemp)} C`
    : "Sin rango";
  return `
    <section class="visual-response weather-visual">
      <div class="visual-hero">
        <div>
          <span class="visual-kicker">${escapeHtml(visual.source || "Open-Meteo")}</span>
          <strong>${escapeHtml(visual.title)}</strong>
        </div>
        <span class="conviction-pill neutral">${escapeHtml(visual.condition || "Clima")}</span>
      </div>
      <p class="visual-thesis">${escapeHtml(visual.thesis)}</p>
      <div class="weather-hero-metric">
        <span class="weather-icon">${escapeHtml(visual.icon || "☁️")}</span>
        <div>
          <strong>${escapeHtml(visual.temperature === null || visual.temperature === undefined ? "Sin dato" : `${compactNumber(visual.temperature)} C`)}</strong>
          <small>${escapeHtml(visual.condition || "Condicion no confirmada")}</small>
        </div>
      </div>
      <div class="visual-market-strip">
        <span><small>Rango</small><strong>${escapeHtml(range)}</strong></span>
        <span><small>Lluvia</small><strong>${escapeHtml(visual.rain === null || visual.rain === undefined ? "Sin dato" : `${compactNumber(visual.rain)}%`)}</strong></span>
        <span><small>Viento</small><strong>${escapeHtml(visual.wind === null || visual.wind === undefined ? "Sin dato" : `${compactNumber(visual.wind)} km/h`)}</strong></span>
        <span><small>Actualizacion</small><strong>${escapeHtml(formatDate(visual.updatedAt))}</strong></span>
      </div>
    </section>
  `;
}

function comparisonVisualMarkup(visual) {
  return `
    <section class="visual-response comparison-visual">
      <div class="visual-hero">
        <div>
          <span class="visual-kicker">Genesis</span>
          <strong>${escapeHtml(visual.title)}</strong>
        </div>
        <span class="conviction-pill neutral">Comparacion</span>
      </div>
      <p class="visual-thesis">${escapeHtml(visual.thesis)}</p>
      <div class="visual-market-strip">
        ${visual.quotes.map((quote) => {
          const tone = positiveClass(quoteChangePct(quote) ?? quoteChange(quote));
          return `<span><small>${escapeHtml(quote.ticker || "Activo")}</small><strong class="${tone}">${escapeHtml(money(quotePrice(quote), "Sin precio"))} ${escapeHtml(formatPercent(quoteChangePct(quote), ""))}</strong></span>`;
        }).join("")}
      </div>
    </section>
  `;
}

function newsBriefVisualMarkup(visual) {
  const rows = Array.isArray(visual.rows) ? visual.rows.slice(0, 5) : [];
  return `
    <section class="visual-response feed-visual news-brief-visual">
      <div class="visual-hero">
        <div>
          <span class="visual-kicker">Noticias</span>
          <strong>${escapeHtml(visual.title || "Noticias que importan")}</strong>
        </div>
        <span class="conviction-pill neutral">${escapeHtml(`${rows.length} notas`)}</span>
      </div>
      <p class="visual-thesis">${escapeHtml(visual.thesis)}</p>
      <div class="visual-grid briefing-grid">
        ${visualTextMetricMarkup("Importantes", String(visual.metrics?.important ?? rows.length))}
        ${visualTextMetricMarkup("Ultimas", String(visual.metrics?.latest ?? 0))}
        ${visualTextMetricMarkup("Fuente", cleanCopy(visual.metrics?.source || "FMP / RSS"))}
      </div>
      <div class="visual-feed-cards news-brief-cards">
        ${(rows.length ? rows : [{ title: "Sin titular externo confirmado", summary: "Genesis mantiene lectura con precio, volumen y alertas disponibles.", impact: "neutral", category: "macro" }]).map((row) => {
          const image = newsImageForItem(row);
          const category = cleanCopy(row.category || row.placeholder_key || "macro").toLowerCase().replace(/[^a-z0-9_-]+/g, "-");
          const tone = newsImpactTone(row.impact);
          return `
            <article class="chat-news-card">
              <span class="chat-news-thumb ${image ? "" : `is-placeholder placeholder-${escapeHtml(category)}`}">
                ${newsImageTag(row)}
              </span>
              <div>
                <strong>${escapeHtml(newsDisplayTitle(row))}</strong>
                <p>${escapeHtml(cleanCopy(row.genesisTakeaway || row.genesis_takeaway || row.summary || "Lectura pendiente de confirmacion."))}</p>
                <div class="news-card-bottom">
                  <span>${escapeHtml(cleanCopy(row.source || "Fuente activa"))}</span>
                  <span class="${tone}">${escapeHtml(cleanCopy(row.impact || "Neutral"))}</span>
                </div>
              </div>
            </article>
          `;
        }).join("")}
      </div>
    </section>
  `;
}

function alertsDigestVisualMarkup(visual) {
  const rows = Array.isArray(visual.rows) ? visual.rows.slice(0, 5) : [];
  return `
    <section class="visual-response feed-visual alert-digest-visual">
      <div class="visual-hero">
        <div>
          <span class="visual-kicker">Radar Genesis</span>
          <strong>${escapeHtml(visual.title || "Alertas")}</strong>
        </div>
        <span class="conviction-pill neutral">${escapeHtml(String(visual.metrics?.total ?? rows.length))}</span>
      </div>
      <p class="visual-thesis">${escapeHtml(visual.thesis)}</p>
      <div class="visual-grid briefing-grid">
        ${visualTextMetricMarkup("Alta prioridad", String(visual.metrics?.high ?? rows.filter((row) => row.severity === "high").length))}
        ${visualTextMetricMarkup("Tecnicas", String(visual.metrics?.technical ?? rows.filter((row) => row.source === "technical").length))}
        ${visualTextMetricMarkup("Fuente", "Backend / FMP")}
      </div>
      <div class="visual-feed-cards">
        ${(rows.length ? rows : [{ title: "Sin alerta fuerte", summary: "Genesis sigue vigilando precio, volumen y niveles." }]).map((row) => `
          <article>
            <strong>${escapeHtml(cleanCopy(row.ticker || row.title || "Alerta"))}</strong>
            <p>${escapeHtml(stripMarkdownCopy(cleanCopy(row.genesis_reading || row.what_it_means || row.summary || "Evento en vigilancia.")))}</p>
            <div class="visual-market-strip compact">
              <span><small>Precio</small><strong>${escapeHtml(row.price === null || row.price === undefined ? "No aplica" : money(row.price, "No aplica"))}</strong></span>
              <span><small>Cambio</small><strong>${escapeHtml(formatPercent(row.change_pct, "Sin dato"))}</strong></span>
              <span><small>Vol. rel</small><strong>${escapeHtml(row.relative_volume ? `${compactNumber(row.relative_volume)}x` : "Sin dato")}</strong></span>
            </div>
            <div class="mini-flow-bar"><i style="width:${Math.max(12, Math.min(100, Math.abs(numberOrNull(row.change_pct || row.relative_volume || 0) || 12) * 12))}%"></i></div>
          </article>
        `).join("")}
      </div>
    </section>
  `;
}

function whaleFlowVisualMarkup(visual) {
  const rows = Array.isArray(visual.rows) ? visual.rows.slice(0, 5) : [];
  return `
    <section class="visual-response feed-visual whale-flow-visual">
      <div class="visual-hero">
        <div>
          <span class="visual-kicker">Smart money</span>
          <strong>${escapeHtml(visual.title || "Ballenas")}</strong>
        </div>
        <span class="conviction-pill neutral">${escapeHtml(cleanCopy(visual.metrics?.confidence || "vigilancia"))}</span>
      </div>
      <p class="visual-thesis">${escapeHtml(visual.thesis)}</p>
      <div class="visual-grid briefing-grid">
        ${visualTextMetricMarkup("Confirmado", visual.metrics?.confirmed_value ? money(visual.metrics.confirmed_value) : "No confirmado")}
        ${visualTextMetricMarkup("Volumen vigilado", visual.metrics?.watched_volume ? money(visual.metrics.watched_volume) : "Sin cifra")}
        ${visualTextMetricMarkup("Eventos", String((visual.metrics?.confirmed_count || 0) + (visual.metrics?.estimated_count || rows.length || 0)))}
      </div>
      <div class="visual-feed-cards">
        ${(rows.length ? rows : [{ ticker: "Mercado", genesis_reading: "Sin ballena confirmada; Genesis mantiene vigilancia de flujo." }]).map((row) => {
          const confirmed = Boolean((row.event_type === "whale_confirmed" || row.type === "whale_confirmed" || row.confirmed) && (row.entity_name || row.entity || row.confirmed_amount_usd || row.confirmedAmountUsd || row.amount_usd));
          const amount = confirmed
            ? (row.confirmed_amount_usd ?? row.confirmedAmountUsd ?? row.amount_usd ?? row.amountUsd)
            : (row.monitored_dollar_volume ?? row.monitoredDollarVolume ?? row.dollar_volume ?? row.dollarVolume);
          const relative = row.relative_volume ?? row.relativeVolume;
          const flow = row.net_flow ?? row.netFlow ?? amount;
          const reading = row.genesis_reading || row.genesis_reading_es || row.read || row.summary || row.answer;
          const source = row.source || row.provider || "market_flow";
          return `
            <article>
              <strong>${escapeHtml(cleanCopy(row.ticker || "Mercado"))} ${escapeHtml(confirmed ? "confirmado" : "estimado")}</strong>
              <p>${escapeHtml(stripMarkdownCopy(cleanCopy(reading || "Flujo en vigilancia: volumen y precio activos, sin entidad confirmada.")))}</p>
              <div class="visual-market-strip compact">
                <span><small>${confirmed ? "Monto" : "Volumen $"}</small><strong>${escapeHtml(money(amount, "No confirmado"))}</strong></span>
                <span><small>Vol. rel</small><strong>${escapeHtml(relative ? `${compactNumber(relative)}x` : "Pendiente")}</strong></span>
                <span><small>Fuente</small><strong>${escapeHtml(cleanCopy(source))}</strong></span>
              </div>
              <div class="mini-flow-bar"><i style="width:${Math.max(12, Math.min(100, Math.abs(numberOrNull(flow || 0) || 12) / 1000000))}%"></i></div>
            </article>
          `;
        }).join("")}
      </div>
    </section>
  `;
}

function feedVisualMarkup(visual) {
  const rows = Array.isArray(visual.rows) ? visual.rows.slice(0, 4) : [];
  return `
    <section class="visual-response feed-visual">
      <div class="visual-hero">
        <div>
          <span class="visual-kicker">Eventos</span>
          <strong>${escapeHtml(visual.title)}</strong>
        </div>
        <span class="conviction-pill neutral">${escapeHtml(String(visual.rows?.length || 0))}</span>
      </div>
      <p class="visual-thesis">${escapeHtml(visual.thesis)}</p>
      <div class="visual-feed-cards">
        ${(rows.length ? rows : [{ title: "Sin eventos confirmados", summary: "Genesis vigila flujo institucional, volumen anormal y acumulacion/distribucion sin inventar entidades." }]).map((row) => `
          <article>
            <strong>${escapeHtml(cleanCopy(row.ticker || row.title || row.event || "Evento"))}</strong>
            <p>${escapeHtml(stripMarkdownCopy(cleanCopy(row.genesis_reading || row.read || row.summary || row.answer || "Lectura en vigilancia.")))}</p>
            <div class="mini-flow-bar"><i style="width:${Math.max(10, Math.min(100, Math.abs(numberOrNull(row.amount_usd || row.estimated_value || row.intensity) || 18)))}%"></i></div>
            <small>${escapeHtml(cleanCopy(row.source || "Fuente activa"))} | ${escapeHtml(cleanCopy(row.confidence || "confianza baja"))}</small>
          </article>
        `).join("")}
      </div>
    </section>
  `;
}

function memoryDigestVisualMarkup(visual) {
  const metrics = visual.metrics || {};
  const lines = Array.isArray(visual.lines) ? visual.lines.slice(0, 5) : [];
  return `
    <section class="visual-response memory-visual">
      <div class="visual-hero">
        <div>
          <span class="visual-kicker">Memoria Genesis</span>
          <strong>${escapeHtml(visual.title || "Aprendizaje")}</strong>
        </div>
        <span class="conviction-pill neutral">${escapeHtml(visual.ticker || "contexto")}</span>
      </div>
      <p class="visual-thesis">${escapeHtml(visual.thesis || "Genesis guarda tesis, eventos y resultados para comparar con el tiempo.")}</p>
      <div class="memory-orbit">
        <div class="jarvis-orb memory-orb" aria-hidden="true">
          <span style="--meter:${Math.min(100, 22 + (metrics.signals || 0) * 10 + (metrics.news || 0) * 8)}%"></span>
          <b>${escapeHtml(String((metrics.decisions || 0) + (metrics.signals || 0) + (metrics.news || 0) + (metrics.whales || 0)))}</b>
          <small>eventos</small>
        </div>
        <div class="holo-metrics">
          ${holoMeterMarkup("Decisiones", String(metrics.decisions || 0), Math.max(8, (metrics.decisions || 0) * 24), "flat")}
          ${holoMeterMarkup("Senales", String(metrics.signals || 0), Math.max(8, (metrics.signals || 0) * 18), "up")}
          ${holoMeterMarkup("Noticias", String(metrics.news || 0), Math.max(8, (metrics.news || 0) * 18), "flat")}
          ${holoMeterMarkup("Ballenas", String(metrics.whales || 0), Math.max(8, (metrics.whales || 0) * 22), "flat")}
        </div>
      </div>
      <div class="visual-sections memory-lines">
        ${(lines.length ? lines : ["Aun no hay patron suficiente; Genesis empezara a guardar tesis, alertas, noticias y resultados para este activo."]).map((line) => `<p>${escapeHtml(line)}</p>`).join("")}
      </div>
      <div class="scenario-card">
        <strong>Como se usara</strong>
        <p>La memoria apoya contexto historico; los precios, volumenes y cambios siguen viniendo de FMP/backend antes de cualquier lectura operativa.</p>
      </div>
    </section>
  `;
}

function generalAssistantVisualMarkup(visual) {
  const confidencePct = Math.round(Math.max(0, Math.min(1, visual.confidence || 0)) * 100);
  const sections = Array.isArray(visual.sections) ? visual.sections.slice(0, 3) : [];
  return `
    <section class="visual-response general-assistant-visual">
      <div class="visual-hero">
        <div>
          <span class="visual-kicker">${escapeHtml(visual.mode || "Genesis")}</span>
          <strong>${escapeHtml(visual.title || "Genesis")}</strong>
        </div>
        <span class="conviction-pill bullish">Humano</span>
      </div>
      <div class="assistant-core">
        <div class="assistant-orb" style="--meter:${confidencePct}%">
          <span>G</span>
          <small>${escapeHtml(String(confidencePct))}%</small>
        </div>
      <p class="visual-thesis">${escapeHtml(visual.thesis || "Te escucho y separo conversacion cotidiana de analisis financiero.")}</p>
      </div>
      <div class="assistant-step-grid">
        ${sections.map((section) => `
          <article>
            <strong>${escapeHtml(section.title || "Paso")}</strong>
            ${(section.bullets || []).slice(0, 4).map((line) => `<p>${escapeHtml(line)}</p>`).join("")}
          </article>
        `).join("")}
      </div>
    </section>
  `;
}

function summaryVisualMarkup(visual) {
  return `
    <section class="visual-response summary-visual">
      <div class="visual-hero">
        <div>
          <span class="visual-kicker">Genesis</span>
          <strong>${escapeHtml(visual.title || "Lectura")}</strong>
        </div>
      </div>
      <p class="visual-thesis">${escapeHtml(visual.thesis || "Sin lectura suficiente.")}</p>
      ${visual.sections?.length ? `<div class="visual-sections">${visual.sections.slice(0, 4).map((line) => `<p>${escapeHtml(line)}</p>`).join("")}</div>` : ""}
    </section>
  `;
}

function imageChartVisualMarkup(visual) {
  const confidencePct = Math.round(Math.max(0, Math.min(1, visual.confidence || 0)) * 100);
  const bars = [72, 48, 86, 61, 38].map((height, index) => `<i style="height:${height}%"><b>${index + 1}</b></i>`).join("");
  return `
    <section class="visual-response chart-image-visual">
      <div class="visual-hero">
        <div>
          <span class="visual-kicker">${escapeHtml(visual.ticker || "Grafica")}</span>
          <strong>${escapeHtml(visual.title || "Analisis visual")}</strong>
        </div>
        <span class="conviction-pill neutral">${escapeHtml(`${confidencePct}%`)}</span>
      </div>
      <p class="visual-thesis">${escapeHtml(visual.thesis || "Genesis recibio la imagen y valida la lectura visual.")}</p>
      <div class="image-scan-panel" aria-hidden="true">
        <span></span>
        <div>${bars}</div>
        <small>Lectura visual + reconfirmacion FMP</small>
      </div>
      <div class="visual-market-strip">
        <span><small>Fuente</small><strong>${escapeHtml(visual.source || "Vision Genesis")}</strong></span>
        <span><small>Estado</small><strong>${escapeHtml(visual.status || "analizado")}</strong></span>
        ${visual.model ? `<span><small>Modelo</small><strong>${escapeHtml(visual.model)}</strong></span>` : ""}
      </div>
      <div class="visual-sections image-analysis-sections">
        ${(visual.sections || []).slice(0, 3).map((section) => `
          <article>
            <strong>${escapeHtml(section.title || "Lectura")}</strong>
            ${(section.bullets || []).slice(0, 3).map((line) => `<p>${escapeHtml(line)}</p>`).join("")}
          </article>
        `).join("")}
      </div>
      <small class="visual-footnote">${escapeHtml(visual.policy || "La imagen no reemplaza precios confirmados.")}</small>
    </section>
  `;
}

function chatBubbleMarkup(message) {
  const role = message.role === "user" ? "user" : "assistant";
  const text = stripMarkdownCopy(message.text);
  return `
    <article class="chat-bubble ${role}">
      ${role === "assistant" && message.visual ? visualResponseMarkup(message.visual) : `<p>${escapeHtml(text)}</p>`}
      ${message.imageName ? `<small class="chat-image-chip">Imagen: ${escapeHtml(message.imageName)}</small>` : ""}
      ${message.chart ? chartBlockMarkup(message.chart.ticker, message.chart.range, `chat:${message.id}`) : ""}
    </article>
  `;
}

function fileToDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(reader.error || new Error("No pude leer la imagen."));
    reader.readAsDataURL(file);
  });
}

function speechRecognitionConstructor() {
  if (typeof window === "undefined") return null;
  return window.SpeechRecognition || window.webkitSpeechRecognition || null;
}

function isVoiceInputSupported() {
  return Boolean(speechRecognitionConstructor());
}

function isVoiceOutputSupported() {
  return typeof window !== "undefined" && "speechSynthesis" in window && "SpeechSynthesisUtterance" in window;
}

function setGenesisVoiceStatus(status = "") {
  appState.voiceStatus = status;
  const node = document.getElementById("genesis-voice-status");
  if (node) {
    node.hidden = !status;
    node.textContent = status;
  }
}

function ensureGenesisSpeechRecognition() {
  const Recognition = speechRecognitionConstructor();
  if (!Recognition) return null;
  if (appState.voiceRecognition) return appState.voiceRecognition;
  const recognition = new Recognition();
  recognition.lang = "es-MX";
  recognition.interimResults = true;
  recognition.continuous = false;
  recognition.maxAlternatives = 1;
  recognition.onstart = () => {
    appState.voiceListening = true;
    setGenesisVoiceStatus("Genesis te escucha...");
    if (appState.activeScreen === "genesis") renderGenesisScreen();
  };
  recognition.onresult = (event) => {
    const transcript = Array.from(event.results || [])
      .map((result) => result?.[0]?.transcript || "")
      .join(" ")
      .trim();
    const input = document.getElementById("genesis-chat-input");
    if (input && transcript) input.value = transcript;
    if (transcript) setGenesisVoiceStatus(`Escuchando: ${transcript}`);
    const lastResult = event.results?.[event.results.length - 1];
    if (lastResult?.isFinal && transcript) {
      appState.voiceListening = false;
      setGenesisVoiceStatus("Enviando voz a Genesis...");
      document.getElementById("genesis-chat-form")?.requestSubmit();
    }
  };
  recognition.onerror = (event) => {
    appState.voiceListening = false;
    setGenesisVoiceStatus("");
    const error = cleanCopy(event?.error || "voz no disponible");
    toast(`No pude escuchar bien: ${error}`, "error");
    if (appState.activeScreen === "genesis") renderGenesisScreen();
  };
  recognition.onend = () => {
    appState.voiceListening = false;
    if (appState.voiceStatus === "Genesis te escucha...") setGenesisVoiceStatus("");
    if (appState.activeScreen === "genesis") renderGenesisScreen();
  };
  appState.voiceRecognition = recognition;
  return recognition;
}

function stopGenesisSpeech() {
  if (!isVoiceOutputSupported()) return;
  window.speechSynthesis.cancel();
  appState.voiceSpeaking = false;
  if (appState.voiceStatus === "Genesis respondiendo por voz...") setGenesisVoiceStatus("");
}

function readableVoiceText(message = {}) {
  const text = stripMarkdownCopy(message.text || "");
  return text
    .replace(/\s+/g, " ")
    .replace(/\$([0-9,.]+)/g, "$1 dolares")
    .slice(0, 780)
    .trim();
}

function speakGenesisReply(message = {}) {
  if (!appState.voiceMode || !isVoiceOutputSupported()) return;
  const text = readableVoiceText(message);
  if (!text) return;
  window.speechSynthesis.cancel();
  const utterance = new SpeechSynthesisUtterance(text);
  utterance.lang = "es-MX";
  utterance.rate = 1;
  utterance.pitch = 0.94;
  utterance.volume = 1;
  utterance.onstart = () => {
    appState.voiceSpeaking = true;
    setGenesisVoiceStatus("Genesis respondiendo por voz...");
    if (appState.activeScreen === "genesis") renderGenesisScreen();
  };
  utterance.onend = () => {
    appState.voiceSpeaking = false;
    if (appState.voiceStatus === "Genesis respondiendo por voz...") setGenesisVoiceStatus("");
    if (appState.activeScreen === "genesis") renderGenesisScreen();
  };
  utterance.onerror = () => {
    appState.voiceSpeaking = false;
    setGenesisVoiceStatus("");
  };
  window.speechSynthesis.speak(utterance);
}

function pushGenesisAssistantMessage(message, options = {}) {
  appState.chatMessages.push(message);
  const shouldSpeak = options.speak ?? appState.voiceMode;
  if (shouldSpeak) speakGenesisReply(message);
  return message;
}

function toggleGenesisVoiceInput() {
  if (!isVoiceInputSupported()) {
    toast("Tu navegador no permite dictado por voz aqui. Puedes seguir escribiendo a Genesis.", "error");
    return;
  }
  appState.voiceMode = true;
  if (appState.voiceListening) {
    appState.voiceRecognition?.stop();
    appState.voiceListening = false;
    setGenesisVoiceStatus("");
    renderGenesisScreen();
    return;
  }
  stopGenesisSpeech();
  const recognition = ensureGenesisSpeechRecognition();
  try {
    recognition.start();
  } catch (error) {
    appState.voiceListening = false;
    setGenesisVoiceStatus("");
    toast("Genesis ya estaba escuchando. Intenta de nuevo en un segundo.", "info");
  }
}

async function submitGenesisQuestion(event) {
  event.preventDefault();
  const input = document.getElementById("genesis-chat-input");
  const imageInput = document.getElementById("genesis-image-input");
  const question = String(input?.value || "").trim();
  const imageFile = imageInput?.files?.[0] || null;
  if (!question && !imageFile) return;
  input.value = "";
  if (imageInput) imageInput.value = "";
  const attachmentLabel = document.getElementById("genesis-attachment-name");
  if (attachmentLabel) {
    attachmentLabel.hidden = true;
    attachmentLabel.textContent = "";
  }
  appState.chatMessages.push({ id: nextMessageId(), role: "user", text: question || "Analiza esta grafica.", imageName: imageFile?.name || "" });
  renderGenesisScreen();
  if (imageFile) {
    try {
      const dataUrl = await fileToDataUrl(imageFile);
      const payload = await postJson("/api/genesis/analyze-image", {
        message: question,
        question: question || "Analiza esta grafica financiera.",
        conversation_id: appState.currentConversationId,
        image_data: dataUrl,
        mime_type: imageFile.type || "image/png",
        image: {
          name: imageFile.name,
          type: imageFile.type,
          size: imageFile.size,
          data_url: dataUrl,
        },
      }, { timeoutMs: 60000, attempts: 1 });
      pushGenesisAssistantMessage(genesisAssistantMessageFromPayload(
        { ...payload, intent: payload.intent || "image_chart_analysis" },
        payload.answer || "Recibi la imagen."
      ));
    } catch (error) {
      pushGenesisAssistantMessage({ id: nextMessageId(), role: "assistant", text: `No pude analizar la imagen: ${cleanCopy(error.message)}` });
    }
    renderGenesisScreen();
    return;
  }
  const chartIntent = chartIntentFromText(question);
  if (chartIntent) {
    let routed = null;
    try {
      routed = await postJson("/api/genesis/ask", { message: question, context: appState.activeScreen, conversation_id: appState.currentConversationId });
      if (routed?.chart?.ticker) chartIntent.ticker = routed.chart.ticker;
    } catch (error) {
      routed = null;
    }
    const message = {
      id: nextMessageId(),
      role: "assistant",
      text: stripMarkdownCopy(routed?.answer || `Cargo velas japonesas de ${chartIntent.ticker}.`),
      visual: routed ? genesisVisualFromPayload(routed, routed.answer || "") : assetAnalysisVisual({ intent: "chart_request", chart: chartIntent, quote: { ticker: chartIntent.ticker } }, ""),
      chart: { ticker: chartIntent.ticker, range: chartIntent.range },
    };
    appState.chatMessages.push(message);
    renderGenesisScreen();
    const payload = await loadChartSeries(chartIntent.ticker, chartIntent.range);
    message.text = chartReading(payload);
    speakGenesisReply(message);
    renderGenesisScreen();
    return;
  }
  const normalizedQuestion = question.toLowerCase();
  if (normalizedQuestion.includes("noticia")) {
    await loadNews({ silent: true }).catch(() => null);
  }
  if (normalizedQuestion.includes("alert")) {
    await loadAlerts().catch(() => null);
  }
  if (normalizedQuestion.includes("ballena") || normalizedQuestion.includes("smart money")) {
    await loadWhalesData().catch(() => null);
  }
  try {
    const payload = await postJson(
      "/api/genesis/ask",
      { message: question, context: appState.activeScreen, conversation_id: appState.currentConversationId },
      { timeoutMs: 9000, attempts: 1, localOnly: true }
    );
    const correctedPayload = await correctGenesisIntentPayload(payload, question);
    const enrichedPayload = await enrichGenesisPayloadWithLocalQuote(correctedPayload, question);
    const message = genesisAssistantMessageFromPayload(enrichedPayload, question);
    pushGenesisAssistantMessage(message);
    renderGenesisScreen();
    if (message.chart) {
      loadChartSeries(message.chart.ticker, message.chart.range).then(() => {
        if (appState.activeScreen === "genesis") renderGenesisScreen();
      });
    }
    return;
  } catch (error) {
    const localAsset = await localAssetFallbackMessage(question, error);
    if (localAsset) {
      pushGenesisAssistantMessage(localAsset);
      renderGenesisScreen();
      return;
    }
    try {
      const payload = await getJson(`/api/dashboard/genesis?q=${encodeURIComponent(question)}&context=${encodeURIComponent(appState.activeScreen)}&ticker=&panel_context=`);
      const correctedPayload = await correctGenesisIntentPayload(payload, question);
      pushGenesisAssistantMessage(genesisAssistantMessageFromPayload(await enrichGenesisPayloadWithLocalQuote(correctedPayload, question), question));
    } catch (fallbackError) {
      pushGenesisAssistantMessage(offlineGenesisFallback(question, fallbackError));
    }
  }
  renderGenesisScreen();
}

async function enrichGenesisPayloadWithLocalQuote(payload = {}, question = "") {
  if (isMarketOverviewQuestion(question)) return payload;
  if (isNewsQuestion(question) || isNewsPayload(payload)) return payload;
  if (isWhaleQuestion(question) || isWhalePayload(payload)) return payload;
  const responseType = String(payload?.response_type || "");
  const intent = String(payload?.intent || "");
  if (!(["asset_analysis", "chart_analysis"].includes(responseType) || ["ticker_analysis", "technical_indicators", "chart_request"].includes(intent))) {
    return payload;
  }
  const ticker = normalizeTicker(payload?.quote?.ticker || payload?.tickers?.[0] || payload?.chart?.ticker || tickerFromText(question));
  if (!ticker || quotePrice(payload?.quote || {}) !== null) return payload;
  let asset = findAsset(ticker);
  if (!asset) {
    try {
      const search = await getJson(`/api/dashboard/market/search?q=${encodeURIComponent(ticker)}`);
      const results = Array.isArray(search.results) ? search.results : [];
      asset = results.find((item) => itemTicker(item) === ticker) || results[0] || null;
      if (asset) {
        appState.marketSearchResults.tracking = [asset, ...appState.marketSearchResults.tracking.filter((item) => itemTicker(item) !== itemTicker(asset))].slice(0, 8);
      }
    } catch (_) {
      asset = null;
    }
  }
  if (!asset || itemPrice(asset) === null) return payload;
  const chartPayload = await loadChartSeries(itemTicker(asset) || ticker, "1Y").catch(() => null);
  return {
    ...payload,
    quote: {
      ...(payload.quote || {}),
      ...quoteFromAsset(asset),
    },
    technical: {
      ...(payload.technical || {}),
      ticker: itemTicker(asset) || ticker,
      indicators: {
        ...((payload.technical || {}).indicators || {}),
        ...(chartPayload?.indicators || {}),
      },
    },
    chart: payload.chart || { ticker: itemTicker(asset) || ticker, range: "1Y" },
  };
}

async function localAssetFallbackMessage(question, error) {
  if (isMarketOverviewQuestion(question)) return null;
  if (isNewsQuestion(question)) {
    return genesisAssistantMessageFromPayload(forcedNewsPayloadFromState(question), question);
  }
  if (isWhaleQuestion(question)) {
    return genesisAssistantMessageFromPayload(forcedWhalePayloadFromState(question), question);
  }
  const ticker = tickerFromText(question);
  if (!ticker) return null;
  let asset = findAsset(ticker);
  if (!asset) {
    try {
      const payload = await getJson(`/api/dashboard/market/search?q=${encodeURIComponent(ticker)}`);
      const results = Array.isArray(payload.results) ? payload.results : [];
      asset = results.find((item) => itemTicker(item) === ticker) || results[0] || null;
      if (asset) {
        appState.marketSearchResults.tracking = [asset, ...appState.marketSearchResults.tracking.filter((item) => itemTicker(item) !== itemTicker(asset))].slice(0, 8);
      }
    } catch (_) {
      asset = null;
    }
  }
  if (!asset) return null;
  const normalized = itemTicker(asset) || ticker;
  const chartPayload = await loadChartSeries(normalized, "1Y").catch(() => null);
  const quote = quoteFromAsset(asset);
  const display = getAssetDisplayName(asset);
  const move = `${formatChange(quote.change, "Sin cambio")} ${formatPercent(quote.changesPercentage, "")}`.trim();
  const text = `${display.displayName}: ${money(quote.current_price, "precio pendiente")} ${move}. Lectura rapida con fuente live/cache; ${networkErrorMessage(error).toLowerCase()}`;
  return {
    id: nextMessageId(),
    role: "assistant",
    text,
    visual: assetAnalysisVisual({
      intent: "ticker_analysis",
      response_type: "asset_analysis",
      quote,
      technical: { ticker: normalized, indicators: chartPayload?.indicators || {} },
      chart: { ticker: normalized, range: "1Y" },
    }, text),
    chart: { ticker: normalized, range: "1Y" },
  };
}

function quoteFromAsset(asset = {}) {
  const ticker = itemTicker(asset);
  return {
    ticker,
    name: asset.name || asset.companyName || asset.display_name || assetDisplayName(asset),
    current_price: itemPrice(asset),
    formatted_price: money(itemPrice(asset), ""),
    change: itemDailyUsd(asset),
    changesPercentage: itemDailyPct(asset),
    previous_close: asset.previous_close ?? asset.previousClose,
    day_low: asset.day_low ?? asset.dayLow,
    day_high: asset.day_high ?? asset.dayHigh,
    volume: asset.volume,
    source_label: priceSourceLabel(asset),
  };
}

function offlineGenesisFallback(question, error) {
  const normalized = String(question || "").toLowerCase();
  if (isMarketOverviewQuestion(question)) {
    const movers = marketPulseRows().slice(0, 6);
    const alerts = currentAlertRows().slice(0, 6);
    const news = newsFeedItems(appState.newsSnapshot || {}).slice(0, 5);
    const answer = "Te doy lectura de mercado con lo que ya esta cargado: indices, activos vigilados, alertas y noticias. La API no debe convertir esta pregunta en un ticker.";
    return {
      id: nextMessageId(),
      role: "assistant",
      text: answer,
      visual: briefingVisual({
        intent: "market_overview",
        response_type: "market_summary",
        overview: {
          answer,
          tone: "vigilancia",
          movers,
          alerts,
          news,
          risks: alerts.slice(0, 3),
          watch: movers,
          source_status: { fallback: true },
        },
      }, answer),
    };
  }
  if (normalized.includes("noticia")) {
    const visual = newsBriefVisual({ structured: { summary: "" } }, "");
    return {
      id: nextMessageId(),
      role: "assistant",
      text: "La API conversacional no respondio, pero te dejo las noticias cargadas en la app mientras reintento la conexion.",
      visual,
    };
  }
  if (normalized.includes("alert")) {
    return {
      id: nextMessageId(),
      role: "assistant",
      text: "La API conversacional no respondio, pero el radar local sigue activo con las alertas cargadas.",
      visual: alertsDigestVisual({ structured: { alerts: currentAlertRows(), summary: "Alertas derivadas desde el estado actual." } }, ""),
    };
  }
  if (normalized.includes("ballena") || normalized.includes("smart money")) {
    return {
      id: nextMessageId(),
      role: "assistant",
      text: "La API conversacional no respondio; muestro la lectura local de ballenas y flujo estimado.",
      visual: whaleFlowVisual({ structured: { events: currentWhaleRows(), summary: "Flujos cargados desde el radar local." } }, ""),
    };
  }
  return {
    id: nextMessageId(),
    role: "assistant",
    text: `Genesis no pudo conectar con la API ahora. ${networkErrorMessage(error)} Reintenta en unos segundos; la app sigue mostrando precios y feeds cargados.`,
  };
}

async function loadGenesisMemoryHistory() {
  try {
    const payload = await getJson("/api/genesis/memory/recent?limit=12");
    appState.chatConversations = Array.isArray(payload.conversations) ? payload.conversations : [];
    if (appState.activeScreen === "genesis") renderGenesisScreen();
  } catch (error) {
    // Memory is additive; the chat stays usable if local dev has no store yet.
  }
}

async function openGenesisConversation(conversationId) {
  const id = String(conversationId || "").trim();
  if (!id) return;
  try {
    const payload = await getJson(`/api/genesis/memory/recent?limit=40&conversation_id=${encodeURIComponent(id)}`);
    appState.currentConversationId = id;
    appState.chatConversations = Array.isArray(payload.conversations) ? payload.conversations : appState.chatConversations;
    const messages = Array.isArray(payload.messages) ? payload.messages : [];
    const cleanMessages = messages
      .filter((item) => ["user", "assistant"].includes(String(item.role || "")) && String(item.content || "").trim())
      .slice(-40)
      .map((item, index) => ({
        id: `memory-${index}-${Date.now()}`,
        role: item.role === "user" ? "user" : "assistant",
        text: item.content,
      }));
    appState.chatMessages = cleanMessages.length ? cleanMessages : [initialChatMessage()];
    appState.chatHistoryOpen = false;
    if (appState.activeScreen === "genesis") renderGenesisScreen();
  } catch (error) {
    toast(`No pude abrir ese chat. ${networkErrorMessage(error)}`, "error");
  }
}

async function loadNews(options = {}) {
  if (appState.newsLoading) return appState.newsSnapshot;
  appState.newsLoading = true;
  setLiveRefreshIndicator(true);
  if (!options.silent) renderNewsScreen();
  try {
    const newsUrl = options.force ? `/api/dashboard/news?refresh=${Date.now()}` : "/api/dashboard/news";
    const [newsResult, alertsResult, whalesResult] = await Promise.allSettled([
      getJson(newsUrl),
      getJson("/api/dashboard/alerts"),
      loadWhalesData(),
    ]);
    if (alertsResult.status === "fulfilled") appState.alertsSnapshot = alertsResult.value;
    appState.newsSnapshot = {
      news: newsResult.status === "fulfilled" ? newsResult.value : null,
      errors: [
        newsResult.status === "rejected" ? newsResult.reason?.message : "",
        alertsResult.status === "rejected" ? alertsResult.reason?.message : "",
        whalesResult.status === "rejected" ? whalesResult.reason?.message : "",
      ].filter(Boolean),
      loadedAt: new Date().toISOString(),
    };
  } finally {
    appState.newsLoading = false;
    setLiveRefreshIndicator(false);
    renderNewsScreen();
  }
  return appState.newsSnapshot;
}

function renderNewsScreen() {
  const root = document.getElementById("view-news");
  if (!root) return;
  const snapshot = appState.newsSnapshot || {};
  const newsItems = newsFeedItems(snapshot);
  indexNewsItems(newsItems);
  const activeFeed = newsItemsForActiveFilter(snapshot, newsItems);
  const feedConfig = newsSectionConfig(appState.newsFilter || "important", activeFeed.length);
  root.innerHTML = `
    <section class="screen-stack news-screen investing-feed-screen news-only-screen">
      ${premiumScreenHeader(
        "Noticias",
        "Lo que mueve los mercados, interpretado por IA.",
        `<button type="button" class="icon-action pulse-action ${appState.newsLoading ? "is-loading" : ""}" data-news-refresh aria-label="${appState.newsLoading ? "Actualizando noticias" : "Actualizar noticias"}" aria-busy="${appState.newsLoading ? "true" : "false"}">${iconSvg("refresh")}</button>`
      )}
      <div class="news-toolbar">
        <section class="feed-tabs investing-tabs" aria-label="Filtros de noticias">
          ${newsFilterButtonMarkup("important", "Importantes")}
          ${newsFilterButtonMarkup("latest", "Ultimas")}
          ${newsFilterButtonMarkup("mine", "Mis activos")}
          ${newsFilterButtonMarkup("global", "Global")}
        </section>
      </div>
      <section class="news-section investing-section">
        <div class="section-heading">
          <strong>${escapeHtml(feedConfig.title)}</strong>
          <small>${escapeHtml(feedConfig.countLabel)}</small>
        </div>
        <div class="news-feed important-news-feed investing-news-list">
          ${appState.newsLoading ? newsLoadingMarkup("Actualizando noticias") : ""}
          ${activeFeed.length ? activeFeed.map(newsCardMarkup).join("") : emptyStateMarkup(feedConfig.emptyTitle, feedConfig.emptyBody)}
        </div>
      </section>
    </section>
  `;
}

function scrollNewsScreenToTop() {
  scrollScreenElementToTop(document.getElementById("view-news"));
}

function scrollScreenElementToTop(root, behavior = "smooth") {
  if (!root) return;
  if (typeof root.scrollTo === "function") {
    root.scrollTo({ top: 0, behavior });
    return;
  }
  root.scrollTop = 0;
}

function scrollActiveScreenToTop(screen = appState.activeScreen) {
  requestAnimationFrame(() => {
    const root = document.getElementById(screenId(screen));
    scrollScreenElementToTop(root);
    if (screen === "genesis") {
      scrollScreenElementToTop(document.getElementById("genesis-thread"));
    }
  });
}

function sourceStatusChips(status = {}) {
  const fmp = status.fmp_market_news || status.fmp || {};
  const rss = status.rss_news || {};
  const chips = [
    ["FMP", fmp.status || (fmp.count ? "ok" : "pendiente")],
    ["RSS", rss.status || (rss.count || rss.last_fetch_count ? "ok" : "pendiente")],
  ];
  return chips.map(([label, value]) => {
    const tone = String(value).toLowerCase().includes("ok") ? "up" : String(value).toLowerCase().includes("empty") ? "flat" : "flat";
    return `<span class="${tone}">${escapeHtml(label)} ${escapeHtml(cleanCopy(value || "pendiente"))}</span>`;
  }).join("");
}

function newsHeaderSummary(value) {
  const clean = stripMarkdownCopy(value || "").trim();
  if (!clean || isInternalNewsPlaceholder({ title: clean })) {
    return "Feed financiero reciente ordenado por impacto, recencia y activos en foco.";
  }
  if (clean.toLowerCase().includes("sin contexto macro activo")) {
    return "Feed financiero reciente ordenado por impacto, recencia y activos en foco.";
  }
  return clean.slice(0, 150);
}

function newsFeedItems(snapshot = {}) {
  const newsSnapshot = snapshot.news || {};
  const directNews = [
    ...(Array.isArray(newsSnapshot.important) ? newsSnapshot.important : []),
    ...(Array.isArray(newsSnapshot.latest) ? newsSnapshot.latest : []),
    ...(Array.isArray(newsSnapshot.items) ? newsSnapshot.items : []),
  ];
  const items = [
    ...directNews.map(normalizeNewsItemForUi),
  ].filter((item) => String(item.title || "").trim());

  if (!items.length) {
    return [];
  }

  const seenTitles = new Set();
  const filtered = items.filter((item) => {
    const key = cleanCopy(item.title || "").toLowerCase().replace(/\s+/g, " ").trim();
    if (!key || seenTitles.has(key)) return false;
    if (isInternalNewsPlaceholder(item) && items.some((candidate) => !isInternalNewsPlaceholder(candidate))) return false;
    seenTitles.add(key);
    return true;
  });
  return filtered.filter((item) => !isInternalNewsPlaceholder(item)).slice(0, 16);
}

function newsLoadingMarkup(label = "Actualizando") {
  return `
    <div class="news-refresh-state" aria-live="polite">
      <span class="refresh-spinner">${iconSvg("refresh")}</span>
      <small>${escapeHtml(label)}</small>
    </div>
  `;
}

function newsFilterButtonMarkup(filter, label) {
  return `<button type="button" class="${appState.newsFilter === filter ? "is-active" : ""}" data-news-filter="${escapeHtml(filter)}">${escapeHtml(label)}</button>`;
}

function newsItemsForActiveFilter(snapshot = {}, allItems = []) {
  const filter = appState.newsFilter || "important";
  const sectionItems = newsItemsFromSnapshotSection(snapshot, filter);
  const source = sectionItems.length ? sectionItems : allItems;
  return uniqueNewsItems(filteredNewsItems(source, filter)).slice(0, filter === "latest" ? 14 : 10);
}

function newsItemsFromSnapshotSection(snapshot = {}, section = "important") {
  const newsSnapshot = snapshot.news || {};
  const sections = newsSnapshot.sections || {};
  const aliases = {
    important: ["important", "important_news", "influential"],
    latest: ["latest", "latest_news"],
    mine: ["mine", "my_assets", "watchlist"],
    global: ["global", "market", "macro"],
  };
  const keys = aliases[section] || [section];
  const rows = keys.flatMap((key) => (Array.isArray(sections[key]) ? sections[key] : []));
  if (!rows.length) return [];
  return uniqueNewsItems(rows.map(normalizeNewsItemForUi).filter((item) => item.title && !isInternalNewsPlaceholder(item)));
}

function newsSectionConfig(filter = "important", count = 0) {
  const configs = {
    important: {
      title: "Importantes / influyentes",
      countLabel: `${count} catalizadores`,
      emptyTitle: "Sin catalizadores importantes ahora.",
      emptyBody: "Genesis no mezcla ultimas o globales aqui; usa Actualizar para pedir FMP/RSS otra lectura live.",
    },
    latest: {
      title: "Ultimas noticias",
      countLabel: "24h / 7d / 30d",
      emptyTitle: "Sin ultimas noticias cargadas.",
      emptyBody: "Genesis espera titulares recientes de FMP/RSS sin rellenar con alertas ni ballenas.",
    },
    mine: {
      title: "Mis activos",
      countLabel: `${count} notas ligadas`,
      emptyTitle: "Sin noticias directas de tus activos.",
      emptyBody: "No mezclo noticias globales en este filtro; cambia a Global o actualiza el feed.",
    },
    global: {
      title: "Global",
      countLabel: `${count} notas macro`,
      emptyTitle: "Sin noticias globales confirmadas.",
      emptyBody: "Genesis espera cobertura macro/mercado desde FMP/RSS.",
    },
  };
  return configs[filter] || configs.important;
}

function filteredNewsItems(items = [], filter = appState.newsFilter || "important") {
  const rows = uniqueNewsItems(items).filter((item) => !isInternalNewsPlaceholder(item));
  if (filter === "mine") {
    const focus = new Set(currentFocusAssets().map(itemTicker).filter(Boolean));
    return rows.filter((item) => newsItemTickers(item).some((ticker) => focus.has(normalizeTicker(ticker))));
  }
  if (filter === "global") {
    const focus = new Set(currentFocusAssets().map(itemTicker).filter(Boolean));
    const globalCategories = new Set(["macro", "market", "geopolitics", "commodity", "crypto"]);
    return rows.filter((item) => {
      const tickers = newsItemTickers(item);
      const touchesMine = tickers.some((ticker) => focus.has(normalizeTicker(ticker)));
      const category = cleanCopy(item.category || "").toLowerCase();
      return !touchesMine && (globalCategories.has(category) || !tickers.length);
    });
  }
  if (filter === "latest") return latestNewsItems(rows);
  return importantNewsItems(rows, { strict: true });
}

function newsItemTickers(item = {}) {
  const values = [
    ...(Array.isArray(item.assets) ? item.assets : []),
    ...(Array.isArray(item.tickers) ? item.tickers : []),
    ...(Array.isArray(item.tickersAffected) ? item.tickersAffected : []),
    ...(Array.isArray(item.tickers_affected) ? item.tickers_affected : []),
  ];
  return Array.from(new Set(values.map(normalizeTicker).filter(Boolean)));
}

function uniqueNewsItems(items = []) {
  const seen = new Set();
  const output = [];
  (items || []).forEach((item) => {
    if (!item) return;
    const key = newsItemId(item) || cleanCopy(item.title || "").toLowerCase();
    if (!key || seen.has(key)) return;
    seen.add(key);
    output.push(item);
  });
  return output;
}

function normalizeNewsItemForUi(item = {}) {
  return {
    id: item.id,
    category: item.category || item.placeholder_key || "Mercado",
    title: spanishUiCopy(item.title_es || item.title || item.headline || "Titular de mercado"),
    originalTitle: item.original_title || item.headline || item.title || "",
    source: item.source || item.site || item.publisher || "Fuente activa",
    time: item.published_at || item.publishedDate || item.date,
    relative_time: item.relative_time || item.relativeTime || "",
    impact: item.impact || item.sentiment || "Neutral",
    summary: spanishUiCopy(item.summary_es || item.summary || item.text || item.title_es || item.title || "Titular confirmado."),
    assets: item.tickers || item.assets || [item.symbol || item.ticker].filter(Boolean),
    imageUrl: item.image_url || item.thumbnail_url || item.thumbnail || item.image || "",
    url: item.url || item.link || "",
    genesisTakeaway: spanishUiCopy(item.genesis_takeaway_es || item.genesis_takeaway || ""),
    whyItMatters: spanishUiCopy(item.why_it_matters_es || item.why_it_matters || ""),
    confidence: item.confidence || "media",
    isImportant: Boolean(item.is_important),
    isLatest: Boolean(item.is_latest),
    recencyScore: item.recency_score,
    relevanceScore: item.relevance_score,
    risk: item.risk,
    watch: spanishUiCopy(item.what_to_watch_es || item.watch || ""),
    watchPoints: (item.watch_points || item.watchPoints || (item.what_to_watch_es ? [item.what_to_watch_es] : [])).map(spanishUiCopy),
    tickersAffected: item.tickers_affected || item.tickers || item.assets || [],
  };
}

function spanishUiCopy(value) {
  let text = humanizeInternalTickerText(cleanCopy(value || ""));
  if (!text) return "";
  const replacements = [
    [/The Minister of Finance of Chile Jorge Quiroz Rings the Nasdaq Stock Market Closing Bell - Nasdaq/gi, "El ministro de Finanzas de Chile Jorge Quiroz toca la campana de cierre del Nasdaq"],
    [/The Minister of Finance of Chile Jorge Quiroz Rings the Nasdaq Stock mercado Closing Bell Nasdaq/gi, "El ministro de Finanzas de Chile Jorge Quiroz toca la campana de cierre del Nasdaq"],
    [/The Minister of Finance of Chile Jorge Quiroz.*Closing Bell.*Nasdaq/gi, "El ministro de Finanzas de Chile Jorge Quiroz toca la campana de cierre del Nasdaq"],
    [/\bWhy Is\b/gi, "Por que"],
    [/\bWhy\b/gi, "Por que"],
    [/\bToday\b/gi, "hoy"],
    [/\bFutures\b/gi, "futuros"],
    [/\bHits? Records?\b/gi, "marca maximos"],
    [/\bOil Prices?\b/gi, "precios del petroleo"],
    [/\bCrude Oil\b/gi, "petroleo crudo"],
    [/\bBitcoin\b/gi, "Bitcoin"],
    [/\bNasdaq\b/gi, "Nasdaq"],
    [/\bStock Market Closing Bell\b/gi, "campana de cierre del mercado accionario"],
    [/\bClosing Bell\b/gi, "campana de cierre"],
    [/\bStock market today\b/gi, "Mercado accionario hoy"],
    [/\bstock market\b/gi, "mercado accionario"],
    [/\bmarket\b/gi, "mercado"],
    [/\bstocks\b/gi, "acciones"],
    [/\bstock\b/gi, "accion"],
    [/\binvestors\b/gi, "inversionistas"],
    [/\btraders\b/gi, "operadores"],
    [/\brisk\b/gi, "riesgo"],
    [/\bwhy it matters\b/gi, "por que importa"],
    [/\bwhat to watch\b/gi, "que vigilar"],
    [/\bclosing\b/gi, "cierre"],
  ];
  replacements.forEach(([pattern, replacement]) => {
    text = text.replace(pattern, replacement);
  });
  return text.replace(/\s+/g, " ").trim();
}

function isInternalNewsPlaceholder(item) {
  const title = cleanCopy(item?.title || "").toLowerCase();
  return (
    title.includes("contexto pendiente")
    || title.includes("sin contexto")
    || title.includes("genesis mantiene vigilancia")
    || title.includes("briefing genesis listo")
    || title.includes("contexto relevante")
    || title.includes("weekly market commentary")
  );
}

function importantNewsItems(items, options = {}) {
  const strict = Boolean(options.strict);
  const important = items
    .filter((item) => item.isImportant || item.is_important || newsImpactTone(item.impact) !== "flat" || (numberOrNull(item.relevanceScore ?? item.relevance_score) || 0) >= 2)
    .sort((a, b) => (numberOrNull(b.relevanceScore ?? b.relevance_score) || 0) - (numberOrNull(a.relevanceScore ?? a.relevance_score) || 0));
  if (strict) return important.slice(0, 5);
  return (important.length ? important : items.slice(0, 3)).slice(0, 5);
}

function latestNewsItems(items) {
  return [...items]
    .sort((a, b) => new Date(b.time || b.published_at || 0).getTime() - new Date(a.time || a.published_at || 0).getTime())
    .slice(0, 12);
}

function currentFocusAssets() {
  const rows = [...(appState.paperPositions || []), ...(appState.trackingItems || [])];
  const seen = new Set();
  return rows.filter((item) => {
    const ticker = itemTicker(item);
    if (!ticker || seen.has(ticker)) return false;
    seen.add(ticker);
    return true;
  });
}

function newsAffectedAssets(snapshot = {}) {
  const macro = snapshot.macro?.macro || {};
  const briefingData = snapshot.briefing?.briefing || snapshot.briefing?.overview || {};
  const briefingAssets = Array.isArray(briefingData.watch) ? briefingData.watch.map((item) => item?.ticker || item?.symbol) : [];
  const assets = [...(macro.high_risk_tickers || []), ...(macro.sensitive_tickers || []), ...briefingAssets, ...currentFocusAssets().map(itemTicker)]
    .map(normalizeTicker)
    .filter(Boolean);
  return Array.from(new Set(assets)).slice(0, 4).join(", ");
}

function newsImpactTone(value) {
  const text = String(value || "").toLowerCase();
  if (text.includes("alcista") || text.includes("bull") || text.includes("positivo") || text.includes("info")) return "up";
  if (text.includes("bajista") || text.includes("bear") || text.includes("negativo") || text.includes("risk") || text.includes("error")) return "down";
  return "flat";
}

function newsCategoryPhotoQuery(item = {}) {
  const raw = `${item.title || ""} ${item.originalTitle || ""} ${item.category || ""} ${(item.assets || []).join(" ")}`.toLowerCase();
  if (raw.includes("trump") || raw.includes("iran") || raw.includes("geopolit")) return "politics,world,news";
  if (raw.includes("bitcoin") || raw.includes("btc") || raw.includes("crypto")) return "bitcoin,crypto,market";
  if (raw.includes("oil") || raw.includes("brent") || raw.includes("petroleo") || raw.includes("crude")) return "oil,barrel,energy";
  if (raw.includes("nvidia") || raw.includes("nvda") || raw.includes("ai")) return "semiconductor,chip,technology";
  if (raw.includes("gold") || raw.includes("oro")) return "gold,market";
  if (raw.includes("fed") || raw.includes("inflation") || raw.includes("rates")) return "federal-reserve,finance";
  return "stock-market,trading,finance";
}

function newsImageForItem(item = {}) {
  const image = item.imageUrl || item.image_url || item.thumbnail_url || item.thumbnail || item.image || "";
  const cleaned = String(image || "").trim();
  if (cleaned && !cleaned.includes("source.unsplash.com")) return cleaned;
  return newsFallbackImageForItem(item);
}

function newsFallbackImageForItem(item = {}) {
  const text = `${item.title || ""} ${item.category || ""} ${(item.assets || item.tickers || []).join(" ")}`.toLowerCase();
  if (text.includes("bitcoin") || text.includes("btc") || text.includes("crypto")) return NEWS_FALLBACK_IMAGES.crypto;
  if (text.includes("oil") || text.includes("brent") || text.includes("crude") || text.includes("petroleo")) return NEWS_FALLBACK_IMAGES.commodity;
  if (text.includes("gold") || text.includes("oro")) return NEWS_FALLBACK_IMAGES.gold;
  if (text.includes("nvidia") || text.includes("nvda") || text.includes("chip") || text.includes("ai")) return NEWS_FALLBACK_IMAGES.tech;
  if (text.includes("fed") || text.includes("inflation") || text.includes("rates") || text.includes("macro")) return NEWS_FALLBACK_IMAGES.macro;
  if (text.includes("trump") || text.includes("iran") || text.includes("geopolit")) return NEWS_FALLBACK_IMAGES.geopolitics;
  return NEWS_FALLBACK_IMAGES.market;
}

function newsImageTag(item = {}, className = "") {
  const image = newsImageForItem(item);
  const fallback = newsFallbackImageForItem(item);
  const classes = ["news-photo-frame", className].filter(Boolean).join(" ");
  const classAttr = classes ? ` class="${escapeHtml(classes)}"` : "";
  const styleAttr = ` style="background-image:url(${escapeHtml(fallback)})"`;
  return `<span${classAttr}${styleAttr}><img src="${escapeHtml(image)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.remove();"></span>`;
}

function newsItemId(item) {
  const explicit = cleanCopy(item?.id || item?.news_id || "");
  if (explicit) return explicit;
  const raw = [
    item?.title || "",
    item?.source || "",
    item?.published_at || item?.time || "",
    item?.url || "",
  ].join("|");
  return `news-${simpleHash(raw)}`;
}

function indexNewsItems(items) {
  appState.newsItemsById = {};
  (items || []).forEach((item) => {
    const id = newsItemId(item);
    appState.newsItemsById[id] = { ...item, id };
  });
}

function newsCardMarkup(item) {
  const id = newsItemId(item);
  const assets = Array.isArray(item.assets) ? item.assets.filter(Boolean).slice(0, 4) : [];
  const assetLabels = assets.map((asset) => displayAssetLabel(asset) || cleanCopy(asset));
  const tone = newsImpactTone(item.impact);
  const category = cleanCopy(item.category || item.placeholder_key || "contexto").toLowerCase().replace(/[^a-z0-9_-]+/g, "-");
  const published = item.relative_time || formatDate(item.time);
  const impactLabel = cleanCopy(item.impact || "Neutral");
  return `
    <article class="news-card investing-news-card" data-news-id="${escapeHtml(id)}" data-news-open="${escapeHtml(id)}">
      <button class="news-card-main" type="button" data-news-id="${escapeHtml(id)}" data-news-open="${escapeHtml(id)}">
        <span class="news-thumb placeholder-${escapeHtml(category)}">
          ${newsImageTag(item)}
        </span>
      <div class="news-card-copy">
        <span class="feed-kicker">${escapeHtml(cleanCopy(item.category || "Contexto"))} · ${escapeHtml(cleanCopy(item.source || "Fuente"))}</span>
        <strong>${escapeHtml(newsDisplayTitle(item))}</strong>
        <p>${escapeHtml(spanishUiCopy(item.genesisTakeaway || item.genesis_takeaway || item.summary || "Sin lectura adicional."))}</p>
        <div class="news-card-bottom">
          <span>${escapeHtml(cleanCopy(published))}</span>
          ${assetLabels.slice(0, 3).map((asset) => `<span>${escapeHtml(asset)}</span>`).join("")}
          <span>${escapeHtml(`Conf. ${cleanCopy(item.confidence || "media")}`)}</span>
        </div>
      </div>
      <span class="impact-badge ${tone}">${escapeHtml(impactLabel)}</span>
      </button>
      <div class="news-meta">
        <span>${escapeHtml(cleanCopy(item.source || "Fuente activa"))}</span>
        <span>${escapeHtml(cleanCopy(published))}</span>
        <span class="${tone}">${escapeHtml(cleanCopy(item.impact || "Neutral"))}</span>
        ${item.confidence ? `<span>${escapeHtml(`Confianza ${cleanCopy(item.confidence)}`)}</span>` : ""}
        ${assetLabels.length ? `<span>${escapeHtml(assetLabels.join(", "))}</span>` : ""}
      </div>
    </article>
  `;
}

function newsPlaceholderMarkup(category, item = {}) {
  const ticker = (Array.isArray(item.assets) && item.assets[0]) || "";
  const key = String(category || "").toLowerCase();
  const symbols = {
    commodity: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M8 20h8"/><path d="M10 20V9a4 4 0 0 1 8 0v11"/><path d="M6 20V8h4"/><path d="M4 8h8"/></svg>`,
    crypto: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M9 3v18M15 3v18"/><path d="M7 6h7a3 3 0 0 1 0 6H7h8a3 3 0 0 1 0 6H7"/></svg>`,
    macro: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 16l5-5 4 4 7-8"/><path d="M4 20h16"/></svg>`,
    geopolitics: `<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="8"/><path d="M4 12h16"/><path d="M12 4a12 12 0 0 1 0 16"/><path d="M12 4a12 12 0 0 0 0 16"/></svg>`,
    earnings: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 19V5h14v14z"/><path d="M8 15h8M8 11h8M8 7h5"/></svg>`,
    ticker: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 17l5-5 4 4 7-9"/><path d="M4 20h16"/></svg>`,
  };
  const label = ticker ? normalizeTicker(ticker).slice(0, 5) : "";
  return `${symbols[key] || symbols.ticker}<small>${escapeHtml(label || "Market")}</small>`;
}

function openNewsDetail(newsId) {
  const items = newsFeedItems(appState.newsSnapshot || {});
  if (!Object.keys(appState.newsItemsById || {}).length) indexNewsItems(items);
  let item = appState.newsItemsById?.[newsId];
  if (!item) {
    item = items.find((entry) => newsItemId(entry) === newsId)
      || items.find((entry) => cleanCopy(entry.url || "") && cleanCopy(entry.url) === cleanCopy(newsId))
      || null;
  }
  if (!item) {
    toast("Actualiza Noticias para volver a abrir ese detalle.", "info");
    return;
  }
  appState.selectedNewsId = newsId;
  const sheet = document.getElementById("news-sheet");
  const body = document.getElementById("news-sheet-body");
  if (!sheet || !body) return;
  const image = newsImageForItem(item);
  const assets = Array.isArray(item.tickersAffected) ? item.tickersAffected.filter(Boolean).slice(0, 5) : Array.isArray(item.assets) ? item.assets.filter(Boolean).slice(0, 5) : [];
  const watchPoints = Array.isArray(item.watchPoints) ? item.watchPoints.filter(Boolean).slice(0, 4) : [];
  body.innerHTML = `
    <section class="sheet-hero news-hero">
      ${newsImageTag(item, "news-detail-image")}
      <div>
        <span class="app-kicker">${escapeHtml(cleanCopy(item.category || "Noticias"))}</span>
        <h2>${escapeHtml(newsDisplayTitle(item))}</h2>
      </div>
    </section>
    <div class="news-detail-meta">
      <span>${escapeHtml(cleanCopy(item.source || "Fuente activa"))}</span>
      <span>${escapeHtml(cleanCopy(item.relative_time || formatDate(item.time)))}</span>
      <span class="${newsImpactTone(item.impact)}">${escapeHtml(cleanCopy(item.impact || "Neutral"))}</span>
      ${item.confidence ? `<span>${escapeHtml(`Confianza ${cleanCopy(item.confidence)}`)}</span>` : ""}
    </div>
    <p class="detail-lead">${escapeHtml(spanishUiCopy(item.summary || "Sin resumen disponible."))}</p>
    <section class="genesis-mini investing-detail-block">
      <strong>Lectura Genesis</strong>
      <p>${escapeHtml(spanishUiCopy(item.genesisTakeaway || item.genesis_takeaway || "Genesis usa esta nota como contexto, no como señal aislada."))}</p>
      <p>Por que importa: ${escapeHtml(spanishUiCopy(item.whyItMatters || item.why_it_matters || "Puede afectar apetito de riesgo, momentum o niveles de los activos relacionados."))}</p>
      <p>Riesgo: ${escapeHtml(spanishUiCopy(item.risk || "Impacto aun depende de confirmacion por precio y volumen."))}</p>
      <p>Qué vigilar: ${escapeHtml(watchPoints.length ? watchPoints.map(spanishUiCopy).join(" | ") : "reacción de precio, volumen y confirmación en la siguiente vela relevante.")}</p>
      ${item.watch ? `<p>${escapeHtml(spanishUiCopy(item.watch))}</p>` : ""}
    </section>
    ${assets.length ? `<div class="news-meta">${assets.map((asset) => `<span>${escapeHtml(displayAssetLabel(asset))}</span>`).join("")}</div>` : ""}
    ${item.url ? `<a class="secondary-button full" href="${escapeHtml(item.url)}" target="_blank" rel="noopener noreferrer">Abrir fuente original</a>` : ""}
  `;
  sheet.hidden = false;
}

function newsDisplayTitle(item = {}) {
  return spanishUiCopy(item.title_es || item.title || item.headline || item.originalTitle || "Noticia");
}

function closeNewsDetail() {
  const sheet = document.getElementById("news-sheet");
  if (sheet) sheet.hidden = true;
  appState.selectedNewsId = "";
}

function premiumScreenHeader(title, subtitle, actionMarkup = "") {
  return `
    <section class="premium-screen-head">
      <div>
        <strong>${escapeHtml(title)}</strong>
        <p>${escapeHtml(subtitle)}</p>
      </div>
      ${actionMarkup}
    </section>
  `;
}

function marketPulseHeroMarkup(context = "tracking") {
  ensureMarketPulse();
  const rows = marketPulseRows();
  const priced = rows.filter((item) => itemPrice(item) !== null);
  const avgMove = priced.length
    ? priced.reduce((sum, item) => sum + (itemDailyPct(item) || 0), 0) / priced.length
    : null;
  const tone = positiveClass(avgMove);
  const confidence = priced.length >= 4 ? "alta" : priced.length >= 2 ? "media" : "pendiente";
  const title = tone === "up" ? "Mercado con sesgo comprador" : tone === "down" ? "Mercado bajo presión" : "Mercado en vigilancia";
  const subtitle = context === "alerts"
    ? "Precio, volumen y riesgo conectados a tus activos."
    : "";
  const refreshing = appState.refreshInFlight || appState.marketPulseLoading || appState.opportunityQuotesLoading;
  return `
    <section class="market-pulse-render tone-${tone} ${refreshing ? "is-refreshing" : ""}">
      <div class="pulse-copy">
        <span>Pulso del mercado</span>
        <strong>${escapeHtml(title)}</strong>
        ${subtitle ? `<p>${escapeHtml(subtitle)}</p>` : ""}
        ${refreshing ? liveRefreshBadgeMarkup("Actualizando precios") : ""}
      </div>
      <div class="pulse-confidence">
        ${renderConfidenceBar(confidence)}
      </div>
      <div class="pulse-strip">
        ${rows.map((item) => {
          const ticker = itemTicker(item);
          const display = getAssetDisplayName(item);
          const rowTone = movementTone(item);
          return `
            <button type="button" data-open-asset="${escapeHtml(ticker)}" class="pulse-chip ${rowTone}">
              <span>${renderAssetIcon(item)}</span>
              <b>${escapeHtml(ticker === "BZ=F" ? "Brent" : ticker)}</b>
              <small>${escapeHtml(priceLabel(item))}</small>
              ${renderSparkline(assetMoveValues(item), rowTone)}
              <em>${escapeHtml(display.subtitle || display.displayName)}</em>
            </button>
          `;
        }).join("")}
      </div>
    </section>
  `;
}

function sectionTitleMarkup(title, meta = "") {
  return `
    <div class="section-heading premium-section-title">
      <strong>${escapeHtml(title)}</strong>
      ${meta ? `<small>${escapeHtml(meta)}</small>` : ""}
    </div>
  `;
}

function assetStatusLabel(item) {
  const pct = itemDailyPct(item);
  const vol = numberOrNull(item?.relative_volume ?? item?.relativeVolume);
  if (pct !== null && pct >= 1.25 && (vol === null || vol >= 0.9)) return "Alcista";
  if (pct !== null && pct <= -1.25) return "Bajista";
  if (pct !== null && pct < 0) return "Presionado";
  if (vol !== null && vol >= 1.5) return "Volumen";
  return "Neutral";
}

function watchTodayMarkup(items = []) {
  const rows = (items.length ? items : marketPulseRows()).slice(0, 5);
  const unusual = rows.find((item) => numberOrNull(item?.relative_volume ?? item?.relativeVolume) >= 1.5 || Math.abs(itemDailyPct(item) || 0) >= 2);
  const pressure = rows.find((item) => (itemDailyPct(item) || 0) < 0);
  const leader = [...rows].sort((a, b) => (itemDailyPct(b) || 0) - (itemDailyPct(a) || 0))[0];
  return `
    <section class="watch-today-card">
      <strong>Que vigilar hoy</strong>
      <div>
        ${renderMetricCard("Nivel clave", leader ? `${itemTicker(leader)} ${formatPercent(itemDailyPct(leader), "0.00%")}` : "Sin liderazgo", movementTone(leader))}
        ${renderMetricCard("Volumen inusual", unusual ? `${itemTicker(unusual)} ${formatVolumeCompact(unusual.volume)}` : "Sin ruptura", unusual ? "up" : "flat")}
        ${renderMetricCard("Riesgo macro", pressure ? `${itemTicker(pressure)} presionado` : "Neutral", pressure ? "down" : "flat")}
      </div>
    </section>
  `;
}

function renderTrackingScreen() {
  const root = document.getElementById("view-watchlist");
  if (!root) return;
  const items = filteredTrackingItems();
  ensureMarketPulse();
  root.innerHTML = `
    <section class="screen-stack premium-tracking-screen">
      <div class="compact-actions">
        <button type="button" class="icon-action search-toggle" data-toggle-search="tracking" aria-label="${appState.searchOpen.tracking ? "Cerrar busqueda" : "Buscar activo"}">${iconSvg("search")}</button>
      </div>
      ${marketPulseHeroMarkup("tracking")}
      ${appState.searchOpen.tracking ? `
        <form class="search-card premium-search" id="tracking-search-form">
          <input id="portfolio-search-input" placeholder="Buscar activos, empresas o ETFs..." autocomplete="off" value="${escapeHtml(appState.trackingSearchQuery)}">
          <button class="round-button icon-submit" id="portfolio-search-button" type="button" aria-label="Confirmar busqueda">${iconSvg("check")}</button>
        </form>
        ${searchDiscoveryRail("tracking")}
      ` : ""}
      <div class="market-filters" aria-label="Filtros de seguimiento">
        ${trackingFilterMarkup()}
      </div>
      <div class="search-results" id="portfolio-search-result" ${appState.marketSearchResults.tracking.length ? "" : "hidden"}>
        ${appState.marketSearchResults.tracking.map((item) => searchResultMarkup(item, "tracking")).join("")}
      </div>
      ${sectionTitleMarkup("Tus activos vigilados", `${items.length} activos`)}
      <div class="asset-list" id="watchlist-screen-body">
        ${items.length ? items.map((item) => assetRowMarkup(item, "tracking")).join("") : emptyStateMarkup("Sin activos en seguimiento.", "Busca un ticker y agregalo para ver precio, sesion y movimiento.")}
      </div>
      ${watchTodayMarkup(items)}
    </section>
  `;
  const searchButton = document.getElementById("portfolio-search-button");
  if (searchButton) searchButton.addEventListener("click", searchAndAddPortfolioTicker);
  const searchInput = document.getElementById("portfolio-search-input");
  if (searchInput) {
    searchInput.addEventListener("input", (event) => {
      appState.trackingSearchQuery = event.target.value;
    });
  }
  const searchForm = document.getElementById("tracking-search-form");
  if (searchForm) {
    searchForm.addEventListener("submit", (event) => {
      event.preventDefault();
      searchTrackingOnly();
    });
  }
}

function trackingFilterMarkup() {
  const filters = [
    ["all", "Todos"],
    ["stocks", "Acciones"],
    ["crypto", "Cripto"],
    ["etf", "ETFs"],
    ["commodities", "Materias primas"],
  ];
  return filters.map(([key, label]) => (
    `<button type="button" class="${appState.trackingFilter === key ? "is-active" : ""}" data-tracking-filter="${key}">${label}</button>`
  )).join("");
}

function searchDiscoveryRail(mode = "tracking") {
  const assets = [...(appState.trackingItems || []), ...(appState.paperPositions || [])];
  const seen = new Set();
  const uniqueAssets = assets.filter((item) => {
    const ticker = itemTicker(item);
    if (!ticker || seen.has(ticker)) return false;
    seen.add(ticker);
    return true;
  });
  const recent = uniqueAssets.slice(0, 5);
  const favorites = uniqueAssets.filter((item) => ["NVDA", "META", "AAPL", "MSFT", "BTC-USD", "BZ=F", "BNO"].includes(itemTicker(item))).slice(0, 5);
  const up = uniqueAssets.filter((item) => (itemDailyPct(item) || 0) > 0).sort((a, b) => (itemDailyPct(b) || 0) - (itemDailyPct(a) || 0)).slice(0, 4);
  const down = uniqueAssets.filter((item) => (itemDailyPct(item) || 0) < 0).sort((a, b) => (itemDailyPct(a) || 0) - (itemDailyPct(b) || 0)).slice(0, 4);
  const undervalued = uniqueAssets.filter((item) => {
    const price = itemPrice(item);
    const previous = positiveOrNull(item?.previous_close ?? item?.previousClose);
    return price !== null && previous !== null && price < previous;
  }).slice(0, 4);
  const groups = [
    ["Recientes", recent],
    ["Favoritas", favorites.length ? favorites : recent],
    ["Alcistas", up],
    ["Bajistas", down],
    ["Infravaloradas", undervalued],
  ].filter(([, rows]) => rows.length);
  if (!groups.length) {
    const starter = ["META", "NVDA", "TSLA", "BTC-USD", "SPY", "BZ=F"].map((ticker) => ({ ticker }));
    groups.push(["Favoritas", starter]);
  }
  return `
    <section class="search-discovery" aria-label="Ideas de busqueda">
      ${groups.map(([label, rows]) => `
        <div>
          <span>${escapeHtml(label)}</span>
          <div>
            ${rows.map((item) => {
              const ticker = itemTicker(item);
              const tone = marketToneClass(itemDailyPct(item));
              return `<button type="button" data-search-pick="${escapeHtml(ticker)}" data-search-mode="${escapeHtml(mode)}"><b>${escapeHtml(ticker)}</b><small class="${tone}">${escapeHtml(formatPercent(itemDailyPct(item), ""))}</small></button>`;
            }).join("")}
          </div>
        </div>
      `).join("")}
    </section>
  `;
}

function filteredTrackingItems() {
  if (appState.trackingFilter === "all") return appState.trackingItems;
  return appState.trackingItems.filter((item) => assetCategory(item) === appState.trackingFilter);
}

function assetCategory(item) {
  const ticker = itemTicker(item);
  const type = String(item?.asset_type || item?.type || item?.category || item?.exchange || "").toLowerCase();
  if (ticker.endsWith("-USD") || type.includes("crypto")) return "crypto";
  if (ticker.includes("=F") || ["BNO", "USO", "GLD", "IAU", "SLV"].includes(ticker) || type.includes("commodity")) return "commodities";
  if (type.includes("etf") || ["SPY", "QQQ", "DIA", "IWM", "VTI", "VOO", "IXC"].includes(ticker)) return "etf";
  return "stocks";
}

function renderPortfolioScreen() {
  const root = document.getElementById("view-radar");
  if (!root) return;
  ensureMarketPulse();
  const totals = appState.portfolioTotals;
  const distribution = buildDistribution();
  const topRow = distribution.reduce((best, row) => (!best || row.weight > best.weight ? row : best), null);
  const portfolioPnl = totals.totalPnl ?? totals.dailyPnl;
  const portfolioPnlPct = totals.totalPnlPct ?? totals.dailyPnlPct;
  const portfolioTone = movementTone(portfolioPnlPct ?? portfolioPnl);
  const concentrationLabel = topRow ? `${itemTicker(topRow.item)} ${compactPercent(topRow.weight)}` : "para calcular pesos";
  root.innerHTML = `
    <section class="screen-stack premium-portfolio-screen">
      <div class="compact-actions">
        <button type="button" class="icon-action search-toggle" data-toggle-search="portfolio" aria-label="${appState.searchOpen.portfolio ? "Cerrar busqueda" : "Buscar activo"}">${iconSvg("search")}</button>
      </div>
      ${portfolioHeroMarkup(totals, distribution)}
      ${appState.searchOpen.portfolio ? `
        <form class="search-card premium-search" id="portfolio-buy-search-form">
          <input id="portfolio-buy-search-input" placeholder="Buscar ticker o empresa para simular compra" autocomplete="off" value="${escapeHtml(appState.portfolioSearchQuery)}">
          <button class="round-button icon-submit" type="button" id="portfolio-sim-buy-button" aria-label="Confirmar busqueda">${iconSvg("check")}</button>
        </form>
        ${appState.portfolioSearchQuery.trim() ? searchDiscoveryRail("portfolio") : ""}
      ` : ""}
      <div class="search-results" id="portfolio-buy-search-result" ${appState.searchOpen.portfolio && appState.marketSearchResults.portfolio.length ? "" : "hidden"}>
        ${appState.searchOpen.portfolio ? appState.marketSearchResults.portfolio.map((item) => searchResultMarkup(item, "portfolio")).join("") : ""}
      </div>
      ${sectionTitleMarkup("Asignacion paper", distribution.length ? `${distribution.length} posiciones` : "sin compras")}
      <section class="donut-panel">
        <div class="portfolio-donut ${distribution.length ? "" : "empty"}" id="portfolio-donut" style="background:${donutGradient(distribution)}">
          <div class="donut-center">
            <strong class="market-number flat" id="portfolio-total-value">${distribution.length ? money(totals.totalValue) : "Sin compras"}</strong>
            <span class="market-number ${portfolioTone}" id="portfolio-day-return">${distribution.length ? signedMoney(portfolioPnl, "P/L sin calcular") : "Simula una compra"}</span>
            <small class="market-number flat" id="portfolio-donut-caption">${distribution.length ? concentrationLabel : "para calcular pesos"}</small>
          </div>
        </div>
        <div class="legend" id="radar-ticker-list">${distribution.length ? distribution.map(legendMarkup).join("") : `<span class="pill">Sin posiciones compradas</span>`}</div>
      </section>
      <div class="asset-list" id="portfolio-positions-body">
        ${appState.paperPositions.length ? appState.paperPositions.map((item) => assetRowMarkup(item, "paper", totals.totalValue)).join("") : emptyStateMarkup("Sin compras simuladas.", "Compra paper para ver peso, valor y P/L.")}
      </div>
      ${portfolioInsightCards(totals, distribution)}
      <div id="portfolio-watchlist-body" hidden></div>
      <span id="portfolio-data-state" hidden>Datos directos activos</span>
      <span id="portfolio-last-update" hidden>${escapeHtml(appState.lastUpdated)}</span>
      <span id="portfolio-total-stat" hidden>${escapeHtml(money(totals.totalValue))}</span>
      <span id="portfolio-day-stat" hidden>${escapeHtml(signedMoney(totals.dailyPnl))}</span>
      <span id="radar-tracked-count" hidden>${appState.allItems.length}</span>
      <span id="radar-last-update" hidden>${escapeHtml(appState.lastUpdated)}</span>
      <span id="radar-summary-note" hidden>Cartera</span>
      <span id="radar-investment-count" hidden>${appState.paperPositions.length}</span>
      <span id="radar-reference-count" hidden>${appState.trackingItems.length}</span>
    </section>
  `;
  const buyButton = document.getElementById("portfolio-sim-buy-button");
  if (buyButton) buyButton.addEventListener("click", searchPortfolioBuyTicker);
  const buyInput = document.getElementById("portfolio-buy-search-input");
  if (buyInput) {
    buyInput.addEventListener("input", (event) => {
      appState.portfolioSearchQuery = event.target.value;
      if (!appState.portfolioSearchQuery.trim()) {
        appState.marketSearchResults.portfolio = [];
        renderPortfolioScreen();
      }
    });
    buyInput.addEventListener("blur", () => {
      window.setTimeout(() => {
        const active = document.activeElement;
        if (active?.closest?.("#portfolio-buy-search-form, .search-discovery, #portfolio-buy-search-result")) return;
        if (!appState.portfolioSearchQuery.trim() && !appState.marketSearchResults.portfolio.length) {
          appState.searchOpen.portfolio = false;
          renderPortfolioScreen();
        }
      }, 120);
    });
  }
  const buyForm = document.getElementById("portfolio-buy-search-form");
  if (buyForm) {
    buyForm.addEventListener("submit", (event) => {
      event.preventDefault();
      searchPortfolioBuyTicker();
    });
  }
}

function portfolioHeroMarkup(totals, distribution) {
  const totalValue = numberOrNull(totals.totalValue) || 0;
  const pnl = totals.totalPnl ?? totals.dailyPnl;
  const pnlPct = totals.totalPnlPct ?? totals.dailyPnlPct;
  const tone = positiveClass(pnlPct ?? pnl);
  const values = distribution.map((row) => row.value || row.weight);
  return `
    <section class="portfolio-hero-card tone-${tone}">
      <span>Cartera paper</span>
      <strong>${escapeHtml(totalValue > 0 ? money(totalValue) : "Sin compras")}</strong>
      <p>${escapeHtml(distribution.length ? `P/L ${signedMoney(pnl, "sin calcular")} / ${formatPercent(pnlPct, "0.00%")}` : "Simula una compra para calcular pesos reales.")}</p>
      <div class="portfolio-performance">
        ${renderMiniBars(values.length ? values : [1, 2, 1], 42)}
      </div>
      <div class="portfolio-range-tabs" aria-hidden="true"><span>1D</span><span>1W</span><span>1M</span></div>
    </section>
  `;
}

function portfolioInsightCards(totals, distribution) {
  const top = distribution.reduce((best, row) => (!best || row.weight > best.weight ? row : best), null);
  const risks = appState.paperPositions
    .filter((item) => (itemDailyPct(item) || 0) < 0 || (positionPnl(item) || 0) < 0)
    .slice(0, 2);
  const winners = appState.paperPositions
    .filter((item) => (positionPnl(item) || itemDailyPct(item) || 0) > 0)
    .slice(0, 2);
  return `
    <section class="portfolio-insight-grid">
      ${renderMetricCard("Exposicion", top ? `${itemTicker(top.item)} ${compactPercent(top.weight)}` : "Sin concentracion", top ? "up" : "flat")}
      ${renderMetricCard("Genesis lectura", distribution.length ? "Riesgo medido" : "Esperando compra", distribution.length ? "up" : "flat")}
      ${renderMetricCard("Ganadores", winners.length ? winners.map(itemTicker).join(", ") : "Sin ganadores", winners.length ? "up" : "flat")}
      ${renderMetricCard("Riesgos", risks.length ? risks.map(itemTicker).join(", ") : "Sin presion", risks.length ? "down" : "flat")}
    </section>
  `;
}

function emptyStateMarkup(title, text) {
  return `
    <div class="empty-state">
      <strong>${escapeHtml(title)}</strong>
      <p>${escapeHtml(text)}</p>
    </div>
  `;
}

function shortAssetName(item) {
  const raw = assetDisplayName(item);
  return raw.length > 34 ? `${raw.slice(0, 31)}...` : raw;
}

function compactAssetSubline(item, mode, totalValue = 0) {
  if (mode === "paper") {
    const units = itemUnits(item);
    const value = itemValue(item);
    const weight = totalValue > 0 && value !== null ? (value / totalValue) * 100 : numberOrNull(item.weight_pct);
    return `${units ?? "Sin"} units - ${money(value, "Sin valor")} - ${compactPercent(weight)}`;
  }
  return assetSubtitle(item) || shortAssetName(item);
}

function searchResultMarkup(item, mode) {
  const ticker = itemTicker(item);
  const tone = movementTone(item);
  const display = getAssetDisplayName(item);
  const action = mode === "tracking"
    ? `<button class="compact-action" type="button" data-market-add="${escapeHtml(ticker)}">${iconSvg("add")}<span>Seguimiento</span></button>`
    : `<button class="compact-action" type="button" data-paper-buy="${escapeHtml(ticker)}">${iconSvg("cart")}<span>Comprar</span></button>`;
  return `
    <article class="search-result compact-market-row" data-search-result="${escapeHtml(ticker)}">
      <button class="search-main" type="button" data-open-asset="${escapeHtml(ticker)}">
        <span>
          <strong>${escapeHtml(display.displayName)}</strong>
          <small>${escapeHtml(display.subtitle || ticker)}</small>
        </span>
        <span class="price-stack">
          <strong class="${marketToneClass(item)}">${escapeHtml(priceLabel(item))}</strong>
          <span class="change-stack ${tone}">${dailyMoveMarkup(item)}</span>
        </span>
      </button>
      <div class="row-actions">${action}</div>
    </article>
  `;
}

function assetRowMarkup(item, mode, totalValue = 0) {
  const ticker = itemTicker(item);
  const tone = movementTone(item);
  const pnl = positionPnl(item);
  const subline = compactAssetSubline(item, mode, totalValue);
  const display = getAssetDisplayName(item);
  const volume = numberOrNull(item.volume);
  const relativeVolume = numberOrNull(item.relative_volume ?? item.relativeVolume);
  const status = assetStatusLabel(item);
  return `
    <article class="asset-row compact-market-row" data-ticker="${escapeHtml(ticker)}" data-mode="${mode}">
      <button class="asset-main" type="button" data-open-asset="${escapeHtml(ticker)}">
        <span class="asset-row-leading">
          ${renderAssetIcon(item)}
          <span class="asset-title">
            <strong>${escapeHtml(display.displayName)}</strong>
            <small>${escapeHtml(subline || display.subtitle || ticker)}</small>
          </span>
        </span>
        <span class="asset-row-vitals" aria-hidden="true">
          ${renderSparkline(assetMoveValues(item), tone)}
          <em>${escapeHtml(volume ? formatVolumeCompact(volume) : "Vol. pendiente")}</em>
          <em>${escapeHtml(relativeVolume !== null ? `${compactNumber(relativeVolume)}x` : "Rel. pendiente")}</em>
        </span>
        <span class="price-stack">
          <strong class="${marketToneClass(item)}">${escapeHtml(priceLabel(item))}</strong>
          <span class="change-stack ${tone}">${dailyMoveMarkup(item)}</span>
          <span class="asset-state-badge ${tone}">${escapeHtml(status)}</span>
          ${mode === "paper" ? `<span class="${marketToneClass(pnl)}">${escapeHtml(formatChange(pnl, "P/L sin dato"))}</span>` : ""}
        </span>
      </button>
      ${assetRowMenuMarkup(ticker, mode)}
    </article>
  `;
}

function assetRowMenuMarkup(ticker, mode) {
  const normalized = escapeHtml(ticker);
  return `
    <details class="market-menu">
      <summary aria-label="Acciones ${normalized}" title="Acciones">${iconSvg("menu")}</summary>
      <div class="market-menu-panel">
        <button type="button" data-open-asset="${normalized}">Ver detalle</button>
        ${mode === "paper" ? "" : `<button type="button" data-paper-buy="${normalized}">Compra paper</button>`}
        <button type="button" data-create-alert="${normalized}">Crear alerta</button>
        ${mode === "paper"
          ? `<button class="danger" type="button" data-paper-close="${normalized}">Cerrar paper</button>`
          : `<button class="danger" type="button" data-watch-remove="${normalized}">Quitar seguimiento</button>`}
      </div>
    </details>
  `;
}

function buildDistribution() {
  const total = appState.portfolioTotals.totalValue
    || appState.paperPositions.reduce((sum, item) => sum + (itemValue(item) || 0), 0);
  return appState.paperPositions
    .map((item) => {
      const explicitWeight = numberOrNull(item.weight_pct);
      const value = itemValue(item) ?? (explicitWeight !== null && total > 0 ? (total * explicitWeight) / 100 : 0);
      const weight = explicitWeight ?? (total > 0 ? (value / total) * 100 : 0);
      return { item, value, weight };
    })
    .filter((row) => row.value > 0 || row.weight > 0);
}

function legendMarkup(row, index) {
  return `<span class="legend-pill"><i style="background:${MONEY_COLORS[index % MONEY_COLORS.length]}"></i>${escapeHtml(itemTicker(row.item))} ${compactPercent(row.weight)}</span>`;
}

function donutGradient(distribution) {
  if (!distribution.length) return "conic-gradient(rgba(255,255,255,.08) 0 100%)";
  let cursor = 0;
  const stops = distribution.map((row, index) => {
    const start = cursor;
    cursor += row.weight;
    const color = MONEY_COLORS[index % MONEY_COLORS.length];
    return `${color} ${start}% ${cursor}%`;
  });
  return `conic-gradient(${stops.join(", ")})`;
}

async function loadWhales() {
  await loadWhalesData();
  renderAlertsScreen();
}

async function loadWhalesData() {
  const [learnedResult, causalResult, detectionResult] = await Promise.allSettled([
    getJson("/api/dashboard/whales"),
    getJson("/api/dashboard/money-flow/causal"),
    getJson("/api/dashboard/money-flow/detection"),
  ]);
  const learned = learnedResult.status === "fulfilled" ? learnedResult.value : {};
  const causal = causalResult.status === "fulfilled" ? causalResult.value : {};
  const detection = detectionResult.status === "fulfilled" ? detectionResult.value : {};
  appState.whalesSnapshot = { learned, causal, detection };
  return appState.whalesSnapshot;
}

function renderMoneyFlowSnapshot(causalPayload = {}, detectionPayload = {}) {
  appState.whalesSnapshot = { ...(appState.whalesSnapshot || {}), causal: causalPayload, detection: detectionPayload };
  renderAlertsScreen();
}

function renderWhalesScreen() {
  renderAlertsScreen();
}

function extractWhaleRows(causal, detection, learned = appState.whalesSnapshot?.learned || {}) {
  const learnedEvents = Array.isArray(learned.events) ? learned.events : [];
  const candidates = [
    ...learnedEvents,
    ...(Array.isArray(causal.items) ? causal.items : []),
    ...(Array.isArray(causal.causal?.items) ? causal.causal.items : []),
    ...(Array.isArray(detection.items) ? detection.items : []),
    ...(Array.isArray(detection.detection?.items) ? detection.detection.items : []),
  ];
  const byEvent = new Map();
  candidates.forEach((item) => {
    const ticker = itemTicker(item);
    if (!ticker) return;
    const normalizedLearned = normalizeWhaleEventForUi(item);
    if (normalizedLearned) {
      const hydratedLearned = hydrateWhaleRowWithLiveAsset(normalizedLearned);
      const key = whaleItemId(hydratedLearned);
      if (!byEvent.has(key)) byEvent.set(key, hydratedLearned);
      return;
    }
    const whale = typeof item.whale === "object" && item.whale ? item.whale : {};
    const identified = Boolean(item.whale_identified || whale.identified || whale.entity);
    const relativeVolume = numberOrNull(item.relative_volume ?? item.relativeVolume ?? item.volume_ratio ?? item.intensity);
    const asset = findAsset(ticker) || {};
    const currentPrice = numberOrNull(item.current_price ?? item.price ?? asset.current_price ?? asset.price ?? asset.reference_price);
    const volume = numberOrNull(item.volume ?? asset.volume);
    const avgVolume = numberOrNull(item.avg_volume ?? asset.avg_volume ?? asset.average_volume);
    const computedRelativeVolume = relativeVolume ?? (avgVolume && volume ? volume / avgVolume : null);
    const dollarVolume = numberOrNull(item.dollar_volume ?? item.dollarVolume) ?? (currentPrice !== null && volume !== null ? currentPrice * volume : null);
    const rawAmount = identified ? (whale.movement_value || item.movement_value || item.amount_usd || item.estimated_value || "") : "";
    const amountCheck = saneFlowAmount(rawAmount, dollarVolume);
    const amount = amountCheck.value ?? "";
    const hasFlowSignal = Boolean(
      item.flow_detected
      || item.money_flow_detected
      || item.primary_label
      || item.direction
      || (computedRelativeVolume !== null && computedRelativeVolume >= 1.2)
    );
    if (!identified && !hasFlowSignal) return;
    const actionText = identified ? classifyWhaleType(whale.movement_type || item.direction || item.primary_label) : flowDirectionLabel(item, computedRelativeVolume);
    const row = {
      id: `whale-${ticker}-${String(item.money_flow_timestamp || item.timestamp || item.updated_at || Date.now()).replace(/[^a-zA-Z0-9]/g, "")}`,
      ticker,
      assetName: assetDisplayName(asset) || getAssetDisplayName(ticker).displayName,
      event: actionText,
      direction: /venta|salida|distrib/i.test(actionText) ? "outflow" : /compra|entrada|acumul/i.test(actionText) ? "inflow" : "neutral",
      entity: whale.entity || item.whale_entity || "",
      amount,
      units: whale.shares || item.shares || item.units || "",
      price: whale.price || item.price || currentPrice || "",
      currentPrice,
      volume,
      avgVolume,
      relativeVolume: computedRelativeVolume,
      dollarVolume,
      amountSuspicious: amountCheck.suspicious || Boolean(item.amount_suspicious),
      netFlow: /venta|salida|distrib/i.test(actionText) && numberOrNull(dollarVolume) !== null ? -Math.abs(numberOrNull(dollarVolume)) : numberOrNull(dollarVolume),
      date: item.money_flow_timestamp || item.timestamp || item.updated_at || "",
      source: whale.source || item.source || item.origin || "Fuente activa",
      confidence: whale.confidence || item.confidence || item.confidence_label || (identified ? "media" : "baja"),
      intensity: computedRelativeVolume,
      read: identified
        ? "Genesis detecta una entidad reportada y lo trata como evidencia adicional, no como causalidad garantizada."
        : "Flujo institucional en vigilancia: hay actividad o volumen, pero sin entidad ni monto confirmado.",
      missing: identified
        ? "Falta continuidad y contexto de precio para elevar conviccion."
        : "Falta entidad, monto y fecha confirmada para llamarlo ballena.",
    };
    const hydratedRow = hydrateWhaleRowWithLiveAsset(row);
    const key = whaleItemId(hydratedRow);
    if (!byEvent.has(key)) byEvent.set(key, hydratedRow);
  });
  return Array.from(byEvent.values()).slice(0, 12);
}

function normalizeWhaleEventForUi(item = {}) {
  const eventType = cleanCopy(item.event_type || item.type || "");
  if (!eventType || !(eventType.includes("whale") || eventType.includes("smart") || eventType.includes("flow") || eventType.includes("volume") || eventType.includes("institutional"))) {
    return null;
  }
  const ticker = itemTicker(item);
  if (!ticker) return null;
  const asset = findAsset(ticker) || {};
  const currentPrice = numberOrNull(item.current_price ?? item.price ?? asset.current_price ?? asset.price ?? asset.reference_price);
  const volume = numberOrNull(item.monitored_volume ?? item.volume ?? asset.volume);
  const avgVolume = numberOrNull(item.avg_volume ?? asset.avg_volume ?? asset.average_volume);
  const relativeVolume = numberOrNull(item.relative_volume ?? (avgVolume && volume ? volume / avgVolume : null));
  const rawMonitoredDollarVolume = numberOrNull(item.monitored_dollar_volume ?? item.dollar_volume);
  const monitoredDollarVolume = rawMonitoredDollarVolume ?? (currentPrice !== null && volume !== null ? currentPrice * volume : null);
  const confirmedCheck = saneFlowAmount(item.confirmed_amount_usd ?? item.amount_usd, monitoredDollarVolume);
  const confirmedAmount = confirmedCheck.value;
  const confirmed = Boolean(item.confirmed || (item.entity_name && confirmedAmount !== null));
  const direction = item.estimated_flow_direction || item.direction_estimate || item.direction || (["buy", "accumulation"].includes(item.action) ? "inflow" : ["sell", "reduction", "distribution"].includes(item.action) ? "outflow" : "neutral");
  const display = getAssetDisplayName(ticker).displayName;
  const rawName = cleanCopy(item.asset_name || "");
  return {
    id: whaleItemId(item),
    ticker,
    assetName: rawName && rawName !== ticker ? rawName : display,
    event: confirmed ? "Ballena confirmada" : "Smart money estimado",
    direction,
    entity: item.entity_name || item.entity || "",
    amount: confirmed ? confirmedAmount : null,
    amountUsd: confirmedAmount,
    estimatedValue: null,
    units: item.amount_asset ?? item.units ?? item.amount ?? "",
    price: item.price_used || item.price || currentPrice || "",
    currentPrice: currentPrice || "",
    volume: volume || "",
    avgVolume: avgVolume || "",
    relativeVolume: relativeVolume || "",
    dollarVolume: monitoredDollarVolume,
    monitoredDollarVolume,
    estimatedFlow: item.estimated_flow ?? item.net_flow ?? "",
    amountSuspicious: Boolean(item.amount_suspicious || confirmedCheck.suspicious),
    netFlow: item.net_flow ?? item.estimated_flow ?? monitoredDollarVolume,
    date: item.date || item.timestamp || item.created_at || "",
    source: item.source || "market_flow",
    confidence: item.confidence || (confirmed ? "media" : "baja"),
    intensity: item.relative_volume || item.confidence_score || 1,
    read: item.genesis_reading_es || item.genesis_reading || (confirmed
      ? "Entidad y monto vienen de fuente reportada; Genesis lo trata como evidencia, no como garantia."
      : "Flujo estimado por volumen/precio; no confirma entidad ni compra directa."),
    missing: confirmed
      ? "Vigilar continuidad de precio, volumen y catalizador."
      : "No confirma compra directa ni entidad; sirve como señal secundaria.",
    eventType,
    confirmed,
    raw: item,
  };
}

function hydrateWhaleRowWithLiveAsset(row = {}) {
  const ticker = itemTicker(row);
  if (!ticker) return row;
  const chartAsset = chartQuoteAsset(ticker, appState.assetChartRanges?.[ticker] || "1D");
  const asset = chartAsset || findAsset(ticker) || {};
  const price = positiveOrNull(row.price ?? row.price_used ?? row.currentPrice ?? row.current_price) ?? itemPrice(asset);
  const volume = positiveOrNull(row.volume ?? row.monitoredVolume ?? row.monitored_volume) ?? positiveOrNull(asset.volume);
  const avgVolume = positiveOrNull(row.avgVolume ?? row.avg_volume) ?? positiveOrNull(asset.avg_volume ?? asset.average_volume);
  const relativeVolume = numberOrNull(row.relativeVolume ?? row.relative_volume) ?? numberOrNull(asset.relative_volume ?? asset.relativeVolume) ?? (volume !== null && avgVolume ? volume / avgVolume : null);
  const existingDollar = positiveOrNull(row.monitoredDollarVolume ?? row.monitored_dollar_volume ?? row.dollarVolume ?? row.dollar_volume);
  const dollarVolume = existingDollar ?? (price !== null && volume !== null ? price * volume : null);
  const source = cleanCopy(row.source || "");
  const display = getAssetDisplayName(ticker).displayName;
  const hydrated = {
    ...row,
    ticker,
    assetName: row.assetName || row.asset_name || display,
  };
  if (price !== null) {
    hydrated.price = price;
    hydrated.price_used = price;
    hydrated.currentPrice = price;
    hydrated.current_price = price;
  }
  if (volume !== null) {
    hydrated.volume = volume;
    hydrated.monitoredVolume = volume;
    hydrated.monitored_volume = volume;
  }
  if (avgVolume !== null) {
    hydrated.avgVolume = avgVolume;
    hydrated.avg_volume = avgVolume;
  }
  if (relativeVolume !== null) {
    hydrated.relativeVolume = relativeVolume;
    hydrated.relative_volume = relativeVolume;
  }
  if (dollarVolume !== null) {
    hydrated.dollarVolume = dollarVolume;
    hydrated.dollar_volume = dollarVolume;
    hydrated.monitoredDollarVolume = dollarVolume;
    hydrated.monitored_dollar_volume = dollarVolume;
    hydrated.netFlow = numberOrNull(hydrated.netFlow) ?? dollarVolume;
  }
  if ((price !== null || volume !== null) && (!source || /market_flow|technical|pendiente/i.test(source))) {
    hydrated.source = chartAsset?.source || priceSourceLabel(asset) || "FMP / market_flow";
  }
  if (!cleanCopy(hydrated.read) || /espera volumen|sin volumen|no confirmado/i.test(cleanCopy(hydrated.read))) {
    hydrated.read = dollarVolume !== null
      ? `${display}: ${formatMoneyCompact(dollarVolume)} de volumen vigilado con precio ${money(price, "pendiente")}. No es compra confirmada; es radar de actividad.`
      : `${display}: flujo en vigilancia; Genesis busca precio y volumen live antes de elevar conviccion.`;
  }
  return hydrated;
}

function whaleRowNeedsLiveHydration(row = {}) {
  return Boolean(
    itemTicker(row)
    && (
      positiveOrNull(row.price ?? row.price_used ?? row.currentPrice ?? row.current_price) === null
      || positiveOrNull(row.volume ?? row.monitoredVolume ?? row.monitored_volume) === null
    )
  );
}

function saneFlowAmount(value, dollarVolume = null) {
  const numeric = numberOrNull(value);
  if (numeric === null) return { value: null, suspicious: false };
  if (numeric <= 0 || numeric > 1_000_000_000_000) return { value: null, suspicious: true };
  const dv = numberOrNull(dollarVolume);
  if (dv !== null && dv > 0 && numeric > dv * 20) return { value: null, suspicious: true };
  return { value: numeric, suspicious: false };
}

function whaleFallbackRows() {
  const assets = currentFocusAssets();
  const strongest = assets
    .map((item) => ({ ticker: itemTicker(item), intensity: Math.abs(numberOrNull(item.daily_change_pct) || 0), item }))
    .filter((row) => row.ticker)
    .sort((a, b) => b.intensity - a.intensity);
  return [{
    id: `whale-fallback-${strongest[0]?.ticker || "market"}`,
    ticker: strongest[0]?.ticker || "MERCADO",
    assetName: strongest[0]?.item ? assetDisplayName(strongest[0].item) : "Mercado",
    event: "Smart money estimado",
    direction: "neutral",
    entity: "",
    amount: "",
    estimatedValue: "",
    currentPrice: strongest[0]?.item ? itemPrice(strongest[0].item) : null,
    volume: strongest[0]?.item?.volume || null,
    relativeVolume: strongest[0]?.item?.relative_volume || null,
    dollarVolume: strongest[0]?.item && itemPrice(strongest[0].item) && strongest[0].item.volume ? itemPrice(strongest[0].item) * strongest[0].item.volume : null,
    date: strongest[0]?.item?.quote_timestamp || strongest[0]?.item?.updated_at || appState.lastUpdated,
    source: "technical / market_flow",
    confidence: "baja",
    intensity: strongest[0]?.intensity || 0,
    read: "No hay ballenas confirmadas, pero Genesis vigila flujo institucional, volumen anormal y acumulacion/distribucion en tus activos.",
    missing: "Falta entidad institucional, monto y fuente directa para llamarlo ballena confirmada.",
  }];
}

function flowDirectionLabel(item, relativeVolume) {
  const raw = `${item.direction || ""} ${item.primary_label || ""} ${item.signal || ""}`.toLowerCase();
  if (/outflow|salida|venta|distrib/.test(raw)) return "Salida estimada";
  if (/inflow|entrada|compra|acumul/.test(raw)) return "Entrada estimada";
  if ((numberOrNull(relativeVolume) || 0) >= 1.8) return "Volumen anormal";
  return "Flujo no confirmado";
}

function classifyWhaleType(value) {
  const text = String(value || "").toLowerCase();
  if (text.includes("buy") || text.includes("compra") || text.includes("acquir")) return "Compra";
  if (text.includes("sell") || text.includes("venta") || text.includes("dispos")) return "Venta";
  if (text.includes("reduc")) return "Reduccion";
  if (text.includes("hold") || text.includes("acumul")) return "Acumulacion";
  return "No confirmado";
}

function whaleRowMarkup(row) {
  const display = getAssetDisplayName(row.ticker);
  const whaleId = whaleItemId(row);
  const confirmed = Boolean(row.confirmed || (row.entity && numberOrNull(row.amount || row.amountUsd) !== null && !row.amountSuspicious));
  const directionTone = String(row.direction || "").includes("out") ? "down" : String(row.direction || "").includes("in") ? "up" : "flat";
  const monitored = numberOrNull(row.monitoredDollarVolume || row.dollarVolume);
  const relVolume = numberOrNull(row.relativeVolume);
  const amountLabel = confirmed
    ? money(row.amount || row.amountUsd, "Monto pendiente")
    : money(monitored, "Esperando volumen FMP");
  const priceLabel = money(row.price || row.currentPrice, "Precio pendiente");
  const volumeLabel = row.volume ? compactNumber(row.volume) : "Volumen pendiente";
  const relLabel = relVolume !== null ? `${compactNumber(relVolume)}x` : "Rel. pendiente";
  const directionLabel = directionTone === "down" ? "Salida / distribucion" : directionTone === "up" ? "Entrada / acumulacion" : "Flujo vigilado";
  const rowRead = confirmed
    ? cleanCopy(row.read || `${directionLabel}: ${amountLabel} reportados por ${row.entity || "fuente activa"}.`)
    : monitored !== null
      ? `${directionLabel}: ${amountLabel} de volumen vigilado en ${display.displayName}. No lo trato como compra confirmada.`
      : `${directionLabel}: FMP aun no entrego volumen suficiente para cuantificar. Genesis no inventa monto.`;
  return `
    <article class="whale-row feed-row investing-event-card flow-${directionTone}" data-whale-id="${escapeHtml(whaleId)}">
      <button class="event-main" type="button" data-whale-id="${escapeHtml(whaleId)}">
      <div class="whale-topline investing-event-topline">
        <div>
          <strong>${escapeHtml(display.displayName)}</strong>
          <small>${escapeHtml(directionLabel)}</small>
        </div>
        <span class="event-chip ${directionTone}">${escapeHtml(confirmed ? "Confirmada" : "Vigilado")}</span>
      </div>
      <div class="flow-strip whale-signal-strip">
        <span>${escapeHtml(priceLabel)}</span>
        <div class="mini-spark" aria-hidden="true">${miniSeriesBars([row.netFlow, monitored, row.relativeVolume, row.intensity])}</div>
        <strong>${escapeHtml(row.amountSuspicious ? "Monto no validado" : amountLabel)}</strong>
      </div>
      <div class="whale-metrics-row">
        <span>${escapeHtml(volumeLabel)}</span>
        <span>${escapeHtml(relLabel)}</span>
        <span>${escapeHtml(cleanCopy(row.source || "FMP / market flow"))}</span>
        <span>${escapeHtml(cleanCopy(row.confidence || "media"))}</span>
      </div>
      <p>${escapeHtml(rowRead)}</p>
      </button>
    </article>
  `;
}

function bindMoneyFlowJarvisForm() {
  const form = document.getElementById("money-flow-jarvis-form");
  if (!form) return;
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const input = document.getElementById("money-flow-jarvis-input");
    const question = String(input?.value || "").trim();
    if (!question) return;
    const answerNode = document.getElementById("money-flow-jarvis-answer");
    answerNode.textContent = "Consultando Ballenas...";
    try {
      const payload = await getJson(`/api/dashboard/money-flow/jarvis?q=${encodeURIComponent(question)}`);
      renderMoneyFlowJarvisAnswer(payload);
    } catch (error) {
      answerNode.textContent = cleanCopy(error.message || "No pude consultar Ballenas.");
    }
  });
}

function renderMoneyFlowJarvisAnswer(payload = {}) {
  const node = document.getElementById("money-flow-jarvis-answer");
  if (!node) return;
  const premium = Array.isArray(payload.premium_activity) ? payload.premium_activity : [];
  const items = Array.isArray(payload.items) ? payload.items : [];
  node.innerHTML = `
    <strong>${escapeHtml(cleanCopy(payload.matched_ticker || "Lectura smart money"))}</strong>
    <p>${escapeHtml(cleanCopy(payload.answer || "No hay ballena identificada con la fuente activa."))}</p>
    ${premium.length ? `<div class="whale-premium-list">${premium.slice(0, 3).map((item) => `
      <span>
        <b>${escapeHtml(cleanCopy(item.entity || "Entidad no confirmada"))}</b>
        <small>${escapeHtml(cleanCopy(item.type || item.source || "Movimiento"))} | ${escapeHtml(cleanCopy(item.date || "sin fecha"))}</small>
      </span>
    `).join("")}</div>` : ""}
    ${!premium.length && items.length ? `<small>Flujo local en vigilancia; sin entidad premium confirmada.</small>` : ""}
  `;
}

async function loadAlerts() {
  appState.alertsSnapshot = await getJson("/api/dashboard/alerts");
  return appState.alertsSnapshot;
}

function mergeAlertRowsWithOpportunities(rows = []) {
  const seen = new Set(rows.map((item) => alertItemId(item)));
  const opportunities = marketOpportunityRowsForUi(rows);
  const merged = [];
  opportunities.forEach((item) => {
    const id = alertItemId(item);
    if (seen.has(id)) return;
    seen.add(id);
    merged.push(item);
  });
  rows.forEach((item) => {
    const id = alertItemId(item);
    if (id && merged.some((existing) => alertItemId(existing) === id)) return;
    merged.push(item);
  });
  return merged.slice(0, 14);
}

function marketOpportunityRowsForUi(existingRows = []) {
  const existingTickers = new Set((existingRows || []).map(itemTicker).filter(Boolean));
  const rows = Object.entries(appState.opportunityQuotes || {})
    .map(([ticker, item]) => ({ ticker, item }))
    .filter(({ item }) => item && itemPrice(item) !== null)
    .map(({ ticker, item }) => {
      const price = itemPrice(item);
      const volume = numberOrNull(item.volume);
      const dollarVolume = price !== null && volume !== null ? price * volume : null;
      const changePct = itemDailyPct(item) || 0;
      const support = numberOrNull(item.day_low || item.dayLow);
      const resistance = numberOrNull(item.day_high || item.dayHigh);
      const strategy = localStrategyForOpportunity(ticker, { price, volume, dollarVolume, changePct, support, resistance });
      return { ticker, item, price, volume, dollarVolume, changePct, support, resistance, strategy };
    })
    .filter((row) => {
      if (existingTickers.has(row.ticker) && !row.strategy.isStrong) return false;
      return row.strategy.isImportant;
    })
    .sort((a, b) => (b.strategy.score - a.strategy.score) || ((b.dollarVolume || 0) - (a.dollarVolume || 0)))
    .slice(0, 5);
  return rows.map(({ ticker, item, price, volume, dollarVolume, changePct, support, resistance, strategy }) => ({
    id: `opportunity:${ticker}`,
    alert_id: `opportunity:${ticker}`,
    ticker,
    asset_name: getAssetDisplayName(item).displayName,
    title: `${ticker}: oportunidad externa detectada`,
    title_es: `${ticker}: oportunidad externa detectada`,
    summary: `${ticker}: ${formatPercent(changePct)} con ${formatMoneyCompact(dollarVolume, "volumen pendiente")} de flujo FMP. ${strategy.summary}`,
    summary_es: `${ticker}: ${formatPercent(changePct)} con ${formatMoneyCompact(dollarVolume, "volumen pendiente")} de flujo FMP. ${strategy.summary}`,
    alert_type: "opportunity_scan",
    source: "FMP oportunidad",
    status: "opportunity",
    is_opportunity: true,
    price,
    current_price: price,
    change: itemDailyUsd(item),
    change_pct: changePct,
    volume,
    dollar_volume: dollarVolume,
    support,
    resistance,
    severity: strategy.score >= 72 ? "high" : "medium",
    confidence: strategy.score >= 65 ? "medium" : "low",
    impact: changePct > 0 ? "bullish" : changePct < 0 ? "bearish" : "neutral",
    direction: changePct > 0 ? "bullish" : changePct < 0 ? "bearish" : "neutral",
    trend: changePct > 0 ? "alcista en validación" : changePct < 0 ? "bajista en vigilancia" : "rango en validación",
    momentum: strategy.label,
    risk: strategy.invalidation,
    strategy,
    what_it_means: strategy.summary,
    what_to_watch: strategy.validation.join("; "),
    genesis_reading: strategy.summary,
    genesis_reading_es: strategy.summary,
    mini_series: [changePct, dollarVolume ? Math.log10(Math.abs(dollarVolume)) : 0, strategy.score],
    created_at: new Date().toISOString(),
    affected_portfolio_assets: [],
    affected_watchlist_assets: [],
  }));
}

function localStrategyForOpportunity(ticker, fields = {}) {
  const changePct = numberOrNull(fields.changePct) || 0;
  const dollarVolume = numberOrNull(fields.dollarVolume);
  const volume = numberOrNull(fields.volume);
  const score = Math.max(0, Math.min(100,
    44
    + Math.min(18, Math.abs(changePct) * 4)
    + (dollarVolume ? Math.min(24, Math.log10(Math.abs(dollarVolume) + 1) * 2.2) : 0)
    + (volume && volume > 10_000_000 ? 8 : 0)
  ));
  const grade = score >= 72 ? "A" : score >= 60 ? "B" : score >= 50 ? "C" : "D";
  const label = grade === "A" ? "oportunidad fuerte en validación" : grade === "B" ? "oportunidad importante" : "radar activo";
  const validation = [
    fields.resistance ? `cierre arriba de ${money(fields.resistance)}` : "ruptura de rango con vela firme",
    "volumen sosteniéndose",
    "catalizador/noticia alineado",
  ];
  const invalidation = fields.support ? `pierde ${money(fields.support)}` : "pierde estructura intradía";
  return {
    name: "Genesis 10% mensual - validación por precio, volumen y catalizador",
    grade,
    score: Math.round(score),
    label,
    isImportant: score >= 58 || (dollarVolume !== null && dollarVolume >= 1_000_000_000),
    isStrong: score >= 70,
    bias: changePct > 0 ? "bullish" : changePct < 0 ? "bearish" : "neutral",
    validation,
    entry_condition: "paper solo si confirma ruptura/retest con volumen; no perseguir vela extendida.",
    invalidation,
    risk_note: "No broker, no compra real: solo radar y paper.",
    summary: `${ticker}: ${label} (${grade}, ${Math.round(score)}/100). Validar ${validation[0]} y ${validation[1]}; invalidar si ${invalidation}.`,
  };
}

function renderAlertsScreen() {
  const root = document.getElementById("view-alerts");
  if (!root) return;
  ensureMarketPulse();
  ensureOpportunityQuotes();
  const items = Array.isArray(appState.alertsSnapshot?.items)
    ? appState.alertsSnapshot.items
    : Array.isArray(appState.alertsSnapshot?.recent_alerts)
      ? appState.alertsSnapshot.recent_alerts
      : [];
  const baseAlertRows = (items.length ? items : derivedAlertRows()).map(normalizeAlertRowForUi);
  const alertRows = mergeAlertRowsWithOpportunities(baseAlertRows);
  const whales = extractWhaleRows(appState.whalesSnapshot?.causal || {}, appState.whalesSnapshot?.detection || {});
  const hasConfirmedWhales = whales.some((row) => row.entity && numberOrNull(row.amount) !== null && !row.amountSuspicious);
  const whaleRows = whales.length ? whales : whaleFallbackRows();
  indexAlertItems(alertRows);
  indexWhaleItems(whaleRows);
  root.innerHTML = `
    <section class="screen-stack alerts-investing-screen">
      ${marketPulseHeroMarkup("alerts")}
      <div class="subtabs" aria-label="Eventos">
        <button type="button" class="${appState.alertSubtab === "alerts" ? "is-active" : ""}" data-alert-tab="alerts">Alertas</button>
        <button type="button" class="${appState.alertSubtab === "whales" ? "is-active" : ""}" data-alert-tab="whales">Ballenas</button>
      </div>
      ${appState.alertSubtab === "alerts" ? alertsPanelMarkupV2(alertRows) : whalesPanelMarkup(whaleRows, hasConfirmedWhales)}
    </section>
  `;
  bindMoneyFlowJarvisForm();
}

function normalizeAlertRowForUi(item = {}) {
  const ticker = itemTicker(item);
  if (!ticker || ticker === "MERCADO") return item;
  const asset = findAsset(ticker) || {};
  const price = numberOrNull(item.price ?? asset.current_price ?? asset.price ?? asset.reference_price);
  const changePct = numberOrNull(item.change_pct ?? item.daily_change_pct ?? asset.daily_change_pct ?? asset.change_pct);
  const change = numberOrNull(item.change ?? item.daily_change ?? asset.daily_change ?? asset.change);
  const volume = numberOrNull(item.volume ?? asset.volume);
  const avgVolume = numberOrNull(item.avg_volume ?? asset.avg_volume ?? asset.average_volume);
  const relativeVolume = numberOrNull(item.relative_volume ?? (avgVolume && volume ? volume / avgVolume : null));
  const dollarVolume = numberOrNull(item.dollar_volume) ?? (price !== null && volume !== null ? price * volume : null);
  const display = getAssetDisplayName(ticker).displayName;
  const direction = changePct === null ? "vigilancia de precio" : changePct > 0.2 ? "presión compradora" : changePct < -0.2 ? "presión vendedora" : "rango lateral";
  const generatedTitle = `${display}: ${direction}`;
  const generatedSummary = `${ticker}: ${price !== null ? money(price) : "precio pendiente"} ${changePct !== null ? formatPercent(changePct) : ""}. ${volume ? `Volumen ${compactNumber(volume)}` : "Volumen live pendiente"}.`;
  const title = cleanCopy(item.title_es || item.title || generatedTitle);
  const summary = cleanCopy(item.summary_es || item.summary || generatedSummary);
  return {
    ...item,
    title,
    title_es: title,
    summary,
    summary_es: summary,
    price,
    change,
    change_pct: changePct,
    volume,
    avg_volume: avgVolume,
    relative_volume: relativeVolume,
    dollar_volume: dollarVolume,
    source: item.source || priceSourceLabel(asset),
    mini_series: item.mini_series || [changePct, relativeVolume, change].filter((value) => value !== null),
    strategy: item.strategy || localStrategyForOpportunity(ticker, { price, volume, dollarVolume, changePct, support: item.support, resistance: item.resistance }),
    what_it_means: item.what_it_means || `${display} queda en ${direction}; Genesis mira precio, volumen y soporte antes de subir convicción.`,
    what_to_watch: item.what_to_watch || "Confirmar volumen relativo, ruptura de rango y reacción en soporte/resistencia.",
    genesis_reading: item.genesis_reading || "Señal derivada de mercado live; no es orden, es radar operativo.",
  };
}

function derivedAlertRows() {
  const focusAssets = currentFocusAssets();
  if (!focusAssets.length) {
    return [{
      ticker: "Mercado",
      title: "Mercado: FMP/RSS en vigilancia",
      summary: "Genesis esta esperando precio, volumen o noticia confirmada para elevar una alerta accionable.",
      impact: "neutral",
      direction: "neutral",
      severity: "medium",
      confidence: "medium",
      source: "FMP / RSS",
      price: null,
      change: null,
      change_pct: null,
      volume: null,
      relative_volume: null,
      dollar_volume: null,
      trend: "sin dirección confirmada",
      momentum: "esperando fuente live",
      risk: "riesgo de operar sin confirmación",
      what_it_means: "No hay activo de seguimiento cargado; Genesis no inventa señales.",
      what_to_watch: "Agregar activos a Seguimiento y confirmar FMP live.",
      mini_series: [1, 2, 1],
      genesis_reading: "Alerta de sistema limpia: sin activos no hay señal operable.",
      created_at: appState.lastUpdated || new Date().toISOString(),
      context: "Mercado",
      status: "En vigilancia",
    }];
  }
  return focusAssets
    .map((item) => {
      const ticker = itemTicker(item);
      const pct = itemDailyPct(item);
      const change = itemDailyUsd(item);
      const price = itemPrice(item);
      const volume = numberOrNull(item.volume);
      const avgVolume = numberOrNull(item.avg_volume ?? item.avgVolume ?? item.average_volume);
      const relativeVolume = numberOrNull(item.relative_volume ?? item.relativeVolume) ?? (volume !== null && avgVolume ? volume / avgVolume : null);
      const absPct = Math.abs(pct || 0);
      if (!ticker || (price === null && pct === null && volume === null)) return null;
      const direction = (pct || 0) > 0 ? "bullish" : (pct || 0) < 0 ? "bearish" : "neutral";
      const title = pct === null || absPct < 1
        ? `${ticker}: precio live en vigilancia`
        : pct > 0 ? `${ticker}: impulso positivo` : `${ticker}: presion bajista`;
      return {
        ticker,
        daily_change_pct: pct,
        title,
        summary: `Precio ${money(price, "sin precio")} con movimiento ${formatChange(change, "sin cambio")} / ${formatPercent(pct, "sin dato")}. Revisar volumen, soporte/resistencia y noticia asociada antes de operar.`,
        impact: direction === "bullish" ? "positivo" : direction === "bearish" ? "negativo" : "neutral",
        direction,
        severity: absPct >= 3 ? "high" : "medium",
        confidence: "medium",
        source: priceSourceLabel(item),
        price,
        change,
        change_pct: pct,
        volume,
        avg_volume: avgVolume,
        relative_volume: relativeVolume,
        dollar_volume: price !== null && volume !== null ? price * volume : null,
        support: numberOrNull(item.support ?? item.support_level ?? item.day_low ?? item.dayLow),
        resistance: numberOrNull(item.resistance ?? item.resistance_level ?? item.day_high ?? item.dayHigh),
        trend: (pct || 0) > 1.5 ? "alcista intradia" : (pct || 0) < -1.5 ? "bajista intradia" : "lateral",
        momentum: relativeVolume !== null && relativeVolume >= 1.5 ? "volumen acompana" : "pendiente de volumen",
        risk: (pct || 0) < 0 ? "riesgo de continuidad bajista" : "riesgo de falso rompimiento",
        what_it_means: (pct || 0) > 0 ? `${ticker} intenta extender momentum; falta confirmar volumen.` : (pct || 0) < 0 ? `${ticker} muestra presion; cuidar nivel de soporte.` : `${ticker} no marca direccion clara; Genesis vigila ruptura de rango y volumen.`,
        what_to_watch: "Precio, volumen relativo y reaccion en soporte/resistencia.",
        affected_portfolio_assets: [ticker],
        mini_series: [pct, relativeVolume, change].filter((value) => value !== null),
        genesis_reading: "Alerta tecnica derivada de precio live; confirmar con volumen antes de operar.",
        created_at: item.quote_timestamp || item.updated_at || appState.lastUpdated,
        context: "Precio live",
        status: "En vigilancia",
      };
    })
    .filter(Boolean)
    .sort((a, b) => Math.abs(numberOrNull(b.daily_change_pct) || 0) - Math.abs(numberOrNull(a.daily_change_pct) || 0))
    .slice(0, 10);
}

function alertsPanelMarkup(items) {
  const rows = items.length ? items : derivedAlertRows();
  const high = rows.filter((item) => String(item.severity || "").toLowerCase() === "high").length;
  const withPrice = rows.filter((item) => item.price !== null && item.price !== undefined).length;
  const withVolume = rows.filter((item) => item.volume || item.relative_volume || item.dollar_volume).length;
  const topRows = rows.slice(0, 3);
  const summary = topRows.map((item) => {
    const ticker = itemTicker(item) || "Mercado";
    const move = formatPercent(item.change_pct, "");
    const volume = item.volume || item.relative_volume || item.dollar_volume ? "con volumen" : "sin volumen live";
    return `${ticker}${move ? ` ${move}` : ""} ${volume}`;
  }).join(" · ");
  return `
    <section class="market-pulse-card alert-pulse alert-summary-card">
      <div>
        <span>Resumen de alertas</span>
        <strong>${escapeHtml(`${rows.length} señales · ${high} alta prioridad`)}</strong>
        <p>${escapeHtml(summary || `${withPrice} con precio directo, ${withVolume} con volumen/flujo. Genesis espera señal real antes de gritar alerta.`)}</p>
      </div>
    </section>
    <div class="asset-list investing-event-list">
      ${rows.slice(0, 14).map(alertMarkup).join("")}
    </div>
  `;
}

function alertItemId(item) {
  const explicit = cleanCopy(item?.alert_id || item?.id || "");
  if (explicit) return explicit;
  const raw = [
    itemTicker(item) || "Mercado",
    item?.alert_type || item?.type || "",
    item?.title || item?.event || "",
    item?.created_at || item?.updated_at || item?.timestamp || "",
    item?.source || "",
  ].join("|");
  return `alert-${simpleHash(raw)}`;
}

function indexAlertItems(items) {
  appState.alertItemsById = {};
  (items || []).forEach((item) => {
    const id = alertItemId(item);
    appState.alertItemsById[id] = { ...item, alert_id: id };
  });
}

function whaleFlowBoardMarkup(rows = []) {
  const confirmed = rows.filter((row) => row.confirmed || (row.entity && numberOrNull(row.amount || row.amountUsd) !== null && !row.amountSuspicious));
  const estimated = rows.filter((row) => !confirmed.includes(row));
  const confirmedValue = confirmed.reduce((sum, row) => sum + (numberOrNull(row.amount || row.amountUsd) || 0), 0);
  const monitoredValue = estimated.reduce((sum, row) => sum + (numberOrNull(row.monitoredDollarVolume || row.dollarVolume) || 0), 0);
  const inflow = rows.filter((row) => String(row.direction || "").includes("in")).length;
  const outflow = rows.filter((row) => String(row.direction || "").includes("out")).length;
  return `
    <section class="whale-flow-card investing-flow-board whale-render-board">
      <div>
        <span>Ballenas</span>
        <strong>${escapeHtml(`${confirmed.length} confirmadas / ${estimated.length} estimadas`)}</strong>
        <p>${escapeHtml(rows.length ? "Genesis separa monto confirmado de volumen vigilado. Nada se presenta como compra real sin fuente." : "Esperando flujo confirmado desde fuentes activas.")}</p>
      </div>
      <div class="whale-board-bars" aria-hidden="true">${miniSeriesBars(rows.map((row) => row.amount || row.amountUsd || row.monitoredDollarVolume || row.dollarVolume || row.volume), 52)}</div>
      <div class="whale-board-metrics">
        <span>Entradas ${escapeHtml(String(inflow))}</span>
        <span>Salidas ${escapeHtml(String(outflow))}</span>
        <span>Flujo confirmado ${escapeHtml(formatMoneyCompact(confirmedValue, "$0"))}</span>
        <span>Volumen vigilado ${escapeHtml(formatMoneyCompact(monitoredValue, "Pendiente"))}</span>
      </div>
    </section>
  `;
}

function whalesPanelMarkup(rows, hasConfirmed = false) {
  return `
    ${whaleFlowBoardMarkup(rows)}
    <div class="compact-actions whale-actions">
      <button type="button" class="icon-action search-toggle" data-toggle-search="whales" aria-label="${appState.searchOpen.whales ? "Cerrar consulta" : "Consultar ballenas"}">${iconSvg("search")}</button>
    </div>
    ${appState.searchOpen.whales ? `
      <form class="search-card whale-search premium-search inline-search" id="money-flow-jarvis-form">
        <input id="money-flow-jarvis-input" placeholder="Consultar ticker o flujo smart money" autocomplete="off">
        <button type="submit">${iconSvg("send")}</button>
      </form>
      <div class="whale-answer" id="money-flow-jarvis-answer">Lectura Genesis lista para consultar.</div>
    ` : ""}
    <div class="asset-list whales-list investing-event-list" id="whales-list">
      ${rows.length ? rows.map(whaleRowMarkupV2).join("") : emptyStateMarkup("Sin ballenas confirmadas.", "Cuando FMP confirme entidad, monto y fecha, Genesis lo mostrara aqui sin inventar instituciones.")}
    </div>
  `;
}

function whaleItemId(item) {
  const explicit = cleanCopy(item?.id || item?.whale_id || "");
  if (explicit) return explicit;
  const raw = [
    item?.ticker || "MERCADO",
    item?.event || item?.action || "",
    item?.date || item?.timestamp || "",
    item?.source || "",
  ].join("|");
  return `whale-${simpleHash(raw)}`;
}

function indexWhaleItems(items) {
  appState.whaleItemsById = {};
  (items || []).forEach((item) => {
    const hydrated = hydrateWhaleRowWithLiveAsset(item);
    const id = whaleItemId(hydrated);
    appState.whaleItemsById[id] = { ...hydrated, id };
  });
}

function alertMarkup(item) {
  const ticker = itemTicker(item) || "Mercado";
  const display = ticker === "Mercado" ? { displayName: "Mercado", subtitle: "" } : getAssetDisplayName(ticker);
  const priority = cleanCopy(item.priority || item.severity || item.status_label || item.status || "Seguimiento");
  const date = item.created_at || item.updated_at || item.timestamp || appState.lastUpdated;
  const impact = item.impact || item.impact_probable || item.latest_validation?.outcome_label || priority;
  const tone = newsImpactTone(impact);
  const alertId = alertItemId(item);
  const priceLabel = item.price === null || item.price === undefined ? "No aplica" : money(item.price, "No aplica");
  const changeLabel = formatPercent(item.change_pct, "Sin dato");
  return `
    <article class="whale-row feed-row alert-event investing-event-card tone-${tone}" data-alert-id="${escapeHtml(alertId)}">
      <button class="event-main" type="button" data-alert-id="${escapeHtml(alertId)}">
      <div class="whale-topline investing-event-topline">
        <div>
          <strong>${escapeHtml(display.displayName)}</strong>
          <small>${escapeHtml(cleanCopy(item.title_es || item.title || item.event || item.status || "Alerta"))}</small>
        </div>
        <span class="quote-stack ${tone}">
          <b>${escapeHtml(priceLabel)}</b>
          <small>${escapeHtml(changeLabel)}</small>
        </span>
      </div>
      <p>${escapeHtml(cleanCopy(item.summary_es || item.summary || item.message || item.note || "Revisar antes de operar."))}</p>
      <div class="signal-strip">
        <span class="event-chip">${escapeHtml(priority)}</span>
        <div class="mini-spark" aria-hidden="true">${miniSeriesBars(item.mini_series || [item.change_pct, item.relative_volume, item.signal_strength])}</div>
        <strong>${escapeHtml(cleanCopy(item.confidence || "media"))}</strong>
      </div>
      <div class="asset-meta investing-meta">
        <span class="${tone}">Impacto: ${escapeHtml(cleanCopy(impact || "Por confirmar"))}</span>
        <span>Precio: ${escapeHtml(item.price === null || item.price === undefined ? "No aplica a precio directo" : money(item.price, "No aplica a precio directo"))}</span>
        <span>Cambio: ${escapeHtml(changeLabel)}</span>
        <span>Volumen: ${escapeHtml(item.volume ? compactNumber(item.volume) : "Sin volumen")}</span>
        <span>Vol. rel: ${escapeHtml(item.relative_volume ? `${compactNumber(item.relative_volume)}x` : "Sin dato")}</span>
        <span>Volumen $: ${escapeHtml(money(item.dollar_volume, "Sin dato"))}</span>
        <span>Tendencia: ${escapeHtml(cleanCopy(item.trend || "Sin confirmar"))}</span>
        <span>Momentum: ${escapeHtml(cleanCopy(item.momentum || "Vigilancia"))}</span>
        <span>Contexto: ${escapeHtml(cleanCopy(item.context || item.category || "Mercado"))}</span>
        <span>Fecha: ${escapeHtml(formatDate(date))}</span>
        <span>Estado: ${escapeHtml(cleanCopy(item.status || "En vigilancia"))}</span>
      </div>
      </button>
    </article>
  `;
}

function alertsPanelMarkupV2(items) {
  const rows = items.length ? items : derivedAlertRows();
  const high = rows.filter((item) => String(item.severity || "").toLowerCase() === "high").length;
  const opportunities = rows.filter((item) => item.is_opportunity || String(item.alert_type || "").includes("opportunity")).length;
  const summary = rows.slice(0, 3).map((item) => {
    const ticker = itemTicker(item) || "Mercado";
    const move = formatPercent(item.change_pct, "0.00%");
    const flow = item.dollar_volume ? formatMoneyCompact(item.dollar_volume) : item.volume ? formatVolumeCompact(item.volume) : "volumen pendiente";
    return `${ticker} ${move}: ${flow}`;
  }).join("  /  ");
  return `
    <section class="market-pulse-card alert-pulse alert-summary-card">
      <div>
        <span>Resumen de alertas</span>
        <strong>${escapeHtml(`${rows.length} señales vivas / ${high} alta prioridad / ${opportunities} oportunidades`)}</strong>
        <p>${escapeHtml(summary || "Genesis espera ruptura, volumen inusual o noticia confirmada para elevar urgencia.")}</p>
      </div>
      <div class="alert-summary-bars" aria-hidden="true">${miniSeriesBars(rows.map((item) => item.dollar_volume || item.volume || item.change_pct), 34)}</div>
    </section>
    <div class="asset-list investing-event-list">
      ${rows.slice(0, 14).map(alertMarkupV2).join("")}
    </div>
  `;
}

function alertMarkupV2(item) {
  const ticker = itemTicker(item) || "Mercado";
  const display = ticker === "Mercado" ? { displayName: "Mercado", subtitle: "" } : getAssetDisplayName(ticker);
  const impact = item.impact || item.impact_probable || item.latest_validation?.outcome_label || item.severity || "Vigilancia";
  const tone = newsImpactTone(impact);
  const alertId = alertItemId(item);
  const priceText = item.price === null || item.price === undefined ? "No aplica" : money(item.price, "No aplica");
  const changeText = formatPercent(item.change_pct, "Sin dato");
  const flowText = item.dollar_volume ? formatMoneyCompact(item.dollar_volume) : item.volume ? formatVolumeCompact(item.volume) : "Pendiente";
  const strategy = item.strategy || {};
  const opportunity = item.is_opportunity || String(item.alert_type || "").includes("opportunity");
  const read = alertVisualDigest(item);
  return `
    <article class="whale-row feed-row alert-event investing-event-card tone-${tone}" data-alert-id="${escapeHtml(alertId)}">
      <button class="event-main" type="button" data-alert-id="${escapeHtml(alertId)}">
        <div class="whale-topline investing-event-topline">
          <div>
            <strong>${escapeHtml(display.displayName)}</strong>
            <small>${escapeHtml(cleanCopy(opportunity ? `Oportunidad · ${item.title_es || item.title || "scan activo"}` : item.title_es || item.title || "Alerta Genesis"))}</small>
          </div>
          <span class="quote-stack ${tone}">
            <b>${escapeHtml(priceText)}</b>
            <small>${escapeHtml(changeText)}</small>
          </span>
        </div>
        <p>${escapeHtml(read)}</p>
        <div class="alert-signal-panel">
          <div class="mini-spark alert-spark" aria-hidden="true">${miniSeriesBars(item.mini_series || [item.change_pct, item.relative_volume, item.signal_strength, item.dollar_volume], 30)}</div>
          <span><small>Flujo</small><strong>${escapeHtml(flowText)}</strong></span>
          <span><small>Vol.</small><strong>${escapeHtml(item.volume ? compactNumber(item.volume) : "Pendiente")}</strong></span>
          <span><small>Score</small><strong>${escapeHtml(strategy.score ? `${strategy.score}/100` : cleanCopy(item.confidence || "media"))}</strong></span>
        </div>
        <div class="asset-meta investing-meta">
          <span class="${tone}">Impacto: ${escapeHtml(cleanCopy(impact))}</span>
          ${strategy.grade ? `<span>Estrategia: ${escapeHtml(cleanCopy(`${strategy.grade} · ${strategy.label || "Validación"}`))}</span>` : ""}
          <span>Soporte: ${escapeHtml(money(item.support, "Pendiente"))}</span>
          <span>Resistencia: ${escapeHtml(money(item.resistance, "Pendiente"))}</span>
          <span>Tendencia: ${escapeHtml(cleanCopy(item.trend || "Vigilancia"))}</span>
          <span>Momentum: ${escapeHtml(cleanCopy(item.momentum || "Confirmacion pendiente"))}</span>
        </div>
      </button>
    </article>
  `;
}

function alertVisualDigest(item = {}) {
  const ticker = itemTicker(item) || "Mercado";
  const price = item.price === null || item.price === undefined ? "precio directo no aplica" : money(item.price);
  const pct = formatPercent(item.change_pct, "0.00%");
  const flow = item.dollar_volume ? formatMoneyCompact(item.dollar_volume) : item.volume ? formatVolumeCompact(item.volume) : "volumen pendiente";
  const support = money(item.support, "soporte pendiente");
  const resistance = money(item.resistance, "resistencia pendiente");
  const strategy = item.strategy?.summary ? ` Estrategia: ${cleanCopy(item.strategy.summary)}` : "";
  if (item.price === null || item.price === undefined) {
    return `${ticker}: alerta macro/mercado. No aplica a precio directo; vigilar impacto en activos relacionados, noticias y volatilidad.`;
  }
  return `${ticker}: ${price} (${pct}). Flujo visible ${flow}; zona ${support} - ${resistance}. Genesis lo usa como vigilancia operativa, no como orden.${strategy}`;
}

function whaleRowMarkupV2(row) {
  const display = getAssetDisplayName(row.ticker);
  const whaleId = whaleItemId(row);
  const confirmed = Boolean(row.confirmed || (row.entity && numberOrNull(row.amount || row.amountUsd) !== null && !row.amountSuspicious));
  const directionTone = String(row.direction || "").includes("out") ? "down" : String(row.direction || "").includes("in") ? "up" : "flat";
  const monitored = numberOrNull(row.monitoredDollarVolume || row.dollarVolume);
  const relVolume = numberOrNull(row.relativeVolume);
  const amountLabel = confirmed ? formatMoneyCompact(row.amount || row.amountUsd, "Monto pendiente") : formatMoneyCompact(monitored, "Esperando volumen FMP");
  const priceLabel = money(row.price || row.currentPrice, "Precio pendiente");
  const volumeLabel = row.volume ? compactNumber(row.volume) : "Volumen pendiente";
  const directionLabel = directionTone === "down" ? "Salida / distribución" : directionTone === "up" ? "Entrada / acumulación" : "Flujo vigilado";
  const gaugeLabel = confirmed ? "monto reportado" : "volumen vigilado";
  const rowRead = confirmed
    ? cleanCopy(row.read || `${directionLabel}: ${amountLabel} reportados por ${row.entity || "fuente activa"}.`)
    : monitored !== null
      ? `${directionLabel}: ${amountLabel} de volumen vigilado en ${display.displayName}. Es señal de actividad, no compra confirmada.`
      : `${directionLabel}: FMP aún no entregó volumen suficiente para cuantificar. Genesis no inventa monto.`;
  return `
    <article class="whale-row feed-row investing-event-card flow-${directionTone}" data-whale-id="${escapeHtml(whaleId)}">
      <button class="event-main" type="button" data-whale-id="${escapeHtml(whaleId)}">
        <div class="whale-topline investing-event-topline">
          <div>
            <strong>${escapeHtml(display.displayName)}</strong>
            <small>${escapeHtml(confirmed ? "Ballena confirmada" : "Flujo vigilado")}</small>
          </div>
          <span class="event-chip ${directionTone}">${escapeHtml(confirmed ? "Confirmada" : "Vigilado")}</span>
        </div>
        <div class="whale-signal-strip ${directionTone}">
          <span>${escapeHtml(priceLabel)}</span>
          ${flowGaugeMarkup(row.amountSuspicious ? null : monitored, directionTone, gaugeLabel)}
          <strong>${escapeHtml(row.amountSuspicious ? "Monto no validado" : amountLabel)}</strong>
        </div>
        <div class="whale-metrics-row">
          <span>Volumen ${escapeHtml(volumeLabel)}</span>
          ${relVolume !== null ? `<span>Rel. ${escapeHtml(`${compactNumber(relVolume)}x`)}</span>` : ""}
          <span>${escapeHtml(cleanCopy(row.source || "FMP"))}</span>
          <span>Conf. ${escapeHtml(cleanCopy(row.confidence || "media"))}</span>
        </div>
        <p>${escapeHtml(cleanCopy(rowRead))}</p>
      </button>
    </article>
  `;
}

function flowGaugeMarkup(value, tone = "flat", label = "flujo") {
  const numeric = Math.abs(numberOrNull(value) || 0);
  const width = numeric ? Math.max(18, Math.min(96, 18 + Math.log10(numeric + 1) * 11)) : 18;
  return `
    <span class="flow-gauge ${tone}" aria-hidden="true">
      <i><b style="width:${width}%"></b></i>
      <small>${escapeHtml(label)}</small>
    </span>
  `;
}

function flowVolumeVisualMarkup(row = {}, confirmed = false) {
  const tone = row.direction === "outflow" ? "down" : row.direction === "inflow" ? "up" : movementTone(row.dollarVolume || row.monitoredDollarVolume || row.amountUsd);
  const price = row.price || row.currentPrice || row.current_price;
  const volume = row.volume || row.monitoredVolume || row.monitored_volume;
  const dollarVolume = row.dollarVolume || row.dollar_volume || row.monitoredDollarVolume || row.monitored_dollar_volume || row.amountUsd || row.amount_usd;
  const rel = row.relativeVolume || row.relative_volume;
  const score = numberOrNull(row.strategy?.score ?? row.intensity ?? row.signal_strength) || (dollarVolume ? Math.min(92, Math.log10(Math.abs(dollarVolume) + 1) * 8) : 36);
  const pressure = Math.max(8, Math.min(100, score));
  const pieces = [
    { label: "Precio", value: money(price, "pendiente"), level: price ? 64 : 18 },
    { label: "Volumen", value: formatVolumeCompact(volume, "pendiente"), level: volume ? Math.min(100, Math.log10(Math.abs(volume) + 1) * 10) : 16 },
    { label: confirmed ? "Monto" : "Vol. $", value: formatMoneyCompact(dollarVolume, "pendiente"), level: pressure },
    { label: "Relativo", value: rel ? `${compactNumber(rel)}x` : "pendiente", level: rel ? Math.min(100, rel * 34) : 20 },
  ];
  return `
    <div class="flow-volume-visual tone-${tone}">
      <div class="flow-volume-main">
        <span>${escapeHtml(confirmed ? "Flujo confirmado" : "Volumen vigilado")}</span>
        <strong>${escapeHtml(formatMoneyCompact(dollarVolume, "Pendiente"))}</strong>
      </div>
      <div class="flow-volume-radar" style="--flow:${pressure}%">
        <i></i><b></b>
      </div>
      <div class="flow-volume-grid">
        ${pieces.map((item) => `
          <span style="--level:${Math.max(6, Math.min(100, item.level))}%">
            <small>${escapeHtml(item.label)}</small>
            <strong>${escapeHtml(item.value)}</strong>
            <i></i>
          </span>
        `).join("")}
      </div>
    </div>
  `;
}

function strategyChecklistMarkup(strategy = {}) {
  if (!strategy || typeof strategy !== "object") return "";
  const validation = Array.isArray(strategy.validation) ? strategy.validation : [];
  return `
    <section class="strategy-card">
      <div>
        <span>Estrategia Genesis</span>
        <strong>${escapeHtml(cleanCopy(strategy.label || strategy.name || "Validación activa"))}</strong>
        <em>${escapeHtml(`Score ${strategy.score ?? "pendiente"} · ${strategy.grade || "radar"}`)}</em>
      </div>
      <ul>
        ${validation.slice(0, 3).map((item) => `<li>${escapeHtml(cleanCopy(item))}</li>`).join("")}
      </ul>
      <p>Entrada paper: ${escapeHtml(cleanCopy(strategy.entry_condition || "esperar confirmación de precio y volumen."))}</p>
      <p>Invalidación: ${escapeHtml(cleanCopy(strategy.invalidation || "si pierde estructura o falla el volumen."))}</p>
    </section>
  `;
}

function currentAlertRows() {
  const items = Array.isArray(appState.alertsSnapshot?.items)
    ? appState.alertsSnapshot.items
    : Array.isArray(appState.alertsSnapshot?.recent_alerts)
      ? appState.alertsSnapshot.recent_alerts
      : [];
  return (items.length ? items : derivedAlertRows()).map(normalizeAlertRowForUi);
}

function currentWhaleRows() {
  const whales = extractWhaleRows(appState.whalesSnapshot?.causal || {}, appState.whalesSnapshot?.detection || {});
  return (whales.length ? whales : whaleFallbackRows()).map(hydrateWhaleRowWithLiveAsset);
}

function openAlertDetail(alertId) {
  if (!Object.keys(appState.alertItemsById || {}).length) indexAlertItems(currentAlertRows());
  const item = appState.alertItemsById?.[alertId];
  if (!item) {
    toast(`No encontre alerta ${cleanCopy(alertId)}. Actualiza Alertas.`, "error");
    return;
  }
  appState.selectedAlertId = alertId;
  const ticker = itemTicker(item) || "Mercado";
  const sheet = document.getElementById("event-sheet");
  const body = document.getElementById("event-sheet-body");
  if (!sheet || !body) return;
  body.innerHTML = `
    <span class="app-kicker">Alerta</span>
    <h2>${escapeHtml(cleanCopy(item.title_es || item.title || "Alerta Genesis"))}</h2>
    <p>${escapeHtml(cleanCopy(item.summary_es || item.summary || item.message || "Evento en vigilancia."))}</p>
    ${eventMetricGridMarkup([
      ["Activo", ticker],
      ["Precio", item.price === null || item.price === undefined ? "No aplica a precio directo" : money(item.price, "No aplica")],
      ["Cambio", `${formatChange(item.change, "Sin dato")} ${formatPercent(item.change_pct, "")}`],
      ["Volumen", item.volume ? compactNumber(item.volume) : "Sin volumen"],
      ["Vol. rel", item.relative_volume ? `${compactNumber(item.relative_volume)}x` : "Sin dato"],
      ["Dollar volume", money(item.dollar_volume, "Sin dato")],
      ["Soporte", money(item.support, "Sin dato")],
      ["Resistencia", money(item.resistance, "Sin dato")],
      ["Tendencia", cleanCopy(item.trend || "Sin confirmar")],
      ["Momentum", cleanCopy(item.momentum || "Vigilancia")],
      ["Riesgo", cleanCopy(item.risk || "Confirmacion pendiente")],
      ["Confianza", cleanCopy(item.confidence || "media")],
      ["Fuente", cleanCopy(item.source || "technical")],
    ])}
    ${flowVolumeVisualMarkup(item, false)}
    <section class="genesis-mini">
      <strong>Lectura Genesis</strong>
      <p>${escapeHtml(cleanCopy(item.genesis_reading_es || item.genesis_reading || "No es orden; sirve para decidir si esperar confirmación o reducir riesgo."))}</p>
      <p>Qué significa: ${escapeHtml(cleanCopy(item.why_it_matters_es || item.what_it_means || alertVisualDigest(item)))}</p>
      <p>Qué vigilar: ${escapeHtml(cleanCopy(item.what_to_watch_es || item.what_to_watch || "Confirmación en volumen, soporte/resistencia y noticias relacionadas."))}</p>
      ${(item.affected_portfolio_assets || []).length ? `<p>Afecta vigilancia/cartera: ${escapeHtml((item.affected_portfolio_assets || []).join(", "))}</p>` : ""}
    </section>
    ${strategyChecklistMarkup(item.strategy)}
    ${ticker !== "Mercado" ? `<button class="secondary-button full" type="button" data-open-asset="${escapeHtml(ticker)}">Ver activo</button>` : ""}
  `;
  sheet.hidden = false;
}

function openWhaleDetail(whaleId) {
  if (!Object.keys(appState.whaleItemsById || {}).length) indexWhaleItems(currentWhaleRows());
  let row = appState.whaleItemsById?.[whaleId];
  if (!row) {
    toast(`No encontre flujo ${cleanCopy(whaleId)}. Actualiza Ballenas.`, "error");
    return;
  }
  row = hydrateWhaleRowWithLiveAsset(row);
  appState.whaleItemsById[whaleId] = { ...row, id: whaleId };
  hydrateOpenWhaleDetailFromChart(row, whaleId);
  appState.selectedWhaleId = whaleId;
  const sheet = document.getElementById("event-sheet");
  const body = document.getElementById("event-sheet-body");
  if (!sheet || !body) return;
  const confirmed = Boolean(row.confirmed || (row.entity && numberOrNull(row.amount || row.amountUsd) !== null && !row.amountSuspicious));
  const movementLabel = confirmed ? "Ballena confirmada" : "Smart money estimado";
  const confirmedAmount = confirmed ? money(row.amount || row.amountUsd, "No confirmado") : "No confirmado";
  const watchedVolume = formatMoneyCompact(row.monitoredDollarVolume || row.dollarVolume, "No confirmado");
  const flowAmount = confirmed ? confirmedAmount : watchedVolume;
  const directionText = row.direction === "outflow" ? "salida / distribución" : row.direction === "inflow" ? "entrada / acumulación" : "flujo bajo vigilancia";
  const meaningText = confirmed
    ? `${flowAmount} reportados por fuente activa. Genesis lo trata como evidencia de flujo, no como orden automática.`
    : `${flowAmount} de volumen vigilado a ${money(row.price || row.currentPrice, "precio pendiente")}. No confirma comprador, wallet ni institución; sirve para medir atención y presión del mercado.`;
  const impactText = row.direction === "outflow"
    ? "Si el precio no recupera nivel, puede actuar como presión de distribución."
    : row.direction === "inflow"
      ? "Si el precio sostiene soporte y el volumen continúa, puede apoyar acumulación."
      : "Sin dirección clara: solo sube a prioridad si rompe rango con volumen relativo.";
  body.innerHTML = `
    <span class="app-kicker">Ballenas / smart money</span>
    <h2>${escapeHtml(row.assetName || getAssetDisplayName(row.ticker).displayName)}</h2>
    <p>${escapeHtml(cleanCopy(row.read || "Flujo en vigilancia."))}</p>
    ${eventMetricGridMarkup([
      ["Tipo", movementLabel],
      ["Movimiento", cleanCopy(row.event || "Flujo")],
      ["Dirección", cleanCopy(row.direction || "neutral")],
      ["Entidad", cleanCopy(row.entity || "Sin entidad confirmada")],
      ["Cantidad", row.units ? compactNumber(row.units) : "No confirmada"],
      ["Monto confirmado", row.amountSuspicious ? "No confirmado" : confirmedAmount],
      ["Volumen vigilado", watchedVolume],
      ["Precio usado", money(row.price || row.currentPrice, "Sin precio")],
      ["Volumen", row.volume ? compactNumber(row.volume) : "Sin volumen"],
      ["Dollar volume", formatMoneyCompact(row.dollarVolume, "No confirmado")],
      ["Vol. rel", row.relativeVolume ? `${compactNumber(row.relativeVolume)}x` : "No confirmado"],
      ["Fuente", cleanCopy(row.source || "market_flow")],
      ["Confianza", cleanCopy(row.confidence || "baja")],
    ])}
    ${flowVolumeVisualMarkup(row, confirmed)}
    <section class="genesis-mini">
      <strong>Lectura Genesis</strong>
      <p>Movimiento: ${escapeHtml(cleanCopy(directionText))}.</p>
      <p>Qué significa: ${escapeHtml(cleanCopy(meaningText))}</p>
      <p>Qué NO significa: ${escapeHtml(confirmed ? "no garantiza continuidad ni entrada inmediata." : "no es compra confirmada ni monto institucional confirmado.")}</p>
      <p>Impacto probable: ${escapeHtml(cleanCopy(impactText))}</p>
      <p>Qué vigilar: volumen relativo, ruptura de rango, reacción de precio y exposición de tu cartera/watchlist a ${escapeHtml(row.ticker || "este activo")}.</p>
    </section>
    ${strategyChecklistMarkup(row.strategy)}
    ${row.ticker && row.ticker !== "MERCADO" ? `<button class="secondary-button full" type="button" data-open-asset="${escapeHtml(row.ticker)}">Ver activo</button>` : ""}
  `;
  sheet.hidden = false;
}

function hydrateOpenWhaleDetailFromChart(row = {}, whaleId = "") {
  const ticker = itemTicker(row);
  if (!ticker || !whaleRowNeedsLiveHydration(row)) return;
  const key = chartCacheKey(ticker, "1D");
  if (appState.chartCache[key]?.loading) return;
  loadChartSeries(ticker, "1D").then(() => {
    if (appState.selectedWhaleId !== whaleId) return;
    const hydrated = hydrateWhaleRowWithLiveAsset(appState.whaleItemsById?.[whaleId] || row);
    appState.whaleItemsById[whaleId] = { ...hydrated, id: whaleId };
    if (!whaleRowNeedsLiveHydration(hydrated)) openWhaleDetail(whaleId);
  }).catch(() => {});
}

function closeEventDetail() {
  const sheet = document.getElementById("event-sheet");
  if (sheet) sheet.hidden = true;
  appState.selectedAlertId = "";
  appState.selectedWhaleId = "";
}

function eventMetricGridMarkup(rows) {
  return `<div class="event-metric-grid">${rows.map(([label, value]) => `
    <span><small>${escapeHtml(label)}</small><strong>${escapeHtml(value ?? "Sin dato")}</strong></span>
  `).join("")}</div>`;
}

function miniSeriesBars(values = [], maxHeight = 28) {
  const nums = values.map(numberOrNull).filter((value) => value !== null);
  const max = Math.max(1, ...nums.map((value) => Math.abs(value)));
  const safe = nums.length ? nums : [1, 2, 1.4, 2.6, 1.8];
  const cap = Math.max(18, Math.min(96, numberOrNull(maxHeight) || 28));
  return safe.slice(0, 8).map((value) => `<i style="height:${Math.max(8, Math.min(cap, 8 + (Math.abs(value) / max) * (cap - 8)))}px"></i>`).join("");
}

function openAssetDetail(ticker) {
  const normalized = normalizeTicker(ticker);
  if (!normalized) {
    toast("No encontre ese activo.", "error");
    return;
  }
  if (appState.activeScreen !== "asset-detail") {
    appState.selectedAssetPreviousScreen = appState.activeScreen || "genesis";
  }
  appState.selectedAsset = normalized;
  setActiveScreen("asset-detail");
  const range = appState.assetChartRanges[normalized] || "1Y";
  loadChartSeries(normalized, range).then(() => {
    if (appState.activeScreen === "asset-detail" && appState.selectedAsset === normalized) renderAssetDetailScreen();
  });
  Promise.allSettled([loadAlerts(), loadWhalesData(), loadNews()]).then(() => {
    if (appState.activeScreen === "asset-detail" && appState.selectedAsset === normalized) renderAssetDetailScreen();
  });
}

function renderAssetDetailScreen() {
  const root = document.getElementById("view-asset-detail");
  if (!root) return;
  const normalized = normalizeTicker(appState.selectedAsset);
  if (!normalized) {
    root.innerHTML = emptyStateMarkup("Sin activo seleccionado.", "Toca una fila de Seguimiento o Cartera para abrir su detalle.");
    return;
  }
  const chartRange = appState.assetChartRanges[normalized] || "1Y";
  const item = assetDetailItem(normalized, chartRange);
  const isPaper = appState.paperPositions.some((row) => itemTicker(row) === normalized);
  const isTracked = appState.trackingItems.some((row) => itemTicker(row) === normalized && itemInWatchlist(row));
  const relatedAlerts = assetRelatedAlerts(normalized);
  const relatedWhales = assetRelatedWhales(normalized);
  const relatedNews = assetRelatedNews(normalized);
  const units = itemUnits(item);
  const value = itemValue(item);
  const pnl = positionPnl(item);
  const display = getAssetDisplayName(item);
  root.innerHTML = `
    <section class="asset-detail">
      <div class="detail-topbar">
        <button class="detail-back" type="button" data-asset-back aria-label="Volver">${iconSvg("back")} Volver</button>
        <details class="market-menu detail-menu">
          <summary aria-label="Acciones ${escapeHtml(normalized)}">${iconSvg("menu")}</summary>
          <div class="market-menu-panel">
            ${!isTracked ? `<button type="button" data-market-add="${escapeHtml(normalized)}">Agregar seguimiento</button>` : ""}
            <button type="button" data-paper-buy="${escapeHtml(normalized)}">Compra paper</button>
            <button type="button" data-create-alert="${escapeHtml(normalized)}">Crear alerta</button>
            ${isPaper ? `<button class="danger" type="button" data-paper-close="${escapeHtml(normalized)}">Cerrar paper</button>` : ""}
            ${isTracked ? `<button class="danger" type="button" data-watch-remove="${escapeHtml(normalized)}">Quitar seguimiento</button>` : ""}
          </div>
        </details>
      </div>
      <section class="detail-hero">
        <div>
          <strong>${escapeHtml(display.displayName)}</strong>
          <p>${escapeHtml(display.subtitle ? `${display.subtitle} · ${normalized}` : normalized)}</p>
        </div>
        <div class="detail-price">
          <strong class="${marketToneClass(item)}">${escapeHtml(priceLabel(item))}</strong>
          <span>${dailyMoveMarkup(item)}</span>
        </div>
      </section>
      <section class="detail-metrics">
        <span><small>Sesion anterior</small><strong>${escapeHtml(previousCloseLabel(item).replace("Anterior ", ""))}</strong></span>
        <span><small>Rango</small><strong>${escapeHtml(item.day_low && item.day_high ? `${money(item.day_low)} - ${money(item.day_high)}` : "Sin dato")}</strong></span>
        <span><small>Volumen</small><strong>${escapeHtml(item.volume ? Number(item.volume).toLocaleString("en-US") : "Sin dato")}</strong></span>
        <span><small>Actualizado</small><strong>${escapeHtml(formatDate(item.quote_timestamp || item.updated_at || appState.lastUpdated))}</strong></span>
        ${isPaper ? `<span><small>Paper</small><strong>${escapeHtml(units ?? "Sin")} units - ${escapeHtml(money(value, "Sin valor"))}</strong></span>` : ""}
        ${isPaper ? `<span><small>P/L</small><strong class="${marketToneClass(pnl)}">${escapeHtml(formatChange(pnl, "Sin dato"))}</strong></span>` : ""}
      </section>
      ${chartBlockMarkup(normalized, chartRange, `detail:${normalized}`)}
      ${detailIndicatorsMarkup(normalized, chartRange)}
      <section class="detail-analysis">
        <strong>Lectura Genesis</strong>
        <p>${escapeHtml(assetGenesisReading(item, chartRange))}</p>
      </section>
      <section class="detail-analysis">
        <strong>Catalizadores</strong>
        <p>${escapeHtml(assetCatalystLine(normalized))}</p>
      </section>
      <section class="detail-alerts">
        <strong>Noticias clave</strong>
        ${relatedNews.length ? relatedNews.map(newsCardMarkup).join("") : `<p>Sin noticias relacionadas confirmadas.</p>`}
      </section>
      <section class="detail-alerts">
        <strong>Alertas relacionadas</strong>
        ${relatedAlerts.length ? relatedAlerts.map(alertMarkup).join("") : `<p>Sin alertas relacionadas confirmadas.</p>`}
      </section>
      <section class="detail-alerts">
        <strong>Ballenas relacionadas</strong>
        ${relatedWhales.length ? relatedWhales.map(whaleRowMarkup).join("") : `<p>Sin ballenas relacionadas confirmadas.</p>`}
      </section>
    </section>
  `;
}

function detailIndicatorsMarkup(ticker, range) {
  const key = chartCacheKey(ticker, range);
  const payload = appState.chartCache[key]?.payload;
  const indicators = payload?.indicators || {};
  if (!payload || !indicators || indicators.ok === false) return "";
  const macd = indicators.macd || {};
  const ema = indicators.ema || {};
  const sma = indicators.sma || {};
  const fib = indicators.fibonacci || {};
  const golden = indicators.golden_pocket || {};
  const items = [
    ["RSI", indicators.rsi, compactNumber],
    ["MACD", macd.line, compactNumber],
    ["EMA 20", ema["20"], money],
    ["EMA 50", ema["50"], money],
    ["EMA 200", ema["200"], money],
    ["SMA 200", sma["200"], money],
    ["Fib 0.618", fib["0.618"], money],
    ["Golden", firstKnownValue(golden.from, golden.to), money],
    ["Volatilidad", firstKnownValue(indicators.volatility?.annualized_pct, indicators.volatility?.pct, indicators.volatility), compactNumber],
    ["Soporte", indicators.support, money],
    ["Resistencia", indicators.resistance, money],
  ].filter(([, value]) => numberOrNull(value) !== null);
  if (!items.length) return "";
  return `
    <section class="detail-analysis detail-indicators">
      <strong>Indicadores Genesis</strong>
      <div class="indicator-strip dense">
        ${items.slice(0, 9).map(([label, value, formatter]) => `
          <span>
            <small>${escapeHtml(label)}</small>
            <strong>${escapeHtml(formatter(value))}</strong>
          </span>
        `).join("")}
      </div>
    </section>
  `;
}

function assetRelatedAlerts(ticker) {
  const normalized = normalizeTicker(ticker);
  const items = Array.isArray(appState.alertsSnapshot?.items) ? appState.alertsSnapshot.items : [];
  return items.filter((item) => itemTicker(item) === normalized).slice(0, 3);
}

function assetRelatedWhales(ticker) {
  const normalized = normalizeTicker(ticker);
  return extractWhaleRows(appState.whalesSnapshot?.causal || {}, appState.whalesSnapshot?.detection || {})
    .filter((item) => itemTicker(item) === normalized)
    .slice(0, 3);
}

function assetRelatedNews(ticker) {
  const normalized = normalizeTicker(ticker);
  return newsFeedItems(appState.newsSnapshot || {})
    .filter((item) => (item.assets || []).map(normalizeTicker).includes(normalized) || cleanCopy(item.title || "").toUpperCase().includes(normalized))
    .slice(0, 3);
}

function assetGenesisReading(item, range) {
  const ticker = itemTicker(item);
  const label = assetDisplayName(item) || ticker;
  const tone = movementTone(item);
  const price = priceLabel(item);
  const move = dailyMoveLabel(item);
  if (tone === "up") return `${label} mantiene sesgo positivo con precio ${price} y movimiento ${move}. Veredicto: vigilar continuidad; entrada condicional solo con volumen y cierre firme. Invalidacion: perdida de soporte o deterioro de mercado.`;
  if (tone === "down") return `${label} esta bajo presion con precio ${price} y movimiento ${move}. Veredicto: cautela; entrada condicional solo si recupera estructura. Invalidacion: nuevo minimo o falta de precio confirmado.`;
  return `${label} esta neutral o sin cambio confirmado en ${range}. Veredicto: esperar confirmacion; razon principal: falta direccion clara. Riesgo: operar sin volumen. Siguiente paso: revisar velas y catalizadores.`;
}

function assetCatalystLine(ticker) {
  const relatedNews = assetRelatedNews(ticker);
  const relatedAlerts = assetRelatedAlerts(ticker);
  if (relatedNews.length) return cleanCopy(relatedNews[0].summary || relatedNews[0].title || "Catalizador en vigilancia.");
  if (relatedAlerts.length) return cleanCopy(relatedAlerts[0].summary || relatedAlerts[0].title || "Alerta relacionada en vigilancia.");
  return "Sin catalizadores confirmados en la fuente activa. Genesis no inventa noticias ni eventos.";
}

function openAssetSheet(ticker) {
  const normalized = normalizeTicker(ticker);
  const item = findAsset(normalized);
  if (!item) {
    toast("No encontre ese activo en la lectura actual.", "error");
    return;
  }
  appState.selectedAsset = normalized;
  const isPaper = appState.paperPositions.some((row) => itemTicker(row) === normalized);
  const isTracked = appState.trackingItems.some((row) => itemTicker(row) === normalized && itemInWatchlist(row));
  const chartRange = appState.assetChartRanges[normalized] || "1Y";
  const display = getAssetDisplayName(item);
  const sheet = document.getElementById("asset-sheet");
  const body = document.getElementById("asset-sheet-body");
  body.innerHTML = `
    <span class="app-kicker">${isPaper ? "Paper" : isTracked ? "Seguimiento" : "No agregado"}</span>
    <h2>${escapeHtml(display.displayName)}</h2>
    <p class="asset-name">${escapeHtml(display.subtitle ? `${display.subtitle} · ${normalized}` : normalized)}</p>
    <div class="sheet-price ${movementTone(item)}">
      <strong class="${marketToneClass(item)}">${escapeHtml(priceLabel(item))}</strong>
      <span>${escapeHtml(dailyMoveLabel(item))}</span>
    </div>
    <div class="sheet-grid">
      <span>${escapeHtml(priceSourceLabel(item))}</span>
      <span>${escapeHtml(previousCloseLabel(item))}</span>
      <span>${escapeHtml(extendedLabel(item))}</span>
      <span>${escapeHtml(marketSessionLabel(item))}</span>
      <span>Rango: ${escapeHtml(item.day_low ? `${money(item.day_low)} - ${money(item.day_high)}` : "Sin dato")}</span>
      <span>Volumen: ${escapeHtml(item.volume ? Number(item.volume).toLocaleString("en-US") : "Sin dato")}</span>
      <span>${escapeHtml(formatDate(item.quote_timestamp || item.updated_at))}</span>
      <span>${isPaper ? `${escapeHtml(itemUnits(item))} unidades` : isTracked ? "En seguimiento" : "No agregado"}</span>
    </div>
    ${chartBlockMarkup(normalized, chartRange, `asset:${normalized}`)}
    <section class="genesis-mini">
      <strong>Genesis</strong>
      <p>Veredicto: Vigilar.</p>
      <p>Entrada condicional: esperar confirmacion de precio, volumen y contexto antes de operar.</p>
      <p>Invalidacion: si falta precio live o rompe soporte, baja conviccion.</p>
      <p>Plan: paper primero; sin broker y sin compra real.</p>
    </section>
    <div class="sheet-actions">
      ${!isTracked ? `<button class="secondary-button" type="button" data-market-add="${escapeHtml(normalized)}">+ Seguimiento</button>` : ""}
      <button class="primary-button" type="button" data-paper-buy="${escapeHtml(normalized)}">Carrito</button>
      ${isPaper ? `<button class="danger-button" type="button" data-paper-close="${escapeHtml(normalized)}">Cerrar paper</button>` : ""}
      ${isTracked ? `<button class="danger-button" type="button" data-watch-remove="${escapeHtml(normalized)}">Quitar</button>` : ""}
    </div>
  `;
  sheet.hidden = false;
  const key = chartCacheKey(normalized, chartRange);
  if (!appState.chartCache[key]) {
    loadChartSeries(normalized, chartRange).then(() => {
      if (appState.selectedAsset === normalized && !sheet.hidden) openAssetSheet(normalized);
    });
  }
}

function closeAssetSheet() {
  const sheet = document.getElementById("asset-sheet");
  if (sheet) sheet.hidden = true;
}

function openPaperBuySheet(ticker) {
  const normalized = normalizeTicker(ticker || appState.selectedAsset || "");
  if (!normalized) {
    toast("Selecciona un activo primero.", "info");
    return;
  }
  const item = findAsset(normalized) || { ticker: normalized };
  const price = itemPrice(item);
  document.getElementById("paper-buy-ticker").value = normalized;
  const display = getAssetDisplayName(item);
  document.getElementById("paper-buy-label").value = `${display.displayName} | ${display.subtitle || normalized}`;
  document.getElementById("paper-buy-live-price").value = price === null ? "Sin precio" : `${money(price)} | ${priceSourceLabel(item)}`;
  document.getElementById("paper-buy-units").value = "";
  document.getElementById("paper-buy-entry").value = price === null ? "" : String(price);
  document.getElementById("paper-buy-total").textContent = "Total estimado: Sin calcular";
  document.getElementById("paper-buy-sheet").hidden = false;
}

function closePaperBuySheet() {
  document.getElementById("paper-buy-sheet").hidden = true;
}

function openClosePaperSheet(ticker) {
  const normalized = normalizeTicker(ticker);
  const item = appState.paperPositions.find((row) => itemTicker(row) === normalized);
  if (!item) {
    toast("No encontre posicion paper para cerrar.", "error");
    return;
  }
  document.getElementById("portfolio-close-ticker").value = normalized;
  document.getElementById("portfolio-close-title").textContent = `Cerrar ${normalized}`;
  document.getElementById("portfolio-close-summary").textContent = `${normalized}: ${itemUnits(item)} unidades, valor ${money(itemValue(item))}. No ejecuta venta real.`;
  document.getElementById("portfolio-close-modal").hidden = false;
}

function closeClosePaperSheet() {
  document.getElementById("portfolio-close-modal").hidden = true;
}

function updatePaperTotal() {
  const units = numberOrNull(document.getElementById("paper-buy-units")?.value);
  const entry = numberOrNull(document.getElementById("paper-buy-entry")?.value);
  const total = units !== null && entry !== null ? units * entry : null;
  document.getElementById("paper-buy-total").textContent = `Total estimado: ${money(total, "Sin calcular")}`;
}

function bindGlobalEvents() {
  document.querySelectorAll(".nav-link").forEach((button) => {
    button.addEventListener("click", () => setActiveScreen(normalizeScreen(button.dataset.view)));
  });
  const captureSheetClose = (event) => {
    const target = event.target instanceof Element ? event.target : event.target?.parentElement;
    if (!target) return;
    if (target.closest("[data-news-close]")) {
      event.preventDefault();
      event.stopPropagation();
      closeNewsDetail();
      return;
    }
    if (target.closest("[data-event-close]")) {
      event.preventDefault();
      event.stopPropagation();
      closeEventDetail();
    }
  };
  document.addEventListener("click", captureSheetClose, true);
  document.querySelectorAll("[data-news-close]").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.preventDefault();
      closeNewsDetail();
    });
  });
  document.querySelectorAll("[data-event-close]").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.preventDefault();
      closeEventDetail();
    });
  });

  document.body.addEventListener("click", async (event) => {
    if (event.target.closest("[data-toast-close]")) {
      hideToast();
      return;
    }

    if (event.target.closest("[data-voice-toggle]")) {
      event.preventDefault();
      toggleGenesisVoiceInput();
      return;
    }

    if (event.target.closest("[data-news-refresh]")) {
      event.preventDefault();
      try {
        await loadNews({ force: true });
        toast("Noticias actualizadas con FMP/RSS.", "success");
      } catch (error) {
        toast(networkErrorMessage(error), "error");
      }
      return;
    }

    const newsFilter = event.target.closest("[data-news-filter]");
    if (newsFilter) {
      event.preventDefault();
      appState.newsFilter = newsFilter.dataset.newsFilter || "important";
      renderNewsScreen();
      scrollNewsScreenToTop();
      return;
    }

    const newsOpen = event.target.closest("[data-news-open]");
    if (newsOpen) {
      event.preventDefault();
      openNewsDetail(newsOpen.dataset.newsOpen);
      return;
    }

    if (event.target.closest("[data-news-close]")) {
      event.preventDefault();
      closeNewsDetail();
      return;
    }

    const alertOpen = event.target.closest("[data-alert-id]");
    if (alertOpen) {
      event.preventDefault();
      openAlertDetail(alertOpen.dataset.alertId);
      return;
    }

    const whaleOpen = event.target.closest("[data-whale-id]");
    if (whaleOpen) {
      event.preventDefault();
      openWhaleDetail(whaleOpen.dataset.whaleId);
      return;
    }

    if (event.target.closest("[data-event-close]")) {
      event.preventDefault();
      closeEventDetail();
      return;
    }

    if (event.target.closest("[data-chat-new]")) {
      event.preventDefault();
      appState.chatHistoryOpen = false;
      appState.currentConversationId = `chat-${Date.now()}`;
      appState.chatMessages = [initialChatMessage()];
      renderGenesisScreen();
      return;
    }

    if (event.target.closest("[data-chat-clear]")) {
      event.preventDefault();
      appState.chatHistoryOpen = false;
      appState.currentConversationId = `chat-${Date.now()}`;
      appState.chatMessages = [initialChatMessage()];
      renderGenesisScreen();
      toast("Chat actual limpio. La memoria util se conserva.", "info");
      return;
    }

    if (event.target.closest("[data-chat-history]")) {
      event.preventDefault();
      appState.chatHistoryOpen = !appState.chatHistoryOpen;
      renderGenesisScreen();
      return;
    }

    const historyPick = event.target.closest("[data-chat-history-pick]");
    if (historyPick) {
      event.preventDefault();
      openGenesisConversation(historyPick.dataset.chatHistoryPick);
      return;
    }

    if (
      appState.chatHistoryOpen
      && appState.activeScreen === "genesis"
      && !event.target.closest("[data-chat-history-panel]")
      && !event.target.closest("[data-chat-history]")
    ) {
      event.preventDefault();
      appState.chatHistoryOpen = false;
      renderGenesisScreen();
      return;
    }

    const trackingFilter = event.target.closest("[data-tracking-filter]");
    if (trackingFilter) {
      event.preventDefault();
      appState.trackingFilter = trackingFilter.dataset.trackingFilter || "all";
      renderTrackingScreen();
      return;
    }

    const searchToggle = event.target.closest("[data-toggle-search]");
    if (searchToggle) {
      event.preventDefault();
      const key = searchToggle.dataset.toggleSearch;
      if (key && Object.prototype.hasOwnProperty.call(appState.searchOpen, key)) {
        appState.searchOpen[key] = !appState.searchOpen[key];
        if (key === "tracking") renderTrackingScreen();
        if (key === "portfolio") renderPortfolioScreen();
        if (key === "whales" || key === "alerts") renderAlertsScreen();
        if (key === "news") renderNewsScreen();
      }
      return;
    }

    const alertTab = event.target.closest("[data-alert-tab]");
    if (alertTab) {
      event.preventDefault();
      appState.alertSubtab = alertTab.dataset.alertTab || "alerts";
      renderAlertsScreen();
      return;
    }

    const chartButton = event.target.closest("[data-chart-range]");
    if (chartButton) {
      event.preventDefault();
      const ticker = normalizeTicker(chartButton.dataset.chartTicker);
      const range = String(chartButton.dataset.chartRange || "1Y").toUpperCase();
      const target = String(chartButton.dataset.chartTarget || "");
      if (target.startsWith("chat:")) {
        const messageId = target.slice(5);
        const message = appState.chatMessages.find((item) => item.id === messageId);
        if (message?.chart) message.chart.range = range;
        renderGenesisScreen();
        loadChartSeries(ticker, range).then(() => renderGenesisScreen());
      } else if (target.startsWith("detail:")) {
        appState.assetChartRanges[ticker] = range;
        renderAssetDetailScreen();
        loadChartSeries(ticker, range).then(() => {
          if (appState.selectedAsset === ticker && appState.activeScreen === "asset-detail") renderAssetDetailScreen();
        });
      } else {
        appState.assetChartRanges[ticker] = range;
        openAssetSheet(ticker);
        loadChartSeries(ticker, range).then(() => {
          if (appState.selectedAsset === ticker) openAssetSheet(ticker);
        });
      }
      return;
    }

    const openAsset = event.target.closest("[data-open-asset]");
    if (openAsset) {
      event.preventDefault();
      openAssetDetail(openAsset.dataset.openAsset);
      return;
    }

    if (event.target.closest("[data-asset-back]")) {
      event.preventDefault();
      setActiveScreen(appState.selectedAssetPreviousScreen || "genesis");
      return;
    }

    const searchPick = event.target.closest("[data-search-pick]");
    if (searchPick) {
      event.preventDefault();
      const ticker = normalizeTicker(searchPick.dataset.searchPick);
      const mode = searchPick.dataset.searchMode || "tracking";
      if (mode === "portfolio") appState.portfolioSearchQuery = ticker;
      else appState.trackingSearchQuery = ticker;
      try {
        await searchMarket(ticker, mode);
      } catch (error) {
        toast(error.message, "error");
      }
      return;
    }

    const marketAdd = event.target.closest("[data-market-add]");
    if (marketAdd) {
      event.preventDefault();
      try {
        await addTickerToWatchlist(marketAdd.dataset.marketAdd);
      } catch (error) {
        toast(error.message, "error");
      }
      return;
    }

    const createAlert = event.target.closest("[data-create-alert]");
    if (createAlert) {
      event.preventDefault();
      const ticker = normalizeTicker(createAlert.dataset.createAlert);
      try {
        await postJson("/api/genesis/memory/event", {
          event_type: "alert_request",
          source: "ui",
          confidence: "media",
          payload: { ticker, requested_from: appState.activeScreen },
        });
        toast(`Alerta de ${ticker} guardada como solicitud para Genesis.`, "success");
      } catch (error) {
        toast(`No pude guardar la solicitud de alerta: ${cleanCopy(error.message)}`, "error");
      }
      return;
    }

    const watchRemove = event.target.closest("[data-watch-remove]");
    if (watchRemove) {
      event.preventDefault();
      try {
        await removeTickerFromWatchlist(watchRemove.dataset.watchRemove);
        closeAssetSheet();
      } catch (error) {
        toast(error.message, "error");
      }
      return;
    }

    const paperBuy = event.target.closest("[data-paper-buy]");
    if (paperBuy) {
      event.preventDefault();
      openPaperBuySheet(paperBuy.dataset.paperBuy);
      return;
    }

    const paperClose = event.target.closest("[data-paper-close]");
    if (paperClose) {
      event.preventDefault();
      openClosePaperSheet(paperClose.dataset.paperClose);
      return;
    }

    if (event.target.closest("[data-sheet-close]")) closeAssetSheet();
    if (event.target.closest("[data-paper-cancel]")) closePaperBuySheet();
    if (event.target.closest("[data-close-paper-cancel]")) closeClosePaperSheet();
  });

  document.getElementById("paper-buy-units").addEventListener("input", updatePaperTotal);
  document.getElementById("paper-buy-entry").addEventListener("input", updatePaperTotal);
  document.getElementById("paper-buy-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const ticker = document.getElementById("paper-buy-ticker").value;
    const units = numberOrNull(document.getElementById("paper-buy-units").value);
    const entry = numberOrNull(document.getElementById("paper-buy-entry").value);
    try {
      await savePaperBuy(ticker, units, entry);
      closePaperBuySheet();
      closeAssetSheet();
      setActiveScreen("portfolio");
    } catch (error) {
      toast(error.message, "error");
    }
  });

  document.getElementById("portfolio-close-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const ticker = document.getElementById("portfolio-close-ticker").value;
    try {
      await removePaperTicker(ticker);
      closeClosePaperSheet();
      closeAssetSheet();
      setActiveScreen("portfolio");
    } catch (error) {
      toast(error.message, "error");
    }
  });
}

function initGenesisAppV3() {
  bindGlobalEvents();
  render();
  loadGenesisMemoryHistory();
  refreshPortfolio({ render: false }).then(() => renderActiveScreen()).catch((error) => toast(error.message, "error"));
  loadWhales().catch(() => {});
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden && (appState.activeScreen === "tracking" || appState.activeScreen === "portfolio" || appState.activeScreen === "asset-detail")) {
      refreshPortfolio({ render: true }).catch(() => {});
    }
    if (!document.hidden && appState.activeScreen === "news") {
      loadNews({ silent: true }).catch(() => {});
    }
    if (!document.hidden && appState.activeScreen === "alerts") {
      Promise.allSettled([refreshPortfolio({ render: false, force: true }), loadAlerts(), loadWhalesData()])
        .then(() => renderAlertsScreen());
    }
  });
}

document.addEventListener("DOMContentLoaded", initGenesisAppV3);
