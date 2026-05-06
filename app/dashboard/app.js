const PORTFOLIO_ENDPOINT = "/api/dashboard/portfolio";
const RADAR_ENDPOINT = "/api/dashboard/radar";

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
  chartCache: {},
  assetChartRanges: {},
  refreshTimer: null,
  refreshInFlight: false,
  refreshPromise: null,
  chatHistoryOpen: false,
  chatMessages: [initialChatMessage()],
  chatConversations: [],
  currentConversationId: `chat-${Date.now()}`,
};

const REFRESH_MS = 15000;
const CHART_RANGES = ["1D", "1W", "1M", "1Y", "5Y", "MAX"];
const MONEY_COLORS = ["#7be0ad", "#91a7ff", "#efbd6f", "#ec7f77", "#7fd9df", "#d7c27f", "#b7c5d9"];

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
  return numberOrNull(item?.daily_change_pct ?? item?.change_pct ?? item?.percent_change ?? item?.changesPercentage);
}

function itemDailyUsd(item) {
  return numberOrNull(item?.daily_change ?? item?.change ?? item?.change_usd);
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
    .replace(/\bFMP_API_KEY\b/gi, "fuente privada")
    .replace(/\bapikey\b/gi, "credencial")
    .replace(/\bendpoint\b/gi, "consulta")
    .replace(/\bruntime\b/gi, "sistema")
    .replace(/\bsnapshot\b/gi, "lectura")
    .replace(/\bdegraded\b/gi, "datos parciales")
    .replace(/\bunavailable\b/gi, "sin datos")
    .replace(/\binsufficient_confirmation\b/gi, "no concluyente")
    .replace(/\bTelegram\b/gi, "Genesis")
    .replace(/\blegacy\b/gi, "local");
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
  const response = await fetch(url, { cache: "no-store" });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.message || `HTTP ${response.status}`);
  }
  return payload;
}

async function postJson(url, body) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    cache: "no-store",
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok || payload.ok === false) {
    throw new Error(payload.message || `HTTP ${response.status}`);
  }
  return payload;
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
    upload: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 17V5"/><path d="M7 10l5-5 5 5"/><path d="M5 19h14"/></svg>`,
    menu: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 12h.01M19 12h.01M5 12h.01"/></svg>`,
    history: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 12a9 9 0 1 0 3-6.7"/><path d="M3 4v6h6"/><path d="M12 7v5l3 2"/></svg>`,
    new: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 5v14M5 12h14"/></svg>`,
    clear: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 7h16"/><path d="M10 11v6M14 11v6"/><path d="M6 7l1 13h10l1-13"/><path d="M9 7V4h6v3"/></svg>`,
    back: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M15 6l-6 6 6 6"/></svg>`,
  };
  return icons[name] || "";
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
  const stop = new Set(["ANALIZA", "ANALIZAR", "QUIERO", "REVISA", "REVISAR", "VER", "HAZME", "UNA", "UN", "GRAFICA", "GRAFICAS", "GRAFICO", "GRAFICOS", "CHART", "MUESTRAME", "MOSTRAME", "MUESTRA", "DE", "DEL", "LA", "EL", "POR", "FAVOR", "CON", "VELAS", "VELA", "HORA", "FECHA", "QUE", "RESUMEN", "DIA", "HOY", "OYE", "GENESIS", "MERCADO"]);
  const aliases = { BTC: "BTC-USD", BITCOIN: "BTC-USD", ETH: "ETH-USD", SOL: "SOL-USD", BRENT: "BZ=F" };
  const tokens = normalized.match(/\b[A-Z0-9]{1,12}(?:[.\-=][A-Z0-9]{1,8})?\b/g) || [];
  const rawTicker = tokens.find((token) => !stop.has(token) && /[A-Z0-9]/.test(token));
  const ticker = aliases[rawTicker] || rawTicker;
  return ticker ? { ticker: normalizeTicker(ticker), range: "1Y" } : null;
}

async function loadChartSeries(ticker, range = "1Y") {
  const normalizedTicker = normalizeTicker(ticker);
  const normalizedRange = CHART_RANGES.includes(String(range).toUpperCase()) ? String(range).toUpperCase() : "1Y";
  const key = chartCacheKey(normalizedTicker, normalizedRange);
  if (appState.chartCache[key]?.payload) return appState.chartCache[key].payload;
  appState.chartCache[key] = { loading: true, payload: null };
  try {
    const payload = await getJson(`/api/dashboard/asset/chart?ticker=${encodeURIComponent(normalizedTicker)}&range=${encodeURIComponent(normalizedRange)}`);
    appState.chartCache[key] = { loading: false, payload };
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
    appState.chartCache[key] = { loading: false, payload };
    return payload;
  }
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
  const tone = positiveClass(payload?.summary?.change_pct ?? payload?.summary?.change);
  const quotePrice = payload?.quote?.price ?? payload?.summary?.end_price;
  const change = payload?.quote?.change ?? payload?.summary?.change;
  const changePct = payload?.quote?.changesPercentage ?? payload?.summary?.change_pct;
  const display = getAssetDisplayName({ ticker: normalizedTicker, name: payload?.name });
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

function setActiveScreen(screen) {
  appState.activeScreen = screen;
  document.querySelectorAll(".app-screen").forEach((node) => {
    node.classList.toggle("is-active", node.id === screenId(screen));
  });
  updateNav();
  renderActiveScreen();

  if (screen === "tracking" || screen === "portfolio" || screen === "asset-detail") {
    startPortfolioAutoRefresh();
    refreshPortfolio({ render: true }).catch((error) => toast(error.message, "error"));
  } else {
    stopPortfolioAutoRefresh();
  }

  if (screen === "news") {
    loadNews().catch((error) => toast(error.message, "error"));
  }
  if (screen === "alerts") {
    Promise.all([loadAlerts(), loadWhalesData()]).then(() => renderAlertsScreen()).catch((error) => toast(error.message, "error"));
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
          <label class="chat-attach" title="Adjuntar grafica" aria-label="Adjuntar imagen de grafica">
            ${iconSvg("upload")}
            <input id="genesis-image-input" type="file" accept="image/*">
          </label>
          <input id="genesis-chat-input" placeholder="Pregunta a Genesis..." autocomplete="off">
          <button type="submit" aria-label="Mandar mensaje">${iconSvg("send")}</button>
        </form>
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
    <div class="chat-history-panel">
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
  if (["ticker_analysis", "technical_indicators", "chart_request"].includes(intent) && (payload?.quote || payload?.chart)) {
    return assetAnalysisVisual(payload, answer);
  }
  if (intent === "comparison") return comparisonVisual(payload, answer);
  if (intent === "daily_briefing" || intent === "market_overview") return briefingVisual(payload, answer);
  if (intent === "weather") return weatherVisual(payload, answer);
  if (intent === "alerts") return feedVisual("Alertas", payload?.alerts?.items || [], answer);
  if (intent === "whale_activity") return feedVisual("Ballenas", payload?.whales?.items || payload?.whales?.events || [], answer);
  if (intent === "portfolio_summary") return summaryVisual("Cartera", answer);
  if (intent === "tracking_summary") return summaryVisual("Seguimiento", answer);
  if (intent === "image_chart_analysis") return summaryVisual("Imagen", answer);
  return null;
}

function assetAnalysisVisual(payload, answer = "") {
  const quote = payload?.quote || {};
  const ticker = normalizeTicker(quote.ticker || payload?.chart?.ticker || payload?.tickers?.[0]);
  const changePct = quoteChangePct(quote);
  const bias = biasFromMove(changePct ?? quoteChange(quote));
  const technical = payload?.technical?.indicators || {};
  const sections = cleanSentenceList(answer, 5);
  const price = quotePrice(quote);
  const support = firstKnownValue(technical.support, quote.day_low, quote.dayLow);
  const resistance = firstKnownValue(technical.resistance, quote.day_high, quote.dayHigh);
  return {
    kind: "asset_analysis",
    ticker,
    name: assetDisplayName({ ticker, name: quote.name || payload?.name }),
    subtitle: assetSubtitle({ ticker, name: quote.name || payload?.name }) || ticker,
    thesis: sections[0] || `${ticker || "El activo"} queda en vigilancia con datos confirmados por backend.`,
    bias,
    confidence: confidenceFromQuote(quote),
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
      macd: technical.macd?.line,
      volume: firstKnownValue(quote.volume, technical.relative_volume),
      moneyFlow: payload?.whales?.answer || "Sin flujo institucional confirmado.",
    },
    sections: sections.slice(1),
  };
}

function briefingVisual(payload, answer = "") {
  const source = payload?.briefing || payload?.overview || {};
  const sections = cleanSentenceList(source.answer || answer, 6);
  return {
    kind: "briefing",
    title: payload?.intent === "market_overview" ? "Pulso de mercado" : "Resumen del dia",
    thesis: sections[0] || "Genesis sintetiza mercado, cartera, alertas y fuentes disponibles sin inventar datos.",
    sections: sections.slice(1),
    confidence: 0.62,
  };
}

function weatherVisual(payload, answer = "") {
  const weather = payload?.weather || {};
  return {
    kind: "weather",
    title: weather.city ? `Clima en ${weather.city}` : "Clima",
    thesis: stripMarkdownCopy(weather.answer || answer || "No pude confirmar clima con la fuente activa."),
    temperature: firstKnownValue(weather.temperature, weather.temp),
    condition: weather.condition || weather.description || "",
    wind: weather.wind_speed,
    rain: weather.rain_probability,
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

function feedVisual(title, rows, answer = "") {
  return {
    kind: "feed",
    title,
    thesis: cleanSentenceList(answer, 1)[0] || "Genesis muestra solo eventos confirmados por la fuente activa.",
    rows: Array.isArray(rows) ? rows.slice(0, 4) : [],
  };
}

function visualResponseMarkup(visual) {
  if (!visual) return "";
  if (visual.kind === "asset_analysis") return assetAnalysisVisualMarkup(visual);
  if (visual.kind === "briefing") return briefingVisualMarkup(visual);
  if (visual.kind === "weather") return weatherVisualMarkup(visual);
  if (visual.kind === "comparison") return comparisonVisualMarkup(visual);
  if (visual.kind === "feed") return feedVisualMarkup(visual);
  return summaryVisualMarkup(visual);
}

function assetAnalysisVisualMarkup(visual) {
  const confidencePct = Math.round(Math.max(0, Math.min(1, visual.confidence || 0)) * 100);
  const changeTone = positiveClass(visual.price?.changePct ?? visual.price?.change);
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
      <div class="confidence-row">
        <span>Confianza ${confidencePct}%</span>
        <i><b style="width:${confidencePct}%"></b></i>
      </div>
      <div class="visual-grid">
        ${visualMetricMarkup("Soporte", visual.levels?.support, money)}
        ${visualMetricMarkup("Resistencia", visual.levels?.resistance, money)}
        ${visualMetricMarkup("RSI", visual.indicators?.rsi, compactNumber)}
        ${visualMetricMarkup("MACD", visual.indicators?.macd, compactNumber)}
        ${visualMetricMarkup("Volumen", visual.indicators?.volume, compactNumber)}
        <span><small>Money flow</small><strong>${escapeHtml(stripMarkdownCopy(visual.indicators?.moneyFlow || "Sin dato").slice(0, 42))}</strong></span>
      </div>
      <div class="visual-sections">
        ${(visual.sections?.length ? visual.sections : ["Vigilar confirmacion de precio, volumen y riesgo antes de subir conviccion."]).slice(0, 4).map((line) => `<p>${escapeHtml(line)}</p>`).join("")}
      </div>
    </section>
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
      <div class="visual-sections">
        ${(visual.sections?.length ? visual.sections : ["Sin fuente adicional confirmada; Genesis conserva cautela y usa precio, volumen y alertas disponibles."]).slice(0, 5).map((line) => `<p>${escapeHtml(line)}</p>`).join("")}
      </div>
    </section>
  `;
}

function weatherVisualMarkup(visual) {
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
      <div class="visual-market-strip">
        <span><small>Temperatura</small><strong>${escapeHtml(visual.temperature === null || visual.temperature === undefined ? "Sin dato" : `${compactNumber(visual.temperature)} C`)}</strong></span>
        <span><small>Lluvia</small><strong>${escapeHtml(visual.rain === null || visual.rain === undefined ? "Sin dato" : `${compactNumber(visual.rain)}%`)}</strong></span>
        <span><small>Viento</small><strong>${escapeHtml(visual.wind === null || visual.wind === undefined ? "Sin dato" : `${compactNumber(visual.wind)} km/h`)}</strong></span>
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

function feedVisualMarkup(visual) {
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
      <div class="visual-sections">
        ${(visual.rows?.length ? visual.rows.map((row) => cleanCopy(row.title || row.summary || row.answer || row.ticker || "Evento confirmado")) : ["Sin eventos confirmados con la fuente activa."]).slice(0, 4).map((line) => `<p>${escapeHtml(stripMarkdownCopy(line))}</p>`).join("")}
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
        conversation_id: appState.currentConversationId,
        image: {
          name: imageFile.name,
          type: imageFile.type,
          size: imageFile.size,
          data_url: dataUrl,
        },
      });
      appState.chatMessages.push(genesisAssistantMessageFromPayload(
        { ...payload, intent: payload.intent || "image_chart_analysis" },
        payload.answer || "Recibi la imagen."
      ));
    } catch (error) {
      appState.chatMessages.push({ id: nextMessageId(), role: "assistant", text: `No pude analizar la imagen: ${cleanCopy(error.message)}` });
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
    renderGenesisScreen();
    return;
  }
  try {
    const payload = await postJson("/api/genesis/ask", { message: question, context: appState.activeScreen, conversation_id: appState.currentConversationId });
    const message = genesisAssistantMessageFromPayload(payload, question);
    appState.chatMessages.push(message);
    renderGenesisScreen();
    if (message.chart) {
      loadChartSeries(message.chart.ticker, message.chart.range).then(() => {
        if (appState.activeScreen === "genesis") renderGenesisScreen();
      });
    }
    return;
  } catch (error) {
    try {
      const payload = await getJson(`/api/dashboard/genesis?q=${encodeURIComponent(question)}&context=${encodeURIComponent(appState.activeScreen)}&ticker=&panel_context=`);
      appState.chatMessages.push(genesisAssistantMessageFromPayload(payload, question));
    } catch (fallbackError) {
      appState.chatMessages.push({ id: nextMessageId(), role: "assistant", text: `No pude responder ahora: ${cleanCopy(fallbackError.message)}` });
    }
  }
  renderGenesisScreen();
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
    toast("No pude abrir ese chat.", "error");
  }
}

async function loadNews() {
  if (appState.newsLoading) return appState.newsSnapshot;
  appState.newsLoading = true;
  renderNewsScreen();
  try {
    const [macroResult, briefingResult] = await Promise.allSettled([
      getJson("/api/dashboard/macro-activity"),
      postJson("/api/genesis/ask", { message: "dame resumen del dia", context: "news", conversation_id: appState.currentConversationId }),
    ]);
    appState.newsSnapshot = {
      macro: macroResult.status === "fulfilled" ? macroResult.value : null,
      briefing: briefingResult.status === "fulfilled" ? briefingResult.value : null,
      errors: [
        macroResult.status === "rejected" ? macroResult.reason?.message : "",
        briefingResult.status === "rejected" ? briefingResult.reason?.message : "",
      ].filter(Boolean),
      loadedAt: new Date().toISOString(),
    };
  } finally {
    appState.newsLoading = false;
    renderNewsScreen();
  }
  return appState.newsSnapshot;
}

function renderNewsScreen() {
  const root = document.getElementById("view-news");
  if (!root) return;
  const snapshot = appState.newsSnapshot || {};
  const macro = snapshot.macro?.macro || {};
  const briefing = snapshot.briefing || {};
  const newsItems = newsFeedItems(snapshot);
  const affected = newsAffectedAssets(snapshot);
  root.innerHTML = `
    <section class="screen-stack news-screen">
      <section class="news-briefing">
        <span>Noticias</span>
        <strong>${escapeHtml(cleanCopy(macro.summary || briefing.answer || "Contexto listo para cargar."))}</strong>
        <p>${escapeHtml(cleanCopy(macro.dominant_risk || macro.note || "Genesis separa noticias de alertas: aqui va contexto; en Alertas van eventos accionables."))}</p>
        <button type="button" class="secondary-button small" data-news-refresh>${appState.newsLoading ? "Actualizando..." : "Actualizar"}</button>
      </section>
      <section class="market-table">
        <div><small>Tono</small><strong>${escapeHtml(macro.sentiment?.label || "Neutral")}</strong></div>
        <div><small>Confianza</small><strong>${escapeHtml(`${macro.confidence ?? 0}%`)}</strong></div>
        <div><small>Activos</small><strong>${escapeHtml(affected || "Sin activos")}</strong></div>
      </section>
      <div class="news-feed">
        ${appState.newsLoading ? `<div class="empty-state"><strong>Cargando noticias.</strong><p>Genesis esta armando el resumen del dia.</p></div>` : ""}
        ${newsItems.length ? newsItems.map(newsCardMarkup).join("") : emptyStateMarkup("Sin noticias activas.", "Cuando exista macro, geopolitica, earnings o catalizadores, Genesis lo mostrara aqui sin inventar impacto.")}
      </div>
    </section>
  `;
}

function newsFeedItems(snapshot = {}) {
  const macro = snapshot.macro?.macro || {};
  const activity = snapshot.macro?.activity || {};
  const headlines = Array.isArray(macro.headlines) ? macro.headlines : [];
  const activityItems = Array.isArray(activity.items) ? activity.items : [];
  return [
    ...headlines.map((item) => ({
      category: "Macro",
      title: item.title,
      source: item.source,
      time: item.published_at,
      impact: item.impact_summary || macro.bias_label || "Neutral",
      summary: item.impact_summary || "Catalizador macro en vigilancia.",
      assets: macro.sensitive_tickers || [],
    })),
    ...activityItems.map((item) => ({
      category: item.event || "Evento",
      title: item.summary,
      source: "Genesis",
      time: item.occurred_at,
      impact: item.level || "neutral",
      summary: item.summary,
      assets: Object.values(item.fields || {}).filter((value) => typeof value === "string").slice(0, 3),
    })),
  ].filter((item) => String(item.title || "").trim()).slice(0, 12);
}

function newsAffectedAssets(snapshot = {}) {
  const macro = snapshot.macro?.macro || {};
  const assets = [...(macro.high_risk_tickers || []), ...(macro.sensitive_tickers || [])]
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

function newsCardMarkup(item) {
  const assets = Array.isArray(item.assets) ? item.assets.filter(Boolean).slice(0, 4) : [];
  const assetLabels = assets.map((asset) => {
    const normalized = normalizeTicker(asset);
    if (!normalized) return cleanCopy(asset);
    const display = getAssetDisplayName(normalized);
    return display.displayName === normalized ? normalized : `${display.displayName} (${normalized})`;
  });
  const tone = newsImpactTone(item.impact);
  return `
    <article class="news-card">
      <div>
        <span class="feed-kicker">${escapeHtml(cleanCopy(item.category || "Contexto"))}</span>
        <strong>${escapeHtml(cleanCopy(item.title || "Noticia"))}</strong>
        <p>${escapeHtml(cleanCopy(item.summary || "Sin lectura adicional."))}</p>
      </div>
      <div class="news-meta">
        <span>${escapeHtml(cleanCopy(item.source || "Fuente activa"))}</span>
        <span>${escapeHtml(formatDate(item.time))}</span>
        <span class="${tone}">${escapeHtml(cleanCopy(item.impact || "Neutral"))}</span>
        ${assetLabels.length ? `<span>${escapeHtml(assetLabels.join(", "))}</span>` : ""}
      </div>
    </article>
  `;
}

function renderTrackingScreen() {
  const root = document.getElementById("view-watchlist");
  if (!root) return;
  const items = filteredTrackingItems();
  const status = `
    <div class="screen-status inline-status">
      <span>Datos directos activos</span>
      <small>${appState.lastUpdated ? `Actualizado ${formatDate(appState.lastUpdated)}` : "Actualizando..."}</small>
    </div>
  `;
  root.innerHTML = `
    <section class="screen-stack">
      ${status}
      <form class="search-card premium-search" id="tracking-search-form">
        <input id="portfolio-search-input" placeholder="Buscar activos, empresas o ETFs..." autocomplete="off" value="${escapeHtml(appState.trackingSearchQuery)}">
        <button class="round-button icon-submit" id="portfolio-search-button" type="button" aria-label="Agregar a seguimiento">${iconSvg("add")}</button>
      </form>
      <div class="market-filters" aria-label="Filtros de seguimiento">
        ${trackingFilterMarkup()}
      </div>
      <div class="search-results" id="portfolio-search-result" ${appState.marketSearchResults.tracking.length ? "" : "hidden"}>
        ${appState.marketSearchResults.tracking.map((item) => searchResultMarkup(item, "tracking")).join("")}
      </div>
      <div class="asset-list" id="watchlist-screen-body">
        ${items.length ? items.map((item) => assetRowMarkup(item, "tracking")).join("") : emptyStateMarkup("Sin activos en seguimiento.", "Busca un ticker y agregalo para ver precio, sesion y movimiento.")}
      </div>
    </section>
  `;
  const searchButton = document.getElementById("portfolio-search-button");
  searchButton.addEventListener("click", searchAndAddPortfolioTicker);
  document.getElementById("portfolio-search-input").addEventListener("input", (event) => {
    appState.trackingSearchQuery = event.target.value;
  });
  document.getElementById("tracking-search-form").addEventListener("submit", (event) => {
    event.preventDefault();
    searchTrackingOnly();
  });
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
  const totals = appState.portfolioTotals;
  const distribution = buildDistribution();
  const topRow = distribution.reduce((best, row) => (!best || row.weight > best.weight ? row : best), null);
  const portfolioPnl = totals.totalPnl ?? totals.dailyPnl;
  const portfolioPnlPct = totals.totalPnlPct ?? totals.dailyPnlPct;
  const portfolioTone = movementTone(portfolioPnlPct ?? portfolioPnl);
  const concentrationLabel = topRow ? `${itemTicker(topRow.item)} ${compactPercent(topRow.weight)}` : "para calcular pesos";
  const status = `
    <div class="screen-status inline-status">
      <span>Paper trading</span>
      <small>${appState.lastUpdated ? `Actualizado ${formatDate(appState.lastUpdated)}` : "Actualizando..."}</small>
    </div>
  `;
  root.innerHTML = `
    <section class="screen-stack">
      ${status}
      <form class="search-card premium-search" id="portfolio-buy-search-form">
        <input id="portfolio-buy-search-input" placeholder="Buscar ticker o empresa para simular compra" autocomplete="off" value="${escapeHtml(appState.portfolioSearchQuery)}">
        <button class="primary-button small" type="button" id="portfolio-sim-buy-button">Buscar</button>
      </form>
      <div class="search-results" id="portfolio-buy-search-result" ${appState.marketSearchResults.portfolio.length ? "" : "hidden"}>
        ${appState.marketSearchResults.portfolio.map((item) => searchResultMarkup(item, "portfolio")).join("")}
      </div>
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
  document.getElementById("portfolio-sim-buy-button").addEventListener("click", searchPortfolioBuyTicker);
  document.getElementById("portfolio-buy-search-input").addEventListener("input", (event) => {
    appState.portfolioSearchQuery = event.target.value;
  });
  document.getElementById("portfolio-buy-search-form").addEventListener("submit", (event) => {
    event.preventDefault();
    searchPortfolioBuyTicker();
  });
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
  return `
    <article class="asset-row compact-market-row" data-ticker="${escapeHtml(ticker)}" data-mode="${mode}">
      <button class="asset-main" type="button" data-open-asset="${escapeHtml(ticker)}">
        <span class="asset-title">
          <strong>${escapeHtml(display.displayName)}</strong>
          <small>${escapeHtml(subline || display.subtitle || ticker)}</small>
        </span>
        <span class="price-stack">
          <strong class="${marketToneClass(item)}">${escapeHtml(priceLabel(item))}</strong>
          <span class="change-stack ${tone}">${dailyMoveMarkup(item)}</span>
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
  const [causal, detection] = await Promise.all([
    fetch("/api/dashboard/money-flow/causal", { cache: "no-store" }).then((response) => response.json()),
    fetch("/api/dashboard/money-flow/detection", { cache: "no-store" }).then((response) => response.json()),
  ]);
  appState.whalesSnapshot = { causal, detection };
  return appState.whalesSnapshot;
}

function renderMoneyFlowSnapshot(causalPayload = {}, detectionPayload = {}) {
  appState.whalesSnapshot = { causal: causalPayload, detection: detectionPayload };
  renderAlertsScreen();
}

function renderWhalesScreen() {
  renderAlertsScreen();
}

function extractWhaleRows(causal, detection) {
  const candidates = [
    ...(Array.isArray(causal.items) ? causal.items : []),
    ...(Array.isArray(causal.causal?.items) ? causal.causal.items : []),
    ...(Array.isArray(detection.items) ? detection.items : []),
    ...(Array.isArray(detection.detection?.items) ? detection.detection.items : []),
  ];
  const byTicker = new Map();
  candidates.forEach((item) => {
    const ticker = itemTicker(item);
    if (!ticker || byTicker.has(ticker)) return;
    const whale = typeof item.whale === "object" && item.whale ? item.whale : {};
    const identified = Boolean(item.whale_identified || whale.identified || whale.entity);
    if (!identified) return;
    byTicker.set(ticker, {
      ticker,
      event: classifyWhaleType(whale.movement_type || item.direction || item.primary_label),
      entity: whale.entity || item.whale_entity || "",
      amount: whale.movement_value || item.movement_value || item.amount_usd || "",
      date: item.money_flow_timestamp || item.timestamp || item.updated_at || "",
      source: whale.source || item.source || item.origin || "Fuente activa",
      confidence: whale.confidence || item.confidence || item.confidence_label || "no concluyente",
      read: "Genesis detecta una entidad reportada y lo trata como evidencia adicional, no como causalidad garantizada.",
      missing: "Falta continuidad y contexto de precio para elevar conviccion.",
    });
  });
  return Array.from(byTicker.values()).slice(0, 12);
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
  const eventClass = `event-${String(row.event || "no-confirmado").toLowerCase().replace(/\s+/g, "-")}`;
  const display = getAssetDisplayName(row.ticker);
  return `
    <article class="whale-row feed-row">
      <div class="whale-topline">
        <div>
          <strong>${escapeHtml(display.displayName)}</strong>
          <small>${escapeHtml(row.entity || "Entidad no identificada")}</small>
        </div>
        <span class="event-chip ${eventClass}">${escapeHtml(row.event)}</span>
      </div>
      <p>${escapeHtml(row.read)}</p>
      <div class="asset-meta">
        <span>Monto: ${escapeHtml(row.amount || "No confirmado")}</span>
        <span>Fecha: ${escapeHtml(formatDate(row.date))}</span>
        <span>Fuente: ${escapeHtml(row.source || "Fuente activa")}</span>
        <span>Confianza: ${escapeHtml(row.confidence)}</span>
      </div>
      <small>${escapeHtml(row.missing)}</small>
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
      const payload = await fetch(`/api/dashboard/money-flow/jarvis?q=${encodeURIComponent(question)}`, { cache: "no-store" }).then((response) => response.json());
      renderMoneyFlowJarvisAnswer(payload);
    } catch (error) {
      answerNode.textContent = cleanCopy(error.message || "No pude consultar Ballenas.");
    }
  });
}

function renderMoneyFlowJarvisAnswer(payload = {}) {
  const node = document.getElementById("money-flow-jarvis-answer");
  if (!node) return;
  node.textContent = cleanCopy(payload.answer || "No hay ballena identificada con la fuente activa.");
}

async function loadAlerts() {
  appState.alertsSnapshot = await getJson("/api/dashboard/alerts");
  return appState.alertsSnapshot;
}

function renderAlertsScreen() {
  const root = document.getElementById("view-alerts");
  if (!root) return;
  const items = Array.isArray(appState.alertsSnapshot?.items) ? appState.alertsSnapshot.items : [];
  const whales = extractWhaleRows(appState.whalesSnapshot?.causal || {}, appState.whalesSnapshot?.detection || {});
  root.innerHTML = `
    <section class="screen-stack">
      <section class="feed-intro">
        <div>
          <strong>Eventos</strong>
          <p>${appState.alertSubtab === "alerts" ? (items.length ? "Feed limpio de senales activas." : "Sin alertas activas por ahora.") : (whales.length ? "Smart money confirmado por fuente activa." : "Sin ballena institucional confirmada ahora.")}</p>
        </div>
        <span>${appState.alertSubtab === "alerts" ? `${items.length} eventos` : `${whales.length} ballenas`}</span>
      </section>
      <div class="subtabs" aria-label="Eventos">
        <button type="button" class="${appState.alertSubtab === "alerts" ? "is-active" : ""}" data-alert-tab="alerts">Alertas</button>
        <button type="button" class="${appState.alertSubtab === "whales" ? "is-active" : ""}" data-alert-tab="whales">Ballenas</button>
      </div>
      ${appState.alertSubtab === "alerts" ? alertsPanelMarkup(items) : whalesPanelMarkup(whales)}
    </section>
  `;
  bindMoneyFlowJarvisForm();
}

function alertsPanelMarkup(items) {
  return `
    <div class="asset-list">
      ${items.length ? items.slice(0, 14).map(alertMarkup).join("") : emptyStateMarkup("Sin alertas activas.", "Genesis mantiene la pantalla limpia hasta que exista una senal relevante.")}
    </div>
  `;
}

function whalesPanelMarkup(rows) {
  return `
    <section class="whale-flow-card">
      <div>
        <span>Flujo de ballenas</span>
        <strong>${rows.length ? `${rows.length} eventos relevantes` : "Sin flujo confirmado"}</strong>
        <p>${rows.length ? "Genesis muestra solo eventos con entidad o evidencia reportada." : "No hay ballena institucional confirmada con la fuente activa."}</p>
      </div>
      <div class="whale-flow-bars" aria-hidden="true">
        ${rows.length ? rows.slice(0, 6).map((row, index) => `<i style="height:${Math.max(18, 34 + index * 8)}px"></i>`).join("") : `<i></i><i></i><i></i>`}
      </div>
    </section>
    <form class="search-card whale-search" id="money-flow-jarvis-form">
      <input id="money-flow-jarvis-input" placeholder="Consultar ticker o flujo smart money" autocomplete="off">
      <button type="submit">${iconSvg("send")}</button>
    </form>
    <div class="whale-answer" id="money-flow-jarvis-answer">Lectura Genesis lista para consultar.</div>
    <div class="asset-list whales-list" id="whales-list">
      ${rows.length ? rows.map(whaleRowMarkup).join("") : emptyStateMarkup("Sin ballenas confirmadas.", "Cuando FMP confirme entidad, monto y fecha, Genesis lo mostrara aqui sin inventar instituciones.")}
    </div>
  `;
}

function alertMarkup(item) {
  const ticker = itemTicker(item) || "Mercado";
  const display = ticker === "Mercado" ? { displayName: "Mercado", subtitle: "" } : getAssetDisplayName(ticker);
  const priority = cleanCopy(item.priority || item.severity || item.status_label || item.status || "Seguimiento");
  const date = item.created_at || item.updated_at || item.timestamp || appState.lastUpdated;
  const impact = item.impact || item.impact_probable || item.latest_validation?.outcome_label || priority;
  const tone = newsImpactTone(impact);
  return `
    <article class="whale-row feed-row alert-event tone-${tone}">
      <div class="whale-topline">
        <div>
          <strong>${escapeHtml(display.displayName)}</strong>
          <small>${escapeHtml(cleanCopy(item.title || item.event || item.status || "Alerta"))}</small>
        </div>
        <span class="event-chip">${escapeHtml(priority)}</span>
      </div>
      <p>${escapeHtml(cleanCopy(item.summary || item.message || item.note || "Revisar antes de operar."))}</p>
      <div class="mini-spark" aria-hidden="true"><i></i><i></i><i></i><i></i><i></i></div>
      <div class="asset-meta">
        <span class="${tone}">Impacto: ${escapeHtml(cleanCopy(impact || "Por confirmar"))}</span>
        <span>Contexto: ${escapeHtml(cleanCopy(item.context || item.category || "Mercado"))}</span>
        <span>Fecha: ${escapeHtml(formatDate(date))}</span>
        <span>Estado: ${escapeHtml(cleanCopy(item.status || "En vigilancia"))}</span>
      </div>
    </article>
  `;
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
  const item = findAsset(normalized) || { ticker: normalized };
  const isPaper = appState.paperPositions.some((row) => itemTicker(row) === normalized);
  const isTracked = appState.trackingItems.some((row) => itemTicker(row) === normalized && itemInWatchlist(row));
  const chartRange = appState.assetChartRanges[normalized] || "1Y";
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

  document.body.addEventListener("click", async (event) => {
    if (event.target.closest("[data-toast-close]")) {
      hideToast();
      return;
    }

    if (event.target.closest("[data-news-refresh]")) {
      event.preventDefault();
      loadNews().catch((error) => toast(error.message, "error"));
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

    const trackingFilter = event.target.closest("[data-tracking-filter]");
    if (trackingFilter) {
      event.preventDefault();
      appState.trackingFilter = trackingFilter.dataset.trackingFilter || "all";
      renderTrackingScreen();
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
  });
}

document.addEventListener("DOMContentLoaded", initGenesisAppV3);
