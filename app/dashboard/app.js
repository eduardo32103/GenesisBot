const PORTFOLIO_ENDPOINT = "/api/dashboard/portfolio";
const RADAR_ENDPOINT = "/api/dashboard/radar";

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
  whalesSnapshot: null,
  alertsSnapshot: null,
  refreshTimer: null,
  refreshInFlight: false,
  refreshPromise: null,
  chatMessages: [
    {
      role: "assistant",
      text: "Estoy listo. Dame un ticker, una posicion o una alerta y te devuelvo una lectura compacta.",
    },
  ],
};

const SCREEN_META = {
  genesis: { title: "Genesis", kicker: "Tu analista privado" },
  tracking: { title: "Seguimiento", kicker: "Datos directos activos" },
  portfolio: { title: "Cartera", kicker: "Paper trading" },
  whales: { title: "Ballenas", kicker: "Smart money" },
  alerts: { title: "Alertas", kicker: "Senales" },
};

const REFRESH_MS = 15000;
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

function itemTicker(item) {
  return normalizeTicker(item?.ticker || item?.symbol);
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

function formatChange(value, empty = "Sin cambio") {
  return signedMoney(value, empty);
}

function formatPercent(value, empty = "Sin dato") {
  return percent(value, empty);
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

function movementTone(itemOrValue) {
  const value = typeof itemOrValue === "object"
    ? itemDailyPct(itemOrValue) ?? itemDailyUsd(itemOrValue) ?? positionPnl(itemOrValue)
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
  };
  return icons[name] || "";
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
  if (view === "money-flow") return "whales";
  return view || "genesis";
}

function screenId(screen) {
  if (screen === "tracking") return "view-watchlist";
  if (screen === "portfolio") return "view-radar";
  if (screen === "whales") return "view-money-flow";
  return `view-${screen}`;
}

function updateNav() {
  document.querySelectorAll(".nav-link").forEach((button) => {
    button.classList.toggle("is-active", normalizeScreen(button.dataset.view) === appState.activeScreen);
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
    if (appState.activeScreen !== "tracking" && appState.activeScreen !== "portfolio") return;
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

  if (screen === "tracking" || screen === "portfolio") {
    startPortfolioAutoRefresh();
    refreshPortfolio({ render: true }).catch((error) => toast(error.message, "error"));
  } else {
    stopPortfolioAutoRefresh();
  }

  if (screen === "whales") {
    loadWhales().catch((error) => toast(error.message, "error"));
  }
  if (screen === "alerts") {
    loadAlerts().catch((error) => toast(error.message, "error"));
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
  renderTrackingScreen();
  renderPortfolioScreen();
  renderWhalesScreen();
  renderAlertsScreen();
  updateNav();
}

function renderActiveScreen() {
  if (appState.activeScreen === "genesis") renderGenesisScreen();
  if (appState.activeScreen === "tracking") renderTrackingScreen();
  if (appState.activeScreen === "portfolio") renderPortfolioScreen();
  if (appState.activeScreen === "whales") renderWhalesScreen();
  if (appState.activeScreen === "alerts") renderAlertsScreen();
  updateNav();
}

function screenHeaderMarkup(screen, right = "") {
  const meta = SCREEN_META[screen] || SCREEN_META.genesis;
  return `
    <header class="screen-header">
      <div>
        <span class="app-kicker">${escapeHtml(meta.kicker)}</span>
        <h1>${escapeHtml(meta.title)}</h1>
      </div>
      ${right}
    </header>
  `;
}

function renderGenesisScreen() {
  const root = document.getElementById("view-genesis");
  if (!root) return;
  root.innerHTML = `
    <section class="genesis-stage">
      <section class="genesis-hero-card" aria-label="Genesis">
        <div class="genesis-mark" aria-hidden="true">G</div>
        <div class="genesis-identity">
          <span class="app-kicker">Tu analista financiero privado</span>
          <h1>Genesis</h1>
          <p>Pregunta. Analiza. Decide con contexto.</p>
        </div>
        <div class="genesis-hero-footer">
          <span>Live</span>
          <span>Paper</span>
          <span>Alertas</span>
        </div>
      </section>
      <div class="genesis-conversation">
        <div class="chat-thread" id="genesis-thread">
          ${appState.chatMessages.map(chatBubbleMarkup).join("")}
        </div>
        <form class="chat-form" id="genesis-chat-form">
          <input id="genesis-chat-input" placeholder="Pregunta a Genesis..." autocomplete="off">
          <button type="submit">Enviar</button>
        </form>
      </div>
    </section>
  `;
  const form = document.getElementById("genesis-chat-form");
  form.addEventListener("submit", submitGenesisQuestion);
  const thread = document.getElementById("genesis-thread");
  if (thread) thread.scrollTop = thread.scrollHeight;
}

function chatBubbleMarkup(message) {
  const role = message.role === "user" ? "user" : "assistant";
  return `
    <article class="chat-bubble ${role}">
      <strong>${role === "user" ? "Tu" : "Genesis"}</strong>
      <p>${escapeHtml(cleanCopy(message.text))}</p>
    </article>
  `;
}

async function submitGenesisQuestion(event) {
  event.preventDefault();
  const input = document.getElementById("genesis-chat-input");
  const question = String(input?.value || "").trim();
  if (!question) return;
  input.value = "";
  appState.chatMessages.push({ role: "user", text: question });
  renderGenesisScreen();
  try {
    const payload = await getJson(`/api/dashboard/genesis?q=${encodeURIComponent(question)}&context=${encodeURIComponent(appState.activeScreen)}&ticker=&panel_context=`);
    const answer = payload.assistant_narrative || payload.answer || "No tengo lectura suficiente.";
    appState.chatMessages.push({ role: "assistant", text: cleanCopy(answer) });
  } catch (error) {
    appState.chatMessages.push({ role: "assistant", text: `No pude responder ahora: ${cleanCopy(error.message)}` });
  }
  renderGenesisScreen();
}

function renderTrackingScreen() {
  const root = document.getElementById("view-watchlist");
  if (!root) return;
  const status = `
    <div class="screen-status">
      <span>Datos directos activos</span>
      <small>${appState.lastUpdated ? `Actualizado ${formatDate(appState.lastUpdated)}` : "Actualizando..."}</small>
    </div>
  `;
  root.innerHTML = `
    <section class="screen-stack">
      ${screenHeaderMarkup("tracking", status)}
      <form class="search-card premium-search" id="tracking-search-form">
        <input id="portfolio-search-input" placeholder="Buscar ticker o empresa" autocomplete="off" value="${escapeHtml(appState.trackingSearchQuery)}">
        <button class="round-button icon-submit" id="portfolio-search-button" type="button" aria-label="Agregar a seguimiento">${iconSvg("add")}</button>
      </form>
      <div class="search-results" id="portfolio-search-result" ${appState.marketSearchResults.tracking.length ? "" : "hidden"}>
        ${appState.marketSearchResults.tracking.map((item) => searchResultMarkup(item, "tracking")).join("")}
      </div>
      <div class="asset-list" id="watchlist-screen-body">
        ${appState.trackingItems.length ? appState.trackingItems.map((item) => assetRowMarkup(item, "tracking")).join("") : emptyStateMarkup("Sin activos en seguimiento.", "Busca un ticker y agregalo para ver precio, sesion y movimiento.")}
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
    <div class="screen-status">
      <span>Paper trading</span>
      <small>${appState.lastUpdated ? `Actualizado ${formatDate(appState.lastUpdated)}` : "Actualizando..."}</small>
    </div>
  `;
  root.innerHTML = `
    <section class="screen-stack">
      ${screenHeaderMarkup("portfolio", status)}
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
      <span id="radar-summary-note" hidden>Genesis App V3</span>
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

function searchResultMarkup(item, mode) {
  const ticker = itemTicker(item);
  const tone = movementTone(item);
  const action = mode === "tracking"
    ? `<button class="compact-action" type="button" data-market-add="${escapeHtml(ticker)}">${iconSvg("add")}<span>Seguimiento</span></button>`
    : `<button class="compact-action" type="button" data-paper-buy="${escapeHtml(ticker)}">${iconSvg("cart")}<span>Comprar</span></button>`;
  return `
    <article class="search-result" data-search-result="${escapeHtml(ticker)}">
      <button class="search-main" type="button" data-open-asset="${escapeHtml(ticker)}">
        <span>
          <strong>${escapeHtml(ticker)}</strong>
          <small>${escapeHtml(item.name || item.display_name || ticker)}</small>
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
  const value = itemValue(item);
  const units = itemUnits(item);
  const weight = mode === "paper" && totalValue > 0 && value !== null ? (value / totalValue) * 100 : numberOrNull(item.weight_pct);
  const pnl = positionPnl(item);
  const updated = item.quote_timestamp || item.updated_at;
  const action = mode === "paper"
    ? `<button class="icon-action danger" type="button" data-paper-close="${escapeHtml(ticker)}" aria-label="Cerrar paper ${escapeHtml(ticker)}" title="Cerrar paper">${iconSvg("remove")}</button>`
    : `<button class="icon-action danger" type="button" data-watch-remove="${escapeHtml(ticker)}" aria-label="Quitar ${escapeHtml(ticker)} de seguimiento" title="Quitar">${iconSvg("remove")}</button>`;
  const contextChips = `
    <span>${escapeHtml(previousCloseLabel(item))}</span>
    <span>${escapeHtml(extendedLabel(item))}</span>
    <span>${escapeHtml(marketSessionLabel(item))}</span>
  `;
  const paperMetrics = mode === "paper"
    ? `
      <div class="position-metrics">
        <span><small>Units</small><strong class="flat">${escapeHtml(units)}</strong></span>
        <span><small>Valor</small><strong class="market-number flat">${escapeHtml(money(value))}</strong></span>
        <span><small>Peso</small><strong class="market-number flat">${escapeHtml(compactPercent(weight))}</strong></span>
        <span><small>P/L</small><strong class="${marketToneClass(pnl)}">${escapeHtml(formatChange(pnl, "Sin dato"))}</strong></span>
      </div>
    `
    : "";
  return `
    <article class="asset-row" data-ticker="${escapeHtml(ticker)}" data-mode="${mode}">
      <button class="asset-main" type="button" data-open-asset="${escapeHtml(ticker)}">
        <span class="asset-title">
          <strong>${escapeHtml(ticker)}</strong>
          <small>${escapeHtml(item.name || item.display_name || ticker)}</small>
          <span class="row-chipline">${contextChips}</span>
        </span>
        <span class="price-stack">
          <strong class="${marketToneClass(item)}">${escapeHtml(priceLabel(item))}</strong>
          <span class="change-stack ${tone}">${dailyMoveMarkup(item)}</span>
        </span>
      </button>
      <div class="asset-meta">
        <span>${escapeHtml(priceSourceLabel(item))}</span>
        <span>${escapeHtml(formatDate(updated))}</span>
      </div>
      ${paperMetrics}
      <div class="row-actions">
        <button class="icon-action" type="button" data-paper-buy="${escapeHtml(ticker)}" aria-label="Compra simulada ${escapeHtml(ticker)}" title="Compra simulada">${iconSvg("cart")}</button>
        ${action}
      </div>
    </article>
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
  const [causal, detection] = await Promise.all([
    fetch("/api/dashboard/money-flow/causal", { cache: "no-store" }).then((response) => response.json()),
    fetch("/api/dashboard/money-flow/detection", { cache: "no-store" }).then((response) => response.json()),
  ]);
  appState.whalesSnapshot = { causal, detection };
  renderWhalesScreen();
}

function renderMoneyFlowSnapshot(causalPayload = {}, detectionPayload = {}) {
  appState.whalesSnapshot = { causal: causalPayload, detection: detectionPayload };
  renderWhalesScreen();
}

function renderWhalesScreen() {
  const root = document.getElementById("view-money-flow");
  if (!root) return;
  const snapshot = appState.whalesSnapshot || {};
  const rows = extractWhaleRows(snapshot.causal || {}, snapshot.detection || {});
  root.innerHTML = `
    <section class="screen-stack">
      ${screenHeaderMarkup("whales", `<div class="screen-status"><span>${rows.length ? `${rows.length} lecturas` : "Sin entidad"}</span><small>${appState.lastUpdated ? formatDate(appState.lastUpdated) : "Fuente activa"}</small></div>`)}
      <section class="whales-hero">
        <strong>Dinero grande, sin inventar.</strong>
        <p>Genesis muestra entidad, monto y fuente solo cuando la evidencia existe.</p>
      </section>
      <form class="search-card" id="money-flow-jarvis-form">
        <input id="money-flow-jarvis-input" placeholder="Preguntar a Ballenas" autocomplete="off">
        <button type="submit">Preguntar</button>
      </form>
      <div class="whale-answer" id="money-flow-jarvis-answer">Lectura Genesis lista para consultar.</div>
      <div class="asset-list whales-list" id="whales-list">
        ${rows.length ? rows.map(whaleRowMarkup).join("") : emptyStateMarkup("No hay ballena institucional confirmada con la fuente activa.", "Cuando la fuente confirme entidad, monto y fecha, Genesis lo mostrara como feed limpio.")}
      </div>
    </section>
  `;
  bindMoneyFlowJarvisForm();
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
    byTicker.set(ticker, {
      ticker,
      event: identified ? classifyWhaleType(whale.movement_type || item.direction || item.primary_label) : "No confirmado",
      entity: identified ? (whale.entity || item.whale_entity || "") : "",
      amount: whale.movement_value || item.movement_value || item.amount_usd || "",
      date: item.money_flow_timestamp || item.timestamp || item.updated_at || "",
      source: whale.source || item.source || item.origin || "Fuente activa",
      confidence: whale.confidence || item.confidence || item.confidence_label || "no concluyente",
      read: identified
        ? `Genesis detecta una entidad reportada y lo trata como evidencia adicional, no como causalidad garantizada.`
        : "No hay entidad institucional confirmada con la fuente activa.",
      missing: identified ? "Falta continuidad y contexto de precio para elevar conviccion." : "Faltan entidad, monto o fecha confirmada.",
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
  return `
    <article class="whale-row feed-row">
      <div class="whale-topline">
        <div>
          <strong>${escapeHtml(row.ticker)}</strong>
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
  renderAlertsScreen();
}

function renderAlertsScreen() {
  const root = document.getElementById("view-alerts");
  if (!root) return;
  const items = Array.isArray(appState.alertsSnapshot?.items) ? appState.alertsSnapshot.items : [];
  root.innerHTML = `
    <section class="screen-stack">
      ${screenHeaderMarkup("alerts", `<div class="screen-status"><span>${items.length} eventos</span><small>${appState.lastUpdated ? formatDate(appState.lastUpdated) : "En vigilancia"}</small></div>`)}
      <div class="asset-list">
        ${items.length ? items.slice(0, 14).map(alertMarkup).join("") : emptyStateMarkup("Sin alertas activas.", "Genesis mantiene la pantalla limpia hasta que exista una senal relevante.")}
      </div>
    </section>
  `;
}

function alertMarkup(item) {
  const ticker = itemTicker(item) || "Mercado";
  const priority = cleanCopy(item.priority || item.severity || item.status_label || item.status || "Seguimiento");
  const date = item.created_at || item.updated_at || item.timestamp || appState.lastUpdated;
  return `
    <article class="whale-row feed-row">
      <div class="whale-topline">
        <div>
          <strong>${escapeHtml(ticker)}</strong>
          <small>${escapeHtml(cleanCopy(item.title || item.event || item.status || "Alerta"))}</small>
        </div>
        <span class="event-chip">${escapeHtml(priority)}</span>
      </div>
      <p>${escapeHtml(cleanCopy(item.summary || item.message || item.note || "Revisar antes de operar."))}</p>
      <div class="asset-meta">
        <span>Contexto: ${escapeHtml(cleanCopy(item.context || item.category || "Mercado"))}</span>
        <span>Fecha: ${escapeHtml(formatDate(date))}</span>
        <span>Estado: ${escapeHtml(cleanCopy(item.status || "En vigilancia"))}</span>
      </div>
    </article>
  `;
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
  const sheet = document.getElementById("asset-sheet");
  const body = document.getElementById("asset-sheet-body");
  body.innerHTML = `
    <span class="app-kicker">${isPaper ? "Paper" : isTracked ? "Seguimiento" : "No agregado"}</span>
    <h2>${escapeHtml(normalized)}</h2>
    <p class="asset-name">${escapeHtml(item.name || item.display_name || normalized)}</p>
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
  document.getElementById("paper-buy-label").value = `${normalized} | ${item.name || item.display_name || normalized}`;
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

    const openAsset = event.target.closest("[data-open-asset]");
    if (openAsset) {
      event.preventDefault();
      openAssetSheet(openAsset.dataset.openAsset);
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
  refreshPortfolio({ render: false }).then(() => renderActiveScreen()).catch((error) => toast(error.message, "error"));
  loadWhales().catch(() => {});
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden && (appState.activeScreen === "tracking" || appState.activeScreen === "portfolio")) {
      refreshPortfolio({ render: true }).catch(() => {});
    }
  });
}

document.addEventListener("DOMContentLoaded", initGenesisAppV3);
