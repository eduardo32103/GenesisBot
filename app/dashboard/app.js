const appState = {
  activeScreen: "genesis",
  tracking: [],
  portfolio: [],
  paperPositions: [],
  selectedAsset: "",
  marketQuotes: new Map(),
  lastUpdated: "",
  loading: false,
  error: "",
  searchQuery: "",
  searchResults: [],
  portfolioSnapshot: null,
  whalesSnapshot: null,
  alertsSnapshot: null,
  refreshTimer: null,
  refreshInFlight: false,
};

const SCREEN_META = {
  genesis: { title: "Genesis", kicker: "Asistente financiero" },
  tracking: { title: "Seguimiento", kicker: "Mercado" },
  portfolio: { title: "Cartera", kicker: "Paper trading" },
  whales: { title: "Ballenas", kicker: "Smart money" },
  alerts: { title: "Alertas", kicker: "Senales" },
};

const MONEY_COLORS = ["#8ce0b8", "#88a9ff", "#f1b86d", "#ed8078", "#b7c5d9", "#77d7df"];
const REFRESH_MS = 30000;

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

function money(value, empty = "Sin valor") {
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

function toast(message, tone = "ok") {
  const node = document.getElementById("app-toast");
  if (!node) return;
  node.textContent = cleanCopy(message);
  node.dataset.tone = tone;
  node.hidden = false;
  clearTimeout(toast._timer);
  toast._timer = setTimeout(() => {
    node.hidden = true;
  }, 2800);
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

function normalizeTicker(value) {
  return String(value || "").trim().toUpperCase();
}

function itemTicker(item) {
  return normalizeTicker(item?.ticker || item?.symbol);
}

function itemPrice(item) {
  return positiveOrNull(item?.current_price) ?? positiveOrNull(item?.price) ?? positiveOrNull(item?.reference_price);
}

function itemDailyPct(item) {
  return numberOrNull(item?.daily_change_pct ?? item?.change_pct ?? item?.percent_change ?? item?.changesPercentage);
}

function itemDailyUsd(item) {
  return numberOrNull(item?.daily_change ?? item?.change ?? item?.change_usd);
}

function itemUnits(item) {
  return positiveOrNull(item?.units);
}

function itemInWatchlist(item) {
  return item?.watchlist === true || itemUnits(item) === null;
}

function itemValue(item) {
  const explicit = positiveOrNull(item?.market_value) ?? positiveOrNull(item?.current_value);
  if (explicit !== null) return explicit;
  const units = itemUnits(item);
  const price = itemPrice(item);
  return units !== null && price !== null ? units * price : null;
}

function positionPnl(item) {
  const explicit = numberOrNull(item?.unrealized_pnl ?? item?.pnl_usd);
  if (explicit !== null) return explicit;
  const value = itemValue(item);
  const cost = positiveOrNull(item?.cost_basis) ?? positiveOrNull(item?.amount_usd);
  return value !== null && cost !== null ? value - cost : null;
}

function priceLabel(item) {
  const price = itemPrice(item);
  return price === null ? "Sin precio" : money(price);
}

function dailyMoveLabel(item) {
  const usd = itemDailyUsd(item);
  const pct = itemDailyPct(item);
  if (usd === null && pct === null) return "Sin cambio";
  if (usd === null) return percent(pct);
  if (pct === null) return signedMoney(usd);
  return `${signedMoney(usd)} ${percent(pct)}`;
}

function movementTone(itemOrValue) {
  const value = typeof itemOrValue === "object" ? itemDailyPct(itemOrValue) : numberOrNull(itemOrValue);
  if (value === null || value === 0) return "flat";
  return value > 0 ? "up" : "down";
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

function updateHeader() {
  const meta = SCREEN_META[appState.activeScreen] || SCREEN_META.genesis;
  document.getElementById("screen-title").textContent = meta.title;
  document.getElementById("screen-kicker").textContent = meta.kicker;
  document.getElementById("last-updated").textContent = appState.lastUpdated
    ? `Actualizado ${formatDate(appState.lastUpdated)}`
    : "Actualizando...";
  document.querySelectorAll(".nav-link").forEach((button) => {
    button.classList.toggle("is-active", normalizeScreen(button.dataset.view) === appState.activeScreen);
  });
}

function normalizeScreen(view) {
  if (view === "watchlist") return "tracking";
  if (view === "radar") return "portfolio";
  if (view === "money-flow") return "whales";
  if (view === "command-center") return "genesis";
  return view || "genesis";
}

function screenId(screen) {
  if (screen === "tracking") return "view-watchlist";
  if (screen === "portfolio") return "view-radar";
  if (screen === "whales") return "view-money-flow";
  return `view-${screen}`;
}

function setActiveScreen(screen) {
  appState.activeScreen = screen;
  document.querySelectorAll(".app-screen").forEach((node) => {
    node.classList.toggle("is-active", node.id === screenId(screen));
  });
  updateHeader();
  if (screen === "tracking" || screen === "portfolio") {
    refreshPortfolio().catch((error) => toast(error.message, "bad"));
    startPortfolioAutoRefresh();
  } else {
    stopPortfolioAutoRefresh();
  }
  if (screen === "whales") {
    loadWhales().catch((error) => toast(error.message, "bad"));
  }
  if (screen === "alerts") {
    loadAlerts().catch((error) => toast(error.message, "bad"));
  }
  render();
}

function splitPortfolioItems(items) {
  const rows = Array.isArray(items) ? items : [];
  appState.paperPositions = rows.filter((item) => itemUnits(item) !== null);
  appState.tracking = rows.filter((item) => itemInWatchlist(item));
  appState.portfolio = rows;
  appState.marketQuotes = new Map(rows.map((item) => [itemTicker(item), item]));
}

async function refreshPortfolio() {
  if (appState.refreshInFlight) return appState.portfolioSnapshot;
  appState.refreshInFlight = true;
  try {
    const snapshot = await getJson("/api/dashboard/portfolio");
    appState.portfolioSnapshot = snapshot;
    splitPortfolioItems(snapshot.items || []);
    appState.lastUpdated = snapshot.summary?.last_update || snapshot.generated_at || new Date().toISOString();
    render();
    return snapshot;
  } finally {
    appState.refreshInFlight = false;
  }
}

function startPortfolioAutoRefresh() {
  if (appState.refreshTimer) return;
  appState.refreshTimer = setInterval(() => {
    if (document.hidden) return;
    if (appState.activeScreen !== "tracking" && appState.activeScreen !== "portfolio") return;
    refreshPortfolio().catch(() => {});
  }, REFRESH_MS);
}

function stopPortfolioAutoRefresh() {
  if (!appState.refreshTimer) return;
  clearInterval(appState.refreshTimer);
  appState.refreshTimer = null;
}

async function searchMarket(query) {
  const value = String(query || "").trim();
  if (!value) throw new Error("Escribe un ticker o nombre.");
  appState.searchQuery = value;
  const payload = await getJson(`/api/dashboard/market/search?q=${encodeURIComponent(value)}`);
  const results = Array.isArray(payload.results) ? payload.results : [];
  appState.searchResults = results;
  renderTrackingScreen();
  if (!payload.ok || !results.length) {
    throw new Error(payload.message || "No encontre ese ticker en mercado.");
  }
  return results;
}

async function searchAndAddPortfolioTicker() {
  const input = document.getElementById("portfolio-search-input");
  const query = input?.value || appState.searchQuery || "";
  try {
    const results = await searchMarket(query);
    await addTickerToWatchlist(results[0].ticker);
  } catch (error) {
    toast(error.message || "No pude agregar el activo.", "bad");
  }
}

async function addTickerToWatchlist(ticker) {
  const normalized = normalizeTicker(ticker);
  if (!normalized) throw new Error("Ticker no valido.");
  await postJson("/api/dashboard/portfolio/watchlist/add", { ticker: normalized });
  await refreshPortfolio();
  const exists = appState.tracking.some((item) => itemTicker(item) === normalized)
    || appState.paperPositions.some((item) => itemTicker(item) === normalized);
  if (!exists) throw new Error("El activo no aparecio en el snapshot actualizado.");
  toast(`${normalized} agregado a seguimiento.`);
}

async function removeTickerFromWatchlist(ticker) {
  const normalized = normalizeTicker(ticker);
  if (!normalized) return;
  await postJson("/api/dashboard/portfolio/watchlist/remove", { ticker: normalized });
  await refreshPortfolio();
  const stillThere = appState.tracking.some((item) => itemTicker(item) === normalized && itemInWatchlist(item));
  if (stillThere) throw new Error(`${normalized} sigue en seguimiento despues de quitarlo.`);
  toast(`${normalized} quitado de seguimiento.`);
}

async function savePaperBuy(ticker, units, entryPrice) {
  const normalized = normalizeTicker(ticker);
  await postJson("/api/dashboard/portfolio/paper-buy", {
    ticker: normalized,
    units,
    entry_price: entryPrice,
    mode: "paper",
  });
  await refreshPortfolio();
  const position = appState.paperPositions.find((item) => itemTicker(item) === normalized);
  if (!position) throw new Error("La compra simulada no aparecio en Cartera.");
  toast(`Compra simulada de ${normalized} guardada.`);
}

async function removePaperTicker(ticker) {
  const normalized = normalizeTicker(ticker);
  if (!normalized) return;
  await postJson("/api/dashboard/portfolio/paper-remove", { ticker: normalized });
  await refreshPortfolio();
  const stillThere = appState.paperPositions.some((item) => itemTicker(item) === normalized);
  if (stillThere) throw new Error(`${normalized} sigue como paper despues de cerrarlo.`);
  toast(`Paper de ${normalized} cerrado.`);
}

function render() {
  updateHeader();
  renderGenesisScreen();
  renderTrackingScreen();
  renderPortfolioScreen();
  renderWhalesScreen();
  renderAlertsScreen();
}

function renderGenesisScreen() {
  const root = document.getElementById("view-genesis");
  if (!root || root.dataset.rendered === "true") return;
  root.dataset.rendered = "true";
  root.innerHTML = `
    <section class="genesis-card">
      <div class="chat-thread" id="genesis-thread">
        <article class="chat-bubble assistant">
          <strong>Genesis</strong>
          <p>Estoy listo. Preguntame por un activo, una alerta o tu cartera y te respondo en modo compacto.</p>
        </article>
      </div>
      <form class="chat-form" id="genesis-chat-form">
        <input id="genesis-chat-input" placeholder="Pregunta a Genesis..." autocomplete="off">
        <button type="submit">Enviar</button>
      </form>
    </section>
  `;
  document.getElementById("genesis-chat-form").addEventListener("submit", submitGenesisQuestion);
}

async function submitGenesisQuestion(event) {
  event.preventDefault();
  const input = document.getElementById("genesis-chat-input");
  const question = String(input?.value || "").trim();
  if (!question) return;
  input.value = "";
  appendGenesisMessage("user", question);
  try {
    const payload = await getJson(`/api/dashboard/genesis?q=${encodeURIComponent(question)}&context=${encodeURIComponent(appState.activeScreen)}&ticker=&panel_context=`);
    const answer = payload.assistant_narrative || payload.answer || "No tengo lectura suficiente.";
    appendGenesisMessage("assistant", cleanCopy(answer));
  } catch (error) {
    appendGenesisMessage("assistant", `No pude responder ahora: ${cleanCopy(error.message)}`);
  }
}

function appendGenesisMessage(role, text) {
  const thread = document.getElementById("genesis-thread");
  if (!thread) return;
  const article = document.createElement("article");
  article.className = `chat-bubble ${role}`;
  article.innerHTML = `<strong>${role === "user" ? "Tu" : "Genesis"}</strong><p>${escapeHtml(text)}</p>`;
  thread.appendChild(article);
  thread.scrollTop = thread.scrollHeight;
}

function renderTrackingScreen() {
  const root = document.getElementById("view-watchlist");
  if (!root) return;
  root.innerHTML = `
    <section class="screen-stack">
      <div class="screen-hero compact">
        <div>
          <h2>Seguimiento</h2>
          <p>Precios visibles sin abrir detalle.</p>
        </div>
        <span class="pill">${appState.tracking.length} activos</span>
      </div>
      <form class="search-card" id="tracking-search-form">
        <input id="portfolio-search-input" placeholder="META, Tesla, NVDA, SPY, BTC-USD" autocomplete="off" value="${escapeHtml(appState.searchQuery)}">
        <button class="round-button" id="portfolio-search-button" type="button" aria-label="Buscar y agregar">+</button>
      </form>
      <p class="muted-note" id="portfolio-search-note">Escribe ticker o nombre y pulsa +.</p>
      <div class="search-results" id="portfolio-search-result" ${appState.searchResults.length ? "" : "hidden"}>
        ${appState.searchResults.map(searchResultMarkup).join("")}
      </div>
      <div class="asset-list" id="watchlist-screen-body">
        ${appState.tracking.length ? appState.tracking.map((item) => assetRowMarkup(item, "tracking")).join("") : `<div class="empty-state">Sin activos en seguimiento.</div>`}
      </div>
    </section>
  `;
  const searchButton = document.getElementById("portfolio-search-button");
  searchButton.addEventListener("click", searchAndAddPortfolioTicker);
  document.getElementById("portfolio-search-input").addEventListener("input", (event) => {
    appState.searchQuery = event.target.value;
  });
  document.getElementById("tracking-search-form").addEventListener("submit", (event) => {
    event.preventDefault();
    searchAndAddPortfolioTicker();
  });
}

function searchResultMarkup(item) {
  const ticker = itemTicker(item);
  return `
    <article class="search-result">
      <div>
        <strong>${escapeHtml(ticker)}</strong>
        <small>${escapeHtml(item.name || ticker)} | ${escapeHtml(priceLabel(item))} | ${escapeHtml(dailyMoveLabel(item))}</small>
      </div>
      <div class="row-actions">
        <button type="button" data-market-add="${escapeHtml(ticker)}">+</button>
        <button type="button" data-paper-buy="${escapeHtml(ticker)}">Carrito</button>
      </div>
    </article>
  `;
}

function renderPortfolioScreen() {
  const root = document.getElementById("view-radar");
  if (!root) return;
  const totalValue = appState.paperPositions.reduce((sum, item) => sum + (itemValue(item) || 0), 0);
  const totalDaily = appState.paperPositions.reduce((sum, item) => sum + (numberOrNull(item.daily_pnl) || 0), 0);
  const distribution = appState.paperPositions
    .map((item) => ({ item, value: itemValue(item) || 0 }))
    .filter((row) => row.value > 0)
    .map((row) => ({ ...row, weight: totalValue > 0 ? (row.value / totalValue) * 100 : 0 }));
  root.innerHTML = `
    <section class="screen-stack">
      <div class="screen-hero">
        <div>
          <h2>Cartera</h2>
          <p>Solo compras simuladas. Seguimiento no entra al circulo.</p>
        </div>
        <button class="primary-button small" type="button" id="portfolio-sim-buy-button">Agregar compra simulada</button>
      </div>
      <section class="donut-card">
        <div class="portfolio-donut ${distribution.length ? "" : "empty"}" id="portfolio-donut" style="background:${donutGradient(distribution)}">
          <div class="donut-center">
            <strong id="portfolio-total-value">${distribution.length ? money(totalValue) : "Sin compras"}</strong>
            <span id="portfolio-day-return">${distribution.length ? signedMoney(totalDaily, "Sin P/L diario") : "Simula una compra"}</span>
            <small id="portfolio-donut-caption">${distribution.length ? "Hoy" : "para calcular pesos"}</small>
          </div>
        </div>
        <div class="legend" id="radar-ticker-list">${distribution.length ? distribution.map(legendMarkup).join("") : `<span class="pill">Sin posiciones compradas</span>`}</div>
      </section>
      <div class="asset-list" id="portfolio-positions-body">
        ${appState.paperPositions.length ? appState.paperPositions.map((item) => assetRowMarkup(item, "paper", totalValue)).join("") : `<div class="empty-state">Sin compras simuladas.</div>`}
      </div>
      <div id="portfolio-watchlist-body" hidden></div>
      <span id="portfolio-data-state" hidden>Datos directos activos</span>
      <span id="portfolio-last-update" hidden>${escapeHtml(appState.lastUpdated)}</span>
      <span id="portfolio-total-stat" hidden>${escapeHtml(money(totalValue))}</span>
      <span id="portfolio-day-stat" hidden>${escapeHtml(signedMoney(totalDaily))}</span>
      <span id="radar-tracked-count" hidden>${appState.portfolio.length}</span>
      <span id="radar-last-update" hidden>${escapeHtml(appState.lastUpdated)}</span>
      <span id="radar-summary-note" hidden>Cartera App Mode V2</span>
      <span id="radar-investment-count" hidden>${appState.paperPositions.length}</span>
      <span id="radar-reference-count" hidden>${appState.portfolio.length}</span>
    </section>
  `;
  document.getElementById("portfolio-sim-buy-button").addEventListener("click", () => openPaperBuySheet(appState.selectedAsset || appState.tracking[0]?.ticker || ""));
}

function assetRowMarkup(item, mode, totalValue = 0) {
  const ticker = itemTicker(item);
  const tone = movementTone(item);
  const value = itemValue(item);
  const weight = mode === "paper" && totalValue > 0 && value !== null ? (value / totalValue) * 100 : null;
  const pnl = positionPnl(item);
  const updated = item.quote_timestamp || item.updated_at;
  return `
    <article class="asset-row" data-ticker="${escapeHtml(ticker)}" data-mode="${mode}">
      <button class="asset-main" type="button" data-open-asset="${escapeHtml(ticker)}">
        <span>
          <strong>${escapeHtml(ticker)}</strong>
          <small>${escapeHtml(item.name || item.display_name || ticker)} | ${escapeHtml(formatDate(updated))}</small>
        </span>
        <span class="price-stack">
          <strong>${escapeHtml(priceLabel(item))}</strong>
          <small class="${tone}">${escapeHtml(dailyMoveLabel(item))}</small>
        </span>
      </button>
      <div class="asset-meta">
        <span>${escapeHtml(previousCloseLabel(item))}</span>
        <span>${escapeHtml(extendedLabel(item))}</span>
        <span>${escapeHtml(marketSessionLabel(item))}</span>
        ${mode === "paper" ? `<span>${escapeHtml(itemUnits(item))} unidades</span><span>${escapeHtml(money(value))}</span><span>${escapeHtml(compactPercent(weight))}</span><span class="${movementTone(pnl)}">${escapeHtml(signedMoney(pnl, "P/L sin calcular"))}</span>` : ""}
      </div>
      <div class="row-actions">
        <button type="button" data-paper-buy="${escapeHtml(ticker)}">Carrito</button>
        ${mode === "paper"
          ? `<button class="danger" type="button" data-paper-close="${escapeHtml(ticker)}">-</button>`
          : `<button class="danger" type="button" data-watch-remove="${escapeHtml(ticker)}">-</button>`}
      </div>
    </article>
  `;
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
  const causal = snapshot.causal || {};
  const detection = snapshot.detection || {};
  const rows = extractWhaleRows(causal, detection);
  root.innerHTML = `
    <section class="screen-stack">
      <div class="screen-hero compact">
        <div>
          <h2>Ballenas</h2>
          <p>Smart money sin inventar instituciones.</p>
        </div>
        <span class="pill">${rows.length} alertas</span>
      </div>
      <form class="search-card" id="money-flow-jarvis-form">
        <input id="money-flow-jarvis-input" placeholder="Ej: que pasa con BNO segun ballenas">
        <button type="submit">Preguntar</button>
      </form>
      <div class="whale-answer" id="money-flow-jarvis-answer">Lectura Genesis lista para consultar.</div>
      <div class="asset-list whales-list" id="whales-list">
        ${rows.length ? rows.map(whaleRowMarkup).join("") : `<div class="empty-state">No hay ballena identificada con la fuente activa.</div>`}
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
      event: identified ? "Entidad detectada" : (item.flow_detected ? "Flujo detectado" : "No confirmado"),
      direction: item.direction || item.flow_direction || item.primary_label || "No confirmado",
      entity: whale.entity || item.whale_entity || "",
      amount: whale.movement_value || item.movement_value || item.amount_usd || "",
      date: item.money_flow_timestamp || item.timestamp || item.updated_at || "",
      confidence: item.confidence || item.confidence_label || "No concluyente",
      read: identified
        ? `Movimiento confirmado por ${whale.entity || item.whale_entity}.`
        : "No hay entidad institucional confirmada con la fuente activa.",
      missing: identified ? "Validar continuidad y contexto." : "Falta entidad, monto y causalidad confirmada.",
    });
  });
  return Array.from(byTicker.values()).slice(0, 12);
}

function whaleRowMarkup(row) {
  return `
    <article class="whale-row">
      <div>
        <strong>${escapeHtml(row.ticker)}</strong>
        <small>${escapeHtml(row.event)} | ${escapeHtml(row.direction)}</small>
      </div>
      <p>${escapeHtml(row.read)}</p>
      <div class="asset-meta">
        <span>Entidad: ${escapeHtml(row.entity || "Sin ballena identificada")}</span>
        <span>Monto: ${escapeHtml(row.amount || "No confirmado")}</span>
        <span>Fecha: ${escapeHtml(formatDate(row.date))}</span>
        <span>Confianza: ${escapeHtml(row.confidence)}</span>
      </div>
      <small>${escapeHtml(row.missing)}</small>
    </article>
  `;
}

function bindMoneyFlowJarvisForm() {
  const form = document.getElementById("money-flow-jarvis-form");
  if (!form || form.dataset.bound === "true") return;
  form.dataset.bound = "true";
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
      <div class="screen-hero compact">
        <div>
          <h2>Alertas</h2>
          <p>Senales importantes sin ruido tecnico.</p>
        </div>
        <span class="pill">${items.length} eventos</span>
      </div>
      <div class="asset-list">
        ${items.length ? items.slice(0, 12).map(alertMarkup).join("") : `<div class="empty-state">Sin alertas activas.</div>`}
      </div>
    </section>
  `;
}

function alertMarkup(item) {
  const ticker = itemTicker(item) || "Mercado";
  return `
    <article class="whale-row">
      <div>
        <strong>${escapeHtml(ticker)}</strong>
        <small>${escapeHtml(cleanCopy(item.title || item.event || item.status || "Alerta"))}</small>
      </div>
      <p>${escapeHtml(cleanCopy(item.summary || item.message || item.note || "Revisar antes de operar."))}</p>
    </article>
  `;
}

function openAssetSheet(ticker) {
  const normalized = normalizeTicker(ticker);
  const item = appState.portfolio.find((row) => itemTicker(row) === normalized);
  if (!item) return;
  appState.selectedAsset = normalized;
  const rowMode = itemUnits(item) !== null ? "Paper" : "Seguimiento";
  const sheet = document.getElementById("asset-sheet");
  const body = document.getElementById("asset-sheet-body");
  body.innerHTML = `
    <span class="app-kicker">${escapeHtml(rowMode)}</span>
    <h2>${escapeHtml(normalized)}</h2>
    <p class="asset-name">${escapeHtml(item.name || item.display_name || normalized)}</p>
    <div class="sheet-price ${movementTone(item)}">
      <strong>${escapeHtml(priceLabel(item))}</strong>
      <span>${escapeHtml(dailyMoveLabel(item))}</span>
    </div>
    <div class="sheet-grid">
      <span>${escapeHtml(previousCloseLabel(item))}</span>
      <span>${escapeHtml(extendedLabel(item))}</span>
      <span>${escapeHtml(marketSessionLabel(item))}</span>
      <span>Rango: ${escapeHtml(item.day_low ? `${money(item.day_low)} - ${money(item.day_high)}` : "Sin dato")}</span>
      <span>Volumen: ${escapeHtml(item.volume ? Number(item.volume).toLocaleString("en-US") : "Sin dato")}</span>
      <span>${escapeHtml(formatDate(item.quote_timestamp || item.updated_at))}</span>
    </div>
    <section class="genesis-mini">
      <strong>Genesis</strong>
      <p>Veredicto: Vigilar.</p>
      <p>Entrada condicional: esperar confirmacion de precio y volumen antes de operar.</p>
      <p>Invalidacion: si pierde soporte o no hay datos directos, bajar conviccion.</p>
      <p>Plan: usa paper para medir exposicion sin ejecutar compra real.</p>
    </section>
    <div class="sheet-actions">
      <button class="primary-button" type="button" data-paper-buy="${escapeHtml(normalized)}">Carrito</button>
      ${itemUnits(item) !== null
        ? `<button class="danger-button" type="button" data-paper-close="${escapeHtml(normalized)}">Cerrar paper</button>`
        : `<button class="danger-button" type="button" data-watch-remove="${escapeHtml(normalized)}">Quitar</button>`}
    </div>
  `;
  body.querySelector("[data-paper-buy]")?.addEventListener("click", (event) => {
    event.preventDefault();
    event.stopPropagation();
    openPaperBuySheet(normalized);
  });
  body.querySelector("[data-paper-close]")?.addEventListener("click", (event) => {
    event.preventDefault();
    event.stopPropagation();
    openClosePaperSheet(normalized);
  });
  body.querySelector("[data-watch-remove]")?.addEventListener("click", async (event) => {
    event.preventDefault();
    event.stopPropagation();
    try {
      await removeTickerFromWatchlist(normalized);
      closeAssetSheet();
    } catch (error) {
      toast(error.message, "bad");
    }
  });
  sheet.hidden = false;
}

function closeAssetSheet() {
  const sheet = document.getElementById("asset-sheet");
  if (sheet) sheet.hidden = true;
}

function openPaperBuySheet(ticker) {
  const normalized = normalizeTicker(ticker || appState.selectedAsset || appState.tracking[0]?.ticker || "");
  if (!normalized) {
    toast("Selecciona un activo primero.", "bad");
    return;
  }
  const item = appState.portfolio.find((row) => itemTicker(row) === normalized) || { ticker: normalized };
  const price = itemPrice(item);
  document.getElementById("paper-buy-ticker").value = normalized;
  document.getElementById("paper-buy-label").value = `${normalized} | ${item.name || item.display_name || normalized}`;
  document.getElementById("paper-buy-live-price").value = price === null ? "Sin precio" : money(price);
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
    toast("No encontre posicion paper para cerrar.", "bad");
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
  document.getElementById("paper-buy-total").textContent = `Total estimado: ${money(total)}`;
}

function bindGlobalEvents() {
  document.querySelectorAll(".nav-link").forEach((button) => {
    button.addEventListener("click", () => setActiveScreen(normalizeScreen(button.dataset.view)));
  });

  document.body.addEventListener("click", async (event) => {
    const openAsset = event.target.closest("[data-open-asset]");
    if (openAsset) {
      event.preventDefault();
      openAssetSheet(openAsset.dataset.openAsset);
      return;
    }
    const marketAdd = event.target.closest("[data-market-add]");
    if (marketAdd) {
      event.preventDefault();
      try { await addTickerToWatchlist(marketAdd.dataset.marketAdd); } catch (error) { toast(error.message, "bad"); }
      return;
    }
    const watchRemove = event.target.closest("[data-watch-remove]");
    if (watchRemove) {
      event.preventDefault();
      try { await removeTickerFromWatchlist(watchRemove.dataset.watchRemove); closeAssetSheet(); } catch (error) { toast(error.message, "bad"); }
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
    if (event.target.closest("[data-legacy-modal-cancel]")) document.getElementById("portfolio-action-modal").hidden = true;
  });

  document.getElementById("paper-buy-units").addEventListener("input", updatePaperTotal);
  document.getElementById("paper-buy-entry").addEventListener("input", updatePaperTotal);
  document.getElementById("paper-buy-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const ticker = document.getElementById("paper-buy-ticker").value;
    const units = numberOrNull(document.getElementById("paper-buy-units").value);
    const entry = numberOrNull(document.getElementById("paper-buy-entry").value);
    try {
      if (!units || !entry) throw new Error("Necesito unidades y precio de entrada.");
      await savePaperBuy(ticker, units, entry);
      closePaperBuySheet();
      closeAssetSheet();
      setActiveScreen("portfolio");
    } catch (error) {
      toast(error.message, "bad");
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
      toast(error.message, "bad");
    }
  });
}

function initGenesisAppV2() {
  bindGlobalEvents();
  render();
  refreshPortfolio().catch((error) => toast(error.message, "bad"));
  loadWhales().catch(() => {});
  setActiveScreen("genesis");
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden && (appState.activeScreen === "tracking" || appState.activeScreen === "portfolio")) {
      refreshPortfolio().catch(() => {});
    }
  });
}

document.addEventListener("DOMContentLoaded", initGenesisAppV2);
