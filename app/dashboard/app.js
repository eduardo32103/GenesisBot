const navLinks = Array.from(document.querySelectorAll(".nav-link"));
const views = Array.from(document.querySelectorAll(".view"));
const viewTitle = document.getElementById("view-title");

const titles = {
  "command-center": "Genesis",
  "radar": "Activos",
  "money-flow": "Dinero Grande",
  "alerts": "Alertas",
  "dependencies": "Datos",
  "macro": "Mundo",
  "activity": "Historial",
};

const genesisScopeLabels = {
  alerts: "Alertas",
  executive_queue: "Cola ejecutiva",
  general: "Panorama",
  money_flow: "Dinero Grande",
  radar: "Activos",
  reliability: "Confiabilidad",
  ticker: "Ticker activo",
};

const genesisTechnicalReplacements = [
  ["Failed to fetch", "no pude conectar con Genesis"],
  ["TypeError", "respuesta no disponible"],
  ["SyntaxError", "respuesta incompleta"],
  ["queue_source", "cola ejecutiva"],
  ["health_status", "salud del sistema"],
  ["radar_drilldown_decision_layer", "lectura del radar"],
  ["detection_ready_causality_disabled", "deteccion lista; causalidad no confirmada"],
  ["panel_context", "contexto del panel"],
  ["ticker_not_found", "ticker sin datos suficientes"],
  ["unavailable", "sin dato disponible"],
  ["available", "disponible"],
  ["unknown", "sin dato"],
  ["degraded", "datos parciales"],
  ["insufficient_confirmation", "no concluyente"],
  ["portfolio_fallback", "datos locales"],
];

const genesisRawErrorPatterns = [
  /\bHTTP\s+\d{3}\b/i,
  /\bTraceback\b/i,
  /\bException\b/i,
  /\bstack\b/i,
  /^\s*[\[{]/,
];

const loadedViews = new Set();
const radarDrilldownCache = new Map();
let radarSelectedTicker = "";
let radarDrilldownRequestId = 0;
const alertsDrilldownCache = new Map();
let alertsSelectedId = "";
let alertsDrilldownRequestId = 0;
let currentViewKey = "command-center";
let genesisContext = { scope: "general", ticker: "", label: "General" };
let genesisMessageId = 0;

function sanitizeShellCopy(value) {
  return String(value ?? "")
    .replace(/Faltan credenciales de Telegram en el entorno\./gi, "Datos del panel disponibles.")
    .replace(/Credenciales de Telegram incompletas\./gi, "Modo local.")
    .replace(/Dependencia legacy sin configurar\./gi, "Datos del panel disponibles.")
    .replace(/Canal legacy no configurado\./gi, "Modo local.")
    .replace(/Telegram/gi, "panel local")
    .replace(/legacy/gi, "local")
    .replace(/\bdegraded\b/gi, "Datos parciales");
}

function humanizeDashboardCopy(value) {
  return String(value ?? "")
    .replace(/Dependencias\s*\/\s*FMP/gi, "Fuentes")
    .replace(/\bFMP\b/gi, "datos de mercado")
    .replace(/\bMoney Flow\b/gi, "flujo")
    .replace(/\bTelegram\b/gi, "panel local")
    .replace(/\blegacy\b/gi, "local")
    .replace(/\bdegraded\b/gi, "Datos parciales")
    .replace(/\bunavailable\b/gi, "Sin datos disponibles")
    .replace(/\binsufficient_confirmation\b/gi, "No concluyente")
    .replace(/\bportfolio_fallback\b/gi, "Datos locales")
    .replace(/\bdependencies\b/gi, "fuentes")
    .replace(/\bunknown\b/gi, "Sin dato")
    .replace(/\bamount_usd\b/gi, "Capital")
    .replace(/\bentry_price\b/gi, "Precio de entrada")
    .replace(/\bcurrent_price\b/gi, "Precio actual")
    .replace(/\bcurrent_value\b/gi, "Valor actual")
    .replace(/\bpnl_usd\b/gi, "Ganancia / perdida")
    .replace(/\bpnl_pct\b/gi, "Rendimiento")
    .replace(/\bopened_at\b/gi, "Abierto desde")
    .replace(/\bquote_timestamp\b/gi, "Ultima cotizacion")
    .replace(/\balert_created_at\b/gi, "Alerta creada")
    .replace(/\balert_evaluated_at\b/gi, "Ultima revision")
    .replace(/\bcreated_at\b/gi, "Creado")
    .replace(/\bevaluated_at\b/gi, "Ultima revision")
    .replace(/\bsymbol\b/gi, "Ticker")
    .replace(/\bstatus\b/gi, "Estado")
    .replace(/\bsource\b/gi, "Fuente")
    .replace(/\bunits\b/gi, "Unidades")
    .replace(/\bruntime\b/gi, "sistema local")
    .replace(/\bendpoint\b/gi, "consulta")
    .replace(/\bcache hits\b/gi, "datos guardados")
    .replace(/\bcache hit\b/gi, "dato guardado")
    .replace(/\bcache\b/gi, "datos guardados")
    .replace(/\bcontingency\b/gi, "datos de respaldo")
    .replace(/\bcontingencia\b/gi, "datos de respaldo")
    .replace(/\bsnapshot\b/gi, "lectura guardada")
    .replace(/\bsnapshots\b/gi, "lecturas guardadas")
    .replace(/\bquote\b/gi, "Cotizacion")
    .replace(/\bnews\b/gi, "Noticias")
    .replace(/\bintraday\b/gi, "Intra dia")
    .replace(/\beod\b/gi, "Cierre diario")
    .replace(/\blive\b/gi, "datos directos")
    .replace(/\bfallback\b/gi, "datos locales")
    .replace(/\bquota\b/gi, "limite")
    .replace(/\baccess\b/gi, "acceso")
    .replace(/\bthrottle\b/gi, "pausa")
    .replace(/detection_ready_causality_disabled/gi, "Datos disponibles; causalidad no confirmada")
    .replace(/probable_causality_ready/gi, "Causalidad probable disponible")
    .replace(/_/g, " ");
}

function setText(id, value) {
  const node = document.getElementById(id);
  if (!node) return;
  node.textContent = humanizeDashboardCopy(value);
  node.classList.remove("is-loading");
}

function isLegacyOnlyShellNotice(value) {
  const normalized = String(value || "").toLowerCase();
  return [
    "faltan credenciales de telegram en el entorno.",
    "credenciales de telegram incompletas.",
    "dependencia legacy sin configurar.",
    "canal legacy no configurado.",
  ].includes(normalized.trim());
}

function isHealthyShellStatus(value) {
  const normalized = String(value || "").toLowerCase();
  return ["ok", "ready", "stable", "healthy", "online", "operativo"].some((token) => normalized.includes(token));
}

function formatSystemTopbarValue(system) {
  const status = system?.status || "";
  const summary = system?.summary || "";
  const cleanSummary = sanitizeShellCopy(summary).trim();
  if (isHealthyShellStatus(status) || isLegacyOnlyShellNotice(summary)) {
    return "Panel local | Operativo";
  }
  if (!cleanSummary || cleanSummary.toLowerCase() === "sin resumen") {
    return "Panel local | Datos del panel disponibles";
  }
  return "Panel local | Datos parciales";
}

function formatSystemStateToken(system) {
  const status = system?.status || "";
  const summary = system?.summary || "";
  const cleanSummary = sanitizeShellCopy(summary).trim();
  if (isHealthyShellStatus(status) || isLegacyOnlyShellNotice(summary)) {
    return "Operativo";
  }
  if (!cleanSummary || cleanSummary.toLowerCase() === "sin resumen") {
    return "Datos del panel disponibles";
  }
  return "Datos parciales";
}

function systemTopbarTone(system) {
  const status = system?.status || "";
  if (isHealthyShellStatus(status) || isLegacyOnlyShellNotice(system?.summary)) {
    return "ok";
  }
  return status || "degraded";
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatHeartbeatAge(seconds) {
  if (seconds === null || seconds === undefined || Number.isNaN(Number(seconds))) {
    return "Sin heartbeat confirmado";
  }
  return `${seconds}s desde el ultimo heartbeat`;
}

function formatIso(value) {
  if (!value) return "Sin timestamp";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return String(value);
  }
  return parsed.toLocaleString("es-MX", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function formatPrice(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return "Sin datos disponibles";
  }
  return `$${numeric.toFixed(2)}`;
}

function formatPercent(value) {
  if (value === null || value === undefined || value === "") {
    return "N/D";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "N/D";
  }
  return `${numeric.toFixed(1)}%`;
}

function formatSignedScore(value) {
  if (value === null || value === undefined || value === "") {
    return "N/D";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "N/D";
  }
  return `${numeric >= 0 ? "+" : ""}${numeric.toFixed(2)}`;
}

function setTokenValue(id, value, tone = "state-loading") {
  const node = document.getElementById(id);
  if (!node) return;
  node.textContent = humanizeDashboardCopy(value);
  node.classList.remove("is-loading", "state-token", "state-ok", "state-degraded", "state-quota", "state-access", "state-loading");
  node.classList.add("state-token", tone);
}

function readPanelText(id, fallback = "") {
  const node = document.getElementById(id);
  if (!node) return fallback;
  const value = String(node.textContent || "").replace(/\s+/g, " ").trim();
  if (!value || value === "..." || value === "Cargando...") return fallback;
  return value.slice(0, 120);
}

function createGenesisContext(scope, label, ticker = "", viewKey = currentViewKey) {
  return {
    scope,
    label,
    ticker,
    view: viewKey,
    viewLabel: titles[viewKey] || "Panel",
  };
}

function getGenesisScopeLabel(scope) {
  return genesisScopeLabels[scope] || "General";
}

function getGenesisDataLabel(scope) {
  return genesisScopeLabels[scope] || "panel";
}

function resolveGenesisContext(viewKey = currentViewKey) {
  if (viewKey === "radar" && radarSelectedTicker) {
    return createGenesisContext("ticker", "Ticker activo", radarSelectedTicker, viewKey);
  }
  if (viewKey === "radar") {
    return createGenesisContext("radar", "Radar", "", viewKey);
  }
  if (viewKey === "alerts") {
    return createGenesisContext("alerts", "Alertas", "", viewKey);
  }
  if (viewKey === "money-flow") {
    return createGenesisContext("money_flow", "Dinero Grande", radarSelectedTicker || "", viewKey);
  }
  return createGenesisContext("general", "General", radarSelectedTicker || "", viewKey);
}

function refineGenesisContextForQuestion(question, context) {
  const text = String(question || "").toLocaleLowerCase("es-MX");
  const normalized = text.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  if (normalized.includes("confiab") || normalized.includes("confianza")) {
    return createGenesisContext("reliability", "Confiabilidad", context.ticker || radarSelectedTicker || "", context.view || currentViewKey);
  }
  if (normalized.includes("flujo") || normalized.includes("capital") || normalized.includes("money flow")) {
    return createGenesisContext("money_flow", "Dinero Grande", context.ticker || radarSelectedTicker || "", context.view || currentViewKey);
  }
  if (normalized.includes("alerta") || normalized.includes("evento")) {
    return createGenesisContext("alerts", "Alertas", context.ticker || "", context.view || currentViewKey);
  }
  return context;
}

function buildGenesisPanelContext(context) {
  return {
    active_view: context.view || currentViewKey,
    scope: context.scope || "general",
    label: context.label || "General",
    ticker: context.ticker || radarSelectedTicker || "",
    radar: {
      selected_ticker: radarSelectedTicker || "",
      tracked: readPanelText("radar-tracked-count", readPanelText("metric-radar-size")),
      summary: readPanelText("radar-summary-note"),
    },
    alerts: {
      selected_id: alertsSelectedId || "",
      total_recent: readPanelText("alerts-total-recent"),
      summary: readPanelText("alerts-summary-note"),
    },
    money_flow: {
      detected: readPanelText("money-flow-detected-count"),
      non_conclusive: readPanelText("money-flow-non-conclusive-count"),
      summary: readPanelText("money-flow-summary-note"),
    },
    reliability: {
      level: readPanelText("reliability-level"),
      decision: readPanelText("reliability-decision"),
    },
    executive_queue: {
      total: readPanelText("executive-queue-total"),
      review_now: readPanelText("executive-queue-review-count"),
      reliability: readPanelText("executive-queue-reliability"),
    },
  };
}

function updateGenesisContext(context = genesisContext) {
  genesisContext = context;
  setText("genesis-context-chip", `Contexto: ${context.label || "General"}`);
  setText("genesis-view-chip", `Vista: ${context.viewLabel || titles[context.view] || "Panel"}`);
  setText("genesis-ticker-chip", `Ticker: ${context.ticker || "ninguno"}`);
  setText("genesis-data-chip", `Datos: ${getGenesisDataLabel(context.scope === "general" ? "general" : context.scope)}`);
}

function updateGenesisContextFromPayload(payload) {
  const payloadContext = payload.context || {};
  if (!payloadContext.scope && !payloadContext.ticker) return;
  const scope = payloadContext.scope || genesisContext.scope || "general";
  updateGenesisContext(
    createGenesisContext(
      scope,
      getGenesisScopeLabel(scope),
      payloadContext.ticker || genesisContext.ticker || "",
      payloadContext.active_view || genesisContext.view || currentViewKey
    )
  );
}

function appendGenesisMessage(role, text, meta = "", options = {}) {
  const thread = document.getElementById("genesis-thread");
  if (!thread) return "";
  const id = `genesis-message-${++genesisMessageId}`;
  const node = document.createElement("div");
  node.id = id;
  node.className = `genesis-message genesis-message-${role}${options.loading ? " genesis-message-loading" : ""}`;
  node.innerHTML = `
    <strong>${role === "user" ? "Tu" : "Genesis"}</strong>
    <p>${escapeHtml(text)}</p>
    ${meta ? `<small>${escapeHtml(meta)}</small>` : ""}
  `;
  thread.appendChild(node);
  thread.scrollTop = thread.scrollHeight;
  return id;
}

function humanizeGenesisCopy(value, fallback = "") {
  let text = String(value ?? "").replace(/\s+/g, " ").trim();
  if (!text) return fallback;
  if (genesisRawErrorPatterns.some((pattern) => pattern.test(text))) {
    return fallback;
  }
  genesisTechnicalReplacements.forEach(([raw, replacement]) => {
    text = text.replaceAll(raw, replacement);
  });
  text = text.replaceAll("_", " ").trim();
  return text || fallback;
}

function getGenesisFallbackCopy(reason = "snapshots") {
  const normalized = String(reason || "").toLowerCase();
  if (normalized.includes("connection")) {
    return {
      answer: "No pude conectar con Genesis ahora. Puedo darte una lectura general, pero no confirmar datos del panel.",
      summary: "Genesis no pudo confirmar conexion con la consulta local.",
      executive: "Lectura no concluyente; solo sirve como orientacion con el contexto visible.",
      risk: "La respuesta del panel no esta confirmada en este momento.",
      next: "Reintentar cuando el panel confirme conexion o revisar solo datos visibles.",
    };
  }
  if (normalized.includes("incomplete")) {
    return {
      answer: "Genesis recibio una respuesta incompleta. Puedo orientar con cautela, pero no confirmar la lectura del panel.",
      summary: "La respuesta del backend no trajo todos los bloques necesarios.",
      executive: "Lectura no concluyente hasta recuperar una respuesta completa.",
      risk: "Faltan campos para sostener una decision confiable.",
      next: "Reintentar la consulta o revisar el ticker desde Radar.",
    };
  }
  if (normalized.includes("degraded")) {
    return {
      answer: "La fuente esta degradada. Puedo orientar con el contexto disponible, pero no elevar la lectura a confiable.",
      summary: "La fuente disponible esta degradada.",
      executive: "Lectura util solo como contexto, no como decision fuerte.",
      risk: "La evidencia no alcanza para confirmar datos del panel.",
      next: "Esperar confirmacion de lecturas guardadas antes de actuar.",
    };
  }
  return {
    answer: "No pude leer las lecturas guardadas activas. Puedo darte una lectura general, pero no confirmar datos del panel ahora.",
    summary: "Lecturas guardadas activas no confirmadas.",
    executive: "Lectura no concluyente; Genesis conserva el contexto visible sin inventar datos.",
    risk: "Falta evidencia suficiente del panel.",
    next: "Reintentar cuando el panel confirme lecturas guardadas o revisar solo lo visible.",
  };
}

function createGenesisFallbackBlocks(reason = "snapshots") {
  const copy = getGenesisFallbackCopy(reason);
  return {
    summary: copy.summary,
    executive_read: copy.executive,
    main_signals: ["Contexto visible del panel conservado.", "Sin datos nuevos confirmados."],
    risks: [copy.risk],
    reliability: "no concluyente",
    next_step: copy.next,
  };
}

function createGenesisFallbackPayload(reason = "snapshots", context = genesisContext) {
  const copy = getGenesisFallbackCopy(reason);
  return {
    fallback: true,
    answer: copy.answer,
    honesty_note: "Respuesta local y conservadora. No confirma datos no disponibles.",
    context: {
      scope: context.scope || "general",
      ticker: context.ticker || "",
      active_view: context.view || currentViewKey,
      label: context.label || "General",
    },
    blocks: createGenesisFallbackBlocks(reason),
  };
}

function normalizeGenesisList(values, fallback) {
  const items = Array.isArray(values) ? values : [];
  const cleanItems = items
    .map((item) => humanizeGenesisCopy(item, ""))
    .filter(Boolean)
    .slice(0, 4);
  return cleanItems.length ? cleanItems : [fallback];
}

function normalizeGenesisBlocks(blocks, reason = "payload_incomplete") {
  const fallback = createGenesisFallbackBlocks(reason);
  if (!blocks || typeof blocks !== "object") return fallback;
  return {
    summary: humanizeGenesisCopy(blocks.summary, fallback.summary),
    executive_read: humanizeGenesisCopy(blocks.executive_read, fallback.executive_read),
    main_signals: normalizeGenesisList(blocks.main_signals, fallback.main_signals[0]),
    risks: normalizeGenesisList(blocks.risks, fallback.risks[0]),
    reliability: humanizeGenesisCopy(blocks.reliability, fallback.reliability) || "no concluyente",
    next_step: humanizeGenesisCopy(blocks.next_step, fallback.next_step),
  };
}

function normalizeGenesisPayload(payload, reason = "payload_incomplete", context = genesisContext) {
  const fallback = createGenesisFallbackPayload(reason, context);
  if (!payload || typeof payload !== "object") return fallback;
  const blocks = normalizeGenesisBlocks(payload.blocks, reason);
  const answer = humanizeGenesisCopy(payload.answer, fallback.answer);
  const honestyNote = humanizeGenesisCopy(payload.honesty_note, fallback.honesty_note);
  return {
    ...payload,
    answer: answer || fallback.answer,
    honesty_note: honestyNote || fallback.honesty_note,
    context: payload.context && typeof payload.context === "object" ? payload.context : fallback.context,
    blocks,
  };
}

function renderGenesisBlocks(blocks) {
  blocks = normalizeGenesisBlocks(blocks);
  const signals = Array.isArray(blocks.main_signals) ? blocks.main_signals : [];
  const risks = Array.isArray(blocks.risks) ? blocks.risks : [];
  const renderList = (items, fallback) => {
    const values = items.length ? items : [fallback];
    return `<ul>${values.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>`;
  };
  return `
    <div class="genesis-blocks">
      <section>
        <span>Resumen</span>
        <p>${escapeHtml(blocks.summary || "Lectura no concluyente.")}</p>
      </section>
      <section>
        <span>Lectura ejecutiva</span>
        <p>${escapeHtml(blocks.executive_read || "Sin lectura ejecutiva suficiente.")}</p>
      </section>
      <section>
        <span>Senales principales</span>
        ${renderList(signals, "Sin senal suficiente en lecturas guardadas actuales.")}
      </section>
      <section>
        <span>Riesgos / frenos</span>
        ${renderList(risks, "Sin freno dominante visible.")}
      </section>
      <section>
        <span>Confiabilidad</span>
        <p>${escapeHtml(blocks.reliability || "no concluyente")}</p>
      </section>
      <section>
        <span>Siguiente paso</span>
        <p>${escapeHtml(blocks.next_step || "Esperar confirmacion.")}</p>
      </section>
    </div>
  `;
}

function updateGenesisMessage(id, text, meta = "", blocks = null, options = {}) {
  const node = document.getElementById(id);
  if (!node) return;
  node.classList.remove("genesis-message-loading");
  node.classList.toggle("genesis-message-fallback", Boolean(options.fallback));
  node.innerHTML = `
    <strong>Genesis</strong>
    <p>${escapeHtml(text)}</p>
    ${renderGenesisBlocks(blocks)}
    ${meta ? `<small>${escapeHtml(meta)}</small>` : ""}
  `;
  const thread = document.getElementById("genesis-thread");
  if (thread) {
    thread.scrollTop = thread.scrollHeight;
  }
}

function renderGenesisAnswer(payload, messageId) {
  payload = normalizeGenesisPayload(payload, "payload_incomplete", genesisContext);
  updateGenesisContextFromPayload(payload);
  updateGenesisMessage(
    messageId,
    payload.answer || "No hay respuesta disponible. Lectura no concluyente.",
    payload.honesty_note || "Respuesta conservadora.",
    payload.blocks,
    { fallback: Boolean(payload.fallback) || payload.blocks?.reliability === "no concluyente" }
  );
}

function renderGenesisError(message, messageId, context = genesisContext) {
  renderGenesisAnswer(createGenesisFallbackPayload("connection_error", context), messageId);
}

async function loadGenesisAnswer(question, context = genesisContext) {
  const query = String(question || "").trim() || "que esta pasando";
  context = refineGenesisContextForQuestion(query, context);
  const panelContext = buildGenesisPanelContext(context);
  updateGenesisContext(context);
  appendGenesisMessage("user", query, `Contexto: ${context.label || "General"}${context.ticker ? ` | ${context.ticker}` : ""}`);
  const loadingId = appendGenesisMessage("assistant", "Leyendo lecturas guardadas del dashboard...", "Sin LLM externo ni consultas nuevas.", { loading: true });
  try {
    const params = new URLSearchParams({
      q: query,
      context: context.scope || "general",
      ticker: context.ticker || "",
      panel_context: JSON.stringify(panelContext),
    });
    const response = await fetch(`/api/dashboard/genesis?${params.toString()}`, { cache: "no-store" });
    if (!response.ok) {
      renderGenesisAnswer(createGenesisFallbackPayload("endpoint_error", context), loadingId);
      return;
    }
    let payload = null;
    try {
      payload = await response.json();
    } catch (error) {
      renderGenesisAnswer(createGenesisFallbackPayload("payload_incomplete", context), loadingId);
      return;
    }
    renderGenesisAnswer(normalizeGenesisPayload(payload, "payload_incomplete", context), loadingId);
  } catch (error) {
    renderGenesisError(error.message, loadingId, context);
  }
}

function bindGenesisQueryForm() {
  const form = document.getElementById("genesis-query-form");
  const input = document.getElementById("genesis-query-input");
  if (!form || !input || form.dataset.bound === "true") return;
  form.dataset.bound = "true";
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    const context = resolveGenesisContext(currentViewKey);
    activateView("command-center");
    loadGenesisAnswer(input.value, context);
    input.value = "";
  });
}

function bindGenesisChatForm() {
  const form = document.getElementById("genesis-chat-form");
  const input = document.getElementById("genesis-chat-input");
  if (!form || !input || form.dataset.bound === "true") return;
  form.dataset.bound = "true";
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    loadGenesisAnswer(input.value, genesisContext);
    input.value = "";
  });
}

function formatDetailMoney(value) {
  if (value === null || value === undefined || value === "") {
    return "Sin dato";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "Sin dato";
  }
  return `$${numeric.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function formatDetailPrice(value) {
  if (value === null || value === undefined || value === "") {
    return "Sin dato";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "Sin dato";
  }
  return `$${numeric.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 4 })}`;
}

function formatDetailUnits(value) {
  if (value === null || value === undefined || value === "") {
    return "Sin dato";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "Sin dato";
  }
  return numeric.toFixed(8).replace(/0+$/, "").replace(/\.$/, "");
}

function formatDetailText(value) {
  const raw = String(value ?? "").trim();
  return raw ? humanizeDashboardCopy(raw) : "Sin dato";
}

function formatDetailTimestamp(value) {
  const raw = String(value ?? "").trim();
  if (!raw) {
    return "Sin dato";
  }
  const formatted = formatIso(raw);
  return formatted || "Sin dato";
}

function getDrilldownStatusMeta(status) {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === "gain") {
    return { label: "En ganancia", tone: "state-ok" };
  }
  if (normalized === "loss") {
    return { label: "En perdida", tone: "state-degraded" };
  }
  if (normalized === "flat") {
    return { label: "En equilibrio", tone: "state-loading" };
  }
  if (normalized === "unpriced") {
    return { label: "Sin precio live", tone: "state-access" };
  }
  if (normalized === "watchlist") {
    return { label: "En vigilancia", tone: "state-loading" };
  }
  return { label: "Sin dato", tone: "state-loading" };
}

function getExecutivePriorityTone(priority) {
  const normalized = String(priority || "").trim().toLowerCase();
  if (normalized === "alta") return "state-degraded";
  if (normalized === "media") return "state-access";
  if (normalized === "baja") return "state-ok";
  return "state-loading";
}

function setDrilldownMetric(id, value, formatter = formatDetailText) {
  const node = document.getElementById(id);
  if (!node) return;
  node.textContent = formatter(value);
}

function renderDrilldownChipList(id, values, emptyText = "Sin dato") {
  const node = document.getElementById(id);
  if (!node) return;
  const items = Array.isArray(values) ? values.filter((value) => String(value ?? "").trim()) : [];
  if (!items.length) {
    node.innerHTML = `<span class="chip chip-muted">${escapeHtml(humanizeDashboardCopy(emptyText))}</span>`;
    return;
  }
  node.innerHTML = items.map((value) => `<span class="chip chip-muted">${escapeHtml(humanizeDashboardCopy(value))}</span>`).join("");
}

function renderRelatedAlertsList(alerts) {
  const container = document.getElementById("drilldown-related-alert-list");
  if (!container) {
    return;
  }

  const items = Array.isArray(alerts) ? alerts : [];
  if (!items.length) {
    container.innerHTML = `
      <div class="detail-activity-item">
        <strong>Sin alertas asociadas</strong>
        <small>Este activo no tiene alertas recientes dentro de la ventana actual.</small>
      </div>
    `;
    return;
  }

  container.innerHTML = items
    .map((alert) => {
      const title = alert.summary || alert.title || "Sin resumen adicional.";
      const score = formatAlertScore(alert.score);
      const result = formatDetailText(alert.result);
      const validation = formatDetailText(alert.validation);
      const timing = alert.evaluated_at ? `Evaluada ${formatIso(alert.evaluated_at)}` : `Creada ${formatIso(alert.created_at)}`;
      return `
        <div class="detail-activity-item">
          <strong>${escapeHtml(humanizeDashboardCopy(alert.alert_type_label || "Alerta"))} | ${escapeHtml(humanizeDashboardCopy(alert.status_label || "Sin estado"))}</strong>
          <small>${escapeHtml(validation)} | ${escapeHtml(result)} | score ${escapeHtml(score)} | ${escapeHtml(timing)}</small>
          <p>${escapeHtml(humanizeDashboardCopy(title))}</p>
        </div>
      `;
    })
    .join("");
}

function updateRadarDrilldownSelection(ticker) {
  const normalized = String(ticker || "").trim().toUpperCase();
  document.querySelectorAll("[data-drilldown-ticker]").forEach((node) => {
    const nodeTicker = String(node.dataset.drilldownTicker || "").trim().toUpperCase();
    node.classList.toggle("is-selected", Boolean(normalized) && nodeTicker === normalized);
  });
}

function renderRadarDrilldownEmpty(message, ticker = "") {
  const symbol = String(ticker || "").trim().toUpperCase();
  const status = getDrilldownStatusMeta("");
  setText("drilldown-symbol", symbol || "Sin seleccionar");
  setTokenValue("drilldown-status", status.label, status.tone);
  setText("radar-drilldown-note", message);
  setDrilldownMetric("drilldown-amount-usd", null, formatDetailMoney);
  setDrilldownMetric("drilldown-entry-price", null, formatDetailPrice);
  setDrilldownMetric("drilldown-units", null, formatDetailUnits);
  setDrilldownMetric("drilldown-current-price", null, formatDetailPrice);
  setDrilldownMetric("drilldown-current-value", null, formatDetailMoney);
  setDrilldownMetric("drilldown-pnl-usd", null, formatDetailMoney);
  setDrilldownMetric("drilldown-pnl-pct", null, () => "Sin dato");
  setDrilldownMetric("drilldown-symbol-detail", symbol || null);
  setDrilldownMetric("drilldown-status-detail", null);
  setDrilldownMetric("drilldown-source", null);
  setDrilldownMetric("drilldown-opened-at", null);
  setDrilldownMetric("drilldown-quote-timestamp", null);
  setDrilldownMetric("drilldown-last-alert-created-at", null);
  setDrilldownMetric("drilldown-last-alert-evaluated-at", null);
  setDrilldownMetric("drilldown-related-alert-count", null);
  setDrilldownMetric("drilldown-alert-state-summary", null);
  setTokenValue("drilldown-exec-priority", "Sin dato", "state-loading");
  setDrilldownMetric("drilldown-exec-decision", null);
  setDrilldownMetric("drilldown-exec-reason", null);
  setDrilldownMetric("drilldown-exec-signal", null);
  setDrilldownMetric("drilldown-exec-risk", null);
  setDrilldownMetric("drilldown-exec-reliability", null);
  setDrilldownMetric("drilldown-exec-timestamp", null);
  setDrilldownMetric("drilldown-context-note", null);
  setDrilldownMetric("drilldown-reliability-note", null);
  setDrilldownMetric("drilldown-exec-note", null);
  setDrilldownMetric("drilldown-explain-factor", null);
  setDrilldownMetric("drilldown-explain-decision", null);
  renderDrilldownChipList("drilldown-explain-supporting", [], "Sin senales de apoyo");
  renderDrilldownChipList("drilldown-explain-blocking", [], "Sin frenos visibles");
  renderDrilldownChipList("drilldown-explain-upgrade", [], "Sin requisitos visibles");
  renderRelatedAlertsList([]);
}

function renderRadarDrilldownLoading(ticker) {
  const normalized = String(ticker || "").trim().toUpperCase();
  setText("drilldown-symbol", normalized || "Cargando...");
  setTokenValue("drilldown-status", "Cargando", "state-loading");
  setText("radar-drilldown-note", `Abriendo ficha tactica de ${normalized || "ticker"}...`);
  setDrilldownMetric("drilldown-amount-usd", null, formatDetailMoney);
  setDrilldownMetric("drilldown-entry-price", null, formatDetailPrice);
  setDrilldownMetric("drilldown-units", null, formatDetailUnits);
  setDrilldownMetric("drilldown-current-price", null, formatDetailPrice);
  setDrilldownMetric("drilldown-current-value", null, formatDetailMoney);
  setDrilldownMetric("drilldown-pnl-usd", null, formatDetailMoney);
  setDrilldownMetric("drilldown-pnl-pct", null, () => "Sin dato");
  setDrilldownMetric("drilldown-symbol-detail", normalized || null);
  setDrilldownMetric("drilldown-status-detail", "Cargando");
  setDrilldownMetric("drilldown-source", "Cargando");
  setDrilldownMetric("drilldown-opened-at", null);
  setDrilldownMetric("drilldown-quote-timestamp", null);
  setDrilldownMetric("drilldown-last-alert-created-at", null);
  setDrilldownMetric("drilldown-last-alert-evaluated-at", null);
  setDrilldownMetric("drilldown-related-alert-count", null);
  setDrilldownMetric("drilldown-alert-state-summary", "Cargando");
  setTokenValue("drilldown-exec-priority", "Cargando", "state-loading");
  setDrilldownMetric("drilldown-exec-decision", "Cargando");
  setDrilldownMetric("drilldown-exec-reason", "Cargando");
  setDrilldownMetric("drilldown-exec-signal", "Cargando");
  setDrilldownMetric("drilldown-exec-risk", "Cargando");
  setDrilldownMetric("drilldown-exec-reliability", "Cargando");
  setDrilldownMetric("drilldown-exec-timestamp", null);
  setDrilldownMetric("drilldown-context-note", "Cargando");
  setDrilldownMetric("drilldown-reliability-note", "Cargando");
  setDrilldownMetric("drilldown-exec-note", "Cargando");
  setDrilldownMetric("drilldown-explain-factor", "Cargando");
  setDrilldownMetric("drilldown-explain-decision", "Cargando");
  renderDrilldownChipList("drilldown-explain-supporting", [], "Cargando apoyos");
  renderDrilldownChipList("drilldown-explain-blocking", [], "Cargando frenos");
  renderDrilldownChipList("drilldown-explain-upgrade", [], "Cargando requisitos");
  renderRelatedAlertsList([]);
}

function buildRadarDrilldownNote(detail) {
  if (!detail.found) {
    return "No encontre un registro operativo para este ticker dentro del radar actual.";
  }
  if (detail.related_alerts_count > 0 && detail.current_price !== null && detail.current_price !== undefined) {
    return "La ficha combina cartera, precio actual y alertas relacionadas para decidir rapido.";
  }
  if (detail.related_alerts_count > 0) {
    return "La ficha combina radar y alertas relacionadas persistidas, aunque no todo el bloque financiero este completo.";
  }
  if (detail.is_investment && detail.current_price !== null && detail.current_price !== undefined) {
    return "Posicion abierta con precio actual disponible. La ficha conserva el capital desplegado y su trazabilidad.";
  }
  if (detail.is_investment) {
    return "Posicion abierta detectada. El capital esta confirmado, pero la cotizacion no estuvo disponible en este corte.";
  }
  if (detail.current_price !== null && detail.current_price !== undefined) {
    return "Ticker en vigilancia. Se muestra el precio disponible y se dejan en limpio los huecos del resto del activo.";
  }
  return "Ticker en vigilancia. La ficha unifica radar y alertas, dejando en limpio cualquier dato que no este disponible.";
}

function renderRadarDrilldown(detail) {
  if (!detail || !detail.found) {
    renderRadarDrilldownEmpty("No encontre datos reales para el ticker seleccionado dentro del radar activo.", detail?.symbol || detail?.ticker || "");
    return;
  }

  const symbol = formatDetailText(detail.symbol || detail.ticker);
  const status = getDrilldownStatusMeta(detail.status);
  setText("drilldown-symbol", symbol);
  setTokenValue("drilldown-status", status.label, status.tone);
  setText("radar-drilldown-note", buildRadarDrilldownNote(detail));
  setDrilldownMetric("drilldown-amount-usd", detail.amount_usd, formatDetailMoney);
  setDrilldownMetric("drilldown-entry-price", detail.entry_price, formatDetailPrice);
  setDrilldownMetric("drilldown-units", detail.units, formatDetailUnits);
  setDrilldownMetric("drilldown-current-price", detail.current_price, formatDetailPrice);
  setDrilldownMetric("drilldown-current-value", detail.current_value, formatDetailMoney);
  setDrilldownMetric("drilldown-pnl-usd", detail.pnl_usd, formatDetailMoney);
  setDrilldownMetric("drilldown-pnl-pct", detail.pnl_pct, (value) => {
    if (value === null || value === undefined || value === "") {
      return "Sin dato";
    }
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "Sin dato";
    }
    const sign = numeric > 0 ? "+" : "";
    return `${sign}${numeric.toFixed(2)}%`;
  });
  setDrilldownMetric("drilldown-symbol-detail", detail.symbol || detail.ticker);
  setDrilldownMetric("drilldown-status-detail", status.label);
  setDrilldownMetric("drilldown-source", detail.source_label || detail.source);
  setDrilldownMetric("drilldown-opened-at", detail.opened_at, formatDetailTimestamp);
  setDrilldownMetric("drilldown-quote-timestamp", detail.quote_timestamp, formatDetailTimestamp);
  setDrilldownMetric("drilldown-last-alert-created-at", detail.latest_alert_created_at, formatDetailTimestamp);
  setDrilldownMetric("drilldown-last-alert-evaluated-at", detail.latest_alert_evaluated_at, formatDetailTimestamp);
  setDrilldownMetric("drilldown-related-alert-count", detail.related_alerts_count, (value) => {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? `${numeric}` : "Sin dato";
  });
  setDrilldownMetric("drilldown-alert-state-summary", detail.alert_state_summary);
  setTokenValue("drilldown-exec-priority", formatDetailText(detail.priority), getExecutivePriorityTone(detail.priority));
  setDrilldownMetric("drilldown-exec-decision", detail.decision);
  setDrilldownMetric("drilldown-exec-reason", detail.main_reason);
  setDrilldownMetric("drilldown-exec-signal", detail.dominant_signal);
  setDrilldownMetric("drilldown-exec-risk", detail.main_risk);
  setDrilldownMetric("drilldown-exec-reliability", detail.current_reliability);
  setDrilldownMetric("drilldown-exec-timestamp", detail.decision_timestamp, formatDetailTimestamp);
  setDrilldownMetric("drilldown-context-note", detail.context_note);
  setDrilldownMetric("drilldown-reliability-note", detail.reliability_note);
  setDrilldownMetric("drilldown-exec-note", detail.executive_note);
  setDrilldownMetric("drilldown-explain-factor", detail.dominant_factor);
  setDrilldownMetric("drilldown-explain-decision", detail.decision_explanation);
  renderDrilldownChipList("drilldown-explain-supporting", detail.supporting_signals, "Sin senales de apoyo");
  renderDrilldownChipList("drilldown-explain-blocking", detail.blocking_signals, "Sin frenos visibles");
  renderDrilldownChipList("drilldown-explain-upgrade", detail.upgrade_requirements, "Sin requisitos visibles");
  renderRelatedAlertsList(detail.related_alerts);
}

async function loadRadarDrilldown(ticker, forceRefresh = false) {
  const normalized = String(ticker || "").trim().toUpperCase();
  if (!normalized) {
    return;
  }

  radarSelectedTicker = normalized;
  updateRadarDrilldownSelection(normalized);
  renderRadarDrilldownLoading(normalized);

  if (!forceRefresh && radarDrilldownCache.has(normalized)) {
    renderRadarDrilldown(radarDrilldownCache.get(normalized));
    return;
  }

  const requestId = ++radarDrilldownRequestId;
  try {
    const response = await fetch(`/api/dashboard/radar/drilldown?ticker=${encodeURIComponent(normalized)}`, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const payload = await response.json();
    radarDrilldownCache.set(normalized, payload);
    if (requestId !== radarDrilldownRequestId) {
      return;
    }
    renderRadarDrilldown(payload);
  } catch (error) {
    if (requestId !== radarDrilldownRequestId) {
      return;
    }
    renderRadarDrilldownEmpty(`No pude cargar la ficha tactica de ${normalized} (${error.message}).`, normalized);
  }
}

function bindRadarDrilldownTargets() {
  document.querySelectorAll(".chip-action[data-drilldown-ticker]").forEach((node) => {
    if (node.dataset.drilldownBound === "true") {
      return;
    }
    node.dataset.drilldownBound = "true";
    node.addEventListener("click", () => {
      loadRadarDrilldown(node.dataset.drilldownTicker);
    });
  });

  document.querySelectorAll(".table-row-action[data-drilldown-ticker]").forEach((node) => {
    if (node.dataset.drilldownBound === "true") {
      return;
    }
    node.dataset.drilldownBound = "true";
    node.addEventListener("click", () => {
      loadRadarDrilldown(node.dataset.drilldownTicker);
    });
    node.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") {
        return;
      }
      event.preventDefault();
      loadRadarDrilldown(node.dataset.drilldownTicker);
    });
  });
}

function formatAlertScore(value) {
  if (value === null || value === undefined || value === "") {
    return "Sin dato";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "Sin dato";
  }
  return `${numeric >= 0 ? "+" : ""}${numeric.toFixed(2)}`;
}

function getAlertDrilldownStatusMeta(detail) {
  const validation = String(detail?.validation || "").toLowerCase();
  const result = String(detail?.result || "").toLowerCase();
  const status = String(detail?.status || "").toLowerCase();
  const label = formatDetailText(detail?.status_label);

  if (result.includes("ganadora")) {
    return { label, tone: "state-ok" };
  }
  if (result.includes("fallida")) {
    return { label, tone: "state-degraded" };
  }
  if (validation.includes("pendiente") || status === "tracking") {
    return { label, tone: "state-loading" };
  }
  return { label, tone: "state-access" };
}

function updateAlertsDrilldownSelection(alertId) {
  const normalized = String(alertId || "").trim();
  document.querySelectorAll("[data-alert-id]").forEach((node) => {
    const nodeAlertId = String(node.dataset.alertId || "").trim();
    node.classList.toggle("is-selected", Boolean(normalized) && nodeAlertId === normalized);
  });
}

function renderAlertsDrilldownEmpty(message, alertKey = "") {
  setText("alert-drilldown-key", alertKey || "Sin seleccionar");
  setTokenValue("alert-drilldown-status", "Pendiente", "state-loading");
  setText("alert-drilldown-note", message);
  setDrilldownMetric("alert-detail-ticker", null);
  setDrilldownMetric("alert-detail-type", null);
  setDrilldownMetric("alert-detail-title", null);
  setDrilldownMetric("alert-detail-summary", null);
  setDrilldownMetric("alert-detail-horizon", null);
  setDrilldownMetric("alert-detail-status", null);
  setDrilldownMetric("alert-detail-score", null, formatAlertScore);
  setDrilldownMetric("alert-detail-validation", null);
  setDrilldownMetric("alert-detail-result", null);
  setDrilldownMetric("alert-detail-created-at", null, formatDetailTimestamp);
  setDrilldownMetric("alert-detail-evaluated-at", null, formatDetailTimestamp);
  setDrilldownMetric("alert-detail-context", null);
  setDrilldownMetric("alert-detail-reliability", null);
}

function renderAlertsDrilldownLoading(alertKey = "") {
  setText("alert-drilldown-key", alertKey || "Cargando...");
  setTokenValue("alert-drilldown-status", "Cargando", "state-loading");
  setText("alert-drilldown-note", `Abriendo detalle de ${alertKey || "alerta"}...`);
  setDrilldownMetric("alert-detail-ticker", null);
  setDrilldownMetric("alert-detail-type", null);
  setDrilldownMetric("alert-detail-title", null);
  setDrilldownMetric("alert-detail-summary", null);
  setDrilldownMetric("alert-detail-horizon", null);
  setDrilldownMetric("alert-detail-status", "Cargando");
  setDrilldownMetric("alert-detail-score", null, formatAlertScore);
  setDrilldownMetric("alert-detail-validation", null);
  setDrilldownMetric("alert-detail-result", null);
  setDrilldownMetric("alert-detail-created-at", null, formatDetailTimestamp);
  setDrilldownMetric("alert-detail-evaluated-at", null, formatDetailTimestamp);
  setDrilldownMetric("alert-detail-context", null);
  setDrilldownMetric("alert-detail-reliability", null);
}

function buildAlertDrilldownKey(detail) {
  const ticker = formatDetailText(detail?.ticker);
  const type = formatDetailText(detail?.alert_type_label);
  if (ticker === "Sin dato" && type === "Sin dato") {
    return "Sin seleccionar";
  }
  return `${ticker} | ${type}`;
}

function buildAlertListKey(alert) {
  const ticker = formatDetailText(alert?.ticker);
  const type = formatDetailText(alert?.alert_type_label);
  if (ticker === "Sin dato" && type === "Sin dato") {
    return "Alerta";
  }
  return `${ticker} | ${type}`;
}

function buildAlertDrilldownNote(detail) {
  if (!detail?.found) {
    return "No encontre un detalle operativo para la alerta seleccionada dentro del dashboard activo.";
  }
  if (String(detail.validation || "").toLowerCase().includes("validada")) {
    return "La ficha muestra la ultima validacion persistida, sin recalcular el motor.";
  }
  if (String(detail.status || "").toLowerCase() === "tracking") {
    return "La alerta sigue en seguimiento. El panel conserva el contexto actual y deja en limpio lo que aun no existe.";
  }
  return "La ficha reutiliza solo evidencia guardada del sistema local para una lectura ejecutiva breve.";
}

function renderAlertsDrilldown(detail) {
  if (!detail || !detail.found) {
    renderAlertsDrilldownEmpty(detail?.reliability_note || "No encontre datos reales para la alerta seleccionada.");
    return;
  }

  const status = getAlertDrilldownStatusMeta(detail);
  setText("alert-drilldown-key", buildAlertDrilldownKey(detail));
  setTokenValue("alert-drilldown-status", status.label, status.tone);
  setText("alert-drilldown-note", buildAlertDrilldownNote(detail));
  setDrilldownMetric("alert-detail-ticker", detail.ticker);
  setDrilldownMetric("alert-detail-type", detail.alert_type_label);
  setDrilldownMetric("alert-detail-title", detail.title);
  setDrilldownMetric("alert-detail-summary", detail.summary);
  setDrilldownMetric("alert-detail-horizon", detail.horizon);
  setDrilldownMetric("alert-detail-status", detail.status_label);
  setDrilldownMetric("alert-detail-score", detail.score, formatAlertScore);
  setDrilldownMetric("alert-detail-validation", detail.validation);
  setDrilldownMetric("alert-detail-result", detail.result);
  setDrilldownMetric("alert-detail-created-at", detail.created_at, formatDetailTimestamp);
  setDrilldownMetric("alert-detail-evaluated-at", detail.evaluated_at, formatDetailTimestamp);
  setDrilldownMetric("alert-detail-context", detail.context_note);
  setDrilldownMetric("alert-detail-reliability", detail.reliability_note);
}

async function loadAlertsDrilldown(alertId, forceRefresh = false, alertKey = "") {
  const normalized = String(alertId || "").trim();
  if (!normalized) {
    return;
  }

  alertsSelectedId = normalized;
  updateAlertsDrilldownSelection(normalized);
  renderAlertsDrilldownLoading(alertKey || normalized);

  if (!forceRefresh && alertsDrilldownCache.has(normalized)) {
    renderAlertsDrilldown(alertsDrilldownCache.get(normalized));
    return;
  }

  const requestId = ++alertsDrilldownRequestId;
  try {
    const response = await fetch(`/api/dashboard/alerts/drilldown?alert_id=${encodeURIComponent(normalized)}`, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const payload = await response.json();
    alertsDrilldownCache.set(normalized, payload);
    if (requestId !== alertsDrilldownRequestId) {
      return;
    }
    renderAlertsDrilldown(payload);
  } catch (error) {
    if (requestId !== alertsDrilldownRequestId) {
      return;
    }
    renderAlertsDrilldownEmpty(`No pude cargar el detalle de la alerta (${error.message}).`, alertKey || normalized);
  }
}

function bindAlertsDrilldownTargets() {
  document.querySelectorAll(".alert-item-action[data-alert-id]").forEach((node) => {
    if (node.dataset.alertBound === "true") {
      return;
    }
    node.dataset.alertBound = "true";
    node.addEventListener("click", () => {
      loadAlertsDrilldown(node.dataset.alertId, false, node.dataset.alertKey || "");
    });
    node.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") {
        return;
      }
      event.preventDefault();
      loadAlertsDrilldown(node.dataset.alertId, false, node.dataset.alertKey || "");
    });
  });
}

function sourceBadgeMarkup(source, label, note) {
  const normalized = String(source || "unavailable").toLowerCase();
  return `<span class="source-badge source-${escapeHtml(normalized)}" title="${escapeHtml(humanizeDashboardCopy(note || ""))}">${escapeHtml(humanizeDashboardCopy(label || normalized))}</span>`;
}

function statusClassName(value) {
  const normalized = String(value || "").toLowerCase();
  if (normalized.includes("quota")) return "state-quota";
  if (normalized.includes("access")) return "state-access";
  if (
    normalized.includes("ok") ||
    normalized.includes("ready") ||
    normalized.includes("stable") ||
    normalized.includes("operativo") ||
    normalized.includes("disponible")
  ) {
    return "state-ok";
  }
  if (normalized.includes("loading")) return "state-loading";
  return "state-degraded";
}

function setStateToken(id, value) {
  const node = document.getElementById(id);
  if (!node) return;
  node.textContent = humanizeDashboardCopy(value);
  node.classList.remove("is-loading");
  node.classList.remove("state-token", "state-ok", "state-degraded", "state-quota", "state-access", "state-loading");
  node.classList.add("state-token", statusClassName(value));
}

function setStatusPillState(id, value) {
  const node = document.getElementById(id);
  if (!node) return;
  node.classList.remove("state-ok", "state-degraded", "state-quota", "state-access");
  node.classList.add(statusClassName(value));
}

function chipMarkup(text, tone = "neutral") {
  return `<span class="chip chip-tone-${escapeHtml(tone)}">${escapeHtml(humanizeDashboardCopy(text))}</span>`;
}

function getReliabilityTone(level) {
  const normalized = String(level || "").trim().toUpperCase();
  if (normalized === "ALTA") {
    return "state-ok";
  }
  if (normalized === "MEDIA") {
    return "state-loading";
  }
  return "state-degraded";
}

function renderReliabilityPartList(id, values, tone = "neutral", emptyText = "Sin dato visible") {
  const node = document.getElementById(id);
  if (!node) {
    return;
  }
  const items = Array.isArray(values) ? values.filter((value) => String(value ?? "").trim()) : [];
  if (!items.length) {
    node.innerHTML = `<span class="chip chip-muted">${escapeHtml(humanizeDashboardCopy(emptyText))}</span>`;
    return;
  }
  node.innerHTML = items.map((value) => chipMarkup(value, tone)).join("");
}

function renderReliabilitySnapshot(payload) {
  const reliability = payload.reliability || {};

  setText("reliability-summary-note", reliability.summary || "Sin lectura ejecutiva disponible.");
  setTokenValue("reliability-level", formatDetailText(reliability.level), getReliabilityTone(reliability.level));
  setText("reliability-decision", reliability.decision_note || "Sin lectura");
  setText("reliability-live-count", String(reliability.live_count ?? 0));
  setText("reliability-fallback-count", String(reliability.fallback_count ?? 0));
  setText("reliability-degraded-count", String(reliability.degraded_count ?? 0));
  setText("reliability-fmp-status", reliability.fmp_status_label || "Sin dato");

  renderReliabilityPartList("reliability-live-list", reliability.live_parts, "ok", "Sin datos directos confirmados");
  renderReliabilityPartList("reliability-fallback-list", reliability.fallback_parts, "warn", "Sin datos locales visibles");
  renderReliabilityPartList("reliability-fmp-list", reliability.fmp_dependent_parts, "neutral", "Sin fuente de mercado visible");
  renderReliabilityPartList("reliability-degraded-list", reliability.degraded_parts, "danger", "Sin datos parciales relevantes");
}

function renderReliabilitySnapshotError(message) {
  setText("reliability-summary-note", message);
  setTokenValue("reliability-level", "BAJA", "state-degraded");
  setText("reliability-decision", "No concluyente");
  setText("reliability-live-count", "0");
  setText("reliability-fallback-count", "0");
  setText("reliability-degraded-count", "1");
  setText("reliability-fmp-status", "Sin dato");

  renderReliabilityPartList("reliability-live-list", [], "ok", "Sin datos directos confirmados");
  renderReliabilityPartList("reliability-fallback-list", [], "warn", "Sin datos locales visibles");
  renderReliabilityPartList("reliability-fmp-list", [], "neutral", "Sin fuente de mercado visible");
  renderReliabilityPartList("reliability-degraded-list", [message], "danger", "Sin datos parciales relevantes");
}

function executiveQueueItemMarkup(item) {
  const ticker = formatDetailText(item.ticker);
  const priority = formatDetailText(item.priority);
  const decision = formatDetailText(item.decision);
  const reason = formatDetailText(item.main_reason);
  const reliability = formatDetailText(item.current_reliability);
  const timestamp = formatDetailTimestamp(item.timestamp);
  const signal = formatDetailText(item.signal_or_context || item.dominant_signal || item.context_note);

  return `
    <button type="button" class="detail-activity-item executive-queue-item" data-queue-ticker="${escapeHtml(ticker)}">
      <strong>${escapeHtml(ticker)} | ${escapeHtml(decision)}</strong>
      <small>${escapeHtml(priority)} | confiabilidad ${escapeHtml(reliability)} | ${escapeHtml(timestamp)}</small>
      <p>${escapeHtml(reason)}</p>
      <small>${escapeHtml(signal)}</small>
    </button>
  `;
}

function renderExecutiveQueueList(id, items, emptyText) {
  const node = document.getElementById(id);
  if (!node) {
    return;
  }
  const visibleItems = Array.isArray(items) ? items : [];
  if (!visibleItems.length) {
    node.innerHTML = `
      <div class="detail-activity-item">
        <strong>Sin activos</strong>
        <small>${escapeHtml(emptyText)}</small>
      </div>
    `;
    return;
  }
  node.innerHTML = visibleItems.map(executiveQueueItemMarkup).join("");
}

function bindExecutiveQueueTargets() {
  document.querySelectorAll("[data-queue-ticker]").forEach((node) => {
    if (node.dataset.queueBound === "true") {
      return;
    }
    node.dataset.queueBound = "true";
    node.addEventListener("click", () => {
      const ticker = node.dataset.queueTicker || "";
      activateView("radar");
      loadRadarDrilldown(ticker);
    });
  });
}

function renderExecutiveQueueSnapshot(payload) {
  const summary = payload.summary || {};
  const buckets = payload.buckets || {};
  const waitItems = [...(buckets["esperar"] || []), ...(buckets["no concluyente"] || [])];

  setText("executive-queue-note", summary.note || "Sin lectura ejecutiva global disponible.");
  setText("executive-queue-total", String(summary.total_assets ?? 0));
  setText("executive-queue-review-count", String(summary.review_now_count ?? 0));
  setText("executive-queue-watch-count", String(summary.watch_count ?? 0));
  setText("executive-queue-wait-count", String(summary.wait_count ?? 0));
  setTokenValue("executive-queue-reliability", formatDetailText(summary.reliability_level), getReliabilityTone(summary.reliability_level));

  renderExecutiveQueueList("executive-queue-review-list", buckets["revisar ahora"], "No hay activos con senal reciente prioritaria.");
  renderExecutiveQueueList("executive-queue-watch-list", buckets.vigilar, "No hay activos en vigilancia ejecutiva.");
  renderExecutiveQueueList("executive-queue-wait-list", waitItems, "No hay activos en espera o no concluyentes.");
  bindExecutiveQueueTargets();
}

function renderExecutiveQueueError(message) {
  setText("executive-queue-note", message);
  setText("executive-queue-total", "0");
  setText("executive-queue-review-count", "0");
  setText("executive-queue-watch-count", "0");
  setText("executive-queue-wait-count", "0");
  setTokenValue("executive-queue-reliability", "BAJA", "state-degraded");
  renderExecutiveQueueList("executive-queue-review-list", [], "No pude cargar la cola ejecutiva.");
  renderExecutiveQueueList("executive-queue-watch-list", [], "No pude cargar la cola ejecutiva.");
  renderExecutiveQueueList("executive-queue-wait-list", [], "No pude cargar la cola ejecutiva.");
}

function getMoneyFlowStatusTone(status) {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized.includes("ready")) {
    return "state-ok";
  }
  if (normalized.includes("unavailable") || normalized.includes("error")) {
    return "state-degraded";
  }
  return "state-loading";
}

function formatMoneyFlowConfidence(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "high") return "Alta";
  if (normalized === "medium") return "Media";
  if (normalized === "low") return "Baja";
  return normalized || "No concluyente";
}

function getMoneyFlowSignalTone(signalType) {
  const normalized = String(signalType || "").trim();
  if (normalized === "strong_inflow" || normalized === "volume_breakout") return "ok";
  if (normalized === "strong_outflow") return "danger";
  if (normalized === "price_volume_divergence" || normalized === "sector_pressure") return "warn";
  if (normalized === "risk_on_risk_off" || normalized === "rotation") return "neutral";
  return "muted";
}

function buildMoneyFlowDetectionMap(payload) {
  const map = new Map();
  const items = Array.isArray(payload?.items) ? payload.items : [];
  items.forEach((item) => {
    const ticker = String(item?.ticker || "").trim().toUpperCase();
    if (ticker) {
      map.set(ticker, item);
    }
  });
  return map;
}

function mergeMoneyFlowItems(causalPayload, detectionPayload) {
  const detectionMap = buildMoneyFlowDetectionMap(detectionPayload);
  const merged = [];
  const seen = new Set();
  const causalItems = Array.isArray(causalPayload?.items) ? causalPayload.items : [];

  causalItems.forEach((item) => {
    const ticker = String(item?.ticker || "").trim().toUpperCase();
    if (!ticker) return;
    merged.push(normalizeMoneyFlowItem(item, detectionMap.get(ticker)));
    seen.add(ticker);
  });

  detectionMap.forEach((item, ticker) => {
    if (!seen.has(ticker)) {
      merged.push(normalizeMoneyFlowItem(item, item));
    }
  });

  return merged;
}

function normalizeMoneyFlowItem(item, detectionItem = {}) {
  if (item.signal_type && item.signal_code) {
    return item;
  }

  const signalType = String(
    item.money_flow_primary_signal ||
    item.primary_signal ||
    detectionItem.primary_signal ||
    "insufficient_confirmation"
  ).trim();
  const probableCause = String(item.probable_cause || "").trim();
  const nonConclusive = signalType === "insufficient_confirmation" || probableCause === "inconclusive";
  const reason = String(item.reason || item.context_note || "").trim();
  const signalLabel = item.money_flow_primary_label || item.primary_label || detectionItem.primary_label || signalType || "senal Money Flow";
  const causeLabel = item.probable_cause_label || (nonConclusive ? "no concluyente" : "sin causa probable disponible");
  const executiveRead = nonConclusive
    ? `No concluyente: ${reason || "falta evidencia suficiente."}`
    : `${signalType}: ${signalLabel}. Causa probable: ${causeLabel}.`;

  return {
    ticker: String(item.ticker || detectionItem.ticker || "").trim().toUpperCase(),
    signal_type: signalType,
    signal_code: signalType,
    signal_label: signalLabel,
    signal_tone: getMoneyFlowSignalTone(signalType),
    probable_cause_label: causeLabel,
    confidence: item.confidence || "no concluyente",
    timestamp: item.money_flow_timestamp || item.timestamp || detectionItem.timestamp || "",
    status: nonConclusive ? "no_conclusive" : "active",
    executive_read: executiveRead,
  };
}

function renderMoneyFlowSnapshot(causalPayload, detectionPayload = {}) {
  const rawSummary = causalPayload.summary || {};
  const detectionSummary = detectionPayload.summary || {};
  const sourceStatus = causalPayload.source_status || {};
  const items = mergeMoneyFlowItems(causalPayload, detectionPayload);
  const tableBody = document.getElementById("money-flow-table-body");
  const totalAssets = rawSummary.total_assets ?? detectionSummary.total_assets ?? items.length;
  const nonConclusiveCount = rawSummary.non_conclusive_count ?? rawSummary.assets_inconclusive ?? detectionSummary.assets_insufficient_confirmation ?? items.filter((item) => item.status === "no_conclusive").length;
  const detectedCount = rawSummary.detected_count ?? detectionSummary.assets_with_detected_flow ?? Math.max(0, Number(totalAssets) - Number(nonConclusiveCount));
  const causeCount = rawSummary.with_probable_cause ?? rawSummary.assets_with_probable_cause ?? 0;
  const detectionStatus = detectionPayload.status || sourceStatus.money_flow_detection_status || "detection_ready_causality_disabled";
  const causalStatus = sourceStatus.money_flow_causal_status || causalPayload.status || "probable_causality_ready";

  setText("money-flow-summary-note", rawSummary.note || "Sin lectura de flujo disponible.");
  setText("money-flow-total-assets", String(totalAssets ?? 0));
  setText("money-flow-detected-count", String(detectedCount ?? 0));
  setText("money-flow-non-conclusive-count", String(nonConclusiveCount ?? 0));
  setText("money-flow-cause-count", String(causeCount ?? 0));
  setTokenValue(
    "money-flow-detection-status",
    "5.2",
    getMoneyFlowStatusTone(detectionStatus)
  );
  setTokenValue(
    "money-flow-causal-status",
    "5.3",
    getMoneyFlowStatusTone(causalStatus)
  );
  setText("money-flow-table-note", `5.2 ${detectionStatus} + 5.3 ${causalStatus}. Lectura probable, sin causalidad confirmada.`);

  if (!tableBody) return;
  if (!items.length) {
    tableBody.innerHTML = `<tr><td colspan="6">${escapeHtml(humanizeDashboardCopy(causalPayload.empty_state || "Sin senales de flujo visibles."))}</td></tr>`;
    return;
  }

  tableBody.innerHTML = items
    .map((item) => {
      const nonConclusive = item.status === "no_conclusive";
      const rowClass = nonConclusive ? "money-flow-row money-flow-row--muted" : "money-flow-row";
      const signalTone = item.signal_tone || "muted";
      const statusNote = nonConclusive ? '<span class="money-flow-status-note">No concluyente</span>' : "";
      return `
        <tr class="${rowClass}">
          <td><strong>${escapeHtml(item.ticker)}</strong></td>
          <td>
            <span class="money-flow-signal money-flow-signal-${escapeHtml(signalTone)}">${escapeHtml(humanizeDashboardCopy(item.signal_code || item.signal_type))}</span>
            <small class="money-flow-signal-label">${escapeHtml(humanizeDashboardCopy(item.signal_label || "Sin lectura de senal"))}</small>
            ${statusNote}
          </td>
          <td>${escapeHtml(humanizeDashboardCopy(item.probable_cause_label || "sin causa probable disponible"))}</td>
          <td>${escapeHtml(formatMoneyFlowConfidence(item.confidence))}</td>
          <td class="detail-value-time">${escapeHtml(item.timestamp ? formatIso(item.timestamp) : "Sin timestamp")}</td>
          <td>${escapeHtml(humanizeDashboardCopy(item.executive_read || "Sin lectura ejecutiva disponible."))}</td>
        </tr>
      `;
    })
    .join("");
}

function renderMoneyFlowError(message) {
  setText("money-flow-summary-note", message);
  setText("money-flow-total-assets", "0");
  setText("money-flow-detected-count", "0");
  setText("money-flow-non-conclusive-count", "0");
  setText("money-flow-cause-count", "0");
  setTokenValue("money-flow-detection-status", "5.2", "state-degraded");
  setTokenValue("money-flow-causal-status", "5.3", "state-degraded");
  setText("money-flow-table-note", "No pude cargar la lectura de flujo desde el endpoint local.");

  const tableBody = document.getElementById("money-flow-table-body");
  if (tableBody) {
    tableBody.innerHTML = `<tr><td colspan="6">${escapeHtml(humanizeDashboardCopy(message))}</td></tr>`;
  }
}

function renderMoneyFlowJarvisAnswer(payload) {
  const node = document.getElementById("money-flow-jarvis-answer");
  if (!node) return;
  const source = payload.source_status || {};
  node.innerHTML = `
    <strong>${escapeHtml(humanizeDashboardCopy(payload.answer || "No hay respuesta disponible."))}</strong>
    <small>Fuente: 5.2 ${escapeHtml(humanizeDashboardCopy(source.detection_status || "unknown"))} + 5.3 ${escapeHtml(humanizeDashboardCopy(source.causal_status || "unknown"))}</small>
    <small>${escapeHtml(humanizeDashboardCopy(payload.honesty_note || "Lectura conservadora."))}</small>
  `;
}

function renderMoneyFlowJarvisError(message) {
  const node = document.getElementById("money-flow-jarvis-answer");
  if (!node) return;
  node.innerHTML = `
    <strong>No pude responder la lectura Genesis de flujo.</strong>
    <small>${escapeHtml(humanizeDashboardCopy(message))}</small>
  `;
}

async function loadMoneyFlowJarvisAnswer(question) {
  const query = String(question || "").trim();
  const node = document.getElementById("money-flow-jarvis-answer");
  if (node) {
    node.innerHTML = "<strong>Leyendo flujo...</strong><small>Consultando datos disponibles del panel.</small>";
  }
  try {
    const response = await fetch(`/api/dashboard/money-flow/jarvis?q=${encodeURIComponent(query)}`, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const payload = await response.json();
    renderMoneyFlowJarvisAnswer(payload);
  } catch (error) {
    renderMoneyFlowJarvisError(error.message);
  }
}

function bindMoneyFlowJarvisForm() {
  const form = document.getElementById("money-flow-jarvis-form");
  const input = document.getElementById("money-flow-jarvis-input");
  if (!form || !input || form.dataset.bound === "true") return;
  form.dataset.bound = "true";
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    loadMoneyFlowJarvisAnswer(input.value || "flujo");
  });
}

function renderOperationalHealth(payload) {
  const system = payload.system || {};
  const bot = payload.bot || {};
  const radar = payload.radar || {};
  const provider = payload.provider || {};

  setText("status-system-label", "Sistema");
  setText("status-system-value", formatSystemTopbarValue(system));
  setStatusPillState("status-pill-system", systemTopbarTone(system));
  setText("status-leader-label", "Coordinacion");
  setText("status-leader-value", `${bot.hostname || "n/a"} | ${bot.leader || "sin registro"}`);
  setText("status-radar-label", "Radar");
  setText("status-radar-value", `${radar.size ?? 0} activos`);

  setStateToken("metric-system-state", formatSystemStateToken(system));
  setText("metric-system-summary", sanitizeShellCopy(system.summary || "Sin resumen operativo"));
  setText("metric-boot-stage", bot.boot_stage || "unknown");
  setText("metric-runtime-note", bot.runtime_note || "Sin nota de runtime");
  setText("metric-last-update", system.last_update || "Sin actualizacion");
  setText("metric-heartbeat-age", formatHeartbeatAge(bot.heartbeat_age_seconds));
  setText("metric-radar-size", `${radar.size ?? 0}`);

  setText("detail-bot-status", bot.configured ? "Configurado y visible para el dashboard." : "Modo local activo.");
  setText("detail-leader", `${bot.leader || "sin registro"} en ${bot.hostname || "local"}`);
  setText("detail-boot-stage", bot.boot_stage || "unknown");
  setText("detail-last-update", system.last_update || "Sin timestamp");
  setText("detail-radar", `${radar.size ?? 0} activos vigilados.`);
  setText("provider-note", provider.note || "Sin nota de proveedor.");
}

function renderOperationalHealthError(message) {
  setText("status-system-label", "Sistema");
  setText("status-system-value", "Sin conexion al endpoint local");
  setStatusPillState("status-pill-system", "degraded");
  setText("status-leader-label", "Coordinacion");
  setText("status-leader-value", "No disponible");
  setText("status-radar-label", "Radar");
  setText("status-radar-value", "No disponible");
  setStateToken("metric-system-state", "offline");
  setText("metric-system-summary", message);
  setText("metric-boot-stage", "sin datos");
  setText("metric-runtime-note", "El shell sigue navegable sin backend.");
  setText("metric-last-update", "Sin datos");
  setText("metric-heartbeat-age", "Sin heartbeat confirmado");
  setText("metric-radar-size", "0");
  setText("detail-bot-status", message);
  setText("detail-leader", "Sin datos");
  setText("detail-boot-stage", "Sin datos");
  setText("detail-last-update", "Sin datos");
  setText("detail-radar", "Sin datos");
  setText("provider-note", "El shell del dashboard sigue disponible aunque el endpoint local no este activo.");
}

function renderRadarSnapshot(payload) {
  const summary = payload.summary || {};
  const items = Array.isArray(payload.items) ? payload.items : [];
  const tickerList = document.getElementById("radar-ticker-list");
  const tableBody = document.getElementById("radar-table-body");
  const availableTickers = new Set(items.map((item) => String(item?.ticker || "").trim().toUpperCase()).filter(Boolean));

  setText("radar-summary-note", summary.note || "Sin nota de radar.");
  setText("radar-tracked-count", String(summary.tracked_count ?? 0));
  setText("radar-investment-count", String(summary.investment_count ?? 0));
  setText("radar-reference-count", String(summary.reference_count ?? 0));
  setText("radar-last-update", summary.last_update ? formatIso(summary.last_update) : "Sin timestamp");
  setText("radar-table-note", `Origen: ${summary.data_origin || "unknown"}. Fuente: datos directos, cache o contingencia.`);

  if (tickerList) {
    if (!items.length) {
      tickerList.innerHTML = '<span class="chip chip-muted">Sin activos vigilados</span>';
    } else {
      tickerList.innerHTML = items
        .map((item) => `
          <button type="button" class="chip chip-action" data-drilldown-ticker="${escapeHtml(item.ticker || "")}">
            ${escapeHtml(item.ticker)}
          </button>
        `)
        .join("");
    }
  }

  if (tableBody) {
    if (!items.length) {
      tableBody.innerHTML = '<tr><td colspan="5">Sin activos vigilados todavia.</td></tr>';
    } else {
      tableBody.innerHTML = items
        .map((item) => {
          const updated = item.updated_at ? formatIso(item.updated_at) : "Sin timestamp";
          return `
            <tr class="table-row-action" data-drilldown-ticker="${escapeHtml(item.ticker || "")}" tabindex="0">
              <td><strong>${escapeHtml(item.ticker || "n/a")}</strong></td>
              <td>${escapeHtml(humanizeDashboardCopy(item.signal || "Sin senal"))}</td>
              <td>${escapeHtml(formatPrice(item.reference_price))}</td>
              <td>${sourceBadgeMarkup(item.source, item.source_label, item.source_note)}</td>
              <td>${escapeHtml(updated)}</td>
            </tr>
          `;
        })
        .join("");
    }
  }

  if (!items.length) {
    radarSelectedTicker = "";
    renderRadarDrilldownEmpty("El radar aun no tiene activos visibles para abrir detalle.");
  }
  if (radarSelectedTicker && !availableTickers.has(radarSelectedTicker)) {
    radarSelectedTicker = "";
    renderRadarDrilldownEmpty("El ticker seleccionado ya no aparece en la lectura actual del radar.");
  }
  updateRadarDrilldownSelection(radarSelectedTicker);
  bindRadarDrilldownTargets();
}

function renderRadarSnapshotError(message) {
  setText("radar-summary-note", message);
  setText("radar-tracked-count", "0");
  setText("radar-investment-count", "0");
  setText("radar-reference-count", "0");
  setText("radar-last-update", "Sin datos");
  setText("radar-table-note", "No pude cargar el snapshot de radar desde el endpoint local.");

  const tickerList = document.getElementById("radar-ticker-list");
  if (tickerList) {
    tickerList.innerHTML = '<span class="chip chip-muted">Sin datos</span>';
  }

  const tableBody = document.getElementById("radar-table-body");
  if (tableBody) {
    tableBody.innerHTML = `<tr><td colspan="5">${escapeHtml(humanizeDashboardCopy(message))}</td></tr>`;
  }

  radarSelectedTicker = "";
  renderRadarDrilldownEmpty("No pude abrir la ficha tactica porque el radar no esta disponible.");
}

function renderAlertsSnapshot(payload) {
  const summary = payload.summary || {};
  const recentAlerts = Array.isArray(payload.recent_alerts) ? payload.recent_alerts : [];
  const recentList = document.getElementById("alerts-recent-list");
  const availableAlertIds = new Set(recentAlerts.map((alert) => String(alert?.alert_id || "").trim()).filter(Boolean));

  setText("alerts-summary-note", summary.engine_summary || "Sin resumen del motor.");
  setText("alerts-total-recent", String(summary.total_recent ?? 0));
  setText("alerts-active-count", String(summary.active_alerts ?? 0));
  setText("alerts-validated-count", String(summary.validated_alerts ?? 0));
  setText("alerts-avg-score", formatSignedScore(summary.avg_score));
  setText("alerts-win-rate", formatPercent(summary.win_rate));
  setText("alerts-pass-rate", formatPercent(summary.pass_rate));
  setText("alerts-list-note", `Ventana: ${summary.window_days ?? 0} dias. Origen: ${summary.data_origin || "unknown"}. Update: ${summary.last_update ? formatIso(summary.last_update) : "sin timestamp"}.`);

  if (!recentList) return;

  if (!recentAlerts.length) {
    recentList.innerHTML = '<div class="activity-item"><span class="dot dot-muted"></span><div><strong>Sin alertas recientes</strong><small>El motor todavia no tiene eventos en la ventana actual.</small></div></div>';
    alertsSelectedId = "";
    updateAlertsDrilldownSelection("");
    renderAlertsDrilldownEmpty("Todavia no hay alertas recientes para abrir un detalle ejecutivo.");
    return;
  }

  recentList.innerHTML = recentAlerts
    .map((alert) => {
      const alertId = String(alert.alert_id || "").trim();
      const alertKey = buildAlertListKey(alert);
      const scoreText = alert.latest_validation && Number.isFinite(Number(alert.latest_validation.score_value))
        ? ` | puntaje ${formatSignedScore(alert.latest_validation.score_value)}`
        : "";
      const validationText = alert.latest_validation && alert.latest_validation.evaluated_at
        ? `Validada ${formatIso(alert.latest_validation.evaluated_at)}${scoreText}`
        : `Creada ${formatIso(alert.created_at)}`;
      const summaryText = alert.summary || "Sin resumen corto disponible.";
      return `
        <div class="activity-item alert-item alert-item-action" role="button" tabindex="0" aria-label="Abrir detalle de ${escapeHtml(alertKey)}" data-alert-id="${escapeHtml(alertId)}" data-alert-key="${escapeHtml(alertKey)}">
          <span class="dot ${alert.status === "completed" ? "dot-ok" : "dot-warn"}"></span>
          <div>
            <strong>${escapeHtml(alertKey)}</strong>
            <small>${escapeHtml(humanizeDashboardCopy(alert.state_label || "Sin estado"))} | ${escapeHtml(humanizeDashboardCopy(validationText))}</small>
            <p class="alert-summary">${escapeHtml(humanizeDashboardCopy(summaryText))}</p>
          </div>
        </div>
      `;
    })
    .join("");

  if (alertsSelectedId && !availableAlertIds.has(alertsSelectedId)) {
    alertsSelectedId = "";
    renderAlertsDrilldownEmpty("La alerta seleccionada ya no aparece dentro de la ventana actual.");
  } else if (!alertsSelectedId) {
    renderAlertsDrilldownEmpty("Selecciona una alerta reciente para abrir su detalle ejecutivo.");
  }

  updateAlertsDrilldownSelection(alertsSelectedId);
  bindAlertsDrilldownTargets();
}

function renderAlertsSnapshotError(message) {
  setText("alerts-summary-note", message);
  setText("alerts-total-recent", "0");
  setText("alerts-active-count", "0");
  setText("alerts-validated-count", "0");
  setText("alerts-avg-score", "N/D");
  setText("alerts-win-rate", "N/D");
  setText("alerts-pass-rate", "N/D");
  setText("alerts-list-note", "No pude cargar el snapshot de alertas desde el endpoint local.");

  const recentList = document.getElementById("alerts-recent-list");
  if (recentList) {
    recentList.innerHTML = `<div class="activity-item"><span class="dot dot-muted"></span><div><strong>Lectura no disponible</strong><small>${escapeHtml(humanizeDashboardCopy(message))}</small></div></div>`;
  }

  alertsSelectedId = "";
  updateAlertsDrilldownSelection("");
  renderAlertsDrilldownEmpty("No pude abrir el detalle de alertas porque la lectura guardada no esta disponible.");
}

function renderDependenciesSnapshot(payload) {
  const provider = payload.provider || {};
  const usage = payload.usage || {};
  const signals = payload.signals || {};
  const lastIncident = payload.last_incident || {};

  setText("dependencies-summary-note", provider.note || "Sin nota de proveedor.");
  setStateToken("dependencies-status", provider.status || "DEGRADED");
  setText("dependencies-cooldown-active", String(signals.cooldown_active ?? 0));
  setText("dependencies-cache-hit-total", String(signals.cache_hit ?? 0));
  setText("dependencies-quota-total", String(signals.quota ?? 0));
  setText("dependencies-access-total", String(signals.access ?? 0));
  setText("dependencies-last-snapshot", payload.generated_at ? formatIso(payload.generated_at) : "Sin timestamp");
  setText(
    "dependencies-endpoint-note",
    `Origen: ${(payload.meta || {}).source || "unknown"}. Ventana de resumen: ${provider.summary_window_seconds ?? 0}s. El dashboard no dispara consultas nuevas.`
  );

  const signalList = document.getElementById("dependencies-signal-list");
  if (signalList) {
    const chips = [
      chipMarkup(`Cache: ${signals.cache_hit ?? 0}`, "ok"),
      chipMarkup(`Pausa: ${signals.throttle ?? 0}`, "warn"),
      chipMarkup(`Pausa activa: ${signals.cooldown_active ?? 0}`, "neutral"),
      chipMarkup(`Limite: ${signals.quota ?? 0}`, "danger"),
      chipMarkup(`Acceso: ${signals.access ?? 0}`, "warn"),
    ];
    signalList.innerHTML = chips.join("");
  }

  const endpointBody = document.getElementById("dependencies-endpoint-body");
  if (endpointBody) {
    const rows = ["quote", "eod", "intraday", "news"].map((kind) => {
      const bucket = usage[kind] || {};
      return `
        <tr>
          <td><strong>${escapeHtml(humanizeDashboardCopy(kind))}</strong></td>
          <td>${escapeHtml(String(bucket.fetch ?? 0))}</td>
          <td>${escapeHtml(String(bucket.ok ?? 0))}</td>
          <td>${escapeHtml(String(bucket.cache_hit ?? 0))}</td>
          <td>${escapeHtml(String(bucket.throttle ?? 0))}</td>
          <td>${escapeHtml(String(bucket.quota ?? 0))}</td>
          <td>${escapeHtml(String(bucket.access ?? 0))}</td>
        </tr>
      `;
    });
    endpointBody.innerHTML = rows.join("");
  }

  const incidentNote = document.getElementById("dependencies-incident-note");
  if (incidentNote) {
    if (!lastIncident.category) {
      incidentNote.textContent = "Sin incidencia de mercado persistida todavia.";
    } else {
      const detail = lastIncident.detail ? ` | ${humanizeDashboardCopy(lastIncident.detail)}` : "";
      const status = lastIncident.status_code ? ` | HTTP ${lastIncident.status_code}` : "";
      const updated = lastIncident.updated_at ? ` | ${formatIso(lastIncident.updated_at)}` : "";
      incidentNote.textContent = `${humanizeDashboardCopy(lastIncident.category)} | ${lastIncident.ticker || "sin ticker"}${status}${updated}${detail}`;
    }
  }
}

function renderDependenciesSnapshotError(message) {
  setText("dependencies-summary-note", message);
  setStateToken("dependencies-status", "DEGRADED");
  setText("dependencies-cooldown-active", "0");
  setText("dependencies-cache-hit-total", "0");
  setText("dependencies-quota-total", "0");
  setText("dependencies-access-total", "0");
  setText("dependencies-last-snapshot", "Sin datos");
  setText("dependencies-endpoint-note", "No pude cargar el estado de fuentes desde el endpoint local.");
  setText("dependencies-incident-note", message);

  const signalList = document.getElementById("dependencies-signal-list");
  if (signalList) {
    signalList.innerHTML = '<span class="chip chip-muted">Sin datos de proveedor</span>';
  }

  const endpointBody = document.getElementById("dependencies-endpoint-body");
  if (endpointBody) {
    endpointBody.innerHTML = `<tr><td colspan="7">${escapeHtml(humanizeDashboardCopy(message))}</td></tr>`;
  }
}

function renderMacroSnapshot(payload) {
  const macro = payload.macro || {};
  const sentiment = macro.sentiment || {};
  const highRisk = Array.isArray(macro.high_risk_tickers) ? macro.high_risk_tickers : [];
  const sensitive = Array.isArray(macro.sensitive_tickers) ? macro.sensitive_tickers : [];
  const headlines = Array.isArray(macro.headlines) ? macro.headlines : [];

  setText("macro-summary-note", macro.note || "Sin lectura macro guardada.");
  setText("macro-bias-label", macro.bias_label || "macro mixto");
  setText("macro-sentiment-label", `${sentiment.icon || "N/D"} ${sentiment.label || "Neutral"}`);
  setText("macro-confidence", macro.confidence ? `${macro.confidence}%` : "N/D");
  setText("macro-last-update", macro.last_update ? formatIso(macro.last_update) : "Sin timestamp");
  setText("macro-sensitive-count", String(sensitive.length));
  setText("macro-high-risk-count", String(highRisk.length));
  setText("macro-dominant-risk-note", macro.dominant_risk || macro.summary || "Sin lectura macro persistida todavía.");
  setText("macro-headline-note", `Origen: ${(payload.meta || {}).macro_source || "unknown"}. La vista reutiliza lectura guardada del sistema local.`);

  const macroChips = document.getElementById("macro-high-risk-list");
  if (macroChips) {
    const values = sensitive.length ? sensitive : highRisk;
    if (!values.length) {
      macroChips.innerHTML = '<span class="chip chip-muted">Sin tickers sensibles persistidos</span>';
    } else {
      macroChips.innerHTML = values.map((item) => `<span class="chip">${escapeHtml(item)}</span>`).join("");
    }
  }

  const headlineList = document.getElementById("macro-headline-list");
  if (!headlineList) return;
  if (!headlines.length) {
    headlineList.innerHTML = '<div class="activity-item"><span class="dot dot-muted"></span><div><strong>Sin contexto reciente</strong><small>El sistema local todavia no ha guardado una lectura macro reciente.</small></div></div>';
    return;
  }

  headlineList.innerHTML = headlines
    .map((item) => `
      <div class="activity-item">
        <span class="dot dot-ok"></span>
        <div>
          <strong>${escapeHtml(humanizeDashboardCopy(item.title || "Titular"))}</strong>
          <small>${escapeHtml(humanizeDashboardCopy(item.source || "Fuente"))} | ${escapeHtml(item.published_at || "reciente")}</small>
          <p class="alert-summary">${escapeHtml(humanizeDashboardCopy(item.impact_summary || "Sin resumen adicional."))}</p>
        </div>
      </div>
    `)
    .join("");
}

function renderActivitySnapshot(payload) {
  const activity = payload.activity || {};
  const items = Array.isArray(activity.items) ? activity.items : [];
  const errorCount = items.filter((item) => item.level === "error").length;
  const warningCount = items.filter((item) => item.level === "warning").length;
  const lastEvent = items.length ? items[0] : null;

  setText("activity-summary-note", activity.note || "Sin actividad operativa.");
  setText("activity-total-events", String(items.length));
  setText("activity-error-count", String(errorCount));
  setText("activity-warning-count", String(warningCount));
  setText("activity-last-update", lastEvent && lastEvent.occurred_at ? formatIso(lastEvent.occurred_at) : "Sin timestamp");
  setText("activity-source-label", `${(payload.meta || {}).activity_source || "unknown"}`);
  setText("activity-event-note", `Origen: ${(payload.meta || {}).activity_source || "unknown"}. La vista resume solo eventos utiles del sistema local.`);

  const eventList = document.getElementById("activity-event-list");
  if (!eventList) return;
  if (!items.length) {
    eventList.innerHTML = '<div class="activity-item"><span class="dot dot-muted"></span><div><strong>Sin actividad guardada</strong><small>El sistema local todavia no ha guardado eventos utiles para esta vista.</small></div></div>';
    return;
  }

  eventList.innerHTML = items
    .map((item) => {
      const dotClass = item.level === "error" ? "dot-warn" : (item.level === "warning" ? "dot-muted" : "dot-ok");
      const levelLabel = item.level === "error" ? "Error" : (item.level === "warning" ? "Aviso" : "Info");
      return `
        <div class="activity-item">
          <span class="dot ${dotClass}"></span>
          <div>
            <strong>${escapeHtml(humanizeDashboardCopy(item.event || "Evento"))}</strong>
            <small>${escapeHtml(formatIso(item.occurred_at))} | ${escapeHtml(levelLabel)}</small>
            <p class="alert-summary">${escapeHtml(humanizeDashboardCopy(item.summary || "Sin detalle adicional."))}</p>
          </div>
        </div>
      `;
    })
    .join("");
}

function renderMacroActivityError(message) {
  setText("macro-summary-note", message);
  setText("macro-bias-label", "macro mixto");
  setText("macro-sentiment-label", "N/D");
  setText("macro-confidence", "N/D");
  setText("macro-last-update", "Sin datos");
  setText("macro-sensitive-count", "0");
  setText("macro-high-risk-count", "0");
  setText("macro-dominant-risk-note", message);
  setText("macro-headline-note", "No pude cargar la lectura de Mundo / Historial desde la consulta local.");
  setText("activity-summary-note", message);
  setText("activity-total-events", "0");
  setText("activity-error-count", "0");
  setText("activity-warning-count", "0");
  setText("activity-last-update", "Sin datos");
  setText("activity-source-label", "unavailable");
  setText("activity-event-note", "No pude cargar la lectura de Mundo / Historial desde la consulta local.");

  const macroChips = document.getElementById("macro-high-risk-list");
  if (macroChips) {
    macroChips.innerHTML = '<span class="chip chip-muted">Sin datos macro</span>';
  }
  const headlineList = document.getElementById("macro-headline-list");
  if (headlineList) {
    headlineList.innerHTML = `<div class="activity-item"><span class="dot dot-muted"></span><div><strong>Lectura no disponible</strong><small>${escapeHtml(humanizeDashboardCopy(message))}</small></div></div>`;
  }
  const eventList = document.getElementById("activity-event-list");
  if (eventList) {
    eventList.innerHTML = `<div class="activity-item"><span class="dot dot-muted"></span><div><strong>Lectura no disponible</strong><small>${escapeHtml(humanizeDashboardCopy(message))}</small></div></div>`;
  }
}

async function loadOperationalHealth() {
  try {
    const response = await fetch("/api/dashboard/health", { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const payload = await response.json();
    renderOperationalHealth(payload);
  } catch (error) {
    renderOperationalHealthError(`No pude cargar la salud operativa (${error.message}).`);
  }
}

async function loadOperationalReliability(forceRefresh = false) {
  if (!forceRefresh && loadedViews.has("reliability")) {
    return;
  }
  try {
    const response = await fetch("/api/dashboard/reliability", { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const payload = await response.json();
    renderReliabilitySnapshot(payload);
    loadedViews.add("reliability");
  } catch (error) {
    renderReliabilitySnapshotError(`No pude cargar la lectura ejecutiva de confiabilidad (${error.message}).`);
  }
}

async function loadExecutiveQueue(forceRefresh = false) {
  if (!forceRefresh && loadedViews.has("executive-queue")) {
    return;
  }
  try {
    const response = await fetch("/api/dashboard/executive-queue", { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const payload = await response.json();
    renderExecutiveQueueSnapshot(payload);
    loadedViews.add("executive-queue");
  } catch (error) {
    renderExecutiveQueueError(`No pude cargar la cola ejecutiva (${error.message}).`);
  }
}

async function loadMoneyFlowSnapshot(forceRefresh = false) {
  if (!forceRefresh && loadedViews.has("money-flow")) {
    return;
  }
  try {
    const [detectionResponse, causalResponse] = await Promise.all([
      fetch("/api/dashboard/money-flow/detection", { cache: "no-store" }),
      fetch("/api/dashboard/money-flow/causal", { cache: "no-store" }),
    ]);
    if (!detectionResponse.ok) {
      throw new Error(`5.2 HTTP ${detectionResponse.status}`);
    }
    if (!causalResponse.ok) {
      throw new Error(`5.3 HTTP ${causalResponse.status}`);
    }
    const detectionPayload = await detectionResponse.json();
    const causalPayload = await causalResponse.json();
    renderMoneyFlowSnapshot(causalPayload, detectionPayload);
    bindMoneyFlowJarvisForm();
    loadedViews.add("money-flow");
  } catch (error) {
    renderMoneyFlowError(`No pude cargar la lectura de flujo (${error.message}).`);
  }
}

async function loadRadarSnapshot(forceRefresh = false) {
  if (!forceRefresh && loadedViews.has("radar")) {
    return;
  }
  try {
    const response = await fetch("/api/dashboard/radar", { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const payload = await response.json();
    renderRadarSnapshot(payload);
    loadedViews.add("radar");
    if (!radarSelectedTicker) {
      renderRadarDrilldownEmpty("Selecciona un ticker desde el radar o desde la tabla para abrir la ficha tactica.");
    }
  } catch (error) {
    renderRadarSnapshotError(`No pude cargar Radar / Cartera (${error.message}).`);
  }
}

async function loadAlertsSnapshot(forceRefresh = false) {
  if (!forceRefresh && loadedViews.has("alerts")) {
    return;
  }
  try {
    const response = await fetch("/api/dashboard/alerts", { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const payload = await response.json();
    renderAlertsSnapshot(payload);
    loadedViews.add("alerts");
  } catch (error) {
    renderAlertsSnapshotError(`No pude cargar Alertas (${error.message}).`);
  }
}

async function loadDependenciesSnapshot(forceRefresh = false) {
  if (!forceRefresh && loadedViews.has("dependencies")) {
    return;
  }
  try {
    const response = await fetch("/api/dashboard/fmp", { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const payload = await response.json();
    renderDependenciesSnapshot(payload);
    loadedViews.add("dependencies");
  } catch (error) {
    renderDependenciesSnapshotError(`No pude cargar Fuentes (${error.message}).`);
  }
}

async function loadMacroActivitySnapshot(forceRefresh = false) {
  if (!forceRefresh && loadedViews.has("macro-activity")) {
    return;
  }
  try {
    const response = await fetch("/api/dashboard/macro-activity", { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const payload = await response.json();
    renderMacroSnapshot(payload);
    renderActivitySnapshot(payload);
    loadedViews.add("macro-activity");
  } catch (error) {
    renderMacroActivityError(`No pude cargar Macro / Actividad (${error.message}).`);
  }
}

function activateView(viewKey) {
  const targetViewKey = viewKey === "genesis" ? "command-center" : viewKey;
  navLinks.forEach((link) => {
    link.classList.toggle("is-active", link.dataset.view === targetViewKey);
  });

  views.forEach((view) => {
    const isVisible = view.id === `view-${targetViewKey}`;
    view.classList.toggle("is-visible", isVisible);
  });

  viewTitle.textContent = titles[targetViewKey] || "Dashboard";
  currentViewKey = targetViewKey;
  genesisContext = resolveGenesisContext(targetViewKey);
  updateGenesisContext(genesisContext);

  if (targetViewKey === "command-center") {
    loadOperationalReliability();
    loadExecutiveQueue();
  }
  if (targetViewKey === "money-flow") {
    loadMoneyFlowSnapshot();
  }
  if (targetViewKey === "radar") {
    loadRadarSnapshot();
  }
  if (targetViewKey === "alerts") {
    loadAlertsSnapshot();
  }
  if (targetViewKey === "dependencies") {
    loadDependenciesSnapshot();
  }
  if (targetViewKey === "macro" || targetViewKey === "activity") {
    loadMacroActivitySnapshot();
  }
}

navLinks.forEach((link) => {
  link.addEventListener("click", () => activateView(link.dataset.view));
});

bindGenesisQueryForm();
bindGenesisChatForm();
activateView("command-center");
loadOperationalHealth();
