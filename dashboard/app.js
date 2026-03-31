const THEMES = [
  { id: "nocturne", label: "Nocturne" },
  { id: "voltage", label: "Voltage" },
  { id: "paper", label: "Paper" },
];
const EVENT_TYPES = ["node.update", "score.change", "task.execution", "log.append"];
const HUMAN_API_KEY_ROLES = ["viewer", "operator", "super_admin"];
const AUDIT_TIME_RANGES = {
  "1h": 1000 * 60 * 60,
  "24h": 1000 * 60 * 60 * 24,
  all: 0,
};
const VIEW_IDS = {
  OVERVIEW: "overview",
  API_KEYS: "api-keys",
  AUDIT_LOGS: "audit-logs",
};
const API_KEY_STORAGE_KEY = "privon-dashboard-api-key";
const STREAM_RECONNECT_DELAY_MS = 2500;
const AUDIT_REFRESH_INTERVAL_MS = 5000;

const state = {
  theme: localStorage.getItem("privon-theme") || "nocturne",
  apiKey: sessionStorage.getItem(API_KEY_STORAGE_KEY) || "",
  connected: false,
  eventSource: null,
  reconnectTimer: null,
  renewalTimer: null,
  auditRefreshTimer: null,
  view: VIEW_IDS.OVERVIEW,
  filterOptions: {
    shardIds: [],
    replicaGroups: [],
    eventTypes: [...EVENT_TYPES],
  },
  filters: {
    shardId: "",
    replicaGroup: "",
    events: [...EVENT_TYPES],
  },
  session: {
    role: "viewer",
    expiresAt: 0,
    keyExpiresAt: 0,
  },
  admin: {
    apiKeys: [],
    message: "",
    tone: "info",
    revealedKeys: {},
  },
  audit: {
    logs: [],
    filterOptions: {
      eventTypes: [],
    },
    filters: {
      eventType: "",
      timeRange: "24h",
    },
    message: "",
    tone: "info",
  },
  snapshot: {
    nodes: [],
    stats: {
      summary: {},
      replicaGroups: [],
      shardSummaries: [],
      recentExecutions: [],
    },
    logs: [],
  },
};

const themeSwitcher = document.getElementById("theme-switcher");
const summaryStrip = document.getElementById("summary-strip");
const nodeTable = document.getElementById("node-table");
const taskStream = document.getElementById("task-stream");
const logConsole = document.getElementById("log-console");
const laneList = document.getElementById("lane-list");
const replicaLanes = document.getElementById("replica-lanes");
const shardChart = document.getElementById("shard-chart");
const lastUpdated = document.getElementById("last-updated");
const authShell = document.getElementById("auth-shell");
const workspace = document.getElementById("workspace");
const authForm = document.getElementById("auth-form");
const apiKeyInput = document.getElementById("api-key-input");
const authStatus = document.getElementById("auth-status");
const connectButton = document.getElementById("connect-button");
const connectionPill = document.getElementById("connection-pill");
const shardFilter = document.getElementById("shard-filter");
const replicaFilter = document.getElementById("replica-filter");
const eventFilterBar = document.getElementById("event-filter-bar");
const clearFiltersButton = document.getElementById("clear-filters-button");
const sessionRole = document.getElementById("session-role");
const sessionExpiry = document.getElementById("session-expiry");
const renewButton = document.getElementById("renew-button");
const logoutButton = document.getElementById("logout-button");
const focusCopy = document.getElementById("focus-copy");
const viewButtons = Array.from(document.querySelectorAll("[data-view-button]"));
const viewPanels = Array.from(document.querySelectorAll("[data-view-panel]"));
const apiKeysNavItem = document.getElementById("api-keys-nav-item");
const auditLogsNavItem = document.getElementById("audit-logs-nav-item");
const adminMessage = document.getElementById("admin-message");
const apiKeyList = document.getElementById("api-key-list");
const apiKeyCreateForm = document.getElementById("api-key-create-form");
const apiKeyRoleInput = document.getElementById("api-key-role");
const apiKeyExpiresAtInput = document.getElementById("api-key-expires-at");
const createApiKeyButton = document.getElementById("create-api-key-button");
const refreshApiKeysButton = document.getElementById("refresh-api-keys-button");
const auditMessage = document.getElementById("audit-message");
const auditEventFilter = document.getElementById("audit-event-filter");
const auditTimeFilter = document.getElementById("audit-time-filter");
const refreshAuditLogsButton = document.getElementById("refresh-audit-logs-button");
const auditLogTable = document.getElementById("audit-log-table");

function formatRelativeTime(timestamp) {
  if (!timestamp) {
    return "waiting";
  }

  const deltaSeconds = Math.max(0, Math.round((Date.now() - timestamp) / 1000));

  if (deltaSeconds < 5) {
    return "just now";
  }

  if (deltaSeconds < 60) {
    return `${deltaSeconds}s ago`;
  }

  const minutes = Math.round(deltaSeconds / 60);
  return `${minutes}m ago`;
}

function formatFutureTime(timestamp) {
  if (!timestamp) {
    return "pending";
  }

  const deltaSeconds = Math.max(0, Math.round((timestamp - Date.now()) / 1000));

  if (deltaSeconds < 60) {
    return `${deltaSeconds}s`;
  }

  const minutes = Math.round(deltaSeconds / 60);
  return `${minutes}m`;
}

function formatDateTime(timestamp) {
  if (!timestamp) {
    return "pending";
  }

  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(timestamp));
}

function toDatetimeLocalValue(timestamp) {
  const date = new Date(timestamp);
  const pad = (value) => String(value).padStart(2, "0");

  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function humanizeRole(role) {
  return String(role || "viewer").replaceAll("_", " ");
}

function humanizeEventType(eventType) {
  return String(eventType || "activity")
    .replaceAll(".", " ")
    .replaceAll("_", " ");
}

function isSuperAdmin() {
  return state.session.role === "super_admin";
}

function maskApiKey(key) {
  const value = String(key || "").trim();

  if (!value) {
    return "****";
  }

  return `****${value.slice(-4)}`;
}

function createEmptyState(message) {
  return `<div class="empty-state">${message}</div>`;
}

function buildFilterQuery(includeEvents = false) {
  const query = new URLSearchParams();

  if (state.filters.shardId) {
    query.set("shardId", state.filters.shardId);
  }

  if (state.filters.replicaGroup) {
    query.set("replicaGroup", state.filters.replicaGroup);
  }

  if (includeEvents && state.filters.events.length > 0) {
    query.set("events", state.filters.events.join(","));
  }

  const serialized = query.toString();
  return serialized ? `?${serialized}` : "";
}

function applyTheme(themeId) {
  state.theme = themeId;
  localStorage.setItem("privon-theme", themeId);
  document.body.dataset.theme = themeId;
  renderThemeButtons();
}

function renderThemeButtons() {
  themeSwitcher.innerHTML = THEMES.map((theme) => `
    <button
      class="theme-button ${state.theme === theme.id ? "active" : ""}"
      type="button"
      data-theme="${theme.id}"
    >${theme.label}</button>
  `).join("");

  themeSwitcher.querySelectorAll("button").forEach((button) => {
    button.addEventListener("click", () => applyTheme(button.dataset.theme));
  });
}

function statusClass(status) {
  return `status-${status || "inactive"}`;
}

function setLastUpdated(timestamp = Date.now()) {
  lastUpdated.textContent = `updated ${formatRelativeTime(timestamp)}`;
}

function setConnectionState(mode, message) {
  const labels = {
    locked: "locked",
    connecting: "connecting",
    live: "live stream",
    degraded: "reconnecting",
  };
  const className = mode === "live"
    ? "status-active"
    : mode === "connecting" || mode === "degraded"
      ? "status-probation"
      : "status-inactive";

  connectionPill.className = `connection-pill ${className}`;
  connectionPill.textContent = labels[mode] || mode;

  if (message) {
    authStatus.textContent = message;
  }
}

function updateVisibility() {
  const hasSession = state.connected;
  workspace.hidden = !hasSession;
  authShell.hidden = hasSession;
}

function renderSession() {
  sessionRole.textContent = humanizeRole(state.session.role || "viewer");
  sessionExpiry.textContent = state.session.expiresAt
    ? `${formatFutureTime(state.session.expiresAt)} remaining`
    : "pending";
}

function syncViewPanels() {
  if (!isSuperAdmin() && [VIEW_IDS.API_KEYS, VIEW_IDS.AUDIT_LOGS].includes(state.view)) {
    state.view = VIEW_IDS.OVERVIEW;
  }

  apiKeysNavItem.hidden = !isSuperAdmin();
  auditLogsNavItem.hidden = !isSuperAdmin();

  viewButtons.forEach((button) => {
    const isActive = button.dataset.viewButton === state.view;
    button.classList.toggle("active", isActive);
  });

  viewPanels.forEach((panel) => {
    panel.hidden = panel.dataset.viewPanel !== state.view;
  });
}

function setAdminMessage(message = "", tone = "info") {
  state.admin.message = message;
  state.admin.tone = tone;

  if (!message) {
    adminMessage.hidden = true;
    adminMessage.textContent = "";
    adminMessage.className = "message-banner";
    return;
  }

  adminMessage.hidden = false;
  adminMessage.textContent = message;
  adminMessage.className = `message-banner tone-${tone}`;
}

function setAuditMessage(message = "", tone = "info") {
  state.audit.message = message;
  state.audit.tone = tone;

  if (!message) {
    auditMessage.hidden = true;
    auditMessage.textContent = "";
    auditMessage.className = "message-banner";
    return;
  }

  auditMessage.hidden = false;
  auditMessage.textContent = message;
  auditMessage.className = `message-banner tone-${tone}`;
}

function renderSummary(stats) {
  const summary = stats.summary || {};

  summaryStrip.innerHTML = `
    <div class="metric-row">
      <article class="metric-chip">
        <p class="mini-label">Tracked nodes</p>
        <strong>${summary.totalNodes || 0}</strong>
      </article>
      <article class="metric-chip">
        <p class="mini-label">Active lanes</p>
        <strong>${summary.activeNodes || 0}</strong>
      </article>
      <article class="metric-chip">
        <p class="mini-label">Probation lanes</p>
        <strong>${summary.probationNodes || 0}</strong>
      </article>
    </div>
    <div class="metric-row">
      <article class="metric-chip">
        <p class="mini-label">Tasks handled</p>
        <strong>${summary.totalTasksHandled || 0}</strong>
      </article>
      <article class="metric-chip">
        <p class="mini-label">Average score</p>
        <strong>${summary.averageScore || 0}</strong>
      </article>
      <article class="metric-chip">
        <p class="mini-label">Recent failures</p>
        <strong>${summary.recentFailures || 0}</strong>
      </article>
    </div>
  `;
}

function renderNodes(nodes) {
  if (!nodes.length) {
    nodeTable.innerHTML = createEmptyState("No nodes match the current drill-down filters.");
    return;
  }

  nodeTable.innerHTML = nodes.map((node) => {
    const scoreWidth = Math.max(0, Math.min(100, Math.round((node.score / 200) * 100)));
    return `
      <article class="node-row">
        <div class="meta-stack">
          <strong class="node-url">${node.url}</strong>
          <span class="meta">${node.shardId} · ${node.replicaGroup}</span>
        </div>
        <div class="meta-stack">
          <span class="status-pill ${statusClass(node.status)}">${node.status}</span>
          <span class="meta">last seen ${formatRelativeTime(node.lastSeen)}</span>
        </div>
        <div class="meta-stack">
          <span class="score-pill">${node.score}
            <span class="score-track"><span style="width:${scoreWidth}%"></span></span>
          </span>
          <span class="meta">score / 200</span>
        </div>
        <div class="meta-stack">
          <strong>${node.tasksHandled}</strong>
          <span class="meta">tasks handled</span>
        </div>
        <div class="meta-stack">
          <strong>${node.successfulTasks}/${node.failedTasks}</strong>
          <span class="meta">success / fail</span>
        </div>
      </article>
    `;
  }).join("");
}

function renderTasks(executions) {
  if (!executions.length) {
    taskStream.innerHTML = createEmptyState("No fragment executions match the current filters.");
    return;
  }

  taskStream.innerHTML = executions.map((task) => `
    <article class="task-item">
      <div class="meta-stack">
        <span class="status-pill ${task.status === "verified" ? "status-active" : "status-probation"}">${task.status}</span>
        <strong>Fragment ${task.fragmentIndex}: ${task.fragment || "n/a"}</strong>
        <span class="meta">job ${task.jobId} · ${task.replicaGroup}</span>
      </div>
      <div class="task-grid">
        <div>
          <p class="mini-label">Verified nodes</p>
          <div class="inline-list">${(task.nodes || []).map((node) => `<span class="inline-tag">${node}</span>`).join("") || '<span class="meta">none</span>'}</div>
        </div>
        <div>
          <p class="mini-label">Probation lane</p>
          <div class="inline-list">${task.probation?.attempted ? `<span class="inline-tag">${task.probation.nodeUrl}</span>` : '<span class="meta">not sampled</span>'}</div>
        </div>
      </div>
    </article>
  `).join("");
}

function renderLogs(logs) {
  if (!logs.length) {
    logConsole.innerHTML = createEmptyState("No coordinator logs match the current filters.");
    return;
  }

  logConsole.innerHTML = logs.map((entry) => `
    <article class="log-item">
      <span class="log-level level-${entry.level}"></span>
      <div class="meta-stack">
        <span class="log-meta">${entry.type} · ${formatRelativeTime(entry.timestamp)}</span>
        <strong>${entry.message}</strong>
      </div>
    </article>
  `).join("");
}

function setShardFilter(shardId) {
  state.filters.shardId = shardId;
  applyFilters();
}

function setReplicaFilter(replicaGroup) {
  state.filters.replicaGroup = replicaGroup;
  applyFilters();
}

function attachDrilldownHandlers() {
  document.querySelectorAll("[data-shard-id]").forEach((button) => {
    button.addEventListener("click", () => {
      setShardFilter(button.dataset.shardId || "");
    });
  });

  document.querySelectorAll("[data-replica-group]").forEach((button) => {
    button.addEventListener("click", () => {
      setReplicaFilter(button.dataset.replicaGroup || "");
    });
  });
}

function renderReplicaHealth(replicaGroups) {
  if (!replicaGroups.length) {
    replicaLanes.innerHTML = createEmptyState("Replica groups will appear once nodes register.");
    laneList.innerHTML = createEmptyState("Waiting for replica metadata.");
    return;
  }

  replicaLanes.innerHTML = replicaGroups.map((group) => {
    const healthWidth = Math.max(10, Math.round((group.activeNodes / Math.max(1, group.totalNodes)) * 100));
    return `
      <button class="lane-pill interactive-card" type="button" data-replica-group="${group.key}">
        <div class="lane-head">
          <strong>${group.key}</strong>
          <span class="meta">${group.activeNodes}/${group.totalNodes} active</span>
        </div>
        <div class="lane-bar"><span style="width:${healthWidth}%"></span></div>
        <div class="lane-meta-grid">
          <span class="meta">avg score ${group.averageScore}</span>
          <span class="meta">${group.tasksHandled} tasks</span>
        </div>
      </button>
    `;
  }).join("");

  laneList.innerHTML = replicaGroups.map((group) => `
    <button class="lane-item interactive-card" type="button" data-replica-group="${group.key}">
      <div class="lane-head">
        <strong>${group.key}</strong>
        <span class="meta">${group.probationNodes} probation · ${group.inactiveNodes} inactive</span>
      </div>
      <div class="inline-list">${group.members.map((member) => `<span class="inline-tag ${statusClass(member.status)}">${member.shardId}</span>`).join("")}</div>
    </button>
  `).join("");
}

function renderShardCharts(shardSummaries) {
  if (!shardSummaries.length) {
    shardChart.innerHTML = createEmptyState("Shard charts appear once nodes report shard metadata.");
    return;
  }

  const maxTasks = Math.max(...shardSummaries.map((summary) => summary.tasksHandled || 0), 1);

  shardChart.innerHTML = shardSummaries.map((summary) => {
    const loadWidth = Math.max(8, Math.round(((summary.tasksHandled || 0) / maxTasks) * 100));
    const scoreWidth = Math.max(8, Math.round((summary.averageScore / 200) * 100));
    return `
      <button class="shard-card interactive-card" type="button" data-shard-id="${summary.key}">
        <div class="lane-head">
          <strong>${summary.key}</strong>
          <span class="meta">${summary.totalNodes} node(s)</span>
        </div>
        <div class="chart-row">
          <span class="mini-label">task share</span>
          <div class="chart-track load"><span style="width:${loadWidth}%"></span></div>
          <span class="meta">${summary.tasksHandled}</span>
        </div>
        <div class="chart-row">
          <span class="mini-label">score</span>
          <div class="chart-track score"><span style="width:${scoreWidth}%"></span></div>
          <span class="meta">${summary.averageScore}</span>
        </div>
      </button>
    `;
  }).join("");
}

function renderFocusCard(stats) {
  const hasShard = Boolean(state.filters.shardId);
  const hasReplica = Boolean(state.filters.replicaGroup);

  if (!hasShard && !hasReplica) {
    focusCopy.innerHTML = `
      <p>Showing the full network surface.</p>
      <p class="meta">Use the controls above or click a shard or replica card to narrow the live stream.</p>
    `;
    return;
  }

  focusCopy.innerHTML = `
    <p><strong>Focused scope</strong></p>
    <p>${hasShard ? `Shard ${state.filters.shardId}` : "All shards"} · ${hasReplica ? `Replica ${state.filters.replicaGroup}` : "All replicas"}</p>
    <p class="meta">${stats.summary.totalNodes || 0} nodes, ${stats.summary.totalTasksHandled || 0} tasks, ${stats.summary.activeNodes || 0} active lanes in this view.</p>
  `;
}

function renderFilters() {
  const shardOptions = ["", ...state.filterOptions.shardIds.filter((value) => value !== state.filters.shardId)];
  shardFilter.innerHTML = shardOptions.map((value) => `
    <option value="${value}">${value || "All shards"}</option>
  `).join("");
  shardFilter.value = state.filters.shardId;

  const replicaOptions = ["", ...state.filterOptions.replicaGroups.filter((value) => value !== state.filters.replicaGroup)];
  replicaFilter.innerHTML = replicaOptions.map((value) => `
    <option value="${value}">${value || "All replica groups"}</option>
  `).join("");
  replicaFilter.value = state.filters.replicaGroup;

  eventFilterBar.innerHTML = EVENT_TYPES.map((eventName) => `
    <button
      class="event-chip ${state.filters.events.includes(eventName) ? "active" : ""}"
      data-event-name="${eventName}"
      type="button"
    >${eventName}</button>
  `).join("");

  eventFilterBar.querySelectorAll("button").forEach((button) => {
    button.addEventListener("click", () => {
      const eventName = button.dataset.eventName;

      if (state.filters.events.includes(eventName)) {
        if (state.filters.events.length === 1) {
          return;
        }

        state.filters.events = state.filters.events.filter((entry) => entry !== eventName);
      } else {
        state.filters.events = [...state.filters.events, eventName];
      }

      renderFilters();
      connectStream();
    });
  });
}
function renderApiKeys() {
  if (!isSuperAdmin()) {
    apiKeyList.innerHTML = createEmptyState("Only super_admin sessions can manage API keys.");
    return;
  }

  if (!state.admin.apiKeys.length) {
    apiKeyList.innerHTML = createEmptyState("No managed keys yet. Create one to grant dashboard or node access.");
    return;
  }

  apiKeyList.innerHTML = state.admin.apiKeys.map((record) => {
    const isRevealed = Boolean(state.admin.revealedKeys[record.key]);
    const keyLabel = isRevealed ? record.key : maskApiKey(record.key);
    const isActive = record.status === "active";

    return `
      <article class="api-key-row">
        <div class="api-key-main">
          <button class="key-visibility-button" type="button" data-key-visibility="${record.key}">
            ${keyLabel}
          </button>
          <span class="meta">${isRevealed ? "click to mask" : "click to reveal"}</span>
        </div>
        <div class="meta-stack">
          <span class="mini-label">Role</span>
          <strong>${humanizeRole(record.role)}</strong>
        </div>
        <div class="meta-stack">
          <span class="mini-label">Created</span>
          <strong>${formatDateTime(record.createdAt)}</strong>
        </div>
        <div class="meta-stack">
          <span class="mini-label">Expires</span>
          <strong>${formatDateTime(record.expiresAt)}</strong>
        </div>
        <div class="meta-stack">
          <span class="status-pill ${statusClass(record.status === "active" ? "active" : "inactive")}">${record.status}</span>
          <span class="meta">credential state</span>
        </div>
        <div class="api-key-actions">
          <button
            class="ghost-button revoke-button"
            type="button"
            data-revoke-key="${record.key}"
            ${isActive ? "" : "disabled"}
          >Revoke</button>
        </div>
      </article>
    `;
  }).join("");

  apiKeyList.querySelectorAll("[data-key-visibility]").forEach((button) => {
    button.addEventListener("click", () => {
      const key = button.dataset.keyVisibility;
      state.admin.revealedKeys[key] = !state.admin.revealedKeys[key];
      renderApiKeys();
    });
  });

  apiKeyList.querySelectorAll("[data-revoke-key]").forEach((button) => {
    button.addEventListener("click", async () => {
      await revokeApiKey(button.dataset.revokeKey);
    });
  });
}


function getAuditSinceForRange(timeRange) {
  const offset = AUDIT_TIME_RANGES[timeRange] ?? AUDIT_TIME_RANGES["24h"];
  return offset > 0 ? Date.now() - offset : 0;
}

function renderAuditFilters() {
  if (!isSuperAdmin()) {
    auditEventFilter.innerHTML = '<option value="">All event types</option>';
    auditTimeFilter.value = "24h";
    return;
  }

  const eventTypes = ["", ...(state.audit.filterOptions.eventTypes || [])
    .filter((value) => value !== state.audit.filters.eventType)];

  auditEventFilter.innerHTML = eventTypes.map((eventType) => `
    <option value="${eventType}">${eventType || "All event types"}</option>
  `).join("");
  auditEventFilter.value = state.audit.filters.eventType;
  auditTimeFilter.value = state.audit.filters.timeRange;
}

function renderAuditLogs() {
  if (!isSuperAdmin()) {
    auditLogTable.innerHTML = createEmptyState("Only super_admin sessions can view audit logs.");
    return;
  }

  if (!state.audit.logs.length) {
    auditLogTable.innerHTML = createEmptyState("No audit activity matched the current filter set.");
    return;
  }

  auditLogTable.innerHTML = `
    <div class="audit-row audit-header">
      <span>Time</span>
      <span>Event Type</span>
      <span>Actor</span>
      <span>Summary</span>
    </div>
    ${state.audit.logs.map((entry) => `
      <article class="audit-row severity-${entry.severity || "info"}">
        <div class="audit-cell">
          <strong>${formatDateTime(entry.timestamp)}</strong>
          <span class="meta">${formatRelativeTime(entry.timestamp)}</span>
        </div>
        <div class="audit-cell">
          <span class="inline-tag">${entry.eventType}</span>
          <span class="meta">${humanizeEventType(entry.eventType)}</span>
        </div>
        <div class="audit-cell">
          <strong>${entry.actor || "system"}</strong>
        </div>
        <div class="audit-cell">
          <strong>${entry.summary || "No summary available."}</strong>
          <span class="meta">${Object.entries(entry.details || {})
            .map(([key, value]) => `${key}: ${value}`)
            .join(" · ") || "No additional details."}</span>
        </div>
      </article>
    `).join("")}
  `;
}
function render(snapshot) {
  const nodes = snapshot.nodes || [];
  const stats = snapshot.stats || {};
  const logs = snapshot.logs || [];

  renderSession();
  syncViewPanels();
  renderFilters();
  renderSummary(stats);
  renderNodes(nodes);
  renderTasks(stats.recentExecutions || []);
  renderLogs(logs);
  renderReplicaHealth(stats.replicaGroups || []);
  renderShardCharts(stats.shardSummaries || []);
  renderFocusCard(stats);
  renderApiKeys();
  renderAuditFilters();
  renderAuditLogs();
  attachDrilldownHandlers();
  setLastUpdated(stats.lastUpdatedAt || Date.now());
}

function updateSnapshot(partial) {
  state.snapshot = {
    nodes: partial.nodes || state.snapshot.nodes || [],
    stats: partial.stats || state.snapshot.stats || {},
    logs: partial.logs || state.snapshot.logs || [],
  };

  if (partial.filters) {
    state.filterOptions = {
      ...state.filterOptions,
      ...partial.filters,
    };
  }

  render(state.snapshot);
}

function clearTimers() {
  if (state.reconnectTimer) {
    window.clearTimeout(state.reconnectTimer);
    state.reconnectTimer = null;
  }

  if (state.renewalTimer) {
    window.clearTimeout(state.renewalTimer);
    state.renewalTimer = null;
  }

  if (state.auditRefreshTimer) {
    window.clearInterval(state.auditRefreshTimer);
    state.auditRefreshTimer = null;
  }
}

function scheduleReconnect() {
  if (!state.connected || state.reconnectTimer) {
    return;
  }

  state.reconnectTimer = window.setTimeout(() => {
    state.reconnectTimer = null;
    connectStream();
  }, STREAM_RECONNECT_DELAY_MS);
}

function scheduleSessionRenewal() {
  if (!state.connected || !state.session.expiresAt) {
    return;
  }

  if (state.renewalTimer) {
    window.clearTimeout(state.renewalTimer);
  }

  const msRemaining = state.session.expiresAt - Date.now();
  const leadTime = Math.min(60000, Math.max(5000, Math.floor(msRemaining / 2)));
  const delay = Math.max(1000, msRemaining - leadTime);

  state.renewalTimer = window.setTimeout(() => {
    renewSession(true);
  }, delay);
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, {
    credentials: "same-origin",
    ...options,
  });

  if (response.status === 401) {
    throw new Error("Unauthorized");
  }

  if (response.status === 403) {
    throw new Error("Forbidden");
  }

  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }

  return response.json();
}

async function loadApiKeys(options = {}) {
  if (!isSuperAdmin()) {
    state.admin.apiKeys = [];
    renderApiKeys();
    return;
  }

  const silent = options.silent ?? false;

  try {
    const response = await fetchJson("/admin/api-keys");
    state.admin.apiKeys = response.apiKeys || [];
    renderApiKeys();

    if (!silent) {
      setAdminMessage(`Loaded ${state.admin.apiKeys.length} managed key${state.admin.apiKeys.length === 1 ? "" : "s"}.`, "success");
    }
  } catch (error) {
    renderApiKeys();

    if (!silent) {
      setAdminMessage(
        error.message === "Forbidden"
          ? "Only super_admin sessions can manage API keys."
          : `Unable to load API keys: ${error.message}`,
        "error",
      );
    }

    if (error.message === "Unauthorized") {
      await logout(false);
    }
  }
}


function buildAuditQuery() {
  const query = new URLSearchParams();
  const since = getAuditSinceForRange(state.audit.filters.timeRange);

  if (state.audit.filters.eventType) {
    query.set("eventType", state.audit.filters.eventType);
  }

  if (since > 0) {
    query.set("since", String(since));
  }

  query.set("limit", "150");
  return `?${query.toString()}`;
}

async function loadAuditLogs(options = {}) {
  if (!isSuperAdmin()) {
    state.audit.logs = [];
    state.audit.filterOptions = { eventTypes: [] };
    renderAuditFilters();
    renderAuditLogs();
    return;
  }

  const silent = options.silent ?? false;

  try {
    const response = await fetchJson(`/admin/audit-logs${buildAuditQuery()}`);
    state.audit.logs = response.auditLogs || [];
    state.audit.filterOptions = {
      eventTypes: response.filterOptions?.eventTypes || [],
    };
    renderAuditFilters();
    renderAuditLogs();

    if (!silent) {
      setAuditMessage(`Loaded ${state.audit.logs.length} audit event${state.audit.logs.length === 1 ? "" : "s"}.`, "success");
    }
  } catch (error) {
    renderAuditFilters();
    renderAuditLogs();

    if (!silent) {
      setAuditMessage(
        error.message === "Forbidden"
          ? "Only super_admin sessions can view audit logs."
          : `Unable to load audit logs: ${error.message}`,
        "error",
      );
    }

    if (error.message === "Unauthorized") {
      await logout(false);
    }
  }
}

function startAuditRefresh() {
  if (!isSuperAdmin() || state.view !== VIEW_IDS.AUDIT_LOGS) {
    return;
  }

  if (state.auditRefreshTimer) {
    window.clearInterval(state.auditRefreshTimer);
  }

  state.auditRefreshTimer = window.setInterval(() => {
    loadAuditLogs({ silent: true }).catch((error) => console.error(error));
  }, AUDIT_REFRESH_INTERVAL_MS);
}

function stopAuditRefresh() {
  if (state.auditRefreshTimer) {
    window.clearInterval(state.auditRefreshTimer);
    state.auditRefreshTimer = null;
  }
}
async function loadSnapshot() {
  const query = buildFilterQuery(false);
  const [meta, nodes, stats, logs] = await Promise.all([
    fetchJson("/dashboard/meta"),
    fetchJson(`/dashboard/nodes${query}`),
    fetchJson(`/dashboard/stats${query}`),
    fetchJson(`/dashboard/logs${query}${query ? "&" : "?"}limit=24`),
  ]);

  state.session = meta.session || state.session;
  state.filterOptions = {
    ...state.filterOptions,
    ...(meta.filters || {}),
  };

  updateSnapshot({
    nodes: nodes.nodes || [],
    stats,
    logs: logs.logs || [],
    filters: meta.filters || {},
  });

  if (isSuperAdmin() && state.view === VIEW_IDS.API_KEYS) {
    await loadApiKeys({ silent: true });
  }

  if (isSuperAdmin() && state.view === VIEW_IDS.AUDIT_LOGS) {
    await loadAuditLogs({ silent: true });
  }

  scheduleSessionRenewal();
}

function handleStreamEvent(eventName, payload) {
  if (eventName === "snapshot") {
    updateSnapshot(payload);
    return;
  }

  if (eventName === "node.update") {
    updateSnapshot({
      nodes: payload.nodes || state.snapshot.nodes,
      stats: payload.stats || state.snapshot.stats,
      filters: payload.filters,
    });
    return;
  }

  if (eventName === "task.execution") {
    updateSnapshot({
      stats: {
        ...state.snapshot.stats,
        ...payload.stats,
        recentExecutions: payload.recentExecutions || state.snapshot.stats.recentExecutions || [],
      },
    });
    return;
  }

  if (eventName === "log.append") {
    updateSnapshot({
      logs: payload.logs || state.snapshot.logs,
    });

    if (isSuperAdmin() && state.view === VIEW_IDS.AUDIT_LOGS) {
      loadAuditLogs({ silent: true }).catch((error) => console.error(error));
    }
    return;
  }

  if (eventName === "score.change") {
    updateSnapshot({
      stats: payload.stats || state.snapshot.stats,
    });
  }
}

function connectStream() {
  if (!state.connected) {
    return;
  }

  state.eventSource?.close();
  if (state.reconnectTimer) {
    window.clearTimeout(state.reconnectTimer);
    state.reconnectTimer = null;
  }
  setConnectionState("connecting", "Secure session established. Opening filtered live stream.");

  const eventSource = new EventSource(`/dashboard/stream${buildFilterQuery(true)}`);
  state.eventSource = eventSource;

  eventSource.addEventListener("open", () => {
    setConnectionState("live", "Streaming filtered node, task, and score updates live.");
  });

  ["snapshot", ...EVENT_TYPES].forEach((eventName) => {
    eventSource.addEventListener(eventName, (event) => {
      try {
        handleStreamEvent(eventName, JSON.parse(event.data));
      } catch (error) {
        console.error(`Failed to parse ${eventName}`, error);
      }
    });
  });

  eventSource.onerror = () => {
    setConnectionState("degraded", "Stream interrupted. Reconnecting.");
    eventSource.close();
    if (state.eventSource === eventSource) {
      state.eventSource = null;
    }
    scheduleReconnect();
  };
}

async function establishSession(apiKey) {
  const trimmedApiKey = String(apiKey || "").trim();

  if (!trimmedApiKey) {
    throw new Error("API key is required.");
  }

  setConnectionState("connecting", "Authorizing dashboard session.");
  connectButton.disabled = true;

  try {
    const response = await fetchJson("/dashboard/session", {
      method: "POST",
      headers: {
        authorization: `Bearer ${trimmedApiKey}`,
      },
    });

    sessionStorage.setItem(API_KEY_STORAGE_KEY, trimmedApiKey);
    state.apiKey = trimmedApiKey;
    state.session = response.session || state.session;
    state.connected = true;
    updateVisibility();
    await loadSnapshot();
    if (isSuperAdmin()) {
      await loadApiKeys({ silent: true });
      if (state.view === VIEW_IDS.AUDIT_LOGS) {
        await loadAuditLogs({ silent: true });
        startAuditRefresh();
      }
    }
    connectStream();
  } finally {
    connectButton.disabled = false;
  }
}
async function renewSession(background = false) {
  try {
    const response = await fetchJson("/dashboard/session/renew", {
      method: "POST",
    });
    state.session = response.session || state.session;
    renderSession();
    scheduleSessionRenewal();
  } catch (error) {
    if (background && state.apiKey) {
      try {
        await establishSession(state.apiKey);
        return;
      } catch (sessionError) {
        console.error(sessionError);
      }
    }

    if (!background) {
      authStatus.textContent = error.message === "Unauthorized"
        ? "Session renewal failed. Log in again."
        : error.message;
    }

    await logout(false);
  }
}

async function logout(callApi = true) {
  clearTimers();
  state.eventSource?.close();
  state.eventSource = null;

  if (callApi) {
    try {
      await fetchJson("/dashboard/logout", { method: "POST" });
    } catch (error) {
      console.error(error);
    }
  }

  state.connected = false;
  state.view = VIEW_IDS.OVERVIEW;
  state.session = {
    role: "viewer",
    expiresAt: 0,
    keyExpiresAt: 0,
  };
  state.admin = {
    apiKeys: [],
    message: "",
    tone: "info",
    revealedKeys: {},
  };
  state.audit = {
    logs: [],
    filterOptions: {
      eventTypes: [],
    },
    filters: {
      eventType: "",
      timeRange: "24h",
    },
    message: "",
    tone: "info",
  };
  state.snapshot = {
    nodes: [],
    stats: { summary: {}, replicaGroups: [], shardSummaries: [], recentExecutions: [] },
    logs: [],
  };
  sessionStorage.removeItem(API_KEY_STORAGE_KEY);
  state.apiKey = "";
  updateVisibility();
  renderSession();
  syncViewPanels();
  setAdminMessage("");
  setAuditMessage("");
  renderApiKeys();
  renderAuditFilters();
  renderAuditLogs();
  setConnectionState("locked", "Dashboard session cleared.");
}

async function applyFilters() {
  if (!state.connected) {
    return;
  }

  await loadSnapshot();
  connectStream();
}

async function switchView(viewId) {
  if (viewId === VIEW_IDS.API_KEYS && !isSuperAdmin()) {
    setAdminMessage("Only super_admin sessions can manage API keys.", "error");
    state.view = VIEW_IDS.OVERVIEW;
    syncViewPanels();
    return;
  }

  if (viewId === VIEW_IDS.AUDIT_LOGS && !isSuperAdmin()) {
    setAuditMessage("Only super_admin sessions can view audit logs.", "error");
    state.view = VIEW_IDS.OVERVIEW;
    syncViewPanels();
    return;
  }

  state.view = viewId;
  syncViewPanels();
  stopAuditRefresh();

  if (state.view === VIEW_IDS.API_KEYS) {
    await loadApiKeys({ silent: true });
  }

  if (state.view === VIEW_IDS.AUDIT_LOGS) {
    await loadAuditLogs({ silent: true });
    startAuditRefresh();
  }
}

async function createApiKey(event) {
  event.preventDefault();

  if (!isSuperAdmin()) {
    setAdminMessage("Only super_admin sessions can create API keys.", "error");
    return;
  }

  const role = apiKeyRoleInput.value;
  const expiresAtValue = apiKeyExpiresAtInput.value;
  const expiresAt = new Date(expiresAtValue).getTime();

  if (!HUMAN_API_KEY_ROLES.includes(role)) {
    setAdminMessage("Choose a valid dashboard role before creating a key.", "error");
    return;
  }

  if (!Number.isFinite(expiresAt) || expiresAt <= Date.now()) {
    setAdminMessage("Choose an expiration date in the future.", "error");
    return;
  }

  createApiKeyButton.disabled = true;

  try {
    const response = await fetchJson("/admin/api-keys/create", {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({
        role,
        expiresAt,
      }),
    });

    state.admin.revealedKeys = {
      ...state.admin.revealedKeys,
      [response.apiKey.key]: false,
    };

    await loadApiKeys({ silent: true });
    setAdminMessage(`Created ${humanizeRole(role)} key ${maskApiKey(response.apiKey.key)}.`, "success");
    apiKeyRoleInput.value = "viewer";
    apiKeyExpiresAtInput.value = toDatetimeLocalValue(Date.now() + (1000 * 60 * 60 * 24 * 30));
  } catch (error) {
    setAdminMessage(
      error.message === "Forbidden"
        ? "Only super_admin sessions can create API keys."
        : `Unable to create API key: ${error.message}`,
      "error",
    );
  } finally {
    createApiKeyButton.disabled = false;
  }
}

async function revokeApiKey(key) {
  if (!key) {
    return;
  }

  try {
    const response = await fetchJson("/admin/api-keys/revoke", {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ key }),
    });

    state.admin.apiKeys = state.admin.apiKeys.map((record) => (
      record.key === key ? response.apiKey : record
    ));
    renderApiKeys();
    setAdminMessage(`Revoked key ${maskApiKey(key)}.`, "success");
  } catch (error) {
    setAdminMessage(`Unable to revoke API key: ${error.message}`, "error");
  }
}

async function bootstrap() {
  applyTheme(state.theme);
  renderThemeButtons();
  renderFilters();
  renderSession();
  syncViewPanels();
  setAdminMessage("");
  setAuditMessage("");
  apiKeyRoleInput.value = "viewer";
  apiKeyExpiresAtInput.value = toDatetimeLocalValue(Date.now() + (1000 * 60 * 60 * 24 * 30));
  renderAuditFilters();
  renderAuditLogs();

  if (state.apiKey) {
    apiKeyInput.value = state.apiKey;
  }

  try {
    state.connected = true;
    updateVisibility();
    await loadSnapshot();
    if (isSuperAdmin()) {
      await loadApiKeys({ silent: true });
      if (state.view === VIEW_IDS.AUDIT_LOGS) {
        await loadAuditLogs({ silent: true });
        startAuditRefresh();
      }
    }
    connectStream();
    return;
  } catch (error) {
    state.connected = false;
    updateVisibility();

    if (error.message !== "Unauthorized") {
      authStatus.textContent = `Initial dashboard check failed: ${error.message}`;
      return;
    }
  }

  if (!state.apiKey) {
    setConnectionState("locked", "Waiting for a valid dashboard session.");
    return;
  }

  try {
    await establishSession(state.apiKey);
  } catch (error) {
    state.connected = false;
    updateVisibility();
    setConnectionState("locked", error.message === "Unauthorized"
      ? "Stored API key was rejected. Enter a fresh key."
      : `Unable to open session: ${error.message}`);
  }
}

authForm.addEventListener("submit", async (event) => {
  event.preventDefault();

  try {
    await establishSession(apiKeyInput.value);
  } catch (error) {
    state.connected = false;
    updateVisibility();
    setConnectionState("locked", error.message === "Unauthorized"
      ? "API key rejected. Check the coordinator key and try again."
      : error.message);
  }
});

viewButtons.forEach((button) => {
  button.addEventListener("click", async () => {
    await switchView(button.dataset.viewButton);
  });
});

shardFilter.addEventListener("change", async () => {
  state.filters.shardId = shardFilter.value;
  await applyFilters();
});

replicaFilter.addEventListener("change", async () => {
  state.filters.replicaGroup = replicaFilter.value;
  await applyFilters();
});

clearFiltersButton.addEventListener("click", async () => {
  state.filters.shardId = "";
  state.filters.replicaGroup = "";
  state.filters.events = [...EVENT_TYPES];
  await applyFilters();
});

renewButton.addEventListener("click", async () => {
  await renewSession(false);
});

logoutButton.addEventListener("click", async () => {
  await logout(true);
});

apiKeyCreateForm.addEventListener("submit", async (event) => {
  await createApiKey(event);
});

refreshApiKeysButton.addEventListener("click", async () => {
  await loadApiKeys({ silent: false });
});

auditEventFilter.addEventListener("change", async () => {
  state.audit.filters.eventType = auditEventFilter.value;
  await loadAuditLogs({ silent: true });
});

auditTimeFilter.addEventListener("change", async () => {
  state.audit.filters.timeRange = auditTimeFilter.value;
  await loadAuditLogs({ silent: true });
});

refreshAuditLogsButton.addEventListener("click", async () => {
  await loadAuditLogs({ silent: false });
});

window.addEventListener("beforeunload", () => {
  clearTimers();
  state.eventSource?.close();
});

bootstrap();
