const THEMES = [
  { id: "nocturne", label: "Nocturne" },
  { id: "voltage", label: "Voltage" },
  { id: "paper", label: "Paper" },
];
const API_KEY_STORAGE_KEY = "privon-dashboard-api-key";
const STREAM_RECONNECT_DELAY_MS = 2500;

const state = {
  theme: localStorage.getItem("privon-theme") || "nocturne",
  apiKey: sessionStorage.getItem(API_KEY_STORAGE_KEY) || "",
  connected: false,
  eventSource: null,
  reconnectTimer: null,
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

function createEmptyState(message) {
  return `<div class="empty-state">${message}</div>`;
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
    : mode === "connecting"
      ? "status-probation"
      : mode === "degraded"
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
    nodeTable.innerHTML = createEmptyState("No nodes registered yet.");
    return;
  }

  nodeTable.innerHTML = nodes.map((node) => {
    const scoreWidth = Math.max(0, Math.min(100, Math.round((node.score / 200) * 100)));
    return `
      <article class="node-row">
        <div class="meta-stack">
          <strong class="node-url">${node.url}</strong>
          <span class="meta">${node.shardId} � ${node.replicaGroup}</span>
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
    taskStream.innerHTML = createEmptyState("No fragment executions recorded yet.");
    return;
  }

  taskStream.innerHTML = executions.map((task) => `
    <article class="task-item">
      <div class="meta-stack">
        <span class="status-pill ${task.status === "verified" ? "status-active" : "status-probation"}">${task.status}</span>
        <strong>Fragment ${task.fragmentIndex}: ${task.fragment || "n/a"}</strong>
        <span class="meta">job ${task.jobId} � ${task.replicaGroup}</span>
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
    logConsole.innerHTML = createEmptyState("No coordinator logs yet.");
    return;
  }

  logConsole.innerHTML = logs.map((entry) => `
    <article class="log-item">
      <span class="log-level level-${entry.level}"></span>
      <div class="meta-stack">
        <span class="log-meta">${entry.type} � ${formatRelativeTime(entry.timestamp)}</span>
        <strong>${entry.message}</strong>
      </div>
    </article>
  `).join("");
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
      <article class="lane-pill">
        <div class="lane-head">
          <strong>${group.key}</strong>
          <span class="meta">${group.activeNodes}/${group.totalNodes} active</span>
        </div>
        <div class="lane-bar"><span style="width:${healthWidth}%"></span></div>
        <div class="lane-meta-grid">
          <span class="meta">avg score ${group.averageScore}</span>
          <span class="meta">${group.tasksHandled} tasks</span>
        </div>
      </article>
    `;
  }).join("");

  laneList.innerHTML = replicaGroups.map((group) => `
    <article class="lane-item">
      <div class="lane-head">
        <strong>${group.key}</strong>
        <span class="meta">${group.probationNodes} probation � ${group.inactiveNodes} inactive</span>
      </div>
      <div class="inline-list">${group.members.map((member) => `<span class="inline-tag ${statusClass(member.status)}">${member.shardId}</span>`).join("")}</div>
    </article>
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
      <article class="shard-card">
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
      </article>
    `;
  }).join("");
}

function render(snapshot) {
  const nodes = snapshot.nodes || [];
  const stats = snapshot.stats || {};
  const logs = snapshot.logs || [];

  renderSummary(stats);
  renderNodes(nodes);
  renderTasks(stats.recentExecutions || []);
  renderLogs(logs);
  renderReplicaHealth(stats.replicaGroups || []);
  renderShardCharts(stats.shardSummaries || []);
  setLastUpdated(stats.lastUpdatedAt || Date.now());
}

function updateSnapshot(partial) {
  state.snapshot = {
    nodes: partial.nodes || state.snapshot.nodes || [],
    stats: partial.stats || state.snapshot.stats || {},
    logs: partial.logs || state.snapshot.logs || [],
  };

  render(state.snapshot);
}

async function fetchJson(url) {
  const response = await fetch(url, {
    credentials: "same-origin",
  });

  if (response.status === 401) {
    throw new Error("Unauthorized");
  }

  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }

  return response.json();
}

async function loadSnapshot() {
  const [nodes, stats, logs] = await Promise.all([
    fetchJson("/dashboard/nodes"),
    fetchJson("/dashboard/stats"),
    fetchJson("/dashboard/logs?limit=24"),
  ]);

  updateSnapshot({
    nodes: nodes.nodes || [],
    stats,
    logs: logs.logs || [],
  });
}

function clearReconnectTimer() {
  if (state.reconnectTimer) {
    window.clearTimeout(state.reconnectTimer);
    state.reconnectTimer = null;
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

function handleStreamEvent(eventName, payload) {
  if (eventName === "snapshot") {
    updateSnapshot(payload);
    return;
  }

  if (eventName === "node.update") {
    updateSnapshot({
      nodes: payload.nodes || state.snapshot.nodes,
      stats: payload.stats || state.snapshot.stats,
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
  clearReconnectTimer();
  setConnectionState("connecting", "Secure session established. Opening live stream.");

  const eventSource = new EventSource("/dashboard/stream");
  state.eventSource = eventSource;

  eventSource.addEventListener("open", () => {
    setConnectionState("live", "Streaming node, task, and score updates live.");
  });

  ["snapshot", "node.update", "score.change", "task.execution", "log.append"].forEach((eventName) => {
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
    const response = await fetch("/dashboard/session", {
      method: "POST",
      credentials: "same-origin",
      headers: {
        authorization: `Bearer ${trimmedApiKey}`,
      },
    });

    if (response.status === 401) {
      throw new Error("Unauthorized");
    }

    if (!response.ok) {
      throw new Error(`Session request failed: ${response.status}`);
    }

    sessionStorage.setItem(API_KEY_STORAGE_KEY, trimmedApiKey);
    state.apiKey = trimmedApiKey;
    state.connected = true;
    updateVisibility();
    await loadSnapshot();
    connectStream();
  } finally {
    connectButton.disabled = false;
  }
}

async function bootstrap() {
  applyTheme(state.theme);
  renderThemeButtons();

  if (state.apiKey) {
    apiKeyInput.value = state.apiKey;
  }

  try {
    state.connected = true;
    updateVisibility();
    await loadSnapshot();
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
      ? "API key rejected. Check the coordinator API key and try again."
      : error.message);
  }
});

window.addEventListener("beforeunload", () => {
  state.eventSource?.close();
  clearReconnectTimer();
});

bootstrap();
