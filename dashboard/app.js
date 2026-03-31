const THEMES = [
  { id: "nocturne", label: "Nocturne" },
  { id: "voltage", label: "Voltage" },
  { id: "paper", label: "Paper" },
];
const REFRESH_MS = 3000;

const state = {
  theme: localStorage.getItem("privon-theme") || "nocturne",
};

const themeSwitcher = document.getElementById("theme-switcher");
const summaryStrip = document.getElementById("summary-strip");
const nodeTable = document.getElementById("node-table");
const taskStream = document.getElementById("task-stream");
const logConsole = document.getElementById("log-console");
const laneList = document.getElementById("lane-list");
const replicaLanes = document.getElementById("replica-lanes");
const lastUpdated = document.getElementById("last-updated");

function formatRelativeTime(timestamp) {
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

function renderReplicaLanes(nodes) {
  const grouped = nodes.reduce((accumulator, node) => {
    const key = node.replicaGroup || "default-replica";
    if (!accumulator[key]) {
      accumulator[key] = [];
    }
    accumulator[key].push(node);
    return accumulator;
  }, {});

  const entries = Object.entries(grouped);

  if (!entries.length) {
    laneList.innerHTML = createEmptyState("Replica groups will appear once nodes register.");
    replicaLanes.innerHTML = createEmptyState("Waiting for replica metadata.");
    return;
  }

  laneList.innerHTML = entries.map(([groupName, members]) => `
    <article class="lane-item">
      <div class="lane-head">
        <strong>${groupName}</strong>
        <span class="meta">${members.length} node(s)</span>
      </div>
      <div class="inline-list">${members.map((member) => `<span class="inline-tag">${member.shardId}</span>`).join("")}</div>
    </article>
  `).join("");

  replicaLanes.innerHTML = entries.map(([groupName, members]) => {
    const activeCount = members.filter((member) => member.status === "active").length;
    const barWidth = Math.max(12, Math.round((activeCount / members.length) * 100));
    return `
      <article class="lane-pill">
        <div class="lane-head">
          <strong>${groupName}</strong>
          <span class="meta">${activeCount}/${members.length} active</span>
        </div>
        <div class="lane-bar"><span style="width:${barWidth}%"></span></div>
        <span class="meta">${members.map((member) => member.url.split(":").slice(-1)[0]).join(" � ")}</span>
      </article>
    `;
  }).join("");
}

async function refresh() {
  try {
    const [nodesResponse, statsResponse, logsResponse] = await Promise.all([
      fetch("/dashboard/nodes"),
      fetch("/dashboard/stats"),
      fetch("/dashboard/logs?limit=24"),
    ]);

    const [nodesData, statsData, logsData] = await Promise.all([
      nodesResponse.json(),
      statsResponse.json(),
      logsResponse.json(),
    ]);

    renderSummary(statsData);
    renderNodes(nodesData.nodes || []);
    renderTasks(statsData.recentExecutions || []);
    renderLogs(logsData.logs || []);
    renderReplicaLanes(nodesData.nodes || []);
    lastUpdated.textContent = `updated ${formatRelativeTime(Date.now())}`;
  } catch (error) {
    lastUpdated.textContent = `refresh failed: ${error.message}`;
  }
}

applyTheme(state.theme);
renderThemeButtons();
refresh();
setInterval(refresh, REFRESH_MS);
