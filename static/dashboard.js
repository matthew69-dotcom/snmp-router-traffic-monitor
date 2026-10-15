
const $ = (id) => document.getElementById(id);

const chart = new Chart($("trafficChart"), {
  type: "line",
  data: {
    labels: [],
    datasets: [
      {
        label: "RX",
        data: [],
        borderColor: "#38bdf8",
        backgroundColor: "rgba(56,189,248,0.10)",
        fill: true,
        tension: 0.28,
        pointRadius: 0,
        borderWidth: 2
      },
      {
        label: "TX",
        data: [],
        borderColor: "#a78bfa",
        backgroundColor: "rgba(167,139,250,0.08)",
        fill: true,
        tension: 0.28,
        pointRadius: 0,
        borderWidth: 2
      }
    ]
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    interaction: { intersect: false, mode: "index" },
    plugins: {
      legend: {
        labels: { color: "#cbd5e1", usePointStyle: true, padding: 20 }
      }
    },
    scales: {
      x: {
        ticks: { color: "#94a3b8", maxTicksLimit: 8 },
        grid: { color: "rgba(148,163,184,0.07)" }
      },
      y: {
        beginAtZero: true,
        title: { display: true, text: "Mbps", color: "#94a3b8" },
        ticks: { color: "#94a3b8" },
        grid: { color: "rgba(148,163,184,0.09)" }
      }
    }
  }
});

function formatMbps(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number.toFixed(4) : "0.0000";
}

function formatTime(timestamp) {
  if (!timestamp) return "—";
  const date = new Date(timestamp);
  return Number.isNaN(date.getTime()) ? "—" : date.toLocaleTimeString();
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#039;"
  })[char]);
}

function setStatus(online, text) {
  $("statusText").textContent = text;
  $("statusDot").className = `status-dot ${online ? "online" : "offline"}`;
  $("dataStatus").textContent = online ? "Receiving data" : "No recent data";
  $("dataStatus").className = online ? "good-text" : "muted-text";
}

function renderInterfaces(interfaces) {
  const tbody = $("interfaceTable");

  if (!interfaces.length) {
    tbody.innerHTML =
      '<tr><td colspan="6" class="empty">Waiting for SNMP samples...</td></tr>';
    return;
  }

  tbody.innerHTML = interfaces
    .slice()
    .sort((a, b) =>
      String(a.router).localeCompare(String(b.router)) ||
      String(a.interface).localeCompare(String(b.interface))
    )
    .map((item) => `
      <tr>
        <td><span class="router-name">${escapeHtml(item.router)}</span></td>
        <td>${escapeHtml(item.router_ip || "—")}</td>
        <td><span class="interface-name">${escapeHtml(item.interface)}</span></td>
        <td class="rx-value">${formatMbps(item.rx_mbps)}</td>
        <td class="tx-value">${formatMbps(item.tx_mbps)}</td>
        <td>${formatTime(item.timestamp)}</td>
      </tr>
    `).join("");
}

function updateRouterFilter(history) {
  const select = $("routerFilter");
  const selected = select.value;
  const routers = [...new Set(history.map(item => item.router).filter(Boolean))].sort();

  select.innerHTML = '<option value="all">All routers</option>' +
    routers.map(router =>
      `<option value="${escapeHtml(router)}">${escapeHtml(router)}</option>`
    ).join("");

  if (routers.includes(selected)) select.value = selected;
}

function renderChart(history) {
  const selectedRouter = $("routerFilter").value;
  const filtered = history.filter(item =>
    selectedRouter === "all" || item.router === selectedRouter
  );

  chart.data.labels = filtered.map(item => formatTime(item.timestamp));
  chart.data.datasets[0].data = filtered.map(item => Number(item.rx_mbps) || 0);
  chart.data.datasets[1].data = filtered.map(item => Number(item.tx_mbps) || 0);
  chart.update("none");
}

async function refreshDashboard() {
  try {
    const [summaryResponse, interfacesResponse, historyResponse] =
      await Promise.all([
        fetch("/api/summary", { cache: "no-store" }),
        fetch("/api/routers", { cache: "no-store" }),
        fetch("/api/history", { cache: "no-store" })
      ]);

    if (!summaryResponse.ok || !interfacesResponse.ok || !historyResponse.ok) {
      throw new Error("Dashboard API request failed");
    }

    const [summary, interfaces, history] = await Promise.all([
      summaryResponse.json(),
      interfacesResponse.json(),
      historyResponse.json()
    ]);

    $("routerCount").textContent = summary.router_count ?? 0;
    $("interfaceCount").textContent = summary.interface_count ?? 0;
    $("rxTotal").textContent = formatMbps(summary.rx_total_mbps);
    $("txTotal").textContent = formatMbps(summary.tx_total_mbps);
    $("interfaceBadge").textContent =
      `${summary.interface_count ?? 0} interfaces`;

    renderInterfaces(interfaces);
    updateRouterFilter(history);
    renderChart(history);

    const age = summary.data_age_seconds;
    const hasRecentData = age !== null && age !== undefined && age <= 15;

    setStatus(
      hasRecentData,
      hasRecentData ? "Receiving data" : "Waiting for recent data"
    );

    $("lastReceived").textContent = summary.last_received_unix
      ? new Date(summary.last_received_unix * 1000).toLocaleTimeString()
      : "No samples yet";
  } catch (error) {
    setStatus(false, "API unavailable");
    $("lastReceived").textContent = "Unable to reach API";
  }
}

$("routerFilter").addEventListener("change", () => {
  refreshDashboard();
});

refreshDashboard();
setInterval(refreshDashboard, 2000);

