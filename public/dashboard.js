const form = document.querySelector("#filters");
const status = document.querySelector("#status");

form.addEventListener("submit", (event) => { event.preventDefault(); refresh(); });
refresh();
setInterval(refresh, 15_000);

async function refresh() {
  const periodDays = Number(document.querySelector("#period").value);
  const bucketMs = Number(document.querySelector("#bucket").value);
  const clientId = document.querySelector("#clientId").value.trim();
  const to = Date.now();
  const from = to - periodDays * 86_400_000;
  const query = new URLSearchParams({ from, to, bucketMs });
  if (clientId) query.set("clientId", clientId);
  try {
    const response = await fetch(`/api/usage?${query}`);
    if (!response.ok) throw new Error("Analytics unavailable");
    const usage = await response.json();
    document.querySelector("#requests").textContent = usage.approvedRequests.toLocaleString();
    document.querySelector("#latency").textContent = `${usage.averageDecisionMs.toFixed(3)} ms`;
    document.querySelector("#range").textContent = `${periodDays} days`;
    status.textContent = `Updated ${new Date().toLocaleTimeString()}`;
    drawChart(usage.series, from, to);
  } catch (error) {
    status.textContent = "Analytics temporarily unavailable. Rate checks are unaffected.";
  }
}

function drawChart(series, from, to) {
  const chart = document.querySelector("#chart");
  if (!series.length) { chart.innerHTML = '<text x="500" y="140" text-anchor="middle">No approved requests in this period</text>'; return; }
  const width = 1000, height = 280, pad = { left: 50, right: 20, top: 20, bottom: 40 };
  const max = Math.max(...series.map((point) => point.requests), 1);
  const x = (timestamp) => pad.left + ((timestamp - from) / (to - from)) * (width - pad.left - pad.right);
  const y = (requests) => height - pad.bottom - (requests / max) * (height - pad.top - pad.bottom);
  const points = series.map((point) => `${x(point.timestamp).toFixed(1)},${y(point.requests).toFixed(1)}`).join(" ");
  const first = series[0], last = series[series.length - 1];
  chart.innerHTML = `<line class="axis" x1="${pad.left}" y1="${height - pad.bottom}" x2="${width - pad.right}" y2="${height - pad.bottom}"/><path class="area" d="M ${x(first.timestamp)},${height - pad.bottom} L ${points.replaceAll(" ", " L ")} L ${x(last.timestamp)},${height - pad.bottom} Z"/><polyline class="line" points="${points}"/><text x="${pad.left}" y="${pad.top}">${max.toLocaleString()}</text><text x="${pad.left}" y="${height - 12}">${new Date(from).toLocaleDateString()}</text><text x="${width - pad.right}" y="${height - 12}" text-anchor="end">${new Date(to).toLocaleDateString()}</text>`;
}
