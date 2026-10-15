const $ = (sel) => document.querySelector(sel);
const api = async (path, opts = {}) => {
  const res = await fetch(path, { headers: { "content-type": "application/json" }, ...opts });
  const body = await res.text();
  let data;
  try { data = JSON.parse(body); } catch { data = null; }
  if (!res.ok) {
    const detail = data?.detail;
    const msg = Array.isArray(detail) ? detail.map((d) => d.msg || JSON.stringify(d)).join("; ") : detail;
    throw new Error(msg || body.slice(0, 200) || res.statusText);
  }
  return data;
};
const fmtMs = (v) => (v == null ? "–" : v < 10000 ? `${Math.round(v)} ms` : v < 3.6e6 ? `${(v / 1000).toFixed(1)} s` : `${(v / 3.6e6).toFixed(1)} h`);
const fmtAge = (s) => (s == null ? "–" : s < 120 ? `${Math.round(s)} s ago` : s < 7200 ? `${Math.round(s / 60)} min ago` : `${Math.round(s / 3600)} h ago`);
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const human = (t) => t.charAt(0) + t.slice(1).toLowerCase().replaceAll("_", " ");
const tagHtml = (t) => `<span class="tag ${t}" title="${t}">${human(t)}</span>`;
const hitHtml = (h) => `<span class="hit">⚑ ${esc(h.name)}</span>`;
const SEVERITY = [["SCHEDULE_CHANGE", "red"], ["SEAT_REMOVED", "amber"], ["SEAT_CHANGED", "amber"], ["PAYMENT_CHANGED", "teal"],
  ["CONTACT_CHANGED", "purple"], ["SEGMENT_ADDED", "blue"], ["SEGMENT_REMOVED", "blue"], ["SEGMENT_MODIFIED", "blue"]];
const severity = (tags) => (SEVERITY.find(([t]) => tags.includes(t)) || [null, "none"])[1];
const ageText = (iso) => fmtAge((Date.now() - new Date(iso).getTime()) / 1000);

// ---- navigation
document.querySelectorAll("nav button").forEach((b) =>
  b.addEventListener("click", () => {
    document.querySelectorAll("nav button, .view").forEach((el) => el.classList.remove("active"));
    b.classList.add("active");
    $(`#${b.dataset.view}`).classList.add("active");
    if (b.dataset.view === "rules") loadRules();
    if (b.dataset.view === "traveler") loadCards();
  })
);

// ---- ops console
async function refreshKpis() {
  try {
    const k = await api("/api/kpis");
    $("#k-versions").textContent = k.versions_5m;
    $("#k-significant").textContent = `${k.significant_5m} (${k.versions_5m ? Math.round((100 * k.significant_5m) / k.versions_5m) : 0}%)`;
    $("#k-p50").textContent = fmtMs(k.p50_ms);
    $("#k-p95").textContent = fmtMs(k.p95_ms);
    $("#k-rtm").textContent = fmtAge(k.rtm_freshness_s);
    $("#k-batch").textContent = fmtAge(k.batch_freshness_s);
    const live = k.rtm_freshness_s != null && k.rtm_freshness_s < 30;
    $("#live").className = `live ${live ? "on" : "idle"}`;
    $("#live span").textContent = live ? "Stream live" : "Stream idle";
  } catch (e) { console.error(e); }
}
setInterval(refreshKpis, 2000);
refreshKpis();

// Pause the feed while the pointer is over it so a row can be clicked; buffered events show on leave.
let feedPaused = false, feedBuffer = [];
$("#feed").addEventListener("mouseenter", () => { feedPaused = true; });
$("#feed").addEventListener("mouseleave", () => { feedPaused = false; feedBuffer.splice(0).forEach(addFeedItem); });

const shownIds = new Set();
function addFeedItem(ev) {
  if (feedPaused) { feedBuffer.push(ev); return; }
  if (shownIds.has(ev.id)) return;   // reconnects replay recent events; show each once
  shownIds.add(ev.id);
  const li = document.createElement("li");
  li.className = `sev-${severity(ev.tags)}`;
  li.dataset.trip = ev.trip_id;
  const detectedMs = new Date(ev.detected_ts).getTime() - new Date(ev.event_ts).getTime();
  const fresh = Date.now() - new Date(ev.event_ts).getTime() < 60000;
  const versions = ev.from_version == null ? `v${ev.to_version}` : `v${ev.from_version} → v${ev.to_version}`;
  li.innerHTML = `<div><span class="trip">${esc(ev.trip_id)}</span><span class="vers">${versions}</span></div>
    <div class="meta"><span class="lat ${fresh ? "" : "slow"}" title="event → detected">${Math.round(detectedMs)} ms</span>
      <div>${fresh ? "just now" : ageText(ev.event_ts)}</div></div>
    <div>${ev.tags.map(tagHtml).join("")}${ev.rule_hits.map(hitHtml).join("")}</div>`;
  li.addEventListener("click", () => loadTrip(ev.trip_id));
  $("#feed").prepend(li);
  while ($("#feed").children.length > 60) $("#feed").lastChild.remove();
}
new EventSource("/api/stream").onmessage = (m) => addFeedItem(JSON.parse(m.data));

async function loadTrip(tripId) {
  const t = await api(`/api/trips/${encodeURIComponent(tripId)}`);
  document.querySelectorAll("#feed li").forEach((li) => li.classList.toggle("selected", li.dataset.trip === tripId));
  const significant = t.versions.filter((v) => !v.tags.includes("NO_SIGNIFICANT_CHANGE")).length;
  $("#trip-title").textContent = `${tripId} · ${t.latest.record_locator} · ${t.latest.traveler.name ?? ""}`;
  $("#trip-sub").textContent = `${t.versions.length} versions, ${significant} significant`;
  $("#timeline").innerHTML = t.versions.map((v) => `
    <div class="ver ${v.tags.includes("NO_SIGNIFICANT_CHANGE") ? "noise" : "sig"}">
      <div><span class="vnum">v${v.to_version}</span>${v.tags.map(tagHtml).join("")}${v.rule_hits.map(hitHtml).join("")}</div>
      ${v.changes.map((c) => `<div class="chg">${describeChange(c)}</div>`).join("")}
    </div>`).join("");
}

function describeChange(c) {
  if (c.field === "*") return `${esc(c.segment)} — segment ${c.new === "added" ? "added" : "removed"}`;
  return `${esc(c.segment ?? "")} <code>${esc(c.field)}</code> <span class="old">${esc(c.old ?? "—")}</span><span class="arrow">→</span><b>${esc(c.new ?? "—")}</b>`;
}

// ---- rule studio
async function loadRules() {
  const rules = await api("/api/rules");
  $("#rules-table tbody").innerHTML = rules.map((r) => `
    <tr><td><b>${esc(r.name)}</b><span class="nl" title="${esc(r.nl_text)}">${esc(r.nl_text)}</span></td>
    <td><span class="status ${r.status}">${r.status}</span></td><td class="num">${r.hits}</td>
    <td>${r.status === "active" ? `<button data-disable="${r.rule_id}">Disable</button>` : ""}</td></tr>`).join("");
  document.querySelectorAll("[data-disable]").forEach((b) =>
    b.addEventListener("click", async () => { await api(`/api/rules/${b.dataset.disable}/disable`, { method: "POST" }); loadRules(); }));
}
setInterval(() => { if ($("#rules").classList.contains("active")) loadRules(); }, 3000);

$("#draft").addEventListener("click", async () => {
  $("#explanation").textContent = "Drafting…";
  try {
    const d = await api("/api/rules/draft", { method: "POST", body: JSON.stringify({ text: $("#nl").value }) });
    $("#rule-name").value = d.name;
    $("#logic").value = JSON.stringify(d.json_logic, null, 2);
    $("#explanation").textContent = d.explanation;
  } catch (e) { $("#explanation").textContent = `Could not draft: ${e.message}`; }
});

const currentLogic = () => JSON.parse($("#logic").value);
$("#preview").addEventListener("click", async () => {
  try {
    const p = await api("/api/rules/preview", { method: "POST", body: JSON.stringify({ json_logic: currentLogic() }) });
    $("#preview-result").innerHTML = `Would have matched <b>${p.matched}</b> of ${p.evaluated} recent versions. ` +
      p.samples.map((s) => `<span class="tag">${esc(s.trip_id)} v${s.to_version}</span>`).join(" ");
  } catch (e) { $("#preview-result").textContent = `Invalid rule: ${e.message}`; }
});
$("#activate").addEventListener("click", async () => {
  try {
    await api("/api/rules", { method: "POST", body: JSON.stringify({
      name: $("#rule-name").value, nl_text: $("#nl").value, json_logic: currentLogic(),
      action: { type: "RULE_MATCHED", target: $("#action-target").value } }) });
    $("#preview-result").innerHTML = `<span class="ok">✓ Active.</span> The stream picks it up within ~5 seconds — watch the Hits column.`;
    loadRules();
  } catch (e) { $("#preview-result").textContent = `Could not activate: ${e.message}`; }
});

// ---- traveler view
async function loadCards() {
  const items = await api("/api/traveler/notifications?limit=10");
  $("#cards").innerHTML = items.map((n) => {
    const p = n.payload, o = p.old_flight, f = p.new_flight;
    const t = (iso) => new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", timeZone: "UTC" });
    return `<div class="card">
      <span class="alert">Schedule change</span>
      <div class="who">${esc(p.traveler_name)} · Booking ${esc(p.record_locator)}</div>
      <div class="route"><span class="ap">${esc(o.origin)}</span>
        <span class="line"><span>${esc(f.carrier)}${esc(f.flight_number)}</span></span><span class="ap">${esc(o.destination)}</span></div>
      <div class="times"><s>${t(o.start)}</s><span>→</span><b>${t(f.start)}</b>
        <span class="delta">${p.delta_minutes > 0 ? "+" : ""}${p.delta_minutes} min</span></div>
      ${n.response ? `<p class="done">✓ You chose: ${n.response === "ACCEPT" ? "keep the new time" : "rebook me"}</p>` :
        `<div class="row"><button class="primary" data-r="ACCEPT" data-id="${n.id}">Accept new time</button><button data-r="REBOOK" data-id="${n.id}">Rebook</button></div>`}
    </div>`;
  }).join("") || "<p class='muted'>No schedule changes yet.</p>";
  document.querySelectorAll("[data-r]").forEach((b) => b.addEventListener("click", async () => {
    await api(`/api/traveler/notifications/${b.dataset.id}/respond`, { method: "POST", body: JSON.stringify({ response: b.dataset.r }) });
    loadCards();
  }));
}
setInterval(() => { if ($("#traveler").classList.contains("active")) loadCards(); }, 3000);

// ---- charts (plain SVG; one series per colour, text in ink colours, hover tooltips) ----------------------
const SVGNS = "http://www.w3.org/2000/svg";
const fmtInt = (v) => (v == null ? "–" : Math.round(Number(v)).toLocaleString());
const fmtTime = (d) => d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
const tip = document.createElement("div");
tip.className = "tip";
document.body.appendChild(tip);
const showTip = (evt, html) => { tip.innerHTML = html; tip.style.display = "block";
  tip.style.left = `${Math.min(evt.clientX + 14, window.innerWidth - 240)}px`; tip.style.top = `${evt.clientY + 14}px`; };
const hideTip = () => { tip.style.display = "none"; };

function svgEl(tag, attrs = {}) {
  const el = document.createElementNS(SVGNS, tag);
  Object.entries(attrs).forEach(([k, v]) => el.setAttribute(k, v));
  return el;
}

// Line/area chart: series = [{key, label, cls, area}], rows = [{x: Date, <key>: number}]
function lineChart(host, rows, series, { height = 220, yFmt = fmtInt } = {}) {
  host.innerHTML = "";
  if (rows.length < 2) { host.innerHTML = `<div class="empty">Waiting for data…</div>`; return; }
  const W = host.clientWidth || 600, H = height, m = { l: 44, r: 12, t: 10, b: 24 };
  const xs = rows.map((r) => r.x.getTime()), x0 = Math.min(...xs), x1 = Math.max(...xs);
  const ymax = Math.max(1, ...rows.flatMap((r) => series.map((s) => r[s.key] || 0))) * 1.1;
  const X = (t) => m.l + ((t - x0) / (x1 - x0 || 1)) * (W - m.l - m.r);
  const Y = (v) => H - m.b - (v / ymax) * (H - m.t - m.b);
  const svg = svgEl("svg", { width: W, height: H, class: "svgchart" });
  for (let i = 0; i <= 3; i++) {
    const v = (ymax / 1.1) * (i / 3), y = Y(v);
    svg.append(svgEl("line", { x1: m.l, x2: W - m.r, y1: y, y2: y, class: "grid" }));
    const t = svgEl("text", { x: m.l - 6, y: y + 4, class: "axis", "text-anchor": "end" }); t.textContent = yFmt(v); svg.append(t);
  }
  [rows[0], rows[Math.floor(rows.length / 2)], rows[rows.length - 1]].forEach((r, i) => {
    const t = svgEl("text", { x: X(r.x.getTime()), y: H - 6, class: "axis", "text-anchor": ["start", "middle", "end"][i] });
    t.textContent = fmtTime(r.x); svg.append(t);
  });
  series.forEach((s) => {
    const pts = rows.map((r) => `${X(r.x.getTime())},${Y(r[s.key] || 0)}`);
    if (s.area) svg.append(svgEl("path", { d: `M${X(xs[0])},${Y(0)} L${pts.join(" L")} L${X(xs[xs.length - 1])},${Y(0)} Z`, class: `area ${s.cls}` }));
    svg.append(svgEl("polyline", { points: pts.join(" "), class: `line ${s.cls}` }));
  });
  const cross = svgEl("line", { y1: m.t, y2: H - m.b, class: "cross", visibility: "hidden" });
  svg.append(cross);
  const hit = svgEl("rect", { x: m.l, y: m.t, width: W - m.l - m.r, height: H - m.t - m.b, fill: "transparent" });
  hit.addEventListener("mousemove", (evt) => {
    const box = svg.getBoundingClientRect(), t = x0 + ((evt.clientX - box.left - m.l) / (W - m.l - m.r)) * (x1 - x0);
    const r = rows.reduce((a, b) => (Math.abs(b.x - t) < Math.abs(a.x - t) ? b : a));
    cross.setAttribute("x1", X(r.x.getTime())); cross.setAttribute("x2", X(r.x.getTime())); cross.setAttribute("visibility", "visible");
    showTip(evt, `<b>${fmtTime(r.x)}</b>` + series.map((s) => `<div><i class="sw ${s.cls}"></i>${s.label}: <b>${yFmt(r[s.key] || 0)}</b></div>`).join(""));
  });
  hit.addEventListener("mouseleave", () => { cross.setAttribute("visibility", "hidden"); hideTip(); });
  svg.append(hit);
  host.append(svg);
}

function sparkline(svg, values) {
  svg.innerHTML = "";
  if (values.length < 2) return;
  const W = svg.clientWidth || 160, H = 28, max = Math.max(...values) || 1, min = Math.min(...values);
  const pts = values.map((v, i) => `${(i / (values.length - 1)) * W},${H - 2 - ((v - min) / (max - min || 1)) * (H - 4)}`);
  svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
  svg.append(svgEl("polyline", { points: pts.join(" "), class: "sparkline" }));
}

// Horizontal bars; items = [{label, value, note}]
function barList(host, items, { fmt = fmtInt, empty = "Nothing in this window yet" } = {}) {
  if (!items.length) { host.innerHTML = `<div class="empty small">${empty}</div>`; return; }
  const max = Math.max(...items.map((i) => i.value)) || 1;
  host.innerHTML = items.map((i) => `
    <div class="bar-row" title="${esc(i.label)}: ${fmt(i.value)}">
      <span class="bar-label">${i.label}</span>
      <span class="bar-track"><span class="bar" style="width:${Math.max(2, (100 * i.value) / max)}%"></span></span>
      <span class="bar-val">${fmt(i.value)}${i.note ? ` <span class="muted">${i.note}</span>` : ""}</span>
    </div>`).join("");
}

function histogram(host, buckets, { height = 200 } = {}) {
  host.innerHTML = "";
  if (!buckets.length) { host.innerHTML = `<div class="empty">Waiting for data…</div>`; return; }
  const n = 21, counts = Array(n).fill(0);
  buckets.forEach((b) => { counts[Math.min(b.bucket, n - 1)] = Number(b.n); });
  const total = counts.reduce((a, b) => a + b, 0) || 1;
  const W = host.clientWidth || 500, H = height, m = { l: 8, r: 8, t: 10, b: 24 }, bw = (W - m.l - m.r) / n;
  const max = Math.max(...counts) || 1;
  const svg = svgEl("svg", { width: W, height: H, class: "svgchart" });
  counts.forEach((c, i) => {
    const h = (c / max) * (H - m.t - m.b), x = m.l + i * bw + 1;
    const rect = svgEl("rect", { x, y: H - m.b - h, width: Math.max(1, bw - 2), height: Math.max(0, h), rx: 3, class: i <= 5 ? "barfill" : "barfill slow" });
    const label = i === n - 1 ? "≥ 1,000 ms" : `${i * 50}–${i * 50 + 49} ms`;
    const hitRect = svgEl("rect", { x: x - 1, y: m.t, width: bw, height: H - m.t - m.b, fill: "transparent" });
    hitRect.addEventListener("mousemove", (evt) => showTip(evt, `<b>${label}</b><div>${fmtInt(c)} versions (${((100 * c) / total).toFixed(1)}%)</div>`));
    hitRect.addEventListener("mouseleave", hideTip);
    svg.append(rect, hitRect);
  });
  [[0, "0"], [4, "200"], [10, "500"], [20, "1 s+"]].forEach(([i, txt]) => {
    const t = svgEl("text", { x: m.l + i * bw + bw / 2, y: H - 6, class: "axis", "text-anchor": "middle" }); t.textContent = `${txt}${i === 20 ? "" : " ms"}`; svg.append(t);
  });
  host.append(svg);
}

// ---- analytics view (live, from Lakebase) ----------------------------------------------------------------
let analyticsCache = null;
async function refreshAnalytics() {
  try { analyticsCache = await api("/api/analytics"); } catch (e) { console.error(e); return; }
  const a = analyticsCache, s = a.summary || {};
  // The newest minute is still filling up; leave it out so lines don't dip at the right edge.
  const rows = a.per_minute.slice(0, -1).map((r) => ({ x: new Date(r.minute), versions: Number(r.versions), significant: Number(r.significant), p50: Number(r.p50_ms) }));
  const live = rows.slice(-15);
  sparkline($("#spark-versions"), live.map((r) => r.versions));
  sparkline($("#spark-p50"), live.map((r) => r.p50));
  if (!$("#analytics").classList.contains("active")) return;
  const pct = s.versions ? ((100 * s.significant) / s.versions).toFixed(1) : "0";
  $("#a-window").textContent = `Last ${Math.round(s.window_minutes || 0)} minutes, live from Lakebase, the same store the stream writes to`;
  $("#a-kpis").innerHTML = [
    ["Booking versions", fmtInt(s.versions), `${fmtInt(s.trips)} trips`],
    ["Significant", `${pct}%`, `${fmtInt(s.significant)} versions; the rest are re-saves`],
    ["Schedule changes", fmtInt(s.schedule_changes), "airline re-timings detected"],
    ["Latency p50", fmtMs(s.p50_ms), "event → decision"],
    ["Latency p95", fmtMs(s.p95_ms), "event → decision"],
    ["Latency p99", fmtMs(s.p99_ms), "event → decision"],
  ].map(([l, v, sub], i) => `<div class="kpi${i >= 3 ? " accent" : ""}"><label>${l}</label><span>${v}</span><small>${sub}</small></div>`).join("");
  lineChart($("#c-volume"), rows, [
    { key: "versions", label: "All versions", cls: "s-all", area: true },
    { key: "significant", label: "Significant", cls: "s-sig" },
  ]);
  barList($("#c-mix"), a.change_mix.map((r) => ({ label: tagHtml(r.tag), value: Number(r.n) })));
  histogram($("#c-latency"), a.latency_hist);
  $("#c-routes tbody").innerHTML = a.schedule_routes.map((r) => `<tr><td><b>${esc(r.carrier)}</b></td><td>${esc(r.route)}</td>
    <td class="num">${fmtInt(r.n)}</td><td class="num">${r.avg_shift_min > 0 ? "+" : ""}${fmtInt(r.avg_shift_min)} min</td></tr>`).join("")
    || `<tr><td colspan="4" class="muted">No schedule changes in this window yet</td></tr>`;
  barList($("#c-rules"), a.rules.map((r) => ({ label: `⚑ ${esc(r.name)}`, value: Number(r.hits) })), { empty: "No rules fired in this window yet" });
  $("#c-actions").innerHTML = a.actions.map((r) => `<span class="action-chip"><b>${fmtInt(r.n)}</b> ${human(r.type)} · <span class="status ${r.status === "SENT" ? "active" : ""}">${r.status.toLowerCase()}</span></span>`).join("");
}
setInterval(refreshAnalytics, 5000);
refreshAnalytics();

// ---- data quality view (warehouse, refreshed every minute) ------------------------------------------------
const STATUS = { pass: ["✓", "Pass"], warn: ["!", "Review"], fail: ["✕", "Fail"] };
async function loadQuality() {
  let q;
  try { q = await api("/api/quality"); } catch (e) { $("#q-checks").innerHTML = `<li class="err">${esc(e.message)}</li>`; return; }
  if (q.loading) {
    $("#q-checks").innerHTML = `<li class="genie-wait"><span class="dot"></span> Running data-quality checks on the warehouse…</li>`;
    setTimeout(loadQuality, 3000); return;
  }
  $("#q-updated").textContent = `checked ${ageText(q.computed_at)}`;
  $("#q-lineage").innerHTML = q.lineage.map((n, i) => `
    ${i ? '<span class="lin-arrow">→</span>' : ""}
    <div class="lin-node ${n.kind.replaceAll(" ", "-")}">
      <span class="lin-kind">${esc(n.kind)}</span>
      <b>${esc(n.name).replaceAll("_", "_<wbr>")}</b>
      <span>${n.rows == null ? "every version, ~0.2 s" : `${fmtInt(n.rows)} rows`}</span>
      <span class="muted">${n.latest ? `updated ${ageText(n.latest.endsWith("Z") || n.latest.includes("+") ? n.latest : n.latest + "Z")}` : n.rows == null ? "live" : ""}</span>
    </div>`).join("");
  $("#q-checks").innerHTML = q.checks.map((c) => `
    <li class="check ${c.status}"><span class="badge">${STATUS[c.status][0]}</span>
      <div><b>${esc(c.name)}</b> <span class="muted">${esc(c.rule)}</span><div class="detail">${esc(c.detail)}</div></div>
      <span class="check-status">${STATUS[c.status][1]}</span></li>`).join("");
  const c = q.completeness || {};
  $("#q-complete").innerHTML = `
    <div class="kpi"><label>Carrier populated</label><span>${c.carrier_pct ?? "–"}%</span><small>gold_version_changes</small></div>
    <div class="kpi"><label>Route populated</label><span>${c.route_pct ?? "–"}%</span><small>origin and destination</small></div>`;
}

const GENIE_SUGGESTIONS = [
  "What share of booking versions today were significant vs noise?",
  "Which carriers had the most schedule changes today?",
  "Which business rules fired most, and how many actions did they create?",
];
$("#g-chips").innerHTML = GENIE_SUGGESTIONS.map((q) => `<button class="chip">${esc(q)}</button>`).join("");
document.querySelectorAll("#g-chips .chip").forEach((b) => b.addEventListener("click", () => { $("#g-q").value = b.textContent; askGenie(); }));
$("#g-ask").addEventListener("click", askGenie);
$("#g-q").addEventListener("keydown", (e) => { if (e.key === "Enter") askGenie(); });

async function askGenie() {
  const question = $("#g-q").value.trim();
  if (!question) return;
  $("#g-out").innerHTML = `<div class="genie-wait"><span class="dot"></span> Genie is writing the SQL and running it…</div>`;
  try {
    const r = await api("/api/genie", { method: "POST", body: JSON.stringify({ question }) });
    const table = r.columns.length ? `<table class="mini"><thead><tr>${r.columns.map((c) => `<th>${esc(c)}</th>`).join("")}</tr></thead>
      <tbody>${r.rows.slice(0, 12).map((row) => `<tr>${row.map((v) => `<td>${esc(v)}</td>`).join("")}</tr>`).join("")}</tbody></table>` : "";
    $("#g-out").innerHTML = `<div class="genie-answer">${esc(r.text).replace(/\*\*(.+?)\*\*/g, "<b>$1</b>").replaceAll("\n", "<br>")}</div>${table}
      ${r.sql ? `<details class="sql"><summary>Show the SQL Genie wrote</summary><pre>${esc(r.sql)}</pre></details>` : ""}`;
  } catch (e) { $("#g-out").innerHTML = `<p class="err">${esc(e.message)}</p>`; }
}

document.querySelectorAll("nav button").forEach((b) => b.addEventListener("click", () => {
  if (b.dataset.view === "analytics") refreshAnalytics();
  if (b.dataset.view === "quality") loadQuality();
}));

// ---- light / dark (DuBois tokens switch with the .dark class) ---------------------------------------------
function syncThemeLabel() {
  $("#theme span").textContent = document.documentElement.classList.contains("dark") ? "Light mode" : "Dark mode";
}
$("#theme").addEventListener("click", () => {
  const dark = document.documentElement.classList.toggle("dark");
  localStorage.setItem("theme", dark ? "dark" : "light");
  syncThemeLabel();
});
syncThemeLabel();
