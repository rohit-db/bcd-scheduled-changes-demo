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
const fmtMs = (v) => (v == null ? "–" : `${Math.round(v)} ms`);
const fmtAge = (s) => (s == null ? "–" : s < 120 ? `${Math.round(s)} s ago` : `${Math.round(s / 60)} min ago`);
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

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
  const shownMs = Date.now() - new Date(ev.event_ts).getTime();
  const tags = ev.tags.map((t) => `<span class="tag ${t}">${t}</span>`).join("");
  const hits = ev.rule_hits.map((h) => `<span class="hit">⚑ ${esc(h.name)}</span>`).join("");
  li.innerHTML = `<b>${esc(ev.trip_id)}</b> v${ev.from_version ?? "–"}→v${ev.to_version} ${tags} ${hits}
    <span class="muted">${shownMs} ms event→screen</span>`;
  li.addEventListener("click", () => loadTrip(ev.trip_id));
  $("#feed").prepend(li);
  while ($("#feed").children.length > 60) $("#feed").lastChild.remove();
}
new EventSource("/api/stream").onmessage = (m) => addFeedItem(JSON.parse(m.data));

async function loadTrip(tripId) {
  const t = await api(`/api/trips/${encodeURIComponent(tripId)}`);
  $("#trip-title").textContent = `${tripId} · ${t.latest.record_locator} · ${t.latest.traveler.name ?? ""}`;
  $("#timeline").innerHTML = t.versions.map((v) => `
    <div class="ver ${v.tags.includes("NO_SIGNIFICANT_CHANGE") ? "noise" : ""}">
      <div><b>v${v.to_version}</b> ${v.tags.map((x) => `<span class="tag ${x}">${x}</span>`).join("")}
        ${v.rule_hits.map((h) => `<span class="hit">⚑ ${esc(h.name)}</span>`).join("")}</div>
      ${v.changes.map((c) => `<div class="chg">${describeChange(c)}</div>`).join("")}
    </div>`).join("");
}

function describeChange(c) {
  if (c.field === "*") return `${esc(c.segment)} — segment ${c.new === "added" ? "added" : "removed"}`;
  return `${esc(c.segment ?? "")} <code>${esc(c.field)}</code>: ${esc(c.old ?? "—")} → ${esc(c.new ?? "—")}`;
}

// ---- rule studio
async function loadRules() {
  const rules = await api("/api/rules");
  $("#rules-table tbody").innerHTML = rules.map((r) => `
    <tr><td title="${esc(r.nl_text)}">${esc(r.name)}</td><td>${r.status}</td><td>${r.hits}</td>
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
    $("#preview-result").innerHTML = `Matched <b>${p.matched}</b> of ${p.evaluated} recent versions. ` +
      p.samples.map((s) => `<span class="tag">${esc(s.trip_id)} v${s.to_version}</span>`).join(" ");
  } catch (e) { $("#preview-result").textContent = `Invalid rule: ${e.message}`; }
});
$("#activate").addEventListener("click", async () => {
  try {
    await api("/api/rules", { method: "POST", body: JSON.stringify({
      name: $("#rule-name").value, nl_text: $("#nl").value, json_logic: currentLogic(),
      action: { type: "RULE_MATCHED", target: $("#action-target").value } }) });
    $("#preview-result").textContent = "Active — the stream picks it up within ~5 seconds. Watch the Hits column.";
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
      <div class="muted">Booking ${esc(p.record_locator)} · ${esc(p.traveler_name)}</div>
      <h3>Your flight ${esc(o.carrier)}${esc(o.flight_number)} ${esc(o.origin)} → ${esc(o.destination)} has changed</h3>
      <p><s>${t(o.start)}</s> → <b>${t(f.start)}</b> on ${esc(f.carrier)}${esc(f.flight_number)} (${p.delta_minutes > 0 ? "+" : ""}${p.delta_minutes} min)</p>
      ${n.response ? `<p class="done">You chose: ${n.response}</p>` :
        `<div class="row"><button class="primary" data-r="ACCEPT" data-id="${n.id}">Accept</button><button data-r="REBOOK" data-id="${n.id}">Rebook</button></div>`}
    </div>`;
  }).join("") || "<p class='muted'>No schedule changes yet.</p>";
  document.querySelectorAll("[data-r]").forEach((b) => b.addEventListener("click", async () => {
    await api(`/api/traveler/notifications/${b.dataset.id}/respond`, { method: "POST", body: JSON.stringify({ response: b.dataset.r }) });
    loadCards();
  }));
}
setInterval(() => { if ($("#traveler").classList.contains("active")) loadCards(); }, 3000);
