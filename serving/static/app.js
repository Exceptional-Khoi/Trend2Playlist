/* VN Music Pulse - dashboard (vanilla JS + ECharts + Leaflet).
 * Mọi chuỗi đến từ dữ liệu crawl đều được chèn bằng textContent (hoặc esc() trong tooltip ECharts). */
"use strict";

// ------------------------------------------------------------------ tiện ích
const $ = (s, el = document) => el.querySelector(s);
const $$ = (s, el = document) => Array.from(el.querySelectorAll(s));
const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
const FONT = 'system-ui, -apple-system, "Segoe UI", Roboto, sans-serif';

function h(tag, attrs, ...kids) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v == null || v === false) continue;
    if (k === "class") el.className = v;
    else if (k === "style" && typeof v === "object") Object.assign(el.style, v);
    else if (k.startsWith("on")) el.addEventListener(k.slice(2), v);
    else el.setAttribute(k, v === true ? "" : v);
  }
  for (const k of kids.flat()) {
    if (k == null || k === false) continue;
    el.append(k instanceof Node ? k : document.createTextNode(String(k)));
  }
  return el;
}
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const nf = new Intl.NumberFormat("vi-VN");
const num = (v) => nf.format(Math.round(v || 0));
function compact(v) {
  v = v || 0;
  if (v >= 1e9) return (v / 1e9).toFixed(1).replace(/\.0$/, "") + " tỷ";
  if (v >= 1e6) return (v / 1e6).toFixed(1).replace(/\.0$/, "") + " tr";
  if (v >= 1e4) return (v / 1e3).toFixed(1).replace(/\.0$/, "") + " N";
  return num(v);
}
function ago(ms) {
  if (!ms) return "chưa có";
  const s = Math.max(0, (Date.now() - ms) / 1000);
  if (s < 60) return `${Math.round(s)} giây trước`;
  if (s < 3600) return `${Math.round(s / 60)} phút trước`;
  if (s < 86400) return `${Math.round(s / 3600)} giờ trước`;
  return `${Math.round(s / 86400)} ngày trước`;
}
function bytes(b) {
  if (!b) return "0 B";
  const u = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.min(u.length - 1, Math.floor(Math.log(b) / Math.log(1024)));
  return (b / 1024 ** i).toFixed(i ? 1 : 0) + " " + u[i];
}
const artistsText = (a) => (Array.isArray(a) ? a.join(", ") : a || "");
async function api(path, opts) {
  const r = await fetch(path, opts);
  if (!r.ok) throw new Error(`${r.status} ${path}`);
  return r.json();
}
const store = {
  get(k, d) { try { const v = localStorage.getItem(k); return v == null ? d : JSON.parse(v); } catch { return d; } },
  set(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch { /* private mode */ } },
};

// ------------------------------------------------------------------ màu thể loại (cố định theo thể loại)
const GENRE_GROUPS = [
  ["V-Pop", ["V-Pop"]],
  ["Rap Việt", ["Rap Việt"]],
  ["Nhạc Trữ Tình", ["Nhạc Trữ Tình"]],
  ["EDM/Remix", ["EDM/Remix"]],
  ["Indie/Rock Việt", ["Indie/Rock Việt"]],
  ["Quê Hương/Cải Lương", ["Quê Hương/Cải Lương"]],
  ["Nhạc quốc tế", ["US-UK", "K-Pop/Châu Á"]],
  ["Nhạc Trịnh/Tiền Chiến", ["Nhạc Trịnh/Tiền Chiến"]],
];
const GROUP_OF = {};
GENRE_GROUPS.forEach(([g, members], i) => members.forEach((m) => (GROUP_OF[m] = { group: g, slot: i + 1 })));
const genreGroup = (g) => (GROUP_OF[g] ? GROUP_OF[g].group : "Khác");
const groupColor = (group) => {
  const i = GENRE_GROUPS.findIndex(([g]) => g === group);
  return i >= 0 ? css(`--s${i + 1}`) : css("--other");
};
const genreColor = (g) => groupColor(genreGroup(g));
const GROUP_NAMES = [...GENRE_GROUPS.map(([g]) => g), "Khác"];

const CHART_LABEL = {
  zing_realtime: "Zing MP3 · #zingchart realtime",
  zing_week_vn: "Zing MP3 · BXH tuần Việt Nam",
  zing_new_release: "Zing MP3 · BXH nhạc mới",
  spotify_top50_vn_daily: "Spotify · Top 50 Vietnam (ngày)",
  spotify_top_songs_vn_weekly: "Spotify · Top Songs Vietnam (tuần)",
  spotify_daily_streams_vn: "Spotify · lượt stream ngày (kworb)",
  apple_most_played_vn: "Apple Music · Most Played VN",
  youtube_top_songs_vn_weekly: "YouTube · Top bài hát VN (tuần)",
  youtube_top_videos_vn_weekly: "YouTube · Top video VN (tuần)",
  youtube_top_artists_vn_weekly: "YouTube · Top nghệ sĩ VN (tuần)",
};
const SHORT_CHART = {
  zing_realtime: "Zing RT", zing_week_vn: "Zing tuần", zing_new_release: "Zing mới", spotify_top50_vn_daily: "Spotify",
  spotify_top_songs_vn_weekly: "Spotify tuần", spotify_daily_streams_vn: "Spotify stream", apple_most_played_vn: "Apple",
  youtube_top_songs_vn_weekly: "YouTube", youtube_top_videos_vn_weekly: "YT video",
};
const SOURCE_LABEL = { zing: "Zing MP3", spotify: "Spotify", apple_music: "Apple Music", youtube: "YouTube" };
const SOURCE_SHORT = { zing: "Zing", spotify: "Spotify", apple_music: "Apple", youtube: "YouTube" };

// ------------------------------------------------------------------ ECharts
const charts = {};
const redraws = {};
function chart(id) {
  if (!charts[id]) charts[id] = echarts.init(document.getElementById(id), null, { renderer: "svg" });
  return charts[id];
}
function baseOpt() {
  return {
    textStyle: { fontFamily: FONT, color: css("--ink-2") },
    animationDuration: 300,
    tooltip: {
      backgroundColor: css("--surface"), borderColor: css("--border"), borderWidth: 1,
      textStyle: { color: css("--ink"), fontSize: 12, fontFamily: FONT }, confine: true,
      extraCssText: "box-shadow:0 4px 16px rgba(0,0,0,.14);border-radius:8px;padding:8px 10px;",
    },
  };
}
const axis = (extra) => Object.assign({
  axisLine: { lineStyle: { color: css("--axis") } }, axisTick: { show: false },
  axisLabel: { color: css("--muted"), fontSize: 11 }, splitLine: { lineStyle: { color: css("--grid"), width: 1 } },
}, extra || {});
const tipRow = (color, value, label, line) =>
  `<div style="display:flex;align-items:center;gap:8px;margin-top:2px">` +
  `<span style="display:inline-block;width:${line ? 12 : 8}px;height:${line ? 2 : 8}px;border-radius:${line ? 1 : 2}px;background:${color}"></span>` +
  `<b style="font-weight:600">${esc(value)}</b><span style="color:${css("--ink-2")}">${esc(label)}</span></div>`;
function empty(id, text) {
  chart(id).clear();
  chart(id).setOption({ title: { text, left: "center", top: "middle", textStyle: { color: css("--muted"), fontSize: 13, fontWeight: 400 } } });
}
window.addEventListener("resize", () => Object.values(charts).forEach((c) => c.resize()));

// ------------------------------------------------------------------ theme
function applyTheme(t, redraw = true) {
  if (t) document.documentElement.setAttribute("data-theme", t);
  else document.documentElement.removeAttribute("data-theme");
  store.set("theme", t || null);
  if (redraw) Object.values(redraws).forEach((fn) => { try { fn(); } catch (e) { console.warn(e); } });
  setTiles();
}
$("#theme-btn").addEventListener("click", () => {
  const cur = document.documentElement.getAttribute("data-theme") ||
    (matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
  applyTheme(cur === "dark" ? "light" : "dark");
});
matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => applyTheme(store.get("theme", null)));

// ------------------------------------------------------------------ tabs
let currentView = "map";
$$(".tab").forEach((b) => b.addEventListener("click", () => {
  $$(".tab").forEach((x) => x.setAttribute("aria-selected", x === b ? "true" : "false"));
  $$(".view").forEach((v) => v.classList.toggle("active", v.id === `view-${b.dataset.view}`));
  currentView = b.dataset.view;
  refreshView();
  setTimeout(() => { Object.values(charts).forEach((c) => c.resize()); if (map) map.invalidateSize(); }, 50);
}));
function refreshView() {
  ({ map: loadMapView, charts: loadChartsView, analytics: loadAnalytics, reco: loadReco, pipeline: loadPipeline })[currentView]?.();
}

// ------------------------------------------------------------------ status chips
let meta = null;
function setChip(id, cls, text) {
  const el = $(id);
  el.className = `chip ${cls}`;
  el.lastElementChild.textContent = text;
}
async function loadStatus() {
  try {
    const hl = await api("/api/health");
    const st = hl.elasticsearch;
    setChip("#st-es", st === "green" || st === "yellow" ? "ok" : "err", `Elasticsearch: ${st}`);
  } catch { setChip("#st-es", "err", "API/ES không phản hồi"); }
  try {
    meta = await api("/api/meta");
    const b = meta.batch_views;
    if (!b) setChip("#st-batch", "warn", "Batch view: chưa chạy");
    else setChip("#st-batch", Date.now() - b.run_ts < 2 * 3600e3 ? "ok" : "warn", `Batch view: ${ago(b.run_ts)}`);
  } catch { setChip("#st-batch", "err", "Batch view: lỗi"); }
  try {
    const tl = await api("/api/realtime/timeline?minutes=5");
    const last = tl.items.filter((x) => x.plays + x.skips > 0).pop();
    if (last && Date.now() - last.minute < 3 * 60e3) setChip("#st-rt", "ok", `Realtime: ${num(last.plays + last.skips)} lượt/phút`);
    else setChip("#st-rt", "warn", "Realtime: không có sự kiện mới");
  } catch { setChip("#st-rt", "err", "Realtime: lỗi"); }
}

// ================================================================== 1. BẢN ĐỒ
let map, tiles, markers = {}, provinces = [], selected = store.get("province", "ha-noi"), mode = "merged";
const METRIC_LABEL = { rt_plays: "lượt nghe (15 phút)", mentions_7d: "điểm bình luận nhắc tỉnh (7 ngày)",
  plays_7d: "lượt nghe (7 ngày)", confidence: "% độ tin cậy dữ liệu riêng của tỉnh" };

function setTiles() {
  if (!map) return;
  const dark = css("--page") === "#0d0d0d";
  if (tiles) map.removeLayer(tiles);
  // nền Esri Canvas không nhãn địa danh (không cần API key): tự ghi nhãn Hoàng Sa, Trường Sa của Việt Nam
  const layer = dark ? "World_Dark_Gray_Base" : "World_Light_Gray_Base";
  tiles = L.tileLayer(`https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/${layer}/MapServer/tile/{z}/{y}/{x}`, {
    attribution: "Nền bản đồ &copy; Esri", maxZoom: 10,
  }).addTo(map);
  drawMarkers();
}
function initMap() {
  if (map) return;
  map = L.map("map", { zoomSnap: 0.25, minZoom: 4.5, maxZoom: 9, attributionControl: true })
    .fitBounds([[8.3, 102.2], [23.4, 114.8]]);
  for (const [lat, lon, name] of [[16.5, 112.0, "QĐ. Hoàng Sa (Đà Nẵng)"], [10.2, 114.2, "QĐ. Trường Sa (Khánh Hòa)"]]) {
    L.marker([lat, lon], { icon: L.divIcon({ className: "islands", html: esc(name), iconSize: [150, 14] }), interactive: false }).addTo(map);
  }
  setTiles();
}
function metricValue(p) {
  const m = $("#map-metric").value;
  return m === "confidence" ? Math.round((p.confidence || 0) * 100) : p[m] || 0;
}
function drawMarkers() {
  if (!map || !provinces.length) return;
  Object.values(markers).forEach((m) => map.removeLayer(m));
  markers = {};
  const max = Math.max(1, ...provinces.map(metricValue));
  const c1 = css("--s1"), c2 = css("--s2"), surface = css("--surface");
  for (const p of provinces) {
    const v = metricValue(p);
    const isSel = p.code === selected;
    const m = L.circleMarker([p.lat, p.lon], {
      radius: 5 + 24 * Math.sqrt(v / max), color: isSel ? c2 : surface, weight: isSel ? 3 : 2,
      fillColor: c1, fillOpacity: v > 0 ? 0.55 : 0.15, bubblingMouseEvents: false,
    }).addTo(map);
    const tip = h("div", {}, h("div", { class: "v" }, `${num(v)} ${METRIC_LABEL[$("#map-metric").value]}`),
      h("div", { class: "l" }, p.name), p.top_title ? h("div", { class: "l" }, `#1: ${p.top_title}`) : null);
    m.bindTooltip(tip, { className: "vtip", direction: "top", offset: [0, -6] });
    m.on("click", () => selectProvince(p.code));
    markers[p.code] = m;
  }
  if (markers[selected]) markers[selected].bringToFront();
  $("#map-sub").textContent = `Kích thước = ${METRIC_LABEL[$("#map-metric").value]}. Bấm vào một tỉnh để xem bảng xếp hạng của tỉnh đó`;
}
function fillProvinceSelects() {
  for (const sel of [$("#prov-select"), $("#reco-prov")]) {
    if (sel.options.length) continue;
    for (const region of ["Bắc", "Trung", "Nam"]) {
      const og = h("optgroup", { label: `Miền ${region}` });
      provinces.filter((p) => p.region === region).forEach((p) => og.append(h("option", { value: p.code }, p.name)));
      sel.append(og);
    }
  }
  $("#prov-select").value = selected;
}
async function loadMapView() {
  initMap();
  try {
    const res = await api("/api/provinces?minutes=15");
    provinces = res.items;
    fillProvinceSelects();
    drawMarkers();
    renderFacts();
  } catch (e) { console.warn(e); }
  loadTrending();
}
function selectProvince(code) {
  selected = code;
  store.set("province", code);
  $("#prov-select").value = code;
  drawMarkers();
  renderFacts();
  loadTrending();
}
const CONF_TEXT = {
  "cao": "Nhiều dữ liệu riêng của tỉnh (Google Trends + bình luận)",
  "trung bình": "Có một phần dữ liệu riêng của tỉnh",
  "thấp": "Ít dữ liệu riêng: xếp hạng chủ yếu dựa trên toàn quốc",
};
function renderFacts() {
  const p = provinces.find((x) => x.code === selected);
  if (!p) return;
  $("#prov-name").textContent = p.name;
  $("#prov-region").textContent = `Miền ${p.region} · ${p.subregion}`;
  const fact = (label, value, title) => h("div", { class: "fact", title }, h("div", { class: "label" }, label), h("div", { class: "value" }, value));
  const conf = p.confidence_label || "thấp";
  $("#prov-facts").replaceChildren(
    fact("Độ tin cậy", `${conf} (${Math.round((p.confidence || 0) * 100)}%)`,
      `${CONF_TEXT[conf] || ""}. Tín hiệu dùng được: ${(p.signals || ["toàn quốc"]).join(", ")}. ` +
      `Google Trends: ${num(p.trend_tracks)} bài, phủ ${Math.round((p.trend_coverage || 0) * 100)}% dân số; ` +
      `${num(p.mention_authors)} người bình luận nhắc tỉnh (${num(p.self_mentions)} tự nhận ở tỉnh).`),
    fact("Người bình luận nhắc tỉnh", num(p.mention_authors), `${num(p.self_mentions)} người tự nhận đang ở / quê ở tỉnh này`),
    fact("Nghe 15 phút qua", num(p.rt_plays)),
    fact("Thể loại chính", p.top_genre || "—"),
    fact("Bài đặc trưng", p.fav_title || "—", p.fav_title ? `${p.fav_title}: được quan tâm cao gấp ${p.fav_lift} lần mức trung bình các tỉnh` : null),
  );
  $("#conf-note").textContent = conf === "thấp" ? `⚠ ${CONF_TEXT["thấp"]}.` : "";
}
const W = { batch: 0.6, rt: 0.3, men: 0.1 };
// 4 loại tín hiệu - màu cố định theo tín hiệu
const COMP = [
  ["nat", "Toàn quốc (BXH 4 nền tảng)", "--s1"],
  ["trd", "Google Trends tại tỉnh", "--s2"],
  ["cmt", "Bình luận nhắc tỉnh", "--s3"],
  ["evt", "Lượt nghe tại tỉnh", "--s4"],
];
function renderCompLegend() {
  const shown = mode === "realtime" ? ["cmt", "evt"] : ["nat", "trd", "cmt", "evt"];
  $("#comp-legend").replaceChildren(...COMP.filter(([k]) => shown.includes(k)).map(([, label, v]) =>
    h("span", {}, h("i", { style: { background: css(v) } }), label)));
}
function comps(it) {
  return it.parts || { nat: 0, trd: 0, cmt: 0, evt: 0 };
}
let trendData = null;
async function loadTrending() {
  try {
    trendData = await api(`/api/trending?province=${encodeURIComponent(selected)}&mode=${mode}&limit=20`);
    renderTrending();
  } catch (e) {
    $("#trend-list").replaceChildren(h("li", { class: "empty" }, "Chưa có dữ liệu xu hướng cho tỉnh này."));
  }
  try {
    const q = await api(`/api/mentions?province=${encodeURIComponent(selected)}&limit=6`);
    $("#quotes").replaceChildren(...(q.items.length ? q.items.map((m) => h("div", { class: "quote" },
      h("b", {}, m.title || ""), " — ", (m.text || "").slice(0, 220),
      h("div", { class: "small muted" }, `${m.kind === "self" ? "tự nhận ở tỉnh" : "nhắc tên tỉnh"} · ${ago(m.published_ms)}`)))
      : [h("div", { class: "empty" }, "Chưa thu được bình luận nào nhắc tỉnh này.")]));
  } catch { /* bỏ qua */ }
}
function genreTag(g) {
  return h("span", { class: "gtag" }, h("i", { style: { background: genreColor(g) } }), g || "Khác");
}
function deltaEl(it) {
  if (it.rank_change == null) return h("span", { class: "delta new", title: "Chỉ có trong realtime view" }, "mới");
  if (it.rank_change > 0) return h("span", { class: "delta up", title: "So với hạng trong batch view" }, `▲${it.rank_change}`);
  if (it.rank_change < 0) return h("span", { class: "delta down", title: "So với hạng trong batch view" }, `▼${-it.rank_change}`);
  return h("span", { class: "delta new" }, "=");
}
function renderTrending() {
  renderCompLegend();
  const items = trendData?.items || [];
  if (!items.length) {
    $("#trend-list").replaceChildren(h("li", { class: "empty" }, "Chưa có dữ liệu. Hãy chạy crawler và batch job."));
    return;
  }
  const maxF = Math.max(...items.map((x) => x.final), 1e-9);
  const colors = Object.fromEntries(COMP.map(([k, , v]) => [k, css(v)]));
  $("#trend-list").replaceChildren(...items.map((it) => {
    const c = comps(it);
    const tip = [`Toàn quốc ${(it.national || 0).toFixed(2)}`,
      it.trend_ratio ? `Google Trends ${(it.trends || 0).toFixed(2)} (gấp ${(it.trend_lift || 0).toFixed(1)} lần trung bình các tỉnh)` : "Google Trends: chưa có",
      `${num(it.mention_authors)} người bình luận (${num(it.self_mentions)} tự nhận ở tỉnh)`,
      `realtime: ${num(it.rt_plays)} lượt nghe, ${num(it.rt_likes)} thích`].join(" · ");
    const bar = h("div", { class: "comp", title: tip },
      ...COMP.map(([k]) => k).filter((k) => c[k] > 0).map((k) =>
        h("span", { style: { width: `${(120 * c[k]) / maxF}px`, background: colors[k] } })));
    return h("li", {},
      h("span", { class: "rank" }, it.rank),
      h("div", { style: { minWidth: 0 } }, h("div", { class: "t-title" }, it.title || it.track_key),
        h("div", { class: "t-meta" }, artistsText(it.artists)), bar),
      h("div", { style: { textAlign: "right" } }, genreTag(it.genre), h("div", {}, deltaEl(it))));
  }));
  const run = trendData.batch_run;
  $("#trend-sub").textContent = run ? `Batch view tính lúc ${new Date(run.run_ts).toLocaleString("vi-VN")} · realtime 60 phút gần nhất` : "Batch view chưa chạy - đang dùng realtime view";
}
$("#map-metric").addEventListener("change", drawMarkers);
$("#prov-select").addEventListener("change", (e) => selectProvince(e.target.value));
$$("[data-mode]").forEach((b) => b.addEventListener("click", () => {
  mode = b.dataset.mode;
  $$("[data-mode]").forEach((x) => x.setAttribute("aria-pressed", x === b ? "true" : "false"));
  loadTrending();
}));
redraws.map = () => { drawMarkers(); renderTrending(); };

// ================================================================== 2. BXH ĐA NỀN TẢNG
async function loadChartsView() {
  const [nat, cl, rising, overlap, artists] = await Promise.allSettled([
    api("/api/national?limit=30"), api("/api/charts"), api("/api/analytics/rising"),
    api("/api/analytics/platform-overlap"), api("/api/analytics/artists?scope=VN"),
  ]);
  if (nat.status === "fulfilled") renderNational(nat.value.items);
  if (cl.status === "fulfilled") {
    const sel = $("#chart-select");
    const cur = sel.value;
    const items = cl.value.items.filter((c) => c.chart_id !== "youtube_top_artists_vn_weekly")
      .sort((a, b) => (CHART_LABEL[a.chart_id] || a.chart_id).localeCompare(CHART_LABEL[b.chart_id] || b.chart_id));
    sel.replaceChildren(...items.map((c) => h("option", { value: c.chart_id }, CHART_LABEL[c.chart_id] || c.chart_id)));
    if (cur && items.some((c) => c.chart_id === cur)) sel.value = cur;
    loadOneChart();
  }
  if (rising.status === "fulfilled") renderRising(rising.value.items);
  redraws.overlap = () => overlap.status === "fulfilled" && renderOverlap(overlap.value.items);
  redraws.artists = () => artists.status === "fulfilled" && renderArtists(artists.value.items);
  redraws.overlap();
  redraws.artists();
}
function renderNational(items) {
  const t = $("#nat-table");
  if (!items.length) { t.replaceChildren(h("tr", {}, h("td", { class: "empty" }, "Chưa có batch view."))); return; }
  const max = Math.max(...items.map((x) => x.national_score || 0), 1e-9);
  t.replaceChildren(
    h("thead", {}, h("tr", {}, h("th", { class: "num" }, "#"), h("th", {}, "Bài hát"), h("th", {}, "Có mặt trên"), h("th", {}, "Điểm"))),
    h("tbody", {}, ...items.map((x, i) => {
      const ranks = (x.chart_ranks || []).filter((r) => SHORT_CHART[r.chart_id]).sort((a, b) => a.rank - b.rank)
        .map((r) => `${SHORT_CHART[r.chart_id]} #${r.rank}`).join(" · ");
      return h("tr", {},
        h("td", { class: "num" }, i + 1),
        h("td", { style: { minWidth: "180px" } }, h("div", { class: "t-title" }, x.title), h("div", { class: "t-meta" }, artistsText(x.artists)),
          h("div", { class: "small muted" }, ranks)),
        h("td", { style: { width: "150px" } }, h("div", { class: "badges" }, ...(x.sources || []).map((s) => h("span", { class: "badge", title: SOURCE_LABEL[s] }, SOURCE_SHORT[s] || s)))),
        h("td", { style: { width: "96px" } }, h("div", { class: "bar-cell" }, h("div", { class: "b", style: { width: `${56 * (x.national_score || 0) / max}px` } }),
          h("span", { class: "small" }, (x.national_score || 0).toFixed(2)))));
    })));
}
async function loadOneChart() {
  const id = $("#chart-select").value;
  if (!id) return;
  try {
    const res = await api(`/api/charts/${encodeURIComponent(id)}?limit=30`);
    const items = res.items;
    const hasPrev = items.some((x) => x.previous_rank);  // Apple RSS / playlist Spotify không công bố hạng kỳ trước
    $("#chart-sub").textContent = items.length ? `Crawl lúc ${new Date(items[0].crawled_ms).toLocaleString("vi-VN")}` : "Chưa có dữ liệu";
    $("#chart-list").replaceChildren(...items.map((x) => {
      const moved = x.previous_rank ? x.previous_rank - x.rank : null;
      const metric = x.metric_value ? `${compact(x.metric_value)} ${x.metric_name === "streams" ? "stream" : x.metric_name === "weekly_views" ? "lượt xem/tuần" : "điểm"}` : "";
      return h("li", {}, h("span", { class: "rank" }, x.rank),
        h("div", { style: { minWidth: 0 } }, h("div", { class: "t-title" }, x.title), h("div", { class: "t-meta" }, artistsText(x.artists))),
        h("div", { style: { textAlign: "right" } }, h("div", { class: "small muted" }, metric),
          !hasPrev ? null : moved == null ? h("span", { class: "delta new" }, "mới") : moved > 0 ? h("span", { class: "delta up" }, `▲${moved}`)
            : moved < 0 ? h("span", { class: "delta down" }, `▼${-moved}`) : h("span", { class: "delta new" }, "=")));
    }));
  } catch (e) { console.warn(e); }
}
$("#chart-select").addEventListener("change", loadOneChart);
function renderRising(items) {
  items = items.sort((a, b) => b.momentum - a.momentum).slice(0, 12);
  $("#rising-list").replaceChildren(...(items.length ? items.map((x, i) => h("li", {},
    h("span", { class: "rank" }, i + 1),
    h("div", { style: { minWidth: 0 } }, h("div", { class: "t-title" }, x.title), h("div", { class: "t-meta" }, artistsText(x.artists))),
    h("div", { style: { textAlign: "right" } }, genreTag(x.genre),
      h("div", { class: "delta up" }, x.new_entries > 0 && x.rank_gain <= 0 ? "mới vào BXH" : `+${x.momentum.toFixed(2)}`))))
    : [h("li", { class: "empty" }, "Chưa có dữ liệu")]));
}
function renderOverlap(items) {
  const names = [...new Set(items.flatMap((x) => [x.chart_a, x.chart_b]))].sort();
  if (!names.length) return empty("overlap-chart", "Chưa có dữ liệu");
  const idx = Object.fromEntries(names.map((n, i) => [n, i]));
  const max = Math.max(...items.map((x) => x.jaccard), 0.01);
  const dark = css("--page") === "#0d0d0d";
  const info = {};
  // nhãn trong ô màu: chọn trắng/đen theo độ đậm của ô để luôn đủ tương phản
  const cell = (a, b, x) => {
    info[`${idx[a]}|${idx[b]}`] = x;
    return { value: [idx[a], idx[b], x.jaccard],
      label: { color: x.jaccard / max > 0.55 ? (dark ? "#0b0b0b" : "#ffffff") : css("--ink") } };
  };
  const data = items.flatMap((x) => [cell(x.chart_a, x.chart_b, x), cell(x.chart_b, x.chart_a, x)]);
  const labels = names.map((n) => SHORT_CHART[n] || n);
  chart("overlap-chart").setOption(Object.assign(baseOpt(), {
    grid: { left: 80, right: 16, top: 8, bottom: 70 },
    xAxis: axis({ type: "category", data: labels, splitLine: { show: false }, axisLabel: { color: css("--ink-2"), rotate: 0 } }),
    yAxis: axis({ type: "category", data: labels, splitLine: { show: false }, axisLabel: { color: css("--ink-2") } }),
    visualMap: { min: 0, max, dimension: 2, show: true, orient: "horizontal", left: "center", bottom: 0, itemWidth: 10, itemHeight: 120,
      text: ["nhiều bài chung", "ít"], textStyle: { color: css("--ink-2"), fontSize: 11 },
      inRange: { color: [css("--q100"), css("--q300"), css("--q500"), css("--q700")] } },
    tooltip: Object.assign(baseOpt().tooltip, { formatter: (p) => {
      const x = info[`${p.value[0]}|${p.value[1]}`];
      return `<div style="color:${css("--ink-2")}">${esc(CHART_LABEL[x.chart_a])} ↔ ${esc(CHART_LABEL[x.chart_b])}</div>` +
        tipRow(css("--q500"), `${(x.jaccard * 100).toFixed(1)}%`, "Jaccard (tỉ lệ bài chung)") +
        `<div>${esc(num(x.common))} bài chung · Spearman ${x.spearman == null ? "—" : x.spearman.toFixed(2)}</div>`;
    } }),
    series: [{ type: "heatmap", data, itemStyle: { borderColor: css("--surface"), borderWidth: 2, borderRadius: 4 },
      label: { show: true, formatter: (p) => `${(p.value[2] * 100).toFixed(0)}%`, fontSize: 11 }, emphasis: { itemStyle: { borderColor: css("--ink"), borderWidth: 1 } } }],
  }), true);
}
function renderArtists(items) {
  items = items.filter((x) => x.scope === "VN").sort((a, b) => b.points - a.points).slice(0, 15).reverse();
  if (!items.length) return empty("artist-chart", "Chưa có dữ liệu");
  chart("artist-chart").setOption(Object.assign(baseOpt(), {
    grid: { left: 130, right: 48, top: 8, bottom: 24 },
    xAxis: axis({ type: "value" }),
    yAxis: axis({ type: "category", data: items.map((x) => x.artist), splitLine: { show: false },
      axisLabel: { color: css("--ink-2"), width: 120, overflow: "truncate" } }),
    tooltip: Object.assign(baseOpt().tooltip, { trigger: "item", formatter: (p) => {
      const x = items[p.dataIndex];
      return `<div>${esc(x.artist)}</div>` + tipRow(css("--s1"), x.points.toFixed(2), "điểm BXH") +
        `<div style="color:${css("--ink-2")}">${esc(x.tracks)} bài · ${esc(x.platforms)} nền tảng</div>`;
    } }),
    series: [{ type: "bar", data: items.map((x) => x.points), barMaxWidth: 16, itemStyle: { color: css("--s1"), borderRadius: [0, 4, 4, 0] },
      label: { show: true, position: "right", color: css("--ink-2"), fontSize: 11, formatter: (p) => p.value.toFixed(1) } }],
  }), true);
}

// ================================================================== 3. PHÂN TÍCH
let gscope = "region", genreRows = [], hourRows = [], summaryRows = [], agreementRows = [];
async function loadAnalytics() {
  const [g, hr, s, ag] = await Promise.allSettled([api("/api/analytics/genre-province"), api("/api/analytics/hourly"),
    api("/api/analytics/province-summary"), api("/api/analytics/signal-agreement")]);
  agreementRows = ag.status === "fulfilled" ? ag.value.items : [];
  genreRows = g.status === "fulfilled" ? g.value.items : [];
  hourRows = hr.status === "fulfilled" ? hr.value.items : [];
  summaryRows = s.status === "fulfilled" ? s.value.items : [];
  redraws.analytics();
}
function renderGenre() {
  $("#genre-legend").replaceChildren(...GROUP_NAMES.map((g) => h("span", {}, h("i", { style: { background: groupColor(g) } }), g)));
  if (!genreRows.length) return empty("genre-chart", "Chưa có dữ liệu lượt nghe");
  const key = gscope === "region" ? "region" : "province_name";
  const rows = {};
  for (const r of genreRows) {
    const k = r[key] || "?";
    rows[k] = rows[k] || {};
    const gname = genreGroup(r.genre);
    rows[k][gname] = (rows[k][gname] || 0) + (r.plays || 0);
  }
  let cats = Object.keys(rows);
  if (gscope === "region") cats = ["Bắc", "Trung", "Nam"].filter((c) => rows[c]);
  else cats.sort((a, b) => (rows[a]["V-Pop"] || 0) / sum(rows[a]) - (rows[b]["V-Pop"] || 0) / sum(rows[b]));
  function sum(o) { return Object.values(o).reduce((a, b) => a + b, 0) || 1; }
  const el = document.getElementById("genre-chart");
  el.style.height = gscope === "region" ? "260px" : `${Math.max(320, cats.length * 22 + 60)}px`;
  chart("genre-chart").resize();
  chart("genre-chart").setOption(Object.assign(baseOpt(), {
    grid: { left: gscope === "region" ? 70 : 120, right: 16, top: 4, bottom: 28 },
    xAxis: axis({ type: "value", max: 100, axisLabel: { color: css("--muted"), formatter: "{value}%" } }),
    yAxis: axis({ type: "category", data: cats.map((c) => (gscope === "region" ? `Miền ${c}` : c)), splitLine: { show: false },
      axisLabel: { color: css("--ink-2") } }),
    tooltip: Object.assign(baseOpt().tooltip, { trigger: "axis", axisPointer: { type: "shadow", shadowStyle: { color: "rgba(127,127,127,.08)" } },
      formatter: (ps) => `<div style="color:${css("--ink-2")}">${esc(ps[0].name)}</div>` +
        ps.filter((p) => p.value > 0).sort((a, b) => b.value - a.value).map((p) => tipRow(p.color, `${p.value.toFixed(1)}%`, p.seriesName)).join("") }),
    series: GROUP_NAMES.map((g) => ({
      name: g, type: "bar", stack: "s", barMaxWidth: 22, emphasis: { focus: "series" },
      itemStyle: { color: groupColor(g), borderColor: css("--surface"), borderWidth: 1 },
      data: cats.map((c) => +(100 * (rows[c][g] || 0) / sum(rows[c])).toFixed(2)),
    })),
  }), true);
}
function renderHourly() {
  const region = $("#hour-region").value;
  const grid = {};
  for (const r of hourRows) if (!region || r.region === region) grid[`${r.dow}|${r.hour}`] = (grid[`${r.dow}|${r.hour}`] || 0) + r.listens;
  if (!Object.keys(grid).length) return empty("hour-chart", "Chưa có dữ liệu lượt nghe");
  const days = [[2, "T2"], [3, "T3"], [4, "T4"], [5, "T5"], [6, "T6"], [7, "T7"], [1, "CN"]];
  const data = [];
  days.forEach(([d], yi) => { for (let hr = 0; hr < 24; hr++) data.push([hr, yi, grid[`${d}|${hr}`] || 0]); });
  const max = Math.max(...data.map((x) => x[2]), 1);
  chart("hour-chart").setOption(Object.assign(baseOpt(), {
    grid: { left: 36, right: 12, top: 8, bottom: 60 },
    xAxis: axis({ type: "category", data: [...Array(24).keys()].map((x) => `${x}h`), splitLine: { show: false } }),
    yAxis: axis({ type: "category", data: days.map((d) => d[1]), splitLine: { show: false }, axisLabel: { color: css("--ink-2") } }),
    visualMap: { min: 0, max, dimension: 2, orient: "horizontal", left: "center", bottom: 0, itemWidth: 10, itemHeight: 120, text: ["nhiều", "ít"],
      textStyle: { color: css("--ink-2"), fontSize: 11 }, inRange: { color: [css("--q100"), css("--q300"), css("--q500"), css("--q700")] } },
    tooltip: Object.assign(baseOpt().tooltip, { formatter: (p) =>
      `<div style="color:${css("--ink-2")}">${days[p.data[1]][1]}, ${p.data[0]}h–${p.data[0] + 1}h</div>` + tipRow(css("--q500"), num(p.data[2]), "lượt nghe") }),
    series: [{ type: "heatmap", data, itemStyle: { borderColor: css("--surface"), borderWidth: 2, borderRadius: 3 },
      emphasis: { itemStyle: { borderColor: css("--ink"), borderWidth: 1 } } }],
  }), true);
}
function renderMentions() {
  const items = summaryRows.filter((x) => x.mention_authors > 0).sort((a, b) => b.mention_authors - a.mention_authors).slice(0, 20).reverse();
  if (!items.length) return empty("mention-chart", "Chưa có bình luận nào nhắc tỉnh");
  chart("mention-chart").setOption(Object.assign(baseOpt(), {
    grid: { left: 120, right: 40, top: 8, bottom: 24 },
    xAxis: axis({ type: "value", minInterval: 1 }),
    yAxis: axis({ type: "category", data: items.map((x) => x.province_name), splitLine: { show: false }, axisLabel: { color: css("--ink-2") } }),
    tooltip: Object.assign(baseOpt().tooltip, { trigger: "item", formatter: (p) => {
      const x = items[p.dataIndex];
      return `<div>${esc(x.province_name)}</div>` + tipRow(css("--s1"), num(x.mention_authors), "người bình luận nhắc tỉnh") +
        `<div style="color:${css("--ink-2")}">${esc(num(x.self_mentions))} tự nhận ở tỉnh · ${esc(num(x.mentioned_tracks))} bài khác nhau</div>`;
    } }),
    series: [{ type: "bar", data: items.map((x) => x.mention_authors), barMaxWidth: 16, itemStyle: { color: css("--s1"), borderRadius: [0, 4, 4, 0] },
      label: { show: true, position: "right", color: css("--ink-2"), fontSize: 11 } }],
  }), true);
}
function renderSummary() {
  const rows = [...summaryRows].sort((a, b) => (b.plays_7d + 50 * b.mentions) - (a.plays_7d + 50 * a.mentions));
  $("#summary-table").replaceChildren(
    h("thead", {}, h("tr", {}, h("th", {}, "Tỉnh"), h("th", { class: "num" }, "Bình luận"), h("th", { class: "num" }, "Nghe 7 ngày"),
      h("th", {}, "Độ tin cậy"), h("th", {}, "Thể loại chính"), h("th", {}, "#1 hiện tại"), h("th", {}, "Bài đặc trưng"))),
    h("tbody", {}, ...rows.map((r) => h("tr", {},
      h("td", {}, h("a", { href: "#", onclick: (e) => { e.preventDefault(); selectProvince(r.province_code); $('[data-view="map"]').click(); } }, r.province_name),
        h("div", { class: "small muted" }, `Miền ${r.region}`)),
      h("td", { class: "num" }, num(r.mentions)), h("td", { class: "num" }, num(r.plays_7d)),
      h("td", { title: (r.signals || []).join(", ") }, `${r.confidence_label || "thấp"} (${Math.round((r.confidence || 0) * 100)}%)`),
      h("td", {}, r.top_genre ? genreTag(r.top_genre) : "—"),
      h("td", {}, r.top_title || "—"),
      h("td", {}, r.fav_title ? h("span", {}, r.fav_title, h("span", { class: "small muted" }, ` ×${r.fav_lift}`)) : "—")))));
}
function renderAgreement() {
  const rows = agreementRows.filter((r) => r.rho_tn != null || r.rho_tc != null)
    .sort((a, b) => (a.rho_tn ?? 2) - (b.rho_tn ?? 2));
  const rho = (v, n) => (v == null ? h("span", { class: "muted" }, n ? `— (${n} bài)` : "—") : `${v.toFixed(2)} (${n} bài)`);
  $("#agreement-table").replaceChildren(
    h("thead", {}, h("tr", {}, h("th", {}, "Tỉnh"), h("th", { class: "num" }, "Trends ↔ toàn quốc"),
      h("th", { class: "num" }, "Trends ↔ bình luận"))),
    h("tbody", {}, ...(rows.length ? rows.map((r) => h("tr", {}, h("td", {}, r.province_name),
      h("td", { class: "num" }, rho(r.rho_tn, r.n_tn)), h("td", { class: "num" }, rho(r.rho_tc, r.n_tc))))
      : [h("tr", {}, h("td", { class: "empty", colspan: 3 }, "Chưa có số liệu Google Trends (CronJob crawl-trends chạy mỗi ngày)."))])));
}
redraws.analytics = () => { renderGenre(); renderHourly(); renderMentions(); renderSummary(); renderAgreement(); };
$$("[data-gscope]").forEach((b) => b.addEventListener("click", () => {
  gscope = b.dataset.gscope;
  $$("[data-gscope]").forEach((x) => x.setAttribute("aria-pressed", x === b ? "true" : "false"));
  renderGenre();
}));
$("#hour-region").addEventListener("change", renderHourly);

// ================================================================== 4. GỢI Ý REALTIME
let recoUser = store.get("reco_user", null), recoProv = store.get("reco_prov", "ha-noi"), lastClick = 0, latencyShown = true;
let lastRecoUpdate = 0;
function webUsers() { return store.get("web_users", []); }
async function loadReco() {
  if (!provinces.length) { try { provinces = (await api("/api/provinces")).items; } catch { /* */ } }
  fillProvinceSelects();
  $("#reco-prov").value = recoProv;
  await loadUsers();
  loadPickList();
  pollReco();
}
async function loadUsers() {
  const sel = $("#reco-user");
  let sims = [];
  try { sims = (await api(`/api/users?province=${encodeURIComponent(recoProv)}&limit=40`)).items; } catch { /* */ }
  const mine = webUsers().filter((u) => u.province === recoProv);
  const opts = [...mine.map((u) => h("option", { value: u.id }, `${u.id} (bạn)`)),
    ...sims.filter((s) => !s.user_id.startsWith("web-")).map((s) => h("option", { value: s.user_id }, `${s.user_id} · ${s.events_30m || 0} lượt/30'`))];
  sel.replaceChildren(...(opts.length ? opts : [h("option", { value: "" }, "— chưa có, bấm + Người nghe mới —")]));
  if (recoUser && [...sel.options].some((o) => o.value === recoUser)) sel.value = recoUser;
  recoUser = sel.value || null;
  $("#reco-who").textContent = recoUser || "—";
}
$("#reco-prov").addEventListener("change", async (e) => {
  recoProv = e.target.value; store.set("reco_prov", recoProv);
  await loadUsers(); loadPickList(); pollReco(true);
});
$("#reco-user").addEventListener("change", (e) => { recoUser = e.target.value; store.set("reco_user", recoUser); $("#reco-who").textContent = recoUser; pollReco(true); });
$("#new-user").addEventListener("click", async () => {
  const id = `web-${Math.random().toString(36).slice(2, 7)}`;
  store.set("web_users", [...webUsers(), { id, province: recoProv }]);
  recoUser = id; store.set("reco_user", id);
  await loadUsers();
  pollReco(true);
});
let searchTimer;
$("#search").addEventListener("input", () => { clearTimeout(searchTimer); searchTimer = setTimeout(loadPickList, 300); });
async function loadPickList() {
  const q = $("#search").value.trim();
  let items = [];
  try {
    items = q ? (await api(`/api/search?q=${encodeURIComponent(q)}&limit=15`)).items
      : (await api(`/api/trending?province=${encodeURIComponent(recoProv)}&limit=15`)).items;
  } catch { /* */ }
  $("#pick-list").replaceChildren(...(items.length ? items.map((x, i) => h("li", {},
    h("span", { class: "rank" }, i + 1),
    h("div", { style: { minWidth: 0 } }, h("div", { class: "t-title" }, x.title || x.track_key), h("div", { class: "t-meta" }, artistsText(x.artists))),
    h("div", { class: "track-actions" },
      h("button", { class: "act primary", onclick: () => listen(x, "play"), title: "Nghe hết bài" }, "▶ Nghe"),
      h("button", { class: "act", onclick: () => listen(x, "skip"), title: "Bỏ qua sau vài giây" }, "⏭"),
      h("button", { class: "act", onclick: () => listen(x, "like"), title: "Thích" }, "♥"))))
    : [h("li", { class: "empty" }, q ? "Không tìm thấy bài nào." : "Chưa có dữ liệu xu hướng.")]));
}
async function listen(track, action) {
  if (!recoUser) { $("#new-user").click(); await new Promise((r) => setTimeout(r, 300)); }
  const t0 = Date.now();
  try {
    const res = await api("/api/listen", { method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ user_id: recoUser, province_code: recoProv, track_key: track.track_key, action }) });
    lastClick = t0; latencyShown = false;
    const line = `${new Date().toLocaleTimeString("vi-VN")}  ${action.padEnd(4)}  ${track.title}  → ${res.kafka.topic} p${res.kafka.partition} offset ${res.kafka.offset}`;
    $("#event-log").prepend(h("div", {}, line));
  } catch (e) {
    $("#event-log").prepend(h("div", { style: { color: css("--bad") } }, `Lỗi gửi sự kiện: ${e.message}`));
  }
  pollReco(true);
}
let recoTimer;
async function pollReco(immediate) {
  clearTimeout(recoTimer);
  if (currentView === "reco" && recoUser) {
    try {
      const r = await api(`/api/recommend/${encodeURIComponent(recoUser)}?province=${encodeURIComponent(recoProv)}`);
      renderReco(r);
    } catch (e) { console.warn(e); }
  }
  if (currentView === "reco") recoTimer = setTimeout(pollReco, immediate ? 1000 : 2000);
}
function renderReco(r) {
  const meta = [];
  if (r.cold_start) meta.push(h("span", {}, "Người nghe mới: đang dùng xu hướng của tỉnh. Hãy nghe vài bài để nhận gợi ý riêng."));
  else {
    meta.push(h("span", {}, `Cập nhật ${ago(r.updated_at)}`), h("span", {}, `${r.events_30m || 0} sự kiện trong 30 phút`));
    if (lastClick && !latencyShown && r.updated_at >= lastClick) {
      latencyShown = true;
      meta.push(h("b", {}, `Độ trễ end-to-end: ${((r.updated_at - lastClick) / 1000).toFixed(1)} giây`));
    }
    const based = (r.based_on || []).map((b) => `${b.action === "like" ? "♥ " : b.action === "skip" ? "⏭ " : ""}${b.title}`).join(" · ");
    if (based) meta.push(h("div", { style: { width: "100%" } }, `Dựa trên: ${based}`));
  }
  $("#reco-meta").replaceChildren(...meta);
  const changed = r.updated_at && r.updated_at !== lastRecoUpdate;
  lastRecoUpdate = r.updated_at;
  $("#reco-list").replaceChildren(...(r.tracks || []).map((t) => h("li", { class: changed ? "flash" : "" },
    h("span", { class: "rank" }, t.rank),
    h("div", { style: { minWidth: 0 } }, h("div", { class: "t-title" }, t.title || t.track_key),
      h("div", { class: "t-meta" }, artistsText(t.artists)), h("div", { class: "reason" }, t.reason || "")),
    h("div", { class: "track-actions" }, genreTag(t.genre),
      h("button", { class: "act", onclick: () => listen(t, "play"), title: "Nghe bài này" }, "▶")))));
}

// ================================================================== 5. PIPELINE
let tlData = [];
async function loadPipeline() {
  const [m, tl, cl, st] = await Promise.allSettled([api("/api/meta"), api("/api/realtime/timeline?minutes=60"),
    api("/api/charts"), api("/api/realtime/streaming?minutes=30")]);
  if (m.status === "fulfilled") renderKpis(m.value);
  if (st.status === "fulfilled") renderStreaming(st.value.latest);
  if (tl.status === "fulfilled") { tlData = tl.value.items; renderTimeline(); }
  if (cl.status === "fulfilled") renderFresh(cl.value.items);
}
function renderKpis(mt) {
  const k = mt.kpis || {};
  const tile = (label, value, note) => h("div", { class: "kpi" }, h("div", { class: "label" }, label), h("div", { class: "value" }, value),
    note ? h("div", { class: "note" }, note) : null);
  $("#kpis").replaceChildren(
    tile("Bài hát trong catalog", compact(k.catalog_tracks), `${num(k.aliases_merged)} bài gộp trùng giữa nền tảng`),
    tile("Bản ghi BXH", compact(k.chart_records), `${compact(k.playlist_records)} bản ghi playlist`),
    tile("Bình luận YouTube", compact(k.comments_unique), `${num(k.province_mentions)} người nhắc tỉnh, ${num(k.self_mentions)} tự nhận ở tỉnh`),
    tile("Google Trends", compact(k.trends_records), `${num(k.trends_tracks)} bài × 63 tỉnh cũ`),
    tile("Tỉnh đủ dữ liệu riêng", `${num((k.provinces_high_confidence || 0) + (k.provinces_mid_confidence || 0))}/34`,
      `${num(k.provinces_high_confidence)} tin cậy cao`),
    tile("Sự kiện nghe", compact(k.events_window), `${k.event_days || 7} ngày gần nhất`),
    tile("Master dataset (HDFS)", bytes(k.lake_bytes), "parquet, phân vùng theo topic/ngày"),
    tile("Batch view", mt.batch_views ? ago(mt.batch_views.run_ts) : "chưa chạy", mt.batch_views ? `chạy ${mt.batch_views.duration_sec} giây` : null),
    tile("Item similarity", mt.batch_similarity ? ago(mt.batch_similarity.run_ts) : "chưa chạy",
      mt.batch_similarity ? `${compact(mt.batch_similarity.pairs)} cặp bài tương tự` : null),
  );
  const src = (k.sources || []).sort((a, b) => b.records - a.records);
  $("#source-table").replaceChildren(
    h("thead", {}, h("tr", {}, h("th", {}, "Nền tảng"), h("th", { class: "num" }, "Bản ghi"), h("th", { class: "num" }, "Snapshot"), h("th", {}, "Crawl gần nhất"))),
    h("tbody", {}, ...src.map((s) => h("tr", {}, h("td", {}, SOURCE_LABEL[s.source] || s.source), h("td", { class: "num" }, num(s.records)),
      h("td", { class: "num" }, num(s.snapshots)), h("td", {}, ago(s.last_crawl_ms))))));
}
const QUERY_LABEL = { ingest_raw: "Kafka → HDFS (master dataset)", rt_plays: "Lượt nghe theo tỉnh/phút",
  rt_mentions: "Bình luận nhắc tỉnh", rt_charts: "BXH realtime", recommend: "Gợi ý playlist" };
function renderStreaming(items) {
  items = items.sort((a, b) => a.query.localeCompare(b.query));
  $("#stream-table").replaceChildren(
    h("thead", {}, h("tr", {}, h("th", {}, "Query"), h("th", { class: "num" }, "Batch"), h("th", { class: "num" }, "Vào (dòng/s)"),
      h("th", { class: "num" }, "Xử lý (dòng/s)"), h("th", { class: "num" }, "Thời gian batch"), h("th", {}, "Cập nhật"))),
    h("tbody", {}, ...(items.length ? items.map((q) => h("tr", {},
      h("td", {}, QUERY_LABEL[q.query] || q.query), h("td", { class: "num" }, num(q.batch_id)),
      h("td", { class: "num" }, (q.input_rps || 0).toFixed(1)), h("td", { class: "num" }, (q.processed_rps || 0).toFixed(1)),
      h("td", { class: "num" }, q.duration_ms != null ? `${(q.duration_ms / 1000).toFixed(1)} s` : "—"), h("td", {}, ago(q.ts_ms))))
      : [h("tr", {}, h("td", { class: "empty", colspan: 6 }, "Speed layer chưa ghi số liệu."))])));
}
function renderFresh(items) {
  items = items.sort((a, b) => b.last_crawl_ms - a.last_crawl_ms);
  $("#fresh-table").replaceChildren(
    h("thead", {}, h("tr", {}, h("th", {}, "BXH"), h("th", { class: "num" }, "Số dòng"), h("th", {}, "Cập nhật"))),
    h("tbody", {}, ...items.map((c) => h("tr", {}, h("td", {}, CHART_LABEL[c.chart_id] || c.chart_id),
      h("td", { class: "num" }, num(c.entries)), h("td", {}, ago(c.last_crawl_ms))))));
}
function renderTimeline() {
  const series = [["plays", "Nghe hết", "--s1"], ["skips", "Bỏ qua", "--s2"]];
  $("#tl-legend").replaceChildren(...series.map(([, l, v]) => h("span", {}, h("i", { style: { background: css(v), height: "2px", width: "12px" } }), l)));
  if (!tlData.length) return empty("timeline-chart", "Chưa có sự kiện nghe trong 60 phút qua");
  chart("timeline-chart").setOption(Object.assign(baseOpt(), {
    grid: { left: 48, right: 44, top: 12, bottom: 28 },
    xAxis: axis({ type: "time", splitLine: { show: false }, axisLabel: { color: css("--muted"), formatter: "{HH}:{mm}" } }),
    yAxis: axis({ type: "value", minInterval: 1 }),
    tooltip: Object.assign(baseOpt().tooltip, { trigger: "axis", axisPointer: { type: "line", lineStyle: { color: css("--axis"), width: 1 } },
      formatter: (ps) => `<div style="color:${css("--ink-2")}">${new Date(ps[0].value[0]).toLocaleTimeString("vi-VN", { hour: "2-digit", minute: "2-digit" })}</div>` +
        ps.map((p) => tipRow(p.color, num(p.value[1]), p.seriesName, true)).join("") }),
    series: series.map(([k, name, v]) => ({
      name, type: "line", showSymbol: false, symbolSize: 8, lineStyle: { width: 2, color: css(v) }, itemStyle: { color: css(v) },
      data: tlData.map((x) => [x.minute, x[k]]),
      endLabel: { show: true, color: css("--ink-2"), fontSize: 11, formatter: (p) => num(p.value[1]) },
    })),
  }), true);
}
redraws.pipeline = renderTimeline;

// ------------------------------------------------------------------ khởi động & làm mới định kỳ
applyTheme(store.get("theme", null), false);
loadStatus();
loadMapView();
setInterval(loadStatus, 10000);
setInterval(() => { if (document.visibilityState === "visible" && currentView !== "reco") refreshView(); }, 15000);
