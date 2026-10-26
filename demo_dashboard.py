#!/usr/bin/env python3
"""
VN MUSIC PULSE — INTERACTIVE DEMO DASHBOARD
Standalone zero-dependency web server visualizing crawled outputs from:
- Apple Music (100 charts)
- Zing MP3 (100 charts)
- Spotify (50 charts)
- YouTube (Trending Videos & Real Comments)
- Google Trends (63 Provinces)
- Entity Resolution (241 Canonical Tracks)
- Province Interest Estimator (2,892 Hotspots)
"""
import http.server
import json
import socketserver
import urllib.parse
from pathlib import Path

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "output"
PORT = 8088

def load_jsonl(filename: str) -> list[dict]:
    p = OUT / filename
    if not p.exists():
        return []
    records = []
    for line in p.read_text(encoding="utf-8").splitlines():
        if line.strip():
            try:
                records.append(json.loads(line))
            except Exception:
                pass
    return records

class DemoServerHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        super().end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        if path == "/api/stats":
            self.handle_stats()
        elif path == "/api/canonical":
            self.handle_canonical(query)
        elif path == "/api/provinces":
            self.handle_provinces(query)
        elif path == "/api/youtube":
            self.handle_youtube(query)
        elif path == "/" or path == "/index.html":
            self.serve_html()
        else:
            self.send_error(404, "Not Found")

    def handle_stats(self):
        canonical = load_jsonl("canonical_tracks.jsonl")
        province_interest = load_jsonl("province_interest.jsonl")
        youtube = load_jsonl("youtube_VN.jsonl")
        events = load_jsonl("user_events.jsonl")

        multi_platform = sum(1 for t in canonical if len(t.get("platforms_present", [])) >= 2)
        provinces = sorted(list({p.get("province") for p in province_interest if p.get("province")}))

        data = {
            "total_canonical_tracks": len(canonical),
            "multi_platform_tracks": multi_platform,
            "total_province_estimates": len(province_interest),
            "total_provinces": len(provinces),
            "total_user_events": len(events),
            "youtube_videos": sum(1 for x in youtube if x.get("event_type") == "regional_popular_video"),
            "youtube_comments": sum(1 for x in youtube if x.get("event_type") == "comment"),
            "provinces_list": provinces,
        }
        self.respond_json(data)

    def handle_canonical(self, query):
        canonical = load_jsonl("canonical_tracks.jsonl")
        only_multi = query.get("multi", ["false"])[0].lower() == "true"
        if only_multi:
            canonical = [t for t in canonical if len(t.get("platforms_present", [])) >= 2]
        self.respond_json(canonical)

    def handle_provinces(self, query):
        prov_interest = load_jsonl("province_interest.jsonl")
        target_province = query.get("province", [None])[0]
        if target_province:
            results = [p for p in prov_interest if p.get("province") == target_province]
        else:
            results = prov_interest
        # Top 50
        self.respond_json(results[:50])

    def handle_youtube(self, query):
        youtube = load_jsonl("youtube_VN.jsonl")
        videos = [x for x in youtube if x.get("event_type") == "regional_popular_video"]
        comments = [x for x in youtube if x.get("event_type") == "comment"]
        
        # Group comments by track_key
        comment_map = {}
        for c in comments:
            tk = c.get("track_key")
            comment_map.setdefault(tk, []).append(c)

        for v in videos:
            tk = v.get("track_key")
            v["comments_list"] = comment_map.get(tk, [])[:10]

        self.respond_json(videos)

    def respond_json(self, data):
        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def serve_html(self):
        html = get_dashboard_html()
        payload = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

def get_dashboard_html():
    return """<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>VN Music Pulse — Demo Dashboard</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #090d16;
      --card-bg: rgba(22, 29, 47, 0.75);
      --card-border: rgba(255, 255, 255, 0.08);
      --accent: #6366f1;
      --accent-glow: rgba(99, 102, 241, 0.25);
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --zing: #9b51e0;
      --spotify: #1db954;
      --apple: #fa2d48;
      --youtube: #ff0000;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Plus Jakarta Sans', -apple-system, sans-serif;
      background: var(--bg);
      color: var(--text);
      min-height: 100vh;
      overflow-x: hidden;
      background-image: 
        radial-gradient(circle at 10% 20%, rgba(99, 102, 241, 0.12) 0%, transparent 40%),
        radial-gradient(circle at 90% 80%, rgba(236, 72, 153, 0.08) 0%, transparent 40%);
    }
    header {
      padding: 1.5rem 2rem;
      border-bottom: 1px solid var(--card-border);
      backdrop-filter: blur(16px);
      position: sticky;
      top: 0;
      z-index: 100;
      display: flex;
      justify-content: space-between;
      align-items: center;
      background: rgba(9, 13, 22, 0.85);
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 0.75rem;
    }
    .brand-logo {
      width: 40px;
      height: 40px;
      border-radius: 10px;
      background: linear-gradient(135deg, #6366f1, #ec4899);
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 1.25rem;
      font-weight: 800;
      box-shadow: 0 4px 16px var(--accent-glow);
    }
    .brand-title {
      font-size: 1.25rem;
      font-weight: 800;
      letter-spacing: -0.02em;
      background: linear-gradient(to right, #fff, #94a3b8);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
    }
    .brand-badge {
      font-size: 0.7rem;
      padding: 0.2rem 0.5rem;
      border-radius: 9999px;
      background: rgba(99, 102, 241, 0.2);
      color: #a5b4fc;
      border: 1px solid rgba(99, 102, 241, 0.4);
      font-weight: 600;
    }
    .container {
      max-width: 1400px;
      margin: 0 auto;
      padding: 2rem;
    }
    .stats-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 1.25rem;
      margin-bottom: 2rem;
    }
    .stat-card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 16px;
      padding: 1.25rem;
      backdrop-filter: blur(12px);
      transition: transform 0.2s, border-color 0.2s;
    }
    .stat-card:hover {
      transform: translateY(-3px);
      border-color: rgba(99, 102, 241, 0.4);
    }
    .stat-label {
      font-size: 0.8rem;
      font-weight: 600;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
      margin-bottom: 0.5rem;
    }
    .stat-val {
      font-size: 1.85rem;
      font-weight: 800;
      color: #fff;
    }
    .tabs {
      display: flex;
      gap: 0.75rem;
      border-bottom: 1px solid var(--card-border);
      padding-bottom: 1rem;
      margin-bottom: 1.75rem;
    }
    .tab-btn {
      background: transparent;
      border: 1px solid transparent;
      padding: 0.6rem 1.2rem;
      border-radius: 10px;
      color: var(--text-muted);
      font-weight: 600;
      font-size: 0.9rem;
      cursor: pointer;
      transition: all 0.2s;
      font-family: inherit;
    }
    .tab-btn:hover {
      color: #fff;
      background: rgba(255, 255, 255, 0.05);
    }
    .tab-btn.active {
      color: #fff;
      background: rgba(99, 102, 241, 0.2);
      border-color: rgba(99, 102, 241, 0.5);
      box-shadow: 0 2px 10px var(--accent-glow);
    }
    .tab-content { display: none; }
    .tab-content.active { display: block; }
    .card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 16px;
      padding: 1.5rem;
      backdrop-filter: blur(12px);
    }
    table {
      width: 100%;
      border-collapse: collapse;
      text-align: left;
    }
    th {
      padding: 0.9rem 1rem;
      font-size: 0.75rem;
      font-weight: 700;
      color: var(--text-muted);
      text-transform: uppercase;
      border-bottom: 1px solid var(--card-border);
    }
    td {
      padding: 1rem;
      font-size: 0.9rem;
      border-bottom: 1px solid rgba(255, 255, 255, 0.04);
    }
    tr:hover td {
      background: rgba(255, 255, 255, 0.02);
    }
    .badge {
      display: inline-flex;
      align-items: center;
      gap: 0.3rem;
      padding: 0.25rem 0.6rem;
      border-radius: 6px;
      font-size: 0.75rem;
      font-weight: 700;
      margin-right: 0.3rem;
    }
    .badge-zing { background: rgba(155, 81, 224, 0.2); color: #d8b4fe; border: 1px solid rgba(155, 81, 224, 0.4); }
    .badge-spotify { background: rgba(29, 185, 84, 0.2); color: #86efac; border: 1px solid rgba(29, 185, 84, 0.4); }
    .badge-apple { background: rgba(250, 45, 72, 0.2); color: #fca5a5; border: 1px solid rgba(250, 45, 72, 0.4); }
    .badge-youtube { background: rgba(255, 0, 0, 0.2); color: #fca5a5; border: 1px solid rgba(255, 0, 0, 0.4); }
    .score-bar-bg {
      width: 120px;
      height: 8px;
      background: rgba(255, 255, 255, 0.1);
      border-radius: 9999px;
      overflow: hidden;
      display: inline-block;
      vertical-align: middle;
      margin-right: 0.5rem;
    }
    .score-bar-fill {
      height: 100%;
      background: linear-gradient(90deg, #6366f1, #ec4899);
      border-radius: 9999px;
    }
    select {
      background: rgba(15, 23, 42, 0.8);
      color: #fff;
      border: 1px solid var(--card-border);
      padding: 0.5rem 1rem;
      border-radius: 8px;
      font-family: inherit;
      font-size: 0.9rem;
      outline: none;
      cursor: pointer;
    }
    .comment-bubble {
      background: rgba(15, 23, 42, 0.6);
      border: 1px solid rgba(255, 255, 255, 0.05);
      border-radius: 10px;
      padding: 0.75rem 1rem;
      margin-top: 0.5rem;
      font-size: 0.85rem;
      color: #cbd5e1;
    }
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <div class="brand-logo">🎵</div>
      <div>
        <span class="brand-title">VN Music Pulse</span>
        <span class="brand-badge">LIVE DEMO</span>
      </div>
    </div>
    <div style="font-size: 0.85rem; color: var(--text-muted);">
      Hệ thống Big Data Phân tích Xu hướng & Gợi ý Âm nhạc
    </div>
  </header>

  <div class="container">
    <div class="stats-grid">
      <div class="stat-card">
        <div class="stat-label">Tổng bài hát chuẩn (Canonical)</div>
        <div class="stat-val" id="stat-canonical">-</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Bài xuất hiện Đa nền tảng</div>
        <div class="stat-val" id="stat-multi" style="color: #818cf8;">-</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Ước lượng theo Tỉnh thành</div>
        <div class="stat-val" id="stat-prov">-</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Video & Comments YouTube</div>
        <div class="stat-val" id="stat-yt" style="color: #f43f5e;">-</div>
      </div>
    </div>

    <div class="tabs">
      <button class="tab-btn active" onclick="switchTab('tab-provinces')">🇻🇳 Xu hướng 63 Tỉnh Thành</button>
      <button class="tab-btn" onclick="switchTab('tab-consensus')">🏆 Đối soát BXH Đa Nền Tảng</button>
      <button class="tab-btn" onclick="switchTab('tab-youtube')">📺 YouTube Trending & Comments</button>
    </div>

    <!-- TAB 1: PROVINCES -->
    <div id="tab-provinces" class="tab-content active">
      <div class="card">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1.5rem;">
          <div>
            <h2 style="font-size: 1.15rem; font-weight: 700;">Hotspots Âm Nhạc Theo Địa Phương</h2>
            <p style="font-size: 0.85rem; color: var(--text-muted);">Công thức: 50% Trends + 15% Comment Text Signal + 35% Events</p>
          </div>
          <div>
            <label style="font-size: 0.85rem; color: var(--text-muted); margin-right: 0.5rem;">Chọn tỉnh/thành:</label>
            <select id="province-select" onchange="loadProvinceData()">
              <option value="">Tất cả tỉnh thành</option>
            </select>
          </div>
        </div>
        <table>
          <thead>
            <tr>
              <th>Tỉnh / Thành</th>
              <th>Bài Hát</th>
              <th>Điểm Hot (0-100)</th>
              <th>Thành phần đo lường (Explainable)</th>
              <th>Độ Tin Cậy</th>
            </tr>
          </thead>
          <tbody id="province-table-body">
            <tr><td colspan="5" style="text-align: center; color: var(--text-muted);">Đang tải dữ liệu...</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- TAB 2: CONSENSUS -->
    <div id="tab-consensus" class="tab-content">
      <div class="card">
        <div style="margin-bottom: 1.5rem;">
          <h2 style="font-size: 1.15rem; font-weight: 700;">Entity Resolution: Ghép Nối Ca Khúc Đa Nền Tảng</h2>
          <p style="font-size: 0.85rem; color: var(--text-muted);">So sánh thứ hạng thực tế giữa Zing MP3, Spotify, Apple Music & YouTube</p>
        </div>
        <table>
          <thead>
            <tr>
              <th>Mã Chuẩn (ID)</th>
              <th>Tên Bài Hát</th>
              <th>Nghệ Sĩ</th>
              <th>Nền tảng xuất hiện</th>
              <th>Thứ hạng chéo các BXH</th>
            </tr>
          </thead>
          <tbody id="consensus-table-body">
            <tr><td colspan="5" style="text-align: center; color: var(--text-muted);">Đang tải dữ liệu...</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- TAB 3: YOUTUBE -->
    <div id="tab-youtube" class="tab-content">
      <div class="card">
        <div style="margin-bottom: 1.5rem;">
          <h2 style="font-size: 1.15rem; font-weight: 700;">Top Video Âm Nhạc YouTube & Khai Thác Bình Luận</h2>
          <p style="font-size: 0.85rem; color: var(--text-muted);">Dữ liệu cào trực tiếp qua YouTube Data API v3 chính thức</p>
        </div>
        <div id="youtube-container">Đang tải...</div>
      </div>
    </div>
  </div>

  <script>
    function switchTab(tabId) {
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
      event.target.classList.add('active');
      document.getElementById(tabId).classList.add('active');
    }

    async function init() {
      // Load Stats
      const resStats = await fetch('/api/stats');
      const stats = await resStats.json();
      document.getElementById('stat-canonical').innerText = stats.total_canonical_tracks.toLocaleString();
      document.getElementById('stat-multi').innerText = stats.multi_platform_tracks.toLocaleString();
      document.getElementById('stat-prov').innerText = stats.total_province_estimates.toLocaleString();
      document.getElementById('stat-yt').innerText = `${stats.youtube_videos} MV / ${stats.youtube_comments} cmt`;

      // Fill province select
      const sel = document.getElementById('province-select');
      stats.provinces_list.forEach(p => {
        const opt = document.createElement('option');
        opt.value = p;
        opt.innerText = p;
        sel.appendChild(opt);
      });

      loadProvinceData();
      loadConsensusData();
      loadYoutubeData();
    }

    async function loadProvinceData() {
      const p = document.getElementById('province-select').value;
      const res = await fetch(`/api/provinces${p ? '?province=' + encodeURIComponent(p) : ''}`);
      const data = await res.json();
      const tb = document.getElementById('province-table-body');
      tb.innerHTML = '';
      data.forEach(item => {
        const c = item.components;
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td><strong>${item.province}</strong></td>
          <td style="font-weight: 600; color: #fff;">${item.track_title}</td>
          <td>
            <div class="score-bar-bg"><div class="score-bar-fill" style="width: ${Math.min(100, item.estimated_interest * 2.5)}%;"></div></div>
            <strong>${item.estimated_interest}</strong>
          </td>
          <td style="font-size: 0.8rem; color: var(--text-muted);">
            Trends: ${(c.trends_normalized*100).toFixed(0)}% | Cmt: ${(c.comments_signal_normalized*100).toFixed(0)}% | Events: ${(c.events_normalized*100).toFixed(0)}%
          </td>
          <td><span class="badge" style="background: rgba(99,102,241,0.2); color: #a5b4fc;">${(item.confidence*100).toFixed(0)}%</span></td>
        `;
        tb.appendChild(tr);
      });
    }

    async function loadConsensusData() {
      const res = await fetch('/api/canonical?multi=true');
      const data = await res.json();
      const tb = document.getElementById('consensus-table-body');
      tb.innerHTML = '';
      data.forEach(t => {
        const plats = t.platforms_present.map(p => {
          if (p === 'zing_mp3') return '<span class="badge badge-zing">Zing MP3</span>';
          if (p === 'spotify') return '<span class="badge badge-spotify">Spotify</span>';
          if (p === 'apple_music') return '<span class="badge badge-apple">Apple</span>';
          if (p === 'youtube') return '<span class="badge badge-youtube">YouTube</span>';
          return `<span class="badge">${p}</span>`;
        }).join('');

        const ranks = Object.entries(t.cross_platform_ranks || {}).map(([p, r]) => {
          return `<strong>${p}</strong>: #${r}`;
        }).join(' | ');

        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td style="font-family: monospace; font-size: 0.8rem; color: #94a3b8;">${t.canonical_track_id}</td>
          <td style="font-weight: 700; color: #fff;">${t.title}</td>
          <td>${t.artists.join(', ')}</td>
          <td>${plats}</td>
          <td style="color: #cbd5e1;">${ranks || 'N/A'}</td>
        `;
        tb.appendChild(tr);
      });
    }

    async function loadYoutubeData() {
      const res = await fetch('/api/youtube');
      const data = await res.json();
      const container = document.getElementById('youtube-container');
      container.innerHTML = '';
      data.slice(0, 8).forEach(v => {
        const m = v.metrics || {};
        const cmts = (v.comments_list || []).map(c => `
          <div class="comment-bubble">
            💬 "${c.raw?.text || ''}"
            <div style="font-size: 0.75rem; color: #64748b; margin-top: 0.25rem;">👍 ${c.metrics?.like_count || 0} likes</div>
          </div>
        `).join('');

        const div = document.createElement('div');
        div.style.marginBottom = '1.5rem';
        div.style.paddingBottom = '1.5rem';
        div.style.borderBottom = '1px solid rgba(255,255,255,0.05)';
        div.innerHTML = `
          <div style="display: flex; justify-content: space-between; align-items: flex-start;">
            <div>
              <h3 style="font-size: 1rem; font-weight: 700; color: #fff;">${v.track_title}</h3>
              <p style="font-size: 0.85rem; color: var(--text-muted); margin-top: 0.25rem;">Kênh: ${v.artist}</p>
            </div>
            <div style="text-align: right; font-size: 0.85rem;">
              <span class="badge badge-youtube">YouTube MV</span>
              <div style="color: var(--text-muted); margin-top: 0.3rem;">👁️ ${(m.viewCount || 0).toLocaleString()} views | ❤️ ${(m.likeCount || 0).toLocaleString()} likes</div>
            </div>
          </div>
          <div style="margin-top: 0.75rem;">${cmts || '<p style="color: #64748b; font-size: 0.85rem;">Không có bình luận mẫu.</p>'}</div>
        `;
        container.appendChild(div);
      });
    }

    window.onload = init;
  </script>
</body>
</html>"""

def main():
    with socketserver.TCPServer(("", PORT), DemoServerHandler) as httpd:
        print(f"\n=======================================================")
        print(f"🎵 VN Music Pulse Demo Server is RUNNING at:")
        print(f"👉 http://localhost:{PORT}")
        print(f"=======================================================\n")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nDemo Server stopped.")

if __name__ == "__main__":
    main()
