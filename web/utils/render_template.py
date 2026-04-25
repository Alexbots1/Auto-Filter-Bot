from info import BIN_CHANNEL, URL
from utils import temp
from web.utils.custom_dl import TGCustomYield
import urllib.parse
import html


webapp_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>Media Search</title>
    <script src="https://telegram.org/js/telegram-web-app.js"></script>
    <style>
        :root {
            --bg-main: #141414;
            --accent: #E50914;
            --accent-hover: #C11119;
            --text-main: #FFFFFF;
            --text-muted: #B3B3B3;
            --input-bg: #333333;
            --card-bg: #222222;
            --card-hover: #2F2F2F;
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
            background-color: var(--bg-main);
            color: var(--text-main);
            min-height: 100vh;
            padding: 24px 16px;
            padding-bottom: 90px;
            -webkit-font-smoothing: antialiased;
        }

        .header {
            margin-bottom: 24px;
            text-align: left;
            animation: fadeInDown 0.5s ease;
        }

        .greeting {
            font-size: 32px;
            font-weight: 700;
            margin-bottom: 6px;
            letter-spacing: -0.5px;
        }

        .greeting-name { color: var(--accent); }
        .subtitle { font-size: 15px; color: var(--text-muted); font-weight: 400; }

        .search-container {
            display: flex;
            gap: 10px;
            position: sticky;
            top: 10px;
            z-index: 10;
            margin-bottom: 20px;
            background: var(--bg-main);
            padding: 10px 0;
            animation: fadeInUp 0.5s ease 0.1s both;
        }

        .input-wrapper {
            position: relative;
            flex-grow: 1;
            display: flex;
            align-items: center;
        }

        input[type="text"] {
            width: 100%;
            padding: 16px 45px 16px 20px;
            border-radius: 4px;
            border: 1px solid transparent;
            background: var(--input-bg);
            color: var(--text-main);
            font-size: 16px;
            outline: none;
            transition: all 0.2s ease;
        }

        input[type="text"]:focus { background: #404040; border-color: #555; }
        input[type="text"]::placeholder { color: #8C8C8C; }

        .clear-icon {
            position: absolute;
            right: 14px;
            width: 20px;
            height: 20px;
            color: #8C8C8C;
            cursor: pointer;
            display: none;
            transition: color 0.2s;
        }

        .clear-icon:hover { color: #FFFFFF; }

        .search-btn {
            background: var(--accent);
            color: #ffffff;
            border: none;
            border-radius: 4px;
            padding: 0 24px;
            font-weight: 600;
            font-size: 16px;
            cursor: pointer;
            transition: background 0.2s;
            display: flex;
            align-items: center;
            justify-content: center;
        }

        .search-btn:hover { background: var(--accent-hover); }
        .search-btn:active { transform: scale(0.98); }

        .section-title {
            font-size: 18px;
            font-weight: 600;
            margin-bottom: 12px;
            color: var(--text-main);
            animation: fadeInDown 0.4s ease;
        }

        .results-container {
            display: flex;
            flex-direction: column;
            gap: 8px;
        }

        .file-card {
            display: flex;
            justify-content: space-between;
            align-items: center;
            background: var(--card-bg);
            border-radius: 4px;
            padding: 16px 20px;
            cursor: pointer;
            transition: all 0.2s ease;
            animation: fadeInUp 0.4s ease;
            border-left: 4px solid transparent;
        }

        .file-card:hover {
            background: var(--card-hover);
            border-left: 4px solid var(--accent);
        }

        .file-card:active { background: #404040; }

        .file-info {
            display: flex;
            flex-direction: column;
            overflow: hidden;
            padding-right: 15px;
        }

        .file-name {
            font-weight: 500;
            font-size: 16px;
            line-height: 1.4;
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
            overflow: hidden;
            margin-bottom: 6px;
            color: var(--text-main);
        }

        .file-size { font-size: 13px; font-weight: 500; color: var(--text-muted); }

        .get-icon {
            display: flex;
            align-items: center;
            justify-content: center;
            width: 40px;
            height: 40px;
            background: transparent;
            border: 2px solid var(--text-muted);
            color: var(--text-muted);
            border-radius: 50%;
            font-size: 18px;
            flex-shrink: 0;
            transition: all 0.2s;
        }
        
        .file-card:hover .get-icon {
            border-color: var(--text-main);
            color: var(--text-main);
            background: rgba(255, 255, 255, 0.1);
        }

        .pagination {
            position: fixed;
            bottom: 20px;
            left: 50%;
            transform: translateX(-50%);
            width: calc(100% - 32px);
            max-width: 400px;
            display: none;
            justify-content: space-between;
            align-items: center;
            background: rgba(20, 20, 20, 0.95);
            padding: 12px 16px;
            border-radius: 8px;
            box-shadow: 0 -4px 20px rgba(0, 0, 0, 0.8);
            z-index: 20;
        }

        .page-btn {
            background: var(--card-bg);
            color: var(--text-main);
            border: 1px solid #404040;
            padding: 10px 20px;
            border-radius: 4px;
            font-weight: 500;
            font-size: 14px;
            cursor: pointer;
            transition: all 0.2s;
        }

        .page-btn:hover:not(:disabled) {
            background: var(--card-hover);
            border-color: var(--text-muted);
        }

        .page-btn:disabled { opacity: 0.3; cursor: not-allowed; }
        .page-indicator { font-weight: 500; font-size: 15px; color: var(--text-main); }

        .loader {
            text-align: center;
            padding: 40px 20px;
            color: var(--accent);
            font-weight: 500;
            display: none;
            animation: pulse 1.5s infinite;
        }

        @keyframes fadeInUp {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        @keyframes fadeInDown {
            from { opacity: 0; transform: translateY(-10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        @keyframes pulse {
            0% { opacity: 0.5; }
            50% { opacity: 1; }
            100% { opacity: 0.5; }
        }
    </style>
</head>
<body>

    <div class="header">
        <h1 class="greeting">Welcome, <span id="userName" class="greeting-name">Loading...</span></h1>
        <p class="subtitle">Find your favorite movies and series.</p>
    </div>

    <div class="search-container">
        <div class="input-wrapper">
            <input type="text" id="searchInput" placeholder="Titles, people, genres" onkeypress="handleEnter(event)" oninput="toggleClearIcon()">
            <svg id="clearIcon" class="clear-icon" onclick="clearSearch()" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <line x1="18" y1="6" x2="6" y2="18"></line>
                <line x1="6" y1="6" x2="18" y2="18"></line>
            </svg>
        </div>
        <button class="search-btn" onclick="performSearch(0)">Search</button>
    </div>

    <h2 id="sectionTitle" class="section-title">Recently Added</h2>
    <div id="loader" class="loader">Loading files...</div>
    <div id="results" class="results-container"></div>

    <div id="pagination" class="pagination">
        <button id="backBtn" class="page-btn" onclick="changePage('back')">Back</button>
        <div id="pageIndicator" class="page-indicator">1/1</div>
        <button id="nextBtn" class="page-btn" onclick="changePage('next')">Next</button>
    </div>

    <script>
        const tg = window.Telegram.WebApp;
        tg.expand();
        tg.setBackgroundColor('#141414');
        tg.setHeaderColor('#141414');

        const user = tg.initDataUnsafe?.user;
        const userNameElement = document.getElementById('userName');
        
        if (user && user.first_name) {
            userNameElement.innerText = user.first_name;
        } else {
            userNameElement.innerText = "Guest";
        }

        const userId = user?.id || 'unknown';

        let currentQuery = '';
        let currentOffset = 0;
        let nextOffset = null;
        let botUsername = '';
        let maxResultsPerPage = 10; // Default, will update from API

        function handleEnter(e) {
            if (e.key === 'Enter') performSearch(0);
        }

        function toggleClearIcon() {
            const input = document.getElementById('searchInput');
            const clearIcon = document.getElementById('clearIcon');
            if (input.value.length > 0) {
                clearIcon.style.display = 'block';
            } else {
                clearIcon.style.display = 'none';
                performSearch(0); 
            }
        }

        function clearSearch() {
            const input = document.getElementById('searchInput');
            input.value = '';
            toggleClearIcon(); 
            input.focus();
            performSearch(0);
        }

        async function performSearch(offset = 0) {
            const query = document.getElementById('searchInput').value.trim();
            const sectionTitle = document.getElementById('sectionTitle');
            
            if (query.length > 0) {
                sectionTitle.innerText = "Search Results";
            } else {
                sectionTitle.innerText = "Recently Added";
            }

            currentQuery = query;
            currentOffset = offset;

            document.getElementById('results').innerHTML = '';
            document.getElementById('loader').style.display = 'block';
            document.getElementById('pagination').style.display = 'none';

            try {
                const response = await fetch(`/api/search?q=${encodeURIComponent(query)}&offset=${offset}`);
                const data = await response.json();
                
                botUsername = data.bot_username;
                maxResultsPerPage = data.max_btn; 
                
                document.getElementById('loader').style.display = 'none';
                renderResults(data);
                renderPagination(data);
                
                window.scrollTo({ top: 0, behavior: 'smooth' });
            } catch (error) {
                document.getElementById('loader').innerText = 'Connection error. Please try again.';
            }
        }

        function renderResults(data) {
            const resultsDiv = document.getElementById('results');
            
            if (!data.files || data.files.length === 0) {
                resultsDiv.innerHTML = `
                    <div style="text-align:center; padding:60px 20px; color:var(--text-muted);">
                        <div style="font-size: 48px; margin-bottom: 16px;">🔍</div>
                        <h3 style="color: white; margin-bottom: 8px;">No matching results</h3>
                        <p>Explore more by checking your spelling or trying a different search term.</p>
                    </div>`;
                return;
            }

            data.files.forEach((file, index) => {
                const card = document.createElement('div');
                card.className = 'file-card';
                card.style.animationDelay = `${index * 0.05}s`;
                
                card.innerHTML = `
                    <div class="file-info">
                        <span class="file-name">${file.name}</span>
                        <span class="file-size">${file.size} HD</span>
                    </div>
                    <div class="get-icon">▶</div>
                `;

                card.onclick = () => {
                    const payload = `file_${file.id}`;
                    const link = `https://t.me/${botUsername}?start=${payload}`;
                    
                    if (userId === 'unknown') {
                        window.open(link, '_blank');
                    } else {
                        tg.openTelegramLink(link);
                        setTimeout(() => { tg.close(); }, 100);
                    }
                };

                resultsDiv.appendChild(card);
            });
        }

        function renderPagination(data) {
            const pagDiv = document.getElementById('pagination');
            nextOffset = data.next_offset;

            if (data.total_results <= data.max_btn) {
                pagDiv.style.display = 'none';
                return;
            }

            pagDiv.style.display = 'flex';

            const totalPages = Math.ceil(data.total_results / data.max_btn);
            const currentPage = Math.ceil(data.current_offset / data.max_btn) + 1;

            document.getElementById('pageIndicator').innerText = `Page ${currentPage} of ${totalPages}`;

            const backBtn = document.getElementById('backBtn');
            if (data.current_offset > 0) {
                backBtn.style.visibility = 'visible';
                backBtn.disabled = false;
            } else {
                backBtn.style.visibility = 'hidden';
            }

            const nextBtn = document.getElementById('nextBtn');
            if (nextOffset !== null) {
                nextBtn.style.visibility = 'visible';
                nextBtn.disabled = false;
            } else {
                nextBtn.style.visibility = 'hidden';
            }
        }

        function changePage(direction) {
            if (direction === 'next' && nextOffset !== null) {
                performSearch(nextOffset);
            } else if (direction === 'back') {
                let prevOffset = currentOffset - maxResultsPerPage;
                if (prevOffset < 0) prevOffset = 0;
                performSearch(prevOffset);
            }
        }

        // Trigger initial search on load
        window.onload = () => {
            performSearch(0);
        };
    </script>
</body>
</html>
"""
# ─────────────────────────────────────────────────────────────────────────────
# WATCH PAGE TEMPLATE  (route: /watch/{id})
# ─────────────────────────────────────────────────────────────────────────────
watch_tmplt = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{heading}</title>
    <!-- Plyr CSS -->
    <link rel="stylesheet" href="https://cdn.plyr.io/3.7.8/plyr.css">
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap">
    <style>
        :root {
            --p:#818cf8; --p2:#6366f1; --sec:#a78bfa; --acc:#38bdf8;
            --txt:#f1f5f9; --txt2:#94a3b8;
            --bg:#020617; --glass:rgba(10,18,38,.8); --gb:rgba(129,140,248,.13);
        }
        *, *::before, *::after { margin:0; padding:0; box-sizing:border-box; }
        body {
            font-family:'Inter',sans-serif;
            background:var(--bg); color:var(--txt);
            min-height:100vh;
            display:flex; flex-direction:column;
            overflow-x:hidden;
        }
        body::before {
            content:''; position:fixed; inset:0; z-index:-1;
            background:
                radial-gradient(ellipse 75% 50% at 10% 20%, rgba(99,102,241,.12) 0%, transparent 58%),
                radial-gradient(ellipse 60% 40% at 90% 80%, rgba(167,139,250,.09) 0%, transparent 55%),
                linear-gradient(160deg, #020617 0%, #070c1b 45%, #0f172a 100%);
        }

        /* Header */
        header {
            padding:.8rem 1.5rem;
            backdrop-filter:blur(24px) saturate(180%);
            -webkit-backdrop-filter:blur(24px) saturate(180%);
            background:var(--glass);
            border-bottom:1px solid var(--gb);
            display:flex; flex-direction:column; align-items:center; justify-content:center;
            box-shadow:0 1px 32px rgba(0,0,0,.45);
        }
        .header-logo {
            font-size:1rem; font-weight:800; letter-spacing:-.01em;
            background:linear-gradient(90deg,#e2e8f0 0%,var(--p) 50%,var(--acc) 100%);
            -webkit-background-clip:text; -webkit-text-fill-color:transparent; background-clip:text;
        }
        #file-name {
            font-size:.82rem; color:var(--txt2); margin-top:.3rem; font-weight:500;
            white-space:nowrap; overflow:hidden; text-overflow:ellipsis;
            max-width:100%; text-align:center;
        }

        /* Container */
        .container {
            flex:1; display:flex; flex-direction:column; align-items:center;
            padding:2.5rem 1.5rem 3rem; width:100%;
        }

        /* Badge */
        .badge {
            display:inline-flex; align-items:center; gap:.4rem;
            background:rgba(16,185,129,.12); border:1px solid rgba(16,185,129,.3);
            padding:.3rem .9rem; border-radius:30px;
            font-size:.7rem; font-weight:700; letter-spacing:.05em; text-transform:uppercase; color:#10b981;
            margin-bottom:1.5rem; backdrop-filter:blur(8px);
        }
        .badge-dot {
            width:6px; height:6px; background:#10b981; border-radius:50%;
            box-shadow:0 0 10px #10b981; animation:pulse 2s cubic-bezier(.4,0,.6,1) infinite;
        }
        @keyframes pulse { 50% { opacity:.3; box-shadow:none; } }

        /* Player Wrap */
        .player-wrap {
            position:relative; width:100%; max-width:1060px;
            border-radius:24px; padding:1px; z-index:10;
        }
        .player-ambient {
            position:absolute; inset:-2px; z-index:-1;
            background:linear-gradient(135deg,rgba(99,102,241,.4),rgba(167,139,250,.2),rgba(56,189,248,.3));
            filter:blur(35px); opacity:.3; transform:translateZ(0); border-radius:inherit;
        }
        .player-card {
            position:relative; background:#000; border-radius:22px;
            overflow:hidden; box-shadow:0 25px 65px rgba(0,0,0,.5);
            aspect-ratio:16/9; display:flex; align-items:center; justify-content:center;
            width:100%;
        }
        .player-card video, .plyr video {
            width:100% !important; height:100% !important;
            object-fit:cover !important; border-radius:22px;
        }

        /* Load Skeleton */
        .skeleton {
            position:absolute; inset:0; background:#0a0e1c; z-index:20;
            overflow:hidden; pointer-events:none; transition:opacity .4s, visibility .4s;
        }
        .skeleton::after {
            content:''; position:absolute; inset:0;
            background:linear-gradient(90deg,transparent,rgba(129,140,248,.08),transparent);
            transform:translateX(-100%); animation:shimmer 1.8s infinite;
        }
        @keyframes shimmer { 100% { transform:translateX(100%); } }
        .skeleton.gone { opacity:0; visibility:hidden; }

        /* Video Error Overlay */
        .player-err-overlay {
            position:absolute; inset:0; z-index:50;
            background:rgba(2,6,23,.92); backdrop-filter:blur(14px);
            opacity:0; visibility:hidden;
            display:flex; align-items:center; justify-content:center;
            border-radius:22px; text-align:center; padding:2rem;
            transition:opacity .4s ease, visibility .4s ease;
        }
        .player-err-overlay.show { opacity:1; visibility:visible; }
        .err-card-sm { max-width:440px; width:100%; }
        .err-card-sm h2 { font-size:1.4rem; font-weight:800; margin-bottom:.5rem; letter-spacing:-.02em; }
        .err-card-sm p { font-size:.85rem; color:var(--txt2); margin-bottom:1.5rem; line-height:1.5; }
        .err-btn-grid {
            display:grid; grid-template-columns:repeat(3, 1fr); gap:.6rem; margin-top:1.2rem;
        }

        /* Buttons */
        .btn-row {
            display:grid; grid-template-columns:repeat(3, 1fr); gap:.8rem;
            margin-top:1.2rem;
            width:100%; max-width:1060px;
        }
        .xbtn {
            position:relative; overflow:hidden;
            display:flex; align-items:center; justify-content:center; gap:.5rem;
            width:100%; padding:.72rem .9rem;
            border-radius:11px; border:none;
            font-family:'Inter',sans-serif;
            font-size:.84rem; font-weight:600;
            letter-spacing:.01em;
            cursor:pointer; text-decoration:none; color:#fff;
            transition:transform .2s, box-shadow .2s, filter .2s;
        }
        .xbtn::after {
            content:''; position:absolute; inset:0;
            background:rgba(255,255,255,.08);
            opacity:0; transition:opacity .18s;
        }
        .xbtn:hover::after { opacity:1; }
        .xbtn:hover {
            transform:scale(1.02);
            filter:brightness(1.08);
        }
        .xbtn:active { transform:scale(.98); }

        /* Download – indigo */
        .btn-dl {
            background:linear-gradient(135deg,#4f46e5,#818cf8,#a78bfa);
            box-shadow:0 4px 16px rgba(99,102,241,.38);
        }
        .btn-dl:hover { box-shadow:0 7px 24px rgba(99,102,241,.55); }

        /* VLC – amber */
        .btn-vlc {
            background:linear-gradient(135deg,#92400e,#f59e0b,#fde68a);
            box-shadow:0 4px 16px rgba(245,158,11,.35);
        }
        .btn-vlc:hover { box-shadow:0 7px 24px rgba(245,158,11,.52); }

        /* MX – emerald */
        .btn-mx {
            background:linear-gradient(135deg,#065f46,#10b981,#6ee7b7);
            box-shadow:0 4px 16px rgba(16,185,129,.35);
        }
        .btn-mx:hover { box-shadow:0 7px 24px rgba(16,185,129,.52); }

        /* Footer */
        footer {
            padding:.85rem 1.5rem; text-align:center;
            color:var(--txt2); font-size:.73rem;
            margin-top:auto;
        }
        footer::before {
            content:''; display:block;
            width:90px; height:1px;
            background:linear-gradient(90deg,transparent,rgba(129,140,248,.28),transparent);
            margin:0 auto .7rem;
        }
        .ha-link {
            color:var(--p); text-decoration:none; font-weight:600;
            transition:opacity .2s;
        }
        .ha-link:hover { opacity:.7; }

        /* Plyr overrides */
        .plyr { width: 100% !important; height: 100% !important; }
        .plyr__controls {
            width: 100% !important;
            bottom: 0 !important;
            padding: 10px 15px !important;
            justify-content: space-between !important;
        }
        .plyr__progress { flex-grow: 1 !important; display: flex !important; }
        .plyr--video .plyr__control--overlaid {
            position: absolute !important;
            top: 50% !important;
            left: 50% !important;
            transform: translate(-50%, -50%) !important;
            background:linear-gradient(135deg,var(--p2),var(--sec));
            box-shadow:0 0 20px rgba(129,140,248,.5);
            transition:opacity .2s ease, box-shadow .2s ease !important;
        }
        .plyr--video .plyr__control--overlaid:hover {
            transform: translate(-50%, -50%) !important;
            box-shadow:0 0 30px rgba(129,140,248,.7);
        }
        .plyr--video .plyr__control:hover,
        .plyr--video .plyr__control[aria-expanded="true"] { background:var(--p2); }
        .plyr__control.plyr__tab-focus { box-shadow:0 0 0 5px rgba(99,102,241,.4); }
        .plyr--full-ui input[type=range]  { color:var(--p); }
        .plyr__progress input[type=range] { color:var(--p); }
        .plyr__progress__buffer { color:rgba(129,140,248,.2); }
        .plyr__menu__container .plyr__control[role=menuitemradio][aria-checked=true]::before { background:var(--p); }

        /* Responsive */
        @media (max-width:600px) {
            .container { padding:1rem .85rem .85rem; }
            .btn-row, .err-btn-grid { grid-template-columns:1fr; gap:.6rem; }
            .xbtn { padding:.78rem 1rem; }
            #file-name { font-size:.78rem; }
        }
    </style>
</head>
<body>

<header>
    <span class="header-logo">HA Bots</span>
    <div id="file-name">{file_name}</div>
</header>

<div class="container">

    <div class="badge">
        <span class="badge-dot"></span>
        ONLINE
    </div>

    <div class="player-wrap">
        <div class="player-ambient"></div>
        <div class="player-card">
            <div class="skeleton" id="skel"></div>

            <div class="player-err-overlay" id="vidErr">
                <div class="err-card-sm">
                    <div style="margin-bottom:1.2rem; color:rgba(255,255,255,0.7);">
                        <svg width="42" height="42" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>
                    </div>
                    <h2>Oops! The video failed to load.</h2>
                    <p>Please try downloading or opening it in an external player.</p>
                </div>
            </div>

            <video src="{src}" class="player" playsinline controls></video>
        </div>
    </div>

    <div class="btn-row">
        <!-- Download -->
        <a href="{src}" class="xbtn btn-dl" download>
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.3" stroke-linecap="round" stroke-linejoin="round">
                <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
                <polyline points="7 10 12 15 17 10"/>
                <line x1="12" y1="15" x2="12" y2="3"/>
            </svg>
            Download
        </a>

        <!-- VLC -->
        <a href="vlc://{src}" class="xbtn btn-vlc">
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.3" stroke-linecap="round" stroke-linejoin="round">
                <polygon points="5 3 19 12 5 21 5 3"/>
            </svg>
            Play in VLC
        </a>

        <!-- MX Player -->
        <a href="intent:{src}#Intent;package=com.mxtech.videoplayer.ad;end" class="xbtn btn-mx">
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.3" stroke-linecap="round" stroke-linejoin="round">
                <circle cx="12" cy="12" r="10"/>
                <polygon points="10 8 16 12 10 16 10 8"/>
            </svg>
            MX Player
        </a>
    </div>

</div>

<footer>
    <p>Powered by <a href="https://t.me/HA_Bots" class="ha-link" target="_blank" rel="noopener">HA Bots</a></p>
</footer>

<script src="https://cdn.plyr.io/3.7.8/plyr.js"></script>
<script>
document.addEventListener('DOMContentLoaded', () => {
    const skel    = document.getElementById('skel');
    const vidErr  = document.getElementById('vidErr');
    const videoEl = document.querySelector('.player');

    const player = new Plyr('.player', {
        controls: ['play-large','play','progress','current-time','duration',
                   'mute','volume','captions','settings','pip','airplay','fullscreen'],
        settings: ['captions','quality','speed'],
        hideControls: false,
        tooltips: { controls:true, seek:true }
    });

    let errTriggered = false;
    const hideSkel = () => { if (skel) skel.classList.add('gone'); };
    const showError = () => {
        if (errTriggered) return;
        errTriggered = true;
        hideSkel();
        if (vidErr) vidErr.classList.add('show');
        if (player && player.elements && player.elements.container) {
            player.elements.container.style.display = 'none';
        }
    };
    
    videoEl.addEventListener('loadedmetadata', hideSkel);
    videoEl.addEventListener('canplay', hideSkel);
    
    // Core HTML5 error events
    ['error', 'abort', 'stalled'].forEach(evt => {
        videoEl.addEventListener(evt, () => {
            if (videoEl.error || videoEl.networkState === 3) showError();
        });
    });
    
    // Fallback timeout for unresponsive streams
    let loadTimeout = setTimeout(() => {
        if (videoEl.readyState === 0) showError();
        hideSkel();
    }, 12000);
    
    videoEl.addEventListener('playing', () => clearTimeout(loadTimeout));
});
</script>
</body>
</html>
"""


# ─────────────────────────────────────────────────────────────────────────────
# ERROR PAGE TEMPLATE
# ─────────────────────────────────────────────────────────────────────────────
error_tmplt = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Error — HA Bots</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap">
    <style>
        :root {
            --p:#818cf8; --p2:#6366f1; --sec:#a78bfa; --acc:#38bdf8;
            --txt:#f1f5f9; --txt2:#94a3b8;
            --bg:#020617; --glass:rgba(10,18,38,.8); --gb:rgba(129,140,248,.13);
            --err:#f43f5e; --err2:rgba(244,63,94,.15);
        }
        *, *::before, *::after { margin:0; padding:0; box-sizing:border-box; }
        body {
            font-family:'Inter',sans-serif;
            background:var(--bg);
            color:var(--txt);
            min-height:100vh;
            display:flex; flex-direction:column;
            overflow-x:hidden;
        }
        body::before {
            content:''; position:fixed; inset:0; z-index:-1;
            background:
                radial-gradient(ellipse 65% 45% at 50% 40%, rgba(244,63,94,.07) 0%, transparent 62%),
                radial-gradient(ellipse 75% 50% at 10% 20%, rgba(99,102,241,.10) 0%, transparent 58%),
                linear-gradient(160deg, #020617 0%, #070c1b 45%, #0f172a 100%);
        }

        /* Header */
        header {
            padding:.8rem 1.5rem;
            backdrop-filter:blur(24px) saturate(180%);
            -webkit-backdrop-filter:blur(24px) saturate(180%);
            background:var(--glass);
            border-bottom:1px solid var(--gb);
            box-shadow:0 1px 32px rgba(0,0,0,.45);
            display:flex; justify-content:center; align-items:center;
            animation:fadeDown .45s ease both;
        }
        @keyframes fadeDown {
            from { opacity:0; transform:translateY(-12px); }
            to   { opacity:1; transform:translateY(0); }
        }
        .header-logo {
            font-size:1rem; font-weight:800; letter-spacing:-.01em;
            background:linear-gradient(90deg,#e2e8f0 0%,var(--p) 50%,var(--acc) 100%);
            -webkit-background-clip:text; -webkit-text-fill-color:transparent; background-clip:text;
        }

        /* Error layout */
        main {
            flex:1; display:flex; align-items:center; justify-content:center;
            padding:3rem 1.25rem;
        }
        .error-card {
            background:var(--glass);
            border:1px solid rgba(244,63,94,.22);
            border-radius:20px;
            padding:2.5rem 2rem;
            text-align:center;
            max-width:440px; width:100%;
            box-shadow:
                0 0 0 1px rgba(255,255,255,.04),
                0 12px 48px rgba(0,0,0,.5),
                0 0 45px rgba(244,63,94,.09);
            backdrop-filter:blur(20px);
            animation:cardIn .5s ease both;
        }
        @keyframes cardIn {
            from { opacity:0; transform:translateY(22px) scale(.97); }
            to   { opacity:1; transform:translateY(0) scale(1); }
        }

        /* Error icon */
        .err-icon {
            width:66px; height:66px; border-radius:50%;
            margin:0 auto 1.4rem;
            background:var(--err2);
            border:1px solid rgba(244,63,94,.28);
            display:flex; align-items:center; justify-content:center;
            box-shadow:0 0 28px rgba(244,63,94,.18);
        }
        .err-icon svg { color:var(--err); }

        .err-label {
            font-size:.65rem; font-weight:700; letter-spacing:.1em;
            text-transform:uppercase; color:var(--err); margin-bottom:.55rem;
        }
        .error-card h2 {
            font-size:1.6rem; font-weight:800; letter-spacing:-.02em;
            margin-bottom:.65rem;
        }
        .error-card p {
            font-size:.88rem; color:var(--txt2); line-height:1.7;
            margin-bottom:1.75rem;
        }

        /* Buttons */
        .err-btns { display:flex; flex-direction:column; gap:.75rem; width:100%; align-items:center; }
        .ebtn {
            display:flex; align-items:center; justify-content:center; gap:.5rem;
            width:100%; max-width:280px;
            padding:.85rem 1.5rem; border-radius:12px;
            font-family:'Inter',sans-serif; font-size:.9rem; font-weight:700;
            cursor:pointer; text-decoration:none; color:#fff; border:none;
            transition:transform .2s, box-shadow .2s, filter .2s;
            background:linear-gradient(135deg,var(--p2),var(--p),var(--sec));
            box-shadow:0 4px 20px rgba(99,102,241,.45);
        }
        .ebtn:hover {
            transform:scale(1.02);
            box-shadow:0 6px 24px rgba(99,102,241,.55);
            filter:brightness(1.08);
        }
        .ebtn:active { transform:scale(.98); }

        /* Footer */
        footer {
            padding:.85rem 1.5rem; text-align:center;
            color:var(--txt2); font-size:.73rem;
        }
        footer::before {
            content:''; display:block;
            width:90px; height:1px;
            background:linear-gradient(90deg,transparent,rgba(129,140,248,.25),transparent);
            margin:0 auto .7rem;
        }
        .ha-link {
            color:var(--p); text-decoration:none; font-weight:600;
            transition:opacity .2s;
        }
        .ha-link:hover { opacity:.7; }
    </style>
</head>
<body>

<header>
    <span class="header-logo">HA Bots</span>
</header>

<main>
  <div class="error-card">

    <div class="err-icon">
      <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
        <circle cx="12" cy="12" r="10"/>
        <line x1="12" y1="8" x2="12" y2="12"/>
        <line x1="12" y1="16" x2="12.01" y2="16"/>
      </svg>
    </div>

    <div class="err-label">Error</div>
    <h2>Something went wrong</h2>
    <p>We couldn't load this file. It may have expired or there was a temporary issue.</p>

    <div class="err-btns">
      <button class="ebtn" onclick="location.reload()">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"></polyline><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"></path></svg>
        Try Again
      </button>
      <a href="https://t.me/HA_Bots_Support" class="ebtn" target="_blank" rel="noopener">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"></path><circle cx="9" cy="7" r="4"></circle><path d="M23 21v-2a4 4 0 0 0-3-3.87"></path><path d="M16 3.13a4 4 0 0 1 0 7.75"></path></svg>
        Support Group
      </a>
    </div>

  </div>
</main>

<footer>
  <p>Powered by <a href="https://t.me/HA_Bots" class="ha-link" target="_blank" rel="noopener">HA Bots</a></p>
</footer>
</body>
</html>
"""


# ─────────────────────────────────────────────────────────────────────────────
# Backend helpers
# ─────────────────────────────────────────────────────────────────────────────
async def media_watch(message_id):
    media_msg = await temp.BOT.get_messages(BIN_CHANNEL, message_id)
    media = getattr(media_msg, media_msg.media.value, None)
    src = urllib.parse.urljoin(URL, f'download/{message_id}')
    tag = media.mime_type.split('/')[0].strip()
    if tag == 'video':
        heading = html.escape(f'Watch — {media.file_name}')
        html_ = (watch_tmplt
                 .replace('{heading}',   heading)
                 .replace('{file_name}', media.file_name)
                 .replace('{src}',       src))
    else:
        html_ = error_tmplt
    return html_
