/* ltinfo-embed.js  v1.1.0  (Oct. 26, 2026)
   Shared behavior for TRPA fragments that render inside laketahoeinfo.org pages. Exposes one
   global, LTI. Load as a classic script after Plotly, AG Grid, and the ArcGIS SDK, and after
   ltinfo-embed.css. The host layout loads it once; every fragment on the page uses it.

   A fragment is <section class="lti" data-kind="..."> with its markup, a config JSON block
   (<script type="application/json" data-lti="config">) and an optional snapshot block
   (data-lti="snapshot"). A fragment has no element ids: it marks elements with data-ref and
   its module finds them inside its own root. Fragment modules register with LTI.define(kind,
   fn); LTI.mount(root) reads the blocks and calls fn(root, config, snapshot). On
   DOMContentLoaded the kit mounts every fragment in the document and resolves every
   <div data-lti-include="url"> by fetching that page's fragment and mounting it inline.

   Module lookup: data-module on the fragment root (a URL, relative to the page that holds the
   fragment, or to the included file), else kit/fragments/<kind>.js next to this file.

   What it gives a fragment module
     LTI.define(kind, fn), LTI.mount(root), LTI.mountAll(scope), LTI.include(el, url)
     LTI.ref(root, name)                                                root.querySelector('[data-ref=name]')
     LTI.FONT, LTI.INK, LTI.COLORS, LTI.HAZARD, LTI.SEQ, LTI.accent(root) colors and font
     LTI.layout(overrides), LTI.PLOT_CONFIG                              Plotly defaults
     LTI.plot(el, traces, layout, config)                               draw a chart (always this, never Plotly.newPlot)
     LTI.showEmpty(el, title, bullets)                                  swap a chart container for an empty state
     LTI.fmt(n, decimals), LTI.esc(s), LTI.narrow(el), LTI.wrapLabel(s, el)
     LTI.query(url, params)                                             ArcGIS REST query with a JSONP fallback
     LTI.load(loader, snapshot)                                         live data or the snapshot, and which one
     LTI.hbar(el, entries, opts)                                        horizontal bar of a {key: value} object
     LTI.grid(el, columnDefs, rowData, exportName)                      AG Grid with the host look and a CSV button
     LTI.toggle(control, views)                                         Chart/Table segmented control wiring
     LTI.emptyState(title, bullets)                                     the explicit "not yet loaded" block
     LTI.guardMap(view), LTI.placeLegend(view, legend, Expand)          map behavior in a short slot
     LTI.sourceText(live, capturedDate, serviceName)                    the standard source line
     LTI.params(), LTI.onParams(fn), LTI.setParam(key, value)           deep links in the page URL (and the host, when framed)
     LTI.notifyHeight()                                                 interim: post the document height when framed

   Interim iframe behavior (today's Dashboard card): the height and params messages in
   CONTRACT.md run only when window.parent !== window. Inline they are inert.

   Breaking changes go to kit/v2/, never to this file. */
(function () {
  'use strict';

  const root = document.documentElement;
  const framed = window.parent !== window;
  const KIT_BASE = (document.currentScript && document.currentScript.src) ? new URL('.', document.currentScript.src).href : new URL('.', location.href).href;
  const cssVar = (name, fallback, el) => (getComputedStyle(el || root).getPropertyValue(name).trim() || fallback);
  const byId = (el) => (typeof el === 'string' ? document.getElementById(el) : el);

  const FONT = 'Lexend Deca, sans-serif';
  const INK = '#333333';
  const COLORS = ['#3e72b0', '#e49937', '#89bf40', '#5a9cb0', '#be4c4c', '#646820', '#89cad7', '#2c5f72'];
  const HAZARD = ['#3e72b0', '#e49937', '#be4c4c'];
  const SEQ = ['#daf0f3', '#b9e1e8', '#89cad7', '#52aabe', '#368ea4', '#30748a', '#2c5f72'];
  const accent = (el) => cssVar('--lti-accent', '#789440', el || document.querySelector('.lti') || root);

  /* Formatting */
  const fmt = (n, d = 0) => (n === null || n === undefined || Number.isNaN(Number(n))) ? '—'
    : Number(n).toLocaleString('en-US', { maximumFractionDigits: d, minimumFractionDigits: d });
  const esc = (s) => String(s ?? '').replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  const narrow = (el) => ((el && el.clientWidth) || root.clientWidth) < 640;
  const wrapLabel = (s, el) => {
    s = String(s);
    if (!narrow(el) || s.length <= 14) return s;
    const i = s.lastIndexOf(' ', Math.ceil(s.length / 2) + 3);
    return i > 0 ? s.slice(0, i) + '<br>' + s.slice(i + 1) : s;
  };
  const ref = (fragmentRoot, name) => fragmentRoot.querySelector(`[data-ref="${name}"]`);

  /* Plotly */
  const PLOT_CONFIG = { responsive: true, displayModeBar: false, displaylogo: false };
  function layout(overrides = {}) {
    const base = {
      font: { family: FONT, color: INK, size: 13 },
      paper_bgcolor: 'transparent', plot_bgcolor: 'transparent',
      margin: { t: 8, r: 16, b: 40, l: 50 },
      xaxis: { gridcolor: '#dcdcdc', zerolinecolor: '#bdbdbd', linecolor: '#bdbdbd', automargin: true },
      yaxis: { gridcolor: '#dcdcdc', zeroline: false, automargin: true, tickfont: { size: 12 } },
      hoverlabel: { font: { family: FONT }, bgcolor: '#FFFFFF', bordercolor: '#bdbdbd' },
      colorway: COLORS,
      legend: { orientation: 'h', y: -0.2 },
      uniformtext: { minsize: 9, mode: 'hide' },
    };
    const out = { ...base, ...overrides };
    if (overrides.xaxis) out.xaxis = { ...base.xaxis, ...overrides.xaxis };
    if (overrides.yaxis) out.yaxis = { ...base.yaxis, ...overrides.yaxis };
    if (overrides.margin) out.margin = { ...base.margin, ...overrides.margin };
    return out;
  }

  /* Draw a chart. Use this instead of Plotly.newPlot. It gives the container a definite pixel
     height from layout.height, so Plotly's responsive resize (which switches the chart to
     autosize and a 100 percent tall SVG container) cannot collapse it when the container's
     size changes after the first draw. */
  function plot(el, traces, lay, config) {
    el = byId(el);
    if (lay && lay.height) el.style.height = lay.height + 'px';
    return Plotly.newPlot(el, traces, lay, config || PLOT_CONFIG);
  }
  /* Replace a chart container's contents with an empty state (see emptyState below). */
  function showEmpty(el, title, bullets) {
    el = byId(el);
    if (el.data && window.Plotly) Plotly.purge(el);
    el.style.height = '';
    el.classList.remove('plot');
    el.innerHTML = emptyState(title, bullets);
  }

  /* A dashed target or standard line with a label at the right end. */
  function targetLine(y, label, color = '#4e4e4e') {
    return {
      shapes: [{ type: 'line', xref: 'paper', x0: 0, x1: 1, y0: y, y1: y, line: { color, width: 1.5, dash: 'dash' } }],
      annotations: [{ xref: 'paper', x: 1, y, text: label, showarrow: false, xanchor: 'right', yanchor: 'bottom', font: { size: 11, color } }],
    };
  }

  /* ArcGIS REST query. fetch first; on failure retry as JSONP, which the office network allows
     when CORS is blocked. Resolves the parsed JSON or throws the service error. */
  function query(url, params = {}) {
    const p = new URLSearchParams({ f: 'json', ...params });
    if (p.get('outStatistics') && typeof params.outStatistics !== 'string') p.set('outStatistics', JSON.stringify(params.outStatistics));
    const full = `${url}/query?${p}`;
    return fetch(full).then(r => r.json()).then(json => {
      if (json.error) throw new Error(json.error.message || 'Service error');
      return json;
    }).catch(() => jsonp(full));
  }
  function jsonp(url) {
    return new Promise((resolve, reject) => {
      const cb = `__lti_cb_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;
      const s = document.createElement('script');
      const timer = setTimeout(() => { cleanup(); reject(new Error('JSONP timeout')); }, 15000);
      const cleanup = () => { clearTimeout(timer); delete window[cb]; s.remove(); };
      window[cb] = (json) => { cleanup(); json.error ? reject(new Error(json.error.message)) : resolve(json); };
      s.onerror = () => { cleanup(); reject(new Error('JSONP failed')); };
      s.src = url.replace('f=json', 'f=pjson') + `&callback=${cb}`;
      document.head.append(s);
    });
  }

  /* Run the live loader; fall back to the snapshot. Returns { data, live }.
     The snapshot must carry the same shape as the loader's result plus `captured`. */
  async function load(loader, snapshot) {
    try {
      const data = await loader();
      return { data, live: true };
    } catch (err) {
      console.warn('Live query failed; using snapshot.', err);
      return { data: snapshot, live: false };
    }
  }

  function sourceText(live, captured, serviceName) {
    return `Source: ${serviceName}, ${live ? 'queried at page load' : `snapshot captured ${captured}`}.`;
  }

  /* Horizontal bar from a {key: value} object. opts: labels, color, height, unit ('acres'),
     hoverName, sort (default true). Returns the rows for a table view. */
  function hbar(el, entries, { labels = {}, color, height, unit = '', axisTitle, hoverName = '', sort = true, decimals = 0 } = {}) {
    el = byId(el);
    color = color || COLORS[0];
    let rows = Object.entries(entries).map(([k, v]) => ({ key: k, label: labels[k] ?? (k || 'Not classified'), value: Number(v) || 0 }));
    if (sort) rows.sort((a, b) => b.value - a.value);
    const total = rows.reduce((a, r) => a + r.value, 0) || 1;
    const trace = {
      type: 'bar', orientation: 'h',
      y: rows.map(r => wrapLabel(r.label, el)), x: rows.map(r => r.value),
      marker: { color, line: { width: 0 } }, width: 0.55,
      text: rows.map(r => `${fmt(r.value, decimals)}${unit ? ' ' + unit : ''} · ${(r.value / total * 100).toFixed(r.value / total < 0.01 ? 1 : 0)}%`),
      textposition: 'outside', textfont: { size: 11, color: INK }, cliponaxis: false,
      hovertemplate: `<b>%{y}</b><br>%{x:,.${decimals}f}${unit ? ' ' + unit : ''}${hoverName ? ' · ' + hoverName : ''}<extra></extra>`,
    };
    const max = Math.max(...rows.map(r => r.value), 0);
    const lay = layout({
      height: height ?? (40 + rows.length * 38),
      margin: { t: 8, r: 70, b: 40, l: 10 },
      bargap: 0.35,
      xaxis: { tickformat: ',d', range: [0, max * 1.35], title: (axisTitle || unit) ? { text: axisTitle || unit.charAt(0).toUpperCase() + unit.slice(1), font: { size: 12 } } : undefined },
      yaxis: { showgrid: false, autorange: 'reversed' },
    });
    plot(el, [trace], lay, PLOT_CONFIG);
    return rows;
  }

  /* AG Grid with the host look. Works with the Theming API (ag-grid-community 33 and later)
     and falls back to the legacy CSS theme class on older builds. One grid per container. */
  const grids = new WeakMap();
  function gridTheme() {
    if (!window.agGrid || !agGrid.themeQuartz) return null;
    return agGrid.themeQuartz.withParams({
      accentColor: '#3e72b0', fontFamily: FONT, fontSize: 14,
      headerBackgroundColor: '#efefef', headerTextColor: '#3d3d3d', headerFontWeight: 600,
      foregroundColor: '#181d1f', backgroundColor: '#ffffff', oddRowBackgroundColor: '#fafafa',
      borderColor: 'rgba(0,0,0,.1)', rowBorder: { color: 'rgba(0,0,0,.15)' },
      wrapperBorderRadius: 4, headerHeight: 40, rowHeight: 40,
    });
  }
  function grid(container, columnDefs, rowData, exportName = 'data') {
    container = byId(container);
    if (grids.has(container)) { grids.get(container).setGridOption('rowData', rowData); return grids.get(container); }
    const toolbar = document.createElement('div');
    toolbar.className = 'grid-toolbar';
    const btn = document.createElement('calcite-button');
    btn.setAttribute('appearance', 'outline'); btn.setAttribute('scale', 's'); btn.setAttribute('icon-start', 'download');
    btn.textContent = 'Download CSV';
    toolbar.append(btn);
    const div = document.createElement('div');
    const theme = gridTheme();
    if (!theme) div.className = 'ag-theme-quartz';
    container.append(toolbar, div);
    const options = {
      columnDefs, rowData,
      defaultColDef: { sortable: true, filter: true, resizable: true, flex: 1, minWidth: 110 },
      domLayout: 'autoHeight', suppressCellFocus: true,
    };
    if (theme) options.theme = theme;
    const api = agGrid.createGrid(div, options);
    grids.set(container, api);
    btn.addEventListener('click', () => api.exportDataAsCsv({ fileName: `${exportName}.csv` }));
    return api;
  }

  /* Wire a calcite-segmented-control to views. `views` maps each item value to a function
     that shows that view; the function receives the value. The 'table' view is the convention
     for the grid. After the switch the height is re-posted (framed only). */
  function toggle(control, views) {
    if (typeof control === 'string') control = document.querySelector(control);
    if (!control) return;
    control.addEventListener('calciteSegmentedControlChange', (e) => {
      const value = e.target.selectedItem?.value || e.target.value;
      if (views[value]) views[value](value);
      notifyHeight();
    });
  }
  /* Show one of plot or grid; resize the plot when it comes back. */
  function showPlot(plotEl, gridEl) { gridEl.classList.add('hidden'); plotEl.classList.remove('hidden'); if (window.Plotly?.Plots && plotEl.data) Plotly.Plots.resize(plotEl); }
  function showGrid(plotEl, gridEl) { plotEl.classList.add('hidden'); gridEl.classList.remove('hidden'); }

  function emptyState(title, bullets = []) {
    return `<div class="empty"><calcite-icon icon="information" scale="m" aria-hidden="true"></calcite-icon><div><strong>${esc(title)}</strong>${bullets.length ? `<ul>${bullets.map(b => `<li>${esc(b)}</li>`).join('')}</ul>` : ''}</div></div>`;
  }

  /* Maps in a short slot: page scrolling must not become map zooming, and a floating legend
     popover can escape a short frame on phones. */
  function guardMap(view) { view.on('mouse-wheel', evt => { evt.stopPropagation(); }); }
  function placeLegend(view, legend, Expand, position = 'top-right') {
    if (window.innerWidth > 700) view.ui.add(legend, position);
    else view.ui.add(new Expand({ view, content: legend, expanded: false, mode: 'drawer', expandIcon: 'legend', expandTooltip: 'Legend' }), position);
  }

  /* Deep links. The page URL's query string is the source of truth; a fragment reads it at
     mount and writes a selection back with replaceState so the URL reproduces the view.
     When framed, the host may also post { type: 'ltinfo-embed:params', params } and the
     fragment posts the same message up on a selection (interim). */
  const params = {};
  for (const [k, v] of new URLSearchParams(location.search)) params[k] = v;
  const paramListeners = [];
  function onParams(fn) { paramListeners.push(fn); }
  function setParam(key, value) {
    if (params[key] === value) return;
    params[key] = value;
    try {
      const u = new URL(location.href);
      if (value === null || value === undefined || value === '') u.searchParams.delete(key); else u.searchParams.set(key, value);
      history.replaceState(history.state, '', u);
    } catch (e) { /* a sandboxed frame may refuse; the message below still carries it */ }
    if (framed) window.parent.postMessage({ type: 'ltinfo-embed:params', params: { [key]: value } }, '*');
  }
  if (framed) {
    window.addEventListener('message', (e) => {
      const m = e.data;
      if (!m || m.type !== 'ltinfo-embed:params' || typeof m.params !== 'object') return;
      Object.assign(params, m.params);
      paramListeners.forEach(fn => fn({ ...params }));
    });
  }

  /* Interim height reporter. When framed, posts { type: 'ltinfo-embed:height', height, source }
     to the parent on load, on resize, and whenever the document changes size. Inline it does
     nothing. `source` is the page's data-embed name or its path. */
  const source = root.dataset.embed || location.pathname.split('/').pop().replace(/\.html$/, '') || 'embed';
  let lastHeight = 0;
  function notifyHeight() {
    if (!framed) return;
    const height = Math.ceil(root.scrollHeight);
    if (height === lastHeight) return;
    lastHeight = height;
    window.parent.postMessage({ type: 'ltinfo-embed:height', height, source }, '*');
  }
  if (framed) {
    window.addEventListener('load', notifyHeight);
    window.addEventListener('resize', notifyHeight);
    if ('ResizeObserver' in window) {
      const ro = new ResizeObserver(() => notifyHeight());
      ro.observe(root);
      document.addEventListener('DOMContentLoaded', () => { if (document.body) ro.observe(document.body); });
    }
  }

  /* ───────────── Fragments: define, mount, include ───────────── */
  const kinds = {};          // kind -> mount function
  const moduleLoads = {};    // url -> promise
  function define(kind, fn) { kinds[kind] = fn; }

  function readBlock(fragmentRoot, name) {
    const s = fragmentRoot.querySelector(`script[type="application/json"][data-lti="${name}"]`);
    if (!s) return null;
    try { return JSON.parse(s.textContent); } catch (err) { console.error(`Bad JSON in data-lti="${name}"`, err); return null; }
  }
  function loadScript(url) {
    if (!moduleLoads[url]) {
      moduleLoads[url] = new Promise((resolve, reject) => {
        const s = document.createElement('script');
        s.src = url; s.async = true;
        s.onload = () => resolve(url);
        s.onerror = () => reject(new Error(`Could not load ${url}`));
        document.head.append(s);
      });
    }
    return moduleLoads[url];
  }
  /* Make sure the module for a fragment is loaded. `base` is the URL the fragment came from
     (the page, or the included file), used to resolve data-module. */
  async function ensure(fragmentRoot, base) {
    const kind = fragmentRoot.dataset.kind;
    if (kinds[kind]) return kinds[kind];
    const url = fragmentRoot.dataset.module
      ? new URL(fragmentRoot.dataset.module, base || document.baseURI).href
      : new URL(`fragments/${kind}.js`, KIT_BASE).href;
    await loadScript(url);
    if (!kinds[kind]) throw new Error(`${url} did not define fragment kind "${kind}"`);
    return kinds[kind];
  }
  /* Mount one fragment root. Resolves when the module's mount function has returned. */
  async function mount(fragmentRoot, config, snapshot, base) {
    if (fragmentRoot.dataset.mounted) return;
    fragmentRoot.dataset.mounted = '1';
    try {
      const fn = await ensure(fragmentRoot, base);
      config = config || readBlock(fragmentRoot, 'config') || {};
      snapshot = snapshot || readBlock(fragmentRoot, 'snapshot') || null;
      await fn(fragmentRoot, config, snapshot);
    } catch (err) {
      console.error(`Fragment "${fragmentRoot.dataset.kind}" failed to mount`, err);
      fragmentRoot.insertAdjacentHTML('beforeend', emptyState('This section could not be drawn.', [String(err.message || err)]));
    }
    notifyHeight();
  }
  /* Fetch a harness page, take its fragment (markup plus JSON blocks), put it in `el`, and
     mount it. `el` is a placeholder <div data-lti-include="url">; its own children are kept
     as the fallback and `failed` is added to it if anything goes wrong. */
  async function include(el, url) {
    url = new URL(url || el.dataset.ltiInclude, document.baseURI).href;
    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
      const doc = new DOMParser().parseFromString(await res.text(), 'text/html');
      const frag = doc.querySelector('.lti[data-kind]');
      if (!frag) throw new Error('No .lti[data-kind] fragment in ' + url);
      const node = document.importNode(frag, true);
      delete node.dataset.mounted;
      // The page that includes decides the site accent; the fragment keeps its own if the page has none.
      const pageSite = el.closest('.lti[data-site]')?.dataset.site;
      if (pageSite) node.dataset.site = pageSite;
      el.classList.add('include');
      el.prepend(node);
      await mount(node, null, null, url);
      return node;
    } catch (err) {
      console.error('Include failed', url, err);
      el.classList.add('include', 'failed');
      return null;
    }
  }
  /* Mount every fragment and resolve every include inside `scope` (default: the document). */
  function mountAll(scope) {
    scope = scope || document;
    const jobs = [];
    scope.querySelectorAll('[data-lti-include]').forEach(el => { if (!el.dataset.included) { el.dataset.included = '1'; jobs.push(include(el)); } });
    scope.querySelectorAll('.lti[data-kind]').forEach(el => { if (!el.dataset.mounted) jobs.push(mount(el)); });
    return Promise.all(jobs);
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', () => mountAll());
  else mountAll();

  window.LTI = {
    FONT, INK, COLORS, HAZARD, SEQ, accent,
    layout, targetLine, PLOT_CONFIG, plot,
    fmt, esc, narrow, wrapLabel, ref,
    query, load, sourceText,
    hbar, grid, toggle, showPlot, showGrid, emptyState, showEmpty,
    guardMap, placeLegend,
    params: () => ({ ...params }), onParams, setParam,
    notifyHeight,
    define, mount, mountAll, include,
    framed, kitBase: KIT_BASE,
    version: '1.1.0',
  };
})();
