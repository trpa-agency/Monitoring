/* ltinfo-embed.js  v1.0.0  (Oct. 26, 2026)
   Shared behavior for TRPA pages that laketahoeinfo.org pulls in by URL. Exposes one
   global, LTI. Load as a classic script after Plotly, AG Grid, and the ArcGIS SDK, and
   after ltinfo-embed.css. Nothing here runs until the page calls it, except the height
   reporter, which starts on its own so a host that listens can size the iframe.

   What it gives a page
     LTI.FONT, LTI.INK, LTI.COLORS, LTI.HAZARD, LTI.SEQ, LTI.accent()   colors and font
     LTI.layout(overrides), LTI.PLOT_CONFIG                              Plotly defaults
     LTI.plot(el, traces, layout, config)                               draw a chart (always this, never Plotly.newPlot)
     LTI.showEmpty(el, title, bullets)                                  swap a chart container for an empty state
     LTI.fmt(n, decimals), LTI.esc(s), LTI.narrow(), LTI.wrapLabel(s)   small helpers
     LTI.query(url, params)                                             ArcGIS REST query with a JSONP fallback
     LTI.load(loader, snapshot)                                         live data or the snapshot, and which one
     LTI.hbar(id, entries, opts)                                        horizontal bar of a {key: value} object
     LTI.grid(id, columnDefs, rowData, exportName)                      AG Grid with the host look and a CSV button
     LTI.toggle(control, views)                                         Chart/Table segmented control wiring
     LTI.emptyState(title, bullets)                                     the explicit "not yet loaded" block
     LTI.guardMap(view), LTI.placeLegend(view, legend, Expand)          map behavior in a short iframe
     LTI.sourceText(live, capturedDate, serviceName)                    the standard source line
     LTI.params(), LTI.onParams(fn)                                     deep-link parameters from the URL or the host
     LTI.notifyHeight()                                                 post the document height now

   Breaking changes go to kit/v2/, never to this file. See CONTRACT.md for the messages. */
(function () {
  'use strict';

  const root = document.documentElement;
  const cssVar = (name, fallback) => (getComputedStyle(root).getPropertyValue(name).trim() || fallback);

  const FONT = 'Lexend Deca, sans-serif';
  const INK = '#333333';
  const COLORS = ['#3e72b0', '#e49937', '#89bf40', '#5a9cb0', '#be4c4c', '#646820', '#89cad7', '#2c5f72'];
  const HAZARD = ['#3e72b0', '#e49937', '#be4c4c'];
  const SEQ = ['#daf0f3', '#b9e1e8', '#89cad7', '#52aabe', '#368ea4', '#30748a', '#2c5f72'];
  const accent = () => cssVar('--lti-accent', '#789440');

  /* Formatting */
  const fmt = (n, d = 0) => (n === null || n === undefined || Number.isNaN(Number(n))) ? '—'
    : Number(n).toLocaleString('en-US', { maximumFractionDigits: d, minimumFractionDigits: d });
  const esc = (s) => String(s ?? '').replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  const narrow = () => root.clientWidth < 640;
  const wrapLabel = (s) => {
    s = String(s);
    if (!narrow() || s.length <= 14) return s;
    const i = s.lastIndexOf(' ', Math.ceil(s.length / 2) + 3);
    return i > 0 ? s.slice(0, i) + '<br>' + s.slice(i + 1) : s;
  };

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
     autosize and a 100 percent tall SVG container) cannot collapse it inside an iframe whose
     size changes after the first draw. */
  function plot(el, traces, lay, config) {
    el = typeof el === 'string' ? document.getElementById(el) : el;
    if (lay && lay.height) el.style.height = lay.height + 'px';
    return Plotly.newPlot(el, traces, lay, config || PLOT_CONFIG);
  }
  /* Replace a chart container's contents with an empty state (see emptyState below). */
  function showEmpty(el, title, bullets) {
    el = typeof el === 'string' ? document.getElementById(el) : el;
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

  /* Run the live loader; fall back to the embedded snapshot. Returns { data, live }.
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
  function hbar(containerId, entries, { labels = {}, color, height, unit = '', axisTitle, hoverName = '', sort = true, decimals = 0 } = {}) {
    color = color || COLORS[0];
    let rows = Object.entries(entries).map(([k, v]) => ({ key: k, label: labels[k] ?? (k || 'Not classified'), value: Number(v) || 0 }));
    if (sort) rows.sort((a, b) => b.value - a.value);
    const total = rows.reduce((a, r) => a + r.value, 0) || 1;
    const trace = {
      type: 'bar', orientation: 'h',
      y: rows.map(r => wrapLabel(r.label)), x: rows.map(r => r.value),
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
    plot(containerId, [trace], lay, PLOT_CONFIG);
    return rows;
  }

  /* AG Grid with the host look. Works with the Theming API (ag-grid-community 33 and later)
     and falls back to the legacy CSS theme class on older builds. */
  const grids = {};
  function gridTheme() {
    if (!window.agGrid || !agGrid.themeQuartz) return null;
    return agGrid.themeQuartz.withParams({
      accentColor: cssVar('--lti-primary', '#3e72b0'), fontFamily: FONT, fontSize: 14,
      headerBackgroundColor: '#efefef', headerTextColor: '#3d3d3d', headerFontWeight: 600,
      foregroundColor: '#181d1f', backgroundColor: '#ffffff', oddRowBackgroundColor: '#fafafa',
      borderColor: 'rgba(0,0,0,.1)', rowBorder: { color: 'rgba(0,0,0,.15)' },
      wrapperBorderRadius: 4, headerHeight: 40, rowHeight: 40,
    });
  }
  function grid(containerId, columnDefs, rowData, exportName = 'data') {
    if (grids[containerId]) { grids[containerId].setGridOption('rowData', rowData); return grids[containerId]; }
    const container = document.getElementById(containerId);
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
    grids[containerId] = agGrid.createGrid(div, options);
    btn.addEventListener('click', () => grids[containerId].exportDataAsCsv({ fileName: `${exportName}.csv` }));
    return grids[containerId];
  }

  /* Wire a calcite-segmented-control to views. `views` maps each item value to a function
     that shows that view; the function receives the value. The 'table' view is the convention
     for the grid. After the switch the height is re-posted. */
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
  function showPlot(plotEl, gridEl) { gridEl.classList.add('hidden'); plotEl.classList.remove('hidden'); if (window.Plotly?.Plots) Plotly.Plots.resize(plotEl); }
  function showGrid(plotEl, gridEl) { plotEl.classList.add('hidden'); gridEl.classList.remove('hidden'); }

  function emptyState(title, bullets = []) {
    return `<div class="empty"><calcite-icon icon="information" scale="m" aria-hidden="true"></calcite-icon><div><strong>${esc(title)}</strong>${bullets.length ? `<ul>${bullets.map(b => `<li>${esc(b)}</li>`).join('')}</ul>` : ''}</div></div>`;
  }

  /* Maps in a short iframe: page scrolling must not become map zooming, and a floating
     legend popover can escape the frame on phones. */
  function guardMap(view) { view.on('mouse-wheel', evt => { evt.stopPropagation(); }); }
  function placeLegend(view, legend, Expand, position = 'top-right') {
    if (window.innerWidth > 700) view.ui.add(legend, position);
    else view.ui.add(new Expand({ view, content: legend, expanded: false, mode: 'drawer', expandIcon: 'legend', expandTooltip: 'Legend' }), position);
  }

  /* Deep-link parameters: the URL query first, then anything the host posts as
     { type: 'ltinfo-embed:params', params: {...} }. */
  const params = {};
  for (const [k, v] of new URLSearchParams(location.search)) params[k] = v;
  const paramListeners = [];
  function onParams(fn) { paramListeners.push(fn); }
  window.addEventListener('message', (e) => {
    const m = e.data;
    if (!m || m.type !== 'ltinfo-embed:params' || typeof m.params !== 'object') return;
    Object.assign(params, m.params);
    paramListeners.forEach(fn => fn({ ...params }));
  });

  /* Height reporter. Posts { type: 'ltinfo-embed:height', height, source } to the parent on
     load, on resize, and whenever the body changes size. Harmless when there is no parent or
     the parent does not listen. `source` is the page's data-embed name or its path. */
  const source = root.dataset.embed || location.pathname.split('/').pop().replace(/\.html$/, '') || 'embed';
  let lastHeight = 0;
  function notifyHeight() {
    if (window.parent === window) return;
    const height = Math.ceil(root.scrollHeight);
    if (height === lastHeight) return;
    lastHeight = height;
    window.parent.postMessage({ type: 'ltinfo-embed:height', height, source }, '*');
  }
  window.addEventListener('load', notifyHeight);
  window.addEventListener('resize', notifyHeight);
  // The script usually loads in <head>, before <body> exists, so observe the root element,
  // whose scroll height is what gets posted anyway.
  if ('ResizeObserver' in window) {
    const ro = new ResizeObserver(() => notifyHeight());
    ro.observe(root);
    document.addEventListener('DOMContentLoaded', () => { if (document.body) ro.observe(document.body); });
  }

  window.LTI = {
    FONT, INK, COLORS, HAZARD, SEQ, accent,
    layout, targetLine, PLOT_CONFIG, plot,
    fmt, esc, narrow, wrapLabel,
    query, load, sourceText,
    hbar, grid, toggle, showPlot, showGrid, emptyState, showEmpty,
    guardMap, placeLegend,
    params: () => ({ ...params }), onParams,
    notifyHeight,
    version: '1.0.0',
  };
})();
