/* kit/fragments/sites-timeseries.js  v1.1.0
   A map of monitoring sites, a site picker, a few tiles, and the selected site's record over
   time as a chart with a table and CSV. For any program whose data is a point layer of sites
   plus a table of observations.

   Config
     sitesLayer, siteIdField (stable id for deep links), siteNameField (display name and join key)
     recordsTable, recordSiteField (matches siteNameField), dateField, valueField
     aggregate 'sum' | 'mean' per month, valueLabel, unitLabel, defaultSiteId
     mapCenter [lon, lat], mapZoom, seriesCaption, sourceName, exportPrefix
   Snapshot
     { captured, total: {n, first, last}, sites: [{id, name, lon, lat, n, first, last, total}],
       series: { siteId: [[yyyy-mm, total, days], ...] } }
   Deep link: ?site=<siteId> in the page URL. Selecting a site writes it back. */
LTI.define('sites-timeseries', async function (root, CONFIG, SNAPSHOT) {
  'use strict';
  const { fmt, esc } = LTI;
  const $ = (name) => LTI.ref(root, name);
  SNAPSHOT = SNAPSHOT || { captured: 'unknown', total: null, sites: [], series: {} };
  const sq = s => String(s).replace(/'/g, "''");
  const monthKey = ms => new Date(ms).toISOString().slice(0, 7);
  const monthLabel = k => new Date(k + '-15T00:00:00Z').toLocaleDateString('en-US', { month: 'short', year: 'numeric', timeZone: 'UTC' });
  const dateLabel = iso => iso ? new Date(iso + 'T00:00:00Z').toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' }) : '—';

  /* ───── data ───── */
  async function loadSites() {
    const [pts, stats, total] = await Promise.all([
      LTI.query(CONFIG.sitesLayer, { where: '1=1', outFields: `${CONFIG.siteIdField},${CONFIG.siteNameField}`, returnGeometry: 'true', outSR: '4326' }),
      LTI.query(CONFIG.recordsTable, { where: '1=1', returnGeometry: 'false', groupByFieldsForStatistics: CONFIG.recordSiteField, outStatistics: [
        { statisticType: 'count', onStatisticField: CONFIG.dateField, outStatisticFieldName: 'n' },
        { statisticType: 'min', onStatisticField: CONFIG.dateField, outStatisticFieldName: 'd0' },
        { statisticType: 'max', onStatisticField: CONFIG.dateField, outStatisticFieldName: 'd1' },
        { statisticType: 'sum', onStatisticField: CONFIG.valueField, outStatisticFieldName: 'total' } ] }),
      LTI.query(CONFIG.recordsTable, { where: '1=1', returnGeometry: 'false', outStatistics: [
        { statisticType: 'count', onStatisticField: CONFIG.dateField, outStatisticFieldName: 'n' },
        { statisticType: 'min', onStatisticField: CONFIG.dateField, outStatisticFieldName: 'd0' },
        { statisticType: 'max', onStatisticField: CONFIG.dateField, outStatisticFieldName: 'd1' } ] }),
    ]);
    const byName = new Map(stats.features.map(f => [f.attributes[CONFIG.recordSiteField], f.attributes]));
    const iso = ms => ms ? new Date(ms).toISOString().slice(0, 10) : null;
    const sites = pts.features.map(f => {
      const a = f.attributes, s = byName.get(a[CONFIG.siteNameField]);
      return { id: a[CONFIG.siteIdField], name: a[CONFIG.siteNameField], lon: f.geometry.x, lat: f.geometry.y,
        n: s?.n ?? 0, first: iso(s?.d0), last: iso(s?.d1), total: s?.total ?? 0 };
    }).sort((a, b) => a.name.localeCompare(b.name));
    const t = total.features[0].attributes;
    return { sites, total: { n: t.n, first: iso(t.d0), last: iso(t.d1) } };
  }

  async function loadSeries(site) {
    const json = await LTI.query(CONFIG.recordsTable, { where: `${CONFIG.recordSiteField} = '${sq(site.name)}'`, outFields: `${CONFIG.dateField},${CONFIG.valueField}`, orderByFields: CONFIG.dateField, returnGeometry: 'false' });
    const months = new Map();
    for (const f of json.features) {
      const a = f.attributes; if (a[CONFIG.dateField] === null) continue;
      const k = monthKey(a[CONFIG.dateField]); const m = months.get(k) || [0, 0];
      m[0] += Number(a[CONFIG.valueField]) || 0; m[1] += 1; months.set(k, m);
    }
    return [...months.entries()].map(([k, [total, days]]) => [k, total, days]);
  }

  /* ───── state and render ───── */
  let sites = [], total = null, live = false, selected = null, highlight = null;
  const seriesCache = {};
  const plotEl = $('plot'), gridEl = $('grid'), toggleEl = $('toggle'), picker = $('site-select');

  function renderTiles() {
    const withData = sites.filter(s => s.n > 0).length;
    const tiles = [
      { v: `${fmt(sites.length)} <small>sites</small>`, l: `Counter locations, ${fmt(withData)} with data` },
      { v: `${fmt(total?.n)} <small>site days</small>`, l: 'Daily records across all sites' },
      { v: total ? String(total.first.slice(0, 4)) : '—', l: `Record begins (${dateLabel(total?.first)})` },
      { v: total ? dateLabel(total.last) : '—', l: 'Most recent count' },
    ];
    $('tiles').innerHTML = tiles.map(t => `<div class="tile"><div class="v">${t.v}</div><div class="l">${esc(t.l)}</div></div>`).join('');
    $('glance-source').textContent = `Site list and record summary ${live ? 'queried live' : `from a snapshot captured ${SNAPSHOT.captured}`} from the ${CONFIG.sourceName}.`;
  }

  function renderPicker() {
    picker.innerHTML = sites.map(s => `<calcite-option value="${esc(s.id)}" ${s.n ? '' : 'disabled'}>${esc(s.name)}${s.n ? '' : ' (no records)'}</calcite-option>`).join('');
    picker.addEventListener('calciteSelectChange', e => selectSite(e.target.selectedOption?.value || e.target.value, 'picker'));
  }

  function renderFacts(site) {
    $('site-facts').innerHTML = site ? `
      <dt>Site</dt><dd>${esc(site.name)}</dd>
      <dt>Records</dt><dd>${fmt(site.n)} daily counts</dd>
      <dt>Period</dt><dd>${dateLabel(site.first)} to ${dateLabel(site.last)}</dd>
      <dt>Total counted</dt><dd>${fmt(site.total)} ${esc(CONFIG.unitLabel)}</dd>` : '';
  }

  function renderSeries(site, months) {
    $('series-title').textContent = `Monthly counts: ${site.name}`;
    $('series-caption').textContent = CONFIG.seriesCaption || '';
    gridEl.classList.add('hidden'); plotEl.classList.remove('hidden');
    if (!months || !months.length) {
      LTI.showEmpty(plotEl, `No records loaded for ${site.name}.`, live ? ['This site has no counts in the records table.'] : ['The live query is unavailable and the snapshot holds only the default site. Reload when the network allows.']);
      toggleEl.classList.add('hidden');
      return;
    }
    toggleEl.classList.remove('hidden');
    plotEl.innerHTML = ''; plotEl.classList.add('plot');   // clear the loading note or an empty state before the chart draws
    const rows = months.map(([k, totalV, days]) => ({ month: k, label: monthLabel(k), total: totalV, days, perDay: days ? totalV / days : null }));
    const y = rows.map(r => CONFIG.aggregate === 'mean' ? r.perDay : r.total);
    LTI.plot(plotEl, [{
      type: 'bar', x: rows.map(r => r.month + '-01'), y, marker: { color: LTI.COLORS[0] },
      customdata: rows.map(r => [r.label, r.days, r.perDay]),
      hovertemplate: '<b>%{customdata[0]}</b><br>%{y:,.0f} counted over %{customdata[1]} days<br>%{customdata[2]:,.0f} per day<extra></extra>',
    }], LTI.layout({
      height: 320, bargap: 0.15,
      xaxis: { type: 'date', tickformat: '%b<br>%Y', dtick: 'M12' },
      yaxis: { title: { text: CONFIG.aggregate === 'mean' ? `${CONFIG.valueLabel} per day` : `${CONFIG.valueLabel} per month`, font: { size: 12 } }, rangemode: 'tozero' },
    }), LTI.PLOT_CONFIG);
    toggleEl.selectedItem = toggleEl.querySelector('[value="chart"]');
    if (!toggleEl.dataset.wired) {
      toggleEl.dataset.wired = '1';
      LTI.toggle(toggleEl, {
        chart: () => LTI.showPlot(plotEl, gridEl),
        table: () => {
          const s = selected;
          LTI.grid(gridEl, [
            { field: 'label', headerName: 'Month', minWidth: 120 },
            { field: 'days', headerName: 'Days counted', type: 'rightAligned' },
            { field: 'total', headerName: 'Total ' + CONFIG.unitLabel, type: 'rightAligned', valueFormatter: p => fmt(p.value) },
            { field: 'perDay', headerName: 'Per day', type: 'rightAligned', valueFormatter: p => fmt(p.value) },
          ], seriesCache[s.id].rows, `${CONFIG.exportPrefix}-${s.id}`);
          LTI.showGrid(plotEl, gridEl);
        },
      });
    }
    seriesCache[site.id].rows = rows;
  }

  async function selectSite(id, from) {
    const site = sites.find(s => s.id === id);
    if (!site || site === selected) return;
    selected = site;
    if (from !== 'picker' && picker.value !== id) picker.value = id;
    renderFacts(site);
    if (highlight) highlight(site);
    if (!seriesCache[id]) {
      plotEl.innerHTML = '<p class="loading-note">Loading counts…</p>';
      let months = (SNAPSHOT.series && SNAPSHOT.series[id]) || null;
      if (live) { try { months = await loadSeries(site); } catch (err) { console.warn('Series query failed', err); } }
      seriesCache[id] = { months };
    }
    if (selected !== site) return;
    renderSeries(site, seriesCache[id].months);
    $('series-source').textContent = LTI.sourceText(live, SNAPSHOT.captured, CONFIG.sourceName);
    if (from !== 'init') LTI.setParam('site', id);
    LTI.notifyHeight();
  }

  /* ───── map ───── */
  function buildMap() {
    const container = $('map');
    if (typeof require !== 'function') { container.querySelector('.map-placeholder').textContent = 'The map needs the ArcGIS Maps SDK, which this page did not load. Use the site list instead.'; return; }
    require(['esri/Map', 'esri/views/MapView', 'esri/layers/FeatureLayer', 'esri/Graphic', 'esri/widgets/Home', 'esri/widgets/Fullscreen'],
      function (Map, MapView, FeatureLayer, Graphic, Home, Fullscreen) {
        const accent = LTI.accent(root);
        const layer = new FeatureLayer({
          url: CONFIG.sitesLayer, outFields: [CONFIG.siteIdField, CONFIG.siteNameField], popupEnabled: false,
          renderer: { type: 'simple', symbol: { type: 'simple-marker', size: 9, color: accent, outline: { color: 'white', width: 1.2 } } },
        });
        const map = new Map({ basemap: 'topo-vector', layers: [layer] });
        const view = new MapView({ container, map, center: CONFIG.mapCenter, zoom: CONFIG.mapZoom, constraints: { snapToZoom: false } });
        view.ui.move('zoom', 'top-left');
        view.ui.add(new Home({ view }), 'top-left');
        view.ui.add(new Fullscreen({ view }), 'top-left');
        LTI.guardMap(view);
        view.when(() => {
          container.querySelector('.map-placeholder')?.remove();
          if (selected) highlight(selected);
          view.on('click', async evt => {
            const hit = await view.hitTest(evt, { include: layer });
            const g = hit.results[0]?.graphic;
            if (g) selectSite(g.attributes[CONFIG.siteIdField], 'map');
          });
          view.on('pointer-move', async evt => {
            const hit = await view.hitTest(evt, { include: layer });
            view.container.style.cursor = hit.results.length ? 'pointer' : 'default';
          });
        }).catch(err => {
          const ph = container.querySelector('.map-placeholder');
          if (ph) ph.textContent = 'The map could not be loaded. Use the site list instead.';
          console.error('Map failed to load', err);
        });
        highlight = (site) => {
          view.graphics.removeAll();
          if (!site) return;
          view.graphics.add(new Graphic({
            geometry: { type: 'point', longitude: site.lon, latitude: site.lat },
            symbol: { type: 'simple-marker', size: 16, color: [0, 0, 0, 0], outline: { color: '#e49937', width: 3 } },
          }));
        };
      });
  }

  /* ───── init ───── */
  buildMap();
  const result = await LTI.load(loadSites, { sites: SNAPSHOT.sites, total: SNAPSHOT.total });
  live = result.live; sites = result.data.sites; total = result.data.total;
  renderTiles();
  renderPicker();
  $('sites-source').textContent = LTI.sourceText(live, SNAPSHOT.captured, CONFIG.sourceName);
  $('footer-vintage').textContent = `Counts ${live ? 'queried live from maps.trpa.org' : `from a snapshot captured ${SNAPSHOT.captured}`}. Most recent record ${dateLabel(total?.last)}.`;
  const wanted = LTI.params().site || CONFIG.defaultSiteId;
  await selectSite(sites.some(s => s.id === wanted && s.n) ? wanted : (sites.find(s => s.n)?.id), 'init');
  LTI.onParams(p => { if (p.site) selectSite(p.site, 'host'); });
});
