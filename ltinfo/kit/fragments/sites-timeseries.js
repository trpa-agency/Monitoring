/* kit/fragments/sites-timeseries.js  v1.2.0
   A map of monitoring sites, a site picker, a few tiles, and the selected site's record over
   time as a chart with a table and CSV. For any program whose data is a point layer of sites
   plus a table of observations.

   One click on a site selects it: the story popup opens on the map, the facts and the chart
   follow, and the URL carries the site. Hover highlights. Tiles and the chart show skeletons
   until the first data arrives; the snapshot shows within a second if the live query is slow.

   Config
     sitesLayer, siteIdField (stable id for deep links), siteNameField (display name and join key)
     recordsTable, recordSiteField (matches siteNameField), dateField, valueField
     aggregate 'sum' | 'mean' per month, valueLabel, unitLabel, defaultSiteId
     mapCenter [lon, lat], mapZoom, seriesCaption, sourceName, exportPrefix
     popup (optional) Arcade file in kit/popups/, default bike-ped-counter.arcade
   Snapshot
     { captured, total: {n, first, last}, sites: [{id, name, lon, lat, n, first, last, total}],
       series: { siteId: [[yyyy-mm, total, days], ...] } }
   Client-side layer attributes (what the Arcade popup sees)
     id, name, n, firstMs, lastMs, total, perDay, rank, sites, trendPct
   Deep link: ?site=<siteId> in the page URL. Selecting a site writes it back. */
LTI.define('sites-timeseries', async function (root, CONFIG, SNAPSHOT) {
  'use strict';
  const { fmt, esc } = LTI;
  const $ = (name) => LTI.ref(root, name);
  const C = LTI.colors(root);
  SNAPSHOT = SNAPSHOT || { captured: 'unknown', total: null, sites: [], series: {} };
  const pad2 = i => String(i).padStart(2, '0');
  /* The past 12 months as popup chart attributes: l01..l12 labels and m01..m12 values. */
  function chartAttrs(rows) {
    const out = {};
    const last = rows ? rows.slice(-12) : [];
    for (let i = 1; i <= 12; i++) { const r = last[i - 1]; out['l' + pad2(i)] = r ? r.label : null; out['m' + pad2(i)] = r ? r.total : null; }
    return out;
  }
  const sq = s => String(s).replace(/'/g, "''");
  const monthKey = ms => new Date(ms).toISOString().slice(0, 7);
  const monthLabel = k => new Date(k + '-15T00:00:00Z').toLocaleDateString('en-US', { month: 'short', year: 'numeric', timeZone: 'UTC' });
  const dateLabel = iso => iso ? new Date(iso + 'T00:00:00Z').toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' }) : '—';
  const ms = iso => iso ? Date.parse(iso + 'T00:00:00Z') : null;

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
    const iso = v => v ? new Date(v).toISOString().slice(0, 10) : null;
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
    return [...months.entries()].sort((a, b) => a[0].localeCompare(b[0])).map(([k, [total, days]]) => [k, total, days]);
  }

  /* Rows for the chart and table, plus what is typical for each calendar month and the
     past-12-months trend. */
  function shape(months) {
    const rows = months.map(([k, total, days]) => ({ month: k, label: monthLabel(k), total, days, perDay: days ? total / days : null }));
    const byCal = {};
    rows.forEach(r => { const mm = r.month.slice(5); (byCal[mm] = byCal[mm] || []).push(CONFIG.aggregate === 'mean' ? r.perDay : r.total); });
    rows.forEach(r => { const v = byCal[r.month.slice(5)].filter(x => x !== null); r.typical = v.length ? v.reduce((a, b) => a + b, 0) / v.length : null; });
    let trendPct = null, recent = null, prior = null;
    if (rows.length >= 24) {
      const last12 = rows.slice(-12), prev12 = rows.slice(-24, -12);
      recent = last12.reduce((a, r) => a + r.total, 0); prior = prev12.reduce((a, r) => a + r.total, 0);
      if (prior > 0) trendPct = (recent - prior) / prior * 100;
    }
    return { rows, trendPct, recent, prior };
  }

  /* ───── state ───── */
  let sites = [], total = null, live = false, selected = null, applied = false;
  let mapParts = null, layer = null;
  const oids = {};              // site id -> client layer object id
  const seriesCache = {};
  const plotEl = $('plot'), gridEl = $('grid'), toggleEl = $('toggle'), picker = $('site-select');

  LTI.skeletonTiles($('tiles'), 4);
  LTI.skeleton(plotEl, 320);
  $('site-facts').innerHTML = '<dt class="skeleton">Site</dt><dd class="skeleton">loading</dd><dt class="skeleton">Records</dt><dd class="skeleton">loading</dd>';

  /* ───── render ───── */
  function renderTiles() {
    const withData = sites.filter(s => s.n > 0);
    const busiest = withData.slice().sort((a, b) => b.total - a.total)[0];
    const tiles = [
      { v: `${fmt(sites.length)} <small>sites</small>`, l: 'Counter locations', d: `${fmt(withData.length)} with counts in the record` },
      { v: `${fmt(total?.n)} <small>site days</small>`, l: 'Daily records across all sites', d: total ? `since ${dateLabel(total.first)}` : '' },
      { v: total ? dateLabel(total.last) : '—', l: 'Most recent count', d: total ? `${fmt(Math.round((Date.now() - ms(total.last)) / 864e5))} days ago` : '' },
      { v: busiest ? `${fmt(Math.round(busiest.total / busiest.n))} <small>a day</small>` : '—', l: 'Busiest counter', d: busiest ? busiest.name : '' },
    ];
    $('tiles').innerHTML = tiles.map(t => `<div class="tile"><div class="v">${t.v}</div><div class="l">${esc(t.l)}</div>${t.d ? `<div class="d">${esc(t.d)}</div>` : ''}</div>`).join('');
    $('glance-source').textContent = `Site list and record summary ${live ? 'queried live' : `from a snapshot captured ${SNAPSHOT.captured}`} from the ${CONFIG.sourceName}.`;
  }

  function renderPicker() {
    picker.innerHTML = sites.map(s => `<calcite-option value="${esc(s.id)}" ${s.n ? '' : 'disabled'}>${esc(s.name)}${s.n ? '' : ' (no records)'}</calcite-option>`).join('');
    if (!picker.dataset.wired) {
      picker.dataset.wired = '1';
      picker.addEventListener('calciteSelectChange', e => selectSite(e.target.selectedOption?.value || e.target.value, 'picker'));
    }
  }

  function trendText(t) {
    if (t === null || t === undefined) return 'record too short to compare';
    const p = Math.round(Math.abs(t));
    return p < 1 ? 'about even with the prior 12 months' : `${p} percent ${t > 0 ? 'above' : 'below'} the prior 12 months`;
  }

  function renderFacts(site, shaped) {
    $('site-facts').innerHTML = site ? `
      <dt>Site</dt><dd>${esc(site.name)}</dd>
      <dt>Records</dt><dd>${fmt(site.n)} daily counts, ${dateLabel(site.first)} to ${dateLabel(site.last)}</dd>
      <dt>Total counted</dt><dd>${fmt(site.total)} ${esc(CONFIG.unitLabel)}, about ${fmt(site.n ? site.total / site.n : null)} a day</dd>
      <dt>Past 12 months</dt><dd>${shaped ? esc(trendText(shaped.trendPct)) : 'loading'}</dd>` : '';
  }

  function renderSeries(site, shaped) {
    $('series-title').textContent = `Monthly counts: ${site.name}`;
    $('series-caption').textContent = CONFIG.seriesCaption || '';
    gridEl.classList.add('hidden'); plotEl.classList.remove('hidden');
    if (!shaped || !shaped.rows.length) {
      LTI.showEmpty(plotEl, `No records loaded for ${site.name}.`, live ? ['This site has no counts in the records table.'] : ['The live query is unavailable and the snapshot holds only the default site. Reload when the network allows.']);
      toggleEl.classList.add('hidden');
      $('series-legend').innerHTML = '';
      return;
    }
    toggleEl.classList.remove('hidden');
    const rows = shaped.rows;
    const yKey = CONFIG.aggregate === 'mean' ? 'perDay' : 'total';
    const yTitle = CONFIG.aggregate === 'mean' ? `${CONFIG.valueLabel} per day` : `${CONFIG.valueLabel} per month`;
    LTI.plot(plotEl, [
      {
        type: 'bar', name: 'This month', x: rows.map(r => r.month + '-01'), y: rows.map(r => r[yKey]), marker: { color: C[0] },
        customdata: rows.map(r => [r.label, r.days, r.perDay]),
        hovertemplate: '<b>%{customdata[0]}</b><br>%{y:,.0f} counted over %{customdata[1]} days<br>%{customdata[2]:,.0f} per day<extra></extra>',
      },
      {
        type: 'scatter', mode: 'lines', name: 'Typical for the month', x: rows.map(r => r.month + '-01'), y: rows.map(r => r.typical),
        line: { color: C[1], width: 2 }, hovertemplate: 'Typical: %{y:,.0f}<extra></extra>',
      },
    ], LTI.layout({
      height: 320, bargap: 0.15, showlegend: false, hovermode: 'x unified',
      xaxis: { type: 'date', tickformat: '%b<br>%Y', dtick: 'M12' },
      yaxis: { title: { text: yTitle, font: { size: 12 } }, rangemode: 'tozero' },
    }, root), LTI.PLOT_CONFIG);
    $('series-legend').innerHTML = `<span><i style="background:${C[0]}"></i>Counted that month</span><span><i style="background:${C[1]};height:2px"></i>Typical for that calendar month at this site</span>`;
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
            { field: 'typical', headerName: 'Typical for the month', type: 'rightAligned', valueFormatter: p => fmt(p.value) },
          ], seriesCache[s.id].rows, `${CONFIG.exportPrefix}-${s.id}`);
          LTI.showGrid(plotEl, gridEl);
        },
      });
    }
  }

  async function selectSite(id, from) {
    const site = sites.find(s => s.id === id);
    if (!site || site === selected) return;
    selected = site;
    if (from !== 'picker' && picker.value !== id) picker.value = id;
    renderFacts(site, seriesCache[id]);
    if (mapParts) LTI.ringAt(mapParts.view, site.lon, site.lat);
    if (from === 'picker' && mapParts && layer) LTI.openFeature(mapParts.view, layer, `id = '${sq(id)}'`, { goTo: true }).catch(() => {});
    if (!seriesCache[id]) {
      LTI.skeleton(plotEl, 320);
      let months = (SNAPSHOT.series && SNAPSHOT.series[id]) || null;
      if (live) { try { months = await loadSeries(site); } catch (err) { console.warn('Series query failed', err); } }
      seriesCache[id] = months ? shape(months) : { rows: [], trendPct: null };
      if (layer && oids[id] !== undefined && seriesCache[id].rows.length) {
        LTI.updateFeature(layer, oids[id], { trendPct: seriesCache[id].trendPct, ...chartAttrs(seriesCache[id].rows) }).then(() => {
          const v = mapParts.view;
          if (v.popup?.visible && v.popup.selectedFeature?.attributes?.id === id) LTI.openFeature(v, layer, `id = '${sq(id)}'`).catch(() => {});
        }).catch(err => console.warn('Trend update failed', err));
      }
    }
    if (selected !== site) return;
    renderFacts(site, seriesCache[id]);
    renderSeries(site, seriesCache[id]);
    $('series-source').textContent = LTI.sourceText(live, SNAPSHOT.captured, CONFIG.sourceName);
    if (from !== 'init') LTI.setParam('site', id);
    LTI.notifyHeight();
  }

  /* ───── map ───── */
  const mapReady = (LTI.map ? LTI.map($('map'), { center: CONFIG.mapCenter, zoom: CONFIG.mapZoom }) : Promise.reject(new Error('ltinfo-map.js is not loaded')))
    .catch(err => {
      const ph = $('map').querySelector('.map-placeholder');
      if (ph) ph.textContent = 'The map could not be loaded. Use the site list instead.';
      console.error('Map failed to load', err);
      return null;
    });

  async function buildLayer() {
    const parts = await mapReady;
    if (!parts) return;
    mapParts = parts;
    const withData = sites.filter(s => s.n > 0).sort((a, b) => b.total - a.total);
    const rank = new Map(withData.map((s, i) => [s.id, i + 1]));
    const features = sites.map((s, i) => {
      oids[s.id] = i + 1;
      return { lon: s.lon, lat: s.lat, attributes: {
        id: s.id, name: s.name, n: s.n, firstMs: ms(s.first), lastMs: ms(s.last), total: s.total,
        perDay: s.n ? s.total / s.n : null, rank: rank.get(s.id) ?? null, sites: withData.length,
        trendPct: seriesCache[s.id]?.trendPct ?? null,
        ...chartAttrs(seriesCache[s.id]?.rows),
      } };
    });
    const popupTemplate = await LTI.popup([CONFIG.popup || 'bike-ped-counter.arcade', CONFIG.popupChart || 'bike-ped-counter-chart.arcade'], { title: '{name}' }).catch(err => { console.warn('Popup unavailable', err); return null; });
    const accent = LTI.accent(root);
    const monthFields = [];
    for (let i = 1; i <= 12; i++) monthFields.push({ name: 'l' + pad2(i), type: 'string' }, { name: 'm' + pad2(i), type: 'double' });
    const fresh = await LTI.clientLayer({
      title: 'Monitoring sites', features, popupTemplate,
      fields: [
        { name: 'id', type: 'string' }, { name: 'name', type: 'string' }, { name: 'n', type: 'integer' },
        { name: 'firstMs', type: 'double' }, { name: 'lastMs', type: 'double' }, { name: 'total', type: 'double' },
        { name: 'perDay', type: 'double' }, { name: 'rank', type: 'integer' }, { name: 'sites', type: 'integer' }, { name: 'trendPct', type: 'double' },
        ...monthFields,
      ],
      renderer: { type: 'unique-value', valueExpression: 'IIf($feature.n > 0, "data", "none")',
        uniqueValueInfos: [
          { value: 'data', label: 'Counter with records', symbol: { type: 'simple-marker', size: 9, color: accent, outline: { color: 'white', width: 1.2 } } },
          { value: 'none', label: 'Counter with no records yet', symbol: { type: 'simple-marker', size: 7, color: '#bdbdbd', outline: { color: 'white', width: 1 } } },
        ] },
    });
    if (layer) parts.map.remove(layer);
    layer = fresh;
    parts.map.add(layer);
    await LTI.wire(parts.view, layer, { onSelect: (a) => { if (a.n > 0) selectSite(a.id, 'map'); } });
    if (!parts.legend) parts.legend = await LTI.legend(parts.view, layer); else parts.legend.layerInfos = [{ layer }];
    if (selected) LTI.ringAt(parts.view, selected.lon, selected.lat);
  }

  /* ───── init: snapshot first if the live query is slow, then live ───── */
  function apply(result) {
    live = result.live; sites = result.data.sites || []; total = result.data.total || null; applied = true;
    renderTiles();
    renderPicker();
    $('sites-source').textContent = LTI.sourceText(live, SNAPSHOT.captured, CONFIG.sourceName);
    $('footer-vintage').textContent = `Counts ${live ? 'queried live from maps.trpa.org' : `from a snapshot captured ${SNAPSHOT.captured}`}. Most recent record ${dateLabel(total?.last)}.`;
  }
  async function start(result) {
    apply(result);
    const wanted = (selected && selected.id) || LTI.params().site || CONFIG.defaultSiteId;
    selected = null;
    for (const k in seriesCache) delete seriesCache[k];
    await buildLayer();
    await selectSite(sites.some(s => s.id === wanted && s.n) ? wanted : (sites.find(s => s.n)?.id), 'init');
  }

  const result = await LTI.load(loadSites, { sites: SNAPSHOT.sites, total: SNAPSHOT.total }, (first) => { start(first); });
  if (result.live || !applied) await start(result);
  LTI.onParams(p => { if (p.site) selectSite(p.site, 'host'); });
});
