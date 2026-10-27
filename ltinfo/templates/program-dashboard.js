/* templates/program-dashboard.js  v1.2.0
   The module behind program-dashboard.html, filled with Tahoe Yellow Cress. A program
   dashboard is specific to its program, so its module lives next to the page, not in
   kit/fragments/, and the fragment names it with data-module. A copy for another program
   gets its own kind name (program:<slug>) and its own module file.

   Tiles and charts show skeletons until the first data arrives, and the snapshot shows within
   a second if the live queries are slow. The map is a client-side layer of sites in the latest
   survey year with a story popup (kit/popups/yellow-cress-site.arcade); one click opens it,
   hover highlights.

   Config
     sitesLayer, siteNameField, yearField, statusField, ownerField, surveyedValue, countField
     summaryTable, summaryYear, summaryValue, summaryTarget, summaryAux
     mapCenter, mapZoom, sourceName, exportPrefix, ownerLabels {code: label}, popup
   Snapshot: the shape loadLive() returns plus `captured`. */
LTI.define('program-dashboard', async function (root, CONFIG, SNAPSHOT) {
  'use strict';
  const { fmt, esc } = LTI;
  const $ = (name) => LTI.ref(root, name);
  const C = LTI.colors(root);
  const pad2 = i => String(i).padStart(2, '0');
  CONFIG.ownerLabels = CONFIG.ownerLabels || {};
  CONFIG.countField = CONFIG.countField || 'COUNT_VALUE';
  const ownerName = code => CONFIG.ownerLabels[code] ?? code ?? 'Unknown';

  /* ───── data ───── */
  async function loadLive() {
    const [summary, rows] = await Promise.all([
      LTI.query(CONFIG.summaryTable, { where: '1=1', outFields: '*', orderByFields: CONFIG.summaryYear, returnGeometry: 'false' }),
      LTI.query(CONFIG.sitesLayer, { where: '1=1', returnGeometry: 'false', orderByFields: `${CONFIG.siteNameField},${CONFIG.yearField}`,
        outFields: `${CONFIG.siteNameField},${CONFIG.yearField},${CONFIG.statusField},${CONFIG.ownerField},${CONFIG.countField},LATITUDE,LONGITUDE` }),
    ]);
    const years = {}, bySite = {};
    for (const f of rows.features) {
      const a = f.attributes, y = a[CONFIG.yearField], name = a[CONFIG.siteNameField];
      if (y === null || !name) continue;
      const surveyed = a[CONFIG.statusField] === CONFIG.surveyedValue;
      years[y] = years[y] || { year: y, surveyed: 0, tracked: 0 };
      years[y].tracked += 1; if (surveyed) years[y].surveyed += 1;
      const s = bySite[name] = bySite[name] || { name, firstYear: y, surveyedYears: 0, occupiedYears: 0, hasCounts: false, lastOccupied: null, latest: null, counts: [] };
      s.firstYear = Math.min(s.firstYear, y);
      if (surveyed) s.surveyedYears += 1;
      const c = a[CONFIG.countField];
      if (c !== null && c !== undefined) {
        s.hasCounts = true;
        if (surveyed) { s.counts.push([y, c]); if (c > 0) { s.occupiedYears += 1; s.lastOccupied = Math.max(s.lastOccupied ?? y, y); } }
      }
      if (!s.latest || y > s.latest[CONFIG.yearField]) s.latest = a;
    }
    const coverage = Object.values(years).sort((a, b) => a.year - b.year);
    const latestYear = coverage[coverage.length - 1].year;
    const sites = Object.values(bySite).map(s => {
      const a = s.latest;
      return { name: s.name, lon: a.LONGITUDE, lat: a.LATITUDE, year: a[CONFIG.yearField], status: a[CONFIG.statusField], owner: ownerName(a[CONFIG.ownerField]),
        count: a[CONFIG.countField], firstYear: s.firstYear, surveyedYears: s.surveyedYears,
        occupiedYears: s.hasCounts ? s.occupiedYears : null, lastOccupied: s.lastOccupied,
        counts: s.counts.sort((p, q) => p[0] - q[0]).slice(-10) };
    }).sort((a, b) => a.name.localeCompare(b.name));
    const inLatest = sites.filter(s => s.year === latestYear);
    const statusLatest = { [CONFIG.surveyedValue]: inLatest.filter(s => s.status === CONFIG.surveyedValue).length };
    statusLatest['No Survey'] = inLatest.length - statusLatest[CONFIG.surveyedValue];
    const ownerLatest = {};
    inLatest.forEach(s => { ownerLatest[s.owner] = (ownerLatest[s.owner] || 0) + 1; });
    return { summary: summary.features.map(f => f.attributes), latestYear, sitesTracked: sites.length, statusLatest, ownerLatest, coverage, sites };
  }

  /* ───── state ───── */
  let data = SNAPSHOT, live = false, applied = false, mapParts = null, layer = null;
  const S = () => data.summary.map(r => ({ year: r[CONFIG.summaryYear], occupied: r[CONFIG.summaryValue], target: r[CONFIG.summaryTarget], lake: r[CONFIG.summaryAux] }));

  LTI.skeletonTiles($('glance-tiles'), 6);
  LTI.skeleton($('occupied-plot'), 340);
  LTI.skeleton($('owner-plot'), 300);
  LTI.skeleton($('coverage-plot'), 300);

  /* ───── render ───── */
  function renderGlance() {
    const s = S(); const last = s[s.length - 1], prev = s[s.length - 2];
    const surveyed = data.statusLatest[CONFIG.surveyedValue] ?? 0;
    const cov = data.coverage || [];
    const prevCov = cov.length > 1 ? cov[cov.length - 2] : null;
    const tiles = [
      { v: `${fmt(data.sitesTracked)} <small>sites</small>`, l: 'Known sites tracked around the lake', d: cov.length ? `first recorded ${cov[0].year}` : '' },
      { v: `${fmt(surveyed)} <small>of ${fmt(data.sitesTracked)}</small>`, l: `Sites surveyed in ${data.latestYear}`, d: prevCov ? LTI.change(surveyed, prevCov.surveyed, '', prevCov.year) : '' },
      { v: `${fmt(last.occupied)} <small>sites</small>`, l: `Occupied in ${last.year}, the latest summary year`, d: prev ? LTI.change(last.occupied, prev.occupied, '', prev.year) : '' },
      { v: `${fmt(last.target)} <small>sites</small>`, l: 'Standard: occupied sites', d: last.occupied >= last.target ? `met in ${last.year}` : `${fmt(last.target - last.occupied)} short in ${last.year}` },
      { v: `${fmt(last.lake)} <small>ft</small>`, l: `Lake level in ${last.year}`, d: prev ? LTI.change(last.lake, prev.lake, 'ft', prev.year) : '' },
      { v: 'Not yet shown', l: 'Plants counted per site', pending: true, d: 'count field under review' },
    ];
    $('glance-tiles').innerHTML = tiles.map(t => `<div class="tile${t.pending ? ' pending' : ''}"><div class="v">${t.v}</div><div class="l">${esc(t.l)}</div>${t.d ? `<div class="d">${esc(t.d)}</div>` : ''}</div>`).join('');
    $('glance-source').textContent = `Figures ${live ? 'queried live' : `from a snapshot captured ${SNAPSHOT.captured}`} from the ${CONFIG.sourceName}. Occupied-site summaries run through ${last.year}; site surveys through ${data.latestYear}.`;
  }

  function wireToggle(name, plotEl, gridEl, columns, rowsFn, exportName) {
    const control = $(name);
    if (control.dataset.wired) return;
    control.dataset.wired = '1';
    LTI.toggle(control, {
      chart: () => LTI.showPlot(plotEl, gridEl),
      table: () => { LTI.grid(gridEl, columns, rowsFn(), exportName); LTI.showGrid(plotEl, gridEl); },
    });
  }

  function renderOccupied() {
    const s = S();
    const target = s.find(r => r.target !== null)?.target;
    const plotEl = $('occupied-plot'), gridEl = $('occupied-grid');
    LTI.plot(plotEl, [
      { type: 'bar', name: 'Occupied sites', x: s.map(r => r.year), y: s.map(r => r.occupied), marker: { color: C[0] }, hovertemplate: '%{x}: %{y} occupied sites<extra></extra>' },
      { type: 'scatter', mode: 'lines+markers', name: 'Lake level (ft)', x: s.map(r => r.year), y: s.map(r => r.lake), yaxis: 'y2', line: { color: C[1], width: 2 }, marker: { size: 5 }, hovertemplate: '%{x}: %{y:,.0f} ft<extra></extra>' },
    ], LTI.layout({
      height: 340, showlegend: false, hovermode: 'x unified', margin: { r: 50 },
      xaxis: { title: { text: 'Survey year', font: { size: 12 } }, tickformat: 'd', dtick: LTI.narrow(root) ? 10 : 5 },
      yaxis: { title: { text: 'Occupied sites', font: { size: 12 } }, rangemode: 'tozero' },
      yaxis2: { title: { text: 'Lake level (ft)', font: { size: 12 } }, overlaying: 'y', side: 'right', showgrid: false, range: [6218, 6232] },
      ...(target !== undefined ? LTI.targetLine(target, `Standard: ${target} sites`) : {}),
    }, root), LTI.PLOT_CONFIG);
    $('occupied-legend').innerHTML = `<span><i style="background:${C[0]}"></i>Occupied sites</span><span><i style="background:${C[1]}"></i>Lake level, right axis</span><span><i style="background:#4e4e4e;height:2px"></i>Standard</span>`;
    wireToggle('occupied-toggle', plotEl, gridEl, [
      { field: 'year', headerName: 'Year', minWidth: 90 },
      { field: 'occupied', headerName: 'Occupied sites', type: 'rightAligned' },
      { field: 'target', headerName: 'Standard', type: 'rightAligned' },
      { field: 'lake', headerName: 'Lake level (ft)', type: 'rightAligned', valueFormatter: p => fmt(p.value) },
    ], S, `${CONFIG.exportPrefix}-occupied-sites`);
  }

  let ownerRows = [];
  function renderOwner() {
    const plotEl = $('owner-plot'), gridEl = $('owner-grid');
    ownerRows = LTI.hbar(plotEl, data.ownerLatest, { labels: CONFIG.ownerLabels, unit: 'sites', axisTitle: 'Sites', color: C[0] });
    $('owner-caption').textContent = `Who manages the land at each of the ${fmt(ownerRows.reduce((a, r) => a + r.value, 0))} sites tracked in ${data.latestYear}.`;
    wireToggle('owner-toggle', plotEl, gridEl, [
      { field: 'label', headerName: 'Land owner', flex: 2, minWidth: 180 },
      { field: 'value', headerName: 'Sites', type: 'rightAligned' },
    ], () => ownerRows, `${CONFIG.exportPrefix}-sites-by-owner`);
  }

  function renderCoverage() {
    const el = $('coverage-plot');
    if (!data.coverage) {
      LTI.showEmpty(el, 'Survey coverage by year needs the live site query.', ['The snapshot holds the latest year only. Reload when the network allows.']);
      return;
    }
    const c = data.coverage;
    LTI.plot(el, [
      { type: 'bar', name: 'Surveyed', x: c.map(r => r.year), y: c.map(r => r.surveyed), marker: { color: C[0] }, hovertemplate: '%{x}: %{y} surveyed<extra></extra>' },
      { type: 'bar', name: 'Not surveyed', x: c.map(r => r.year), y: c.map(r => r.tracked - r.surveyed), marker: { color: '#dcdcdc' }, hovertemplate: '%{x}: %{y} not surveyed<extra></extra>' },
    ], LTI.layout({ height: 300, barmode: 'stack', showlegend: false, hovermode: 'x unified', xaxis: { tickformat: 'd', dtick: LTI.narrow(root) ? 10 : 5 }, yaxis: { title: { text: 'Sites', font: { size: 12 } } } }, root), LTI.PLOT_CONFIG);
  }

  function renderCounts() {
    $('counts-body').innerHTML = LTI.emptyState('Per-site plant counts are not shown yet. When they are, this section will show:', [
      'Plants counted at each site in the latest survey, as a table with CSV download.',
      'The ten largest sites over time.',
      'Sites that changed from occupied to unoccupied, and back, between years.',
    ]);
  }

  /* ───── map ───── */
  const mapReady = (LTI.map ? LTI.map($('map'), { center: CONFIG.mapCenter, zoom: CONFIG.mapZoom }) : Promise.reject(new Error('ltinfo-map.js is not loaded')))
    .catch(err => {
      const ph = $('map').querySelector('.map-placeholder');
      if (ph) ph.textContent = 'The map could not be loaded. The tiles and tables cover the same sites.';
      console.error('Map failed to load', err);
      return null;
    });

  async function buildLayer() {
    const parts = await mapReady;
    if (!parts) return;
    mapParts = parts;
    const sites = (data.sites || []).filter(s => s.year === data.latestYear && s.lon !== null && s.lat !== null);
    if (!sites.length) {
      const ph = $('map').querySelector('.map-placeholder');
      if (ph) ph.textContent = 'Sites appear when the live service is reachable.';
      return;
    }
    const popupTemplate = await LTI.popup([CONFIG.popup || 'yellow-cress-site.arcade', CONFIG.popupChart || 'yellow-cress-site-chart.arcade'], { title: '{name}' }).catch(err => { console.warn('Popup unavailable', err); return null; });
    const accent = LTI.accent(root);
    const yearFields = [];
    for (let i = 1; i <= 10; i++) yearFields.push({ name: 'y' + pad2(i), type: 'string' }, { name: 'c' + pad2(i), type: 'integer' });
    const fresh = await LTI.clientLayer({
      title: `Survey sites, ${data.latestYear}`, popupTemplate,
      features: sites.map(s => {
        const { counts, ...rest } = s;
        const a = { ...rest };
        for (let i = 1; i <= 10; i++) { const yc = (counts || [])[i - 1]; a['y' + pad2(i)] = yc ? String(yc[0]) : null; a['c' + pad2(i)] = yc ? yc[1] : null; }
        return { lon: s.lon, lat: s.lat, attributes: a };
      }),
      fields: [
        { name: 'name', type: 'string' }, { name: 'year', type: 'integer' }, { name: 'status', type: 'string' }, { name: 'owner', type: 'string' },
        { name: 'count', type: 'integer' }, { name: 'firstYear', type: 'integer' }, { name: 'surveyedYears', type: 'integer' },
        { name: 'occupiedYears', type: 'integer' }, { name: 'lastOccupied', type: 'integer' }, { name: 'lon', type: 'double' }, { name: 'lat', type: 'double' },
        ...yearFields,
      ],
      renderer: { type: 'unique-value', field: 'status',
        defaultSymbol: { type: 'simple-marker', size: 8, color: '#bdbdbd', outline: { color: 'white', width: 1 } }, defaultLabel: 'Not surveyed',
        uniqueValueInfos: [{ value: CONFIG.surveyedValue, label: 'Surveyed', symbol: { type: 'simple-marker', size: 9, color: accent, outline: { color: 'white', width: 1.2 } } }] },
    });
    if (layer) parts.map.remove(layer);
    layer = fresh;
    parts.map.add(layer);
    await LTI.wire(parts.view, layer, {});
    if (!parts.legend) parts.legend = await LTI.legend(parts.view, layer);
    else parts.legend.layerInfos = [{ layer }];
  }

  /* ───── init: snapshot first if the live queries are slow, then live ───── */
  function apply(result) {
    data = result.data; live = result.live; applied = true;
    $('map-title').textContent = `Survey sites, ${data.latestYear}`;
    $('map-caption').textContent = `${fmt(data.statusLatest[CONFIG.surveyedValue] ?? 0)} of ${fmt(data.sitesTracked)} sites were surveyed in ${data.latestYear}. Click a site for its story. The tables below give the same sites without the map.`;
    renderGlance();
    renderOccupied();
    renderOwner();
    renderCoverage();
    root.querySelectorAll('[data-source-line]').forEach(p => { p.textContent = LTI.sourceText(live, SNAPSHOT.captured, CONFIG.sourceName); });
    $('footer-vintage').textContent = `Figures ${live ? 'queried live from maps.trpa.org' : `from a snapshot captured ${SNAPSHOT.captured}`}. Plant counts per site: not shown.`;
    buildLayer();
  }

  renderCounts();
  const result = await LTI.load(loadLive, SNAPSHOT, (first) => apply(first));
  if (result.live || !applied) apply(result);
});
