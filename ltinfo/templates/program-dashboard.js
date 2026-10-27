/* templates/program-dashboard.js
   The module behind program-dashboard.html, filled with Tahoe Yellow Cress. A program
   dashboard is specific to its program, so its module lives next to the page, not in
   kit/fragments/, and the fragment names it with data-module. A copy for another program
   gets its own kind name (program:<slug>) and its own module file.

   Config
     sitesLayer, siteNameField, yearField, statusField, ownerField, surveyedValue
     summaryTable, summaryYear, summaryValue, summaryTarget, summaryAux
     mapCenter, mapZoom, sourceName, exportPrefix, ownerLabels {code: label}
   Snapshot: the shape loadLive() returns plus `captured`. */
LTI.define('program-dashboard', async function (root, CONFIG, SNAPSHOT) {
  'use strict';
  const { fmt, esc } = LTI;
  const $ = (name) => LTI.ref(root, name);
  const stat = (t, f, n) => ({ statisticType: t, onStatisticField: f, outStatisticFieldName: n });
  CONFIG.ownerLabels = CONFIG.ownerLabels || {};

  async function loadLive() {
    const [summary, byYear, bySite] = await Promise.all([
      LTI.query(CONFIG.summaryTable, { where: '1=1', outFields: '*', orderByFields: CONFIG.summaryYear, returnGeometry: 'false' }),
      LTI.query(CONFIG.sitesLayer, { where: '1=1', returnGeometry: 'false', groupByFieldsForStatistics: `${CONFIG.yearField},${CONFIG.statusField}`, outStatistics: [stat('count', 'OBJECTID', 'n')] }),
      LTI.query(CONFIG.sitesLayer, { where: '1=1', returnGeometry: 'false', groupByFieldsForStatistics: CONFIG.siteNameField, outStatistics: [stat('count', 'OBJECTID', 'n')] }),
    ]);
    const years = {};
    for (const f of byYear.features) {
      const a = f.attributes; const y = a[CONFIG.yearField]; years[y] = years[y] || { year: y, surveyed: 0, tracked: 0 };
      years[y].tracked += a.n; if (a[CONFIG.statusField] === CONFIG.surveyedValue) years[y].surveyed += a.n;
    }
    const coverage = Object.values(years).sort((a, b) => a.year - b.year);
    const latestYear = coverage[coverage.length - 1].year;
    const owners = await LTI.query(CONFIG.sitesLayer, { where: `${CONFIG.yearField} = ${latestYear}`, returnGeometry: 'false', groupByFieldsForStatistics: CONFIG.ownerField, outStatistics: [stat('count', 'OBJECTID', 'n')] });
    const statusLatest = {}; const latest = coverage[coverage.length - 1];
    statusLatest[CONFIG.surveyedValue] = latest.surveyed; statusLatest['No Survey'] = latest.tracked - latest.surveyed;
    return {
      summary: summary.features.map(f => f.attributes),
      latestYear, sitesTracked: bySite.features.length, statusLatest,
      ownerLatest: Object.fromEntries(owners.features.map(f => [f.attributes[CONFIG.ownerField] ?? 'Unknown', f.attributes.n])),
      coverage,
    };
  }

  let data = SNAPSHOT, live = false;
  const S = () => data.summary.map(r => ({ year: r[CONFIG.summaryYear], occupied: r[CONFIG.summaryValue], target: r[CONFIG.summaryTarget], lake: r[CONFIG.summaryAux] }));

  function renderGlance() {
    const s = S(); const last = s[s.length - 1];
    const surveyed = data.statusLatest[CONFIG.surveyedValue] ?? 0;
    const tiles = [
      { v: `${fmt(data.sitesTracked)} <small>sites</small>`, l: 'Known sites tracked around the lake' },
      { v: `${fmt(surveyed)} <small>of ${fmt(data.sitesTracked)}</small>`, l: `Sites surveyed in ${data.latestYear}` },
      { v: `${fmt(last.occupied)} <small>sites</small>`, l: `Occupied in ${last.year}, the latest summary year` },
      { v: `${fmt(last.target)} <small>sites</small>`, l: 'Standard: occupied sites' },
      { v: `${fmt(last.lake)} <small>ft</small>`, l: `Lake level in ${last.year}` },
      { v: 'Not yet shown', l: 'Plants counted per site', pending: true },
    ];
    $('glance-tiles').innerHTML = tiles.map(t => `<div class="tile${t.pending ? ' pending' : ''}"><div class="v">${t.v}</div><div class="l">${esc(t.l)}</div></div>`).join('');
    $('glance-source').textContent = `Figures ${live ? 'queried live' : `from a snapshot captured ${SNAPSHOT.captured}`} from the ${CONFIG.sourceName}. Occupied-site summaries run through ${last.year}; site surveys through ${data.latestYear}.`;
  }

  function renderOccupied() {
    const s = S();
    const target = s.find(r => r.target !== null)?.target;
    const plotEl = $('occupied-plot'), gridEl = $('occupied-grid');
    const traces = [
      { type: 'bar', name: 'Occupied sites', x: s.map(r => r.year), y: s.map(r => r.occupied), marker: { color: LTI.COLORS[0] }, hovertemplate: '%{x}: %{y} occupied sites<extra></extra>' },
      { type: 'scatter', mode: 'lines+markers', name: 'Lake level (ft)', x: s.map(r => r.year), y: s.map(r => r.lake), yaxis: 'y2', line: { color: LTI.COLORS[3], width: 2 }, marker: { size: 5 }, hovertemplate: '%{x}: %{y:,.0f} ft<extra></extra>' },
    ];
    const layout = LTI.layout({
      height: 340, showlegend: false, hovermode: 'x unified', margin: { r: 50 },
      xaxis: { title: { text: 'Survey year', font: { size: 12 } }, tickformat: 'd', dtick: LTI.narrow(root) ? 10 : 5 },
      yaxis: { title: { text: 'Occupied sites', font: { size: 12 } }, rangemode: 'tozero' },
      yaxis2: { title: { text: 'Lake level (ft)', font: { size: 12 } }, overlaying: 'y', side: 'right', showgrid: false, range: [6218, 6232] },
      ...(target !== undefined ? LTI.targetLine(target, `Standard: ${target} sites`) : {}),
    });
    LTI.plot(plotEl, traces, layout, LTI.PLOT_CONFIG);
    $('occupied-legend').innerHTML = `<span><i style="background:${LTI.COLORS[0]}"></i>Occupied sites</span><span><i style="background:${LTI.COLORS[3]}"></i>Lake level, right axis</span><span><i style="background:#4e4e4e;height:2px"></i>Standard</span>`;
    LTI.toggle($('occupied-toggle'), {
      chart: () => LTI.showPlot(plotEl, gridEl),
      table: () => {
        LTI.grid(gridEl, [
          { field: 'year', headerName: 'Year', minWidth: 90 },
          { field: 'occupied', headerName: 'Occupied sites', type: 'rightAligned' },
          { field: 'target', headerName: 'Standard', type: 'rightAligned' },
          { field: 'lake', headerName: 'Lake level (ft)', type: 'rightAligned', valueFormatter: p => fmt(p.value) },
        ], s, `${CONFIG.exportPrefix}-occupied-sites`);
        LTI.showGrid(plotEl, gridEl);
      },
    });
  }

  function renderOwner() {
    const plotEl = $('owner-plot'), gridEl = $('owner-grid');
    const rows = LTI.hbar(plotEl, data.ownerLatest, { labels: CONFIG.ownerLabels, unit: 'sites', axisTitle: 'Sites', color: LTI.COLORS[0] });
    $('owner-caption').textContent = `Who manages the land at each of the ${fmt(rows.reduce((a, r) => a + r.value, 0))} sites tracked in ${data.latestYear}.`;
    LTI.toggle($('owner-toggle'), {
      chart: () => LTI.showPlot(plotEl, gridEl),
      table: () => {
        LTI.grid(gridEl, [
          { field: 'label', headerName: 'Land owner', flex: 2, minWidth: 180 },
          { field: 'value', headerName: 'Sites', type: 'rightAligned' },
        ], rows, `${CONFIG.exportPrefix}-sites-by-owner`);
        LTI.showGrid(plotEl, gridEl);
      },
    });
  }

  function renderCoverage() {
    const el = $('coverage-plot');
    if (!data.coverage) {
      LTI.showEmpty(el, 'Survey coverage by year needs the live site query.', ['The snapshot holds the latest year only. Reload when the network allows.']);
      return;
    }
    const c = data.coverage;
    LTI.plot(el, [
      { type: 'bar', name: 'Surveyed', x: c.map(r => r.year), y: c.map(r => r.surveyed), marker: { color: LTI.COLORS[0] }, hovertemplate: '%{x}: %{y} surveyed<extra></extra>' },
      { type: 'bar', name: 'Not surveyed', x: c.map(r => r.year), y: c.map(r => r.tracked - r.surveyed), marker: { color: '#dcdcdc' }, hovertemplate: '%{x}: %{y} not surveyed<extra></extra>' },
    ], LTI.layout({ height: 300, barmode: 'stack', showlegend: false, hovermode: 'x unified', xaxis: { tickformat: 'd', dtick: LTI.narrow(root) ? 10 : 5 }, yaxis: { title: { text: 'Sites', font: { size: 12 } } } }), LTI.PLOT_CONFIG);
  }

  function renderCounts() {
    $('counts-body').innerHTML = LTI.emptyState('Per-site plant counts are not shown yet. When they are, this section will show:', [
      'Plants counted at each site in the latest survey, as a table with CSV download.',
      'The ten largest sites over time.',
      'Sites that changed from occupied to unoccupied, and back, between years.',
    ]);
  }

  function buildMap() {
    const container = $('map');
    if (typeof require !== 'function') { container.querySelector('.map-placeholder').textContent = 'The map needs the ArcGIS Maps SDK, which this page did not load. The tiles and tables cover the same sites.'; return; }
    require(['esri/Map', 'esri/views/MapView', 'esri/layers/FeatureLayer', 'esri/widgets/Legend', 'esri/widgets/Home', 'esri/widgets/Expand', 'esri/widgets/Fullscreen'],
      function (Map, MapView, FeatureLayer, Legend, Home, Expand, Fullscreen) {
        const accent = LTI.accent(root);
        const layer = new FeatureLayer({
          url: CONFIG.sitesLayer, title: `Survey sites, ${data.latestYear}`,
          definitionExpression: `${CONFIG.yearField} = ${data.latestYear}`, outFields: [CONFIG.siteNameField, CONFIG.statusField, CONFIG.ownerField],
          renderer: { type: 'unique-value', field: CONFIG.statusField,
            defaultSymbol: { type: 'simple-marker', size: 8, color: '#bdbdbd', outline: { color: 'white', width: 1 } }, defaultLabel: 'Not surveyed',
            uniqueValueInfos: [{ value: CONFIG.surveyedValue, label: 'Surveyed', symbol: { type: 'simple-marker', size: 9, color: accent, outline: { color: 'white', width: 1.2 } } }] },
          popupTemplate: { title: `{${CONFIG.siteNameField}}`, content: `{${CONFIG.statusField}} in ${data.latestYear}. Land owner: {${CONFIG.ownerField}}.` },
        });
        const map = new Map({ basemap: 'topo-vector', layers: [layer] });
        const view = new MapView({ container, map, center: CONFIG.mapCenter, zoom: CONFIG.mapZoom, constraints: { snapToZoom: false }, popup: { dockEnabled: false, dockOptions: { buttonEnabled: false } } });
        view.ui.move('zoom', 'top-left');
        view.ui.add(new Home({ view }), 'top-left');
        view.ui.add(new Fullscreen({ view }), 'top-left');
        LTI.guardMap(view);
        view.when(() => {
          LTI.placeLegend(view, new Legend({ view, layerInfos: [{ layer }] }), Expand);
          container.querySelector('.map-placeholder')?.remove();
        }).catch(err => {
          const ph = container.querySelector('.map-placeholder');
          if (ph) ph.textContent = 'The map could not be loaded. The tiles and tables cover the same sites.';
          console.error('Map failed to load', err);
        });
      });
  }

  renderCounts();
  const result = await LTI.load(loadLive, SNAPSHOT);
  data = result.data; live = result.live;
  $('map-title').textContent = `Survey sites, ${data.latestYear}`;
  $('map-caption').textContent = `${fmt(data.statusLatest[CONFIG.surveyedValue] ?? 0)} of ${fmt(data.sitesTracked)} sites were surveyed in ${data.latestYear}. The tables below give the same sites without the map.`;
  buildMap();
  renderGlance();
  renderOccupied();
  renderOwner();
  renderCoverage();
  root.querySelectorAll('[data-source-line]').forEach(p => { p.textContent = LTI.sourceText(live, SNAPSHOT.captured, CONFIG.sourceName); });
  $('footer-vintage').textContent = `Figures ${live ? 'queried live from maps.trpa.org' : `from a snapshot captured ${SNAPSHOT.captured}`}. Plant counts per site: not shown.`;
});
