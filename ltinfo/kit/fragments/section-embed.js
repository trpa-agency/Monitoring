/* kit/fragments/section-embed.js  v1.1.0
   One chart (or table) with a title, a Chart/Table toggle, a CSV download, a caption, and a
   source line. Config-driven: any table or layer with an x field, a y field, an optional
   series field, and an optional target field.

   Config
     title, service, where, orderBy
     xField, yField, seriesField (null for one series), targetField (null for no standard)
     chartType 'line' | 'bar', xCategorical (true for period labels like '2016-2019')
     xLabel, yLabel, decimals, seriesLabels {value: label}
     caption, sourceName, exportName
   Snapshot
     { captured, rows: [...] } with the same field names as the query. */
LTI.define('section-embed', async function (root, CONFIG, SNAPSHOT) {
  'use strict';
  const { fmt, esc } = LTI;
  const $ = (name) => LTI.ref(root, name);
  const plotEl = $('plot'), gridEl = $('grid'), toggleEl = $('toggle');
  CONFIG.seriesLabels = CONFIG.seriesLabels || {};
  SNAPSHOT = SNAPSHOT || { captured: 'unknown', rows: [] };

  $('title').textContent = CONFIG.title;
  $('caption').textContent = CONFIG.caption || '';

  function normalize(rows) {
    return rows.map(r => ({
      x: r[CONFIG.xField],
      series: CONFIG.seriesField ? String(r[CONFIG.seriesField]) : CONFIG.title,
      y: r[CONFIG.yField] === null ? null : Number(r[CONFIG.yField]),
      target: CONFIG.targetField && r[CONFIG.targetField] !== null ? Number(r[CONFIG.targetField]) : null,
    }));
  }
  function sortedX(rows) {
    const xs = [...new Set(rows.map(r => r.x))];
    return xs.sort((a, b) => (typeof a === 'number' && typeof b === 'number') ? a - b : String(a).localeCompare(String(b)));
  }

  function render(rows) {
    const xs = sortedX(rows);
    const seriesNames = [...new Set(rows.map(r => r.series))];
    const traces = [], legend = [];
    seriesNames.forEach((name, i) => {
      const color = LTI.COLORS[i % LTI.COLORS.length];
      const label = CONFIG.seriesLabels[name] ?? name;
      const byX = new Map(rows.filter(r => r.series === name).map(r => [r.x, r]));
      const ys = xs.map(x => byX.get(x)?.y ?? null);
      traces.push({
        type: CONFIG.chartType === 'bar' ? 'bar' : 'scatter', mode: 'lines+markers', name: label,
        x: xs, y: ys, connectgaps: true,
        line: { color, width: 2 }, marker: { color, size: 7 },
        hovertemplate: `${esc(label)}: %{y:,.${CONFIG.decimals}f}<extra></extra>`,
      });
      const target = rows.find(r => r.series === name && r.target !== null)?.target;
      if (target !== undefined && target !== null) {
        traces.push({
          type: 'scatter', mode: 'lines', name: `${label} standard`, x: xs, y: xs.map(() => target),
          line: { color, width: 1.5, dash: 'dash' }, hoverinfo: 'skip', showlegend: false,
        });
      }
      legend.push(`<span><i style="background:${color}"></i>${esc(label)}${target !== undefined && target !== null ? ` (standard ${fmt(target, CONFIG.decimals)})` : ''}</span>`);
    });
    const layout = LTI.layout({
      height: 340, showlegend: false, hovermode: 'x unified', barmode: 'group',
      xaxis: { type: CONFIG.xCategorical ? 'category' : undefined, title: { text: CONFIG.xLabel, font: { size: 12 } }, tickangle: LTI.narrow(root) ? -45 : 0 },
      yaxis: { title: { text: CONFIG.yLabel, font: { size: 12 } }, rangemode: 'tozero' },
    });
    LTI.plot(plotEl, traces, layout, LTI.PLOT_CONFIG);
    $('legend').innerHTML = seriesNames.length > 1 || CONFIG.targetField ? legend.join('') : '';
  }

  function tableColumns() {
    const cols = [{ field: 'x', headerName: CONFIG.xLabel, minWidth: 130 }];
    if (CONFIG.seriesField) cols.push({ field: 'series', headerName: CONFIG.seriesField.replace(/_/g, ' '), flex: 2, minWidth: 180, valueFormatter: p => CONFIG.seriesLabels[p.value] ?? p.value });
    cols.push({ field: 'y', headerName: CONFIG.yLabel, type: 'rightAligned', valueFormatter: p => fmt(p.value, CONFIG.decimals) });
    if (CONFIG.targetField) cols.push({ field: 'target', headerName: 'Standard', type: 'rightAligned', valueFormatter: p => fmt(p.value, CONFIG.decimals) });
    return cols;
  }

  const { data, live } = await LTI.load(async () => {
    const json = await LTI.query(CONFIG.service, { where: CONFIG.where || '1=1', outFields: '*', orderByFields: CONFIG.orderBy || '' });
    if (!json.features?.length) throw new Error('No rows returned');
    return { rows: json.features.map(f => f.attributes) };
  }, SNAPSHOT);
  const rows = normalize(data.rows || []);
  if (!rows.length) {
    LTI.showEmpty(plotEl, 'No data loaded yet.', ['This section will show ' + String(CONFIG.title).toLowerCase() + '.']);
    toggleEl.classList.add('hidden');
  } else {
    render(rows);
    LTI.toggle(toggleEl, {
      chart: () => LTI.showPlot(plotEl, gridEl),
      table: () => { LTI.grid(gridEl, tableColumns(), rows, CONFIG.exportName); LTI.showGrid(plotEl, gridEl); },
    });
  }
  $('source').textContent = LTI.sourceText(live, SNAPSHOT.captured, CONFIG.sourceName);
});
