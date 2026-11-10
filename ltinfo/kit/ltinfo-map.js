/* ltinfo-map.js  v1.0.0  (Oct. 26, 2026)
   Map helpers for LT Info fragments. Load after the ArcGIS Maps SDK and ltinfo-embed.js; it
   adds to the LTI global. Everything resolves ArcGIS modules on demand through the SDK's AMD
   loader, so a page without a map pays nothing.

     LTI.map(container, { center, zoom, layers, terrain })   -> { view, map, hillshade, setTerrain }
         Human Geography basemap (Esri's public vector tile items, no key), a hillshade layer
         under the data with a Terrain switch in the map corner (off by default), Home and
         Fullscreen, wheel-zoom guard, popup that never docks, default click popup off so a
         fragment can make one click do everything.
     LTI.popup(arcadeFiles, { title })       -> popupTemplate whose content is the Arcade files' elements, in order
         each file is a name in kit/popups/ (bike-ped-counter.arcade) or a URL; one returns the
         story text, another can return a media element with a chart. The popup docks in the
         top right corner of the map.
     LTI.clientLayer({ features, fields, renderer, popupTemplate, title }) -> FeatureLayer
         A client-side point layer from [{ lon, lat, attributes }], so the popup can tell a
         story from numbers the fragment computed, not only the service's fields.
     LTI.wire(view, layer, { onSelect, popup, hover }) -> { layerView }
         One click selects (onSelect(attributes, graphic)) and opens the story popup; hover
         highlights the feature and shows a pointer.
     LTI.updateFeature(layer, oid, attributes)          change a client-side feature's attributes
     LTI.openFeature(view, layer, where)                open the popup for the feature matching where
     LTI.legend(view, layers, position)                  Legend inline on wide screens, a drawer on phones
     LTI.ringAt(view, lon, lat)                          the selection ring graphic (orange), one at a time

   Breaking changes go to kit/v2/, never to this file. */
(function () {
  'use strict';
  if (!window.LTI) { console.error('ltinfo-map.js needs ltinfo-embed.js first'); return; }

  const HG = { base: '2afe5b807fa74006be6363fd243ffb30', detail: '97fa1365da1e43eabb90d0364326bc2d', label: 'ba52238d338745b1a355407ec9df6768' };
  const HILLSHADE = 'https://services.arcgisonline.com/arcgis/rest/services/Elevation/World_Hillshade/MapServer';
  const SELECT_COLOR = '#e49937';
  const views = [];

  function esri(...names) {
    return new Promise((resolve, reject) => {
      if (typeof require !== 'function') return reject(new Error('The ArcGIS Maps SDK is not loaded on this page.'));
      require(names, (...m) => resolve(m), reject);
    });
  }

  async function map(container, opts = {}) {
    const [Map, MapView, Basemap, VectorTileLayer, TileLayer, Home, Fullscreen] = await esri(
      'esri/Map', 'esri/views/MapView', 'esri/Basemap', 'esri/layers/VectorTileLayer', 'esri/layers/TileLayer', 'esri/widgets/Home', 'esri/widgets/Fullscreen');
    // All three Human Geography layers are base layers, not reference layers, so the data
    // points draw above the detail and label layers. The hillshade sits between the base and
    // the detail layer and multiplies into it when the Terrain switch is on.
    const hillshade = new TileLayer({ url: HILLSHADE, title: 'Terrain', blendMode: 'multiply', opacity: 0.6, visible: false, listMode: 'hide', legendEnabled: false });
    const basemap = new Basemap({
      title: 'Human Geography',
      baseLayers: [
        new VectorTileLayer({ portalItem: { id: HG.base }, title: 'Human Geography Base' }),
        hillshade,
        new VectorTileLayer({ portalItem: { id: HG.detail }, title: 'Human Geography Detail' }),
        new VectorTileLayer({ portalItem: { id: HG.label }, title: 'Human Geography Label' }),
      ],
    });
    const m = new Map({ basemap, layers: opts.layers || [] });
    const view = new MapView({
      container, map: m, center: opts.center, zoom: opts.zoom,
      constraints: { snapToZoom: false },
      popupEnabled: false,
      // The story popup docks in the top right corner of the map, every time, at every width.
      popup: { dockEnabled: true, dockOptions: { position: 'top-right', buttonEnabled: false, breakpoint: false }, visibleElements: { featureNavigation: false, collapseButton: false } },
    });
    view.ui.move('zoom', 'top-left');
    view.ui.add(new Home({ view }), 'top-left');
    view.ui.add(new Fullscreen({ view }), 'top-left');
    LTI.guardMap(view);
    if (opts.terrain !== false) terrainControl(view, hillshade);
    await view.when();
    container.querySelector('.map-placeholder')?.remove();
    views.push(view);   // LTI.views, for QA in the console and the review frame
    return { view, map: m, hillshade, setTerrain: (on) => { hillshade.visible = !!on; } };
  }

  /* Terrain switch, top right of the map. Off by default and remembers nothing. */
  function terrainControl(view, hillshade) {
    const wrap = document.createElement('div');
    wrap.className = 'esri-widget lti-map-ctl';
    wrap.innerHTML = '<calcite-label layout="inline" scale="s">Terrain <calcite-switch scale="s" label="Show terrain"></calcite-switch></calcite-label>';
    const sw = wrap.querySelector('calcite-switch');
    sw.addEventListener('calciteSwitchChange', () => { hillshade.visible = sw.checked; });
    view.ui.add(wrap, 'top-left');   // under Zoom, Home, and Fullscreen; the top right belongs to the popup
    return wrap;
  }

  /* Popup template from one or more Arcade files. Each file returns one popup element (a text
     story, or a media element with a chart) and they stack in order; the title is a field
     template. Cached per URL. */
  const arcade = {};
  function arcadeUrl(file) { return /^(https?:)?\/\/|\//.test(file) ? new URL(file, document.baseURI).href : new URL('popups/' + file, LTI.kitBase).href; }
  async function popup(files, { title = '{name}', outFields = ['*'] } = {}) {
    files = Array.isArray(files) ? files : [files];
    const content = [];
    for (const file of files) {
      const url = arcadeUrl(file);
      if (!arcade[url]) arcade[url] = fetch(url).then(r => { if (!r.ok) throw new Error(`${r.status} loading ${url}`); return r.text(); });
      content.push({ type: 'expression', expressionInfo: { expression: await arcade[url], title: '' } });
    }
    return { title, outFields, content };
  }

  /* Client-side point layer. features: [{ lon, lat, attributes }]. fields: [{ name, type }]
     where type is string | double | integer | date. Object ids are 1..n in input order. */
  async function clientLayer({ features, fields, renderer, popupTemplate, title = 'Sites', outFields = ['*'], objectIdField = 'oid' }) {
    const [FeatureLayer, Graphic] = await esri('esri/layers/FeatureLayer', 'esri/Graphic');
    const source = features.map((f, i) => new Graphic({
      geometry: { type: 'point', longitude: f.lon, latitude: f.lat },
      attributes: { [objectIdField]: i + 1, ...f.attributes },
    }));
    return new FeatureLayer({
      source, fields: [{ name: objectIdField, type: 'oid' }, ...fields], objectIdField,
      geometryType: 'point', spatialReference: { wkid: 4326 },
      renderer, popupTemplate, title, outFields, popupEnabled: !!popupTemplate,
    });
  }

  /* One click: select and tell the story. Hover: highlight and a pointer. */
  async function wire(view, layer, { onSelect, popup: openPopup = true, hover = true } = {}) {
    const layerView = await view.whenLayerView(layer);
    let hl = null, hoverId = null;
    if (hover) {
      view.on('pointer-move', async (evt) => {
        const hit = await view.hitTest(evt, { include: layer });
        const g = hit.results[0]?.graphic || null;
        view.container.style.cursor = g ? 'pointer' : '';
        const id = g ? g.attributes[layer.objectIdField] : null;
        if (id === hoverId) return;
        hoverId = id;
        if (hl) { hl.remove(); hl = null; }
        if (g) hl = layerView.highlight(g);
      });
      view.on('pointer-leave', () => { if (hl) { hl.remove(); hl = null; } hoverId = null; view.container.style.cursor = ''; });
    }
    view.on('click', async (evt) => {
      const hit = await view.hitTest(evt, { include: layer });
      const g = hit.results[0]?.graphic;
      if (!g) { view.closePopup(); return; }
      if (openPopup && layer.popupTemplate) view.openPopup({ features: [g], location: g.geometry });
      if (onSelect) onSelect(g.attributes, g);
    });
    return { layerView };
  }

  async function updateFeature(layer, oid, attributes) {
    const [Graphic] = await esri('esri/Graphic');
    return layer.applyEdits({ updateFeatures: [new Graphic({ attributes: { [layer.objectIdField]: oid, ...attributes } })] });
  }

  /* Open the story popup for the one feature matching `where`; returns the graphic or null. */
  async function openFeature(view, layer, where, { goTo = false } = {}) {
    const q = await layer.queryFeatures({ where, outFields: ['*'], returnGeometry: true });
    const g = q.features[0];
    if (!g) return null;
    view.openPopup({ features: [g], location: g.geometry });
    if (goTo) view.goTo(g.geometry).catch(() => {});   // not awaited: the popup opens even if the pan animation never runs
    return g;
  }

  async function legend(view, layers, position = 'bottom-left') {
    const [Legend, Expand] = await esri('esri/widgets/Legend', 'esri/widgets/Expand');
    const lg = new Legend({ view, layerInfos: (Array.isArray(layers) ? layers : [layers]).map(layer => ({ layer })) });
    LTI.placeLegend(view, lg, Expand, position);
    return lg;
  }

  /* The selection ring: one per view, orange, drawn above the layer. */
  const rings = new WeakMap();
  async function ringAt(view, lon, lat) {
    const [Graphic] = await esri('esri/Graphic');
    const old = rings.get(view);
    if (old) view.graphics.remove(old);
    if (lon === undefined || lon === null) return null;
    const g = new Graphic({
      geometry: { type: 'point', longitude: lon, latitude: lat },
      symbol: { type: 'simple-marker', size: 18, color: [0, 0, 0, 0], outline: { color: SELECT_COLOR, width: 3 } },
    });
    view.graphics.add(g);
    rings.set(view, g);
    return g;
  }

  Object.assign(window.LTI, { map, popup, clientLayer, wire, updateFeature, openFeature, legend, ringAt, views, mapVersion: '1.0.0' });
})();
