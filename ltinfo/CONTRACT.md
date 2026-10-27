# LT Info content contract

How TRPA-built monitoring content and Lake Tahoe Info fit together. The destination is content that renders inline in LT Info pages, built in ESA's framework, with staff authoring in ESA's repository once the framework supports it. TRPA builds the prototypes in the Monitoring repo in that shape, and keeps owning the data behind them. Until the framework lands, the same content runs in today's Dashboard card, which is an iframe. Version 1.1.0, Oct. 26, 2026.

## Three kinds of content

| Kind | Where it appears on LT Info | What TRPA supplies | Size it lives in |
|---|---|---|---|
| Program dashboard | The Dashboard card on a Monitoring Program page (program type Program With Dashboard) | One fragment per program: at a glance, map, charts with tables, measurement notes, sources | The card body, about 980 px wide at desktop, on `#f7f7f7`. The card supplies the header and a Fullscreen control. |
| Section | A section on any program page, beside the program's own LT Info data (requested framework change 1) | One chart, map, or table fragment with its own title line, caption, and source | Full card width, white or `#f7f7f7` body, height set by the content |
| Staff topic page | A page at a vanity URL under monitoring.laketahoeinfo.org, inside the LT Info header, navigation, and footer (requested framework change 2) | Narrative blocks written in the CMS plus one or more section fragments | Main column, about 1,000 px |

A threshold element (one chart or map on thresholds.laketahoeinfo.org) and a climate chart follow the same rules at their own slot sizes. See the ltinfo-platform skill.

## A fragment

A fragment is the unit that moves between TRPA and ESA. It is one `<section class="lti" data-site="monitoring" data-kind="section-embed">` holding its markup, a config JSON block (`<script type="application/json" data-lti="config">`: fields, service URL, labels, captions), and an optional snapshot JSON block with the same shape as the live query. A module registered with `LTI.define(kind, fn)` gives the kind its behavior; the kit's `LTI.mount(root)` runs after the fragment is in the document, reads the blocks, and calls the module, which queries, draws, and wires the controls. A fragment has no element ids (elements carry `data-ref`), loads no executable script of its own, and never reaches outside its root. Two fragments on one page do not interact.

In the Monitoring repo each fragment sits inside a harness page, a standalone HTML file that supplies the document, the pinned libraries, and the kit, so the fragment can be reviewed at a URL now. `fragment:start` and `fragment:end` markers delimit the fragment from the harness. A page can also pull a fragment in with `<div data-lti-include="url">`, which fetches the harness page, takes the fragment, and mounts it inline; this is how the topic page template composes sections today. In ESA's framework the harness is their layout and the include is however their framework renders a section.

## What every fragment does

1. **No chrome.** No logo, page title, navigation, or footer. The host provides them. Program name, category, partners, documents, and indicators stay in the CMS and are not repeated.
2. **Matches the host.** Lexend Deca on every text element, including Plotly layouts and AG Grid. Background matches the slot (`#f7f7f7` in a Dashboard card, white elsewhere). Section labels, tile accents, and controls use the host dashboard's card header color (Monitoring `#789440`, Thresholds `#6c91a4`, Transportation `#2c5282`). No TRPA, EIP, or Tahoe Living brand colors.
3. **Fluid width.** Designed for the slot and checked at 375 px. No horizontal scroll at any width.
4. **Sizes itself in the flow.** Normal document height, no fixed page height. Charts get a definite pixel height through `LTI.plot` so a container resize does not collapse them. When framed (interim, below), the kit also reports height to the parent.
5. **Stays inside its root.** Maps stop wheel events from propagating so page scrolling does not zoom the map. Legends stay inside the fragment (inline on wide screens, a drawer on phones). No global ids, no styles outside `.lti`, no popover that escapes the slot.
6. **Every chart has a table and a CSV.** A Chart/Table toggle on each chart card and a Download CSV button on the table. Every map has the same facts in text or a table nearby.
7. **Data is live with a fallback.** Numbers are queried at mount from a TRPA service and fall back to a snapshot with the same shape and a capture date. The fragment says which one is showing. No chart is ever drawn from placeholder numbers; a section without data shows an explicit empty state saying what will appear there.
8. **Sources and vintage.** A source line under every card and a data vintage line in the footer.
9. **Accessible.** WCAG 2.1 AA: text contrast, keyboard-reachable controls, labelled regions, alt text or a text equivalent for every map and chart.
10. **Libraries from the host, pinned.** Plotly, Calcite, the ArcGIS Maps SDK, and AG Grid are loaded once by the layout at fixed versions (Calcite and the ArcGIS SDK must match). The fragment uses whatever the layout loaded. No multi-megabyte inlined chart exports.
11. **Writes nothing to the host.** No cookies, storage, or requests to LT Info beyond the anonymous read API.
12. **Deep links from the page URL.** A fragment that has a selection (a site, a year) reads it from the page's query string (`?site=651`) and writes it back with `history.replaceState`, so a URL reproduces a view.
13. **Maps read as part of the page.** Esri Human Geography (light) basemap from Esri's public vector tile items, with the data points drawn above its labels, and a terrain switch that adds hillshade, off by default. One click on a feature selects it, opens its story, and every other card on the fragment follows; hover highlights. Popups dock in the top right corner of the map and are Arcade files kept under `kit/popups/`, fetched at mount: a text element that leads with the story of that site, then the facts, and a media element with a chart in the dashboard's accent. The same Arcade runs in ArcGIS Pro and Portal web maps. No field dumps, no "view details" step.
15. **Charts lead with the dashboard's color.** Series colors come from the fragment's `data-site` palette (`LTI.colors`): monitoring green first on the Monitoring Dashboard, the host navy second, then the host palette. The LT Info blue colorway is the default only for fragments with no `data-site`.
14. **Loads without a blank.** Skeleton blocks stand in for tiles and charts while queries run. If a query is slow the snapshot shows within a second and the live numbers replace it. Controls are the same across fragments: segmented control for Chart/Table, select for site, switch for terrain, button for CSV, all Calcite.

## What the port needs from ESA's framework

The prototypes are built so that moving them is mechanical. For that to hold, the framework needs five things. Each is a question for the scoping conversation, not a demand.

| Need | Why | What the prototype does meanwhile |
|---|---|---|
| A way to render a fragment inline: a partial view, a CMS section type, or a static include | The fragment is markup plus a mount call, not a URL | The harness page renders it |
| A config field per fragment instance (JSON) that staff can edit | Fields, labels, and captions change per program without code changes | A `CONFIG` object in the file |
| A hook that runs `LTI.mount` after each fragment renders, or the kit's own DOMContentLoaded pass if fragments are in the initial HTML | Charts and maps draw after the markup exists | The kit mounts on DOMContentLoaded |
| The kit CSS and JS in the layout, and the four libraries loaded once at pinned versions | One copy per page, no duplicate Plotly or Calcite | Each harness loads its own |
| Tokens exposed as CSS variables, or an agreed mapping from the kit's variables to theirs | The kit's values are sampled from the live site and will drift | `kit/ltinfo-embed.css` declares them on `:root` and `.lti` |

Snapshot data stays TRPA's. A job in the Monitoring repo writes each fragment's snapshot JSON to GitHub Pages, and the fragment fetches it at mount when the live query fails. ESA's framework never needs to hold TRPA data.

## Interim: the Dashboard card iframe

Today's Dashboard card loads a URL in an iframe at a fixed height (about 765 px), and the three Forest Health dashboards run there. Every prototype works in it unchanged, because the harness page is a complete document. Two things the kit does only when framed (`window.parent !== window`):

| Direction | Message | When | Fields |
|---|---|---|---|
| page to host | `{ type: 'ltinfo-embed:height', height: 1234, source: 'subalpine-conifer' }` | load, resize, any content size change, after a Chart/Table switch | `height` is `document.documentElement.scrollHeight` in CSS pixels; `source` is the page's `data-embed` name |
| host to page | `{ type: 'ltinfo-embed:params', params: { site: '651' } }` | after the iframe loads, and when the host wants to change a selection | any string keys; the page merges them with its URL query parameters |

If LT Info sets the iframe height from the height message (requested framework change 5), the card stops clipping at 765 px. If it does not, the pages still render at the fixed height with their own scroll. Both sides check `type` only until an origin allowlist exists. This section goes away when inline rendering lands.

## Hosting and origins

Prototypes are served from GitHub Pages at `https://trpa-agency.github.io/Monitoring/ltinfo/...`, with `data.trpa.gov` and `tools.trpa.gov` to follow. For the interim iframe the requested allowlist of embeddable origins is `trpa-agency.github.io`, `data.trpa.gov`, `tools.trpa.gov`, `maps.trpa.org`, and `tahoeopendata.org`. Inline, the only cross-origin traffic is the fragment's data requests to `maps.trpa.org` and the snapshot fetch from GitHub Pages, both anonymous GETs with CORS enabled.

If a fragment fails to mount, the slot should show the section's description and a direct link to the prototype page instead of an empty block.

## The style kit

`ltinfo/kit/ltinfo-embed.css` and `ltinfo/kit/ltinfo-embed.js` in the Monitoring repo carry the host tokens, the card and label-bar styles, the stat tiles, the skeleton and empty states, the AG Grid look, the Plotly defaults, `LTI.plot`, `LTI.mount`, the query-with-snapshot helper, and the interim messages. `ltinfo/kit/ltinfo-map.js` adds `LTI.map` (basemap, terrain switch, docked popup), `LTI.popup` (Arcade files to a popup template), `LTI.clientLayer` (a layer from numbers the fragment computed), `LTI.wire` (one-click select and hover), `LTI.legend`, and `LTI.ringAt`. `ltinfo/kit/popups/` holds the Arcade files, a story file and a chart file per layer. Values were sampled from the live site on Oct. 13, 2026 and will drift as ESA updates the frame. The kit is written to move into ESA's build as is: the CSS scopes under `.lti`, the JS touches nothing outside the root it is given. A versioned kit published by ESA (requested framework change 6) replaces these files; until then TRPA keeps them current and versions them by path (`kit/` is 1.x, breaking changes go to `kit/v2/`).

## Data

Most numbers come from the LTInfo_Monitoring map service on `maps.trpa.org` (anonymous, CORS enabled, `maxRecordCount` 2,000,000) and from program-specific services and repo outputs. Fragments do not call the LT Info internal API from the browser; where LT Info data is needed, a server-side job snapshots it to GitHub Pages first. A documented public read API (requested framework change 7) would let fragments draw from LT Info directly.

## Checklist before a fragment goes to ESA or the LT Info admin

1. Renders at the slot width and at 375 px with no horizontal scroll, on the right background.
2. Lexend Deca everywhere, including Plotly and AG Grid.
3. No console errors on mount, alone and with a second fragment on the same page.
4. Live query succeeds, and the snapshot renders when the network is blocked.
5. Every chart has a working Table toggle and CSV download. Sources and vintage are stated.
6. No element ids, no styles outside `.lti`, no executable script inside the fragment markers (JSON blocks only).
7. Map on the Human Geography basemap with the points above its labels, terrain switch works, the popup docks top right with the story and the chart from the Arcade files, one click selects and the cards follow.
8. Skeletons show while loading; nothing renders blank.
9. Height message observed in the review frame (interim only).
10. Committed, pushed, and the GitHub Pages URL returns the harness page.
11. `trpa-dashboard-qa` run and passed.
