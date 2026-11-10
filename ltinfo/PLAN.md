# LT Info Monitoring pages: plan

Prototype pages for monitoring.laketahoeinfo.org, a shared kit, a Monitoring section on the TRPA design system, and an update to the trpa-monitoring-dashboard skill. Prototypes are built in this folder of the Monitoring repo and publish at https://trpa-agency.github.io/Monitoring/ltinfo/. The destination is ESA's framework: pages render inline in LT Info, not in iframes, and staff eventually author them in ESA's GitHub. Written Oct. 26, 2026, revised the same day for the inline target.

## Decisions so far

- ESA is scoping a collaborative integration: TRPA content renders inline in LT Info pages, with staff authoring in ESA's repository once the framework supports it. No iframes in the end state.
- Prototypes are built here first, in a shape that ports with as little rewriting as possible: each template is a fragment (markup, config, and a mount call) plus a thin harness page that wraps it for local review. The fragment and the kit are what move to ESA. The data, the snapshot job, the harness pages, and the review frame stay here.
- Until ESA's framework lands, the Dashboard card on LT Info is an iframe, and the three Forest Health embeds live in it. Every prototype keeps working there. The height message contract is the interim, not the target.
- First pages: the three Forest Health programs (Subalpine Conifer, Forest Structure, Fire Dynamics), Bicycle and Pedestrian, and Noise.
- Audience is all three at once: ESA scoping material, Jeanne and leadership review, and working templates for staff.
- The tools repo only lists the prototypes as beta cards on qa/monitoring through apps.json.
- Data comes from the LTInfo_Monitoring map service on maps.trpa.org, with a snapshot fallback in each page.
- Extend trpa-monitoring-dashboard rather than add a new skill.
- The design system page stays at tools/qa/data/design-system.html.

## What moves and what stays

| Piece | Today (Monitoring repo) | In ESA's framework |
|---|---|---|
| Tokens | `kit/ltinfo-embed.css` `:root` block, sampled from the live site | Their own stylesheet. The sampled values become a diff against their real variables. |
| Kit styles | `kit/ltinfo-embed.css` classes under a wrapper | A component stylesheet in their build, same class names |
| Kit script | `kit/ltinfo-embed.js`, `window.LTI` | One script their layout loads once; `LTI.mount(rootEl, config)` runs after each fragment renders |
| Libraries | Pinned CDN tags in each harness page | Their layout loads Plotly, AG Grid, Calcite, and the ArcGIS SDK once, at the pinned versions |
| Fragment | The body of each template between the harness markers | A partial view or a CMS section type that renders the markup and a config JSON field |
| Config | A `CONFIG` object in the file | A JSON field on the section record, edited in the CMS or in their repo |
| Popups | `kit/popups/*.arcade`, fetched at mount | The same files in their repo, or fetched from Pages. Plain text, no build step. |
| Snapshot | A `SNAPSHOT` object in the file | A JSON file written by a TRPA job and fetched at mount, or kept in the record |
| Data queries | Browser to maps.trpa.org | Same. Nothing calls the LT Info internal API from the browser. |
| Topic page | `templates/topic-page.html` with iframes | A CMS page at a vanity URL holding narrative blocks and fragment sections inline |
| Review frame | `templates/_frame.html` sizing iframes | Retired, or kept as a mock that renders fragments inline |
| Height and params messages | `LTI.notifyHeight`, `LTI.onParams` | Dropped. Inline content sizes itself in the flow and reads the page URL. |

ESA's framework is ProjectFirma (ASP.NET MVC with Razor views and Bootstrap 3, as far as the shell shows). Confirm with ESA which of these holds before phase 8: how a fragment is rendered (partial view, CMS section type, or static include), where the config lives, and how a script runs after render.

## Folder layout

```
ltinfo/
  PLAN.md                 this file
  CONTRACT.md             what a fragment does, the interim iframe contract, and what the port needs from ESA
  kit/
    ltinfo-embed.css      host tokens and every style, scoped under .lti; harness page styles under .lti-harness (v1.1.0, Oct. 26)
    ltinfo-embed.js       LTI.define, mount, mountAll, include; query helper with snapshot fallback, Chart/Table toggle, CSV, LTI.plot, setParam, interim height and params messages gated on being framed (v1.1.0, Oct. 26)
    fragments/
      section-embed.js    the module behind the section fragment, config-driven
      sites-timeseries.js the module behind the sites fragment, config-driven
    ltinfo-map.js         LTI.map (Human Geography basemap, terrain switch, docked story popup), popup, clientLayer, wire, legend, ringAt (v1.0.0, Oct. 26)
    popups/               Arcade files, fetched at mount and stacked as the popup's content; each returns one popup element
      bike-ped-counter.arcade          the story text for one counter
      bike-ped-counter-chart.arcade    column chart of the past 12 months, monitoring green
      yellow-cress-site.arcade         the story text for one site in the latest survey year
      yellow-cress-site-chart.arcade   column chart of plants counted, last ten counted years
      noise-site.arcade                (with the noise page, phase 5)
  qa/
    subalpine-conifer-kit.html   fixture: the published Subalpine Conifer embed rebuilt on the kit. Not deployed.
  templates/                (each a harness page around one fragment; the fragment sits between fragment:start and fragment:end)
    program-dashboard.html    the Forest Health section pattern. Default data: Tahoe Yellow Cress (layer 9, table 86)
    program-dashboard.js      its module, program-specific, named by data-module on the fragment
    section-embed.html        one chart with title, Chart/Table toggle, CSV, caption, source. Default: plan area noise by land use (table 74)
    sites-timeseries.html     site map, site picker, monthly series, table. Default: bike ped counters (layer 5, table 1). Deep link ?site=
    topic-page.html           narrative blocks plus fragment sections rendered inline through data-lti-include. Default: a short noise page
    _frame.html               mock LT Info shell for review: ?mode=dashboard|section|topic&page=... (interim iframe case)
  pages/                    (built Oct. 26; each is a harness page with kit paths at ../../kit/)
    bike-ped/index.html       the sites fragment filled for Bicycle and Pedestrian (program 33): layer 5 counters, table 1 daily counts
    noise/index.html          topic page spanning Plan Areas (61), Shorezone (62), and Highways (65), with two sections inline
    noise/plan-area-noise.html        section: average CNEL by land use against each standard (table 74)
    noise/shoreline-exceedances.html  section: average exceedances per day across shoreline sites, bars against a standard of zero (table 78)
    forest-health/index.html  hub page: the three Forest Health programs, what each measures, links to the ForestHealth dashboards and program records
  assets/                   thumbnails and any static images (the tools repo holds the card thumbnails)
```

Each published prototype is a single HTML file that links the kit by absolute URL. Nothing is bundled.

## Fragment form (the shape that ports)

Built Oct. 26, 2026 (phase 3).

- A fragment is one `<section class="lti" data-site="monitoring" data-kind="section-embed">` holding its markup, a `<script type="application/json" data-lti="config">` block, and an optional `data-lti="snapshot"` block. `data-site` sits on this wrapper; `data-slot` sits on the harness body.
- Kit CSS scopes everything under `.lti`. Tokens are declared on `.lti`, not `:root`, so the stylesheet can sit in ESA's cascade without touching it. When ESA publishes its own variables, the values on `.lti` become a mapping to theirs.
- Kit JS mounts fragments. `LTI.define(kind, fn)` registers a module; `LTI.mount(root)` reads the JSON blocks and calls `fn(root, config, snapshot)`; `LTI.mountAll()` runs on DOMContentLoaded. A fragment has no element ids: elements carry `data-ref` and the module finds them with `LTI.ref(root, name)`. Two fragments on one page do not collide.
- Modules: generic kinds live in `kit/fragments/<kind>.js` and load on demand. A program dashboard is specific to its program, so its module sits next to the page and the fragment names it with `data-module`.
- The harness page is the standalone file: `<html>`, `<head>` with the pinned CDN tags and the kit links, `<body class="lti-harness" data-slot="card">`, and one fragment between `<!-- fragment:start -->` and `<!-- fragment:end -->`. No inline script. The markers delimit what moves.
- Inline inclusion: `<div data-lti-include="section-embed.html">` on any page. The kit fetches the harness page, takes its fragment, puts it in the placeholder, loads the module, and mounts it. The placeholder's own children are the fallback. This stands in for whatever ESA's framework uses to render a fragment server-side.
- Deep links read and write the page URL (`LTI.params`, `LTI.setParam` with `replaceState`). Height and params messages stay in the kit but only run when `window.parent !== window`. Inline, they are inert.
- Libraries are the harness's job, loaded once per page. A fragment never loads a script tag of its own.

## Map and interaction pass (what the prototypes still need)

The templates prove the data and the slot. They do not yet feel like one product. The goal is a seamless reading experience: a visitor lands on a page and understands the story without hunting. Requirements for every fragment with a map or a selection:

- **Basemap.** Esri Human Geography (light), muted so the data reads first. The `arcgis/human-geography` basemap style needs a token, so the kit builds the basemap from Esri's three public Human Geography vector tile items (base, detail, label), all as base layers so the data points draw above the labels. A terrain switch under the map's zoom controls turns a hillshade layer on and off (multiply blend between the base and the detail layer), off by default, and remembers nothing.
- **Popups that tell a story.** Each layer's popup is one or more Arcade files under `kit/popups/`, fetched at mount and stacked as the popup's content: a text element that leads with the sentence a reader wants (where this site stands, how it compares, what changed), then the facts, and a media element with a column chart in the monitoring green. No field list dumps. The popup docks in the top right corner of the map at every width. The same Arcade files work in ArcGIS Pro and a Portal web map on any layer with the same attribute names, which is the reason to keep them separate from the page. Since the service's own fields rarely carry the story, the fragment builds a client-side layer with the numbers it computed (rank, trend, the past 12 months) and the Arcade reads those.
- **Fewer clicks.** One click on a map point selects the site, opens its story, and the facts, chart, and table update at once. Hover highlights the point and shows a pointer. The picker, the map, and the URL stay in sync, and a `?site=` link lands on the right view. No "view details" buttons, no second step.
- **Visual hierarchy.** Tiles answer the headline question in one glance, each with a direction line (`up from 30 in 2022`, `42 days ago`). Charts show the standard or target as a labeled line, not a legend entry. Default chart views are the comparison that matters: the site's monthly counts against what is typical there for that calendar month; occupied sites against the standard with lake level alongside.
- **Loading and empty.** Skeleton blocks in place of each tile and chart while queries run, never spinners alone, and the snapshot shows within a second if the query is slow (`LTI.load` with an onSnapshot callback; `?lti-slow=1` and `?lti-snapshot=1` exercise both paths). Empty states say what will appear and when.
- **Colors.** Every chart on a Monitoring Dashboard page leads with the monitoring green (`#789440`) and takes its second series from the host navy; the palette is set per `data-site` in the kit CSS and read with `LTI.colors(root)`, so the same fragment on the Threshold or Transportation dashboard leads with that dashboard's accent instead.
- **Consistency.** One control vocabulary across every fragment: segmented control for Chart/Table, select for site, switch for terrain, button for CSV. Same positions, same labels, same spacing. Calcite components only.

These are specified against the ltinfo-platform and dataviz guidance and are checked in the review frame before any page ships.

## Data

Service: `https://maps.trpa.org/server/rest/services/LTInfo_Monitoring/MapServer` (maxRecordCount 2,000,000, CORS works from outside the office network; JSONP `callback=` is the office fallback).

| Page | Layer or table | Records | Notes |
|---|---|---|---|
| Bike Ped sites | 5 Bike and Pedestrian Counter Location (point) | 56 | `counter_id`, `counter_name` |
| Bike Ped counts | 1 Bike and Pedestrian Counts (table) | 73,245 | daily `count_of_bike_ped`, `counter_name`, `counter_category`, `season_of_year`, `count_date`, 2016-11-01 to 2026-09-15. `counter_id` is null, so the join to layer 5 is on `counter_name`: 54 of 56 counters match. Eight table-only names are spelling variants (trailing space, different dash, "Pneumatic Tubes Floating (2)") and two layer counters have no rows. Fix the names at the source. |
| Noise plan areas | 6 Plan Area Noise Monitoring (point) | 409 | one row per site, year, and category; `CNEL_LAND_USE`, `COUNT_VALUE`, link to the LT Info site page |
| Noise shoreline | 7 Shoreline Noise Monitoring (point) | 133 | `COUNT_VALUE` is a string |
| Noise highways | 11 Transportation Noise Monitoring (point) | 103 | `CNEL_LIMIT`, `CNEL_MAX_VALUE`, `CNEL_AVG_VALUE`, distance from edge |
| Noise plan area averages | 74 Plan Area Noise Average (table) | 55 | by category and five-year period against `Threshold_Value` |
| Noise shoreline exceedances | 78 Shoreline Noise Average Exceedances Per Day (table) | 12 | 2009 onward |
| Yellow cress sites | 9 Tahoe Yellowcress (point) | 2,576 | one row per site and year, 65 sites, 1978 to 2025; `STATUS` is Surveyed or No Survey, `COUNT_VALUE` is assumed to be the plant count |
| Yellow cress summary | 86 Occupied Sites by Year (table) | 40 | `Occupied_Sites`, `Lake_Level`, `Threshold` 26, 1978 to 2023 |

Forest Health numbers keep their current sources (Vegetation_Type, Fire map services, repo CSV outputs) as documented in the skill.

Snapshot rule: every fragment has a `SNAPSHOT` with the same shape as the live query and a capture date, and says on the page which one is showing. In ESA's framework the snapshot becomes a JSON file a TRPA job writes to GitHub Pages, so the Monitoring repo keeps owning the data even after authoring moves.

## Publishing

The repo's Pages source is GitHub Actions. A Pages deployment replaces the whole site, so a second workflow that staged only `ltinfo/` would have wiped Currents; instead `.github/workflows/currents-daily.yml` now stages `ltinfo/kit`, `templates`, `pages`, `CONTRACT.md`, and `PLAN.md` beside `Currents/`, and also runs on a push to `main` that touches `ltinfo/**` (the archive refresh runs too, which is harmless because it merges from the live site). `qa/` is not deployed. The root redirect to Currents stays for now. Nothing deploys until MTB-Edits merges to `main`.

Tools repo (branch mtb-edits): four `apps.json` entries with `"category": "monitoring"` and `"beta": true`, each with an SVG thumbnail in `assets/thumbs/`: `ltinfo-bike-ped`, `ltinfo-noise`, `ltinfo-forest-health`, and `ltinfo-review-frame`. The qa/monitoring gallery renders them; the URLs resolve once the Monitoring deploy above has run.

## Phases

1. Kit and contract. Done Oct. 26, 2026. `kit/` is built from the Subalpine Conifer embed and the two skills, `CONTRACT.md` is written, and `tools/assets/trpa-tokens.css` carries a `data-site` accent in the ltinfo layer. `qa/subalpine-conifer-kit.html` renders the same as the published page at 980 and 375 px, with the Table toggle working on ag-grid-community 36 through the Theming API. Local preview: the `monitoring` server in `.claude/launch.json` serves the repo root on port 8766.
2. Templates. Done Oct. 26, 2026. Four templates plus the review frame, each on real LTInfo_Monitoring data with a snapshot fallback, explicit empty states, and a head comment that says what to replace. Each renders at 980 and 375 px with no console errors and no horizontal scroll. Two rules came out of it: draw every chart with `LTI.plot`, never `Plotly.newPlot`, because Plotly's responsive resize collapses a chart with no definite height when its container changes size after the first draw; and pin `ag-grid-community@36.2.0`.
3. Fragment form. Done Oct. 26, 2026. Kit 1.1.0: CSS scoped under `.lti`, `LTI.define`, `mount`, `mountAll`, `include`, `setParam`, messages gated on being framed. The three fragment templates have no element ids, carry config and snapshot as JSON blocks, and are mounted by `kit/fragments/section-embed.js`, `kit/fragments/sites-timeseries.js`, and `templates/program-dashboard.js`. The topic page renders its two sections inline through `data-lti-include`, zero iframes, and both fragments mount with working Table toggles. Verified: live queries on all four pages, no console errors, no horizontal overflow at 375 px with two fragments on one page, `?site=` deep link in and the URL written back on selection, the review frame still receiving height messages in topic mode (3,482 px), and the Subalpine Conifer fixture rendering inside a `.lti` wrapper with ag-grid pinned.
4. Map and interaction pass. Done Oct. 26, 2026. `kit/ltinfo-map.js` and four Arcade files in `kit/popups/`; the sites fragment and the program dashboard reworked against the list above; the section fragment and the kit palette switched to the per-site colors. Verified in the browser: basemap and hillshade loaded with the four Human Geography layers as base layers and the sites layer above them; popup docked top right; both Arcade pairs evaluated against real features through the SDK's Arcade executor and returned the story text and a column chart dictionary with the green color; picker selection opens the popup, writes `?site=`, and pushes the trend and the past 12 months into the feature so the chart fills; skeletons present before data and gone after; snapshot-first path logged on `?lti-slow=1`; snapshot path renders the map, the coverage chart, and the tiles on `?lti-snapshot=1`; no horizontal overflow at 375 px. Not yet seen by eye: the Claude browser pane was hidden during this pass, so the map canvas and the popup body never drew (they render on animation frames). The first look at the docked popup, the chart in it, and the hillshade belongs to the next session with the pane open, or to Mason.
5. First pages. Done Oct. 26, 2026, short of the deploy. `pages/bike-ped/` on the sites fragment, `pages/noise/` on the topic template with the plan area and shoreline sections inline, `pages/forest-health/` as a hub of the three programs. The Pages workflow stages `ltinfo/` and four cards are in the tools `apps.json`. `trpa-dashboard-qa` run and passed with four warnings (`qa/qa-report-2026-10-26.md`). Not yet done: the deploy itself, which waits on MTB-Edits merging to `main`, and a look at the maps with the browser pane open.
6. Design system section. Done Oct. 26, 2026. Section 19, "LT Info Monitoring", in `tools/qa/data/design-system.html`, after Lake Tahoe Info embeds, with a nav entry and pointers from the embeds, map conventions, and skills sections. It covers where the files live, the per-site palettes and color roles, a specimen styled by the real kit stylesheet with a `data-site` switch, the map and popup pattern with a docked popup mock and its Arcade, fragment anatomy, a live shoreline section, the templates and pages with links, do and don't rules, and the checklist. The kit stylesheet loads into the page because it is scoped under `.lti`; the fragment scripts cannot, because the page runs Calcite 5 and the kit needs Calcite 2.13 for ArcGIS Maps SDK 4.31, so the live fragment sits in a frame there. Everything loads from `https://trpa-agency.github.io/Monitoring/ltinfo/`, so the specimens show unstyled with a note until MTB-Edits deploys; `?ltinfo=http://localhost:8766/ltinfo/` points the section at the local preview. Building it surfaced a kit bug: the height reporter read `documentElement.scrollHeight`, which inside a frame never reports less than the frame, so a frame could grow but never shrink. It now reports the body's content height; CONTRACT.md says so.
7. Skill. Done Oct. 26, 2026, trpa-claude-skills `4087559` and `dad24f5` on main. `trpa-monitoring-dashboard` now carries the fragment form, the templates and the review frame in `assets/templates/` (kit linked by absolute URL), the Arcade files in `assets/popups/`, `references/sections.md`, the map and popup rules, the per-site palettes, the data table above, new stack gotchas, pointers to the kit and design system section 19, and the contract's checklist. `ltinfo-platform`'s height snippet now posts the body's height. Validated with `claude plugin validate .`; the handoff check passes. The skill's copies are not canonical: change `ltinfo/templates/` here first, verify, then recopy (trpa-claude-skills DECISIONS 2026-10-26).
8. Handoff and port. Send the prototype URLs and `CONTRACT.md` to Jeanne for ESA scoping. With ESA, settle how fragments render in their framework and where config lives, then port: tokens become a diff against their variables, the kit CSS and JS move into their build, each fragment becomes a partial view or section type, and the snapshot job starts writing JSON to Pages. Until the port lands, ask the LT Info admin to point program 83 and the Bike Ped and Noise programs at the prototype URLs in the Dashboard card.

## Open items

- Confirm with ESA how a fragment renders in ProjectFirma (partial view, CMS section type, or static include), where config JSON lives, and how a script runs after render. This sets the final shape of phase 8 and may adjust phase 3.
- Whether the `arcgis/human-geography` basemap style can load without a key from the fragment. If not, decide between a TRPA Portal item and an ArcGIS location platform key stored outside the repo.
- The three Forest Health embeds load `ag-grid-community` from jsDelivr with no version (36.2.0 today). Pin it there, and pin it in every template.
- Whether the three Forest Health embeds move from the ForestHealth repo into `pages/forest-health/` or stay put and only load the kit. Moving them changes URLs the LT Info admin may already hold.
- Bike Ped counter names: eight table-only spelling variants and two counters with no rows (see Data). Fix at the source before the Bike Ped page ships.
- `COUNT_VALUE` on layer 9 is assumed to be a plant count. Confirm with the program lead before the plant counts section fills in.
- Whether ESA's layout will load the pinned library versions the kit expects, or whether the kit must tolerate theirs. Calcite and the ArcGIS SDK versions must match each other. The design system page already shows the problem: it runs Calcite 5, so the kit's fragments cannot mount inline there.
- The design system page lays out at 819 px on a phone because of its Brands by category table, not the LT Info Monitoring section. Worth a scroll wrapper on that table in the tools repo.
