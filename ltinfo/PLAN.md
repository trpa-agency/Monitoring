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
    ltinfo-embed.css      host tokens, Lexend Deca, card and label bar, stat tile, table toggle, empty state (built Oct. 26)
    ltinfo-embed.js       query helper with snapshot fallback, Chart/Table toggle, CSV, LTI.plot, wheel-zoom guard, height and params messages (built Oct. 26)
    ltinfo-map.js         LTI.map: human geography basemap, terrain switch, popup loader, selection wiring (phase 4)
    popups/               one Arcade file per layer, fetched at mount and set as the popup's expression (phase 4)
      bike-ped-counter.arcade
      noise-site.arcade
      yellow-cress-site.arcade
  qa/
    subalpine-conifer-kit.html   fixture: the published Subalpine Conifer embed rebuilt on the kit. Not deployed.
  templates/                (built Oct. 26 as whole pages; phase 3 splits each into fragment plus harness)
    program-dashboard.html    the Forest Health section pattern. Default data: Tahoe Yellow Cress (layer 9, table 86)
    section-embed.html        one chart with title, Chart/Table toggle, CSV, caption, source. Default: plan area noise by land use (table 74)
    sites-timeseries.html     site map, site picker, monthly series, table. Default: bike ped counters (layer 5, table 1). Deep link ?site=
    topic-page.html           narrative blocks plus sections. Default: a short noise page. Today the sections are iframes; phase 3 makes them inline fragments
    _frame.html               mock LT Info shell for review: ?mode=dashboard|section|topic&page=...
  pages/
    bike-ped/                 sites-timeseries filled: layer 5 counters, table 1 daily counts
    noise/                    topic page spanning Plan Area (61), Shorezone (62), and Highways (65), with section fragments
    forest-health/            hub page linking the three ForestHealth embeds, migrated to the kit
  assets/                   thumbnails and any static images
```

Each published prototype is a single HTML file that links the kit by absolute URL. Nothing is bundled.

## Fragment form (the shape that ports)

- A fragment is one `<section class="lti" data-site="monitoring" data-kind="section-embed">` holding its markup. `data-site` and `data-slot` move from `<html>` to this wrapper.
- Kit CSS scopes everything under `.lti`. Tokens are declared on `.lti` as well as `:root`, so the fragment carries its own values when ESA's cascade lacks them and defers to ESA's when it has them.
- Kit JS exposes `LTI.mount(rootEl, config)`. The mount reads `config` and an optional `snapshot`, queries, draws, and wires controls. No element ids: every lookup is `rootEl.querySelector`. Two fragments on one page do not collide.
- The harness page is the current standalone file: `<html>`, `<head>` with the pinned CDN tags and the kit links, one fragment, and a script that calls `LTI.mount` with the file's `CONFIG` and `SNAPSHOT`. Markers (`<!-- fragment:start -->` and `<!-- fragment:end -->`) delimit what moves.
- Height and params messages stay in the kit but only run when `window.parent !== window`. Inline, they are inert.
- Libraries are the harness's job. A fragment never loads a script tag of its own.

## Map and interaction pass (what the prototypes still need)

The templates prove the data and the slot. They do not yet feel like one product. The goal is a seamless reading experience: a visitor lands on a page and understands the story without hunting. Requirements for every fragment with a map or a selection:

- **Basemap.** Esri Human Geography (light) as the default, muted so the data reads first. A terrain switch in the map corner turns a hillshade layer on and off, off by default, and remembers nothing. The `arcgis/human-geography` basemap style needs an ArcGIS location platform key or the TRPA Portal; if neither is available anonymously, fall back to the Human Geography vector tile layer item from ArcGIS Online. Confirm in phase 4.
- **Popups that tell a story.** Each layer's popup is an Arcade expression in its own file under `kit/popups/`, fetched at mount and set as the popup template's expression. The popup leads with the sentence a reader wants (where this site stands, how it compares, what changed), then the facts, then a link. No field list dumps. The same Arcade file works in the fragment, in ArcGIS Pro, and in a Portal web map, which is the reason to keep it separate from the page.
- **Fewer clicks.** One click on a map point selects the site and the chart, tiles, and table update at once; the popup is the story, not the gate to the chart. Hover highlights the point and the matching legend entry. The picker, the map, and the URL stay in sync, and a `?site=` link lands on the right view with the map centered. No "view details" buttons, no second step.
- **Visual hierarchy.** Tiles answer the headline question in one glance, each with a direction word or sparkline where the data supports it. Charts show the standard or target as a labeled line, not a legend entry. Default chart views are the comparison that matters (this site against all sites, this year against the period), not the raw series.
- **Loading and empty.** Skeleton blocks in place of each tile and chart while queries run, never spinners alone, and the snapshot shows within a second if the query is slow. Empty states say what will appear and when.
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

The repo's Pages source is GitHub Actions, and the only workflow (`.github/workflows/currents-daily.yml`) stages `Currents/` alone. Add a second job or a separate `ltinfo-pages.yml` that copies `ltinfo/` into the site on push to `main` when files under `ltinfo/` change, and keep the Currents daily run untouched. The root redirect to Currents stays for now.

Tools repo: one `apps.json` entry per prototype with `"category": "monitoring"`, `"beta": true`, an `assets/thumbs/` image, and the github.io URL. The qa/monitoring gallery renders them.

## Phases

1. Kit and contract. Done Oct. 26, 2026. `kit/` is built from the Subalpine Conifer embed and the two skills, `CONTRACT.md` is written, and `tools/assets/trpa-tokens.css` carries a `data-site` accent in the ltinfo layer. `qa/subalpine-conifer-kit.html` renders the same as the published page at 980 and 375 px, with the Table toggle working on ag-grid-community 36 through the Theming API. Local preview: the `monitoring` server in `.claude/launch.json` serves the repo root on port 8766.
2. Templates. Done Oct. 26, 2026. Four templates plus the review frame, each on real LTInfo_Monitoring data with a snapshot fallback, explicit empty states, and a head comment that says what to replace. Each renders at 980 and 375 px with no console errors and no horizontal scroll. Two rules came out of it: draw every chart with `LTI.plot`, never `Plotly.newPlot`, because Plotly's responsive resize collapses a chart with no definite height when its container changes size after the first draw; and pin `ag-grid-community@36.2.0`.
3. Fragment form. Restructure the kit and the four templates as described under Fragment form: scoped CSS, `LTI.mount`, no ids, harness markers, messages gated on being framed. The topic page renders its sections inline from the same fragment files instead of iframes, which also proves two fragments on one page. The review frame keeps working for the interim iframe case. Done when every template renders the same as today in its harness, inline in the topic page, and in the frame.
4. Map and interaction pass. Build `kit/ltinfo-map.js` and `kit/popups/`, then rework the sites fragment and the program dashboard against the Map and interaction pass list: human geography basemap with terrain switch, Arcade story popups, one-click selection with hover, skeleton loading, comparison defaults, one control vocabulary. Done when the sites fragment on bike ped data reads as a finished product in the review frame at 980 and 375 px, and a second person can find a site's story in one click.
5. First pages. Bike Ped on the sites fragment, Noise on the topic template with two section fragments, Forest Health hub. Done when each passes `trpa-dashboard-qa`, the Pages workflow publishes it, and it is listed in apps.json.
6. Design system section. "LT Info Monitoring" as a sibling of the existing Lake Tahoe Info embeds section: monitoring accent and card anatomy, the four fragments as live specimens with links, the section pattern, the map and popup pattern, copy rules, and the pre-CMS checklist.
7. Skill. Extend trpa-monitoring-dashboard with `assets/` holding the templates and Arcade files, `references/sections.md`, the fragment form, the map and interaction rules, pointers to the kit and the design system section, and the data table above. Validate, update the handoff docs, bump the marketplace.
8. Handoff and port. Send the prototype URLs and `CONTRACT.md` to Jeanne for ESA scoping. With ESA, settle how fragments render in their framework and where config lives, then port: tokens become a diff against their variables, the kit CSS and JS move into their build, each fragment becomes a partial view or section type, and the snapshot job starts writing JSON to Pages. Until the port lands, ask the LT Info admin to point program 83 and the Bike Ped and Noise programs at the prototype URLs in the Dashboard card.

## Open items

- Confirm with ESA how a fragment renders in ProjectFirma (partial view, CMS section type, or static include), where config JSON lives, and how a script runs after render. This sets the final shape of phase 8 and may adjust phase 3.
- Whether the `arcgis/human-geography` basemap style can load without a key from the fragment. If not, decide between a TRPA Portal item and an ArcGIS location platform key stored outside the repo.
- The three Forest Health embeds load `ag-grid-community` from jsDelivr with no version (36.2.0 today). Pin it there, and pin it in every template.
- Whether the three Forest Health embeds move from the ForestHealth repo into `pages/forest-health/` or stay put and only load the kit. Moving them changes URLs the LT Info admin may already hold.
- Bike Ped counter names: eight table-only spelling variants and two counters with no rows (see Data). Fix at the source before the Bike Ped page ships.
- `COUNT_VALUE` on layer 9 is assumed to be a plant count. Confirm with the program lead before the plant counts section fills in.
- Whether ESA's layout will load the pinned library versions the kit expects, or whether the kit must tolerate theirs. Calcite and the ArcGIS SDK versions must match each other.
