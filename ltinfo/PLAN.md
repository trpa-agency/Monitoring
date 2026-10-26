# LT Info Monitoring pages: plan

Prototype page templates for monitoring.laketahoeinfo.org, a shared embed kit, a Monitoring section on the TRPA design system, and an update to the trpa-monitoring-dashboard skill. Work happens in this folder of the Monitoring repo and publishes at https://trpa-agency.github.io/Monitoring/ltinfo/. Written Oct. 26, 2026.

## Decisions so far

- ESA is scoping this as a collaborative integration with their GitHub, not only an iframe embed. Until that lands, every page must still work in today's Dashboard card.
- First pages: the three Forest Health programs (Subalpine Conifer, Forest Structure, Fire Dynamics), Bicycle and Pedestrian, and Noise.
- Audience is all three at once: ESA scoping material, Jeanne and leadership review, and working templates for staff.
- Code lives here. The tools repo only lists the pages as beta cards on qa/monitoring through apps.json.
- Data comes from the LTInfo_Monitoring map service on maps.trpa.org, with a snapshot fallback in each page.
- Extend trpa-monitoring-dashboard rather than add a new skill.
- The design system page stays at tools/qa/data/design-system.html.

## Folder layout

```
ltinfo/
  PLAN.md                 this file
  CONTRACT.md             the embed and page contract to hand ESA (height message, origins, slot sizes, style kit)
  kit/
    ltinfo-embed.css      host tokens, Lexend Deca, card and label bar, stat tile, table toggle, empty state (built Oct. 26)
    ltinfo-embed.js       height postMessage, Chart/Table toggle, CSV download, wheel-zoom guard, query helper with snapshot fallback (built Oct. 26)
  qa/
    subalpine-conifer-kit.html   fixture: the published Subalpine Conifer embed rebuilt on the kit, for checking the kit. Not deployed.
  templates/                (built Oct. 26; each file's head comment says what to replace)
    program-dashboard.html    Dashboard card, 980 px, the Forest Health section pattern. Default data: Tahoe Yellow Cress (layer 9, table 86)
    section-embed.html        one chart with title, Chart/Table toggle, CSV, caption, source. Default: plan area noise by land use (table 74)
    sites-timeseries.html     site map, site picker, monthly series, table. Default: bike ped counters (layer 5, table 1). Deep link ?site=
    topic-page.html           narrative blocks plus embedded pages sized from their height messages. Default: a short noise page
    _frame.html               mock LT Info shell for review: ?mode=dashboard|section|topic&page=... Sizes the frame from height messages
  pages/
    bike-ped/                 sites-timeseries filled: layer 5 counters, table 1 daily counts
    noise/                    topic page spanning Plan Area (61), Shorezone (62), and Highways (65), with section embeds
    forest-health/            hub page linking the three ForestHealth embeds, migrated to the kit
  assets/                   thumbnails and any static images
```

Each published page is a single HTML file that links the kit by absolute URL. Nothing is bundled.

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

| Yellow cress sites | 9 Tahoe Yellowcress (point) | 2,576 | one row per site and year, 65 sites, 1978 to 2025; `STATUS` is Surveyed or No Survey, `COUNT_VALUE` is the plant count |
| Yellow cress summary | 86 Occupied Sites by Year (table) | 40 | `Occupied_Sites`, `Lake_Level`, `Threshold` 26, 1978 to 2023 |

Forest Health numbers keep their current sources (Vegetation_Type, Fire map services, repo CSV outputs) as documented in the skill.

Snapshot rule: every page embeds a `SNAPSHOT` with the same shape as the live query and a capture date, and says on the page which one is showing.

## Publishing

The repo's Pages source is GitHub Actions, and the only workflow (`.github/workflows/currents-daily.yml`) stages `Currents/` alone. Add a second job or a separate `ltinfo-pages.yml` that copies `ltinfo/` into the site on push to `main` when files under `ltinfo/` change, and keep the Currents daily run untouched. The root redirect to Currents stays for now.

Tools repo: one `apps.json` entry per page with `"category": "monitoring"`, `"beta": true`, an `assets/thumbs/` image, and the github.io URL. The qa/monitoring gallery renders them.

## Phases

1. Kit and contract. Done Oct. 26, 2026. `kit/` is built from the Subalpine Conifer embed and the two skills, `CONTRACT.md` is written, and `tools/assets/trpa-tokens.css` carries a `data-site` accent in the ltinfo layer. `qa/subalpine-conifer-kit.html` renders the same as the published page at 980 and 375 px, with the Table toggle working on ag-grid-community 36 through the Theming API. Local preview: the `monitoring` server in `.claude/launch.json` serves the repo root on port 8766.
2. Templates. Done Oct. 26, 2026. Four templates plus the review frame, each on real LTInfo_Monitoring data with a snapshot fallback, explicit empty states, and a head comment that says what to replace. Each renders at 980 and 375 px with no console errors and no horizontal scroll; the frame receives height messages in all three modes, and the topic page sizes its embedded pages from theirs. Two rules came out of it: draw every chart with `LTI.plot`, never `Plotly.newPlot`, because Plotly's responsive resize collapses a chart with no definite height when the iframe around it changes size; and pin `ag-grid-community@36.2.0`.
3. First pages. Bike Ped on the sites template, Noise on the topic template with two section embeds, Forest Health hub. Done when each passes `trpa-dashboard-qa`, the Pages workflow publishes it, and it is listed in apps.json.
4. Design system section. "LT Info Monitoring" as a sibling of the existing Lake Tahoe Info embeds section: monitoring accent and card anatomy, the four templates as live specimens with links, the section pattern, copy rules, and the pre-CMS checklist.
5. Skill. Extend trpa-monitoring-dashboard with `assets/` holding the templates, `references/sections.md`, pointers to the kit and the design system section, and the data table above. Validate, update the handoff docs, bump the marketplace.
6. Handoff. Send the review frame URLs and `CONTRACT.md` to Jeanne for ESA scoping. Ask the LT Info admin to point program 83 and the Bike Ped and Noise programs at the new URLs once approved.

## Open items

- The three Forest Health embeds load `ag-grid-community` from jsDelivr with no version (36.2.0 today). Pin it there, and pin it in every template.
- Verify the height message against a listening parent once `templates/_frame.html` exists.

- Whether the three Forest Health embeds move from the ForestHealth repo into `pages/forest-health/` or stay put and only load the kit. Moving them changes URLs the LT Info admin may already hold.
- Bike Ped counter names: eight table-only spelling variants and two counters with no rows (see Data). Fix at the source before the Bike Ped page ships.
- `COUNT_VALUE` on layer 9 is assumed to be a plant count. Confirm with the program lead before the plant counts section fills in.
- Whether ESA will accept the height postMessage contract as written or wants their own script. `CONTRACT.md` is written so either works.
