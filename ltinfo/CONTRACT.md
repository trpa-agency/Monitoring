# LT Info embed and page contract

How TRPA-built pages and Lake Tahoe Info fit together. TRPA builds and hosts the content; LT Info supplies the frame. This is the contract the pages in this folder follow today, written so ESA can adopt it as is, replace parts of it, or build the collaborative integration on top of it. Version 1.0.0, Oct. 26, 2026.

## Three kinds of content

| Kind | Where it appears on LT Info | What TRPA supplies | Size it lives in |
|---|---|---|---|
| Program dashboard | The Dashboard card on a Monitoring Program page (program type Program With Dashboard) | One complete dashboard per program in one HTML file: at a glance, map, charts with tables, measurement notes, sources | The card body, about 980 px wide at desktop, on `#f7f7f7`. The card supplies the header and a Fullscreen control. |
| Section embed | An embed section on any program page, beside the program's own LT Info data (requested framework change 1) | One chart, map, or table per file with its own title line, caption, and source | Full card width, white or `#f7f7f7` body, height set by the content |
| Staff topic page | A page at a vanity URL under monitoring.laketahoeinfo.org, inside the LT Info header, navigation, and footer (requested framework change 2) | Narrative blocks written in the CMS plus one or more section embeds | Main column, about 1,000 px |

A threshold element (one chart or map on thresholds.laketahoeinfo.org) and a climate chart follow the same rules at their own slot sizes. See the ltinfo-platform skill.

## What every embedded file does

1. **No chrome.** No logo, page title, navigation, or footer. The host provides them. Program name, category, partners, documents, and indicators stay in the CMS and are not repeated.
2. **Matches the host.** Lexend Deca on every text element, including Plotly layouts and AG Grid. Background matches the slot (`#f7f7f7` in a Dashboard card, white elsewhere). Section labels, tile accents, and controls use the host dashboard's card header color (Monitoring `#789440`, Thresholds `#6c91a4`, Transportation `#2c5282`). No TRPA, EIP, or Tahoe Living brand colors.
3. **Fluid width.** Designed for the slot and checked at 375 px. No horizontal scroll at any width.
4. **Owns its height.** Posts its document height to the parent (message below) on load, on resize, and whenever the content changes size. Works at whatever fixed height the host sets if the host does not listen.
5. **Behaves in an iframe.** Maps stop wheel events from propagating so page scrolling does not zoom the map. Legends stay inside the frame (inline on wide screens, a drawer on phones). Nothing opens a popover that can escape the frame.
6. **Every chart has a table and a CSV.** A Chart/Table toggle on each chart card and a Download CSV button on the table. Every map has the same facts in text or a table nearby.
7. **Data is live with a fallback.** Numbers are queried at load from a TRPA service and fall back to a snapshot embedded in the file with the same shape and a capture date. The page says which one is showing. No chart is ever drawn from placeholder numbers; a section without data shows an explicit empty state saying what will appear there.
8. **Sources and vintage.** A source line under every card and a data vintage line in the footer.
9. **Accessible.** WCAG 2.1 AA: text contrast, keyboard-reachable controls, labelled regions, alt text or a text equivalent for every map and chart.
10. **Libraries from CDNs, pinned.** Plotly, Calcite, the ArcGIS Maps SDK, and AG Grid load from their CDNs at fixed versions. No multi-megabyte inlined chart exports.
11. **Writes nothing to the host.** No cookies, storage, or requests to LT Info beyond the anonymous read API.

## Messages between the page and the host

All messages are `window.postMessage` with a plain object. The page sends to `window.parent`; the host sends to the iframe's `contentWindow`. Until an origin allowlist exists both sides accept any origin and check `type` only.

| Direction | Message | When | Fields |
|---|---|---|---|
| page to host | `{ type: 'ltinfo-embed:height', height: 1234, source: 'subalpine-conifer' }` | load, resize, any content size change, after a Chart/Table switch | `height` is `document.documentElement.scrollHeight` in CSS pixels; `source` is the page's `data-embed` name |
| host to page | `{ type: 'ltinfo-embed:params', params: { site: '651' } }` | after the iframe loads, and again when the user picks a site or year on the host | any string keys; the page merges them with its URL query parameters |
| page to host | `{ type: 'ltinfo-embed:params', params: { site: '651' } }` | when the user picks something in the page the host might reflect in its URL | same shape |

The host should set the iframe `height` from the latest height message, keep a sensible default (the Dashboard card renders about 765 px today) for pages that never send one, and set `title` on every iframe. A page can also receive deep links as URL query parameters (`?site=651`) with no host support at all.

## Hosting and origins

TRPA pages are served from GitHub Pages at `https://trpa-agency.github.io/<repo>/...` today, with `data.trpa.gov` and `tools.trpa.gov` to follow. The requested allowlist of embeddable origins is `trpa-agency.github.io`, `data.trpa.gov`, `tools.trpa.gov`, `maps.trpa.org`, and `tahoeopendata.org`. Each page's URL is stable once the LT Info admin holds it; a page that moves leaves a redirect behind.

If an embedded page fails to load, LT Info should show the section's description and a direct link to the page instead of a blank frame.

## The style kit

`ltinfo/kit/ltinfo-embed.css` and `ltinfo/kit/ltinfo-embed.js` in the Monitoring repo carry the host tokens, the card and label-bar styles, the stat tiles, the empty state, the AG Grid look, the Plotly defaults, the height and params messages, and the query-with-snapshot helper. Pages draw charts through the kit's `LTI.plot` so each chart keeps a definite height when the host resizes the iframe; Plotly's own responsive resize otherwise collapses a chart inside a frame whose size changes after the first draw. Values were sampled from the live site on Oct. 13, 2026 and will drift as ESA updates the frame. A versioned kit published by ESA (requested framework change 6) replaces these files; until then TRPA keeps them current and versions them by path (`kit/` is 1.x, breaking changes go to `kit/v2/`).

## Data

Most numbers come from the LTInfo_Monitoring map service on `maps.trpa.org` (anonymous, CORS enabled, `maxRecordCount` 2,000,000) and from program-specific services and repo outputs. Pages do not call the LT Info internal API from the browser; where LT Info data is needed, a server-side job snapshots it to GitHub Pages first. A documented public read API (requested framework change 7) would let pages draw from LT Info directly.

## Checklist before the LT Info admin gets a URL

1. Renders at the slot width and at 375 px with no horizontal scroll, on the right background.
2. Lexend Deca everywhere, including Plotly and AG Grid.
3. No console errors on load.
4. Live query succeeds, and the snapshot renders when the network is blocked.
5. Every chart has a working Table toggle and CSV download. Sources and vintage are stated.
6. Height message observed in a test frame.
7. Committed, pushed, and the GitHub Pages URL returns the file.
8. `trpa-dashboard-qa` run and passed.
