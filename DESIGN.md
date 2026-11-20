# DESIGN.md — AirSQL

Durable design decisions for the AirSQL brand and the documentation site. Read this
before restyling anything; write here when a decision should outlive the task.

## The idea

AirSQL's API is a decorator: `@sql.query`, `@sql.merge`, `@sql.check`. That is the
whole identity, so the mark is the at-sign itself — not a metaphor for data, not a
pinwheel, not a table icon. Every pictorial alternative tried read as something else
at 16px (arrows read as "fast-forward", stacked rows as a burger menu, a semicolon as
a speck). The at-sign survives the favicon, means "decorator" to a Python engineer, and
takes an accent without needing a second shape.

The claw of the at-sign carries `ember`: the stroke that leaves the ring is the data
leaving the decorator. Ring and inner `a` are ink; only that one stroke is coloured.

## Marks

| file | use |
| --- | --- |
| `docs/assets/brand/airsql-mark.svg` | primary mark, ink + ember, on light surfaces |
| `docs/assets/brand/airsql-mark-inverse.svg` | same mark in white, for dark surfaces |
| `docs/assets/brand/airsql-mark-mono.svg` | one-colour, for print or single-tint contexts |
| `docs/assets/brand/airsql-mark-accent.svg` | ember ring and bowl, white stem — for ember grounds only |
| `docs/assets/brand/airsql-tile.svg` | square tile, ember ground, white one-colour mark — favicon, app icon |
| `docs/assets/brand/airsql-lockup.svg` | mark beside the wordmark, outlined text, for tight spaces |
| `docs/assets/brand/airsql-lockup-stacked.svg` | mark **above** the wordmark — README, social cards |
| `docs/assets/brand/airsql-lockup-inverse.svg` | the same lockup in white |

Geometry, 24×24 grid, centre `(12, 12)`:

- ring `r 7.9`, drawn from 58° clockwise to 20° — a 322° arc, so the opening sits at the
  lower right;
- inner bowl `r 2.5` at `(11.3, 11.9)`, stroke 2.25;
- stem at `x 13.8`, from the bowl's top down to `y 13.15`, then a tail that leaves
  through the opening and ends just outside the ring at `r 10.0, 52°`;
- stroke 2.6 for ring and tail, round caps and joins.

Change it only against a 16px render: every earlier attempt (separate claw, squared
ring, spiral, tail in a second colour) failed there or read as a different symbol.

The locker is drawn, never set in a font: the wordmark's `Air` is Inter Medium and
`SQL` is JetBrains Mono Bold, both converted to outlines so GitHub renders them the
same as the docs.

## Colour

| token | value | role |
| --- | --- | --- |
| ink | `#0F1418` | marks, body text, dark bands |
| ember | `#E8582A` | one accent only: the claw, the decorator in code, links on hover, arrows |
| paper | `#FFFFFF` | page ground |
| wash | `--md-code-bg-color` | quiet panels (the counts strip) |

Ember is deliberate: the data-tooling neighbourhood is all blue (Airflow, Postgres,
BigQuery), so a single warm accent is the cheapest way to be recognised. It is spent on
one thing per surface — never as a background wash, never on two elements at once.

The documentation theme carries the same palette. The paper scheme has a paper header
and footer with ink type; the slate scheme has an ink header, footer and ground — the
brand ink itself, not Material's blue-grey. `--md-typeset-a-color` and
`--md-accent-fg-color` are ember in both (darkened to `#C64217` on paper, `#F0703F` on
the dark ground). Headings are set at full ink strength — the
theme's default greys them to the "light" tone, which reads as placeholder text.

Two cascade notes, both learned the hard way: the theme declares
`--md-typeset-a-color` under `[data-md-color-scheme=slate][data-md-color-primary=grey]`,
so a single-attribute selector loses and the brand block repeats
`body[data-md-color-scheme=…][data-md-color-primary]`; and the palette paints
`[data-md-color-primary=black] .md-header` with a hardcoded ground, ignoring the
primary variable entirely, so the header, the tabs bar and the drawer title get an
explicit `background-color` per scheme.

Everything else derives from the docs theme's own variables (`--md-default-fg-color`,
`--md-default-fg-color--light`, `--md-default-fg-color--lightest`) so light and dark
palettes follow without a second set of rules. Dark bands (`#0B1116`) are part of the
composition in both schemes, not a dark-mode variant.

## Type

- Text: **Inter** (400–700), self-hosted at `docs/assets/fonts/inter-latin.woff2`, OFL.
- Code and mono labels: **JetBrains Mono** (400–700), self-hosted, OFL.
- Display sizes are tight: `letter-spacing: -0.035em` on the hero, `-0.025em` on
  section headings. Labels are mono, uppercase, `0.09em`–`0.14em` tracking.
- Never a webfont from a CDN; the two files above are the whole budget (~79 KB).

## The home page

`overrides/partials/landing.html` holds it; `overrides/main.html` swaps it in when
`page.url == ""`. It is the only page that leaves the documentation column: `:has(.lp)`
widens the container, hides the sidebar, the footer nav and the theme's 8px article
spacer.

Rules that came from building it:

- The hero and the closing band follow the surface: ink in the dark scheme, paper in
  the light one, with the mark swapped to match (there is one mark per surface, never a
  white mark on paper). Everything between is paper and hairlines — no cards, no
  shadows, no gradient. The code frames stay ink in both schemes: they are pictures of
  a terminal, not page furniture.
- The mark leads: it sits above the claim in the hero, and above the wordmark in the
  stacked lockup. It is the only brand element allowed above the fold.
- Code is the picture. No screenshots, no illustrations, no fake UI.
- Every claim on the page is checkable in the docs: 12 write operations, 4 transfer
  routes, 2 sensors, MIT, Python ≥ 3.12, Airflow 3.
- The theme fights `pre` and `code` typography from a stylesheet that outranks a linked
  one, so those declarations live in a scoped `<style>` block inside the partial. Same
  for the copy button and the article spacer.

## Social card

`tools/og/og.html` is a 1200×630 page rendered to `docs/assets/brand/og-image.png` by
`tools/og/render.sh` (headless Chrome, scale 1, idempotent). It is built from the same
assets as the site — the inverse mark, Inter and JetBrains Mono from
`docs/assets/fonts`, the ink ground and the ember accent — so the card cannot drift from
the brand. Composition: lockup, the hero claim at display size, the one-line product
statement, then a hairline footer carrying the factual strip (`v0.18.1`, `MIT`,
`Airflow 3`, `Python ≥ 3.12`) and `@sql.merge(…)` as the API signature.

`overrides/partials/og.html` emits the metadata from the page itself: `website` on the
home page, `article` elsewhere, title and description falling back to the site's when a
page has none, absolute image URL built from `site_url`. Re-render the card whenever the
claim, the version or the mark changes — a stale card is worse than none.

## Do not

- Do not add a second accent colour, a gradient, a glow, or an icon inside a tinted
  square.
- Do not use the theme's default `lucide` tag icons as brand elements.
- Do not set the wordmark in live text where a reader could get a different font — the
  lockups are outlined.
- Do not add a logo variant without testing it at 16px on both schemes.
