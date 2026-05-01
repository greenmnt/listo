import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useSearchParams } from "react-router-dom";
import { api } from "../lib/client";
import KindBadge from "../components/KindBadge";
import EmptyState from "../components/EmptyState";
import { KIND_FILTER_OPTIONS } from "../lib/kinds";
import { audCompact, cx, num, pct } from "../lib/format";

export default function ProjectsPage() {
  const [params, setParams] = useSearchParams();
  const [search, setSearch] = useState(params.get("q") ?? "");

  const kind = params.get("kind") ?? "all";
  const suburb = params.get("suburb") ?? undefined;
  const q = params.get("q") ?? undefined;

  const status = useQuery({ queryKey: ["status"], queryFn: () => api.getStatus({}) });
  const apps = useQuery({
    queryKey: ["apps", { kind, suburb, q, limit: 200, analyzedOnly: true }],
    queryFn: () =>
      api.listApplications({
        kind: kind === "all" ? undefined : kind,
        suburb,
        q,
        limit: 200,
        analyzedOnly: true,
      }),
  });
  const suburbs = useQuery({
    queryKey: ["suburb-stats", { kind: "all" }],
    queryFn: () => api.suburbStats({ limit: 12 }),
  });


  function setKind(k: string) {
    const p = new URLSearchParams(params);
    if (k === "all") p.delete("kind");
    else p.set("kind", k);
    setParams(p, { replace: true });
  }

  function submitSearch(e: React.FormEvent) {
    e.preventDefault();
    const p = new URLSearchParams(params);
    if (search.trim()) p.set("q", search.trim());
    else p.delete("q");
    setParams(p, { replace: true });
  }

  function clearSuburb() {
    const p = new URLSearchParams(params);
    p.delete("suburb");
    setParams(p, { replace: true });
  }

  const tiles = [
    {
      emoji: "📊",
      label: "Total applications",
      value: status.data ? num(status.data.appsTotal) : "—",
      hint: status.data
        ? `${num(status.data.appsWithDocs)} with documents`
        : undefined,
    },
    {
      emoji: "🏘️",
      label: "Tracked suburbs",
      value: suburbs.data ? num(suburbs.data.items.length) : "—",
      hint: "with at least one duplex / flat / big-dev",
    },
    {
      emoji: "💰",
      label: "Variable rate",
      value:
        status.data?.currentVarRatePct != null
          ? pct(status.data.currentVarRatePct)
          : "—",
      hint: "RBA F5 owner-occupier discounted",
    },
    {
      emoji: "📁",
      label: "Documents archived",
      value: status.data ? num(status.data.docsDownloaded) : "—",
      hint: status.data
        ? `${audCompact(Number(status.data.docsTotalBytes) / 1)} bytes on disk`.replace(
            "$",
            "",
          )
        : undefined,
    },
  ];

  return (
    <div className="space-y-6">
      {/* Hero tiles */}
      <section className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4">
        {tiles.map((t) => (
          <div key={t.label} className="panel p-5">
            <div className="text-muted text-xs flex items-center gap-1.5">
              <span aria-hidden>{t.emoji}</span>
              <span>{t.label}</span>
            </div>
            <div className="font-mono num text-2xl mt-2">{t.value}</div>
            {t.hint && <div className="text-muted text-xs mt-1">{t.hint}</div>}
          </div>
        ))}
      </section>

      <div>
        {/* Filterable applications table */}
        <section className="space-y-4">
          <div className="panel p-4 flex flex-wrap items-center gap-3">
            <div className="flex items-center gap-1 rounded-lg bg-panel-2 p-1 border border-border/60">
              {KIND_FILTER_OPTIONS.map((opt) => (
                <button
                  key={opt.value}
                  onClick={() => setKind(opt.value)}
                  className={cx(
                    "px-3 py-1 rounded-md text-sm transition-colors flex items-center gap-1.5",
                    kind === opt.value
                      ? "bg-white/[0.08] text-text"
                      : "text-muted hover:text-text",
                  )}
                >
                  <span aria-hidden>{opt.emoji}</span>
                  <span>{opt.label}</span>
                </button>
              ))}
            </div>

            {suburb && (
              <button
                onClick={clearSuburb}
                className="pill border-accent/40 text-accent bg-accent/10"
                title="Click to clear"
              >
                🏘️ {suburb} ✕
              </button>
            )}

            <form
              onSubmit={submitSearch}
              className="ml-auto flex items-center gap-2"
            >
              <input
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search description / address / app id…"
                className="bg-panel-2 border border-border/60 rounded-lg px-3 py-1.5 text-sm w-72 focus:outline-none focus:ring-1 focus:ring-accent"
              />
              <button
                type="submit"
                className="rounded-lg bg-accent/20 border border-accent/40 text-accent text-sm px-3 py-1.5 hover:bg-accent/30"
              >
                Search
              </button>
            </form>
          </div>

          <div className="panel overflow-hidden">
            {apps.isLoading && (
              <div className="px-5 py-3 text-muted text-sm">Loading…</div>
            )}
            {apps.error && (
              <EmptyState
                emoji="💥"
                title="Couldn't reach the API"
                hint={String(apps.error)}
              />
            )}
            {apps.data && apps.data.items.length === 0 && (
              <EmptyState
                emoji="🪹"
                title="No matches"
                hint="Try widening the kind filter or clearing the search box."
              />
            )}
            {apps.data && apps.data.items.length > 0 && (
              <>
                {/* Desktop / wide-viewport: classic table. */}
                <div className="hidden md:block overflow-x-auto">
                  <table className="w-full text-sm border-collapse">
                    <thead className="text-[11px] uppercase tracking-wide text-muted/80 sticky top-0 bg-panel">
                      <tr className="text-center border-b border-border/40">
                        <th className="px-3 py-2.5 font-medium border-x border-border/30">Kind</th>
                        <th className="px-3 py-2.5 font-medium border-r border-border/30">Site cost ($m)</th>
                        <th className="px-3 py-2.5 font-medium border-r border-border/30">Site m²</th>
                        <th className="px-3 py-2.5 font-medium border-r border-border/30">Site sold</th>
                        <th className="px-3 py-2.5 font-medium border-r border-border/30">Site</th>
                        <th className="px-3 py-2.5 font-medium border-r border-border/30">Suburb</th>
                        <th className="px-3 py-2.5 font-medium border-r border-border/30">Postcode</th>
                        <th className="px-3 py-2.5 font-medium border-r border-border/30">Developer</th>
                        <th className="px-3 py-2.5 font-medium border-r border-border/30">Δ Supply</th>
                        <th className="px-3 py-2.5 font-medium border-r border-border/30">
                          <div>Timeline</div>
                          <div className="grid grid-cols-2 gap-2 text-[9px] text-muted/70 mt-1 normal-case tracking-normal">
                            <span>prep</span>
                            <span>project</span>
                          </div>
                        </th>
                        <th className="px-3 py-2.5 font-medium border-r border-border/30">DA</th>
                        <th className="px-3 py-2.5 font-medium border-r border-border/30">Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {apps.data.items.map((a) => (
                        <ProjectRow key={`${a.councilSlug}|${a.applicationId}`} a={a} />
                      ))}
                    </tbody>
                  </table>
                </div>

                {/* Narrow-viewport: stacked cards. Same data, no scroll. */}
                <div className="md:hidden divide-y divide-border/30">
                  {apps.data.items.map((a) => (
                    <ProjectCard key={`${a.councilSlug}|${a.applicationId}`} a={a} />
                  ))}
                </div>

                <div className="px-5 py-3 text-xs text-muted border-t border-border/40">
                  Showing {num(apps.data.items.length)} analysed project
                  {apps.data.items.length === 1 ? "" : "s"}.
                  {" "}Only DAs that have been LLM-summarised appear here.
                </div>
              </>
            )}
          </div>
        </section>
      </div>
    </div>
  );
}


// ---------- Row component ----------

import type { Application, DaInsight, SaleStory } from "../gen/listo_pb";

/** "124 Sunshine Parade" — strips lot/plan + suburb + state + postcode. */
function streetOnly(rawAddress: string | undefined): string {
  if (!rawAddress) return "—";
  // raw_address is like "Lot 376 RP21903, 124 Sunshine Parade, MIAMI  QLD  4220"
  // Drop a leading lot/plan chunk, then the trailing "..., SUBURB STATE POSTCODE".
  const parts = rawAddress.split(",").map((s) => s.trim()).filter(Boolean);
  const filtered = parts.filter((p) => !/^lot\s/i.test(p));
  // The street is the first remaining segment.
  const street = filtered[0] ?? rawAddress;
  // Strip trailing state/postcode if it leaked in (e.g. "27 Anembo Street QLD 4217")
  return street.replace(/\s+[A-Z]{2,3}\s+\d{4}\s*$/, "").trim();
}

// Light-gray cell-border helpers. `cellBase` applies to every td so we
// get a Google-Sheets-style grid; the row hover tints the whole tr.
const cellBase =
  "px-3 py-2 border-r border-border/30 align-middle";

// Deterministic per-suburb colour. Same suburb always gets the same
// class so a sorted/filtered list visually clusters by suburb. All
// classes are written out as literals so Tailwind's content scanner
// picks them up.
const SUBURB_COLOURS = [
  "text-amber-300",
  "text-emerald-300",
  "text-sky-300",
  "text-pink-300",
  "text-violet-300",
  "text-rose-300",
  "text-lime-300",
  "text-teal-300",
  "text-orange-300",
  "text-fuchsia-300",
  "text-indigo-300",
  "text-yellow-300",
  "text-cyan-300",
  "text-red-300",
  "text-green-300",
  "text-blue-300",
];

function suburbColor(suburb: string | null | undefined): string {
  if (!suburb) return "text-muted";
  const h = fnv1a(suburb.toLowerCase().trim());
  return SUBURB_COLOURS[h % SUBURB_COLOURS.length];
}

function ProjectRow({ a }: { a: Application }) {
  const insight = a.insight;
  const sale = a.saleStory;

  return (
    <tr className="row-hover border-b border-border/30">
      {/* Kind */}
      <td className={cx(cellBase, "text-center border-l border-border/30")}>
        <div className="flex justify-center"><KindBadge kind={a.kind} /></div>
      </td>

      {/* Site cost (millions, just the number — header has the $m unit) */}
      <td className={cx(cellBase, "text-center whitespace-nowrap font-mono num text-sm")}>
        {sale?.prePrice ? (
          (Number(sale.prePrice) / 1_000_000).toFixed(2)
        ) : (
          <span className="text-muted text-xs">—</span>
        )}
      </td>

      {/* Site m² — italic + asterisk when sourced from DA docs (LLM-extracted, uncertain) */}
      <td className={cx(cellBase, "text-center whitespace-nowrap font-mono num text-sm")}>
        {sale?.siteAreaM2 ? (
          sale.siteAreaSource === "da_docs" ? (
            <span
              title="Estimated from DA documents — uncertain"
              className="italic text-muted"
            >
              {num(sale.siteAreaM2)}*
            </span>
          ) : (
            <>{num(sale.siteAreaM2)}</>
          )
        ) : (
          <span className="text-muted text-xs">—</span>
        )}
      </td>

      {/* Site sold (month + year) */}
      <td className={cx(cellBase, "text-center whitespace-nowrap font-mono num text-sm")}>
        {sale?.preDate ? monthYear(sale.preDate) : <span className="text-muted text-xs">—</span>}
      </td>

      {/* Site — clickable, just street */}
      <td className={cx(cellBase, "max-w-[260px]")}>
        <Link
          to={`/applications/${a.councilSlug}/${encodeURIComponent(a.applicationId)}`}
          className="block hover:text-accent transition-colors text-sm truncate"
          title={a.rawAddress ?? ""}
        >
          {streetOnly(a.rawAddress)}
        </Link>
      </td>

      {/* Suburb + Postcode share a deterministic per-suburb colour so
          rows in the same suburb visually cluster. */}
      <td className={cx(cellBase, "whitespace-nowrap text-center text-sm font-medium tracking-wide", suburbColor(a.suburb))}>
        {a.suburb ? a.suburb.toUpperCase() : <span className="text-muted text-xs">—</span>}
      </td>
      <td className={cx(cellBase, "whitespace-nowrap text-center font-mono num text-sm", suburbColor(a.suburb))}>
        {a.postcode ?? "—"}
      </td>

      {/* Developer — codename in hot pink, project-count in bottom-right */}
      <td className={cx(cellBase, "max-w-[200px] text-center relative")}>
        {insight?.applicantName ? (
          <ApplicantCell insight={insight} />
        ) : (
          <span className="text-muted text-xs">—</span>
        )}
        {insight?.developerProjectCount != null && insight.developerProjectCount > 1 && (
          <span
            className="absolute bottom-0.5 right-1 text-[10px] font-mono text-muted/80"
            title={`${insight.developerProjectCount} total DAs from this developer in the dataset`}
          >
            ×{insight.developerProjectCount}
          </span>
        )}
      </td>

      {/* Δ Supply */}
      <td className={cx(cellBase, "text-center whitespace-nowrap")}>
        <DeltaSupplyCell insight={insight} sale={sale} />
      </td>

      {/* Timeline */}
      <td className={cx(cellBase, "whitespace-nowrap")}>
        <TimelineCell a={a} sale={sale} />
      </td>

      {/* DA Status — approval state from council. */}
      <td className={cx(cellBase, "text-center")}>
        <div className="flex flex-col items-center justify-center h-full">
          <DaStatusCell a={a} />
        </div>
      </td>

      {/* Project Status — only meaningful when DA is approved. Shows the
          lifecycle: building → finished&sold / finished&rented / dormant /
          don't know. Sale rows render alongside the 'sold' state. */}
      <td className={cx(cellBase, "max-w-[240px] text-center")}>
        <div className="flex flex-col items-center justify-center h-full">
          <ProjectStatusCell a={a} insight={insight} sale={sale} streetSuffix={streetOnly(a.rawAddress)} />
        </div>
      </td>
    </tr>
  );
}

/** Mobile / narrow-viewport rendering. Same data as ProjectRow, stacked
 *  vertically with section headers so everything fits on a phone screen
 *  without horizontal scroll. Reuses the cell components for consistency.
 */
function ProjectCard({ a }: { a: Application }) {
  const insight = a.insight;
  const sale = a.saleStory;
  const street = streetOnly(a.rawAddress);

  return (
    <Link
      to={`/applications/${a.councilSlug}/${encodeURIComponent(a.applicationId)}`}
      className="row-hover block p-4 space-y-3"
    >
      {/* Headline: kind badge + street, suburb on its own line. */}
      <div className="flex items-start gap-3">
        <KindBadge kind={a.kind} />
        <div className="flex-1 min-w-0">
          <div className="text-sm font-medium truncate">{street}</div>
          <div className={cx("text-xs uppercase tracking-wide", suburbColor(a.suburb))}>
            {a.suburb ?? "—"} {a.postcode ?? ""}
          </div>
        </div>
        {/* DA + Status inline at the right edge. */}
        <div className="flex items-center gap-2 shrink-0">
          <DaStatusCell a={a} />
          <ProjectStatusCell a={a} insight={insight} sale={sale} streetSuffix={street} />
        </div>
      </div>

      {/* Site financial summary — three datapoints in a row. */}
      <div className="grid grid-cols-3 gap-2 text-xs">
        <div className="text-center">
          <div className="text-muted text-[10px] uppercase tracking-wide">Site $</div>
          <div className="font-mono num text-sm">
            {sale?.prePrice
              ? `$${(Number(sale.prePrice) / 1_000_000).toFixed(2)}m`
              : <span className="text-muted">—</span>}
          </div>
        </div>
        <div className="text-center">
          <div className="text-muted text-[10px] uppercase tracking-wide">m²</div>
          <div className="font-mono num text-sm">
            {sale?.siteAreaM2 ? (
              sale.siteAreaSource === "da_docs" ? (
                <span title="Estimated from DA documents — uncertain" className="italic text-muted">
                  {num(sale.siteAreaM2)}*
                </span>
              ) : (
                num(sale.siteAreaM2)
              )
            ) : (
              <span className="text-muted">—</span>
            )}
          </div>
        </div>
        <div className="text-center">
          <div className="text-muted text-[10px] uppercase tracking-wide">Sold</div>
          <div className="font-mono num text-sm">
            {sale?.preDate ? monthYear(sale.preDate) : <span className="text-muted">—</span>}
          </div>
        </div>
      </div>

      {/* Δ Supply + Timeline, side by side. */}
      <div className="grid grid-cols-2 gap-3 text-xs">
        <div className="text-center">
          <div className="text-muted text-[10px] uppercase tracking-wide mb-1">Δ Supply</div>
          <DeltaSupplyCell insight={insight} sale={sale} />
        </div>
        <div className="text-center">
          <div className="text-muted text-[10px] uppercase tracking-wide mb-1">
            Timeline (prep · project)
          </div>
          <TimelineCell a={a} sale={sale} />
        </div>
      </div>

      {/* Developer codename + total project count from the backend. */}
      <div className="text-xs flex justify-between items-center">
        <span>
          <span className="text-muted">Developer: </span>
          {insight?.applicantName ? (
            <ApplicantCell insight={insight} />
          ) : (
            <span className="text-muted">—</span>
          )}
        </span>
        {insight?.developerProjectCount != null && insight.developerProjectCount > 1 && (
          <span
            className="text-[10px] font-mono text-muted/80"
            title={`${insight.developerProjectCount} total DAs from this developer in the dataset`}
          >
            ×{insight.developerProjectCount}
          </span>
        )}
      </div>
    </Link>
  );
}

/** "2021-07-28" → "Jul 2021". */
function monthYear(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleDateString("en-AU", { month: "short", year: "numeric" });
}

// ---------- codename: deterministic per company ----------
// Format: "adjective-australian-thing" (lowercase, hyphenated).
// Adjectives are kept neutral-to-positive. Nouns are Australian
// flora/fauna/cultural — keeps the audience smiling and avoids the
// "lockheed martin" energy of the company-name parade.

const CODENAME_ADJ = [
  "bright", "calm", "gentle", "lush", "golden", "mighty", "noble", "swift",
  "vivid", "witty", "zesty", "cosmic", "earthy", "lively", "mellow", "happy",
  "lucky", "royal", "silver", "steel", "fancy", "jolly", "dapper", "eager",
  "glowing", "pure", "fiery", "jade", "mystic", "nimble", "olive", "quick",
  "frosty", "indigo", "hardy", "keen", "sunny", "sandy", "breezy", "coral",
  "dewy", "peachy", "rosy", "pink", "mossy", "plucky", "amber", "scarlet",
];

const CODENAME_NOUN = [
  // animals
  "kookaburra", "koala", "kangaroo", "wombat", "wallaby", "possum", "echidna",
  "platypus", "dingo", "quokka", "numbat", "goanna", "gecko", "magpie", "emu",
  "lyrebird", "bilby", "brumby", "pademelon", "cassowary", "galah", "cockatoo",
  "lorikeet", "currawong", "kingfisher", "ibis", "rosella", "bandicoot",
  "wagtail", "fairywren", "dunnart", "antechinus", "yabby", "potoroo",
  // flora
  "waratah", "banksia", "bottlebrush", "wattle", "grevillea", "paperbark",
  "boronia", "hakea", "callistemon", "frangipani", "jacaranda", "eucalypt",
  "telopea", "kurrajong",
  // cultural
  "vegemite", "lamington", "pavlova", "billabong", "esky", "didgeridoo",
  "boomerang", "bunyip", "milo", "anzac", "tinnie", "snag",
];

function fnv1a(s: string): number {
  // 32-bit FNV-1a — deterministic, no deps.
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

function codenameFor(seed: string | null | undefined): string {
  if (!seed) return "unknown";
  // 3-token format `<adj>-<adj>-<noun>` for ~135K combos vs 2,880 in the
  // 2-token form. Birthday-paradox collisions don't kick in until ~370
  // applicants, comfortable headroom for the dataset's expected scale.
  // We use 3 hash slices + a second pass over the seed for the third
  // slot so adj1/adj2 don't trivially correlate.
  const h = fnv1a(seed);
  const h2 = fnv1a(seed + ":noun");
  const adj1 = CODENAME_ADJ[h % CODENAME_ADJ.length];
  let adj2 = CODENAME_ADJ[(h >>> 8) % CODENAME_ADJ.length];
  // Avoid `bright-bright-X`. If the two hash slots collide on the same
  // adjective, walk forward to the next distinct one.
  if (adj2 === adj1) {
    adj2 = CODENAME_ADJ[(((h >>> 8) % CODENAME_ADJ.length) + 1) % CODENAME_ADJ.length];
  }
  const noun = CODENAME_NOUN[h2 % CODENAME_NOUN.length];
  return `${adj1}-${adj2}-${noun}`;
}

/** Seed for codename hashing AND for grouping projects by developer.
 *  ACN first (most stable), normalised applicant_name as fallback. */
function applicantSeed(insight: DaInsight | undefined): string | null {
  if (!insight) return null;
  if (insight.applicantAcn) return insight.applicantAcn;
  const n = (insight.applicantName ?? "").toLowerCase().replace(/\s+/g, " ").trim();
  return n || null;
}

function ApplicantCell({ insight }: { insight: DaInsight }) {
  const seed = applicantSeed(insight) ?? "";
  const name = codenameFor(seed);
  const tooltip = [
    insight.applicantName,
    insight.applicantAcn ? `ACN ${insight.applicantAcn}` : null,
    insight.applicantAgentName ? `c/- ${insight.applicantAgentName}` : null,
  ]
    .filter(Boolean)
    .join(" · ");
  return (
    <span className="text-sm font-mono text-pink-400 font-semibold" title={tooltip}>
      {name}
    </span>
  );
}

function DeltaSupplyCell({
  insight,
  sale,
}: {
  insight: DaInsight | undefined;
  sale: SaleStory | undefined;
}) {
  if (!insight?.dwellingCount) {
    return <span className="text-muted text-xs">—</span>;
  }
  // "Finished" = at least one post-redev unit has actually sold. We still
  // show numbers when not finished as long as we have something to show
  // (LLM-projected from drawings) — but grayscale + asterisk to mark them
  // as projections, not realised sales.
  const finished = (sale?.unitSales.filter((u) => u.soldPrice).length ?? 0) > 0;
  const projected = sale?.postRoomsSource === "da_docs";

  // Vacant-land starts: parcel had no dwelling before the DA (e.g.
  // residential land subdivided into a duplex). In that case the supply
  // delta starts from 0, not 1, and pre br/ba are 0 not "unknown".
  const preType = (sale?.prePropertyType ?? "").toLowerCase();
  const preIsLand = preType === "land" || preType === "vacant";
  const preDwellings = preIsLand ? 0 : 1;
  const preBr = preIsLand ? 0 : sale?.preBedrooms ?? null;
  const preBa = preIsLand ? 0 : sale?.preBathrooms ?? null;

  const dwellDelta = insight.dwellingCount - preDwellings;
  const bedDelta =
    preBr != null && sale?.postBedrooms != null
      ? sale.postBedrooms - preBr
      : null;
  const bathDelta =
    preBa != null && sale?.postBathrooms != null
      ? sale.postBathrooms - preBa
      : null;

  const projectionTip =
    "Projected from DA plans — actual numbers appear once units list / sell";
  const landNote = preIsLand ? " (vacant land)" : "";
  const cellTip = projected
    ? projectionTip + landNote
    : !finished
      ? "Projected — no post-redev unit sales yet" + landNote
      : preIsLand
        ? "Pre-redev: vacant land"
        : undefined;

  return (
    <div
      className={cx(
        "grid grid-cols-3 gap-x-1 items-baseline text-base",
        !finished && "grayscale opacity-60",
      )}
      title={cellTip}
    >
      <DeltaCell
        emoji="🏠"
        delta={dwellDelta}
        suffix={projected ? "*" : undefined}
        tip={`${preDwellings} → ${insight.dwellingCount}${projected ? " (projected)" : ""}${landNote}`}
      />
      <DeltaCell
        emoji="🛏️"
        delta={bedDelta}
        suffix={projected ? "*" : undefined}
        tip={
          preBr != null || sale?.postBedrooms != null
            ? `${preBr ?? "?"} → ${sale?.postBedrooms ?? "?"}${projected ? " (projected)" : ""}${landNote}`
            : undefined
        }
      />
      <DeltaCell
        emoji="🛁"
        delta={bathDelta}
        suffix={projected ? "*" : undefined}
        tip={
          preBa != null || sale?.postBathrooms != null
            ? `${preBa ?? "?"} → ${sale?.postBathrooms ?? "?"}${projected ? " (projected)" : ""}${landNote}`
            : undefined
        }
      />
    </div>
  );
}

function DeltaCell({
  emoji,
  delta,
  tip,
  suffix,
}: {
  emoji: string;
  delta: number | null;
  tip?: string;
  suffix?: string;
}) {
  const tone =
    delta == null
      ? "text-muted"
      : delta > 0
        ? "text-good"
        : delta < 0
          ? "text-bad"
          : "text-muted";
  return (
    <span
      className="inline-flex items-baseline justify-center"
      title={tip}
    >
      <span aria-hidden>{emoji}</span>
      {delta != null && (
        <sup className={cx("font-mono num text-[10px] ml-0.5", tone)}>
          {delta}{suffix}
        </sup>
      )}
    </span>
  );
}

function TimelineCell({ a, sale }: { a: Application; sale: SaleStory | undefined }) {
  const bought = sale?.preDate;
  const approved = a.decisionDate;
  // First sell — earliest unit_sales sold_date.
  const firstSell = sale?.unitSales
    .map((u) => u.soldDate)
    .filter((d): d is string => !!d)
    .sort()[0];

  const daysBetween = (x?: string | null, y?: string | null): number | null => {
    if (!x || !y) return null;
    const d = (new Date(y).getTime() - new Date(x).getTime()) / (1000 * 60 * 60 * 24);
    return Math.round(d);
  };

  const fmtDuration = (d: number | null): string => {
    if (d == null) return "—";
    if (d < 60) return `${d}d`;
    if (d < 365) return `${Math.round(d / 30)}mo`;
    return `${(d / 365).toFixed(1)}y`;
  };

  // PREP = bought → DA approved. If the DA isn't approved yet, fall
  // back to bought → DA lodged, since that's the meaningful "how long
  // did they sit on the lot before pulling the trigger" number for
  // ongoing projects. PROJECT only makes sense once approved + sold;
  // before that we just say "ongoing".
  const finished = (sale?.unitSales.filter((u) => u.soldPrice).length ?? 0) > 0;
  const prepEnd = approved ?? a.lodgedDate;
  const prepDays = daysBetween(bought, prepEnd);
  const projectDays = approved ? daysBetween(approved, firstSell) : null;

  if (prepDays == null && projectDays == null && !finished) {
    return <span className="text-muted text-xs">—</span>;
  }

  return (
    <div className="grid grid-cols-2 gap-2 items-baseline text-center leading-tight">
      <span className="font-mono num text-sm">{fmtDuration(prepDays)}</span>
      {finished ? (
        <span className="font-mono num text-sm">{fmtDuration(projectDays)}</span>
      ) : (
        <span className="font-mono num text-sm text-muted">ongoing</span>
      )}
    </div>
  );
}

/** DA approval state from the council — independent of what happened
 *  after. Three buckets:
 *    ✅ approved   — decision_outcome contains 'approv'
 *    ❌ denied    — decision_outcome contains 'refus' or status 'withdrawn'
 *    ⏳ awaiting  — anything else (Lodged / Information Request / pending)
 */
function DaStatusCell({ a }: { a: Application }) {
  const outcome = (a.decisionOutcome ?? "").toLowerCase();
  const statusLower = (a.status ?? "").toLowerCase();

  let icon = "⏳";
  let label = "Awaiting decision";
  if (outcome.includes("approv")) {
    icon = "✅";
    label = "Approved";
  } else if (outcome.includes("refus") || statusLower.includes("withdrawn")) {
    icon = "❌";
    label = outcome.includes("refus") ? "Refused" : "Withdrawn";
  }

  const tip =
    a.status && a.status !== a.decisionOutcome
      ? `${label} — ${a.status}`
      : label;
  return (
    <span title={tip} className="text-base leading-none">
      {icon}
    </span>
  );
}

/** Project lifecycle. Only meaningful when the DA is approved; otherwise
 *  renders '—'. States:
 *    ✅ Sold N%    — at least one post-redev unit has sold (renders sale rows)
 *    🏘️ Rented    — unit-prefixed listing exists but no sale (built_unsold)
 *    🏗️ Building   — approved <2y ago, no built signals yet
 *    ❓ Don't know — approved 2-3y ago, no built signals (between
 *                    'still building' and 'definitely dormant')
 *    💤 Dormant    — approved >3y ago, no built signals (built_status =
 *                    abandoned_likely from the API)
 */
function ProjectStatusCell({
  a,
  insight,
  sale,
  streetSuffix,
}: {
  a: Application;
  insight: DaInsight | undefined;
  sale: SaleStory | undefined;
  streetSuffix: string;
}) {
  const isApproved = (a.decisionOutcome ?? "").toLowerCase().includes("approv");
  if (!isApproved) {
    return <span className="text-muted text-xs">—</span>;
  }

  const dwellingCount = insight?.dwellingCount ?? 0;
  const nSold = sale?.unitSales.filter((u) => u.soldPrice).length ?? 0;
  const anySold = nSold > 0;

  // Pull "124" out of "124 Sunshine Parade" so the SALE rows show
  // "1/124", "2/124" etc.
  const streetNumber = streetSuffix.match(/^(\d+[A-Za-z]?)/)?.[1] ?? "";

  const renderSaleRows = () =>
    sale?.unitSales.map((u, i) =>
      u.soldPrice ? (
        <div key={i} className="font-mono num text-[11px] text-good leading-tight">
          <span className="opacity-70 mr-1">SALE</span>
          {u.unitNumber}/{streetNumber} {audCompact(Number(u.soldPrice))}
        </div>
      ) : null,
    );

  // Finished and sold (covers all-sold and some-sold; the percent shows
  // progress so a single 'sold' state is enough).
  if (anySold && sale) {
    const pct = dwellingCount > 0
      ? Math.round((nSold / dwellingCount) * 100)
      : null;
    const label = pct === 100 ? "Sold" : pct != null ? `Sold ${pct}%` : "Sold";
    const toneCls = pct === 100 ? "text-good" : "text-warn";
    return (
      <div className="flex flex-col items-center justify-center leading-tight space-y-1">
        <div className={cx("text-xs font-medium", toneCls)}>✅ {label}</div>
        <div className="space-y-0.5 text-center">{renderSaleRows()}</div>
      </div>
    );
  }

  // Approved-but-not-sold buckets are driven by built_status from the API.
  const built = sale?.builtStatus;

  if (built === "built_unsold") {
    return (
      <div
        className="text-xs font-medium"
        title="Finished — built but not sold (held / rented)"
      >
        <span className="text-good">✅</span>{" "}
        <span className="text-warn">Rented</span>
      </div>
    );
  }

  if (built === "abandoned_likely") {
    return (
      <span title="Dormant — >3 years approved, no built signals" className="text-base leading-none">
        💤
      </span>
    );
  }

  // Unknown — split by age. Approved <15mo ago → 'Building' (15 months
  // is roughly the floor for a duplex from approval to completion).
  // 15-36mo with no signals → 'Don't know' (longer than typical, but
  // not yet old enough to call dormant). >36mo handled above by the
  // SQL's abandoned_likely branch.
  const decisionDate = a.decisionDate ? new Date(a.decisionDate) : null;
  const monthsSinceDecision = decisionDate
    ? (Date.now() - decisionDate.getTime()) / (1000 * 60 * 60 * 24 * 30.44)
    : null;
  if (monthsSinceDecision != null && monthsSinceDecision < 15) {
    return (
      <span title={`Building — approved ${monthsSinceDecision.toFixed(0)}mo ago (under 15mo, the typical duplex build floor)`} className="text-base leading-none">
        🏗️
      </span>
    );
  }
  return (
    <span title="Don't know — approved long enough we'd expect signals, but none on file" className="text-base leading-none">
      ❓
    </span>
  );
}
