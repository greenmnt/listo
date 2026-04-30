import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useSearchParams } from "react-router-dom";
import { api } from "../lib/client";
import KindBadge from "../components/KindBadge";
import StatusPill from "../components/StatusPill";
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

      <div className="grid grid-cols-1 xl:grid-cols-5 gap-4">
        {/* Trending suburbs leaderboard */}
        <section className="panel xl:col-span-1 self-start">
          <header className="px-4 py-3 border-b border-border/60">
            <h2 className="text-xs font-semibold tracking-wide uppercase text-muted">
              🔥 Trending suburbs
            </h2>
          </header>
          <div className="divide-y divide-border/40">
            {suburbs.data?.items.length === 0 && (
              <EmptyState
                emoji="🌱"
                title="No suburb stats yet"
                hint="Trending data appears once the scraper has indexed at least a few months of DAs."
              />
            )}
            {suburbs.data?.items.slice(0, 12).map((s, i) => {
              const last = Number(s.nLast30d);
              const prev = Number(s.nPrev30d);
              const delta = last - prev;
              const trendEmoji = delta > 0 ? "📈" : delta < 0 ? "📉" : "➡️";
              const trendClass =
                delta > 0 ? "text-good" : delta < 0 ? "text-bad" : "text-muted";
              return (
                <Link
                  key={s.suburb}
                  to={`/?suburb=${encodeURIComponent(s.suburb)}`}
                  className="row-hover flex items-center gap-2 px-3 py-2 text-xs"
                >
                  <span className="w-4 text-muted num text-right">{i + 1}</span>
                  <span className="flex-1 truncate" title={titleCase(s.suburb)}>
                    {titleCase(s.suburb)}
                  </span>
                  <span className="font-mono num text-text w-7 text-right">
                    {num(s.nKind)}
                  </span>
                  <span className={cx("font-mono num w-10 text-right", trendClass)}>
                    {trendEmoji}
                    {delta > 0 ? `+${delta}` : delta}
                  </span>
                </Link>
              );
            })}
          </div>
        </section>

        {/* Filterable applications table */}
        <section className="xl:col-span-4 space-y-4">
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
              <div className="overflow-x-auto">
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
                      <th className="px-3 py-2.5 font-medium border-r border-border/30">Timeline</th>
                      <th className="px-3 py-2.5 font-medium border-r border-border/30">Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {apps.data.items.map((a) => (
                      <ProjectRow key={`${a.councilSlug}|${a.applicationId}`} a={a} />
                    ))}
                  </tbody>
                </table>
                <div className="px-5 py-3 text-xs text-muted border-t border-border/40">
                  Showing {num(apps.data.items.length)} analysed project
                  {apps.data.items.length === 1 ? "" : "s"}.
                  {" "}Only DAs that have been LLM-summarised appear here.
                </div>
              </div>
            )}
          </div>
        </section>
      </div>
    </div>
  );
}

function titleCase(s: string): string {
  return s
    .toLowerCase()
    .split(" ")
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
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

      {/* Developer — codename in hot pink */}
      <td className={cx(cellBase, "max-w-[200px] text-center")}>
        {insight?.applicantName ? (
          <ApplicantCell insight={insight} />
        ) : (
          <span className="text-muted text-xs">—</span>
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

      {/* Status */}
      <td className={cx(cellBase, "max-w-[260px]")}>
        <StatusCell a={a} insight={insight} sale={sale} streetSuffix={streetOnly(a.rawAddress)} />
      </td>
    </tr>
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
  const h = fnv1a(seed);
  const adj = CODENAME_ADJ[h % CODENAME_ADJ.length];
  const noun = CODENAME_NOUN[(h >>> 8) % CODENAME_NOUN.length];
  return `${adj}-${noun}`;
}

function ApplicantCell({ insight }: { insight: DaInsight }) {
  // ACN is the most stable seed; fall back to normalised name when absent.
  const seed =
    insight.applicantAcn ||
    (insight.applicantName ?? "").toLowerCase().replace(/\s+/g, " ").trim();
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
  // "Finished" = at least one post-redev unit has actually sold. Until
  // then we still render the three emojis in fixed column order so rows
  // line up vertically — but we grayscale them to signal "projected,
  // not realised yet".
  const finished = (sale?.unitSales.filter((u) => u.soldPrice).length ?? 0) > 0;

  // Pre-redev state: a single house on the lot. Post: from the LLM
  // (dwellings) and from per-unit domain rows joined in build_sale_story
  // (bedrooms, bathrooms).
  const dwellDelta = insight.dwellingCount - 1;
  const bedDelta =
    sale?.preBedrooms != null && sale?.postBedrooms != null
      ? sale.postBedrooms - sale.preBedrooms
      : null;
  const bathDelta =
    sale?.preBathrooms != null && sale?.postBathrooms != null
      ? sale.postBathrooms - sale.preBathrooms
      : null;

  // Pre-approval stage: show only the greyed emojis, no numbers — the
  // deltas are speculative and the user explicitly wants the cell to
  // stay quiet until the project's actually built and selling.
  const showNumbers = finished;

  return (
    <div
      className={cx(
        "grid grid-cols-3 gap-x-1 items-baseline text-base",
        !finished && "grayscale opacity-60",
      )}
      title={!finished ? "Projected — no post-redev unit sales yet" : undefined}
    >
      <DeltaCell
        emoji="🏠"
        delta={showNumbers ? dwellDelta : null}
        tip={`1 → ${insight.dwellingCount}`}
      />
      <DeltaCell
        emoji="🛏️"
        delta={showNumbers ? bedDelta : null}
        tip={
          sale?.preBedrooms != null || sale?.postBedrooms != null
            ? `${sale?.preBedrooms ?? "?"} → ${sale?.postBedrooms ?? "?"}`
            : undefined
        }
      />
      <DeltaCell
        emoji="🛁"
        delta={showNumbers ? bathDelta : null}
        tip={
          sale?.preBathrooms != null || sale?.postBathrooms != null
            ? `${sale?.preBathrooms ?? "?"} → ${sale?.postBathrooms ?? "?"}`
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
}: {
  emoji: string;
  delta: number | null;
  tip?: string;
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
          {delta}
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
    <div className="leading-tight">
      <div className="flex items-baseline gap-2">
        <span className="text-[10px] text-muted uppercase tracking-wide w-14">Prep</span>
        <span className="font-mono num text-sm">{fmtDuration(prepDays)}</span>
      </div>
      <div className="flex items-baseline gap-2 mt-0.5">
        <span className="text-[10px] text-muted uppercase tracking-wide w-14">Project</span>
        {finished ? (
          <span className="font-mono num text-sm">{fmtDuration(projectDays)}</span>
        ) : (
          <span className="font-mono num text-sm text-muted">ongoing</span>
        )}
      </div>
    </div>
  );
}

/** Synthesize the right "Status" given the full picture:
 *  - All units sold → ✅ Completed (and show each sale price)
 *  - Some sold → 🚧 In progress (% sold)
 *  - Approved + no sales yet → 📐 Approved
 *  - Otherwise → council status (Pending / Refused / etc.)
 */
function StatusCell({
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
  const dwellingCount = insight?.dwellingCount ?? 0;
  const nSold = sale?.unitSales.filter((u) => u.soldPrice).length ?? 0;
  const allSold = dwellingCount > 0 && nSold >= dwellingCount;
  const someSold = nSold > 0 && !allSold;
  const isApproved = (a.decisionOutcome ?? "").toLowerCase().includes("approved");

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

  if (allSold && sale) {
    return (
      <div className="leading-tight space-y-1">
        <div className="pill border-good/40 text-good bg-good/10 text-xs inline-flex">
          ✅ Completed · 100%
        </div>
        <div className="space-y-0.5">{renderSaleRows()}</div>
      </div>
    );
  }

  if (someSold && sale) {
    const pct = Math.round((nSold / Math.max(dwellingCount, 1)) * 100);
    return (
      <div className="leading-tight space-y-1">
        <div className="pill border-warn/40 text-warn bg-warn/10 text-xs inline-flex">
          🚧 Selling · {pct}%
        </div>
        <div className="space-y-0.5">{renderSaleRows()}</div>
      </div>
    );
  }

  if (isApproved) {
    return (
      <div className="leading-tight">
        <div className="pill border-accent/40 text-accent bg-accent/10 text-xs inline-flex">
          📐 Approved
        </div>
        {a.status && a.status !== a.decisionOutcome && (
          <div className="text-[10px] text-muted mt-1">{a.status}</div>
        )}
      </div>
    );
  }

  // No decision recorded yet — DA is still working through council.
  // Covers councilStatus = 'Lodged' / 'Information Request' / null and
  // anything that's not refused/withdrawn.
  const refused =
    (a.decisionOutcome ?? "").toLowerCase().includes("refus") ||
    (a.status ?? "").toLowerCase().includes("withdrawn");
  if (!refused) {
    return (
      <div className="leading-tight">
        <div className="pill border-warn/40 text-warn bg-warn/10 text-xs inline-flex">
          🟠 Getting approvals
        </div>
        {a.status && (
          <div className="text-[10px] text-muted mt-1">{a.status}</div>
        )}
      </div>
    );
  }

  return <StatusPill status={a.status} />;
}
