import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
} from "recharts";
import { api } from "../lib/client";
import { KIND_META } from "../lib/kinds";
import { DaKind } from "../gen/listo_pb";
import EmptyState from "../components/EmptyState";
import { cx, num, titleCase } from "../lib/format";

export default function TrendsPage() {
  const [suburb, setSuburb] = useState<string>("");

  const trends = useQuery({
    queryKey: ["trends", { suburb }],
    queryFn: () => api.trendStats({ suburb: suburb || undefined }),
  });

  const suburbs = useQuery({
    queryKey: ["suburb-stats", "for-trends"],
    queryFn: () => api.suburbStats({ limit: 60 }),
  });

  const data = (trends.data?.buckets ?? []).map((b) => ({
    bucket: b.bucketStart,
    granny: Number(b.nGranny),
    duplex: Number(b.nDuplex),
    big_dev: Number(b.nBigDev),
    other: Number(b.nOther),
    approved: Number(b.nApproved),
    total: Number(b.nTotal),
  }));

  return (
    <div className="space-y-4">
      <div className="panel p-4 flex flex-wrap gap-3 items-center">
        <label className="text-sm">
          <span className="text-muted text-xs mr-2">Suburb</span>
          <select
            value={suburb}
            onChange={(e) => setSuburb(e.target.value)}
            className="bg-panel-2 border border-border/60 rounded-lg px-3 py-1.5 text-sm"
          >
            <option value="">All suburbs</option>
            {suburbs.data?.items.map((s) => (
              <option key={s.suburb} value={s.suburb}>
                {s.suburb}
              </option>
            ))}
          </select>
        </label>
        <div className="text-xs text-muted">
          {data.length} monthly buckets
        </div>
      </div>

      {/* Trending suburbs leaderboard — moved here from the Projects page. */}
      <section className="panel">
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

      {data.length === 0 ? (
        <EmptyState
          emoji="📉"
          title="No trend data yet"
          hint="Trends populate once the scraper has indexed at least one DA with a lodged_date."
        />
      ) : (
        <>
          <section className="panel p-5">
            <h2 className="text-sm font-semibold uppercase tracking-wide text-muted mb-3">
              📊 Applications by kind, per month
            </h2>
            <div className="h-72">
              <ResponsiveContainer>
                <BarChart data={data}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#26282f" />
                  <XAxis dataKey="bucket" stroke="#8a8f9a" fontSize={11} />
                  <YAxis stroke="#8a8f9a" fontSize={11} />
                  <Tooltip
                    contentStyle={{
                      background: "#14161d",
                      border: "1px solid #26282f",
                      borderRadius: 8,
                    }}
                  />
                  <Legend />
                  <Bar
                    dataKey="granny"
                    stackId="a"
                    fill={KIND_META[DaKind.GRANNY].hex}
                    name="🍏 Flat"
                  />
                  <Bar
                    dataKey="duplex"
                    stackId="a"
                    fill={KIND_META[DaKind.DUPLEX].hex}
                    name="🏘️ Duplex"
                  />
                  <Bar
                    dataKey="big_dev"
                    stackId="a"
                    fill={KIND_META[DaKind.BIG_DEV].hex}
                    name="🏗️ Big dev"
                  />
                  <Bar
                    dataKey="other"
                    stackId="a"
                    fill={KIND_META[DaKind.OTHER].hex}
                    name="🏠 Other"
                  />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </section>

          <section className="panel p-5">
            <h2 className="text-sm font-semibold uppercase tracking-wide text-muted mb-3">
              ✅ Approved share, per month
            </h2>
            <div className="h-56">
              <ResponsiveContainer>
                <LineChart
                  data={data.map((d) => ({
                    bucket: d.bucket,
                    rate: d.total > 0 ? (d.approved / d.total) * 100 : 0,
                  }))}
                >
                  <CartesianGrid strokeDasharray="3 3" stroke="#26282f" />
                  <XAxis dataKey="bucket" stroke="#8a8f9a" fontSize={11} />
                  <YAxis stroke="#8a8f9a" fontSize={11} unit="%" />
                  <Tooltip
                    contentStyle={{
                      background: "#14161d",
                      border: "1px solid #26282f",
                      borderRadius: 8,
                    }}
                    formatter={(v: number) => `${v.toFixed(1)}%`}
                  />
                  <Line
                    type="monotone"
                    dataKey="rate"
                    stroke="#34d399"
                    strokeWidth={2}
                    dot={false}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </section>
        </>
      )}
    </div>
  );
}
