import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
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
                    name="🍏 Granny"
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
