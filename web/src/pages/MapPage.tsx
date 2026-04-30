import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { MapContainer, TileLayer, CircleMarker, Tooltip } from "react-leaflet";
import { api } from "../lib/client";
import { DaKind } from "../gen/listo_pb";
import { KIND_META, KIND_FILTER_OPTIONS } from "../lib/kinds";
import { cx, num } from "../lib/format";

const GC_CENTRE: [number, number] = [-28.005, 153.405];

export default function MapPage() {
  const [kind, setKind] = useState<string>("all");

  const points = useQuery({
    queryKey: ["map", { kind }],
    queryFn: () =>
      api.mapPoints({
        kind: kind === "all" ? undefined : kind,
        limit: 3000,
      }),
  });

  const counts = useMemo(() => {
    const c: Record<DaKind, number> = {
      [DaKind.UNSPECIFIED]: 0,
      [DaKind.GRANNY]: 0,
      [DaKind.DUPLEX]: 0,
      [DaKind.BIG_DEV]: 0,
      [DaKind.OTHER]: 0,
    };
    for (const p of points.data?.points ?? []) {
      c[p.kind] = (c[p.kind] ?? 0) + 1;
    }
    return c;
  }, [points.data]);

  return (
    <div className="space-y-3">
      <div className="panel p-3 flex flex-wrap items-center gap-3">
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

        <div className="ml-auto text-xs text-muted flex flex-wrap items-center gap-3">
          <Legend kind={DaKind.GRANNY} n={counts[DaKind.GRANNY]} />
          <Legend kind={DaKind.DUPLEX} n={counts[DaKind.DUPLEX]} />
          <Legend kind={DaKind.BIG_DEV} n={counts[DaKind.BIG_DEV]} />
          <span className="text-muted/70">
            ⚠️ coords are deterministic-dummy until geocoding lands
          </span>
        </div>
      </div>

      <div className="panel overflow-hidden h-[calc(100vh-220px)]">
        <MapContainer
          center={GC_CENTRE}
          zoom={11}
          scrollWheelZoom
          className="h-full w-full"
          style={{ background: "#0b0c10" }}
        >
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
            url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
          />
          {(points.data?.points ?? []).map((p) => {
            const meta = KIND_META[p.kind] ?? KIND_META[DaKind.OTHER];
            const r = Math.min(
              12,
              Math.max(4, 4 + Math.sqrt(p.approvedUnits ?? 1)),
            );
            return (
              <CircleMarker
                key={`${p.councilSlug}|${p.applicationId}`}
                center={[p.lat, p.lng]}
                radius={r}
                pathOptions={{
                  color: meta.hex,
                  fillColor: meta.hex,
                  fillOpacity: 0.7,
                  weight: 1,
                }}
              >
                <Tooltip direction="top" offset={[0, -4]} opacity={0.95}>
                  <div className="text-xs">
                    <div className="font-semibold">
                      {meta.emoji} {p.applicationId}
                    </div>
                    <div>{p.rawAddress ?? p.suburb ?? "—"}</div>
                    {p.status && (
                      <div className="text-muted">{p.status}</div>
                    )}
                  </div>
                </Tooltip>
              </CircleMarker>
            );
          })}
        </MapContainer>
      </div>
    </div>
  );
}

function Legend({ kind, n }: { kind: DaKind; n: number }) {
  const meta = KIND_META[kind];
  return (
    <span className="flex items-center gap-1.5">
      <span
        className="inline-block h-2.5 w-2.5 rounded-full"
        style={{ background: meta.hex }}
        aria-hidden
      />
      <span>{meta.short}</span>
      <span className="font-mono num text-text">{num(n)}</span>
    </span>
  );
}
