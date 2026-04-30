import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "../lib/client";
import { bytes, num, pct } from "../lib/format";

// Sticky thin info-bar at the bottom of every page (axiom.trade-style).
// Polls /GetStatus every 30s for live counters + the current variable rate.
export default function BottomBar() {
  const status = useQuery({
    queryKey: ["status"],
    queryFn: () => api.getStatus({}),
    refetchInterval: 30_000,
  });

  const [now, setNow] = useState(() => new Date());
  useEffect(() => {
    const id = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(id);
  }, []);

  const s = status.data;
  const scrape = s?.latestScrape;
  const isLive = !!scrape && !scrape.finishedAt;

  return (
    <footer className="fixed bottom-0 inset-x-0 z-30 h-7 border-t border-border/60 bg-panel/95 backdrop-blur text-[11px] text-muted font-mono num">
      <div className="max-w-[1600px] mx-auto px-6 h-full flex items-center gap-5 overflow-x-auto whitespace-nowrap">
        <Item emoji="📊" label="apps" value={s ? num(s.appsTotal) : "—"} />
        <Item emoji="📁" label="docs" value={s ? num(s.docsTotal) : "—"} />
        <Item emoji="💾" label="size" value={s ? bytes(s.docsTotalBytes) : "—"} />
        <Item
          emoji="⏰"
          label="scrape"
          value={
            !s
              ? "—"
              : !scrape
                ? "idle"
                : isLive
                  ? `${scrape.councilSlug} live`
                  : `${scrape.councilSlug} ${scrape.status}`
          }
          tone={isLive ? "good" : undefined}
        />
        <Item
          emoji="💰"
          label="var"
          value={s?.currentVarRatePct != null ? pct(s.currentVarRatePct) : "—"}
        />
        <div className="flex-1" />
        <span title="Backend transport">⚡ gRPC-Web</span>
        <span aria-hidden>·</span>
        <span>⌚ {now.toLocaleTimeString("en-AU", { hour12: false })}</span>
      </div>
    </footer>
  );
}

function Item({
  emoji,
  label,
  value,
  tone,
}: {
  emoji: string;
  label: string;
  value: string;
  tone?: "good" | "bad" | "warn";
}) {
  return (
    <span className="flex items-center gap-1">
      <span aria-hidden>{emoji}</span>
      <span className="text-muted/80">{label}</span>
      <span className={tone === "good" ? "text-good" : "text-text"}>
        {value}
      </span>
    </span>
  );
}
