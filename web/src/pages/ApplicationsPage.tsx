import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api } from "../lib/client";
import KindBadge from "../components/KindBadge";
import StatusPill from "../components/StatusPill";
import EmptyState from "../components/EmptyState";
import { relativeDate, shortDate } from "../lib/format";

export default function ApplicationsPage() {
  const apps = useQuery({
    queryKey: ["apps", "recent"],
    queryFn: () => api.listApplications({ limit: 25 }),
  });

  return (
    <div className="space-y-4">
      <section className="panel">
        <header className="px-5 py-4 border-b border-border/60 flex items-center justify-between">
          <h2 className="text-sm font-semibold tracking-wide uppercase text-muted">
            🆕 Recent activity
          </h2>
          <Link to="/" className="text-xs text-accent hover:underline">
            See all (filterable) →
          </Link>
        </header>

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
        {apps.data?.items.length === 0 && (
          <EmptyState
            emoji="🪹"
            title="No applications yet"
            hint="Run `uv run listo council scrape cogc --from … --to …` to populate."
          />
        )}
        {apps.data && apps.data.items.length > 0 && (
          <div className="overflow-hidden">
            <table className="w-full text-sm">
              <thead className="text-[11px] uppercase tracking-wide text-muted/80">
                <tr className="text-left">
                  <th className="px-5 py-2 font-medium">Kind</th>
                  <th className="px-2 py-2 font-medium">App ID</th>
                  <th className="px-2 py-2 font-medium">Address</th>
                  <th className="px-2 py-2 font-medium">Status</th>
                  <th className="px-2 py-2 font-medium num text-right">
                    Lodged
                  </th>
                  <th className="px-5 py-2 font-medium num text-right">
                    Units
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border/30">
                {apps.data.items.map((a) => (
                  <tr
                    key={`${a.councilSlug}|${a.applicationId}`}
                    className="row-hover"
                  >
                    <td className="px-5 py-2">
                      <KindBadge kind={a.kind} />
                    </td>
                    <td className="px-2 py-2 font-mono text-xs">
                      <Link
                        to={`/applications/${a.councilSlug}/${encodeURIComponent(a.applicationId)}`}
                        className="hover:text-accent hover:underline"
                      >
                        {a.applicationId}
                      </Link>
                    </td>
                    <td className="px-2 py-2 truncate max-w-[260px]">
                      {a.rawAddress ?? a.suburb ?? "—"}
                    </td>
                    <td className="px-2 py-2">
                      <StatusPill status={a.status} />
                    </td>
                    <td className="px-2 py-2 font-mono num text-right text-muted text-xs">
                      {shortDate(a.lodgedDate)}
                      <div className="text-[10px]">
                        {relativeDate(a.lodgedDate)}
                      </div>
                    </td>
                    <td className="px-5 py-2 font-mono num text-right">
                      {a.approvedUnits ?? "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}
