import { useParams, Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api } from "../lib/client";
import KindBadge from "../components/KindBadge";
import StatusPill from "../components/StatusPill";
import EmptyState from "../components/EmptyState";
import { DaKind } from "../gen/listo_pb";
import { bytes, num, shortDate } from "../lib/format";

export default function ApplicationDetailPage() {
  const { slug, appId } = useParams<{ slug: string; appId: string }>();
  const detail = useQuery({
    queryKey: ["app", slug, appId],
    queryFn: () =>
      api.getApplication({
        councilSlug: slug!,
        applicationId: decodeURIComponent(appId!),
      }),
    enabled: !!slug && !!appId,
  });

  if (detail.isLoading) {
    return <div className="text-muted text-sm">Loading…</div>;
  }
  if (detail.error || !detail.data) {
    return (
      <EmptyState
        emoji="🚧"
        title="Couldn't load application"
        hint={detail.error ? String(detail.error) : "Not found."}
      />
    );
  }

  const d = detail.data;
  const base = d.base!;

  return (
    <div className="space-y-4">
      <Link
        to="/applications"
        className="text-xs text-muted hover:text-text"
      >
        ← Back to all applications
      </Link>

      <header className="panel p-5">
        <div className="flex flex-wrap items-center gap-3">
          <KindBadge kind={base.kind ?? DaKind.OTHER} size="md" />
          <StatusPill status={base.status} />
          <span className="font-mono num text-xs text-muted">
            {base.applicationId}
          </span>
          {base.applicationUrl && (
            <a
              href={base.applicationUrl}
              target="_blank"
              rel="noreferrer"
              className="ml-auto text-xs text-accent hover:underline"
            >
              ↗ Council page
            </a>
          )}
        </div>
        <h1 className="text-xl font-semibold mt-3">
          {base.rawAddress ?? base.suburb ?? "—"}
        </h1>
        {base.description && (
          <p className="text-sm text-muted mt-2 max-w-3xl">{base.description}</p>
        )}
      </header>

      <section className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <Field label="Council" value={base.councilSlug} />
        <Field label="Vendor" value={d.vendor} />
        <Field label="Type" value={base.typeCode ?? base.applicationType} />
        <Field label="Lodged" value={shortDate(base.lodgedDate)} />
        <Field label="Decided" value={shortDate(base.decisionDate)} />
        <Field label="Decision" value={base.decisionOutcome} />
        <Field label="Approved units" value={base.approvedUnits ?? "—"} />
        <Field label="Applicant" value={d.applicantName} />
        <Field label="Builder" value={d.builderName} />
        <Field label="Architect" value={d.architectName} />
        <Field label="Owner" value={d.ownerName} />
        <Field label="Lot on plan" value={d.lotOnPlan} />
      </section>

      <section className="panel">
        <header className="px-5 py-4 border-b border-border/60 flex items-center justify-between">
          <h2 className="text-sm font-semibold uppercase tracking-wide text-muted">
            📁 Documents ({num(d.documents.length)})
          </h2>
        </header>
        {d.documents.length === 0 ? (
          <EmptyState emoji="📭" title="No documents archived for this DA" />
        ) : (
          <table className="w-full text-sm">
            <thead className="text-[11px] uppercase text-muted/80">
              <tr className="text-left border-b border-border/40">
                <th className="px-5 py-2 font-medium">Type</th>
                <th className="px-2 py-2 font-medium">Title</th>
                <th className="px-2 py-2 font-medium num text-right">Size</th>
                <th className="px-2 py-2 font-medium num text-right">Pages</th>
                <th className="px-5 py-2 font-medium">Published</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border/30">
              {d.documents.map((doc) => (
                <tr key={String(doc.id)} className="row-hover">
                  <td className="px-5 py-2 font-mono text-xs text-muted">
                    {doc.docType ?? "—"}
                  </td>
                  <td className="px-2 py-2">
                    {doc.sourceUrl ? (
                      <a
                        href={doc.sourceUrl}
                        target="_blank"
                        rel="noreferrer"
                        className="hover:text-accent hover:underline"
                      >
                        {doc.title ?? doc.docOid ?? "—"}
                      </a>
                    ) : (
                      doc.title ?? "—"
                    )}
                  </td>
                  <td className="px-2 py-2 font-mono num text-right text-xs">
                    {doc.fileSize != null ? bytes(doc.fileSize) : "—"}
                  </td>
                  <td className="px-2 py-2 font-mono num text-right text-xs">
                    {doc.pageCount ?? "—"}
                  </td>
                  <td className="px-5 py-2 font-mono num text-xs text-muted">
                    {shortDate(doc.publishedAt)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </div>
  );
}

function Field({
  label,
  value,
}: {
  label: string;
  value?: string | number | null;
}) {
  return (
    <div className="panel p-4">
      <div className="text-[11px] uppercase tracking-wide text-muted">
        {label}
      </div>
      <div className="font-mono num text-sm mt-1">
        {value === undefined || value === null || value === "" ? "—" : value}
      </div>
    </div>
  );
}
