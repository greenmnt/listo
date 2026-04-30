export default function AboutPage() {
  return (
    <div className="prose-invert max-w-3xl space-y-6 text-sm leading-relaxed">
      <header>
        <h1 className="text-2xl font-semibold">ℹ️ About Listo</h1>
        <p className="text-muted mt-2">
          A radar for duplex / granny-flat / multi-unit redevelopments across
          Australian coastal councils. Initially Gold Coast (QLD); Newcastle
          and other coastal LGAs to follow.
        </p>
      </header>

      <section>
        <h2 className="text-base font-semibold mt-6 mb-2">
          🪧 The DA process in one screen
        </h2>
        <ol className="list-decimal list-inside space-y-1 text-muted">
          <li>
            <span className="text-text">Lodgement.</span> Owner / consultant
            files a Development Application with the council.
          </li>
          <li>
            <span className="text-text">Information &amp; referral.</span>{" "}
            Council requests further info; state agencies (DTMR, EHP, etc.)
            referred where required.
          </li>
          <li>
            <span className="text-text">Public notification.</span> Larger DAs
            posted for public comment (typically 15–30 business days).
          </li>
          <li>
            <span className="text-text">Decision.</span> Approved, approved
            with conditions, or refused. Reasons and conditions become public.
          </li>
          <li>
            <span className="text-text">Operational works / build.</span> OPW
            and building approvals follow, then construction certificates,
            then occupancy.
          </li>
        </ol>
      </section>

      <section>
        <h2 className="text-base font-semibold mt-6 mb-2">📚 Glossary</h2>
        <dl className="grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-2 text-muted">
          <Term k="MCU" v="Material Change of Use — primary DA type for new dwelling configurations." />
          <Term k="ROL" v="Reconfiguration of a Lot — subdividing, amalgamating or boundary realignments." />
          <Term k="OPW" v="Operational Works — earthworks, vegetation, civil works." />
          <Term k="Dual occupancy" v="Two attached or detached dwellings on one lot. Aka duplex." />
          <Term k="Secondary dwelling" v="Granny flat — smaller dwelling secondary to the main house." />
          <Term k="Big dev" v="Listo's bucket for 3+ unit residential developments (triplex, townhouses, apartments)." />
        </dl>
      </section>

      <section>
        <h2 className="text-base font-semibold mt-6 mb-2">🛠️ How it works</h2>
        <p className="text-muted">
          A Python scraper crawls each council's DA register on a schedule,
          captures every page of every application plus all attached
          documents, and stores them in MySQL. A Rust gRPC service
          (tonic + tonic-web) serves a single source-of-truth API
          described by{" "}
          <code className="bg-panel-2 px-1 rounded">proto/listo.proto</code>.
          This frontend is a Vite + React SPA generated from that same
          proto via{" "}
          <code className="bg-panel-2 px-1 rounded">buf generate</code>.
        </p>
      </section>
    </div>
  );
}

function Term({ k, v }: { k: string; v: string }) {
  return (
    <div className="flex gap-3">
      <dt className="text-text font-medium min-w-[140px]">{k}</dt>
      <dd>{v}</dd>
    </div>
  );
}
