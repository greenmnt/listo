import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "../lib/client";
import { Verdict } from "../gen/listo_pb";
import { aud, audCompact, cx, pct } from "../lib/format";

const PRESETS = [
  {
    label: "Tiny granny flat",
    emoji: "🍏",
    purchasePrice: 850_000,
    buildCost: 220_000,
    holdMonths: 14,
    salePrice: 1_280_000,
  },
  {
    label: "GC duplex",
    emoji: "🏘️",
    purchasePrice: 1_300_000,
    buildCost: 850_000,
    holdMonths: 18,
    salePrice: 2_950_000,
  },
  {
    label: "Triplex",
    emoji: "🏗️",
    purchasePrice: 1_900_000,
    buildCost: 1_650_000,
    holdMonths: 22,
    salePrice: 4_650_000,
  },
];

export default function CalculatorPage() {
  const [purchasePrice, setPurchase] = useState(PRESETS[1].purchasePrice);
  const [buildCost, setBuild] = useState(PRESETS[1].buildCost);
  const [holdMonths, setHold] = useState(PRESETS[1].holdMonths);
  const [salePrice, setSale] = useState(PRESETS[1].salePrice);
  const [ratePct, setRate] = useState<number | undefined>(undefined);

  const rates = useQuery({
    queryKey: ["rates"],
    queryFn: () => api.getCurrentRates({}),
  });

  // Pre-fill the rate from the latest variable rate the first time it loads.
  useEffect(() => {
    if (ratePct == null && rates.data?.variableOoPct != null) {
      setRate(rates.data.variableOoPct);
    }
  }, [rates.data, ratePct]);

  const calc = useQuery({
    queryKey: [
      "calc",
      { purchasePrice, buildCost, holdMonths, salePrice, ratePct },
    ],
    queryFn: () =>
      api.calcProfitability({
        purchasePrice,
        buildCost,
        holdMonths,
        salePrice,
        ratePct,
      }),
    enabled:
      purchasePrice > 0 && buildCost >= 0 && holdMonths > 0 && salePrice > 0,
  });

  return (
    <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
      {/* Inputs */}
      <section className="panel p-5 lg:col-span-1 space-y-4">
        <h2 className="text-sm font-semibold uppercase tracking-wide text-muted">
          🧮 Project inputs
        </h2>

        <div className="flex flex-wrap gap-2">
          {PRESETS.map((p) => (
            <button
              key={p.label}
              onClick={() => {
                setPurchase(p.purchasePrice);
                setBuild(p.buildCost);
                setHold(p.holdMonths);
                setSale(p.salePrice);
              }}
              className="pill border-border text-muted hover:text-text hover:border-accent/40"
            >
              <span aria-hidden>{p.emoji}</span> {p.label}
            </button>
          ))}
        </div>

        <NumberInput label="Purchase price" value={purchasePrice} onChange={setPurchase} prefix="$" />
        <NumberInput label="Build cost" value={buildCost} onChange={setBuild} prefix="$" />
        <NumberInput
          label="Hold (months)"
          value={holdMonths}
          onChange={setHold}
          step={1}
        />
        <NumberInput label="Sale price" value={salePrice} onChange={setSale} prefix="$" />
        <NumberInput
          label="Variable rate"
          value={ratePct ?? 0}
          onChange={(v) => setRate(v)}
          step={0.01}
          suffix="%"
          hint={
            rates.data?.variableOoPct != null
              ? `latest: ${pct(rates.data.variableOoPct)} (RBA F5)`
              : "no rates ingested yet — defaults to 6%"
          }
        />
      </section>

      {/* Results */}
      <section className="panel p-5 lg:col-span-2 space-y-5">
        <h2 className="text-sm font-semibold uppercase tracking-wide text-muted">
          💰 Verdict
        </h2>

        {calc.isLoading && <div className="text-muted text-sm">Calculating…</div>}
        {calc.error && (
          <div className="text-bad text-sm">Error: {String(calc.error)}</div>
        )}

        {calc.data && (
          <>
            <VerdictCard
              verdict={calc.data.verdict}
              reason={calc.data.verdictReason}
            />

            <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
              <Stat
                label="Profit"
                value={aud(calc.data.profit)}
                tone={calc.data.profit >= 0 ? "good" : "bad"}
              />
              <Stat label="Margin" value={pct(calc.data.marginPct)} />
              <Stat
                label="Annualised"
                value={pct(calc.data.annualisedReturnPct)}
              />
              <Stat label="Total cost" value={aud(calc.data.totalCost)} />
              <Stat label="Interest cost" value={aud(calc.data.interestCost)} />
              <Stat
                label="Acquisition"
                value={aud(calc.data.acquisitionCost)}
              />
              <Stat label="Sale cost" value={aud(calc.data.saleCost)} />
              <Stat
                label="Breakeven sale"
                value={audCompact(calc.data.breakevenSalePrice)}
              />
              <Stat label="Rate used" value={pct(calc.data.ratePctUsed)} />
            </div>
          </>
        )}
      </section>
    </div>
  );
}

function VerdictCard({
  verdict,
  reason,
}: {
  verdict: Verdict;
  reason: string;
}) {
  const meta =
    verdict === Verdict.BULL
      ? {
          emoji: "🐂",
          label: "Bull",
          tone: "border-good/40 bg-good/10 text-good",
        }
      : verdict === Verdict.GRASSHOPPER
        ? {
            emoji: "🦗",
            label: "Grasshopper",
            tone: "border-bad/40 bg-bad/10 text-bad",
          }
        : {
            emoji: "🤔",
            label: "Marginal",
            tone: "border-warn/40 bg-warn/10 text-warn",
          };
  return (
    <div className={cx("rounded-2xl border p-5 flex items-start gap-4", meta.tone)}>
      <div className="text-4xl" aria-hidden>{meta.emoji}</div>
      <div>
        <div className="text-lg font-semibold">{meta.label}</div>
        <div className="text-sm opacity-90 mt-1">{reason}</div>
      </div>
    </div>
  );
}

function Stat({
  label,
  value,
  tone,
}: {
  label: string;
  value: string;
  tone?: "good" | "bad";
}) {
  return (
    <div className="rounded-xl bg-panel-2 border border-border/40 p-3">
      <div className="text-[11px] uppercase tracking-wide text-muted">{label}</div>
      <div
        className={cx(
          "font-mono num text-base mt-1",
          tone === "good" && "text-good",
          tone === "bad" && "text-bad",
        )}
      >
        {value}
      </div>
    </div>
  );
}

function NumberInput({
  label,
  value,
  onChange,
  prefix,
  suffix,
  step = 1000,
  hint,
}: {
  label: string;
  value: number;
  onChange: (v: number) => void;
  prefix?: string;
  suffix?: string;
  step?: number;
  hint?: string;
}) {
  return (
    <label className="block">
      <div className="text-xs text-muted mb-1">{label}</div>
      <div className="flex items-center bg-panel-2 border border-border/60 rounded-lg overflow-hidden focus-within:ring-1 focus-within:ring-accent">
        {prefix && <span className="pl-3 text-muted text-sm">{prefix}</span>}
        <input
          type="number"
          inputMode="decimal"
          step={step}
          value={Number.isFinite(value) ? value : 0}
          onChange={(e) => onChange(Number(e.target.value))}
          className="bg-transparent flex-1 px-3 py-2 font-mono num text-text outline-none [appearance:textfield] [&::-webkit-outer-spin-button]:appearance-none [&::-webkit-inner-spin-button]:appearance-none"
        />
        {suffix && <span className="pr-3 text-muted text-sm">{suffix}</span>}
      </div>
      {hint && <div className="text-[11px] text-muted mt-1">{hint}</div>}
    </label>
  );
}
