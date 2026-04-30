import { cx } from "../lib/format";

const APPROVED = /(approved|granted|approved with conditions)/i;
const REFUSED = /(refused|withdrawn|lapsed|rejected)/i;
const PENDING = /(under assessment|with referral|in progress|lodged|notification|info(rmation)? and referral|approval)/i;

type Tone = "good" | "bad" | "warn" | "neutral";

function toneOf(s?: string | null): Tone {
  if (!s) return "neutral";
  if (APPROVED.test(s)) return "good";
  if (REFUSED.test(s)) return "bad";
  if (PENDING.test(s)) return "warn";
  return "neutral";
}

const TONE_CLASS: Record<Tone, string> = {
  good: "text-good bg-good/10 border-good/40",
  bad: "text-bad bg-bad/10 border-bad/40",
  warn: "text-warn bg-warn/10 border-warn/40",
  neutral: "text-muted bg-muted/10 border-muted/30",
};

export default function StatusPill({ status }: { status?: string | null }) {
  const tone = toneOf(status);
  return (
    <span className={cx("pill", TONE_CLASS[tone])} title={status ?? "—"}>
      {status ?? "—"}
    </span>
  );
}
