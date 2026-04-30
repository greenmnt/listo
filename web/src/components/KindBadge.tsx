import { DaKind } from "../gen/listo_pb";
import { KIND_META } from "../lib/kinds";
import { cx } from "../lib/format";

type Props = {
  kind: DaKind;
  size?: "sm" | "md";
  showLabel?: boolean;
};

export default function KindBadge({ kind, size = "sm", showLabel = true }: Props) {
  const m = KIND_META[kind] ?? KIND_META[DaKind.OTHER];
  return (
    <span
      className={cx(
        "pill",
        m.textClass,
        m.bgClass,
        m.borderClass,
        size === "md" && "text-sm px-2.5 py-1",
      )}
      title={m.description}
    >
      <span aria-hidden>{m.emoji}</span>
      {showLabel && <span>{m.short}</span>}
    </span>
  );
}
