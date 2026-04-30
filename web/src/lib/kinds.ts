import { DaKind } from "../gen/listo_pb";

// Single source of truth for how each DaKind is rendered in the UI. Used by
// the kind badge, map dot colour, and table filters.
export type KindMeta = {
  emoji: string;
  label: string;
  short: string;
  /** tailwind text + bg classes already paired */
  textClass: string;
  bgClass: string;
  borderClass: string;
  /** css colour for non-tailwind contexts (Recharts, Leaflet) */
  hex: string;
  description: string;
};

export const KIND_META: Record<DaKind, KindMeta> = {
  [DaKind.UNSPECIFIED]: {
    emoji: "❓",
    label: "Unknown",
    short: "?",
    textClass: "text-muted",
    bgClass: "bg-muted/10",
    borderClass: "border-muted/40",
    hex: "#71717a",
    description: "Unclassified DA",
  },
  [DaKind.GRANNY]: {
    emoji: "🍏",
    label: "Flat",
    short: "Flat",
    textClass: "text-granny",
    bgClass: "bg-granny/10",
    borderClass: "border-granny/40",
    hex: "#a3e635",
    description: "Secondary dwelling on existing lot",
  },
  [DaKind.DUPLEX]: {
    emoji: "🏘️",
    label: "Duplex",
    short: "Duplex",
    textClass: "text-duplex",
    bgClass: "bg-duplex/10",
    borderClass: "border-duplex/40",
    hex: "#38bdf8",
    description: "Dual occupancy on one lot",
  },
  [DaKind.BIG_DEV]: {
    emoji: "🏗️",
    label: "Big development",
    short: "BigDev",
    textClass: "text-bigdev",
    bgClass: "bg-bigdev/10",
    borderClass: "border-bigdev/40",
    hex: "#fb7185",
    description: "Multi-unit / 3+ dwellings",
  },
  [DaKind.OTHER]: {
    emoji: "🏠",
    label: "Other",
    short: "Other",
    textClass: "text-other",
    bgClass: "bg-other/10",
    borderClass: "border-other/40",
    hex: "#71717a",
    description: "Single house / OPW / minor change",
  },
};

export const KIND_FILTER_OPTIONS: { value: string; label: string; emoji: string }[] = [
  { value: "all", label: "All", emoji: "📊" },
  { value: "granny", label: "Flat", emoji: "🍏" },
  { value: "duplex", label: "Duplex", emoji: "🏘️" },
  { value: "big_dev", label: "Big dev", emoji: "🏗️" },
  { value: "other", label: "Other", emoji: "🏠" },
];
