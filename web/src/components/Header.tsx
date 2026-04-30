import { NavLink } from "react-router-dom";
import { cx } from "../lib/format";

const NAV = [
  { to: "/", label: "Projects", emoji: "📊" },
  { to: "/applications", label: "Applications", emoji: "📁" },
  { to: "/map", label: "Map", emoji: "🗺️" },
  { to: "/trends", label: "Trends", emoji: "📈" },
  { to: "/calculator", label: "Calculator", emoji: "🧮" },
  { to: "/about", label: "About", emoji: "ℹ️" },
];

export default function Header() {
  return (
    <header className="border-b border-border/60 bg-bg/80 backdrop-blur sticky top-0 z-20">
      <div className="max-w-[1600px] mx-auto px-6 h-14 flex items-center gap-6">
        <NavLink to="/" className="flex items-center gap-2">
          <span className="text-xl" aria-hidden>🏘️</span>
          <span className="font-semibold tracking-tight">listo</span>
        </NavLink>

        <nav className="flex items-center gap-1 text-sm">
          {NAV.map((n) => (
            <NavLink
              key={n.to}
              to={n.to}
              end={n.to === "/"}
              className={({ isActive }) =>
                cx(
                  "px-3 py-1.5 rounded-lg transition-colors flex items-center gap-1.5",
                  isActive
                    ? "bg-white/[0.06] text-text"
                    : "text-muted hover:text-text hover:bg-white/[0.03]",
                )
              }
            >
              <span aria-hidden>{n.emoji}</span>
              <span className="hidden md:inline">{n.label}</span>
            </NavLink>
          ))}
        </nav>

        <div className="ml-auto flex items-center gap-2 text-xs text-muted">
          <select
            className="bg-panel border border-border/60 rounded-lg px-2 py-1 text-text"
            defaultValue="cogc"
          >
            <option value="cogc">🏖️ City of Gold Coast</option>
            <option value="newcastle" disabled>
              ⛵ Newcastle (soon)
            </option>
          </select>
        </div>
      </div>
    </header>
  );
}
