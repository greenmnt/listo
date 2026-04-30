/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        bg: "#0b0c10",
        panel: "#14161d",
        "panel-2": "#191b23",
        border: "#26282f",
        muted: "#8a8f9a",
        text: "#fcfcfc",
        good: "#34d399",
        bad: "#fb7185",
        warn: "#fbbf24",
        accent: "#5b6fff",
        granny: "#a3e635",
        duplex: "#38bdf8",
        bigdev: "#fb7185",
        other: "#71717a",
      },
      fontFamily: {
        sans: [
          "Geist",
          "ui-sans-serif",
          "system-ui",
          "Inter",
          "-apple-system",
          "Segoe UI",
          "Roboto",
          "sans-serif",
        ],
        mono: [
          "Geist Mono",
          "JetBrains Mono",
          "ui-monospace",
          "SFMono-Regular",
          "Menlo",
          "Consolas",
          "monospace",
        ],
      },
    },
  },
  plugins: [],
};
