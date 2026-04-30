type Props = {
  emoji?: string;
  title: string;
  hint?: string;
};

export default function EmptyState({ emoji = "🫥", title, hint }: Props) {
  return (
    <div className="flex flex-col items-center justify-center py-16 text-center">
      <div className="text-5xl mb-4" aria-hidden>
        {emoji}
      </div>
      <div className="text-text font-medium">{title}</div>
      {hint && <div className="text-muted text-sm mt-1 max-w-md">{hint}</div>}
    </div>
  );
}
