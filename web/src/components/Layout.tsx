import { ReactNode } from "react";
import Header from "./Header";
import BottomBar from "./BottomBar";

export default function Layout({ children }: { children: ReactNode }) {
  return (
    <div className="min-h-full flex flex-col">
      <Header />
      <main className="flex-1 max-w-[1600px] w-full mx-auto px-6 py-6 pb-12">
        {children}
      </main>
      <BottomBar />
    </div>
  );
}
