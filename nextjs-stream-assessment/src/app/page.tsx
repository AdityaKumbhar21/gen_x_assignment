import { Suspense } from "react";
import DashboardClient from "@/components/DashboardClient";
import { loadEvents } from "@/lib/events";
import { filterKey, normalizeFilters } from "@/lib/filters";

export default async function DashboardPage({ searchParams }: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = await searchParams;
  const first = (value: string | string[] | undefined) => Array.isArray(value) ? value[0] || "" : value || "";
  const filters = normalizeFilters(first(params.search), first(params.severity));
  const events = await loadEvents(filters);

  return (
    <main data-filter={filterKey(filters)} style={{ display: "grid", gridTemplateColumns: "1.2fr 1fr", height: "100vh", overflow: "hidden" }}>
      <Suspense fallback={<p>Loading dashboard...</p>}>
        <DashboardClient events={events} filterKey={filterKey(filters)} />
      </Suspense>
    </main>
  );
}
