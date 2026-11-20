import { ALL_EVENTS } from "./mockData";
import type { EventFilters } from "./filters";

// Shared by the server page and API route. Never imported by client components.
export async function loadEvents(filters: EventFilters) {
  const delay = 150 + Math.floor(Math.random() * 400);
  await new Promise((resolve) => setTimeout(resolve, delay));

  const search = filters.search.toLowerCase();
  return ALL_EVENTS.filter((event) => {
    const matchesSeverity = filters.severity === "all" || event.severity === filters.severity;
    const matchesSearch = !search || event.title.toLowerCase().includes(search)
      || event.service.toLowerCase().includes(search) || event.id.toLowerCase().includes(search);
    return matchesSeverity && matchesSearch;
  });
}
