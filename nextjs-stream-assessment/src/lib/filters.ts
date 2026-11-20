import type { Severity } from "./mockData";

export interface EventFilters {
  search: string;
  severity: Severity | "all";
}

export function normalizeFilters(search: string, severity: string): EventFilters {
  return {
    search: search.trim(),
    severity: ["info", "warning", "critical"].includes(severity) ? severity as Severity : "all",
  };
}

export function filterKey(filters: EventFilters): string {
  const params = new URLSearchParams();
  if (filters.search) params.set("search", filters.search);
  if (filters.severity !== "all") params.set("severity", filters.severity);
  return params.toString();
}
