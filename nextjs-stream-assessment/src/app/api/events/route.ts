import { NextRequest, NextResponse } from "next/server";
import { loadEvents } from "@/lib/events";
import { normalizeFilters } from "@/lib/filters";

export async function GET(request: NextRequest) {
  const params = new URL(request.url).searchParams;
  const filters = normalizeFilters(params.get("search") || "", params.get("severity") || "all");
  const events = await loadEvents(filters);
  return NextResponse.json({ events, total: events.length });
}
