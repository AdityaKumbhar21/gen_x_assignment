"use client";

import { memo } from "react";
import type { SystemEvent } from "@/lib/mockData";
import { useRenderCounter } from "./useRenderCounter";

export const EventList = memo(function EventList({ events, selectedId, onSelect }: {
  events: SystemEvent[];
  selectedId: string | null;
  onSelect: (event: SystemEvent) => void;
}) {
  const renders = useRenderCounter<HTMLDivElement>();
  return (
    <div ref={renders} data-testid="event-list" data-render-count="0" style={{ overflowY: "auto", flex: 1 }}>
      {events.map((event) => (
        <div
          key={event.id}
          data-testid={`event-${event.id}`}
          onClick={() => onSelect(event)}
          style={{ padding: "12px 16px", borderBottom: "1px solid #1e293b", cursor: "pointer", background: selectedId === event.id ? "#1e293b" : "transparent", display: "flex", justifyContent: "space-between", alignItems: "center" }}
        >
          <div>
            <div style={{ fontSize: "14px", fontWeight: 500 }}>{event.title}</div>
            <div style={{ fontSize: "12px", color: "#64748b" }}>{event.id} · {event.timestamp}</div>
          </div>
          <span style={{ fontSize: "11px", padding: "2px 8px", borderRadius: "999px", background: event.severity === "critical" ? "#7f1d1d" : event.severity === "warning" ? "#78350f" : "#1e3a8a", color: "#f8fafc" }}>
            {event.severity}
          </span>
        </div>
      ))}
      {events.length === 0 && <p style={{ padding: "16px", color: "#94a3b8" }}>No matching events.</p>}
    </div>
  );
});
