"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import type { SystemEvent } from "@/lib/mockData";
import { EventList } from "./EventList";
import { Filters } from "./Filters";
import { HeavyPayloadFormatter } from "./HeavyPayloadFormatter";
import { SummaryOutput } from "./SummaryOutput";
import { useRenderCounter } from "./useRenderCounter";

export default function DashboardClient({ events, filterKey }: { events: SystemEvent[]; filterKey: string }) {
  const [selection, setSelection] = useState<{ event: SystemEvent; generation: number } | null>(null);
  const generation = useRef(0);
  const controller = useRef<AbortController | null>(null);
  const renders = useRenderCounter<HTMLDivElement>();

  const invalidate = useCallback(() => {
    generation.current += 1;
    controller.current?.abort();
    setSelection(null);
  }, []);
  const select = useCallback((event: SystemEvent) => {
    controller.current?.abort();
    generation.current += 1;
    setSelection({ event, generation: generation.current });
  }, []);
  const isCurrent = useCallback((value: number) => generation.current === value, []);
  const registerController = useCallback((value: number, next: AbortController) => {
    if (value === generation.current) controller.current = next;
    else next.abort();
  }, []);

  useEffect(() => { invalidate(); }, [filterKey, invalidate]);
  useEffect(() => () => {
    generation.current += 1;
    controller.current?.abort();
  }, []);

  return (
    <>
      <div ref={renders} data-testid="dashboard" data-render-count="0" style={{ borderRight: "1px solid #1e293b", display: "flex", flexDirection: "column", overflow: "hidden" }}>
        <Filters invalidate={invalidate} />
        <EventList events={events} selectedId={selection?.event.id || null} onSelect={select} />
      </div>
      <div style={{ padding: "20px", overflowY: "auto", display: "flex", flexDirection: "column", gap: "12px" }}>
        {selection ? (
          <SummaryOutput key={selection.generation} event={selection.event} generation={selection.generation} isCurrent={isCurrent} registerController={registerController} />
        ) : (
          <>
            <h2 style={{ margin: 0, fontSize: "18px" }}>Incident Summary</h2>
            <div style={{ background: "#1e293b", padding: "14px", borderRadius: "8px", border: "1px solid #334155", minHeight: "180px", lineHeight: 1.55 }}>Select an event to generate an AI incident analysis...</div>
          </>
        )}
        <HeavyPayloadFormatter rawPayload={selection?.event.rawPayload || "{}"} />
      </div>
    </>
  );
}
