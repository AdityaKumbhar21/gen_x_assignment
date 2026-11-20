"use client";

import { useEffect, useState } from "react";
import type { SystemEvent } from "@/lib/mockData";
import { useRenderCounter } from "./useRenderCounter";

export function SummaryOutput({ event, generation, isCurrent, registerController }: {
  event: SystemEvent;
  generation: number;
  isCurrent: (generation: number) => boolean;
  registerController: (generation: number, controller: AbortController) => void;
}) {
  const [text, setText] = useState("");
  const [streaming, setStreaming] = useState(true);
  const [error, setError] = useState("");
  const renders = useRenderCounter<HTMLElement>();

  useEffect(() => {
    const controller = new AbortController();
    registerController(generation, controller);
    let cancelled = false;
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    const ownsRequest = () => !cancelled && !controller.signal.aborted && isCurrent(generation);

    async function readSummary() {
      try {
        if (!ownsRequest()) return;
        const response = await fetch(`/api/stream-summary?eventId=${encodeURIComponent(event.id)}`, { signal: controller.signal });
        if (!response.ok) throw new Error(`Summary request failed (${response.status}).`);
        // Some browsers expose an empty ReadableStream even for HTTP 204.
        if (response.status === 204 || !response.body) throw new Error("Summary response has no stream body.");
        reader = response.body.getReader();
        const decoder = new TextDecoder();
        while (ownsRequest()) {
          const { done, value } = await reader.read();
          if (done) break;
          const chunk = decoder.decode(value, { stream: true });
          setText((previous) => ownsRequest() ? previous + chunk : previous);
        }
        const tail = decoder.decode();
        if (tail) setText((previous) => ownsRequest() ? previous + tail : previous);
      } catch (cause) {
        if (ownsRequest() && !(cause instanceof DOMException && cause.name === "AbortError")) {
          setError(cause instanceof Error ? cause.message : "Unable to load summary.");
        }
      } finally {
        reader?.releaseLock();
        if (ownsRequest()) setStreaming(false);
      }
    }
    void readSummary();
    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [event.id, generation, isCurrent, registerController]);

  return (
    <section ref={renders} data-testid="summary-output" data-render-count="0">
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "16px" }}>
        <h2 style={{ margin: 0, fontSize: "18px" }}>Analysis: {event.id}</h2>
        {streaming && <span data-testid="streaming" style={{ fontSize: "12px", color: "#38bdf8" }}>Streaming tokens...</span>}
      </div>
      <div data-testid="summary" style={{ background: "#1e293b", padding: "14px", borderRadius: "8px", border: "1px solid #334155", whiteSpace: "pre-wrap", lineHeight: 1.55, minHeight: "180px" }}>{text}</div>
      {error && <p role="alert" style={{ color: "#fca5a5" }}>{error}</p>}
    </section>
  );
}
