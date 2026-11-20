"use client";

import { memo, useMemo } from "react";
import { useRenderCounter } from "./useRenderCounter";

// Static payload work is independent of the streaming summary. Keep the original
// simulated 12 ms cost, but only recompute it when the raw payload changes.
export const HeavyPayloadFormatter = memo(function HeavyPayloadFormatter({ rawPayload }: { rawPayload: string }) {
  const renders = useRenderCounter<HTMLDivElement>();
  const formatted = useMemo(() => {
    const until = performance.now() + 12;
    while (performance.now() < until) { /* simulated expensive formatting */ }
    let checksum = 0;
    for (let index = 0; index < rawPayload.length; index += 1) {
      checksum = (checksum * 31 + rawPayload.charCodeAt(index)) >>> 0;
    }
    return { text: rawPayload, checksum };
  }, [rawPayload]);

  return (
    <div ref={renders} data-testid="payload" data-render-count="0" data-checksum={formatted.checksum}>
      <details open>
        <summary style={{ cursor: "pointer", color: "#94a3b8", fontSize: "13px" }}>Raw Telemetry Payload</summary>
        <pre style={{ background: "#020617", padding: "12px", borderRadius: "6px", fontSize: "12px", overflowX: "auto" }}>{formatted.text}</pre>
      </details>
    </div>
  );
});
