"use client";

import { useEffect, useRef } from "react";

// Commit diagnostics, kept outside rendered text and React state. Development
// effect replay can increase the initial count; compare deltas during streaming.
export function useRenderCounter<T extends HTMLElement>() {
  const element = useRef<T>(null);
  const count = useRef(0);
  useEffect(() => {
    count.current += 1;
    if (element.current) element.current.dataset.renderCount = String(count.current);
  });
  return element;
}
