"use client";

import { memo, useEffect, useRef, useState, useTransition } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { filterKey, normalizeFilters } from "@/lib/filters";
import { useRenderCounter } from "./useRenderCounter";

export const Filters = memo(function Filters({ invalidate }: { invalidate: () => void }) {
  const router = useRouter();
  const params = useSearchParams();
  const committed = normalizeFilters(params.get("search") || "", params.get("severity") || "all");
  const committedKey = filterKey(committed);
  const [draft, setDraft] = useState(committed.search);
  const [severity, setSeverity] = useState(committed.severity);
  const [pending, startTransition] = useTransition();
  const draftRef = useRef(draft);
  const severityRef = useRef(severity);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const desiredKey = useRef<string | null>(null);
  const renders = useRenderCounter<HTMLDivElement>();

  function clearTimer() {
    if (timer.current !== null) {
      clearTimeout(timer.current);
      timer.current = null;
    }
  }

  // URL -> input only. An older own navigation must not erase newer typing.
  useEffect(() => {
    if (timer.current !== null || (desiredKey.current !== null && desiredKey.current !== committedKey)) return;
    desiredKey.current = null;
    draftRef.current = committed.search;
    severityRef.current = committed.severity;
    setDraft(committed.search);
    setSeverity(committed.severity);
  }, [committedKey, committed.search, committed.severity]);

  useEffect(() => {
    function restoreHistory() {
      clearTimer();
      desiredKey.current = null;
      const query = new URLSearchParams(window.location.search);
      const filters = normalizeFilters(query.get("search") || "", query.get("severity") || "all");
      draftRef.current = filters.search;
      severityRef.current = filters.severity;
      setDraft(filters.search);
      setSeverity(filters.severity);
      invalidate();
    }
    window.addEventListener("popstate", restoreHistory);
    return () => {
      clearTimer();
      window.removeEventListener("popstate", restoreHistory);
    };
  }, [invalidate]);

  function commit(search: string, nextSeverity: string) {
    const next = normalizeFilters(search, nextSeverity);
    const key = filterKey(next);
    if (key === desiredKey.current || (key === committedKey && desiredKey.current === null)) return;
    desiredKey.current = key;
    const query = new URLSearchParams(params.toString());
    if (next.search) query.set("search", next.search);
    else query.delete("search");
    if (next.severity !== "all") query.set("severity", next.severity);
    else query.delete("severity");
    startTransition(() => router.push(`/?${query.toString()}`, { scroll: false }));
  }

  return (
    <div ref={renders} data-testid="filter-bar" data-render-count="0" style={{ padding: "16px", borderBottom: "1px solid #1e293b", display: "flex", gap: "12px", alignItems: "center" }}>
      <input
        aria-label="Search events"
        value={draft}
        onChange={(event) => {
          const value = event.target.value;
          draftRef.current = value;
          setDraft(value);
          invalidate();
          clearTimer();
          timer.current = setTimeout(() => {
            timer.current = null;
            commit(draftRef.current, severityRef.current);
          }, 300);
        }}
        placeholder="Filter by service, title, or ID..."
        style={{ flex: 1, minWidth: 0, padding: "8px 12px", borderRadius: "6px", border: "1px solid #334155", background: "#1e293b", color: "#f8fafc" }}
      />
      <select
        aria-label="Severity"
        value={severity}
        onChange={(event) => {
          const value = normalizeFilters("", event.target.value).severity;
          severityRef.current = value;
          setSeverity(value);
          invalidate();
          clearTimer();
          commit(draftRef.current, value);
        }}
        style={{ padding: "8px 12px", borderRadius: "6px", border: "1px solid #334155", background: "#1e293b", color: "#f8fafc" }}
      >
        <option value="all">All Severities</option>
        <option value="info">Info</option>
        <option value="warning">Warning</option>
        <option value="critical">Critical</option>
      </select>
      {pending && <span role="status" style={{ fontSize: "12px", color: "#94a3b8" }}>Loading events...</span>}
    </div>
  );
});
