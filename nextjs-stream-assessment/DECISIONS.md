# Frontend Architecture & Debugging Decisions

## 1. Root-Cause Diagnosis

- **Stream cross-contamination:** Every selection created another reader, but all readers appended into the page's summaryText. Neither the request nor individual state updates checked whether they still owned the selected session. Filter changes did not cancel streams. The baseline A → B → A reproduction displayed Event B text under Event A's heading.
- **Duplicate fetches and navigation:** Local filter state wrote into the URL in one effect, another effect copied the URL back into local state, and a third fetched on every keystroke. Requests lacked cancellation or stale-result guards. Development mount replay produced two identical event requests. With a real older `a` response delayed, an `auth` query displayed 500 rows instead of 100. Back/Forward eventually worked in the baseline run; persistent navigation failure was not claimed as reproduced.
- **Render cascade:** Token state lived in the page, causing the 500-row map and formatter to be reevaluated. The formatter performed 12 ms of synchronous work each time. The baseline page counter increased from 10 to 202 during overlapping streams. A render-time ref counter and time-dependent formatter checksum also made hydration output unstable.
- **Build and errors:** useSearchParams had no Suspense boundary, so production prerendering failed. Summary fetches did not check HTTP status, handle rejected reads, or clean up loading state after failure.

## 2. Server vs. Client Component Boundaries

`page.tsx` is now an async Server Component. It awaits the Next.js 15 searchParams prop, normalizes the search/severity values, loads matching events and renders the static grid shell. The root layout remains a Server Component. A scoped Suspense boundary surrounds the interactive client subtree.

The shared server loader in `src/lib/events.ts` is used directly by the page and by `/api/events`. Its original 150–550 ms simulated jitter and filtering behavior for supported filters are retained. The browser no longer competes with server loading through a separate event-fetch effect. Filter commits make normal App Router/RSC navigation requests; this is not a claim that navigation performs no network work.

Only plain event objects (string IDs, timestamps, service/severity/title and raw JSON strings) and a normalized filter-key string cross the boundary. Controllers, callbacks, generation refs and selection state are created in the client. Client files import the mock event type only; the generated server dataset is not imported into client code.

`DashboardClient` owns selection and cancellation identity. `Filters` owns temporary input drafts; the URL owns committed filters. `EventList` handles selection. `SummaryOutput` alone owns streaming text, progress and errors. `HeavyPayloadFormatter` is a separate memoized sibling receiving only rawPayload.

No state manager, data-fetching library or application dependency was added. The declared Next.js 15.0.3 / React 18.3.1 dependencies and lockfile are unchanged.

## 3. Stream Cancellation & Race-Condition Strategy

Each selection increments a generation counter, aborts the previous controller and mounts a summary leaf keyed by that generation. A → B → A therefore creates three distinct sessions; comparing event IDs alone would incorrectly allow the first A request to act as the third.

The coordinator accepts controller registration only from the current generation. The leaf additionally owns a local cancellation flag and controller. It checks ownership after asynchronous reads, inside text state updaters, and before setting error/loading state. An older request's catch/finally cannot change the newer session. Reader locks are released, intentional aborts are ignored, and genuine failures render an error and clear progress.

Typing immediately invalidates the selection and stream before the 300 ms search debounce fires. Severity changes, committed filter changes, Back/Forward and unmount also invalidate work. Cleanup handles development effect replay without disabling it. Development may start and immediately abort a first summary effect setup; no stale content or unhandled AbortError is permitted. Event loading has no such client effect or duplicate event API fetch.

The client checks response.ok, missing bodies and explicit HTTP 204. Chrome was observed exposing a non-null but empty ReadableStream for a 204 response. Network failure, HTTP 500 and no-content cases all show an error without leaving the streaming badge active or interpreting the error body as analysis text.

The mock summary still uses the original fixed report and 250 ms / 45 ms timing. Its producer now handles both request abort and stream cancellation, clears/wakes timers, removes its abort listener and avoids enqueuing or closing an already cancelled controller. This remains a self-contained mock; it does not call FastAPI or a model.

## 4. State Synchronization & Render Performance

The input updates immediately in local draft state. A 300 ms timer commits the normalized query with router.push; severity commits immediately and cancels any pending timer. Navigations are skipped when the same effective filter is already committed or in flight. Existing unrelated query parameters are preserved.

URL-to-input synchronization is read-only: it never pushes a URL. Pending draft/desired-key refs prevent an older own navigation from replacing newer typing. Back/Forward clears pending debounce and navigation intent, restores the input values and invalidates streaming. This yields meaningful history entries per committed filter, rather than per keystroke. Unsupported severities are interpreted as all; comparisons use trimmed search and case-insensitive matching.

The App Router owns server-loaded event results, so there is no client events response handler that can overwrite the latest dataset. Tests separately delay the older navigation until before a newer debounce fires and until after the newer results have arrived.

Streaming state stays in SummaryOutput. Memoized list/filter/payload siblings receive stable callbacks and no token text. The original formatter's simulated 12 ms workload is retained inside useMemo keyed by rawPayload; the rendered checksum is deterministic from the payload, not elapsed loop iterations. Memoization supplements the state boundary rather than replacing it.

Commit diagnostics use a small native hook that updates `data-render-count` after a component commits. They do not mutate rendered text or React state. Initial development counts can include effect replay; tests compare deltas only after selection settles. In both production and development runs, streaming produced **55 additional summary commits and zero additional dashboard, list, filter or formatter commits**.

### Acceptance verification

- `npm run build` passes, including compilation, type checks and route generation; `/` is server-rendered on demand.
- `tests/acceptance.cjs` runs **13 strict browser checks in production and the same 13 in development**.
- Initial HTML contains 500 server-loaded rows. Typing `auth` produces exactly one RSC navigation, zero competing `/api/events` fetches and 100 matching rows.
- Back/Forward restores URL, draft, severity and row counts; Back cancels a pending search timer. Deep links initialize filtered data and unrelated query parameters survive changes.
- Controlled older responses cannot erase newer draft input or replace the latest results.
- A → B and A → B → A complete without stale text. Severity changes and typing abort and clear active summaries. Development replay produces no unhandled aborts.
- Network failure, HTTP 500 and no-content responses show error UI and clear loading. No hydration errors/warnings or stream-controller errors were observed.
- The original README acceptance criteria and constraints are retained. Package/lock files, mock event dataset and root layout remain unchanged.

Results are recorded in `.repro/acceptance-production.json`, `.repro/acceptance-development.json` and `.repro/implementation-results.json`. The browser driver uses existing external Playwright tooling and Chrome; neither is added to the application dependencies.
