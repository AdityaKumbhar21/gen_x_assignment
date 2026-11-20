# Frontend reproduction

## Latest reproduction: 2026-11-20

The application, package files, README, decision template and configuration were hash-checked and left unchanged. The backend is not required. The unchanged frontend is running at http://127.0.0.1:3100.

Evidence is saved in `frontend-review-results.json`, `frontend-review-browser.log`, `frontend-review-dev.log`, `frontend-review-build.log`, `frontend-review-stream-bleed.png` and `frontend-review-stale-results.png`. The headless-browser observation script is `reproduce-frontend.cjs`. Browser tooling was installed in a separate temporary directory, not in this project's dependencies. Running the script requires `playwright-core` on `NODE_PATH` and `CHROME_PATH` pointing to a Chrome executable; `REPRO_URL` defaults to the address above.

| Check | Observed behavior | Cause in current source |
|---|---|---|
| Rapid A → B → A switching | Heading returned to evt_0001 but summary contained evt_0002/webhook-worker text; all three streams completed | No cancellation or generation guard; all readers append to one summary state |
| Filter change during streaming | Switching to Info removed critical Event A from the list, but its summary kept streaming from 221 to 564 characters | Filter effects do not cancel/reset selection or streaming work |
| Initial event loading | Two identical initial `/api/events` requests | Client effect has no cleanup/deduplication under development mount behavior |
| Search typing | `a`, `au`, `aut`, and `auth` each generated an event request | No debounce; local state and URL effects feed each other |
| Out-of-order event responses | Search stayed `auth` but the table displayed 500 events instead of the correct 100 | No stale-response guard or cancellation; an older response overwrites events |
| Render isolation | Page counter increased from 10 to 202 across the overlapping streams | Summary state is in the page; it reevaluates the list and expensive formatter |
| Failed summary fetch | Unhandled `Failed to fetch`, with the streaming indicator left on | No error handling or finally cleanup |
| Hydration | Browser reported server/client render-counter text mismatch (1 versus 2) | The render-time ref counter is not stable across server and development client renders; the formatter's time-dependent checksum is another unstable render value |
| Production build | Exit 1 during prerendering `/` | `useSearchParams()` has no Suspense boundary |

The stale-response case uses controlled latency: the real older `search=a` response is delayed by 1,000 ms without changing its data. This makes the existing race deterministic. The failed-fetch case deliberately aborts the request. Stream mixing, render cascade, duplicate mount requests and per-keystroke fetching were reproduced without response modification. Render counts are development observations, not production timing measurements.

Back reached `?search=aut` with input `aut`, and Forward returned to `?search=auth` with input `auth`. Persistent Back/Forward failure was not reproduced. Per-keystroke history entries and the two-way synchronization remain undesirable; do not describe a permanent navigation failure as an observed result.

Source review confirms `layout.tsx` is already a Server Component, while `page.tsx` remains entirely client-side with empty initial event state and effect-driven loading. The API routes read generated in-memory mock events, and summaries are fixed reports streamed as plain text; they do not call FastAPI or a model. The original README requires native React/Next.js state and cancellation primitives and completion of `DECISIONS.md`, which is still a template. No application fixes have been applied.

## Earlier reproduction record

Run these commands from `nextjs-stream-assessment`. npm installs dependencies in this folder's `node_modules`; versions are recorded in `package-lock.json`. FastAPI and its database are not used.

```sh
npm ci
npm run dev -- --hostname 127.0.0.1 --port 3100
```

Open http://127.0.0.1:3100. The development server was left running on that address after verification. Its output is in `dev.log`.

## Stream contamination and render cascade

1. Set severity to Warning.
2. Click `[vector-indexer] Retry queue saturation #4`.
3. Before its summary finishes, click `[webhook-worker] Retry queue saturation #7`.
4. Observe an evt_0007 heading and raw payload while vector-indexer and webhook-worker report words interleave in the summary.
5. Observe the page render counter increasing during streaming, even without changing filters.

Recorded evidence: `stream-evidence.json` and `stream-bleed.jpg`. The page render counter rose from 170 during the two active streams to 358 after completion. Development counters include development-mode rendering overhead; this is evidence of parent updates, not a production render benchmark.

## Filters

1. Clear search and set All Severities.
2. Type `auth` normally.
3. The server log records separate navigations and API requests for `a`, `au`, `aut`, and `auth`, demonstrating missing debouncing. Responses also finished out of order: `aut` finished before `au`.
4. Back moves to `aut` and Forward moves to `auth`; URL and input briefly disagree while effects synchronize.

This session also recorded two identical initial `/api/events?` requests. Persistent failure of Back/Forward was not reproduced; both eventually reached the expected values. See `history-evidence.json` and `dev.log`.

## Production build failure

```sh
npm run build
```

The build compiles and type-checks, then fails prerendering `/` with `useSearchParams() should be wrapped in a suspense boundary`. See `build.log`.

Next.js generated `next-env.d.ts` and added `target: ES2017` to `tsconfig.json` during setup. No application fixes or dependency upgrades were made.
