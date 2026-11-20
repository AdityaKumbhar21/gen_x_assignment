# Frontend Technical Assessment — Event Log & Live AI Summary Workspace

## Context
You are taking over an internal operations dashboard built with **Next.js (App Router)**, **React**, and **TypeScript**. The workspace displays a filterable list of 500 system events on the left and a live streaming AI incident-summary drawer on the right.

Users are reporting three major issues in production:
1. **Stream Cross-Contamination:** Clicking Event A starts a streaming summary; clicking Event B before Event A finishes causes leftover chunks from Event A to bleed into Event B's summary panel.
2. **Sluggish Typing & Broken Back Button:** Typing in the search box or toggling severity filters triggers duplicate API calls and breaks browser Back/Forward navigation.
3. **UI Jank During Streaming:** While a summary is streaming tokens, typing or scrolling feels sluggish because the entire 500-row table and heavy syntax formatter re-render on every single streamed chunk.

## Setup
```bash
npm install
npm run dev
```
Open `http://localhost:3000` in your browser.

## Acceptance Criteria
1. **Zero Stream Bleed:** Rapidly switching between events (or changing filters while a stream is active) must cleanly abort in-flight streams and never append stale chunks to the newly selected event.
2. **URL-Driven Filter State:** `search` and `severity` filters must stay synchronized with URL query parameters (`?search=...&severity=...`) with **no duplicate fetches** on mount/change, proper debouncing on search input, and working browser Back/Forward navigation.
3. **Render Isolation:** Streaming token updates must **only** re-render the leaf summary output component—never the 500-row event list or filter bar (verifiable via React DevTools Profiler or the built-in render counters).
4. **Server/Client Boundary Hygiene:** Move initial data loading and shell layout to React Server Components where appropriate instead of making the entire page `'use client'`.

## Constraints
- **No external state or data-fetching libraries:** Do not install Zustand, Redux, Jotai, TanStack Query, or SWR. Solve state and cancellation using native React and Next.js App Router primitives.
- Complete `DECISIONS.md` before submitting.

## Implemented behavior

The page and static grid shell are Server Components. Initial events and URL-driven updates use one shared server loader, retaining the original simulated jitter. The client owns only interactive filter drafts and selection; streaming text, progress and errors are isolated in the summary leaf. Filters are debounced by 300 ms, preserve query parameters, and create history entries per committed change.

Each selection has a unique request generation and AbortController. Switching events, typing, severity changes, history navigation and unmount invalidate old streams. The formatter remains expensive but only recomputes when its raw payload changes; diagnostic `data-render-count` attributes can be inspected to verify token-update isolation without unstable server-rendered counters.

## Verification

```sh
npm ci
npm run build
npm run start -- --hostname 127.0.0.1 --port 3100
```

In another terminal, from this folder, use the existing external browser-test tools:

```sh
NODE_PATH="/path/to/browser-tools/node_modules" \
CHROME_PATH="/path/to/chrome-executable" \
TEST_MODE=production TEST_OUTPUT=.repro/acceptance-production.json \
node tests/acceptance.cjs
```

The browser-tools directory must provide `playwright-core`; it is deliberately not an application dependency. This session reused the isolated tooling from the reproduction work. `REPRO_URL` defaults to `http://127.0.0.1:3100` and can be overridden. The driver writes its results under `.repro/`, which should exist before running it.

Stop the production server before running the same checks against `npm run dev -- --hostname 127.0.0.1 --port 3100`, with `TEST_MODE=development` and `TEST_OUTPUT=.repro/acceptance-development.json`. Production and development must not concurrently write the same `.next` directory. Run `npm run build` again before starting production after a development session.

The verification runs passed 13 browser checks in each mode, including controlled navigation races, Back/Forward, cancellation, error handling and hydration. Server-rendered HTML contained 500 initial events; one committed `auth` search produced one navigation and 100 rows with no duplicate events API fetch. During streaming, the summary had 55 additional commits while the dashboard, list, filters and payload formatter had zero. See `DECISIONS.md` and `.repro/implementation-results.json` for details. The acceptance criteria above were not changed.
