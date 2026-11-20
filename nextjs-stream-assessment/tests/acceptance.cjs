/* Browser acceptance checks for the unchanged README requirements.
   Uses existing external browser tooling through NODE_PATH, not application dependencies.
   Start npm run start (production) or npm run dev, then run this script.
   Set REPRO_URL, TEST_MODE and TEST_OUTPUT as needed. */
const { chromium } = require('playwright-core');
const fs = require('node:fs/promises');
const assert = require('node:assert/strict');
const baseURL = process.env.REPRO_URL || 'http://127.0.0.1:3100';
const delay = ms => new Promise(r => setTimeout(r, ms));
const report = { mode: process.env.TEST_MODE || 'production', baseURL, checks: [] };
async function until(fn, ms = 10000) { const end = Date.now() + ms; while (Date.now() < end) {
    if (await fn())
        return;
    await delay(25);
} throw Error('Verification condition timed out'); }
async function state(page) { return page.evaluate(() => ({ url: location.href, search: document.querySelector('input')?.value, severity: document.querySelector('select')?.value, rows: document.querySelectorAll('[data-testid^="event-evt_"]').length, summary: document.querySelector('[data-testid="summary"]')?.textContent || '', streaming: !!document.querySelector('[data-testid="streaming"]'), commits: Object.fromEntries(Object.entries({ dashboard: "dashboard", filters: "filter-bar", list: "event-list", payload: "payload", summary: "summary-output" }).map(([name, id]) => [name, Number(document.querySelector(`[data-testid="${id}"]`)?.getAttribute("data-render-count") || 0)])) })); }
async function session(browser) { const context = await browser.newContext({ baseURL }); const page = await context.newPage(); const requests = [], errors = []; page.on('request', r => requests.push({ url: r.url(), rsc: r.headers()['rsc'] === '1' })); page.on('pageerror', e => errors.push(e.message)); page.on('console', message => { if (message.type() === 'error' && /hydration|did not match|server rendered HTML|already closed/i.test(message.text()))
    errors.push(message.text()); }); await page.goto(baseURL); await page.getByTestId('event-evt_0001').waitFor(); await delay(100); return { context, page, requests, errors }; }
function record(name, data) { report.checks.push({ name, passed: true, ...data }); console.log(name, JSON.stringify(data)); }
async function main() {
    const browser = await chromium.launch({ headless: true, executablePath: process.env.CHROME_PATH });
    try {
        const html = await (await fetch(baseURL)).text();
        assert.equal((html.match(/data-testid="event-evt_/g) || []).length, 500);
        record('Initial HTML contains 500 server-loaded rows', {});
        {
            const s = await session(browser);
            assert.equal(s.requests.filter(r => r.rsc).length, 0);
            const before = (await s.page.evaluate(() => history.length));
            await s.page.getByRole('textbox').pressSequentially('auth', { delay: 50 });
            await until(async () => { const p = await state(s.page); return new URL(p.url).searchParams.get('search') === 'auth' && p.rows === 100; });
            await delay(100);
            const rsc = s.requests.filter(r => r.rsc && new URL(r.url).searchParams.get('search') === 'auth');
            assert.equal(rsc.length, 1);
            assert.equal(s.requests.filter(r => r.url.includes('/api/events')).length, 0);
            assert.equal(await s.page.evaluate(() => history.length), before + 1);
            assert.deepEqual(s.errors, []);
            record('Debounced URL-driven loading without duplicate API fetch', { rscRequests: rsc.length, apiEventRequests: 0, rows: (await state(s.page)).rows, hydrationErrors: s.errors.length });
            await s.page.getByRole('combobox').selectOption('critical');
            await until(() => new URL(s.page.url()).searchParams.get('severity') === 'critical');
            await s.page.getByRole('textbox').fill('billing');
            await s.page.goBack();
            await delay(800);
            let p = await state(s.page);
            assert.equal(new URL(p.url).searchParams.get('search'), 'auth');
            assert.equal(new URL(p.url).searchParams.get('severity'), null);
            assert.equal(p.search, 'auth');
            assert.equal(p.severity, 'all');
            assert.equal(p.rows, 100);
            assert.equal(s.requests.filter(r => r.rsc && new URL(r.url).searchParams.get('search') === 'billing').length, 0);
            await s.page.goForward();
            await delay(600);
            p = await state(s.page);
            assert.equal(p.severity, 'critical');
            assert.equal(p.search, 'auth');
            assert.equal(p.rows, 15);
            record('Back cancels pending search and Forward restores filters', {});
            await s.context.close();
        }
        for (const releaseBeforeDebounce of [true, false]) {
            const s = await session(browser);
            let ready = false, release;
            const gate = new Promise(r => release = r);
            await s.page.route('**/*', async (route) => { const r = route.request(), u = new URL(r.url()); if (r.headers()['rsc'] === '1' && u.searchParams.get('search') === 'a') {
                const response = await route.fetch();
                ready = true;
                await gate;
                await route.fulfill({ response });
            }
            else
                await route.continue(); });
            await s.page.getByRole('textbox').fill('a');
            await until(() => ready);
            await s.page.getByRole('textbox').pressSequentially('uth', { delay: 20 });
            if (releaseBeforeDebounce) {
                release();
                await delay(120);
                assert.equal((await state(s.page)).search, 'auth');
            }
            else {
                await until(async () => { const p = await state(s.page); return new URL(p.url).searchParams.get('search') === 'auth' && p.rows === 100; });
                release();
            }
            await until(async () => { const p = await state(s.page); return new URL(p.url).searchParams.get('search') === 'auth' && p.search === 'auth' && p.rows === 100; });
            await delay(600);
            assert.equal((await state(s.page)).rows, 100);
            assert.deepEqual(s.errors, []);
            record(releaseBeforeDebounce ? 'Older navigation cannot erase newer draft' : 'Late older navigation cannot replace latest results', { rows: 100 });
            await s.context.close();
        }
        {
            const s = await session(browser);
            let cancelled = 0;
            s.page.on('requestfailed', r => { if (r.url().includes('/api/stream-summary'))
                cancelled++; });
            await s.page.getByTestId('event-evt_0001').click();
            await until(async () => (await state(s.page)).summary.includes('evt_0001'));
            await s.page.getByTestId('event-evt_0002').click();
            await delay(80);
            await s.page.getByTestId('event-evt_0001').click();
            await until(async () => (await state(s.page)).summary.includes('evt_0001'));
            const before = (await state(s.page)).commits;
            await until(async () => !(await state(s.page)).streaming);
            const p = await state(s.page);
            assert(!p.summary.includes('evt_0002'));
            assert(p.summary.includes('evt_0001'));
            assert(cancelled >= 2);
            for (const name of ['dashboard', 'list', 'filters', 'payload'])
                assert.equal(p.commits[name], before[name], name + ' rerendered during streaming');
            assert(p.commits.summary > before.summary);
            assert.deepEqual(s.errors, []);
            record('A-B-A cancellation and leaf render isolation', { cancelled, summaryCommits: p.commits.summary - before.summary, otherComponentCommits: 0 });
            await s.page.getByTestId('event-evt_0001').click();
            await until(async () => (await state(s.page)).summary.includes('evt_0001'));
            await s.page.getByRole('combobox').selectOption('info');
            await until(async () => !(await state(s.page)).streaming);
            await delay(600);
            assert.equal((await state(s.page)).summary, '');
            assert(cancelled >= 3);
            record('Filter change aborts and clears active stream', { cancelled });
            await s.context.close();
        }
        for (const failure of ['network', 'HTTP 500', 'missing body']) {
            const s = await session(browser);
            await s.page.route('**/api/stream-summary?**', r => failure === 'network' ? r.abort('failed') : r.fulfill({ status: failure === 'HTTP 500' ? 500 : 204, body: failure === 'HTTP 500' ? 'server failed' : '' }));
            await s.page.getByTestId('event-evt_0001').click();
            await s.page.locator('section').getByRole('alert').waitFor();
            assert(!(await state(s.page)).streaming);
            assert.equal((await state(s.page)).summary, '');
            assert.deepEqual(s.errors, []);
            record(failure + ' produces error UI and clears loading', { unhandledErrors: 0 });
            await s.context.close();
        }
        {
            const context = await browser.newContext({ baseURL });
            const page = await context.newPage();
            const errors = [];
            page.on('pageerror', error => errors.push(error.message));
            await page.goto(baseURL + '/?search=billing&severity=critical&view=compact');
            await page.getByRole('textbox').waitFor();
            await delay(100);
            const p = await state(page);
            assert.equal(p.search, 'billing');
            assert.equal(p.severity, 'critical');
            assert.equal(p.rows, 15);
            assert.deepEqual(errors, []);
            await page.getByRole('textbox').fill('auth');
            await until(() => new URL(page.url()).searchParams.get('search') === 'auth');
            assert.equal(new URL(page.url()).searchParams.get('view'), 'compact');
            record('Deep link initializes filtered server data and preserves unrelated query parameters', { initialRows: 15 });
            await context.close();
        }
        {
            const s = await session(browser);
            let cancelled = 0;
            s.page.on('requestfailed', r => { if (r.url().includes('/api/stream-summary'))
                cancelled++; });
            await s.page.getByTestId('event-evt_0001').click();
            await until(async () => (await state(s.page)).summary.includes('evt_0001'));
            await s.page.getByTestId('event-evt_0002').click();
            await until(async () => { const p = await state(s.page); return p.summary.includes('evt_0002') && !p.streaming; });
            assert(!(await state(s.page)).summary.includes('evt_0001'));
            assert(cancelled >= 1);
            assert.deepEqual(s.errors, []);
            record('A-B switching cannot retain A text in completed B summary', { cancelled });
            await s.page.getByTestId('event-evt_0001').click();
            await until(async () => (await state(s.page)).summary.includes('evt_0001'));
            await s.page.getByRole('textbox').fill('billing');
            await delay(80);
            assert.equal((await state(s.page)).summary, '');
            assert.equal((await state(s.page)).streaming, false);
            await until(async () => { const p = await state(s.page); return new URL(p.url).searchParams.get('search') === 'billing' && p.rows === 100; });
            assert(cancelled >= 2);
            assert.deepEqual(s.errors, []);
            record('Typing aborts a stream before its debounced navigation', { cancelled });
            await s.context.close();
        }
    }
    finally {
        await browser.close();
        await fs.writeFile(process.env.TEST_OUTPUT || '.repro/acceptance-results.json', JSON.stringify(report, null, 2) + '\n');
    }
}
main().catch(e => { console.error(e); process.exitCode = 1; });
