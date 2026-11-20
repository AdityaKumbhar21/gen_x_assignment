/* Observational baseline reproduction; does not change application responses/source.
   Run with playwright-core available on NODE_PATH, CHROME_PATH set to a Chrome
   executable, and the unchanged dev server at REPRO_URL (default localhost:3100). */
const { chromium } = require('playwright-core');
const fs = require('node:fs/promises');
const path = require('node:path');
const baseURL = process.env.REPRO_URL || 'http://127.0.0.1:3100';
const output = path.resolve('.repro');
const a = '[auth-gateway] Upstream timeout spike #1';
const b = '[webhook-worker] Batch checkpoint completed #2';
const report = { review_date: '2026-11-20', baseURL, scenarios: {} };
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

async function until(predicate, timeout = 8000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await delay(25);
  }
  throw new Error('Timed out waiting for browser observation');
}

async function panel(page) {
  return page.evaluate(() => ({
    heading: document.querySelector('h2')?.innerText,
    summary: document.querySelector('h2')?.parentElement?.nextElementSibling?.firstElementChild?.textContent || '',
    counter: Number([...document.querySelectorAll('span')].find(el => el.innerText.startsWith('Page renders:'))?.innerText.match(/\d+/)?.[0]),
    search: document.querySelector('input')?.value,
    severity: document.querySelector('select')?.value,
    streaming: document.body.innerText.includes('Streaming tokens...'),
    visibleEventIds: [...document.querySelectorAll('div')].map(el => el.textContent)
      .filter(text => /^evt_\d{4} · \d{4}-\d\d-\d\dT[\d:.]+Z$/.test(text)),
  }));
}

async function session(browser) {
  const context = await browser.newContext({ baseURL, viewport: { width: 1440, height: 1000 } });
  const page = await context.newPage();
  const network = [], consoleErrors = [], pageErrors = [];
  const started = Date.now();
  for (const event of ['request', 'requestfinished', 'requestfailed']) {
    page.on(event, request => {
      const url = new URL(request.url());
      if (url.pathname.startsWith('/api/')) network.push({ event, path: url.pathname,
        query: url.search, elapsedMs: Date.now() - started,
        ...(event === 'requestfailed' ? { failure: request.failure()?.errorText } : {}) });
    });
  }
  page.on('console', message => { if (message.type() === 'error') consoleErrors.push(message.text()); });
  page.on('pageerror', error => pageErrors.push(error.message));
  return { context, page, network, consoleErrors, pageErrors };
}

async function load(page) {
  await page.goto(baseURL, { waitUntil: 'domcontentloaded' });
  await page.getByText(a, { exact: true }).waitFor();
  await page.waitForTimeout(700);
}

async function main() {
  await fs.mkdir(output, { recursive: true });
  const browser = await chromium.launch({ headless: true, executablePath: process.env.CHROME_PATH });
  report.browser = browser.version();
  try {
    const stream = await session(browser);
    await load(stream.page);
    const initial = await panel(stream.page);
    const mountRequests = stream.network.filter(item => item.event === 'request' && item.path === '/api/events');
    await stream.page.getByText(a, { exact: true }).click();
    await until(async () => (await panel(stream.page)).summary.includes('evt_0001'));
    await stream.page.getByText(b, { exact: true }).click();
    await stream.page.waitForTimeout(100);
    await stream.page.getByText(a, { exact: true }).click();
    await until(() => stream.network.filter(item => item.event === 'requestfinished' && item.path === '/api/stream-summary').length === 3);
    await stream.page.waitForTimeout(100);
    const mixed = await panel(stream.page);
    report.scenarios.rapid_A_B_A = { initialCounter: initial.counter, finalCounter: mixed.counter,
      pageRenderIncrease: mixed.counter - initial.counter, initialEventCount: initial.visibleEventIds.length,
      initialEventRequests: mountRequests, heading: mixed.heading, summary: mixed.summary,
      containsStaleEventB: mixed.summary.includes('evt_0002'), network: stream.network,
      consoleErrors: stream.consoleErrors, pageErrors: stream.pageErrors };
    await stream.page.screenshot({ path: path.join(output, 'frontend-review-stream-bleed.png') });
    await stream.context.close();
    console.log('Rapid switching:', JSON.stringify({ mixed: report.scenarios.rapid_A_B_A.containsStaleEventB,
      renders: report.scenarios.rapid_A_B_A.pageRenderIncrease, mountFetches: mountRequests.length }));

    const filter = await session(browser);
    await load(filter.page);
    await filter.page.getByText(a, { exact: true }).click();
    await until(async () => (await panel(filter.page)).summary.includes('evt_0001'));
    await filter.page.getByRole('combobox').selectOption('info');
    await filter.page.waitForTimeout(700);
    const duringFilter = await panel(filter.page);
    await until(() => filter.network.some(item => item.event === 'requestfinished' && item.path === '/api/stream-summary'));
    const afterFilter = await panel(filter.page);
    report.scenarios.filter_during_stream = { severity: afterFilter.severity, heading: afterFilter.heading,
      eventAStillListed: afterFilter.visibleEventIds.some(id => id.startsWith('evt_0001 ')),
      streamingAfterFilter: duringFilter.streaming, summaryLengthDuring: duringFilter.summary.length,
      summaryLengthAfter: afterFilter.summary.length,
      streamFailures: filter.network.filter(item => item.event === 'requestfailed' && item.path === '/api/stream-summary'),
      network: filter.network };
    await filter.context.close();
    console.log('Filter change:', JSON.stringify(report.scenarios.filter_during_stream));

    const search = await session(browser);
    await load(search.page);
    const historyBefore = await search.page.evaluate(() => history.length);
    // Controlled network ordering: retain the real server response but delay only
    // the older 'a' query, so the existing lack of stale-response protection is deterministic.
    await search.page.route('**/api/events?**', async route => {
      const response = await route.fetch();
      if (new URL(route.request().url()).searchParams.get('search') === 'a') await delay(1000);
      await route.fulfill({ response });
    });
    const requestStart = search.network.length;
    await search.page.getByRole('textbox').pressSequentially('auth', { delay: 50 });
    await until(() => ['a', 'auth'].every(query => search.network.some(item => item.event === 'requestfinished'
      && item.path === '/api/events' && new URLSearchParams(item.query).get('search') === query)));
    await search.page.waitForTimeout(100);
    const stale = await panel(search.page);
    const expected = await (await search.context.request.get('/api/events?search=auth')).json();
    const historyAfter = await search.page.evaluate(() => history.length);
    await search.page.screenshot({ path: path.join(output, 'frontend-review-stale-results.png') });
    await search.page.goBack();
    await search.page.waitForTimeout(700);
    const back = { url: search.page.url(), ...(await panel(search.page)) };
    await search.page.goForward();
    await search.page.waitForTimeout(700);
    const forward = { url: search.page.url(), ...(await panel(search.page)) };
    report.scenarios.search_and_history = {
      controlledLatency: 'Real older search=a response delayed by 1000 ms; data unchanged',
      input: stale.search, displayedEventCount: stale.visibleEventIds.length, expectedEventCount: expected.total,
      staleResultsObserved: stale.search === 'auth' && stale.visibleEventIds.length !== expected.total,
      historyBefore, historyAfter, back: { url: back.url, input: back.search },
      forward: { url: forward.url, input: forward.search }, network: search.network.slice(requestStart),
    };
    await search.context.close();
    console.log('Search/history:', JSON.stringify(report.scenarios.search_and_history));

    const failure = await session(browser);
    await load(failure.page);
    await failure.page.route('**/api/stream-summary?**', route => route.abort('failed'));
    await failure.page.getByText(a, { exact: true }).click();
    await until(() => failure.pageErrors.length > 0);
    await failure.page.waitForTimeout(200);
    const failed = await panel(failure.page);
    report.scenarios.network_failure = { injectedFailure: 'Abort stream fetch before response',
      unhandledErrors: failure.pageErrors, streamingIndicatorStuck: failed.streaming };
    await failure.context.close();
    console.log('Network failure:', JSON.stringify(report.scenarios.network_failure));
  } finally {
    await fs.writeFile(path.join(output, 'frontend-review-results.json'), JSON.stringify(report, null, 2) + '\n');
    await browser.close();
  }
}

main().catch(error => { console.error(error); process.exitCode = 1; });
