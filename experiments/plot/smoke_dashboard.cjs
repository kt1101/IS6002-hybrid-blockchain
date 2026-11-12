// Smoke test of the offline results dashboard (not part of pytest):
//
//   node experiments/plot/smoke_dashboard.cjs [screenshot-dir]
//
// Opens results/dashboard.html via file:// in Chromium (Playwright), switches
// to every dataset and clicks every tab, checks that each chart draws SVG data
// marks (or says why it has none), toggles one backend off and on, and fails
// on any console error or page error.
// Screenshots dash-<dataset>-<tab>.png go to screenshot-dir (default: the system temp
// directory). Exits 0 on success.
'use strict';

const os = require('os');
const path = require('path');
const fs = require('fs');
const { pathToFileURL } = require('url');
const { chromium } = require('playwright');

const ROOT = path.resolve(__dirname, '..', '..');
const FILE = path.join(ROOT, 'results', 'dashboard.html');
const SHOTS = path.resolve(process.argv[2] || path.join(os.tmpdir(), 'veritas-dashboard-shots'));

function fail(msg) { throw new Error(msg); }

async function main() {
  fs.mkdirSync(SHOTS, { recursive: true });
  const browser = await chromium.launch();
  const errors = [];
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
    page.on('console', (m) => { if (m.type() === 'error') errors.push('console: ' + m.text()); });
    page.on('pageerror', (e) => errors.push('pageerror: ' + e.message));
    page.on('request', (r) => { if (!r.url().startsWith('file:') && !r.url().startsWith('data:')) errors.push('network request: ' + r.url()); });
    await page.goto(pathToFileURL(FILE).href);

    const title = await page.textContent('h1');
    if (title.trim() !== 'Results overview') fail('h1 = ' + JSON.stringify(title));
    if (await page.isVisible('#pages')) fail('Live demo link visible under file://');

    const datasets = await page.$$eval('#datasets button[data-dataset]', (els) => els.map((e) => e.getAttribute('data-dataset')));
    if (datasets.length < 2) fail('expected a dataset switch, got ' + JSON.stringify(datasets));
    const heroes = {};
    for (const ds of datasets) {
      await page.click('#datasets button[data-dataset=' + ds + ']');
      if ((await page.getAttribute('#datasets button[data-dataset=' + ds + ']', 'aria-pressed')) !== 'true') fail(ds + ': switch not pressed');
      const tabs = await page.$$eval('[role=tab]', (els) => els.map((e) => ({ id: e.id, label: e.textContent })));
      if (tabs.length !== 6) fail('expected 6 tabs, got ' + tabs.length);
      for (const t of tabs) {
        await page.click('#' + t.id);
        const panel = '#' + t.id.replace('tab-', 'panel-');
        await page.waitForSelector(panel, { state: 'visible' });
        const name = t.id.replace('tab-', '');
        if (name === 'overview') {
          const cards = await page.$$eval(panel + ' .card', (els) => els.length);
          const big = await page.$$eval(panel + ' .hero .big', (els) => els.map((e) => e.textContent));
          if (cards < 4 || big.length < 2 || big.some((v) => !/^\d+\.\d\d×$/.test(v))) fail(ds + ' overview cards incomplete: ' + cards + ' cards, hero ' + JSON.stringify(big));
          heroes[ds] = big.join(',');
          if (!(await page.isVisible(panel + ' .ds-card li'))) fail(ds + ': dataset caveats not shown on the overview');
        } else if (name === 'scope') {
          const rows = await page.$$eval(panel + ' table tbody tr', (els) => els.length);
          if (rows < 20) fail('scope tables too small: ' + rows + ' rows');
        } else {
          // every chart box draws marks or says why it has no data
          const charts = await page.$$eval(panel + ' .chart', (els) => els.map((e) => e.querySelector('.empty') ? -1 : e.querySelectorAll('svg .mark, svg .bar').length));
          if (!charts.some((n) => n > 0) || charts.some((n) => n === 0)) fail(ds + ' ' + name + ': chart without data marks: ' + JSON.stringify(charts));
          console.log(ds + ' ' + name + ': ' + charts.length + ' charts, marks ' + JSON.stringify(charts));
        }
        if (name === 'rq3') {
          const rows = await page.$$eval(panel + ' table tbody tr', (els) => els.length);
          if (rows === 0) fail(ds + ' rq3 tables empty');
        }
        await page.screenshot({ path: path.join(SHOTS, 'dash-' + ds + '-' + name + '.png'), fullPage: true });
      }
    }
    if (new Set(Object.values(heroes)).size !== datasets.length) fail('overview identical across datasets: ' + JSON.stringify(heroes));
    await page.click('#datasets button[data-dataset=m3]');

    // Toggle one backend off on the Sensitivity tab: its marks disappear, then return.
    await page.click('#tab-sens');
    const count = () => page.$$eval('#panel-sens svg .s-raft .mark, #panel-sens svg .bar-g.s-raft', (els) => els.length);
    const before = await count();
    await page.click('#filters button[data-backend=raft]');
    if ((await page.getAttribute('#filters button[data-backend=raft]', 'aria-pressed')) !== 'false') fail('toggle did not switch off');
    const off = await count();
    if (before === 0 || off !== 0) fail('raft marks before/after toggle: ' + before + '/' + off);
    await page.screenshot({ path: path.join(SHOTS, 'dash-sens-raft-off.png'), fullPage: true });
    await page.click('#filters button[data-backend=raft]');
    if ((await count()) !== before) fail('raft marks not restored');

    // Tooltip: hovering a chart shows "mean ± std (n reps)".
    const box = await (await page.$('#panel-sens .chart svg')).boundingBox();
    await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
    const tipText = await page.textContent('#tip');
    if (!(await page.isVisible('#tip')) || !/±.*\(\d+ reps?\)/.test(tipText)) fail('tooltip: ' + JSON.stringify(tipText));

    // Keyboard: a data point takes focus and shows the tooltip too.
    await page.mouse.move(0, 0);
    await page.focus('#panel-sens .pt');
    if (!(await page.isVisible('#tip'))) fail('no tooltip on focus');

    if (errors.length) fail(errors.join('\n'));
    console.log('dashboard smoke OK; screenshots in ' + SHOTS);
  } finally {
    await browser.close();
  }
}

main().catch((e) => { console.error('FAIL: ' + e.message); process.exit(1); });
