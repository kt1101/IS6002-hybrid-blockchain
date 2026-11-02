// Smoke test of the live demo UI (not part of `go test`):
//
//   node demo/web/smoke.cjs [screenshot-dir]
//
// Starts `go run ./cmd/demo --mode=sim` on 127.0.0.1:18080, opens the page in
// Chromium (Playwright) and walks scenario steps 1-4, checking each step's
// visible effect and that the page logs no console errors. Screenshots of the
// healthy, verify and divergent states go to screenshot-dir (default: the
// system temp directory). Exits 0 on success.
'use strict';

const { spawn } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { chromium } = require('playwright');

const ADDR = '127.0.0.1:18080';
const BASE = 'http://' + ADDR;
const ROOT = path.resolve(__dirname, '..', '..');
const SHOTS = path.resolve(process.argv[2] || path.join(os.tmpdir(), 'veritas-demo-shots'));

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

async function waitForServer(server, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (server.exitCode !== null) { throw new Error('demo server exited with code ' + server.exitCode); }
    try {
      const res = await fetch(BASE + '/api/state');
      if (res.ok) { return; }
    } catch (e) { /* not up yet */ }
    await sleep(500);
  }
  throw new Error('demo server did not answer /api/state within ' + timeoutMs + ' ms');
}

// expectValues waits until ok(values) holds, values mapping each replica
// card's node to the tracked key's value it shows.
async function expectValues(page, ok) {
  const read = () => page.$$eval('#replicas .replica', (cards) => {
    const v = {};
    cards.forEach((c) => { v[c.getAttribute('data-node')] = c.querySelector('.value').textContent; });
    return v;
  });
  const deadline = Date.now() + 15000;
  let v = {};
  while (Date.now() < deadline) {
    v = await read();
    if (ok(v)) { return; }
    await sleep(250);
  }
  throw new Error('unexpected replica values: ' + JSON.stringify(v));
}

function step(page, n) {
  return page.click('#scenario .step[data-step="' + n + '"]');
}

async function main() {
  fs.mkdirSync(SHOTS, { recursive: true });
  const server = spawn('go', ['run', './cmd/demo', '--mode=sim', '--listen', ADDR], {
    cwd: ROOT, detached: true, stdio: ['ignore', 'ignore', 'pipe']
  });
  let serverLog = '';
  server.stderr.on('data', (b) => { serverLog = (serverLog + b).slice(-4000); });
  const stopServer = () => { try { process.kill(-server.pid, 'SIGTERM'); } catch (e) { /* gone */ } };
  process.on('exit', stopServer);

  let browser;
  try {
    await waitForServer(server, 180000);
    browser = await chromium.launch();
    const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
    const errors = [];
    page.on('console', (m) => { if (m.type() === 'error') { errors.push(m.text()); } });
    page.on('pageerror', (e) => errors.push(String(e)));

    await page.goto(BASE + '/');
    await page.waitForSelector('#conn.connected');
    await page.waitForFunction(() => document.getElementById('banner-title').textContent === 'CONSISTENT');
    console.log('ok   banner CONSISTENT');

    // Step 1: write user42 = hello-committee through node2.
    await step(page, 1);
    await page.waitForFunction(() => {
      const cards = document.querySelectorAll('#replicas .replica .value');
      return cards.length > 0 && Array.prototype.every.call(cards, (c) => c.textContent === 'hello-committee');
    }, null, { timeout: 15000 });
    console.log('ok   step 1: every replica card shows hello-committee');
    await page.waitForFunction(() => /CONSISTENT/.test(document.getElementById('banner-title').textContent));

    // Step 2: 64 clients of YCSB-A for 60 s.
    await step(page, 2);
    await page.waitForSelector('#load-btn[data-running="true"]', { timeout: 20000 });
    // The 1,000-key preload comes first, then the measured clients.
    await page.waitForSelector('#load-btn[data-phase="running"]', { timeout: 20000 });
    // More than the single Set of step 1: the load is committing.
    await page.waitForFunction(() => Number(document.getElementById('tps').textContent.replace(/,/g, '')) > 5, null, { timeout: 20000 });
    console.log('ok   step 2: TPS = ' + await page.textContent('#tps'));
    await sleep(2000);
    await page.screenshot({ path: path.join(SHOTS, 'healthy.png'), fullPage: true });

    // Step 3: the Merkle proof of user42 on node2, valid on the first try.
    await step(page, 3);
    await page.waitForSelector('#view-verify', { state: 'visible', timeout: 5000 });
    await page.waitForFunction(() => document.getElementById('verdict-title').textContent !== 'Verifying…', null, { timeout: 15000 });
    const verdict = await page.textContent('#verdict-title');
    if (verdict !== 'Proof valid') { throw new Error('verify view shows ' + verdict); }
    console.log('ok   step 3: Proof valid');
    await page.screenshot({ path: path.join(SHOTS, 'verify.png'), fullPage: true });

    // Step 4: corrupt node3 and re-send user42; the banner must name node3.
    await step(page, 4);
    await page.waitForFunction(() => {
      const t = document.getElementById('banner-title').textContent;
      return /DIVERGENT/.test(t) && /node3/.test(t);
    }, null, { timeout: 20000 });
    console.log('ok   step 4: banner ' + await page.textContent('#banner-title'));
    // Step 4 also re-sends user42: node3 applies it with the corrupt marker,
    // the others with the plain value (the load's ycsb* keys never touch it).
    await expectValues(page, (v) => v.node1 === 'hello-committee' && v.node2 === 'hello-committee' &&
      v.node4 === 'hello-committee' && v.node3 === 'hello-committee-CORRUPT');
    console.log('ok   step 4: user42 = hello-committee-CORRUPT on node3, hello-committee on node1/2/4');
    await sleep(1500);
    await page.screenshot({ path: path.join(SHOTS, 'divergent.png'), fullPage: true });

    // Step 5: the banner is highlighted and the Recover panel offers Reset.
    await step(page, 5);
    await page.waitForSelector('#recover', { state: 'visible', timeout: 5000 });
    if (!(await page.isVisible('#recover-actions button:has-text("Reset cluster")'))) { throw new Error('step 5 shows no Reset button'); }
    console.log('ok   step 5: Recover panel shown');

    // Crash node4 by hand, see it Down, then heal it from the Recover panel.
    await page.selectOption('#fault-node', 'node4');
    await page.click('#fault-grid .fault[data-kind="crash"]');
    await page.waitForSelector('#replicas .replica.down[data-node="node4"]', { timeout: 10000 });
    await page.waitForSelector('#recover-actions button:has-text("Heal node4")', { timeout: 5000 });
    await sleep(1000);
    await page.screenshot({ path: path.join(SHOTS, 'recover.png'), fullPage: true });
    await page.click('#recover-actions button:has-text("Heal node4")');
    await page.waitForSelector('#replicas .replica[data-node="node4"]:not(.down)', { timeout: 10000 });
    console.log('ok   node4 crashed and healed');

    // Narrow projector width still lays out without horizontal scrolling.
    await page.setViewportSize({ width: 1024, height: 768 });
    await sleep(500);
    const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
    if (overflow > 0) { throw new Error('page scrolls horizontally at 1024 px by ' + overflow + ' px'); }
    await page.screenshot({ path: path.join(SHOTS, 'divergent-1024.png'), fullPage: true });

    if (errors.length) { throw new Error('console errors:\n  ' + errors.join('\n  ')); }
    console.log('ok   no console errors');
    console.log('screenshots in ' + SHOTS);
  } catch (e) {
    console.error('FAIL ' + e.message);
    if (serverLog) { console.error('--- server stderr (tail) ---\n' + serverLog); }
    process.exitCode = 1;
  } finally {
    if (browser) { await browser.close(); }
    stopServer();
  }
}

main();
