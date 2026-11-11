/* Presentation pages: page order, keyboard navigation, and the tables and
   SVG charts drawn from data.js (window.PRESENT_DATA). Plain ES2017, no
   network. Every value shown comes unchanged from data.js; charts only
   parse numbers to place marks, labels keep the source text. */
(function () {
  'use strict';

  var PAGES = [
    ['./', 'Mục lục'],
    ['1-bai-toan.html', 'Bài toán và bài gốc'],
    ['2-kien-truc.html', 'Kiến trúc'],
    ['3-co-che.html', 'Cơ chế giai đoạn 2–3'],
    ['4-ket-qua.html', 'Kết quả'],
    ['5-gioi-han.html', 'Giới hạn và việc còn mở'],
    ['6-kich-ban.html', 'Kịch bản trình bày']
  ];
  var COLORS = { raft: 'var(--raft)', bft2: 'var(--bft2)' };
  var SVGNS = 'http://www.w3.org/2000/svg';
  var D = window.PRESENT_DATA;

  function current() {
    // The table of contents is "./" (/present/); http.FileServer redirects
    // /present/index.html there, so the pages never link to index.html.
    var name = location.pathname.split('/').pop();
    for (var i = 1; i < PAGES.length; i++) if (PAGES[i][0] === name) return i;
    return 0;
  }

  function el(tag, attrs, text) {
    var e = document.createElement(tag);
    for (var k in attrs || {}) e.setAttribute(k, attrs[k]);
    if (text != null) e.textContent = text;
    return e;
  }

  function svg(tag, attrs, text) {
    var e = document.createElementNS(SVGNS, tag);
    for (var k in attrs || {}) e.setAttribute(k, attrs[k]);
    if (text != null) e.textContent = text;
    return e;
  }

  // Markdown cell -> DOM: **bold** and `code`, everything else as text.
  function cell(td, text) {
    var re = /(\*\*[^*]+\*\*|`[^`]+`)/g, last = 0, m;
    while ((m = re.exec(text))) {
      if (m.index > last) td.appendChild(document.createTextNode(text.slice(last, m.index)));
      var t = m[0];
      td.appendChild(t[0] === '`' ? el('code', null, t.slice(1, -1)) : el('strong', null, t.slice(2, -2)));
      last = m.index + t.length;
    }
    if (last < text.length) td.appendChild(document.createTextNode(text.slice(last)));
    if (/^[\d\s,./–()~+−-]+(ms|s)?$/.test(text.replace(/\*\*/g, '')) && /\d/.test(text)) td.className = 'n';
  }

  function source(text) { return el('p', { 'class': 'src' }, text); }

  // ---- navigation ----
  function nav() {
    var i = current();
    var toc = document.getElementById('toc');
    if (toc) {
      PAGES.forEach(function (p, j) {
        var a = el('a', { href: p[0] });
        if (j === i) a.setAttribute('aria-current', 'page');
        a.appendChild(el('span', { 'class': 'num' }, String(j)));
        a.appendChild(document.createTextNode(p[1]));
        toc.appendChild(a);
      });
    }
    var dn = document.getElementById('deck-nav');
    if (dn) {
      dn.appendChild(i > 0 ? el('a', { href: PAGES[i - 1][0], rel: 'prev' }, '← Trước') : el('span', { 'class': 'off' }, '← Trước'));
      dn.appendChild(el('span', { 'class': 'pos' }, i + ' / ' + (PAGES.length - 1)));
      dn.appendChild(i < PAGES.length - 1 ? el('a', { href: PAGES[i + 1][0], rel: 'next' }, 'Tiếp →') : el('span', { 'class': 'off' }, 'Tiếp →'));
    }
    document.addEventListener('keydown', function (e) {
      if (e.altKey || e.ctrlKey || e.metaKey || /INPUT|SELECT|TEXTAREA/.test(document.activeElement.tagName)) return;
      if ((e.key === 'ArrowRight' || e.key === 'PageDown') && i < PAGES.length - 1) location.href = PAGES[i + 1][0];
      if ((e.key === 'ArrowLeft' || e.key === 'PageUp') && i > 0) location.href = PAGES[i - 1][0];
    });
  }

  // ---- tables ----
  function notesTable(host) {
    var t = D.notes[host.getAttribute('data-notes')];
    var wrap = el('div', { 'class': 'table-wrap' });
    var table = el('table', { 'class': 'data' });
    var tr = el('tr');
    t.header.forEach(function (h) { var th = el('th', { scope: 'col' }); cell(th, h); tr.appendChild(th); });
    table.appendChild(el('thead')).appendChild(tr);
    var tb = el('tbody');
    t.rows.forEach(function (r) {
      var row = el('tr'), prev = null;
      r.forEach(function (c) {
        // An empty cell continues the one before it (E12: "không root nào
        // được xác nhận" spans two columns in NOTES).
        if (c === '' && prev) { prev.setAttribute('colspan', String((+prev.getAttribute('colspan') || 1) + 1)); return; }
        var td = el('td'); cell(td, c); row.appendChild(td); prev = td;
      });
      tb.appendChild(row);
    });
    table.appendChild(tb);
    wrap.appendChild(table);
    host.appendChild(wrap);
    host.appendChild(source(t.source + ' · mục “' + t.section + '” · commit ' + t.commit));
  }

  // data-where="col=val;col=val" -> row filter (all must match).
  function whereFilter(host) {
    var w = host.getAttribute('data-where');
    if (!w) return function () { return true; };
    var conds = w.split(';').map(function (c) { return c.split('='); });
    return function (r) { return conds.every(function (c) { return r[c[0]] === c[1]; }); };
  }

  // CSV decimal as printed in the NOTES tables: data-digits decimals, comma
  // separator ("112.374" -> "112,4" with 1). Integers and text stay as is.
  function decimal(v, digits) {
    if (digits == null || !/^-?\d+\.\d+$/.test(v)) return v;
    return (+v).toFixed(+digits).replace('.', ',');
  }

  // Rows of a CSV dataset as a table: data-csv="key", data-cols="a,b,c",
  // data-prefix filters the "exp" column (e.g. "e12-"), data-where as above,
  // data-digits rounds decimals for display (the title keeps the CSV value).
  function csvTable(host) {
    var ds = D.csv[host.getAttribute('data-csv')];
    var cols = host.getAttribute('data-cols').split(',');
    var prefix = host.getAttribute('data-prefix') || '';
    var keep = whereFilter(host);
    var digits = host.getAttribute('data-digits');
    var rows = ds.rows.filter(function (r) { return keep(r) && (!prefix || (r.exp || '').indexOf(prefix) === 0); });
    var wrap = el('div', { 'class': 'table-wrap' });
    var table = el('table', { 'class': 'data' });
    var tr = el('tr');
    cols.forEach(function (c) { tr.appendChild(el('th', { scope: 'col' }, c)); });
    table.appendChild(el('thead')).appendChild(tr);
    var tb = el('tbody');
    rows.forEach(function (r) {
      var row = el('tr');
      cols.forEach(function (c) {
        var raw = r[c] == null ? '' : r[c], shown = decimal(raw, digits);
        var td = el('td'); cell(td, shown);
        if (shown !== raw) td.setAttribute('title', raw);
        row.appendChild(td);
      });
      tb.appendChild(row);
    });
    table.appendChild(tb);
    wrap.appendChild(table);
    host.appendChild(wrap);
    var w = host.getAttribute('data-where');
    host.appendChild(source(ds.source + ' · commit ' + ds.commit + (prefix ? ' · các hàng exp = ' + prefix + '*' : '') + (w ? ' · các hàng ' + w.replace(/;/g, ', ') : '') +
      (digits != null ? ' · làm tròn ' + digits + ' chữ số thập phân (rê chuột để xem giá trị gốc)' : '')));
  }

  // Horizontal bars from a CSV dataset, with the min–max range of the reps:
  // data-csv-chart="key", data-where (rows), data-label="col,col" (bar
  // label), data-y (bar), data-lo / data-hi (range), data-unit.
  function chartCsvBars(host) {
    var ds = D.csv[host.getAttribute('data-csv-chart')];
    var rows = ds.rows.filter(whereFilter(host));
    var lab = host.getAttribute('data-label').split(',');
    var y = host.getAttribute('data-y'), lo = host.getAttribute('data-lo'), hi = host.getAttribute('data-hi');
    var unit = host.getAttribute('data-unit') || '';
    var vmax = Math.max.apply(null, rows.map(function (r) { return num(hi ? r[hi] : r[y]); }));
    var W = 760, rowH = 26, L = 230, R = 90, T = 8;
    var H = T + rows.length * rowH + 30;
    var xt = ticks(vmax, 5);
    var xmax = xt[xt.length - 1];
    function X(v) { return L + (W - L - R) * v / xmax; }
    var s = svg('svg', { viewBox: '0 0 ' + W + ' ' + H, role: 'img', 'class': 'chart', 'aria-label': y + ' theo ' + lab.join(', ') });
    var g = s.appendChild(svg('g', { 'class': 'grid' }));
    xt.forEach(function (v) {
      g.appendChild(svg('line', { x1: X(v), x2: X(v), y1: T, y2: H - 30 }));
      s.appendChild(svg('text', { x: X(v), y: H - 12, 'text-anchor': 'middle' }, String(v)));
    });
    rows.forEach(function (r, j) {
      var top = T + j * rowH, mid = top + rowH / 2;
      var name = lab.map(function (c) { return r[c]; }).join(' · ');
      s.appendChild(svg('text', { x: L - 10, y: mid + 4, 'text-anchor': 'end' }, name));
      var rect = svg('rect', { x: L, y: top + 5, width: Math.max(1, X(num(r[y])) - L), height: rowH - 10, rx: 3, fill: COLORS[r.backend] || 'var(--accent)' });
      rect.appendChild(svg('title', null, name + ': ' + y + ' = ' + r[y] + (lo ? ' (' + lo + ' ' + r[lo] + ', ' + hi + ' ' + r[hi] + ')' : '') + ' ' + unit + ', ' + r.reps + ' rep'));
      s.appendChild(rect);
      var end = X(num(r[y]));
      if (lo && hi) {
        s.appendChild(svg('line', { x1: X(num(r[lo])), x2: X(num(r[hi])), y1: mid, y2: mid, stroke: 'var(--ink)', 'stroke-width': 1.5 }));
        [lo, hi].forEach(function (k) { s.appendChild(svg('line', { x1: X(num(r[k])), x2: X(num(r[k])), y1: mid - 5, y2: mid + 5, stroke: 'var(--ink)', 'stroke-width': 1.5 })); });
        end = Math.max(end, X(num(r[hi])));
      }
      s.appendChild(svg('text', { x: end + 6, y: mid + 4, 'class': 'val' }, r[y] + (unit ? ' ' + unit : '')));
    });
    host.appendChild(s);
    host.appendChild(source(ds.source + ' (cột ' + y + (lo ? ', vạch: ' + lo + '–' + hi : '') + '; ' + host.getAttribute('data-where').replace(/;/g, ', ') + ') · commit ' + ds.commit));
  }

  // ---- charts ----
  function num(s) { return parseFloat(String(s).replace(',', '.')); }

  function ticks(max, n) {
    var raw = max / n, mag = Math.pow(10, Math.floor(Math.log10(raw))), step = mag;
    [1, 2, 2.5, 5, 10].some(function (m) { step = m * mag; return step >= raw; });
    var out = [];
    for (var v = 0; v <= max + 1e-9; v += step) out.push(+v.toFixed(6));
    return out;
  }

  function legend(host, items) {
    var lg = el('div', { 'class': 'legend' });
    items.forEach(function (it) {
      var s = el('span');
      var i = el('i');
      i.style.background = it[1];
      if (it[2]) { i.style.background = 'none'; i.style.borderTop = '2px dashed ' + it[1]; i.style.height = '0'; }
      s.appendChild(i);
      s.appendChild(document.createTextNode(it[0]));
      lg.appendChild(s);
    });
    host.appendChild(lg);
  }

  // E7: write p99 (mean of reps) against the offered rate, per backend.
  function chartE7(host) {
    var ds = D.csv.e7_points;
    var W = 760, H = 340, L = 64, R = 16, T = 14, B = 46;
    var xs = ds.rows.map(function (r) { return num(r.offered_rps); });
    var ys = ds.rows.map(function (r) { return num(r.write_p99_ms_mean); });
    var xmax = Math.max.apply(null, xs), ymax = Math.max.apply(null, ys);
    var yt = ticks(ymax, 6), xt = [0, 250, 500, 750, 1000, 1250, 1500, 1750, 2000];
    ymax = yt[yt.length - 1];
    xmax = Math.max(xmax, xt[xt.length - 1]);
    function X(v) { return L + (W - L - R) * v / xmax; }
    function Y(v) { return T + (H - T - B) * (1 - v / ymax); }
    var s = svg('svg', { viewBox: '0 0 ' + W + ' ' + H, role: 'img', 'class': 'chart',
      'aria-label': 'E7: write p99 theo tốc độ gửi, raft và bft2' });
    var g = s.appendChild(svg('g', { 'class': 'grid' }));
    yt.forEach(function (v) {
      g.appendChild(svg('line', { x1: L, x2: W - R, y1: Y(v), y2: Y(v) }));
      s.appendChild(svg('text', { x: L - 8, y: Y(v) + 4, 'text-anchor': 'end' }, String(v)));
    });
    xt.forEach(function (v) { s.appendChild(svg('text', { x: X(v), y: H - B + 18, 'text-anchor': 'middle' }, String(v))); });
    s.appendChild(svg('text', { x: (L + W - R) / 2, y: H - 6, 'text-anchor': 'middle' }, 'tốc độ gửi (req/s)'));
    s.appendChild(svg('text', { x: 14, y: (T + H - B) / 2, transform: 'rotate(-90 14 ' + ((T + H - B) / 2) + ')', 'text-anchor': 'middle' }, 'write p99 (ms)'));
    [[1000, 'p99 ≤ 1 s (slo_rps)'], [2000, 'p99 ≤ 2 s (sat_rps)']].forEach(function (h) {
      s.appendChild(svg('line', { x1: L, x2: W - R, y1: Y(h[0]), y2: Y(h[0]), stroke: 'var(--amber)', 'stroke-dasharray': '6 4', 'stroke-width': 1.5 }));
      s.appendChild(svg('text', { x: W - R - 4, y: Y(h[0]) - 6, 'text-anchor': 'end', style: 'fill: var(--amber)' }, h[1]));
    });
    s.appendChild(svg('line', { x1: X(480), x2: X(480), y1: T, y2: H - B, stroke: 'var(--accent)', 'stroke-dasharray': '3 3', 'stroke-width': 1.5 }));
    s.appendChild(svg('text', { x: X(480) + 6, y: T + 14, style: 'fill: var(--accent-ink)' }, 'E9–E12: 480 req/s'));
    ['raft', 'bft2'].forEach(function (b) {
      var rows = ds.rows.filter(function (r) { return r.backend === b; });
      var d = rows.map(function (r, j) { return (j ? 'L' : 'M') + X(num(r.offered_rps)).toFixed(1) + ' ' + Y(num(r.write_p99_ms_mean)).toFixed(1); }).join(' ');
      s.appendChild(svg('path', { d: d, fill: 'none', stroke: COLORS[b], 'stroke-width': 2.5 }));
      rows.forEach(function (r) {
        var c = svg('circle', { cx: X(num(r.offered_rps)), cy: Y(num(r.write_p99_ms_mean)), r: 4.5, fill: COLORS[b] });
        c.appendChild(svg('title', null, b + ' ' + r.offered_rps + ' req/s: write p99 ' + r.write_p99_ms_mean + ' ms, tps ' + r.tps_mean + ', dropped ' + r.dropped + ' (' + r.reps + ' rep)'));
        s.appendChild(c);
      });
    });
    var ax = s.appendChild(svg('g', { 'class': 'axis' }));
    ax.appendChild(svg('line', { x1: L, x2: W - R, y1: H - B, y2: H - B }));
    ax.appendChild(svg('line', { x1: L, x2: L, y1: T, y2: H - B }));
    host.appendChild(s);
    legend(host, [['raft', COLORS.raft], ['bft2', COLORS.bft2]]);
    host.appendChild(source(ds.source + ' (cột write_p99_ms_mean, offered_rps) · commit ' + ds.commit));
  }

  // Horizontal grouped bars from a NOTES table: data-notes-chart="key",
  // data-col="header text", rows grouped by column 0 (backend) and labelled
  // by column 1. Bar labels keep the source text.
  function chartBars(host) {
    var t = D.notes[host.getAttribute('data-notes-chart')];
    var col = t.header.indexOf(host.getAttribute('data-col'));
    var unit = host.getAttribute('data-unit') || '';
    var rows = t.rows;
    var vmax = Math.max.apply(null, rows.map(function (r) { return num(r[col]); }));
    var W = 760, rowH = 26, L = 230, R = 70, T = 8;
    var H = T + rows.length * rowH + 30;
    var xt = ticks(vmax, 5);
    var xmax = xt[xt.length - 1];
    function X(v) { return L + (W - L - R) * v / xmax; }
    var s = svg('svg', { viewBox: '0 0 ' + W + ' ' + H, role: 'img', 'class': 'chart',
      'aria-label': t.header[col] + ' theo chế độ đọc' });
    var g = s.appendChild(svg('g', { 'class': 'grid' }));
    xt.forEach(function (v) {
      g.appendChild(svg('line', { x1: X(v), x2: X(v), y1: T, y2: H - 30 }));
      s.appendChild(svg('text', { x: X(v), y: H - 12, 'text-anchor': 'middle' }, String(v)));
    });
    rows.forEach(function (r, j) {
      var y = T + j * rowH;
      s.appendChild(svg('text', { x: L - 10, y: y + rowH / 2 + 4, 'text-anchor': 'end' }, r[0] + ' · ' + r[1]));
      var rect = svg('rect', { x: L, y: y + 4, width: Math.max(1, X(num(r[col])) - L), height: rowH - 8, rx: 3, fill: COLORS[r[0]] || 'var(--accent)' });
      rect.appendChild(svg('title', null, r[0] + ' ' + r[1] + ': ' + t.header[col] + ' = ' + r[col] + ' ' + unit));
      s.appendChild(rect);
      s.appendChild(svg('text', { x: X(num(r[col])) + 6, y: y + rowH / 2 + 4, 'class': 'val' }, r[col] + (unit ? ' ' + unit : '')));
    });
    host.appendChild(s);
    host.appendChild(source(t.source + ' · mục “' + t.section + '”, cột “' + t.header[col] + '” · commit ' + t.commit));
  }

  // Values that the prose cites from a CSV: <span data-csv-val="key"
  // data-where="col=val;col=val" data-field="col">.
  function csvValues() {
    document.querySelectorAll('[data-csv-val]').forEach(function (n) {
      var ds = D.csv[n.getAttribute('data-csv-val')];
      var conds = n.getAttribute('data-where').split(';').map(function (c) { return c.split('='); });
      var row = ds.rows.filter(function (r) { return conds.every(function (c) { return r[c[0]] === c[1]; }); })[0];
      n.textContent = row ? decimal(row[n.getAttribute('data-field')], n.getAttribute('data-digits')) : 'chưa đo';
    });
  }

  function init() {
    nav();
    var commit = document.querySelectorAll('[data-commit]');
    for (var i = 0; i < commit.length; i++) commit[i].textContent = D ? D.commit_short : '?';
    if (!D) return;
    document.querySelectorAll('[data-notes]').forEach(notesTable);
    document.querySelectorAll('[data-csv]').forEach(csvTable);
    document.querySelectorAll('[data-chart="e7"]').forEach(chartE7);
    document.querySelectorAll('[data-notes-chart]').forEach(chartBars);
    document.querySelectorAll('[data-csv-chart]').forEach(chartCsvBars);
    csvValues();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
