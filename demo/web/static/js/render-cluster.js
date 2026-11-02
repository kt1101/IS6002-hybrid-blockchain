// render-cluster.js — draws the live cluster view (header, scenario bar,
// banner, replicas, blocks, throughput, event log, control panels) from the
// hub's state. Every function is idempotent: it redraws its part from data.
(function (VD) {
  'use strict';

  var d = VD.dom;
  var h = d.h;

  var BLOCKS_SHOWN = 10;
  var LOG_SHOWN = 60;
  var VALUE_SHOWN = 40; // characters of a value on a replica card

  var ICONS = {
    consistent: 'M20 6 9 17l-5-5',
    divergent: 'M12 9v4M12 17h.01M10.3 3.9 1.8 18.5A2 2 0 0 0 3.5 21.5h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z',
    unknown: 'M12 7v5l3 2M12 21a9 9 0 1 0 0-18 9 9 0 0 0 0 18z'
  };

  var FAULT_ROLE = {
    crash: 'crashed',
    silent: 'silent: own blocks dropped',
    slow: 'slow: extra message delay',
    corrupt: 'corrupt state injected'
  };

  function has(list, x) { return (list || []).indexOf(x) >= 0; }

  // reference is the lowest-numbered up node, whose blocks the hub reports.
  function reference(nodes) {
    for (var i = 0; i < nodes.length; i++) {
      if (nodes[i].status !== 'down') { return nodes[i].name; }
    }
    return '';
  }

  // ---------- header ----------

  function header(info, conn) {
    var sim = d.byId('mode-sim');
    var docker = d.byId('mode-docker');
    sim.setAttribute('aria-pressed', String(info.mode === 'sim'));
    docker.setAttribute('aria-pressed', String(info.mode === 'docker'));
    var tag = info.mode === 'sim'
      ? info.backend + ' · N=' + info.n + ' · in-process'
      : info.backend + ' · N=' + info.n + ' · f=' + info.f;
    if (!info.mode) { tag = 'connecting…'; }
    d.byId('cluster-tag').textContent = tag;
    connection(conn);
  }

  function connection(conn) {
    var el = d.byId('conn');
    el.className = 'conn ' + conn;
    d.byId('conn-text').textContent =
      conn === 'connected' ? 'Connected' : conn === 'reconnecting' ? 'Reconnecting…' : 'Connecting…';
  }

  // ---------- scenario bar ----------

  function scenario(step) {
    var buttons = document.querySelectorAll('#scenario .step');
    Array.prototype.forEach.call(buttons, function (b) {
      var n = Number(b.getAttribute('data-step'));
      b.classList.toggle('done', n < step);
      b.classList.toggle('active', n === step);
      if (n === step) { b.setAttribute('aria-current', 'step'); } else { b.removeAttribute('aria-current'); }
      var num = b.querySelector('.num');
      if (n < step) {
        d.replace(num, [d.svg('svg', { attrs: { width: 16, height: 16, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', 'stroke-width': 3, 'stroke-linecap': 'round', 'stroke-linejoin': 'round', 'aria-label': 'done' } },
          [d.svg('path', { attrs: { d: ICONS.consistent } })])]);
      } else {
        num.textContent = String(n);
      }
    });
  }

  // ---------- node pickers ----------

  // nodeSelect fills a <select> with the node names, keeping its choice (or
  // taking fallback the first time).
  function nodeSelect(sel, nodes, fallback) {
    var names = nodes.map(function (n) { return n.name; });
    var current = sel.value || sel.getAttribute('data-want') || fallback;
    var same = sel.options.length === names.length &&
      names.every(function (n, i) { return sel.options[i].value === n; });
    if (!same) {
      d.replace(sel, names.map(function (n) { return h('option', { text: n, attrs: { value: n } }); }));
    }
    if (names.indexOf(current) < 0) { current = names.indexOf(fallback) >= 0 ? fallback : (names[0] || ''); }
    sel.value = current;
  }

  // ---------- banner ----------

  function banner(cons, nodes) {
    var el = d.byId('banner');
    var verdict = (cons && cons.verdict) || 'UNKNOWN';
    var down = (cons && cons.down) || [];
    var downText = down.length ? ' · ' + d.list(down) + ' unreachable' : '';
    var title, text, cls;
    // Before the first block the hub has no roots to compare (UNKNOWN), but
    // replicas that are all up at height 0 hold the same empty ledger.
    var genesis = verdict === 'UNKNOWN' && nodes.length > 0 &&
      nodes.every(function (n) { return n.status !== 'down' && !n.height; });
    if (genesis) {
      cls = 'consistent';
      title = 'CONSISTENT';
      text = 'All ' + nodes.length + ' replicas start from the same empty ledger · no blocks yet';
    } else if (verdict === 'CONSISTENT') {
      cls = 'consistent';
      title = 'CONSISTENT';
      text = (down.length ? 'All ' + cons.up + ' reachable replicas' : 'All ' + cons.up + ' replicas') +
        ' report the same ledger root at height ' + d.num(cons.height) + downText;
    } else if (verdict === 'DIVERGENT') {
      cls = 'divergent';
      title = 'DIVERGENT · ' + d.list(cons.divergent || []);
      text = cons.agree > 0
        ? cons.agree + ' of ' + cons.up + ' reachable replicas agree on root A at height ' + d.num(cons.height) + downText
        : 'No majority: the ' + cons.up + ' reachable replicas disagree on the root at height ' + d.num(cons.height) + downText;
    } else {
      cls = 'unknown';
      title = 'WAITING';
      var up = nodes.filter(function (n) { return n.status !== 'down'; }).length;
      text = up === 0 && nodes.length
        ? 'No replica is reachable'
        : 'Waiting for the replicas to report a common block height' + downText;
    }
    var highlighted = el.classList.contains('highlight');
    el.className = 'banner ' + cls + (highlighted ? ' highlight' : '');
    d.byId('banner-icon').setAttribute('d', ICONS[cls]);
    d.byId('banner-title').textContent = title;
    d.byId('banner-text').textContent = text;
  }

  // ---------- replica cards ----------

  function replicas(nodes, trackedKey, entryNode) {
    var box = d.byId('replicas');
    box.className = 'replicas' + (nodes.length % 2 === 0 ? ' even' : '') + (nodes.length === 4 ? ' four' : '');
    d.replace(box, nodes.map(function (n) { return replicaCard(n, trackedKey, entryNode); }));
  }

  function replicaCard(n, trackedKey, entryNode) {
    var down = n.status === 'down';
    var divergent = n.status === 'divergent';
    var status = down ? 'Down' : divergent ? 'Divergent' : 'Up';

    var role = ['replica'];
    if (n.name === entryNode) { role.push('client entry'); }
    (n.faults || []).forEach(function (f) { role.push(FAULT_ROLE[f] || f); });

    var group = down ? '–' : (n.group || '–');
    var chipClass = 'chip';
    if (down || !n.group) { chipClass += ' none'; } else if (n.group !== 'A') { chipClass += ' g' + Math.min(n.group.charCodeAt(0) - 65, 3); }

    var value;
    if (!trackedKey) { value = 'no key tracked yet'; } else if (down) { value = '—'; } else { value = n.value || 'not found'; }

    return h('article', { className: 'replica' + (down ? ' down' : divergent ? ' divergent' : ''), attrs: { 'data-node': n.name, 'aria-label': n.name + ': ' + status } }, [
      h('div', { className: 'replica-head' }, [
        h('span', { className: 'replica-name', text: n.name }),
        h('span', { className: 'pill', text: status })
      ]),
      h('div', { className: 'role', text: role.join(' · ') }),
      h('dl', {}, [
        h('dt', { text: 'Height' }),
        h('dd', { className: 'height', text: n.height ? d.num(n.height) : '–' }),
        h('dt', { text: 'Root' }),
        h('dd', { className: 'root-cell' }, [
          h('span', { className: chipClass, text: group, attrs: { title: n.group ? 'Root group ' + n.group : 'No root group' } }),
          h('span', { className: 'digest', text: down ? 'unreachable' : d.digest(n.root) })
        ]),
        h('dt', { text: trackedKey || 'Tracked key', attrs: { title: trackedKey || '' } }),
        h('dd', { className: 'value', text: d.clip(value, VALUE_SHOWN), attrs: { title: value.length > VALUE_SHOWN ? value : null } })
      ])
    ]);
  }

  // ---------- blocks ----------

  var newestDrawn = 0;

  function blocks(list, ref) {
    var shown = list.slice(-BLOCKS_SHOWN);
    d.byId('blocks-caption').textContent = ref ? 'newest right · as applied on ' + ref : 'newest right';
    var box = d.byId('blocks');
    if (!shown.length) {
      d.replace(box, [h('p', { className: 'empty', text: 'No blocks yet. Submit a transaction or start the load.' })]);
      newestDrawn = 0;
      return;
    }
    var prevNewest = newestDrawn;
    d.replace(box, shown.map(function (b) {
      var txs = b.txs + ' tx' + (b.aborted ? ' · ' + b.aborted + (b.aborted === 1 ? ' abort' : ' aborts') : '');
      var cls = 'block' + (b.aborted ? ' abort' : '') + (prevNewest && b.height > prevNewest ? ' new' : '');
      return h('div', { className: cls }, [
        h('span', { className: 'block-h', text: '#' + d.num(b.height) }),
        h('span', { className: 'block-txs', text: txs }),
        h('span', { className: 'block-by', text: b.proposer ? 'by ' + b.proposer : 'proposer unknown' })
      ]);
    }));
    newestDrawn = shown[shown.length - 1].height;
  }

  // ---------- throughput ----------

  function metrics(m) {
    m = m || {};
    d.byId('tps').textContent = d.num(Math.round(m.tps || 0));
    d.byId('p50').textContent = d.ms(m.p50_ms);
    d.byId('p99').textContent = d.ms(m.p99_ms);
    d.byId('errors').textContent = d.num(m.errors);
    sparkline(m.series || [], m.markers || []);
  }

  function sparkline(series, markers) {
    var W = 300, H = 90, top = 6, bottom = 86;
    var n = series.length;
    var max = Math.max.apply(null, series.concat([1])) * 1.1;
    var x = function (i) { return n > 1 ? (i * W / (n - 1)) : 0; };
    var y = function (v) { return bottom - (v / max) * (bottom - top); };
    var pts = series.map(function (v, i) { return x(i).toFixed(1) + ',' + y(v).toFixed(1); });
    var parts = [d.svg('line', { attrs: { x1: 0, y1: H - 1, x2: W, y2: H - 1, stroke: '#DADCE0' } })];
    if (n > 1) {
      parts.push(d.svg('polygon', { attrs: { points: '0,' + bottom + ' ' + pts.join(' ') + ' ' + W + ',' + bottom, fill: 'rgba(29,78,216,0.08)' } }));
      parts.push(d.svg('polyline', { attrs: { points: pts.join(' '), fill: 'none', stroke: '#1D4ED8', 'stroke-width': 2.5, 'stroke-linejoin': 'round', 'vector-effect': 'non-scaling-stroke' } }));
    }
    markers.forEach(function (mk) {
      var mx = Math.max(1, Math.min(W - 1, x(n - 1 - mk.ago_s)));
      parts.push(d.svg('line', { attrs: { x1: mx, y1: 0, x2: mx, y2: H - 1, stroke: '#B45309', 'stroke-width': 2, 'stroke-dasharray': '4 4', 'vector-effect': 'non-scaling-stroke' } },
        [d.svg('title', { text: mk.label + ', ' + mk.ago_s + ' s ago' })]));
    });
    d.replace(d.byId('spark'), parts);
    var last = markers[markers.length - 1];
    d.byId('marker-label').textContent = last ? last.label + ' · −' + last.ago_s + ' s' : '';
  }

  // ---------- event log ----------

  function log(lines) {
    var shown = lines.slice(-LOG_SHOWN).reverse(); // newest first
    if (!shown.length) {
      d.replace(d.byId('log'), [h('li', {}, [h('span', { className: 't', text: '' }), h('span', { className: 'info', text: 'No events yet.' })])]);
      return;
    }
    d.replace(d.byId('log'), shown.map(function (l) {
      return h('li', {}, [
        h('span', { className: 't', text: l.t }),
        h('span', { className: l.level || 'info', text: l.text })
      ]);
    }));
  }

  // ---------- load generator ----------

  var WORKLOAD = { a: 'YCSB-A', b: 'YCSB-B', c: 'YCSB-C' };

  function load(state, busy) {
    var btn = d.byId('load-btn');
    var running = !!(state && state.running);
    btn.textContent = running ? 'Stop load · ' + state.remaining_s + ' s left' : 'Start load';
    btn.className = 'btn' + (running ? '' : ' primary');
    btn.disabled = !!busy;
    btn.setAttribute('data-running', String(running));
    var res = d.byId('load-result');
    if (running) {
      res.className = 'result';
      res.textContent = 'Running: ' + state.clients + ' clients · ' + (WORKLOAD[state.workload] || state.workload);
    } else if (!res.classList.contains('error')) {
      res.textContent = ''; // keep a failed start's reason on screen
    }
  }

  // ---------- fault panel ----------

  function faults(info, nodes, target, busy) {
    var node = nodes.filter(function (n) { return n.name === target; })[0];
    var active = (node && node.faults) || [];
    var buttons = document.querySelectorAll('#fault-grid .fault[data-kind]');
    Array.prototype.forEach.call(buttons, function (b) {
      var kind = b.getAttribute('data-kind');
      var supported = has(info.faults, kind);
      b.disabled = busy || !supported || !node;
      b.setAttribute('aria-pressed', String(has(active, kind)));
      if (!supported && info.mode) { b.title = 'Not available in ' + info.mode + ' mode'; }
    });
    var healable = active.filter(function (f) { return has(info.healable, f); });
    var heal = d.byId('heal-btn');
    heal.disabled = busy || healable.length === 0;
    heal.title = healable.length
      ? 'Clear ' + healable.join(', ') + ' on ' + target
      : has(active, 'corrupt') ? 'Corrupt state cannot be healed; only Reset clears it' : 'Nothing to heal on ' + (target || 'this node');
    var reset = d.byId('reset-btn');
    reset.hidden = !info.can_reset;
    reset.disabled = busy;
  }

  // ---------- recover callout (scenario step 5) ----------

  function recover(show, info, nodes, actions, busy) {
    var box = d.byId('recover');
    box.hidden = !show;
    if (!show) { return; }
    var healable = nodes.filter(function (n) {
      return (n.faults || []).some(function (f) { return has(info.healable, f); });
    });
    var corrupt = nodes.filter(function (n) { return has(n.faults, 'corrupt'); }).map(function (n) { return n.name; });
    var text;
    if (info.mode === 'sim') {
      text = 'Heal a crashed, silent or slow node to watch it catch up. ' +
        (corrupt.length ? 'Corrupt state on ' + d.list(corrupt) + ' cannot be healed: ' : 'Corrupt state cannot be healed: ') +
        'Reset starts a fresh, empty cluster to run the scenario again.';
    } else {
      text = 'In the Docker cluster a corrupt or crashed replica cannot be undone from here: ' +
        'restart the cluster with experiments/demo_up.sh to run the scenario again.';
    }
    d.byId('recover-text').textContent = text;
    // Rebuild the buttons only when they change, so a periodic redraw never
    // swaps a button out from under a click.
    var key = JSON.stringify([healable.map(function (n) { return n.name; }), info.can_reset, !!busy]);
    if (box.getAttribute('data-key') === key) { return; }
    box.setAttribute('data-key', key);
    var buttons = healable.map(function (n) {
      return h('button', { className: 'btn', text: 'Heal ' + n.name, attrs: { type: 'button', disabled: busy }, on: { click: function () { actions.heal(n.name); } } });
    });
    if (info.can_reset) {
      buttons.push(h('button', { className: 'btn', text: 'Reset cluster', attrs: { type: 'button', disabled: busy }, on: { click: actions.reset } }));
    }
    d.replace(d.byId('recover-actions'), buttons);
  }

  VD.cluster = {
    reference: reference,
    header: header,
    connection: connection,
    scenario: scenario,
    nodeSelect: nodeSelect,
    banner: banner,
    replicas: replicas,
    blocks: blocks,
    metrics: metrics,
    log: log,
    load: load,
    faults: faults,
    recover: recover
  };
})(window.VD = window.VD || {});
