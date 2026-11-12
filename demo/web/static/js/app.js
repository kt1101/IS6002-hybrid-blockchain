// app.js — the page's state and wiring: applies the server's snapshot and
// updates, routes between the live view and #verify, and turns controls and
// scenario steps into API calls (the scenario uses the same calls as the
// manual controls; there is no shortcut).
(function (VD) {
  'use strict';

  var api = VD.api;
  var d = VD.dom;
  var C = VD.cluster;
  var V = VD.verify;

  var BLOCKS_KEPT = 30;
  var LOG_KEPT = 200;
  var NOTICE_MS = 10000;

  // The scenario's fixed values (spec §5.3).
  var DEMO = { key: 'user42', value: 'hello-committee', entry: 'node2', faultNode: 'node3', clients: 64, workload: 'a', durationS: 60 };

  var state = {
    info: {}, nodes: [], consistency: {}, blocks: [], metrics: {}, log: [],
    load: {}, tracked_key: '', scenario_step: 0
  };
  var conn = 'connecting';
  var busy = { tx: false, load: false, fault: false, step: false, verify: false };

  // ---------- state ----------

  function applySnapshot(s) {
    state = {
      info: s.info || {},
      nodes: s.nodes || [],
      consistency: s.consistency || {},
      blocks: s.blocks || [],
      metrics: s.metrics || {},
      log: s.log || [],
      load: s.load || {},
      tracked_key: s.tracked_key || '',
      scenario_step: s.scenario_step || 0
    };
    renderAll();
  }

  function onUpdate(kind, data) {
    switch (kind) {
      case 'nodes':
        state.nodes = data || [];
        renderNodes();
        break;
      case 'consistency':
        state.consistency = data || {};
        C.banner(state.consistency, state.nodes);
        break;
      case 'block':
        addBlock(data);
        C.blocks(state.blocks, C.reference(state.nodes));
        break;
      case 'metrics':
        state.metrics = data || {};
        C.metrics(state.metrics);
        break;
      case 'log':
        state.log.push(data);
        if (state.log.length > LOG_KEPT) { state.log = state.log.slice(-LOG_KEPT); }
        C.log(state.log);
        break;
      case 'load':
        state.load = data || {};
        C.load(state.load, busy.load);
        break;
    }
  }

  // addBlock appends a block. When the reference node changes (e.g. node1
  // crashed), the new reference may be behind: drop what it has not reached.
  function addBlock(b) {
    if (!b) { return; }
    state.blocks = state.blocks.filter(function (x) { return x.height < b.height; });
    state.blocks.push(b);
    if (state.blocks.length > BLOCKS_KEPT) { state.blocks = state.blocks.slice(-BLOCKS_KEPT); }
  }

  // ---------- rendering ----------

  function renderAll() {
    C.header(state.info, conn);
    C.scenario(state.scenario_step);
    renderNodes();
    C.banner(state.consistency, state.nodes);
    C.blocks(state.blocks, C.reference(state.nodes));
    C.metrics(state.metrics);
    C.log(state.log);
    C.load(state.load, busy.load);
  }

  function renderNodes() {
    C.nodeSelect(d.byId('tx-node'), state.nodes, DEMO.entry);
    C.nodeSelect(d.byId('fault-node'), state.nodes, DEMO.faultNode);
    C.nodeSelect(d.byId('verify-node'), state.nodes, DEMO.entry);
    C.replicas(state.nodes, state.tracked_key, d.byId('tx-node').value);
    renderControls();
  }

  function renderControls() {
    C.faults(state.info, state.nodes, d.byId('fault-node').value, busy.fault);
    C.recover(state.scenario_step === 5, state.info, state.nodes, { heal: heal, reset: reset }, busy.fault);
    d.byId('tx-submit').disabled = busy.tx;
    d.byId('verify-submit').disabled = busy.verify;
    Array.prototype.forEach.call(document.querySelectorAll('#scenario .step'), function (b) { b.disabled = busy.step; });
  }

  function setBusy(what, on) {
    busy[what] = on;
    renderControls();
    if (what === 'load') { C.load(state.load, busy.load); }
  }

  var noticeTimer = null;
  function notice(text) {
    var el = d.byId('notice');
    el.textContent = text;
    el.hidden = !text;
    clearTimeout(noticeTimer);
    if (text) { noticeTimer = setTimeout(function () { el.hidden = true; }, NOTICE_MS); }
  }

  function result(id, cls, text) {
    var el = d.byId(id);
    el.className = 'result' + (cls ? ' ' + cls : '') + (el.classList.contains('mono') ? ' mono' : '');
    el.textContent = text;
  }

  // ---------- routing ----------

  function route() {
    var verify = location.hash === '#verify';
    d.byId('view-live').hidden = verify;
    d.byId('view-verify').hidden = !verify;
    window.scrollTo(0, 0);
  }

  function showVerify() {
    if (location.hash !== '#verify') { location.hash = '#verify'; }
    route();
  }

  function showLive() {
    if (location.hash === '#verify') {
      history.pushState(null, '', location.pathname + location.search);
    }
    route();
  }

  // ---------- actions ----------

  function submitTx(key, value, node) {
    setBusy('tx', true);
    result('tx-result', '', 'Sending Set ' + key + ' to ' + node + '…');
    return api.post('/api/tx', { key: key, value: value, node: node }).then(function (r) {
      setBusy('tx', false);
      if (r.ok && r.data.ok) {
        state.tracked_key = key;
        renderNodes();
        // height is the hub's view and can lag the commit by a block.
        var at = r.data.height > 0 ? ' · ledger at #' + d.num(r.data.height) : '';
        result('tx-result', 'ok', 'Set ' + key + ' → committed in ' + r.data.latency_ms + ' ms' + at);
        return true;
      }
      result('tx-result', 'error', 'Set ' + key + ' failed: ' + r.data.error);
      return false;
    });
  }

  function startLoad(clients, workload, durationS) {
    setBusy('load', true);
    return api.post('/api/load', { clients: clients, workload: workload, duration_s: durationS }).then(function (r) {
      setBusy('load', false);
      if (!r.ok) {
        result('load-result', 'error', 'Could not start the load: ' + r.data.error);
        return false;
      }
      result('load-result', '', '');
      return true;
    });
  }

  function stopLoad() {
    setBusy('load', true);
    return api.del('/api/load').then(function (r) {
      setBusy('load', false);
      if (!r.ok) { result('load-result', 'error', 'Could not stop the load: ' + r.data.error); }
      return r.ok;
    });
  }

  function runVerify(key, node) {
    setBusy('verify', true);
    V.pending(key, node);
    return api.post('/api/verify', { key: key, node: node }).then(function (r) {
      setBusy('verify', false);
      if (!r.ok) {
        V.error(r.data.error);
        return false;
      }
      state.tracked_key = key;
      V.result(r.data);
      return true;
    });
  }

  function inject(kind, node) {
    setBusy('fault', true);
    result('fault-result', '', 'Injecting ' + kind + ' on ' + node + '…');
    return api.post('/api/fault', { kind: kind, node: node }).then(function (r) {
      setBusy('fault', false);
      result('fault-result', r.ok ? 'ok' : 'error', r.ok ? kind + ' injected on ' + node : 'Injecting ' + kind + ' on ' + node + ' failed: ' + r.data.error);
      return r.ok;
    });
  }

  function heal(node) {
    setBusy('fault', true);
    result('fault-result', '', 'Healing ' + node + '…');
    return api.post('/api/heal', { node: node }).then(function (r) {
      setBusy('fault', false);
      result('fault-result', r.ok ? 'ok' : 'error', r.ok ? node + ' healed' : 'Healing ' + node + ' failed: ' + r.data.error);
      return r.ok;
    });
  }

  function reset() {
    setBusy('fault', true);
    result('fault-result', '', 'Resetting the cluster…');
    return api.post('/api/reset').then(function (r) {
      setBusy('fault', false);
      if (!r.ok) {
        result('fault-result', 'error', 'Reset failed: ' + r.data.error);
        return false;
      }
      result('fault-result', 'ok', 'Cluster reset: fresh, empty replicas');
      result('tx-result', '', '');
      return api.get('/api/state').then(function (s) {
        if (s.ok) { applySnapshot(s.data); }
        return true;
      });
    });
  }

  function highlightBanner() {
    var el = d.byId('banner');
    el.classList.remove('highlight');
    void el.offsetWidth; // restart the animation
    el.classList.add('highlight');
    el.scrollIntoView({ behavior: 'smooth', block: 'center' });
    el.focus({ preventScroll: true });
  }

  // ---------- scenario ----------

  var STEPS = {
    1: function () {
      showLive();
      d.byId('tx-key').value = DEMO.key;
      d.byId('tx-value').value = DEMO.value;
      d.byId('tx-node').value = DEMO.entry;
      return submitTx(DEMO.key, DEMO.value, DEMO.entry);
    },
    2: function () {
      showLive();
      d.byId('load-clients').value = DEMO.clients;
      d.byId('load-clients-value').textContent = DEMO.clients;
      d.byId('load-workload').value = DEMO.workload;
      d.byId('load-duration').value = String(DEMO.durationS);
      return startLoad(DEMO.clients, DEMO.workload, DEMO.durationS);
    },
    3: function () {
      showVerify();
      d.byId('verify-key').value = DEMO.key;
      d.byId('verify-node').value = DEMO.entry;
      return runVerify(DEMO.key, DEMO.entry);
    },
    4: function () {
      showLive();
      if ((state.info.faults || []).indexOf('corrupt') < 0) {
        notice('Corrupt state is not available in this cluster.');
        return Promise.resolve(false);
      }
      d.byId('fault-node').value = DEMO.faultNode;
      renderControls();
      // Then re-send the scenario's write, so node3's value visibly diverges
      // (and its root moves) even when the load has already finished.
      return inject('corrupt', DEMO.faultNode).then(function (ok) {
        if (!ok) { return false; }
        d.byId('tx-key').value = DEMO.key;
        d.byId('tx-value').value = DEMO.value;
        d.byId('tx-node').value = DEMO.entry;
        return submitTx(DEMO.key, DEMO.value, DEMO.entry);
      });
    },
    5: function () {
      showLive();
      renderControls(); // reveals the Recover panel
      highlightBanner();
      return Promise.resolve(true);
    }
  };

  function runStep(step) {
    setBusy('step', true);
    notice('');
    return api.post('/api/scenario', { step: step }).then(function (r) {
      if (!r.ok) {
        notice('Step ' + step + ' failed: ' + r.data.error);
        return false;
      }
      state.scenario_step = step;
      C.scenario(step);
      return STEPS[step]();
    }).then(function (ok) {
      setBusy('step', false);
      if (ok === false && d.byId('notice').hidden) {
        notice('Step ' + step + ' did not complete; see the panel for the reason.');
      }
    });
  }

  // ---------- wiring ----------

  function wire() {
    Array.prototype.forEach.call(document.querySelectorAll('#scenario .step'), function (b) {
      b.addEventListener('click', function () { runStep(Number(b.getAttribute('data-step'))); });
    });

    d.byId('tx-form').addEventListener('submit', function (ev) {
      ev.preventDefault();
      submitTx(d.byId('tx-key').value, d.byId('tx-value').value, d.byId('tx-node').value);
    });
    d.byId('tx-node').addEventListener('change', function () { C.replicas(state.nodes, state.tracked_key, d.byId('tx-node').value); });

    d.byId('load-clients').addEventListener('input', function () {
      d.byId('load-clients-value').textContent = d.byId('load-clients').value;
    });
    d.byId('load-btn').addEventListener('click', function () {
      if (state.load && state.load.running) {
        stopLoad();
      } else {
        startLoad(Number(d.byId('load-clients').value), d.byId('load-workload').value, Number(d.byId('load-duration').value));
      }
    });

    d.byId('fault-node').addEventListener('change', renderControls);
    Array.prototype.forEach.call(document.querySelectorAll('#fault-grid .fault[data-kind]'), function (b) {
      b.addEventListener('click', function () { inject(b.getAttribute('data-kind'), d.byId('fault-node').value); });
    });
    d.byId('heal-btn').addEventListener('click', function () { heal(d.byId('fault-node').value); });
    d.byId('reset-btn').addEventListener('click', reset);

    d.byId('verify-form').addEventListener('submit', function (ev) {
      ev.preventDefault();
      runVerify(d.byId('verify-key').value, d.byId('verify-node').value);
    });
    d.byId('back-btn').addEventListener('click', showLive);
    window.addEventListener('hashchange', route);
  }

  wire();
  route();
  renderAll();
  api.connect({
    onState: applySnapshot,
    onUpdate: onUpdate,
    onStatus: function (s) {
      conn = s;
      C.connection(s);
    }
  });
})(window.VD = window.VD || {});
