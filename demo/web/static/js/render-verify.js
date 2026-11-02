// render-verify.js — draws the Merkle proof view: the verdict, the same key
// on every replica, and the proof path from the computed root (top) down to
// the leaf (bottom).
(function (VD) {
  'use strict';

  var d = VD.dom;
  var h = d.h;

  var TREE_DEPTH = 256;
  var COLLAPSE_OVER = 8; // more sibling levels than this: fold the middle ones
  var KEEP_EACH_END = 3;
  var VALUE_SHOWN = 48; // characters of a value in the verdict and leaf
  var CELL_SHOWN = 14;  // characters of a value in the replica table

  var expanded = false; // the folded middle levels are shown

  function verdictBox(cls, title, textChildren) {
    var box = d.byId('verdict');
    box.className = 'verdict ' + cls;
    d.byId('verdict-title').textContent = title;
    d.replace(d.byId('verdict-text'), textChildren.map(function (c) {
      return typeof c === 'string' ? document.createTextNode(c) : c;
    }));
  }

  function b(text) { return h('b', { text: text }); }

  function pending(key, node) {
    verdictBox('idle', 'Verifying…', ['Asking ' + node + ' and every other replica for ', b(key), '.']);
  }

  function error(message) {
    verdictBox('invalid', 'Verification failed', [message]);
    d.replace(d.byId('replica-rows'), []);
    d.byId('replica-note').textContent = '';
    d.replace(d.byId('proof-path'), [h('p', { className: 'muted', text: 'No proof: the replica did not answer.' })]);
  }

  function result(res) {
    expanded = false;
    verdict(res);
    replicaTable(res.replicas || []);
    path(res);
  }

  function askedReplica(res) {
    return (res.replicas || []).filter(function (r) { return r.node === res.node; })[0];
  }

  function verdict(res) {
    var siblings = Math.max((res.steps || []).length - 1, 0);
    if (!res.found) {
      verdictBox('idle', 'Key not found', [res.node + ' has no value for ', b(res.key), ' under root ', b(d.digest(res.root)), '.']);
      return;
    }
    if (!res.valid) {
      verdictBox('invalid', 'Proof invalid', ['The proof ' + res.node + ' returned for ', b(res.key),
        ' does not recompute its reported root ', b(d.digest(res.root)), ': the value or the proof was altered.']);
      return;
    }
    var mine = askedReplica(res);
    var minority = mine && mine.verdict !== 'valid · majority';
    var body = [res.key + ' = ', b(d.clip(res.value, VALUE_SHOWN)), ' (version ' + res.version + ') is included in root ', b(d.digest(res.root)),
      ', recomputed from the leaf through ' + siblings + ' sibling hash' + (siblings === 1 ? '' : 'es') +
      '; the other ' + (TREE_DEPTH - siblings) + ' levels are empty subtrees.'];
    if (minority) {
      body.push(' But this root is not the majority root: ' + res.node + ' disagrees with the other replicas.');
      verdictBox('invalid', 'Proof valid · root not majority', body);
    } else {
      verdictBox('valid', 'Proof valid', body);
    }
  }

  function verdictClass(v) {
    if (v === 'valid · majority') { return 'v-majority'; }
    if (v === 'unreachable' || v === 'not found') { return 'v-none'; }
    return 'v-bad';
  }

  function replicaTable(replicas) {
    d.replace(d.byId('replica-rows'), replicas.map(function (r) {
      var unreachable = r.verdict === 'unreachable';
      return h('tr', {}, [
        h('td', { className: 'node', text: r.node }),
        h('td', { className: 'mono nowrap', text: unreachable || !r.value ? '—' : d.clip(r.value, CELL_SHOWN), attrs: { title: r.value || null } }),
        h('td', { className: 'mono nowrap', text: unreachable ? 'unreachable' : d.digest(r.root, 2) }),
        h('td', { className: verdictClass(r.verdict), text: r.verdict })
      ]);
    }));
    var minority = replicas.filter(function (r) { return r.verdict === 'valid · minority root'; }).map(function (r) { return r.node; });
    var invalid = replicas.filter(function (r) { return r.verdict === 'invalid'; }).map(function (r) { return r.node; });
    var notes = [];
    if (minority.length) {
      notes.push(d.list(minority) + (minority.length === 1 ? ' is' : ' are') +
        ' self-consistent (the proof checks against its own root) but the root differs from the majority — this is how a client spots a lying replica.');
    }
    if (invalid.length) {
      notes.push(d.list(invalid) + ' returned a proof that does not match its own root.');
    }
    if (!notes.length && replicas.length) {
      notes.push('Every replica that answered agrees with the majority root.');
    }
    d.byId('replica-note').textContent = notes.join(' ');
  }

  // ---------- proof path ----------

  function link() { return h('div', { className: 'link', attrs: { 'aria-hidden': 'true' } }); }

  function hashBox(cls, label, value, title) {
    return h('div', { className: 'hashbox ' + cls, attrs: { title: title || null } }, [
      h('span', { className: 'hb-label', text: label }),
      h('span', { className: 'hb-hash', text: value })
    ]);
  }

  function levelRow(left, right) {
    return h('div', { className: 'level' }, [left, h('span', { className: 'plus', text: '+', attrs: { 'aria-hidden': 'true' } }), right]);
  }

  // path draws steps (leaf first, as the server sends them) root-first.
  // steps[0] is the leaf; steps[k] (k ≥ 1) combines the hash below it with a
  // sibling at tree level steps[k].level, so the path node at level L is
  // steps[k-1].hash and the last step's hash is the computed root.
  function path(res) {
    var box = d.byId('proof-path');
    var steps = res.steps || [];
    if (!res.found || !steps.length) {
      d.replace(box, [h('p', { className: 'muted', text: res.found ? 'The server returned no proof steps.' : 'There is no leaf for this key, so there is no inclusion path to draw.' })]);
      return;
    }
    var n = steps.length - 1;
    var computed = steps[n].hash;
    var match = computed === res.root;
    var out = [];

    out.push(hashBox('root' + (match ? '' : ' bad'),
      match ? 'COMPUTED ROOT = REPORTED ROOT' : 'COMPUTED ROOT ≠ REPORTED ROOT',
      d.longDigest(computed), 'computed ' + computed + '\nreported ' + res.root));

    var levels = [];
    for (var k = n; k >= 1; k--) { levels.push(k); } // level 1 (top) first
    var fold = !expanded && n > COLLAPSE_OVER;
    levels.forEach(function (k, i) {
      if (fold && i >= KEEP_EACH_END && i < n - KEEP_EACH_END) {
        if (i === KEEP_EACH_END) {
          var hidden = n - 2 * KEEP_EACH_END;
          var from = steps[k].level, to = steps[k - hidden + 1].level;
          out.push(link());
          out.push(levelRow(
            hashBox('', 'levels ' + from + '–' + to, '…'),
            h('button', { className: 'hashbox sibling', attrs: { type: 'button', 'aria-label': 'Show ' + hidden + ' more levels' }, on: { click: function () { expanded = true; path(res); } } }, [
              h('span', { className: 'hb-label', text: hidden + ' more siblings' }),
              h('span', { className: 'hb-hash', text: 'show all' })
            ])));
        }
        return;
      }
      var s = steps[k];
      var emptySibling = /^0*$/.test(s.sibling); // the tree's placeholder for an empty subtree
      out.push(link());
      out.push(levelRow(
        hashBox('', 'level ' + s.level + ' · path node', d.digest(steps[k - 1].hash), steps[k - 1].hash),
        hashBox('sibling' + (emptySibling ? ' empty' : ''), 'sibling (' + s.sibling_side + ')' + (emptySibling ? ' · empty subtree' : ''), d.digest(s.sibling), s.sibling)));
    });

    var leaf = steps[0];
    if (leaf.empty) {
      var first = leaf.level, last = leaf.level + leaf.span - 1;
      out.push(link());
      out.push(levelRow(
        hashBox('', 'levels ' + first + '–' + last + ' · leaf subtree', d.digest(leaf.hash), leaf.hash),
        hashBox('sibling empty', 'empty subtrees', 'default hash × ' + leaf.span)));
    }
    out.push(link());
    out.push(hashBox('leaf', 'LEAF · path = sha256("' + res.key + '")',
      'hash( path ‖ hash("' + d.clip(res.value, VALUE_SHOWN) + '-' + res.version + '") ) = ' + d.digest(leaf.hash), leaf.hash));
    d.replace(box, out);
  }

  VD.verify = { pending: pending, error: error, result: result };
})(window.VD = window.VD || {});
