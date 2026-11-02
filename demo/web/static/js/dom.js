// dom.js — small DOM and formatting helpers shared by the renderers. Data is
// only ever placed with textContent, never parsed as HTML.
(function (VD) {
  'use strict';

  var SVG_NS = 'http://www.w3.org/2000/svg';

  // h builds an element. props: className, text, attrs {name: value},
  // on {event: fn}. children: nodes or strings (as text), nulls skipped.
  function h(tag, props, children) {
    return build(document.createElement(tag), props, children);
  }

  // svg builds an SVG element; same arguments as h.
  function svg(tag, props, children) {
    return build(document.createElementNS(SVG_NS, tag), props, children);
  }

  function build(el, props, children) {
    props = props || {};
    if (props.className) { el.setAttribute('class', props.className); }
    if (props.text !== undefined && props.text !== null) { el.textContent = String(props.text); }
    var attrs = props.attrs || {};
    Object.keys(attrs).forEach(function (k) {
      if (attrs[k] !== undefined && attrs[k] !== null && attrs[k] !== false) { el.setAttribute(k, String(attrs[k])); }
    });
    var on = props.on || {};
    Object.keys(on).forEach(function (k) { el.addEventListener(k, on[k]); });
    (children || []).forEach(function (c) {
      if (c === null || c === undefined) { return; }
      el.appendChild(typeof c === 'string' ? document.createTextNode(c) : c);
    });
    return el;
  }

  // replace swaps el's children for the given ones.
  function replace(el, children) {
    while (el.firstChild) { el.removeChild(el.firstChild); }
    children.forEach(function (c) { if (c) { el.appendChild(c); } });
  }

  function byId(id) { return document.getElementById(id); }

  // digest shortens a hex hash to its first groups of four: a3f9·1c07·5e2b.
  function digest(hex, groups) {
    if (!hex) { return '—'; }
    groups = groups || 3;
    var parts = [];
    for (var i = 0; i < groups && i * 4 < hex.length; i++) { parts.push(hex.slice(i * 4, i * 4 + 4)); }
    return parts.join('·');
  }

  // longDigest shows the start and the end of a hash: a3f9·1c07·5e2b·…·90aa.
  function longDigest(hex) {
    if (!hex) { return '—'; }
    if (hex.length <= 16) { return digest(hex, 4); }
    return digest(hex, 3) + '·…·' + hex.slice(-4);
  }

  // clip shortens text to at most max characters, ending with an ellipsis.
  function clip(text, max) {
    text = String(text);
    return text.length <= max ? text : text.slice(0, max - 1) + '…';
  }

  function num(n) { return Number(n || 0).toLocaleString('en-US'); }

  // ms formats a latency: 74 ms, 1.31 s.
  function ms(v) {
    if (!v) { return '–'; }
    if (v < 1) { return '<1 ms'; }
    if (v < 10) { return v.toFixed(1) + ' ms'; }
    if (v < 1000) { return Math.round(v) + ' ms'; }
    return (v / 1000).toFixed(2) + ' s';
  }

  // list joins names: "node3", "node3 and node4", "node1, node3 and node4".
  function list(names) {
    if (names.length <= 1) { return names.join(''); }
    return names.slice(0, -1).join(', ') + ' and ' + names[names.length - 1];
  }

  VD.dom = { h: h, svg: svg, replace: replace, byId: byId, digest: digest, longDigest: longDigest, clip: clip, num: num, ms: ms, list: list };
})(window.VD = window.VD || {});
