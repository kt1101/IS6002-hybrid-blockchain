// api.js — talks to the demo server: JSON requests and the SSE state stream.
(function (VD) {
  'use strict';

  // request sends one JSON request and resolves to {ok, status, data}. ok is
  // false for HTTP errors and network failures; data.error then says why.
  function request(method, path, body) {
    var opts = { method: method, headers: {} };
    if (body !== undefined) {
      opts.headers['Content-Type'] = 'application/json';
      opts.body = JSON.stringify(body);
    }
    return fetch(path, opts).then(function (res) {
      return res.text().then(function (text) {
        var data = null;
        if (text) {
          try { data = JSON.parse(text); } catch (e) { data = { error: text.trim() }; }
        }
        if (!res.ok && !(data && data.error)) {
          data = { error: 'HTTP ' + res.status };
        }
        return { ok: res.ok, status: res.status, data: data || {} };
      });
    }, function (err) {
      return { ok: false, status: 0, data: { error: 'server unreachable (' + err.message + ')' } };
    });
  }

  var MIN_BACKOFF = 1000;
  var MAX_BACKOFF = 5000;
  var KINDS = ['nodes', 'consistency', 'block', 'metrics', 'log', 'load'];

  // connect opens the event stream and keeps it open. handlers:
  //   onState(snapshot)   full state (first event, and after every reconnect)
  //   onUpdate(kind, data) one hub update
  //   onStatus(status)    'connecting' | 'connected' | 'reconnecting'
  // On a dropped stream it retries with a backoff from 1 s to 5 s and, once
  // back, refetches /api/state so nothing missed while away is lost.
  function connect(handlers) {
    var backoff = MIN_BACKOFF;
    var everConnected = false;
    var source = null;

    function open() {
      handlers.onStatus(everConnected ? 'reconnecting' : 'connecting');
      source = new EventSource('/api/events');
      source.addEventListener('state', function (ev) {
        var snap = parse(ev.data);
        if (!snap) { return; }
        var reconnected = everConnected;
        everConnected = true;
        backoff = MIN_BACKOFF;
        handlers.onStatus('connected');
        handlers.onState(snap);
        if (reconnected) { refetch(); }
      });
      KINDS.forEach(function (kind) {
        source.addEventListener(kind, function (ev) {
          var data = parse(ev.data);
          if (data !== undefined) { handlers.onUpdate(kind, data); }
        });
      });
      source.onerror = function () {
        // Take over from EventSource's own retry so the backoff is ours.
        source.close();
        handlers.onStatus(everConnected ? 'reconnecting' : 'connecting');
        setTimeout(open, backoff);
        backoff = Math.min(backoff * 2, MAX_BACKOFF);
      };
    }

    function refetch() {
      request('GET', '/api/state').then(function (r) {
        if (r.ok) { handlers.onState(r.data); }
      });
    }

    open();
  }

  function parse(text) {
    try { return JSON.parse(text); } catch (e) { return undefined; }
  }

  VD.api = {
    get: function (path) { return request('GET', path); },
    post: function (path, body) { return request('POST', path, body === undefined ? {} : body); },
    del: function (path) { return request('DELETE', path); },
    connect: connect
  };
})(window.VD = window.VD || {});
