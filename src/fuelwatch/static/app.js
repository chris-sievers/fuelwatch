/* Fuelwatch dashboard.
 *
 * Built for the strict Content Security Policy (default-src 'self'):
 * - no inline script or style, no eval, no third-party requests;
 * - no element.style anywhere: layout is classes in app.css. Chart.js
 *   writes canvas.style when it owns a DOM canvas, so each chart draws on
 *   an OffscreenCanvas through CspPlatform below and is copied to the
 *   visible <canvas> after each render. Chart.js never touches the DOM.
 * - text from the server (event labels, status) is set with textContent,
 *   never innerHTML, and links are checked to be https.
 *
 * One request per time range: /api/dashboard?range=war|1y|5y.
 */
'use strict';

(function () {
  const RANGE_NAMES = { war: 'Since the war began', '1y': '1 year', '5y': '5 years' };
  const DAY = 86400000;
  const TILE_SERIES = {
    brent: 'eia.spot.brent',
    gasoline: 'eia.retail.gasoline.us',
    diesel: 'eia.retail.diesel.us',
    tank: 'eia.retail.gasoline.us',
    hormuz: 'portwatch.hormuz.all',
    crude_stocks: 'eia.stocks.crude_commercial',
  };
  const STATUS_WORDS = {
    ok: 'OK',
    late: 'Late',
    down: 'Down',
    backfilling: 'Loading history',
    partial: 'Some history missing',
  };
  const ERROR_WORDS = {
    transport: 'Could not reach the source.',
    http_status: 'The source returned an error.',
    auth: 'The source turned down our key.',
    config: 'This copy is missing a setting for this source.',
    schema: 'The source sent data in an unexpected shape.',
    bad_json: 'The source sent data in an unexpected shape.',
    bad_xlsx: 'The source sent data in an unexpected shape.',
    bad_content: 'The source sent data in an unexpected shape.',
    empty: 'The source sent no data.',
  };

  // --- small helpers -------------------------------------------------------

  const fmtDay = new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' });
  const fmtDayYear = new Intl.DateTimeFormat('en-US', {
    month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC',
  });
  const fmtMonth = new Intl.DateTimeFormat('en-US', { month: 'short', year: 'numeric', timeZone: 'UTC' });
  const fmtTime = new Intl.DateTimeFormat('en-US', {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false, timeZone: 'UTC',
  });
  let thisYear = new Date().getUTCFullYear();

  function ms(iso) {
    return Date.parse(iso.length === 10 ? iso + 'T00:00:00Z' : iso);
  }
  function dateText(iso) {
    if (!iso) return 'no date';
    const t = ms(iso);
    return new Date(t).getUTCFullYear() === thisYear ? fmtDay.format(t) : fmtDayYear.format(t);
  }
  function timeText(iso) {
    return iso ? fmtTime.format(ms(iso)) + ' UTC' : 'never';
  }

  /** el('p', {class: 'x', text: 'y'}, child, ...). No style, no on* handlers. */
  function el(tag, attrs, ...children) {
    const node = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs || {})) {
      if (v === null || v === undefined || v === false) continue;
      if (k === 'text') node.textContent = String(v);
      else if (k === 'class') node.className = v;
      else if (k === 'style' || k.startsWith('on')) throw new Error('not allowed: ' + k);
      else node.setAttribute(k, v === true ? '' : String(v));
    }
    for (const c of children) {
      if (c === null || c === undefined || c === false) continue;
      node.append(typeof c === 'string' ? document.createTextNode(c) : c);
    }
    return node;
  }

  function safeHttps(url) {
    try {
      const u = new URL(url);
      return u.protocol === 'https:' && u.hostname ? u.href : null;
    } catch (e) {
      return null;
    }
  }

  const UNITS = {
    '$/bbl': { money: true, digits: 2, words: 'a barrel' },
    '$/gal': { money: true, digits: 2, words: 'a gallon', cents: true },
    $: { money: true, digits: 2, words: '' },
    'EUR/l': { euro: true, digits: 2, words: 'a litre' },
    'million bbl': { digits: 1, words: 'million barrels' },
    '%': { digits: 1, suffix: '%', words: 'of capacity' },
    '% of normal': { digits: 0, suffix: '%', words: 'of normal', points: true },
    'ships/day': { digits: 0, words: 'ships a day' },
    ships: { digits: 0, words: 'ships' },
    't/day': { digits: 0, words: 'tonnes a day' },
  };
  function unitOf(u) {
    return UNITS[u] || { digits: 1, words: '' };
  }
  function num(v, digits) {
    return v.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits });
  }
  /** "$98.89", "€1.84", "390.1", "24%". */
  function value(v, u) {
    if (v === null || v === undefined) return 'not available';
    const f = unitOf(u);
    const n = num(Math.abs(v), f.digits);
    const sign = v < 0 ? '-' : '';
    if (f.money) return sign + '$' + n;
    if (f.euro) return sign + '€' + n;
    return sign + n + (f.suffix || '');
  }
  function valueWithUnit(v, u) {
    const f = unitOf(u);
    return f.words ? value(v, u) + ' ' + f.words : value(v, u);
  }
  /** "up 42 cents (1.1%)", "down 3 points", "little changed". */
  function changeText(c, u) {
    if (!c || c.value === null || c.value === undefined) return 'not available yet';
    const f = unitOf(u);
    const flat = f.points ? Math.round(Math.abs(c.value)) === 0 : c.percent !== null && Math.abs(c.percent) < 0.5;
    if (c.value === 0 || flat) {
      return 'little changed';
    }
    const dir = c.value > 0 ? 'up ' : 'down ';
    const a = Math.abs(c.value);
    let amount;
    if (f.points) amount = num(a, 0) + (Math.round(a) === 1 ? ' point' : ' points');
    else if (f.cents && a < 0.995) amount = Math.round(a * 100) + (Math.round(a * 100) === 1 ? ' cent' : ' cents');
    else if (f.money) amount = '$' + num(a, f.digits);
    else if (f.euro) amount = '€' + num(a, f.digits);
    else amount = num(a, f.digits) + (f.suffix || '') + (f.words === 'million barrels' ? ' million barrels' : '');
    const pct = !f.points && c.percent !== null && c.percent !== undefined ? ' (' + num(Math.abs(c.percent), 1) + '%)' : '';
    return dir + amount + pct;
  }

  // --- colours from the stylesheet ------------------------------------------

  let C = {};
  function readColours() {
    const cs = getComputedStyle(document.documentElement);
    const names = [
      'surface', 'ink', 'ink-2', 'muted', 'grid', 'axis', 'brent', 'wti', 'gasoline', 'diesel',
      'tanker', 'ships', 'band', 'other', 'gap', 'stocks', 'normal',
    ];
    C = {};
    for (const n of names) C[n] = cs.getPropertyValue('--c-' + n).trim() || '#888';
    C.font = cs.getPropertyValue('--font').trim() || 'system-ui, sans-serif';
  }

  function hatch(colour) {
    const size = 8;
    const c = typeof OffscreenCanvas === 'function' ? new OffscreenCanvas(size, size) : document.createElement('canvas');
    c.width = size;
    c.height = size;
    const g = c.getContext('2d');
    g.strokeStyle = colour;
    g.lineWidth = 2;
    g.beginPath();
    g.moveTo(0, size);
    g.lineTo(size, 0);
    g.moveTo(-2, 2);
    g.lineTo(2, -2);
    g.moveTo(size - 2, size + 2);
    g.lineTo(size + 2, size - 2);
    g.stroke();
    return g.createPattern(c, 'repeat');
  }

  // --- Chart.js under the CSP ------------------------------------------------

  const VIEWS = new WeakMap(); // drawing surface -> visible canvas
  // Native pointer events -> the types Chart.js handles (as its DomPlatform
  // maps them). Chart.js filters plugin events on the native type, so
  // options.events lists the native names.
  const EVENT_TYPES = { pointermove: 'mousemove', pointerleave: 'mouseout', pointerdown: 'mousedown' };

  /** Draws off-screen and listens on the visible canvas. Chart.js's own
   * DomPlatform sets canvas.style (display, box-sizing, width, height);
   * this platform gives it a surface with no style to set. */
  class CspPlatform extends Chart.BasePlatform {
    acquireContext(canvas) {
      const surface = typeof OffscreenCanvas === 'function'
        ? new OffscreenCanvas(1, 1)
        : document.createElement('canvas'); // never attached to the page
      VIEWS.set(surface, canvas);
      return surface.getContext('2d');
    }
    releaseContext() {
      return true;
    }
    addEventListener(chart, type, listener) {
      const view = VIEWS.get(chart.canvas);
      const chartType = EVENT_TYPES[type];
      if (!view || !chartType) return;
      const proxy = (e) => {
        // A finger lifting ends with pointerleave; keep the tapped value shown.
        if (type === 'pointerleave' && e.pointerType === 'touch') return;
        const r = view.getBoundingClientRect();
        listener({ type: chartType, chart, native: e, x: e.clientX - r.left, y: e.clientY - r.top });
      };
      chart.$fwProxies = chart.$fwProxies || {};
      chart.$fwProxies[type] = [type, proxy];
      view.addEventListener(type, proxy, { passive: true });
    }
    removeEventListener(chart, type) {
      const view = VIEWS.get(chart.canvas);
      const p = chart.$fwProxies && chart.$fwProxies[type];
      if (view && p) view.removeEventListener(p[0], p[1]);
    }
    getDevicePixelRatio() {
      return window.devicePixelRatio || 1;
    }
    getMaximumSize(element, width, height) {
      return { width: Math.max(0, width || 0), height: Math.max(0, height || 0) };
    }
    isAttached() {
      return true;
    }
    updateConfig(config) {
      config.options.animation = false; // no animation frames to manage
    }
  }

  const copyToView = {
    id: 'fwCopy',
    afterRender(chart) {
      const view = VIEWS.get(chart.canvas);
      if (!view) return;
      const src = chart.canvas;
      if (view.width !== src.width) view.width = src.width;
      if (view.height !== src.height) view.height = src.height;
      const g = view.getContext('2d');
      g.clearRect(0, 0, view.width, view.height);
      g.drawImage(src, 0, 0);
      view.setAttribute('data-rendered', 'true');
    },
  };

  /** Lines labelled at their right-hand end, nudged apart. */
  const directLabels = {
    id: 'fwLabels',
    afterDatasetsDraw(chart) {
      const ctx = chart.ctx;
      const items = [];
      chart.data.datasets.forEach((ds, i) => {
        if (!ds.fwLabel || !chart.isDatasetVisible(i)) return;
        const meta = chart.getDatasetMeta(i);
        for (let j = meta.data.length - 1; j >= 0; j--) {
          const p = meta.data[j];
          const raw = ds.data[j];
          if (raw && raw.y !== null && p && Number.isFinite(p.y)) {
            items.push({ text: ds.fwLabel, x: p.x, y: p.y, colour: ds.fwLabelColour || C.ink });
            break;
          }
        }
      });
      items.sort((a, b) => a.y - b.y);
      for (let i = 1; i < items.length; i++) {
        if (items[i].y - items[i - 1].y < 13) items[i].y = items[i - 1].y + 13;
      }
      ctx.save();
      ctx.font = '12px ' + C.font;
      ctx.textBaseline = 'middle';
      for (const it of items) {
        ctx.fillStyle = it.colour;
        ctx.fillRect(it.x + 4, it.y - 1, 6, 2);
        ctx.fillStyle = C['ink-2'];
        ctx.fillText(it.text, it.x + 13, it.y);
      }
      ctx.restore();
    },
  };

  /** Vertical markers: the war's start and numbered event markers. */
  const markers = {
    id: 'fwMarkers',
    afterDatasetsDraw(chart, args, opts) {
      const list = (opts && opts.items) || [];
      if (!list.length) return;
      const { ctx, chartArea: a, scales } = chart;
      const x = scales.x;
      ctx.save();
      ctx.font = '11px ' + C.font;
      ctx.textBaseline = 'top';
      for (const m of list) {
        const px = x.getPixelForValue(m.x);
        if (px < a.left || px > a.right) continue;
        ctx.strokeStyle = C.muted;
        ctx.lineWidth = 1;
        ctx.setLineDash(m.strong ? [] : [3, 3]);
        ctx.beginPath();
        ctx.moveTo(px, a.top);
        ctx.lineTo(px, a.bottom);
        ctx.stroke();
        ctx.fillStyle = C['ink-2'];
        // The war label sits at the top, event numbers at the bottom.
        const y = m.strong ? a.top + 2 : a.bottom - 14;
        ctx.fillText(m.text, Math.min(px + 3, a.right - ctx.measureText(m.text).width), y);
      }
      ctx.restore();
    },
  };

  /** Tap or hover: the nearest point of each dataset, within 30 px. */
  Chart.Interaction.modes.fwDate = function (chart, e) {
    const out = [];
    chart.data.datasets.forEach((ds, i) => {
      if (ds.fwNoTip || !chart.isDatasetVisible(i)) return;
      const meta = chart.getDatasetMeta(i);
      let best = -1;
      let dist = Infinity;
      meta.data.forEach((p, j) => {
        const raw = ds.data[j];
        if (!raw || raw.y === null) return;
        const d = Math.abs(p.x - e.x);
        if (d < dist) {
          dist = d;
          best = j;
        }
      });
      if (best >= 0 && dist <= 30) out.push({ element: meta.data[best], datasetIndex: i, index: best });
    });
    return out;
  };

  Chart.register(copyToView, directLabels, markers);

  const charts = [];
  const observer = typeof ResizeObserver === 'function'
    ? new ResizeObserver((entries) => {
      for (const entry of entries) {
        const chart = entry.target.fwChart;
        if (chart) sizeChart(chart, entry.target);
      }
    })
    : null;

  function sizeChart(chart, box) {
    const w = Math.round(box.clientWidth);
    const h = Math.round(box.clientHeight);
    if (w > 0 && h > 0 && (w !== chart.width || h !== chart.height)) chart.resize(w, h);
  }

  function destroyCharts() {
    while (charts.length) {
      const c = charts.pop();
      if (observer) observer.unobserve(c.$fwBox);
      c.destroy();
    }
  }

  function monthTicks(scale) {
    const lo = scale.min;
    const hi = scale.max;
    const months = [];
    const d = new Date(lo);
    let y = d.getUTCFullYear();
    let m = d.getUTCMonth() + 1;
    for (;;) {
      if (m > 11) {
        m = 0;
        y += 1;
      }
      const t = Date.UTC(y, m, 1);
      if (t > hi) break;
      months.push(t);
      m += 1;
    }
    const step = Math.max(1, Math.ceil(months.length / (scale.width < 360 ? 3 : 6)));
    scale.ticks = months.filter((_, i) => i % step === 0).map((v) => ({ value: v }));
  }

  /** Points [[date, value, ...]] -> {x, y}, with a null where a gap is
   * longer than `gapDays`, so missing data shows as a break, never a line. */
  function xy(points, gapDays, pick) {
    const out = [];
    let prev = null;
    for (const p of points || []) {
      const x = ms(p[0]);
      if (prev !== null && gapDays && x - prev > gapDays * DAY) out.push({ x: (x + prev) / 2, y: null });
      out.push({ x, y: pick ? pick(p) : p[1], p });
      prev = x;
    }
    return out;
  }

  function baseOptions(unit, range, opts) {
    const o = opts || {};
    return {
      responsive: false,
      maintainAspectRatio: false,
      animation: false,
      parsing: false,
      normalized: true,
      events: ['pointermove', 'pointerleave', 'pointerdown'],
      interaction: { mode: 'fwDate', intersect: false },
      layout: { padding: { right: o.labelRoom || 0, top: o.top || 4 } },
      font: { family: C.font },
      scales: {
        x: {
          type: 'linear',
          display: !o.spark,
          min: range ? ms(range.start) : undefined,
          max: range ? ms(range.end) : undefined,
          afterBuildTicks: monthTicks,
          ticks: { color: C['ink-2'], maxRotation: 0, callback: (v) => fmtMonth.format(v), font: { size: 11 } },
          grid: { display: false },
          border: { color: C.axis },
        },
        y: {
          display: !o.spark,
          min: o.yMin,
          max: o.yMax,
          grace: o.spark ? '10%' : '5%',
          ticks: {
            color: C['ink-2'],
            maxTicksLimit: o.small ? 4 : 6,
            font: { size: 11 },
            callback: (v) => value(v, unit),
          },
          grid: { color: C.grid },
          border: { display: false },
        },
      },
      plugins: {
        legend: { display: false },
        fwMarkers: { items: o.markers || [] },
        tooltip: {
          enabled: true,
          backgroundColor: C.surface,
          titleColor: C.ink,
          bodyColor: C.ink,
          borderColor: C.axis,
          borderWidth: 1,
          displayColors: true,
          callbacks: {
            title: (items) => (items.length ? fmtDayYear.format(items[0].parsed.x) : ''),
            label: (item) => {
              const ds = item.dataset;
              const raw = ds.data[item.dataIndex];
              const u = ds.fwUnit || unit;
              let text = (ds.fwTip || ds.fwLabel || '') + ': ' + value(item.parsed.y, u);
              if (raw && raw.p && raw.p[2] === true) text += ' (may still change)';
              return text;
            },
          },
        },
      },
    };
  }

  function makeChart(box, config) {
    const canvas = el('canvas', { role: 'img', 'aria-label': config.aria || '' });
    box.append(canvas);
    const chart = new Chart(canvas, Object.assign({ platform: CspPlatform }, config));
    chart.$fwBox = box;
    box.fwChart = chart;
    charts.push(chart);
    sizeChart(chart, box);
    if (observer) observer.observe(box);
    return chart;
  }

  function line(label, points, colour, extra) {
    return Object.assign({
      type: 'line',
      label,
      fwLabel: label,
      fwLabelColour: colour,
      data: points,
      borderColor: colour,
      backgroundColor: colour,
      borderWidth: 2,
      pointRadius: 0,
      pointHoverRadius: 4,
      spanGaps: false,
      tension: 0,
    }, extra || {});
  }

  // --- figures ---------------------------------------------------------------

  let DATA = null;

  function meta(id) {
    return (DATA.series && DATA.series[id]) || null;
  }

  /** "Weekly, data through Oct 12", with a Late flag. */
  function asOf(ids, opts) {
    const o = opts || {};
    const ms_ = ids.map(meta).filter(Boolean);
    const p = el('p', { class: 'asof' });
    if (!ms_.length) return p;
    const how = ms_[0].how_often;
    const dates = ms_.map((m) => m.as_of).filter(Boolean).sort();
    const through = o.date || dates[dates.length - 1] || null;
    p.append(through ? how + ', data through ' + dateText(through) : how + ', no data yet');
    if (o.mayChange) p.append('. The newest two days may still change');
    const late = ms_.filter((m) => m.late);
    if (late.length) {
      const due = late.map((m) => m.late_after).sort()[0];
      p.append(' ', el('strong', { class: 'flag flag-late', text: 'Late' }),
        ' New data was expected by ' + dateText(due.slice(0, 10)) + '.');
    }
    return p;
  }

  function creditLine(text) {
    return el('p', { class: 'credit', 'data-attribution': true, text });
  }

  /** A figure: title, chart box, as-of, source, and View as table. */
  function figure(key, opts) {
    const fig = el('figure', { class: 'figure ' + (opts.wide ? 'wide' : ''), 'data-figure': key, id: 'fig-' + key });
    const cap = el('figcaption', {}, el(opts.small ? 'h4' : 'h3', { text: opts.title }));
    if (opts.note) cap.append(el('p', { class: 'note', text: opts.note }));
    fig.append(cap);
    let box = null;
    if (opts.chart !== false) {
      box = el('div', { class: 'chart ' + (opts.size || 'chart-md') });
      fig.append(box);
    }
    if (opts.after) fig.append(opts.after);
    fig.append(opts.asof);
    if (opts.credit) fig.append(creditLine(opts.credit));
    if (opts.table) fig.append(tableFold(opts.table));
    return { fig, box };
  }

  /** <details>View as table</details>, built when first opened. */
  function tableFold(spec) {
    const d = el('details', { class: 'as-table' }, el('summary', { text: 'View as table' }));
    let built = false;
    d.addEventListener('toggle', () => {
      if (!d.open || built) return;
      built = true;
      d.append(buildTable(spec.columns, spec.rows(), spec.caption));
    });
    return d;
  }

  function buildTable(columns, rows, caption) {
    const t = el('table', { class: 'cards' });
    if (caption) t.append(el('caption', { text: caption }));
    t.append(el('thead', {}, el('tr', {}, ...columns.map((c) => el('th', { scope: 'col', text: c })))));
    const body = el('tbody');
    for (const r of rows) {
      body.append(el('tr', {}, ...r.map((cell, i) => el(i === 0 ? 'th' : 'td', {
        'data-label': columns[i],
        scope: i === 0 ? 'row' : null,
        text: cell,
      }))));
    }
    t.append(body);
    return el('div', { class: 'table-wrap' }, t);
  }

  /** Table rows by date across several [[date, value]] lists, newest first. */
  function dateRows(lists, units, flagIdx) {
    const byDate = new Map();
    lists.forEach((pts, i) => {
      for (const p of pts || []) {
        if (!byDate.has(p[0])) byDate.set(p[0], new Array(lists.length).fill(''));
        let text = value(p[1], units[i]);
        if (flagIdx !== undefined && p[2] === true) text += ' (may still change)';
        byDate.get(p[0]).splice(i, 1, text);
      }
    });
    return [...byDate.keys()].sort().reverse().map((d) => [dateText(d), ...byDate.get(d)]);
  }

  function warMarker() {
    const ref = DATA.settings.reference_date;
    return { x: ms(ref) + DAY, text: 'War began', strong: true };
  }

  // --- the headline strip ---------------------------------------------------

  function renderHeadline() {
    const s = DATA.summary;
    const sentence = document.getElementById('summary');
    sentence.textContent = s.text;
    const sa = document.getElementById('summary-asof');
    sa.textContent = s.as_of
      ? 'Built from the tiles below. Its oldest number has data through ' + dateText(s.as_of) + '.'
      : '';
    const box = document.getElementById('tiles');
    box.replaceChildren();
    const ref = dateText(DATA.settings.reference_date);
    for (const t of s.tiles) box.append(tile(t, ref));
  }

  function tile(t, ref) {
    const sid = TILE_SERIES[t.key];
    const m = meta(sid);
    const art = el('article', { class: 'tile figure', 'data-figure': 'tile-' + t.key });
    art.append(el('h3', { text: t.label }));
    const f = unitOf(t.unit);
    const val = el('p', { class: 'value' }, el('span', { class: 'number', text: value(t.value, t.unit) }));
    let words = f.words;
    if (t.key === 'tank') words = 'for ' + num(DATA.settings.tank_gallons, 0) + ' gallons of gasoline';
    if (t.key === 'hormuz') words = 'of normal';
    if (words && t.value !== null) val.append(' ', el('span', { class: 'unit', text: words }));
    art.append(val);
    art.append(el('p', { class: 'change' }, el('span', { class: 'when', text: 'Since the last reading: ' }), changeText(t.since_last, t.unit)));
    art.append(el('p', { class: 'change' }, el('span', { class: 'when', text: 'Since the war began (' + ref + '): ' }), changeText(t.since_war, t.unit)));
    if (t.typical_range && t.typical_range.position) {
      const r = t.typical_range;
      art.append(el('p', { class: 'change' }, (r.position === 'within' ? 'Within' : r.position === 'below' ? 'Below' : 'Above') +
        ' the typical range for this time of year (' + num(r.low, 0) + ' to ' + num(r.high, 0) + ')'));
    }
    if (t.key === 'hormuz') {
      art.append(el('p', { class: 'note', text: 'Normal is the average of the ' + DATA.settings.baseline_days + ' days to ' + ref + '.' }));
    }
    const box = el('div', { class: 'chart chart-spark' });
    art.append(box);
    art.append(asOf([sid], { date: t.as_of }));
    if (m) art.append(creditLine(m.credit));
    const pts = (t.sparkline || []).map((p) => [p.day, p.value]);
    art.append(tableFold({
      columns: ['Date', t.label],
      rows: () => dateRows([pts], [t.unit]),
      caption: t.label + ', last 90 days',
    }));
    const colour = { brent: C.brent, gasoline: C.gasoline, diesel: C.diesel, tank: C.gasoline, hormuz: C.ships, crude_stocks: C.stocks }[t.key];
    queueMicrotask(() => {
      const opts = baseOptions(t.unit, null, { spark: true });
      makeChart(box, {
        type: 'line',
        aria: t.label + ', last 90 days',
        data: { datasets: [line(t.label, xy(pts, 10), colour, { fwLabel: null, fwTip: t.label, borderWidth: 1.5 })] },
        options: opts,
      });
    });
    return art;
  }

  // --- panels ---------------------------------------------------------------

  function renderCrude(root) {
    const c = DATA.crude;
    const brent = meta('eia.spot.brent');
    const evs = (DATA.events || []).filter((e) => e.date >= DATA.range.start && e.date <= DATA.range.end);
    const evList = el('ol', { class: 'events' });
    evs.forEach((e) => {
      const li = el('li', {}, el('span', { class: 'when', text: dateText(e.date) + ': ' }), e.label);
      const href = safeHttps(e.url);
      if (href) li.append(' ', el('a', { href, rel: 'noopener noreferrer', text: 'Source' }));
      evList.append(li);
    });
    const after = evs.length ? el('div', { class: 'events-box' }, el('p', { class: 'note', text: 'Numbered lines on the chart:' }), evList) : null;
    const { fig, box } = figure('crude', {
      title: 'Brent and WTI crude oil, $ a barrel',
      note: 'Brent is the world price; WTI is the US price.',
      size: 'chart-lg',
      wide: true,
      after,
      asof: asOf(['eia.spot.brent', 'eia.spot.wti']),
      credit: brent && brent.attribution,
      table: {
        columns: ['Date', 'Brent', 'WTI'],
        rows: () => dateRows([c.brent, c.wti], ['$/bbl', '$/bbl']),
      },
    });
    root.append(fig);
    const marks = [warMarker()].concat(evs.map((e, i) => ({ x: ms(e.date), text: String(i + 1) })));
    makeChart(box, {
      type: 'line',
      aria: 'Line chart of Brent and WTI crude prices',
      data: {
        datasets: [
          line('Brent', xy(c.brent, 3.5), C.brent),
          line('WTI', xy(c.wti, 3.5), C.wti),
        ],
      },
      options: baseOptions('$/bbl', DATA.range, { labelRoom: 52, markers: marks, top: 16 }),
    });

    const g = figure('crude-gap', {
      title: 'The gap: Brent minus WTI, $ a barrel',
      note: 'A wider gap means world oil is tighter than US oil.',
      size: 'chart-sm',
      wide: true,
      asof: asOf(['eia.spot.brent', 'eia.spot.wti']),
      credit: brent && brent.attribution,
      table: { columns: ['Date', 'Gap'], rows: () => dateRows([c.gap], ['$/bbl']) },
    });
    root.append(g.fig);
    makeChart(g.box, {
      type: 'bar',
      aria: 'Bar chart of the gap between Brent and WTI',
      data: {
        datasets: [{
          type: 'bar', label: 'Gap', fwTip: 'Brent minus WTI', data: xy(c.gap), backgroundColor: C.gap,
          barPercentage: 1, categoryPercentage: 1, borderWidth: 0,
        }],
      },
      options: baseOptions('$/bbl', DATA.range, { small: true, yMin: 0 }),
    });
  }

  function renderPump(root) {
    const p = DATA.pump;
    const gas = meta('eia.retail.gasoline.us');
    const { fig, box } = figure('pump', {
      title: 'US gasoline and diesel, $ a gallon',
      note: 'Solid lines: what drivers pay. Dashed lines: what stations pay for fuel at New York Harbor, which leads pump prices by one to two weeks.',
      size: 'chart-lg',
      wide: true,
      asof: el('div', {}, asOf(['eia.retail.gasoline.us', 'eia.retail.diesel.us']), asOf(['eia.spot.nyh_gasoline', 'eia.spot.nyh_ulsd'])),
      credit: gas && gas.attribution,
      table: {
        columns: ['Date', 'Gasoline', 'Diesel', 'Stations pay, gasoline', 'Stations pay, diesel'],
        rows: () => dateRows([p.gasoline, p.diesel, p.stations_gasoline, p.stations_diesel], ['$/gal', '$/gal', '$/gal', '$/gal']),
      },
    });
    root.append(fig);
    makeChart(box, {
      type: 'line',
      aria: 'Line chart of US gasoline and diesel prices',
      data: {
        datasets: [
          line('Gasoline', xy(p.gasoline, 10), C.gasoline),
          line('Diesel', xy(p.diesel, 10), C.diesel),
          line('Stations, gasoline', xy(p.stations_gasoline, 3.5), C.gasoline, { borderDash: [5, 4], borderWidth: 1.5 }),
          line('Stations, diesel', xy(p.stations_diesel, 3.5), C.diesel, { borderDash: [5, 4], borderWidth: 1.5 }),
        ],
      },
      options: baseOptions('$/gal', DATA.range, { labelRoom: 116, markers: [warMarker()], top: 16 }),
    });

    // regions
    const rows = p.regions.map((r) => [
      r.name,
      value(r.gasoline.value, '$/gal'),
      changeText(r.gasoline.since_last, '$/gal'),
      value(r.diesel.value, '$/gal'),
      changeText(r.diesel.since_last, '$/gal'),
    ]);
    const regionFig = figure('regions', {
      title: 'By region, $ a gallon',
      chart: false,
      wide: true,
      after: buildTable(['Region', 'Gasoline', 'Since last week', 'Diesel', 'Since last week'], rows),
      asof: asOf(['eia.retail.gasoline.us', 'eia.retail.diesel.us']),
      credit: gas && gas.credit,
    });
    root.append(regionFig.fig);

    const s = p.crude_share;
    const text = s.value === null
      ? 'Not enough data yet to estimate how much of the pump price is crude oil.'
      : 'Estimate: crude oil makes up about ' + num(s.value, 0) + '% of the US gasoline price. ' +
        'This uses Brent on ' + dateText(s.brent_date) + ', two weeks before the pump price, at 42 gallons to a barrel.';
    const share = figure('crude-share', {
      title: 'How much of the pump price is crude oil?',
      chart: false,
      wide: true,
      after: el('p', { class: 'one-line', text }),
      asof: asOf(['eia.retail.gasoline.us'], { date: s.as_of }),
      credit: meta('eia.spot.brent') && meta('eia.spot.brent').credit + ' Calculated by Fuelwatch.',
    });
    root.append(share.fig);

    // Europe
    const AREAS = [['eu27', 'EU average'], ['de', 'Germany'], ['fr', 'France'], ['it', 'Italy'], ['es', 'Spain']];
    for (const [fuel, title, colour] of [['petrol', 'Petrol', C.gasoline], ['diesel', 'Diesel', C.diesel]]) {
      const ids = AREAS.map(([a]) => 'eu_pump_' + fuel + '_' + a);
      const m = meta(ids[0]);
      const f = figure('eu-' + fuel, {
        title: 'Europe: ' + title.toLowerCase() + ', € a litre, with taxes',
        note: 'In euros, with taxes. Not comparable with US prices.',
        size: 'chart-md',
        asof: asOf(ids),
        credit: m && m.attribution,
        table: {
          columns: ['Date'].concat(AREAS.map((x) => x[1])),
          rows: () => dateRows(AREAS.map(([a]) => DATA.eu[fuel][a]), AREAS.map(() => 'EUR/l')),
        },
      });
      root.append(f.fig);
      makeChart(f.box, {
        type: 'line',
        aria: 'Line chart of ' + title.toLowerCase() + ' prices in Europe',
        data: {
          datasets: AREAS.map(([a, name], i) => line(name, xy(DATA.eu[fuel][a], 10), i === 0 ? colour : C.other, {
            borderWidth: i === 0 ? 2.5 : 1.25,
            fwUnit: 'EUR/l',
          })),
        },
        options: baseOptions('EUR/l', DATA.range, { labelRoom: 76, markers: [warMarker()], top: 16 }),
      });
    }
  }

  function renderHormuz(root) {
    const h = DATA.hormuz;
    const pw = meta('portwatch.hormuz.all');
    const ships = xy(h.all, 1.5);
    const tankers = xy(h.tankers, 1.5);
    const seven = xy(h.seven_day, 1.5);
    const n = h.normal;
    const lo = DATA.range.start;
    const hi = DATA.range.end;
    const bandPts = (v) => (v === null ? [] : [{ x: ms(lo), y: v }, { x: ms(hi), y: v }]);
    const hatchShips = hatch(C.ships);
    const hatchTanker = hatch(C.tanker);
    const prov = (pts, colour, hatched) => (ctx) => {
      const raw = pts[ctx.dataIndex];
      return raw && raw.p && raw.p[2] === true ? hatched : colour;
    };
    const normalNote = n.low === null
      ? 'Not enough days before the war to show a normal range.'
      : 'Shaded: the normal range, ' + num(n.low, 0) + ' to ' + num(n.high, 0) +
        ' ships a day (the 7-day average over the ' + DATA.settings.baseline_days + ' days to ' + dateText(n.end) + ').';
    const { fig, box } = figure('hormuz', {
      title: 'Ships crossing the Strait of Hormuz, per day',
      note: 'Grey bars: all ships. Yellow bars: tankers. Line: 7-day average. Hatched bars: the newest days, which may still change. ' + normalNote,
      size: 'chart-lg',
      wide: true,
      asof: asOf(['portwatch.hormuz.all', 'portwatch.hormuz.tankers'], { mayChange: true }),
      credit: pw && pw.attribution,
      table: {
        columns: ['Date', 'All ships', 'Tankers', '7-day average'],
        rows: () => dateRows([h.all, h.tankers, h.seven_day], ['ships/day', 'ships/day', 'ships/day'], 2),
      },
    });
    root.append(fig);
    makeChart(box, {
      type: 'bar',
      aria: 'Bar chart of ships crossing the Strait of Hormuz each day',
      data: {
        datasets: [
          line('7-day average', seven, C.ink, { order: 0, borderWidth: 2 }),
          {
            type: 'bar', label: 'Tankers', fwTip: 'Tankers', data: tankers, order: 1, grouped: false,
            backgroundColor: prov(tankers, C.tanker, hatchTanker), barPercentage: 1, categoryPercentage: 1, borderWidth: 0,
          },
          {
            type: 'bar', label: 'All ships', fwTip: 'All ships', data: ships, order: 2, grouped: false,
            backgroundColor: prov(ships, C.ships, hatchShips), barPercentage: 1, categoryPercentage: 1, borderWidth: 0,
          },
          line('Normal, low', bandPts(n.low), 'transparent', { order: 3, fwLabel: null, fwNoTip: true, borderWidth: 0 }),
          line('Normal', bandPts(n.high), 'transparent', {
            order: 3, fwLabel: 'Normal', fwLabelColour: C.normal, fwNoTip: true, borderWidth: 0, fill: '-1', backgroundColor: C.band,
          }),
        ],
      },
      options: baseOptions('ships/day', DATA.range, { labelRoom: 92, markers: [warMarker()], yMin: 0, top: 16 }),
    });

    // other chokepoints
    const grid = el('div', { class: 'small-multiples' });
    for (const cp of DATA.chokepoints) {
      const m = meta(cp.id);
      const now = cp.value === null ? 'Not enough data' : 'Now ' + num(cp.value, 0) + '% of normal';
      const f = figure('cp-' + cp.id.split('.')[1], {
        title: cp.name,
        small: true,
        note: now,
        size: 'chart-sm',
        asof: asOf([cp.id], { date: cp.as_of }),
        credit: m && m.credit,
        table: { columns: ['Date', '% of normal'], rows: () => dateRows([cp.points], ['% of normal']) },
      });
      grid.append(f.fig);
      makeChart(f.box, {
        type: 'line',
        aria: 'Line chart of ships crossing ' + cp.name + ' as a percent of normal',
        data: {
          datasets: [
            line(cp.name, xy(cp.points, 1.5), C.ships, { fwLabel: null, fwTip: 'Ships crossing, 7-day average', fwUnit: '% of normal' }),
            line('Normal', bandPts(100), C.muted, { borderDash: [3, 3], borderWidth: 1, fwNoTip: true, fwLabel: null }),
          ],
        },
        options: baseOptions('% of normal', DATA.range, { small: true, yMin: 0, markers: [warMarker()] }),
      });
    }
    const wrap = el('div', { class: 'figure-group wide' },
      el('h3', { text: 'Other sea routes, ships crossing as a percent of normal' }),
      el('p', { class: 'note', text: 'Normal is each route\'s average over the ' + DATA.settings.baseline_days + ' days before the war. Ships avoiding the Red Sea go around the Cape of Good Hope.' }),
      grid);
    root.append(wrap);

    // ships broadcasting now
    const a = DATA.ais;
    let line1;
    let when;
    if (!a.enabled) {
      line1 = 'Live counts are switched off on this copy of Fuelwatch.';
      when = 'Not live';
    } else if (!a.ships) {
      line1 = 'No live count yet.';
      when = 'Not live';
    } else {
      const t = a.tankers ? ' (' + num(a.tankers.value, 0) + ' tankers)' : '';
      line1 = num(a.ships.value, 0) + ' ships' + t + ' broadcasting their position in the strait' + (a.live ? ' now.' : ' at the last count.');
      when = a.live
        ? 'Live, every 10 minutes, as of ' + timeText(a.ships.at)
        : 'Not live: the last count was at ' + timeText(a.ships.at);
    }
    const ais = figure('ais-now', {
      title: 'Ships broadcasting their position in the strait now',
      chart: false,
      wide: true,
      after: el('div', {},
        el('p', { class: 'one-line', text: line1 }),
        el('p', { class: 'note', text: 'An indicator only, not an official count. Ships that switch off their position signal, or whose signal is jammed, are missed. It often differs from the weekly counts above.' })),
      asof: el('p', { class: 'asof' }, when, a.enabled && a.ships && !a.live ? el('strong', { class: 'flag flag-late', text: ' Not live' }) : null),
      credit: meta('hormuz_ais_ships') && meta('hormuz_ais_ships').credit,
    });
    root.append(ais.fig);
  }

  function renderStocks(root) {
    const grid = el('div', { class: 'small-multiples' });
    for (const s of DATA.stocks) {
      const m = meta(s.id);
      const pos = s.position ? (s.position === 'within' ? 'Within' : s.position === 'below' ? 'Below' : 'Above') + ' the typical range for this time of year' : 'Typical range not available yet';
      const f = figure('stocks-' + s.id.split('.')[2], {
        title: s.name + ', million barrels',
        small: true,
        note: (s.note ? s.note + '. ' : '') + pos + '.',
        size: 'chart-sm',
        asof: asOf([s.id]),
        credit: m && m.credit,
        table: {
          columns: ['Week ending', 'Stored', 'Typical low', 'Typical high'],
          rows: () => s.points.slice().reverse().map((p) => [dateText(p[0]), value(p[1], 'million bbl'), p[2] === null ? 'not available' : value(p[2], 'million bbl'), p[3] === null ? 'not available' : value(p[3], 'million bbl')]),
        },
      });
      grid.append(f.fig);
      makeChart(f.box, {
        type: 'line',
        aria: 'Line chart of stored ' + s.name.toLowerCase() + ' against its typical range',
        data: {
          datasets: [
            line(s.name, xy(s.points, 10), C.stocks, { fwLabel: null, fwTip: 'Stored', order: 0 }),
            line('Typical low', xy(s.points, 10, (p) => p[2]), 'transparent', { fwLabel: null, fwTip: 'Typical low', borderWidth: 0, order: 1 }),
            line('Typical high', xy(s.points, 10, (p) => p[3]), 'transparent', { fwLabel: null, fwTip: 'Typical high', borderWidth: 0, fill: '-1', backgroundColor: C.band, order: 1 }),
          ],
        },
        options: baseOptions('million bbl', DATA.range, { small: true, markers: [warMarker()] }),
      });
    }
    root.append(el('div', { class: 'figure-group wide' },
      el('h3', { text: 'US stored oil against its typical range' }),
      el('p', { class: 'note', text: 'Shaded: the lowest to highest level for the same week in the previous five years.' }),
      grid));

    const r = DATA.refinery;
    const f = figure('refinery', {
      title: 'Refineries in use, % of what they can run',
      note: 'Near 95% means refineries are running flat out.',
      size: 'chart-bar',
      wide: true,
      after: el('p', { class: 'one-line', text: r.value === null ? 'No data yet.' : value(r.value, '%') + ' in use in the latest week.' }),
      asof: asOf(['eia.refinery.utilization']),
      credit: meta('eia.refinery.utilization') && meta('eia.refinery.utilization').credit,
      table: {
        columns: ['Week ending', 'In use'],
        rows: () => [[dateText(r.as_of), value(r.value, '%')]],
      },
    });
    root.append(f.fig);
    const opts = baseOptions('%', null, {});
    opts.indexAxis = 'y';
    opts.scales = {
      x: { type: 'linear', min: 0, max: 100, ticks: { color: C['ink-2'], callback: (v) => v + '%', font: { size: 11 } }, grid: { color: C.grid }, border: { display: false } },
      y: { type: 'category', labels: ['In use'], display: false },
    };
    opts.interaction = { mode: 'nearest', intersect: false, axis: 'y' };
    opts.plugins.tooltip.callbacks = { title: () => 'Refineries in use', label: (i) => value(i.parsed.x, '%') };
    opts.plugins.fwMarkers = { items: [] };
    makeChart(f.box, {
      type: 'bar',
      aria: 'Bar showing refineries in use: ' + value(r.value, '%'),
      data: { labels: ['In use'], datasets: [{ label: 'In use', data: [r.value], backgroundColor: C.stocks, barPercentage: 0.7, categoryPercentage: 1, borderRadius: 4 }] },
      options: Object.assign(opts, { parsing: true }),
    });
  }

  function renderFooter() {
    const body = document.getElementById('sources-body');
    body.replaceChildren();
    for (const s of DATA.status.sources) {
      const word = STATUS_WORDS[s.status] || s.status;
      const statusCell = el('td', { 'data-label': 'Status' }, el('strong', { class: 'flag flag-' + s.status, text: word }));
      if (s.status === 'down') statusCell.append(' ' + (ERROR_WORDS[s.error_class] || 'The last check failed.'));
      if (s.status === 'late' && s.late_after) {
        statusCell.append(' New data was expected by ' + dateText(s.late_after.slice(0, 10)) + '.');
      }
      const live = s.collector === 'aisstream';
      body.append(el('tr', {},
        el('th', { scope: 'row', 'data-label': 'Source', 'data-attribution': true, text: s.source === 'EIA' ? 'U.S. Energy Information Administration' : s.source }),
        el('td', { 'data-label': 'Data through', text: s.data_through ? (live ? timeText(s.data_through) : dateText(s.data_through)) : 'no data yet' }),
        el('td', { 'data-label': 'Published', text: live ? 'Live' : s.published ? dateText(s.published) : 'not known' }),
        el('td', { 'data-label': 'Last checked', text: live ? (s.connected ? 'Connected now' : 'Not connected') : timeText(s.last_checked) }),
        statusCell));
    }
  }

  // --- page -----------------------------------------------------------------

  let currentRange = 'war';

  function renderAll() {
    readColours();
    destroyCharts();
    thisYear = new Date(ms(DATA.generated_at)).getUTCFullYear();
    document.getElementById('demo-banner').hidden = !DATA.demo;
    if (DATA.demo) document.title = 'Fuelwatch (demo data)';
    renderHeadline();
    for (const [id, fn] of [['crude-body', renderCrude], ['pump-body', renderPump], ['hormuz-body', renderHormuz], ['stocks-body', renderStocks]]) {
      const root = document.getElementById(id);
      root.replaceChildren();
      fn(root);
    }
    renderFooter();
    for (const b of document.querySelectorAll('[data-range]')) {
      b.setAttribute('aria-pressed', String(b.getAttribute('data-range') === currentRange));
    }
    document.getElementById('range-note').textContent = 'Charts show: ' + RANGE_NAMES[currentRange] + ' (' + dateText(DATA.range.start) + ' to ' + dateText(DATA.range.end) + ').';
    // Charts in tiles are made in a microtask; mark the page ready after.
    setTimeout(() => document.body.setAttribute('data-ready', 'true'), 0);
  }

  async function load(range) {
    document.body.removeAttribute('data-ready');
    try {
      const r = await fetch('/api/dashboard?range=' + encodeURIComponent(range), { headers: { accept: 'application/json' } });
      if (!r.ok) throw new Error('status ' + r.status);
      DATA = await r.json();
      currentRange = range;
      renderAll();
    } catch (e) {
      document.getElementById('summary').textContent = 'The numbers could not be loaded just now. Please try again in a few minutes.';
      document.body.setAttribute('data-ready', 'error');
    }
  }

  document.addEventListener('DOMContentLoaded', () => {
    for (const b of document.querySelectorAll('[data-range]')) {
      b.addEventListener('click', () => load(b.getAttribute('data-range')));
    }
    const mq = window.matchMedia('(prefers-color-scheme: dark)');
    const retheme = () => { if (DATA) renderAll(); };
    if (mq.addEventListener) mq.addEventListener('change', retheme);
    load('1y');
  });
}());
