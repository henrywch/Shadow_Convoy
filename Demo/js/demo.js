/* Shadow Convoy demo — snap-scroll + carousel + seed combobox + dynamic charts. */
(function () {
  var wrapper = document.getElementById('wrapper');
  var sections = [].slice.call(document.querySelectorAll('section'));
  var dots = [].slice.call(document.querySelectorAll('.nav button'));
  var bar = document.querySelector('.progress');
  var charts = [];
  var idx = 0, locking = false;
  var DUR = 560;

  // ── ECharts: static (window.CHARTS) + dynamic map charts ──────────────────
  function mapOption(kind) {
    if (!window.MAPDATA) return null;
    var cam = window.MAPDATA.cameras, seg = window.MAPDATA.segments;
    var ax = { show: false, scale: true, axisLine: { show: false }, splitLine: { show: false } };
    if (kind === 'camera') {
      return {
        backgroundColor: '#fff', title: { text: '相机网络地图', subtext: '853 个相机 · 可缩放/悬停', left: 'center', top: 6, textStyle: { fontSize: 15 }, subtextStyle: { fontSize: 11 } },
        tooltip: { formatter: function (p) { return '相机 ' + p.data[2]; } },
        grid: { top: 50, bottom: 16, left: 12, right: 12 }, xAxis: ax, yAxis: ax,
        series: [{ type: 'scatter', symbolSize: 5, data: cam, itemStyle: { color: '#ff9845', opacity: .8 },
          emphasis: { itemStyle: { color: '#37e1ff', borderColor: '#000' } } }]
      };
    }
    var wmax = seg.reduce(function (m, s) { return Math.max(m, s[4]); }, 1);
    var lines = seg.map(function (s) {
      return { coords: [[s[0], s[1]], [s[2], s[3]]], value: s[4],
        lineStyle: { width: 0.6 + 3.4 * s[4] / wmax } };
    });
    return {
      backgroundColor: '#fff', title: { text: '同行走廊流量', subtext: '群体流动（动画箭头）', left: 'center', top: 6, textStyle: { fontSize: 15 }, subtextStyle: { fontSize: 11 } },
      grid: { top: 50, bottom: 16, left: 12, right: 12 }, xAxis: ax, yAxis: ax,
      visualMap: { show: false, min: 0, max: wmax, dimension: 2, inRange: { color: ['#7c6cff', '#37e1ff', '#43e0a0', '#ffc24b'] } },
      series: [
        { type: 'scatter', symbolSize: 3, data: cam, itemStyle: { color: '#dde2ea' }, silent: true },
        { type: 'lines', coordinateSystem: 'cartesian2d', data: lines, polyline: false,
          effect: { show: true, period: 4, trailLength: .4, symbol: 'arrow', symbolSize: 5, color: 'rgba(55,225,255,.9)' },
          lineStyle: { color: '#7c6cff', opacity: .55, curveness: 0 } }
      ]
    };
  }
  function initCharts() {
    document.querySelectorAll('.chart').forEach(function (el) {
      var opt = null;
      if (el.dataset.chart) opt = window.CHARTS && window.CHARTS[el.dataset.chart];
      else if (el.dataset.mapchart) opt = mapOption(el.dataset.mapchart);
      if (!opt) { el.innerHTML = '<p style="color:#888;padding:20px">缺图</p>'; return; }
      var c = echarts.init(el, null, { renderer: 'canvas' });
      c.setOption(opt); charts.push(c);
    });
  }
  function resizeIn(section) {
    section.querySelectorAll('.chart').forEach(function (el) {
      var c = echarts.getInstanceByDom(el); if (c) c.resize();
    });
  }
  window.addEventListener('resize', function () { charts.forEach(function (c) { c.resize(); }); });

  // ── snap-scroll controller ────────────────────────────────────────────────
  function goTo(i) {
    if (locking || i < 0 || i >= sections.length) return;
    locking = true; idx = i;
    wrapper.style.transform = 'translateY(-' + i * 100 + 'vh)';
    bar.style.setProperty('--progress', (i / (sections.length - 1) * 100) + '%');
    dots.forEach(function (d, k) { d.classList.toggle('active', k === i); });
    setTimeout(function () { resizeIn(sections[i]); }, DUR * 0.7);
    setTimeout(function () { locking = false; }, DUR + 80);
  }
  // allow internal scroll inside .results / .combo-list before switching page
  function scrollableUnder(t, dy) {
    while (t && t !== document.body) {
      if (t.classList && (t.classList.contains('results') || t.classList.contains('combo-list'))) {
        var up = dy < 0 && t.scrollTop > 0;
        var down = dy > 0 && t.scrollTop + t.clientHeight < t.scrollHeight - 1;
        if (up || down) return true;
      }
      t = t.parentElement;
    }
    return false;
  }
  window.addEventListener('wheel', function (e) {
    if (scrollableUnder(e.target, e.deltaY)) return;   // let the inner list scroll
    e.preventDefault();
    if (locking) return;
    goTo(idx + (e.deltaY > 0 ? 1 : -1));
  }, { passive: false });

  var tY = 0;
  window.addEventListener('touchstart', function (e) { tY = e.touches[0].clientY; }, { passive: true });
  window.addEventListener('touchend', function (e) {
    if (locking) return;
    var dy = tY - e.changedTouches[0].clientY;
    if (Math.abs(dy) > 42) goTo(idx + (dy > 0 ? 1 : -1));
  }, { passive: true });

  var METHODS = sections.findIndex(function (s) { return s.id === 'methods'; });
  window.addEventListener('keydown', function (e) {
    if (e.key === 'ArrowDown' || e.key === 'PageDown') { e.preventDefault(); goTo(idx + 1); }
    if (e.key === 'ArrowUp' || e.key === 'PageUp') { e.preventDefault(); goTo(idx - 1); }
    if (idx === METHODS && e.key === 'ArrowRight') { e.preventDefault(); car.go(car.i + 1); }
    if (idx === METHODS && e.key === 'ArrowLeft') { e.preventDefault(); car.go(car.i - 1); }
  });
  dots.forEach(function (d, i) { d.addEventListener('click', function () { goTo(i); }); });

  // ── methods carousel ──────────────────────────────────────────────────────
  var car = { i: 0, n: 0, track: null, dots: null,
    go: function (j) {
      this.n = this.track.children.length;
      this.i = (j + this.n) % this.n;
      this.track.style.transform = 'translateX(-' + this.i * 100 + '%)';
      [].forEach.call(this.dots.children, function (b, k) { b.classList.toggle('on', k === car.i); });
      resizeIn(sections[METHODS]);
    } };
  function initCarousel() {
    car.track = document.querySelector('.car-track');
    car.dots = document.querySelector('.car-dots');
    if (!car.track) return;
    car.n = car.track.children.length;
    for (var k = 0; k < car.n; k++) {
      var b = document.createElement('button'); if (k === 0) b.className = 'on';
      (function (kk) { b.addEventListener('click', function () { car.go(kk); }); })(k);
      car.dots.appendChild(b);
    }
    document.querySelector('.car-arrow.prev').addEventListener('click', function () { car.go(car.i - 1); });
    document.querySelector('.car-arrow.next').addEventListener('click', function () { car.go(car.i + 1); });
  }

  // ── seed-vehicle combobox ─────────────────────────────────────────────────
  var DET = { conv: '共现', patt: '路径', emb: '嵌入' };
  function detBadges(list) {
    return ['conv', 'patt', 'emb'].map(function (d) {
      var on = list.indexOf(d) >= 0;
      return '<span class="det ' + (on ? d : 'off') + '">' + DET[d] + '</span>';
    }).join('');
  }
  function initSeed() {
    var input = document.getElementById('seedin');
    var list = document.getElementById('seedlist');
    var out = document.getElementById('seedout');
    if (!input || !window.SEED_LIST) return;
    var active = -1, shown = [];

    function renderList() {
      var q = input.value.trim();
      shown = window.SEED_LIST.filter(function (p) { return !q || p.indexOf(q) >= 0; }).slice(0, 60);
      active = -1;
      list.innerHTML = shown.map(function (p) {
        var d = window.SEED_DATA[p];
        return '<div class="combo-item" data-p="' + p + '">' + p +
          '<span class="mini">' + d.detectors.map(function (x) { return DET[x]; }).join('·') +
          ' · ' + d.companions.length + ' 伴随</span></div>';
      }).join('') || '<div class="combo-item" style="color:#667">无匹配车牌</div>';
      list.classList.add('show');
    }
    function hideList() { list.classList.remove('show'); }
    function query() {
      var p = input.value.trim();
      var d = window.SEED_DATA[p];
      hideList();
      if (!d) { out.innerHTML = '<p class="note">车牌 ' + (p || '—') + ' 不在已验证列表中，请从下拉框选择。</p>'; out.classList.add('show'); return; }
      out.innerHTML = '<p class="note">车牌 <b style="color:#fff">' + p + '</b>（' +
        d.detectors.map(function (x) { return DET[x]; }).join('+') + ' 登记）的伴随车 ' +
        d.companions.length + ' 辆，每行标注登记该车的检测器：</p>' +
        d.companions.map(function (c) {
          return '<div class="rrow"><span class="plate">' + c.plate + '</span>' +
            '<span class="agree">' + '★'.repeat(c.agree) + '☆'.repeat(3 - c.agree) + '</span>' +
            '<span class="dets">' + detBadges(c.detectors) + '</span></div>';
        }).join('');
      out.classList.add('show');
    }

    input.addEventListener('focus', renderList);
    input.addEventListener('input', function () { out.classList.remove('show'); renderList(); });
    input.addEventListener('keydown', function (e) {
      if (!list.classList.contains('show')) return;
      var items = list.querySelectorAll('.combo-item[data-p]');
      if (e.key === 'ArrowDown') { e.preventDefault(); active = Math.min(active + 1, items.length - 1); }
      else if (e.key === 'ArrowUp') { e.preventDefault(); active = Math.max(active - 1, 0); }
      else if (e.key === 'Enter') { e.preventDefault(); if (active >= 0 && items[active]) { input.value = items[active].dataset.p; } query(); return; }
      else return;
      items.forEach(function (it, k) { it.classList.toggle('active', k === active); });
      if (items[active]) items[active].scrollIntoView({ block: 'nearest' });
    });
    list.addEventListener('mousedown', function (e) {       // mousedown beats input blur
      var it = e.target.closest('.combo-item[data-p]');
      if (it) { input.value = it.dataset.p; hideList(); out.classList.remove('show'); }
    });
    document.getElementById('seedbtn').addEventListener('click', query);
    document.addEventListener('click', function (e) { if (!e.target.closest('.combo')) hideList(); });
  }

  // ── boot ──────────────────────────────────────────────────────────────────
  function boot() {
    initCharts(); initCarousel(); initSeed();
    goTo(0); resizeIn(sections[0]);
    var sp = location.search.match(/s=(\d+)/);   // ?s=N jumps instantly (for previews)
    if (sp) {
      wrapper.style.transition = 'none'; locking = false; goTo(+sp[1]);
      setTimeout(function () { wrapper.style.transition = ''; resizeIn(sections[+sp[1]]); }, 60);
    }
  }
  if (document.readyState !== 'loading') boot();
  else document.addEventListener('DOMContentLoaded', boot);
})();
