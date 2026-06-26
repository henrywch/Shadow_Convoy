/* Shadow Convoy demo — render live ECharts from window.CHARTS + scroll UX. */
(function () {
  // 1. instantiate every .chart[data-chart] from the shared config
  var charts = [];
  function initCharts() {
    document.querySelectorAll('.chart[data-chart]').forEach(function (el) {
      var key = el.getAttribute('data-chart');
      var opt = window.CHARTS && window.CHARTS[key];
      if (!opt) { el.innerHTML = '<p style="color:#888;padding:20px">缺图: ' + key + '</p>'; return; }
      var c = echarts.init(el, null, { renderer: 'canvas' });
      c.setOption(opt);
      charts.push(c);
    });
  }
  window.addEventListener('resize', function () { charts.forEach(function (c) { c.resize(); }); });

  // 2. reveal-on-scroll
  var io = new IntersectionObserver(function (es) {
    es.forEach(function (e) { if (e.isIntersecting) e.target.classList.add('in'); });
  }, { threshold: 0.12 });
  document.querySelectorAll('.reveal').forEach(function (el) { io.observe(el); });

  // 3. dot nav active state + scroll progress
  var secs = [].slice.call(document.querySelectorAll('section[id]'));
  var dots = [].slice.call(document.querySelectorAll('.nav a'));
  var bar = document.querySelector('.progress');
  function onScroll() {
    var y = window.scrollY + window.innerHeight * 0.4, cur = 0;
    secs.forEach(function (s, i) { if (s.offsetTop <= y) cur = i; });
    dots.forEach(function (d, i) { d.classList.toggle('active', i === cur); });
    var h = document.body.scrollHeight - window.innerHeight;
    bar.style.width = (100 * window.scrollY / h) + '%';
  }
  window.addEventListener('scroll', onScroll, { passive: true });

  // 4. seed-vehicle query (canned result for the verified demo plate 393966)
  var SEED = {
    '393966': [
      { p: '490759', s: 3, src: 'registry 3-of-3 · ANN', ev: 'cos≈0.94' },
      { p: '541828', s: 3, src: 'registry 3-of-3 · ANN', ev: 'cos≈0.95' },
      { p: '513437', s: 3, src: 'registry 3-of-3 · ANN', ev: 'cos≈0.93' },
      { p: '494845', s: 3, src: 'registry 3-of-3 · ANN', ev: 'cos≈0.93' },
      { p: '395034', s: 2, src: 'registry 3-of-3', ev: '同组成员' },
      { p: '448756', s: 2, src: 'registry 2-of-3 · ANN', ev: 'cos≈0.95' }
    ]
  };
  function dots3(n) { return '★'.repeat(n) + '☆'.repeat(3 - n); }
  window.runSeed = function () {
    var v = (document.getElementById('seedin').value || '393966').trim();
    var box = document.getElementById('seedout');
    var rows = SEED[v] || SEED['393966'];
    box.innerHTML = '<p class="note">车牌 <b style="color:#fff">' + (SEED[v] ? v : '393966') +
      '</b> 的伴随车（三路证据融合，按一致来源数排序）：</p>' +
      rows.map(function (r) {
        return '<div class="rrow"><span class="plate">' + r.p + '</span>' +
          '<span class="dots3">' + dots3(r.s) + '</span>' +
          '<span class="src">' + r.src + '</span>' +
          '<span class="ev">' + r.ev + '</span></div>';
      }).join('');
    box.classList.add('show');
  };

  // screenshot mode: ?shot reveals everything immediately (no scroll needed)
  function param(k) { var m = location.search.match(new RegExp(k + '=([^&]+)')); return m && m[1]; }
  function maybeShot() {
    if (location.search.indexOf('shot') < 0) return;
    document.documentElement.style.scrollBehavior = 'auto';
    document.querySelectorAll('.reveal').forEach(function (e) { e.classList.add('in'); });
    var only = param('only');
    if (only) {                                   // isolate one section at the top
      document.querySelectorAll('section[id]').forEach(function (s) {
        if (s.id !== only) s.style.display = 'none';
      });
      var t = document.getElementById(only);
      if (t) { t.style.minHeight = 'auto'; t.style.paddingTop = '40px'; t.style.paddingBottom = '40px'; }
    }
  }
  if (document.readyState !== 'loading') { maybeShot(); initCharts(); onScroll(); }
  else document.addEventListener('DOMContentLoaded', function () { maybeShot(); initCharts(); onScroll(); });
})();
