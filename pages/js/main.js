/* ═══════════════════════════════════════════════════════════
   SHADOW CONVOY — main.js
   Snap-scroll page controller + per-section animation replay
   ═══════════════════════════════════════════════════════════ */

// ── DOM refs ────────────────────────────────────────────────
const wrapper     = document.getElementById('page-wrapper');
const progressBar = document.getElementById('scroll-progress');
const sections    = [...document.querySelectorAll('.section')];
const dots        = [...document.querySelectorAll('.nav-dot')];

// ── Snap-scroll state ────────────────────────────────────────
let currentIndex  = 0;
let isTransitioning = false;
const DURATION    = 500; // ms — matches CSS transition

// ── Animation replay flags (reset on each section visit) ────
let terminalRunning = false;
let chartRendered   = false;
let resultsRunning  = false;
let clusterRunning  = false;

// ═══════════════════════════════════════════════════════════
// SNAP SCROLL CONTROLLER
// ═══════════════════════════════════════════════════════════

function goTo(index) {
  if (isTransitioning)               return;
  if (index < 0)                     return;
  if (index >= sections.length)      return;

  isTransitioning = true;
  currentIndex    = index;

  // Translate the entire wrapper
  wrapper.style.transform = `translateY(-${index * 100}vh)`;

  // Progress bar
  const pct = sections.length > 1
    ? (index / (sections.length - 1)) * 100
    : 0;
  progressBar.style.setProperty('--progress', pct + '%');

  // Nav dots — active class only (label shown only on hover via CSS)
  dots.forEach((d, i) => d.classList.toggle('active', i === index));

  // Replay animations just before the transition finishes
  setTimeout(() => replaySection(index), DURATION * 0.6);

  // Unlock after full transition
  setTimeout(() => { isTransitioning = false; }, DURATION + 100);
}

// ── Wheel ────────────────────────────────────────────────────
// Allow horizontal scroll to pass through (for the algo track)
window.addEventListener('wheel', (e) => {
  if (Math.abs(e.deltaX) > Math.abs(e.deltaY)) return; // horizontal — ignore
  e.preventDefault();
  if (isTransitioning) return;
  if (e.deltaY > 0) goTo(currentIndex + 1);
  else              goTo(currentIndex - 1);
}, { passive: false });

// Let the algo horizontal track consume its own wheel events
const algoOuter = document.querySelector('.algo-outer');
if (algoOuter) {
  algoOuter.addEventListener('wheel', (e) => {
    if (Math.abs(e.deltaY) > Math.abs(e.deltaX)) {
      algoOuter.scrollLeft += e.deltaY * 1.2;
      e.stopPropagation();
    }
  }, { passive: true });
}

// ── Touch ────────────────────────────────────────────────────
let touchStartY = 0;
window.addEventListener('touchstart', e => {
  touchStartY = e.touches[0].clientY;
}, { passive: true });

window.addEventListener('touchend', e => {
  if (isTransitioning) return;
  const dy = touchStartY - e.changedTouches[0].clientY;
  if (Math.abs(dy) > 40) {
    if (dy > 0) goTo(currentIndex + 1);
    else        goTo(currentIndex - 1);
  }
}, { passive: true });

// ── Keyboard ─────────────────────────────────────────────────
window.addEventListener('keydown', e => {
  if (isTransitioning) return;
  if (e.key === 'ArrowDown' || e.key === 'PageDown') goTo(currentIndex + 1);
  if (e.key === 'ArrowUp'   || e.key === 'PageUp'  ) goTo(currentIndex - 1);
});

// ── Nav dots click ───────────────────────────────────────────
dots.forEach((d, i) => {
  d.addEventListener('click', () => goTo(i));
});

// ═══════════════════════════════════════════════════════════
// SECTION ANIMATION REPLAY DISPATCHER
// ═══════════════════════════════════════════════════════════

function replaySection(index) {
  const section = sections[index];
  resetFadeUps(section);

  switch (section.id) {
    case 'problem': replayTerminal();  break;
    case 'data':    replayChart();     break;
    case 'results': replayResults();   break;
    case 'infra':   replayCluster();   break;
  }
}

// Reset fade-up elements: strip visible, then stagger back in
function resetFadeUps(section) {
  const els = [...section.querySelectorAll('.fade-up')];
  els.forEach(el => el.classList.remove('visible'));
  requestAnimationFrame(() => {
    els.forEach((el, i) => {
      setTimeout(() => el.classList.add('visible'), 60 + i * 80);
    });
  });
}

// Trigger initial render on load (section 0)
window.addEventListener('DOMContentLoaded', () => {
  replaySection(0);
});

// ═══════════════════════════════════════════════════════════
// SECTION 0 — HERO: Particle Canvas
// ═══════════════════════════════════════════════════════════
(function initParticles() {
  const canvas = document.getElementById('particle-canvas');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  let W, H, particles;

  const COLORS        = ['#00f5ff', '#ff00ff', '#39ff14'];
  const N             = 80;
  const CONNECT_DIST  = 120;

  function resize() {
    W = canvas.width  = canvas.offsetWidth;
    H = canvas.height = canvas.offsetHeight;
  }

  function makeParticle() {
    const convoy = Math.random() < 0.15 ? Math.floor(Math.random() * 8) : null;
    const speed  = 0.2 + Math.random() * 0.5;
    const angle  = convoy !== null
      ? (convoy / 8) * Math.PI * 2
      : Math.random() * Math.PI * 2;
    return {
      x: Math.random() * W, y: Math.random() * H,
      vx: Math.cos(angle) * speed,
      vy: Math.sin(angle) * speed,
      r: 1.5 + Math.random() * 2,
      color: COLORS[Math.floor(Math.random() * COLORS.length)],
      convoy,
      alpha: 0.4 + Math.random() * 0.6,
    };
  }

  function draw() {
    ctx.clearRect(0, 0, W, H);
    for (let i = 0; i < particles.length; i++) {
      for (let j = i + 1; j < particles.length; j++) {
        const a = particles[i], b = particles[j];
        const dx = a.x - b.x, dy = a.y - b.y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist > CONNECT_DIST) continue;
        const isConvoy = a.convoy !== null && a.convoy === b.convoy;
        const al = (1 - dist / CONNECT_DIST) * (isConvoy ? 0.6 : 0.08);
        ctx.beginPath();
        ctx.moveTo(a.x, a.y);
        ctx.lineTo(b.x, b.y);
        ctx.strokeStyle = isConvoy
          ? `rgba(255,0,255,${al})`
          : `rgba(0,245,255,${al})`;
        ctx.lineWidth = isConvoy ? 1.5 : 0.5;
        ctx.stroke();
      }
    }
    particles.forEach(p => {
      ctx.beginPath();
      ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
      ctx.fillStyle = p.color + Math.round(p.alpha * 255).toString(16).padStart(2, '0');
      ctx.fill();
      p.x += p.vx; p.y += p.vy;
      if (p.x < -10) p.x = W + 10;
      if (p.x > W + 10) p.x = -10;
      if (p.y < -10) p.y = H + 10;
      if (p.y > H + 10) p.y = -10;
    });
    requestAnimationFrame(draw);
  }

  window.addEventListener('resize', resize);
  resize();
  particles = Array.from({ length: N }, makeParticle);
  draw();
})();

// ═══════════════════════════════════════════════════════════
// SECTION 1 — PROBLEM: Terminal Typewriter
// ═══════════════════════════════════════════════════════════
const TERMINAL_LINES = [
  { cls: 't-prompt', text: '$ hdfs dfs -count /data/31.csv' },
  { cls: 't-out',    text: '  275,893,209 records loaded.' },
  { cls: 't-prompt', text: '$ spark-submit accompany_v1.py ...' },
  { cls: 't-warn',   text: '  [WARN] GC overhead limit exceeded' },
  { cls: 't-warn',   text: '  [ERROR] Java heap space — OOM' },
  { cls: 't-prompt', text: '$ spark-submit accompany_waterfall.py ...' },
  { cls: 't-out',    text: '  [PHASE 1] Converting CSV → Parquet...' },
  { cls: 't-out',    text: '  [PROGRESS] Chunk  1/124 complete.' },
  { cls: 't-out',    text: '  [PROGRESS] Chunk 62/124 complete.' },
  { cls: 't-out',    text: '  [PROGRESS] Chunk 124/124 complete.' },
  { cls: 't-hi',     text: '  [SUCCESS] Found convoy pairs. ✓' },
];

let termTimer = null;

function replayTerminal() {
  if (terminalRunning) return;
  terminalRunning = true;

  const body = document.querySelector('.terminal-body');
  if (!body) return;
  body.innerHTML = '';
  clearTimeout(termTimer);

  let lineIdx = 0, charIdx = 0;
  let lineEl = null;

  function type() {
    if (lineIdx >= TERMINAL_LINES.length) {
      terminalRunning = false;
      return;
    }
    const { cls, text } = TERMINAL_LINES[lineIdx];
    if (charIdx === 0) {
      lineEl = document.createElement('span');
      lineEl.className = `t-line ${cls}`;
      body.appendChild(lineEl);
    }
    if (charIdx < text.length) {
      lineEl.textContent += text[charIdx++];
      termTimer = setTimeout(type, 26);
    } else {
      body.appendChild(document.createElement('br'));
      body.scrollTop = body.scrollHeight;
      lineIdx++; charIdx = 0;
      termTimer = setTimeout(type, 380);
    }
  }

  setTimeout(type, 300);
}

// ═══════════════════════════════════════════════════════════
// SECTION 2 — DATA: D3 Bar Chart
// ═══════════════════════════════════════════════════════════
const CHART_DATA = [
  { window: '50s',  global: 31182, hotspot: 21010 },
  { window: '100s', global: 40429, hotspot: 21189 },
  { window: '200s', global: 54752, hotspot: 21475 },
  { window: '300s', global: 76038, hotspot: 23595 },
];

function replayChart() {
  const svgEl = document.getElementById('chart-svg');
  if (!svgEl) return;

  // Clear previous render
  d3.select('#chart-svg').selectAll('*').remove();
  chartRendered = true;

  const svg    = d3.select('#chart-svg');
  const margin = { top: 20, right: 20, bottom: 50, left: 70 };
  const W      = svgEl.clientWidth || 800;
  const H      = 280;

  svg.attr('viewBox', `0 0 ${W} ${H}`);

  const innerW = W - margin.left - margin.right;
  const innerH = H - margin.top  - margin.bottom;

  const g = svg.append('g')
    .attr('transform', `translate(${margin.left},${margin.top})`);

  const x0 = d3.scaleBand()
    .domain(CHART_DATA.map(d => d.window))
    .range([0, innerW]).paddingInner(0.3).paddingOuter(0.1);
  const x1 = d3.scaleBand()
    .domain(['global', 'hotspot'])
    .range([0, x0.bandwidth()]).padding(0.08);
  const y = d3.scaleLinear()
    .domain([0, d3.max(CHART_DATA, d => d.global) * 1.1])
    .range([innerH, 0]);

  // Grid
  g.append('g').call(d3.axisLeft(y).tickSize(-innerW).tickFormat('').ticks(5))
    .selectAll('line').attr('stroke', '#1a1a3a').attr('stroke-dasharray', '3 3');
  g.select('.domain').remove();

  // X axis
  const xAxis = g.append('g').attr('transform', `translate(0,${innerH})`)
    .call(d3.axisBottom(x0));
  xAxis.selectAll('text')
    .attr('fill', '#556080').attr('font-family', 'JetBrains Mono')
    .attr('font-size', '12px').attr('letter-spacing', '2px');
  xAxis.select('.domain').attr('stroke', '#1a1a3a');
  xAxis.selectAll('.tick line').attr('stroke', '#1a1a3a');

  // Y axis
  g.append('g')
    .call(d3.axisLeft(y).ticks(5).tickFormat(d => (d / 1000).toFixed(0) + 'k'))
    .selectAll('text')
    .attr('fill', '#556080').attr('font-family', 'JetBrains Mono').attr('font-size', '11px');

  const colorMap = { global: '#00f5ff', hotspot: '#ff00ff' };

  const barGroups = g.selectAll('.bar-group').data(CHART_DATA).enter()
    .append('g').attr('transform', d => `translate(${x0(d.window)},0)`);

  ['global', 'hotspot'].forEach(key => {
    barGroups.append('rect')
      .attr('x', x1(key)).attr('width', x1.bandwidth())
      .attr('y', innerH).attr('height', 0)
      .attr('fill', colorMap[key]).attr('opacity', 0.85).attr('rx', 2)
      .transition().duration(900).delay((_, i) => i * 110)
        .attr('y', d => y(d[key]))
        .attr('height', d => innerH - y(d[key]));

    barGroups.append('text')
      .attr('x', x1(key) + x1.bandwidth() / 2)
      .attr('y', innerH - 4)
      .attr('text-anchor', 'middle')
      .attr('font-family', 'JetBrains Mono').attr('font-size', '10px')
      .attr('fill', colorMap[key])
      .text(d => (d[key] / 1000).toFixed(1) + 'k')
      .transition().duration(900).delay((_, i) => i * 110)
        .attr('y', d => y(d[key]) - 6);
  });

  svg.append('text')
    .attr('x', W / 2).attr('y', H - 4)
    .attr('text-anchor', 'middle')
    .attr('fill', '#556080').attr('font-family', 'JetBrains Mono')
    .attr('font-size', '11px').attr('letter-spacing', '2px')
    .text('TIME WINDOW SIZE');
}

// ═══════════════════════════════════════════════════════════
// SECTION 4 — RESULTS: Counters + Pair Bars
// ═══════════════════════════════════════════════════════════
const CONVOY_PAIRS = [
  1119848, 653795, 494458, 461288, 410185, 364396, 299546, 257498,
];
const MAX_PAIR_COUNT = CONVOY_PAIRS[0];

function animateCounter(el, target, duration = 2000) {
  const start = performance.now();
  function step(now) {
    const t    = Math.min((now - start) / duration, 1);
    const ease = t < 0.5 ? 2 * t * t : -1 + (4 - 2 * t) * t;
    el.textContent = Math.round(ease * target).toLocaleString();
    if (t < 1) requestAnimationFrame(step);
  }
  requestAnimationFrame(step);
}

function replayResults() {
  if (resultsRunning) return;
  resultsRunning = true;

  // Reset counters
  const totalEl = document.getElementById('total-pairs-counter');
  const topEl   = document.getElementById('top-count-counter');
  if (totalEl) totalEl.textContent = '0';
  if (topEl)   topEl.textContent   = '0';

  // Reset bars
  document.querySelectorAll('.pair-bar-fill').forEach(b => {
    b.style.transition = 'none';
    b.style.width = '0%';
  });

  setTimeout(() => {
    if (totalEl) animateCounter(totalEl, 2847, 2200);
    if (topEl)   animateCounter(topEl,   MAX_PAIR_COUNT, 2200);

    document.querySelectorAll('.pair-bar-fill').forEach((bar, i) => {
      const pct = (CONVOY_PAIRS[i] || 0) / MAX_PAIR_COUNT * 100;
      bar.style.transition = 'width 1.4s cubic-bezier(0.4,0,0.2,1)';
      setTimeout(() => { bar.style.width = pct + '%'; }, i * 100 + 150);
    });

    resultsRunning = false;
  }, 200);
}

// ═══════════════════════════════════════════════════════════
// SECTION 5 — INFRA: Cluster node animation
// ═══════════════════════════════════════════════════════════
let clusterPulse = null;

function replayCluster() {
  if (clusterRunning) return;
  clusterRunning = true;

  const nodes = [...document.querySelectorAll('.cluster-node-group')];
  const lines = [...document.querySelectorAll('.conn-line')];

  // Reset
  clearInterval(clusterPulse);
  nodes.forEach(n => {
    n.style.transition = 'none';
    n.style.opacity    = '0';
    n.style.transform  = 'scale(0.5)';
  });
  lines.forEach(l => l.classList.remove('active'));

  // Animate in
  setTimeout(() => {
    nodes.forEach((n, i) => {
      n.style.transition = `opacity 0.4s ease ${i * 180}ms, transform 0.4s ease ${i * 180}ms`;
      setTimeout(() => {
        n.style.opacity   = '1';
        n.style.transform = 'scale(1)';
      }, i * 180);
    });

    const totalDelay = nodes.length * 180 + 300;
    setTimeout(() => {
      lines.forEach(l => l.classList.add('active'));

      clusterPulse = setInterval(() => {
        const master = document.querySelector('.master-node');
        if (master) {
          master.style.filter = 'brightness(2.2)';
          setTimeout(() => { master.style.filter = 'brightness(1)'; }, 280);
        }
      }, 2500);

      clusterRunning = false;
    }, totalDelay);
  }, 100);
}

// ═══════════════════════════════════════════════════════════
// SECTION 6 — SCALE: 3-D flip cards (click / tap)
// ═══════════════════════════════════════════════════════════
document.querySelectorAll('.scale-card-wrap').forEach(card => {
  card.addEventListener('click', () => {
    const inner = card.querySelector('.scale-card-inner');
    const flipped = inner.style.transform === 'rotateY(180deg)';
    inner.style.transform = flipped ? '' : 'rotateY(180deg)';
  });
});

// ── Outro glitch interval ────────────────────────────────────
const outroGlitch = document.querySelector('.outro-glitch');
if (outroGlitch) {
  setInterval(() => {
    outroGlitch.style.transform = `skewX(${(Math.random() - 0.5) * 3}deg)`;
    setTimeout(() => { outroGlitch.style.transform = ''; }, 100);
  }, 3000);
}
