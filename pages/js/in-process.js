/* ═══════════════════════════════════════════════════════════
   IN PROCESS — slim snap-scroll controller
   Same UX as Start's main.js: wheel/key/dot → translate
   #page-wrapper by -N · 100vh. No per-section animations.
   ═══════════════════════════════════════════════════════════ */
(() => {
  const wrapper  = document.getElementById('page-wrapper');
  const sections = [...document.querySelectorAll('.section')];
  const dots     = [...document.querySelectorAll('.nav-dot')];
  const progress = document.getElementById('scroll-progress');

  const DURATION = 550;        // matches CSS transition
  let i = 0;
  let busy = false;

  function go(n) {
    if (busy || n < 0 || n >= sections.length || n === i) return;
    busy = true;
    i = n;
    wrapper.style.transform = `translateY(-${n * 100}vh)`;
    if (progress) {
      const pct = sections.length > 1 ? (n / (sections.length - 1)) * 100 : 0;
      progress.style.width = `${pct}%`;
    }
    dots.forEach((d, k) => d.classList.toggle('active', k === n));
    setTimeout(() => { busy = false; }, DURATION);
  }

  // --- Wheel (accumulator avoids jitter on trackpads) ---
  let acc = 0;
  let lastWheel = 0;
  window.addEventListener('wheel', (e) => {
    e.preventDefault();
    if (busy) return;
    const now = performance.now();
    if (now - lastWheel > 200) acc = 0;       // fresh gesture
    lastWheel = now;
    acc += e.deltaY;
    if (acc >  50) { go(i + 1); acc = 0; }
    if (acc < -50) { go(i - 1); acc = 0; }
  }, { passive: false });

  // --- Keyboard ---
  const prevSection = document.body.dataset.prev;
  const nextSection = document.body.dataset.next;

  window.addEventListener('keydown', (e) => {
    if (['ArrowDown', 'PageDown', ' '].includes(e.key)) { e.preventDefault(); go(i + 1); }
    if (['ArrowUp',   'PageUp'].includes(e.key))        { e.preventDefault(); go(i - 1); }
    if (e.key === 'Home') { e.preventDefault(); go(0); }
    if (e.key === 'End')  { e.preventDefault(); go(sections.length - 1); }
    // ← / → at *either* boundary slide (first OR last) → cross-section nav.
    // ← always goes to previous section, → always to next, regardless of which boundary.
    const atBoundary = (i === 0) || (i === sections.length - 1);
    if (atBoundary && e.key === 'ArrowLeft'  && prevSection) {
      e.preventDefault();
      window.location.href = prevSection;
    }
    if (atBoundary && e.key === 'ArrowRight' && nextSection) {
      e.preventDefault();
      window.location.href = nextSection;
    }
  });

  // --- Touch (vertical swipe) ---
  let touchY = 0;
  window.addEventListener('touchstart', (e) => { touchY = e.touches[0].clientY; }, { passive: true });
  window.addEventListener('touchend',   (e) => {
    const dy = touchY - e.changedTouches[0].clientY;
    if (Math.abs(dy) > 50) go(i + (dy > 0 ? 1 : -1));
  }, { passive: true });

  // --- Dots ---
  dots.forEach((d, k) => d.addEventListener('click', () => go(k)));

  // --- §4 Top-5 tab switcher ---
  const tabs  = document.querySelectorAll('.k-tab-btn');
  const panes = document.querySelectorAll('.k-pane');
  tabs.forEach((btn) => {
    btn.addEventListener('click', () => {
      const k = btn.dataset.k;
      tabs.forEach((b) => b.classList.toggle('active', b === btn));
      panes.forEach((p) => p.classList.toggle('active', p.dataset.k === k));
    });
  });

  // --- Init ---
  dots[0]?.classList.add('active');
})();
