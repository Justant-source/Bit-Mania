(function () {
  const KEY = 'bm-theme';
  const root = document.documentElement;

  const apply = (t) => {
    root.setAttribute('data-theme', t);
    try { localStorage.setItem(KEY, t); } catch (e) {}
    document.querySelectorAll('[data-set-theme]').forEach((btn) => {
      btn.classList.toggle('active', btn.dataset.setTheme === t);
    });
    window.dispatchEvent(new CustomEvent('bm:themechange', { detail: t }));
  };

  const saved = (() => {
    try { return localStorage.getItem(KEY); } catch (e) { return null; }
  })() || (matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');

  // Set attribute immediately to prevent flash of wrong theme
  root.setAttribute('data-theme', saved);

  // Sync toggle buttons once DOM is ready (no chart re-render on initial load)
  document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('[data-set-theme]').forEach((btn) => {
      btn.classList.toggle('active', btn.dataset.setTheme === saved);
    });
  });

  // Handle toggle button clicks
  document.addEventListener('click', (e) => {
    const btn = e.target.closest('[data-set-theme]');
    if (btn) apply(btn.dataset.setTheme);
  });
})();
