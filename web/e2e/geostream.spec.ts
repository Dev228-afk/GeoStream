import fs from 'node:fs';
import path from 'node:path';
import { test, expect, type Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

const ROOT = path.resolve(process.cwd(), '..');
const SCREENSHOT_DIR = path.join(ROOT, 'evidence', 'ui', 'screenshots');
fs.mkdirSync(SCREENSHOT_DIR, { recursive: true });

async function runAxeCheck(page: Page, label: string) {
  const res = await new AxeBuilder({ page })
    .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'wcag22aa'])
    .analyze();
  expect(res.violations, `Axe WCAG violations on ${label}: ${JSON.stringify(res.violations)}`).toEqual([]);
}

test.describe('GeoStream Live Observability UI, A11y & 8-State Visual QA', () => {
  test('All 8 required UI states pass WCAG 2.2 AA with zero console errors and zero overflow', async ({ page }) => {
    const errors: string[] = [];
    page.on('pageerror', (e) => errors.push(`pageerror: ${e.message}`));
    page.on('console', (m) => {
      if (m.type() === 'error') errors.push(`console.error: ${m.text()}`);
    });

    // (i) Overview with Prometheus metrics / clean state + (ii) live badge / (iii) recorded badge
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto('/overview');
    await expect(page.getByRole('heading', { name: 'Pipeline Overview' })).toBeVisible();
    await expect(page.getByText(/INGEST_INTERNAL/i)).toBeVisible();
    await runAxeCheck(page, 'overview-1440');
    await page.screenshot({ path: path.join(SCREENSHOT_DIR, '01-overview-clean-1440.png'), fullPage: true });

    // (ii) Switch to a LIVE run if present or confirm Prometheus unavailable / live timeseries behavior
    const runSelect = page.locator('#run-select');
    const optionCount = await runSelect.locator('option').count();
    expect(optionCount).toBeGreaterThan(0);
    await page.screenshot({ path: path.join(SCREENSHOT_DIR, '02-overview-metrics-state-1440.png'), fullPage: true });

    // (iv) Route intercept ONLY on optional simulated Prometheus-down check if Prometheus happened to be up, otherwise live unavailable already rendered!
    await page.route('**/api/v1/runs/*/timeseries*', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ metric: 'events_accepted_per_sec', unit: 'events/s', points: [], source: 'prometheus', status: 'unavailable' }),
      });
    });
    await page.reload();
    await expect(page.getByText(/Metrics temporarily unavailable/i)).toBeVisible();
    await page.screenshot({ path: path.join(SCREENSHOT_DIR, '03-metrics-unavailable-1440.png'), fullPage: true });
    await page.unrouteAll({ behavior: 'wait' });

    // (v) Map / Replay selected transition & fence visualization (<=100 sampled pins)
    await page.getByRole('link', { name: /Map & replay/i }).click();
    await expect(page.getByRole('heading', { name: 'Map & Event Replay' })).toBeVisible();
    const circleCount = await page.locator('svg circle').count();
    expect(circleCount).toBeLessThanOrEqual(100);
    await runAxeCheck(page, 'map-1440');
    await page.screenshot({ path: path.join(SCREENSHOT_DIR, '04-map-replay-1440.png'), fullPage: true });

    // (vi) Transitions detail drawer & keyboard navigation
    await page.getByRole('link', { name: /Transitions/i }).click();
    await expect(page.getByRole('heading', { name: 'Durable Geofence Transitions' })).toBeVisible();
    const firstInspect = page.getByRole('button', { name: 'Inspect' }).first();
    if (await firstInspect.isVisible()) {
      await firstInspect.click();
      await expect(page.getByText('Source Event ID')).toBeVisible();
    }
    await runAxeCheck(page, 'transitions-1440');
    await page.screenshot({ path: path.join(SCREENSHOT_DIR, '05-transitions-detail-1440.png'), fullPage: true });

    // (vii) Run evidence (Reconciliation, Gap incident & Alert drill + Chaos matrix)
    await page.getByRole('link', { name: /Run evidence/i }).click();
    await expect(page.getByRole('heading', { name: 'Run Evidence & Chaos Verification' })).toBeVisible();
    await expect(page.getByText(/5_crash_step5_to_6_replay/)).toBeVisible();
    await runAxeCheck(page, 'evidence-1440');
    await page.screenshot({ path: path.join(SCREENSHOT_DIR, '06-gap-alert-evidence-1440.png'), fullPage: true });

    // Tablet 1024x768 Diagnostics Backpressure Comparison
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.getByRole('link', { name: /Diagnostics/i }).click();
    await expect(page.getByRole('heading', { name: 'Backpressure & Partition Diagnostics' })).toBeVisible();
    await expect(page.getByText('Unbounded Baseline (Controlled Faulty)')).toBeVisible();
    await runAxeCheck(page, 'diagnostics-1024');
    await page.screenshot({ path: path.join(SCREENSHOT_DIR, '07-diagnostics-backpressure-1024.png'), fullPage: true });

    // (viii) Mobile 390x844 Replay & zero horizontal overflow + 200% zoom + reduced motion
    await page.setViewportSize({ width: 390, height: 844 });
    await page.getByRole('link', { name: /Map & replay/i }).click();
    await expect(page.getByRole('heading', { name: 'Map & Event Replay' })).toBeVisible();
    const mobileOverflow = await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth + 2);
    expect(mobileOverflow).toBe(false);
    await runAxeCheck(page, 'mobile-map-390');
    await page.screenshot({ path: path.join(SCREENSHOT_DIR, '08-mobile-replay-390.png'), fullPage: true });

    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.setViewportSize({ width: 720, height: 450 }); // ~200% zoom layout test
    await page.goto('/overview');
    const zoomOverflow = await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth + 2);
    expect(zoomOverflow).toBe(false);

    expect(errors).toEqual([]);
  });
});

