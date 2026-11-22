import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './e2e',
  timeout: 45_000,
  retries: 0,
  use: {
    baseURL: 'http://127.0.0.1:5301',
    headless: true,
    viewport: { width: 1440, height: 900 },
  },
  reporter: [
    ['list'],
    ['json', { outputFile: '../evidence/ui/playwright-report.json' }],
  ],
});

