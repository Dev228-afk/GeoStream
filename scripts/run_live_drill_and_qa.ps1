$ErrorActionPreference = "Stop"
$env:PATH = "$env:APPDATA\npm-global;$env:PATH"
Set-Location 'C:\Users\devan\OneDrive\Desktop\apple_ads\geostream'

New-Item -ItemType Directory -Force -Path 'bin' | Out-Null
go build -o bin\geostream-api.exe .\cmd\api
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$logDir = Join-Path $env:TEMP "geostream_qa_$(Get-Date -Format 'HHmmss')"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

# 1. Start Monitoring stack (worker container + Prometheus + Alertmanager + Grafana) & run live Alert Drill
cmd /c 'wsl -d Ubuntu-24.04 -- bash -lc "cd /mnt/c/Users/devan/OneDrive/Desktop/apple_ads/geostream && docker compose -p geostream --env-file .env -f deploy/docker/docker-compose.yml --profile monitoring up -d --build --wait"'
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

go run .\cmd\alertdrill
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# 2. Launch native Go API (:8200) and Vite Web (:5301) redirecting stdout/stderr to disk files (never pipe deadlock!)
$apiProc = Start-Process -FilePath 'C:\Users\devan\OneDrive\Desktop\apple_ads\geostream\bin\geostream-api.exe' `
  -WorkingDirectory 'C:\Users\devan\OneDrive\Desktop\apple_ads\geostream' `
  -RedirectStandardOutput (Join-Path $logDir 'api.stdout.log') `
  -RedirectStandardError (Join-Path $logDir 'api.stderr.log') `
  -PassThru -NoNewWindow

$viteProc = Start-Process -FilePath 'cmd.exe' `
  -ArgumentList '/c', 'pnpm run dev' `
  -WorkingDirectory 'C:\Users\devan\OneDrive\Desktop\apple_ads\geostream\web' `
  -RedirectStandardOutput (Join-Path $logDir 'vite.stdout.log') `
  -RedirectStandardError (Join-Path $logDir 'vite.stderr.log') `
  -PassThru -NoNewWindow

try {
  $deadline = (Get-Date).AddSeconds(35)
  while ((Get-Date) -lt $deadline) {
    try {
      $r1 = Invoke-WebRequest -Uri 'http://127.0.0.1:8200/api/v1/health' -UseBasicParsing -TimeoutSec 2
      $r2 = Invoke-WebRequest -Uri 'http://127.0.0.1:5301/overview' -UseBasicParsing -TimeoutSec 2
      if ($r1.StatusCode -eq 200 -and $r2.StatusCode -eq 200) { break }
    } catch {}
    Start-Sleep -Milliseconds 500
  }

  Set-Location 'C:\Users\devan\OneDrive\Desktop\apple_ads\geostream\web'
  pnpm run test:e2e
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} finally {
  if ($apiProc -and -not $apiProc.HasExited) { Stop-Process -Id $apiProc.Id -Force -ErrorAction SilentlyContinue }
  if ($viteProc -and -not $viteProc.HasExited) { taskkill /PID $viteProc.Id /T /F | Out-Null }
  # Stop monitoring services to conserve WSL2 RAM cleanly
  cmd /c 'wsl -d Ubuntu-24.04 -- bash -lc "cd /mnt/c/Users/devan/OneDrive/Desktop/apple_ads/geostream && docker compose -p geostream --env-file .env -f deploy/docker/docker-compose.yml stop worker prometheus alertmanager grafana"'
}
Write-Output "GeoStream live alert drill + Playwright/Axe QA succeeded cleanly. Logs: $logDir"

