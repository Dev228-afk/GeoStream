SHELL := powershell.exe
.SHELLFLAGS := -NoProfile -Command
PNPM_PATH := $(APPDATA)\npm-global;$(PATH)

.PHONY: setup seed-routes test test-race smoke integration chaos bench-local bench-stress bench-backpressure reconcile dashboards k8s-dev ui-test ui-e2e cloud-plan cloud-destroy

setup:
	cmd /c 'wsl -d Ubuntu-24.04 -- bash -lc "cd /mnt/c/Users/devan/OneDrive/Desktop/apple_ads/geostream && docker compose -p geostream --env-file .env -f deploy/docker/docker-compose.yml up -d --wait"'
	$$env:Path = "$(PNPM_PATH)"; pnpm --dir web install
	node scripts/gen-contracts.mjs --check

seed-routes:
	go run ./cmd/seedroutes --seed 2026 --out fixtures

test:
	node scripts/gen-contracts.mjs --check
	go vet ./...
	go test -race ./...
	$$env:Path = "$(PNPM_PATH)"; Set-Location web; pnpm run typecheck && pnpm run lint && pnpm run test && pnpm run build

test-race:
	go test -race -count=1 ./...

smoke:
	go run ./cmd/simulator --profile smoke --run-id run-smoke-manual --duration 8

integration:
	go test -race -v ./tests/integration/...

chaos:
	go test -race -v ./tests/chaos/...

reconcile:
	@if (-not "$(RUN_ID)") { Write-Error "Pass RUN_ID=<id>"; exit 2 }
	go run ./cmd/reconcile --run-id "$(RUN_ID)"

bench-local:
	go run ./cmd/simulator --profile dev --run-id run-local-001 --devices 1500 --rate 1500 --duration 12 --out evidence/throughput/run-local-001

bench-stress:
	@Write-Output "TARGET 40K/80K/3-minute stress profile requires explicit approval & high-capacity cluster; run with --allow-stress on approved hardware."
	go run ./cmd/simulator --profile resume-stress --duration 180

bench-backpressure:
	go run ./bench/compare --events 2400 --out evidence/backpressure/run-bp-001

dashboards:
	cmd /c 'wsl -d Ubuntu-24.04 -- bash -lc "cd /mnt/c/Users/devan/OneDrive/Desktop/apple_ads/geostream && docker compose -p geostream --env-file .env -f deploy/docker/docker-compose.yml --profile monitoring up -d --build --wait"'

k8s-dev:
	kubectl kustomize deploy/k8s | Out-Null

ui-test:
	$$env:Path = "$(PNPM_PATH)"; Set-Location web; pnpm run typecheck && pnpm run lint && pnpm run test

ui-e2e:
	powershell -NoProfile -File scripts\run_live_drill_and_qa.ps1

cloud-plan:
	@Write-Output "BLOCKED_EXTERNAL: Inspect CLOUD_APPROVAL.md and export AWS credentials before running terraform plan/apply."
	@exit 1

cloud-destroy:
	@Write-Output "BLOCKED_EXTERNAL: No AWS resources deployed; run terraform destroy -auto-approve in deploy/terraform/aws if provisioned."

