param([int]$ScaleFactor=1,[int]$Repeats=5)
$ErrorActionPreference='Stop'
Set-Location -LiteralPath $PSScriptRoot
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw 'Docker Desktop is required. Install and start it with Linux containers, then rerun this script.'
}
docker info *> $null
if ($LASTEXITCODE -ne 0) { throw 'Docker engine is not running.' }
New-Item -ItemType Directory -Force results/trino | Out-Null
Remove-Item -LiteralPath results/bi-bronze.json -ErrorAction SilentlyContinue
docker compose config --quiet
if ($LASTEXITCODE -ne 0) { throw 'Compose validation failed.' }
docker compose up -d trino
if ($LASTEXITCODE -ne 0) { throw 'Service startup failed.' }
docker compose --profile tools run --rm runner python scripts/run_trino.py --sf $ScaleFactor --repeats $Repeats
if ($LASTEXITCODE -ne 0) { throw 'Pipeline failed. Review service logs; no successful report is claimed.' }
docker compose --profile bi up -d --build superset
if ($LASTEXITCODE -ne 0) { throw 'BI startup failed.' }
$taskDeadline=(Get-Date).AddMinutes(10)
while (-not (Test-Path -LiteralPath results/bi-bronze.json)) {
    if ((Get-Date) -gt $taskDeadline) { throw 'BI verification timed out; inspect docker compose logs superset.' }
    Start-Sleep -Seconds 3
}
docker compose ps --all | Out-File -Encoding utf8 results/trino/docker-ps.txt
docker compose images | Out-File -Encoding utf8 results/trino/docker-images.txt
docker compose logs --no-color | Out-File -Encoding utf8 results/trino/docker.log
docker compose --profile tools run --rm runner python scripts/make_report.py --engine trino
if ($LASTEXITCODE -ne 0) { throw 'Report generation failed.' }
Write-Host 'Completed. Report: отчет.md. Superset: http://localhost:8088 (student / Practice1-local!)'
