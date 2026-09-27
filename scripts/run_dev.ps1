# ── Auto-start Docker Desktop if not running ─────────────────────────────────
Write-Host "[dev] Checking Docker..." -ForegroundColor Yellow
$dockerRunning = (docker info 2>$null) -ne $null

if (-not $dockerRunning) {
    Write-Host "[dev] Docker not running — starting Docker Desktop..." -ForegroundColor Yellow
    $dockerExe = "C:\Program Files\Docker\Docker\Docker Desktop.exe"

    if (Test-Path $dockerExe) {
        Start-Process $dockerExe
        Write-Host "[dev] Waiting for Docker engine (this takes ~30 seconds)..." -ForegroundColor Yellow
        $attempts = 0
        do {
            Start-Sleep -Seconds 5
            $attempts++
            Write-Host "  ... ($($attempts * 5)s)" -ForegroundColor DarkGray
            $dockerRunning = (docker info 2>$null) -ne $null
        } until ($dockerRunning -or $attempts -ge 24)  # max 2 min

        if (-not $dockerRunning) {
            Write-Host "[ERROR] Docker did not start after 2 minutes. Open Docker Desktop manually." -ForegroundColor Red
            exit 1
        }
        Write-Host "[dev] Docker is ready." -ForegroundColor Green
    } else {
        Write-Host "[ERROR] Docker Desktop not found at $dockerExe" -ForegroundColor Red
        Write-Host "        Download from: https://www.docker.com/products/docker-desktop/" -ForegroundColor White
        exit 1
    }
} else {
    Write-Host "[dev] Docker already running." -ForegroundColor Green
}

# ── Confirm docker-compose.yml exists in current directory ───────────────────
if (-not (Test-Path "docker-compose.yml")) {
    Write-Host "[ERROR] docker-compose.yml not found in $PWD" -ForegroundColor Red
    Write-Host "        Run this script from the project root folder." -ForegroundColor White
    exit 1
}