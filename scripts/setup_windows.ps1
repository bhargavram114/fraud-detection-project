# setup_windows.ps1
# One-time project setup on Windows 11 with uv + Python 3.11
# Run from project root in PowerShell (as Administrator first time only):
#   Set-ExecutionPolicy RemoteSigned -Scope CurrentUser
#   .\scripts\setup_windows.ps1

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Host "`n===== ATM Fraud Detection — Windows Setup =====" -ForegroundColor Cyan

# ── Step 1: Install uv if missing ────────────────────────────────────────────
if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    Write-Host "`n[1/5] Installing uv..." -ForegroundColor Yellow
    powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
    # Reload PATH so uv is available in this session
    $env:PATH = [System.Environment]::GetEnvironmentVariable("PATH","Machine") + ";" +
                [System.Environment]::GetEnvironmentVariable("PATH","User")
} else {
    Write-Host "`n[1/5] uv already installed: $(uv --version)" -ForegroundColor Green
}

# ── Step 2: Install Python 3.11 via uv ───────────────────────────────────────
Write-Host "`n[2/5] Installing Python 3.11 via uv..." -ForegroundColor Yellow
uv python install 3.11
Write-Host "Python 3.11 ready" -ForegroundColor Green

# ── Step 3: Create virtual environment pinned to 3.11 ────────────────────────
Write-Host "`n[3/5] Creating .venv with Python 3.11..." -ForegroundColor Yellow
uv venv --python 3.11
Write-Host ".venv created at $PWD\.venv" -ForegroundColor Green

# ── Step 4: Install all dependencies from pyproject.toml ─────────────────────
Write-Host "`n[4/5] Installing dependencies (main + dev)..." -ForegroundColor Yellow
uv sync --extra dev
Write-Host "Dependencies installed" -ForegroundColor Green

# ── Step 5: Install Docker Desktop reminder ───────────────────────────────────
Write-Host "`n[5/5] Checking Docker..." -ForegroundColor Yellow
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "Docker not found. Download Docker Desktop from:" -ForegroundColor Red
    Write-Host "  https://www.docker.com/products/docker-desktop/" -ForegroundColor White
} else {
    Write-Host "Docker found: $(docker --version)" -ForegroundColor Green
}

# ── Done ──────────────────────────────────────────────────────────────────────
Write-Host "`n===== Setup complete! =====" -ForegroundColor Cyan
Write-Host @"

Next steps — open 3 PowerShell terminals:

  Terminal 1 — Start Kafka + Spark:
    docker-compose up -d

  Terminal 2 — Run fraud detection (activate venv first):
    .venv\Scripts\Activate.ps1
    uv run spark-submit ``
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 ``
      jobs\fraud_detection.py

  Terminal 3 — Start transaction generator:
    .venv\Scripts\Activate.ps1
    uv run python producer\transaction_generator.py --mode mixed --tps 10

  Run tests:
    uv run pytest tests\ -v --cov=jobs

  Kafka UI:   http://localhost:8080
  Spark UI:   http://localhost:8081

"@ -ForegroundColor White
