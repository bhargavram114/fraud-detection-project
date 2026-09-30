# setup_windows.ps1 — One-time project setup on Windows 11
# Run from project root in PowerShell (Admin first time):
#   Set-ExecutionPolicy RemoteSigned -Scope CurrentUser
#   .\scripts\setup_windows.ps1

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Write-Host "`n===== ATM Fraud Detection — Windows Setup =====" -ForegroundColor Cyan

# 1. Install uv
if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    Write-Host "`n[1/4] Installing uv..." -ForegroundColor Yellow
    powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
    $env:PATH = [System.Environment]::GetEnvironmentVariable("PATH","Machine") + ";" +
                [System.Environment]::GetEnvironmentVariable("PATH","User")
} else {
    Write-Host "`n[1/4] uv $(uv --version) already installed." -ForegroundColor Green
}

# 2. Install Python 3.11
Write-Host "`n[2/4] Installing Python 3.11..." -ForegroundColor Yellow
uv python install 3.11

# 3. Create venv + install all deps
Write-Host "`n[3/4] Creating .venv and installing dependencies..." -ForegroundColor Yellow
uv venv --python 3.11
uv sync --extra dev

# 4. Check Java (Spark needs JVM)
Write-Host "`n[4/4] Checking Java..." -ForegroundColor Yellow
if (-not (Get-Command java -ErrorAction SilentlyContinue)) {
    Write-Host "  Java not found. Install from: https://adoptium.net/" -ForegroundColor Red
    Write-Host "  After installing, set JAVA_HOME in System Environment Variables." -ForegroundColor Yellow
} else {
    Write-Host "  $(java -version 2>&1 | Select-Object -First 1)" -ForegroundColor Green
}

Write-Host @"

===== Setup complete! =====

Start the pipeline:
  .\scripts\run_dev.ps1

Run tests:
  .\scripts\run_tests.ps1
"@ -ForegroundColor Cyan
