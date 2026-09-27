# run_tests.ps1 — Run full test suite with coverage
# Usage: .\scripts\run_tests.ps1

Write-Host "`n[test] Running full test suite..." -ForegroundColor Cyan

uv run pytest tests/ `
    -v `
    --tb=short `
    --cov=jobs `
    --cov-report=term-missing `
    --cov-report=html:htmlcov `
    --cov-fail-under=80

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n[test] All tests passed. Coverage report: htmlcov\index.html" -ForegroundColor Green
} else {
    Write-Host "`n[test] Tests failed — check output above." -ForegroundColor Red
    exit 1
}
