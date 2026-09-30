# run_tests.ps1 — Run full test suite with coverage
Write-Host "`n[test] Running all 51 tests..." -ForegroundColor Cyan
uv run pytest tests/ -v --tb=short --cov=jobs --cov-report=term-missing --cov-report=html:htmlcov --cov-fail-under=80
if ($LASTEXITCODE -eq 0) {
    Write-Host "`n[test] All tests passed. Coverage report: htmlcov\index.html" -ForegroundColor Green
} else {
    Write-Host "`n[test] Tests failed." -ForegroundColor Red; exit 1
}
