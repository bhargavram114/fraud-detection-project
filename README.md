# ATM Fraud Detection Pipeline

Real-time fraud detection using PySpark Structured Streaming + Kafka.
12 detection rules, full TDD (51 tests), production-ready.

## Tech Stack
| Layer       | Technology                       |
|-------------|----------------------------------|
| Language    | Python 3.11                      |
| Packaging   | uv (pyproject.toml)              |
| Processing  | PySpark 3.5 Structured Streaming |
| Messaging   | Apache Kafka 3.5                 |
| Containers  | Docker Desktop (Windows 11)      |

## Prerequisites (Windows 11)
1. **Docker Desktop** — https://www.docker.com/products/docker-desktop/
2. **uv** — open PowerShell (Admin) and run:
   ```powershell
   powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
   ```
3. **Java 11+** (required by Spark) — https://adoptium.net/
   Set `JAVA_HOME` in System Environment Variables after install.

## First-Time Setup
```powershell
Set-ExecutionPolicy RemoteSigned -Scope CurrentUser   # one time only
.\scripts\setup_windows.ps1
```

## Daily Workflow
```powershell
# Start everything (auto-launches Docker Desktop if closed)
.\scripts\run_dev.ps1

# Terminal 2 — Spark job
uv run spark-submit `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 `
  jobs\fraud_detection.py

# Terminal 3 — Transaction generator
uv run python producer\transaction_generator.py --mode mixed --tps 10

# Stop everything
.\scripts\stop_dev.ps1

# Stop but keep Docker Desktop open
.\scripts\stop_dev.ps1 -KeepDocker

# Stop + wipe volumes/images (full clean)
.\scripts\stop_dev.ps1 -Prune
```

## Tests
```powershell
.\scripts\run_tests.ps1                        # full suite + coverage report
uv run pytest tests\ -v                        # quick run
uv run pytest tests\test_transforms.py -v      # transforms only
uv run pytest tests\test_transforms.py::TestGeoVelocity -v   # single rule
```

## Package Source
```powershell
.\scripts\pack_source.ps1                       # source ZIP (default)
.\scripts\pack_source.ps1 -Format Wheel         # Python wheel
.\scripts\pack_source.ps1 -Format Sdist         # Python source distribution
.\scripts\pack_source.ps1 -Format Python        # wheel + source distribution
.\scripts\pack_source.ps1 -Format Zip -OutputDirectory .\artifacts
```

Packages are written to `dist\` by default. ZIP archives omit generated files, virtual environments, checkpoints, and runtime data. `.nupkg` is a NuGet/.NET package format; use the ZIP or Python distribution formats for this project.

## Project Structure
```
fraud-detection/
├── pyproject.toml                   ← uv config, Python 3.11 pinned
├── docker-compose.yml               ← Kafka + Spark (no version: tag)
├── Dockerfile                       ← multi-stage, uv in builder
├── README.md
├── config/
│   └── app.yaml                     ← all thresholds + env config
├── jobs/
│   ├── schemas.py                   ← TRANSACTION_SCHEMA (14 fields incl. txn_status)
│   ├── fraud_detection.py           ← dev job (console sink)
│   ├── fraud_detection_prod.py      ← prod job (DLQ, health, JSON logging)
│   └── rules/
│       ├── transforms.py            ← 12 pure functions — batch-testable
│       └── fraud_rules.py           ← streaming wrappers (withWatermark here only)
├── producer/
│   └── transaction_generator.py    ← ATM txn simulator + fraud bursts
├── tests/
│   ├── conftest.py                  ← SparkSession + make_txn factory
│   ├── test_schema.py               ← schema contract (5 tests)
│   └── test_transforms.py          ← all 12 rules (46 tests) — 51 total
├── scripts/
│   ├── setup_windows.ps1            ← one-time setup
│   ├── run_dev.ps1                  ← start all services (auto-starts Docker)
│   ├── stop_dev.ps1                 ← stop all services + containers + Docker
│   ├── run_tests.ps1               ← pytest + coverage
│   └── pack_source.ps1             ← ZIP or Python package builder
└── deploy/
    ├── k8s-deployment.yaml          ← Kubernetes manifests + RBAC
    └── ci-cd.yml                    ← GitHub Actions pipeline
```

## 12 Fraud Detection Rules
| # | Rule                     | Window       | Risk |
|---|--------------------------|--------------|------|
| 1 | Velocity Check           | 5min / 1min  | 85   |
| 2 | High Value Single Txn    | none         | 80   |
| 3 | High Window Amount       | 5min / 1min  | 90   |
| 4 | Geographic Velocity      | 30min / 5min | 95   |
| 5 | Unusual Hour             | none         | 50   |
| 6 | Round Amount Structuring | 10min / 2min | 75   |
| 7 | Rapid Merchant Switch    | 5min / 1min  | 70   |
| 8 | Card-Not-Present Spike   | 10min / 2min | 65   |
| 9 | Multi-Card Terminal      | 10min / 2min | 80   |
|10 | Dormant Card Activation  | none         | 60   |
|11 | Sub-Threshold Structuring| 1hr / 5min   | 85   |
|12 | Declined then Approved   | 10min / 2min | 90   |

## uv Cheat Sheet
| Action              | Command                          |
|---------------------|----------------------------------|
| Install all deps    | `uv sync`                        |
| Add a package       | `uv add <package>`               |
| Remove a package    | `uv remove <package>`            |
| Run a script        | `uv run python script.py`        |
| Run pytest          | `uv run pytest`                  |
| Create venv         | `uv venv --python 3.11`          |
