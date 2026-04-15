# =============================================================================
# tests/run_task2_tests.ps1
# =============================================================================
# Task 2 (Orchestration) — Automated test runner
#
# Verifies:
#   1. All 5 DAG files parse without errors
#   2. DAG structure, schedules, dependencies are correct
#   3. Maintenance DAG: OPTIMIZE before VACUUM ordering
#   4. No hardcoded paths, proper docker exec usage
#   5. Airflow services are running (optional)
#
# Usage:
#   cd C:\HCMUTE\nam3ki2_1\bigdata\Crypto-DataLakehouse-Project
#   .\tests\run_task2_tests.ps1
# =============================================================================

$ErrorActionPreference = "Continue"
$ProjectRoot = (Get-Location).Path
$TestDir     = Join-Path $ProjectRoot "tests"
$DagsDir     = Join-Path $ProjectRoot "dags"
$VenvPath    = Join-Path $ProjectRoot ".venv-task2"
$VenvPython  = Join-Path $VenvPath "Scripts\python.exe"
$VenvPip     = Join-Path $VenvPath "Scripts\pip.exe"
$VenvPytest  = Join-Path $VenvPath "Scripts\pytest.exe"

function Print-Step  { param($msg) Write-Host "`n=== $msg ===" -ForegroundColor Cyan }
function Print-OK    { param($msg) Write-Host "  [OK]  $msg" -ForegroundColor Green }
function Print-Fail  { param($msg) Write-Host "  [ERR] $msg" -ForegroundColor Red }
function Print-Info  { param($msg) Write-Host "  [..] $msg"  -ForegroundColor Yellow }
function Print-Warn  { param($msg) Write-Host "  [!!] $msg"  -ForegroundColor Magenta }

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Task 2: Orchestration — Automated Test Suite              " -ForegroundColor Cyan
Write-Host "  Teammate 2 — Airflow DAGs + OPTIMIZE/VACUUM              " -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

# ===========================================================================
# STEP 1: Check prerequisites
# ===========================================================================
Print-Step "STEP 1: Check prerequisites"

if (-not (Test-Path (Join-Path $DagsDir "01_ingestion_dag.py"))) {
    Print-Fail "DAG files not found in $DagsDir"
    exit 1
}
$dagCount = (Get-ChildItem $DagsDir -Filter "*.py" | Measure-Object).Count
Print-OK "Found $dagCount DAG files in dags/"

if (-not (Test-Path (Join-Path $TestDir "test_dags.py"))) {
    Print-Fail "test_dags.py not found in $TestDir"
    exit 1
}
Print-OK "test_dags.py found"

$pyVer = python --version 2>&1
if ($LASTEXITCODE -ne 0) {
    Print-Fail "Python not found in PATH"
    exit 1
}
Print-OK "Python: $pyVer"

# ===========================================================================
# STEP 2: Create virtual environment + install dependencies
# ===========================================================================
Print-Step "STEP 2: Install test dependencies"

if (-not (Test-Path $VenvPytest)) {
    Print-Info "Creating .venv-task2 virtual environment..."
    python -m venv $VenvPath

    Print-Info "Installing apache-airflow and pytest (may take 2-3 min)..."
    $env:AIRFLOW_HOME = Join-Path $VenvPath "airflow_home"
    & $VenvPip install --quiet `
        "apache-airflow==2.8.1" `
        "pytest>=7.0" `
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt"

    if ($LASTEXITCODE -ne 0) {
        Print-Fail "Failed to install dependencies"
        Print-Info "Trying minimal install (pytest only)..."
        & $VenvPip install --quiet pytest
    }
    Print-OK "Dependencies installed"
} else {
    Print-OK "Dependencies already installed in .venv-task2"
}

# ===========================================================================
# STEP 3: Run DAG unit tests
# ===========================================================================
Print-Step "STEP 3: Run DAG unit tests (pytest)"

$AirflowHome = Join-Path $VenvPath "airflow_home"
if (-not (Test-Path $AirflowHome)) { New-Item -ItemType Directory -Path $AirflowHome -Force | Out-Null }
$env:AIRFLOW_HOME = $AirflowHome
$env:AIRFLOW__CORE__UNIT_TEST_MODE = "True"
# Suppress Airflow config warnings
$env:AIRFLOW__CORE__LOAD_EXAMPLES = "false"
# Airflow requires absolute path for SQLite — use a file inside venv
$dbPath = (Join-Path $AirflowHome "airflow_test.db").Replace('\', '/')
$env:AIRFLOW__DATABASE__SQL_ALCHEMY_CONN = "sqlite:////$dbPath"

Print-Info "Running: pytest tests/test_dags.py -v"
& $VenvPytest "$TestDir\test_dags.py" -v --tb=short
$testResult = $LASTEXITCODE

Write-Host ""
if ($testResult -eq 0) {
    Print-OK "ALL TESTS PASSED!"
} else {
    Print-Fail "Some tests FAILED (exit code: $testResult)"
}

# ===========================================================================
# STEP 4: Quick DAG syntax check (parse all DAGs)
# ===========================================================================
Print-Step "STEP 4: Quick DAG syntax check"

$parseErrors = 0
Get-ChildItem $DagsDir -Filter "*.py" | ForEach-Object {
    $result = & $VenvPython -c "
import os, sys
os.environ['AIRFLOW_HOME'] = r'$AirflowHome'
db = r'$AirflowHome'.replace(chr(92), '/') + '/airflow_test.db'
os.environ['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'] = f'sqlite:////{db}'
os.environ['AIRFLOW__CORE__LOAD_EXAMPLES'] = 'false'
sys.path.insert(0, r'$DagsDir')
try:
    exec(open(r'$($_.FullName)', encoding='utf-8').read())
    print('OK: $($_.Name)')
except Exception as e:
    print(f'FAIL: $($_.Name): {e}')
    sys.exit(1)
" 2>&1
    if ($LASTEXITCODE -ne 0) {
        Print-Fail $result
        $parseErrors++
    } else {
        Print-OK $result
    }
}

if ($parseErrors -eq 0) {
    Print-OK "All DAG files parse without errors"
} else {
    Print-Fail "$parseErrors DAG(s) failed to parse"
}

# ===========================================================================
# STEP 5: Check Airflow services (optional)
# ===========================================================================
Print-Step "STEP 5: Check Airflow services (optional)"

$airflowUp = $false
try {
    $null = docker ps --filter "name=airflow-webserver" --format "{{.Names}}" 2>&1
    if ($LASTEXITCODE -eq 0) {
        $container = docker ps --filter "name=airflow-webserver" --format "{{.Names}}" 2>&1
        if ($container -match "airflow") {
            Print-OK "Airflow webserver is running"
            $airflowUp = $true

            # Check DAGs are loaded
            Print-Info "Checking DAGs loaded in Airflow..."
            $dagList = docker exec airflow-webserver airflow dags list 2>&1
            if ($dagList -match "05_delta_lake_maintenance") {
                Print-OK "Maintenance DAG visible in Airflow scheduler"
            } else {
                Print-Warn "DAGs may not be loaded yet (scheduler needs time)"
            }
        } else {
            Print-Info "Airflow not running (skip integration check)"
        }
    }
} catch {
    Print-Info "Docker not available / Airflow not running (skip)"
}

# ===========================================================================
# STEP 6: Results Summary
# ===========================================================================
Print-Step "STEP 6: Results Summary"

Write-Host ""
Write-Host "  +--------------------------------------------------+" -ForegroundColor White
Write-Host "  |          TASK 2 TEST RESULTS                      |" -ForegroundColor White
Write-Host "  +--------------------------------------------------+" -ForegroundColor White

if ($testResult -eq 0 -and $parseErrors -eq 0) {
    Write-Host "  | pytest (DAG tests) : [PASS]                     |" -ForegroundColor Green
    Write-Host "  | DAG parse check    : [PASS]                     |" -ForegroundColor Green
    if ($airflowUp) {
        Write-Host "  | Airflow services   : [RUNNING]                  |" -ForegroundColor Green
    } else {
        Write-Host "  | Airflow services   : [NOT CHECKED]              |" -ForegroundColor Yellow
    }
    Write-Host "  +--------------------------------------------------+" -ForegroundColor White
    Write-Host ""
    Write-Host "  SUCCESS! Task 2 (Orchestration) is complete." -ForegroundColor Green
} else {
    Write-Host "  | pytest (DAG tests) : $(if ($testResult -eq 0) {'[PASS]'} else {'[FAIL]'})                     |" -ForegroundColor $(if ($testResult -eq 0) {'Green'} else {'Red'})
    Write-Host "  | DAG parse check    : $(if ($parseErrors -eq 0) {'[PASS]'} else {'[FAIL]'})                     |" -ForegroundColor $(if ($parseErrors -eq 0) {'Green'} else {'Red'})
    Write-Host "  +--------------------------------------------------+" -ForegroundColor White
    Write-Host ""
    Write-Host "  SOME TESTS FAILED. Review output above." -ForegroundColor Red
}

Write-Host ""
Write-Host "  DAG files: $DagsDir" -ForegroundColor White
Write-Host "  Test file: $TestDir\test_dags.py" -ForegroundColor White
Write-Host "  Airflow UI: http://localhost:8888 (admin/admin)" -ForegroundColor White
Write-Host ""

$timestamp = Get-Date -Format "HH:mm:ss"
Write-Host "Script done. $timestamp" -ForegroundColor DarkGray
