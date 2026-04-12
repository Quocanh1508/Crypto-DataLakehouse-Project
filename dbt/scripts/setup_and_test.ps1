# setup_and_test.ps1 - dbt Setup & Test Script (ASCII only, no encoding issues)
# Run from project root:
#   cd c:\HCMUTE\nam3ki2_1\bigdata\Crypto-DataLakehouse-Project
#   .\dbt\scripts\setup_and_test.ps1

# NOTE: Do NOT set ErrorActionPreference=Stop globally.
# Docker, dbt, etc. write warnings to stderr which would cause false failures.
$ErrorActionPreference = "Continue"
$ProjectRoot = (Get-Location).Path
$DbtDir      = Join-Path $ProjectRoot "dbt"
$ScriptsDir  = Join-Path $DbtDir "scripts"
$TrinoUrl    = "http://localhost:8080"
$VenvPath    = Join-Path $ProjectRoot ".venv-dbt"
$VenvDbt     = Join-Path $VenvPath "Scripts\dbt.exe"
$VenvPip     = Join-Path $VenvPath "Scripts\pip.exe"

function Print-Step  { param($msg) Write-Host "`n=== $msg ===" -ForegroundColor Cyan }
function Print-OK    { param($msg) Write-Host "  [OK]  $msg" -ForegroundColor Green }
function Print-Fail  { param($msg) Write-Host "  [ERR] $msg" -ForegroundColor Red }
function Print-Info  { param($msg) Write-Host "  [..] $msg"  -ForegroundColor Yellow }

Write-Host ""
Write-Host "============================================================" -ForegroundColor Magenta
Write-Host "  dbt Setup and Test - Crypto Data Lakehouse - Teammate 2  " -ForegroundColor Magenta
Write-Host "============================================================" -ForegroundColor Magenta

# ===========================================================================
# STEP 1: Check prerequisites
# ===========================================================================
Print-Step "STEP 1: Check prerequisites"

# Docker running? Use 'docker version' (less verbose than 'docker info')
# Redirect ALL output (stdout + stderr) to $null so warnings don't throw.
$null = docker version 2>&1
if ($LASTEXITCODE -ne 0) {
    Print-Fail "Docker Desktop is NOT running. Please start it and retry."
    exit 1
}
Print-OK "Docker Desktop is running"

# Python available?
$pyVer = python --version 2>&1
if ($LASTEXITCODE -ne 0) {
    Print-Fail "Python not found in PATH"
    exit 1
}
Print-OK "Python: $pyVer"

# Correct working directory?
if (-not (Test-Path (Join-Path $ProjectRoot "docker-compose.yml"))) {
    Print-Fail "Run from project root (where docker-compose.yml is located)"
    Print-Info "cd c:\HCMUTE\nam3ki2_1\bigdata\Crypto-DataLakehouse-Project"
    exit 1
}
Print-OK "Working directory: $ProjectRoot"

# ===========================================================================
# STEP 2: Create ~/.dbt/profiles.yml if missing
# ===========================================================================
Print-Step "STEP 2: Create profiles.yml"

$DbtHome     = Join-Path $env:USERPROFILE ".dbt"
$ProfileFile = Join-Path $DbtHome "profiles.yml"

if (-not (Test-Path $ProfileFile)) {
    Print-Info "profiles.yml not found - creating..."
    if (-not (Test-Path $DbtHome)) {
        New-Item -ItemType Directory -Path $DbtHome -Force | Out-Null
    }

    # Write profiles.yml (all ASCII to avoid encoding issues)
    # IMPORTANT: 'user' is REQUIRED by dbt-trino even with method: none
    $lines = @(
        "# ~/.dbt/profiles.yml",
        "# dbt connection to Trino - DO NOT commit to Git",
        "",
        "crypto_lakehouse:",
        "  target: dev",
        "  outputs:",
        "    dev:",
        "      type: trino",
        "      method: none",
        "      user: dbt_user",
        "      host: localhost",
        "      port: 8080",
        "      database: delta",
        "      schema: default",
        "      threads: 4",
        "      http_scheme: http"
    )
    $lines | Set-Content -Path $ProfileFile -Encoding UTF8
    Print-OK "Created: $ProfileFile"
} else {
    # Check if existing profiles.yml has the required 'user' field
    $existingContent = Get-Content $ProfileFile -Raw
    if ($existingContent -notmatch 'user:') {
        Print-Info "Existing profiles.yml missing 'user' field - updating..."
        $lines = @(
            "# ~/.dbt/profiles.yml",
            "# dbt connection to Trino - DO NOT commit to Git",
            "",
            "crypto_lakehouse:",
            "  target: dev",
            "  outputs:",
            "    dev:",
            "      type: trino",
            "      method: none",
            "      user: dbt_user",
            "      host: localhost",
            "      port: 8080",
            "      database: delta",
            "      schema: default",
            "      threads: 4",
            "      http_scheme: http"
        )
        $lines | Set-Content -Path $ProfileFile -Encoding UTF8
        Print-OK "Updated profiles.yml with required 'user' field"
    } else {
        Print-OK "profiles.yml OK: $ProfileFile"
    }
}

# ===========================================================================
# STEP 3: Install dbt in a virtual environment
# ===========================================================================
Print-Step "STEP 3: Install dbt-core + dbt-trino"

if (-not (Test-Path $VenvDbt)) {
    Print-Info "Creating .venv-dbt virtual environment..."
    python -m venv $VenvPath

    Print-Info "Installing dbt-core and dbt-trino (may take 2-3 minutes)..."
    & $VenvPip install --quiet dbt-core dbt-trino
    Print-OK "dbt installed in .venv-dbt"
} else {
    Print-OK "dbt already installed in .venv-dbt"
}

$dbtVer = & $VenvDbt --version 2>&1 | Select-String "dbt Core"
Print-OK "Version: $dbtVer"

# ===========================================================================
# STEP 4: Start Docker services
# ===========================================================================
Print-Step "STEP 4: Start Docker services (Trino stack only)"

Print-Info "Starting: postgres, minio, mc, hive-metastore, trino"
docker-compose up -d postgres minio mc hive-metastore trino

Print-Info "Waiting 20 seconds for initial startup..."
Start-Sleep -Seconds 20

# ===========================================================================
# STEP 5: Wait for Trino to be ready
# ===========================================================================
Print-Step "STEP 5: Wait for Trino + Hive Metastore to be ready"

# --- 5a: Wait for Trino HTTP ---
$maxAttempts = 24
$attempt     = 0
$trinoReady  = $false

while (-not $trinoReady -and $attempt -lt $maxAttempts) {
    $attempt++
    try {
        $resp = Invoke-WebRequest -Uri "$TrinoUrl/v1/info" -UseBasicParsing -TimeoutSec 5
        $info = $resp.Content | ConvertFrom-Json
        if ($info.starting -eq $false) {
            $trinoReady = $true
            Print-OK "Trino HTTP is ready (attempt $attempt / $maxAttempts)"
        } else {
            Print-Info "Trino still starting... (attempt $attempt / $maxAttempts)"
            Start-Sleep -Seconds 5
        }
    } catch {
        Print-Info "Waiting for Trino... (attempt $attempt / $maxAttempts)"
        Start-Sleep -Seconds 5
    }
}

if (-not $trinoReady) {
    Print-Fail "Trino did not start after $($maxAttempts * 5) seconds."
    Print-Info "Check logs: docker-compose logs trino"
    exit 1
}

# --- 5b: Poll until Hive Metastore is reachable via Trino ---
# Strategy: run "SHOW SCHEMAS FROM delta" - if it succeeds, HM is ready.
# Retry on ANY Trino error (not just 'metastore' messages).
Print-Info "Polling Hive Metastore readiness via Trino..."
$hmHeaders = @{
    "X-Trino-User"    = "health_check"
    "X-Trino-Catalog" = "delta"
    "X-Trino-Schema"  = "default"
    "Content-Type"    = "application/json"
}
$hmReady   = $false
$hmAttempt = 0
while (-not $hmReady -and $hmAttempt -lt 25) {
    $hmAttempt++
    try {
        $sub = Invoke-RestMethod -Uri "$TrinoUrl/v1/statement" -Method POST `
               -Headers $hmHeaders -Body "SHOW SCHEMAS FROM delta" `
               -UseBasicParsing -ErrorAction SilentlyContinue

        $nxt = $sub.nextUri
        $lat = $sub
        $p   = 0
        while ($nxt -and $p -lt 30) {
            Start-Sleep -Milliseconds 800
            $lat = Invoke-RestMethod -Uri $nxt -Method GET -Headers $hmHeaders -UseBasicParsing
            $nxt = $lat.nextUri
            $p++
        }

        if ($lat.error) {
            # ANY error = HM not ready yet, keep retrying
            $errShort = $lat.error.message -replace "`n"," "
            if ($errShort.Length -gt 80) { $errShort = $errShort.Substring(0,80) + "..." }
            Print-Info "HM not ready ($hmAttempt/25): $errShort"
            Start-Sleep -Seconds 8
        } else {
            $hmReady = $true
            Print-OK "Hive Metastore is ready! (attempt $hmAttempt / 25)"
        }
    } catch {
        Print-Info "HM poll error ($hmAttempt/25): $_"
        Start-Sleep -Seconds 8
    }
}

if (-not $hmReady) {
    # WARNING only - do not exit. Let STEP 6 fail with a more specific error.
    # The user needs to check hive-metastore logs to fix the root cause.
    Write-Host ""
    Write-Host "  [WARN] Hive Metastore did not respond after 25 attempts (~3.5 min)." -ForegroundColor Red
    Write-Host "         Root cause: hive-metastore container is likely crashing." -ForegroundColor Red
    Write-Host ""
    Write-Host "  --- DIAGNOSE: run these commands in a NEW PowerShell window ---" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  # 1. Check if container is running or restarting:" -ForegroundColor White
    Write-Host "  docker ps -a | findstr hive" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  # 2. Read hive-metastore startup logs (look for ERROR or Exception):" -ForegroundColor White
    Write-Host "  docker-compose logs --tail=80 hive-metastore" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  # 3. If you see 'initSchema FAILED' - the Postgres schema needs reset:" -ForegroundColor White
    Write-Host "  docker-compose down -v  (WARNING: deletes all data!)" -ForegroundColor Yellow
    Write-Host "  docker-compose up -d postgres minio mc hive-metastore trino" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  # 4. If you see 'OutOfMemoryError' - increase mem_limit in docker-compose.yml:" -ForegroundColor White
    Write-Host "  hive-metastore: mem_limit: 1g  (change from 512m)" -ForegroundColor Yellow
    Write-Host ""
    $cont = Read-Host "  Continue to STEP 6 anyway to see the actual Trino error? (y/n)"
    if ($cont -ne "y") { exit 1 }
}

# ===========================================================================
# STEP 6: Create mock gold_ohlcv table in Trino via REST API
# ===========================================================================
Print-Step "STEP 6: Create mock gold_ohlcv table in Trino"

function Invoke-TrinoSQL {
    param(
        [string]$SQL,
        [string]$Label = "query"
    )
    Print-Info "Running SQL: $Label"

    $headers = @{
        "X-Trino-User"    = "dbt_test"
        "X-Trino-Catalog" = "delta"
        "X-Trino-Schema"  = "default"
        "Content-Type"    = "application/json"
    }

    $submitResp = Invoke-RestMethod `
        -Uri "$TrinoUrl/v1/statement" `
        -Method POST `
        -Headers $headers `
        -Body $SQL `
        -UseBasicParsing `
        -ErrorAction SilentlyContinue

    $nextUri  = $submitResp.nextUri
    $lastResp = $submitResp
    $polls    = 0

    while ($nextUri -and $polls -lt 120) {
        Start-Sleep -Milliseconds 600
        $pollResp = Invoke-RestMethod -Uri $nextUri -Method GET -Headers $headers -UseBasicParsing
        $lastResp = $pollResp
        $nextUri  = $pollResp.nextUri
        $polls++
    }

    if ($lastResp.error) {
        $errMsg = $lastResp.error.message
        if ($errMsg -match "already exists") {
            Print-Info "Table already exists - skipping create"
            return $null
        }
        throw "Trino error in '$Label': $errMsg"
    }
    return $lastResp
}

try {
    # Drop old table
    Invoke-TrinoSQL -SQL "DROP TABLE IF EXISTS delta.default.gold_ohlcv" -Label "DROP IF EXISTS"
    Print-OK "Old table dropped (if existed)"

    # Create table
    $createSQL = @'
CREATE TABLE delta.default.gold_ohlcv (
    symbol         VARCHAR,
    window_start   TIMESTAMP(6),
    window_end     TIMESTAMP(6),
    window_minutes INTEGER,
    open           DOUBLE,
    high           DOUBLE,
    low            DOUBLE,
    close          DOUBLE,
    volume         DOUBLE,
    trade_count    BIGINT,
    sma_5          DOUBLE,
    sma_20         DOUBLE
)
WITH (location = 's3://gold/ohlcv/')
'@
    Invoke-TrinoSQL -SQL $createSQL -Label "CREATE TABLE gold_ohlcv"
    Print-OK "Table gold_ohlcv created in Delta Lake (MinIO s3://gold/ohlcv/)"

    # Insert mock data
    $insertSQL = @'
INSERT INTO delta.default.gold_ohlcv VALUES
('BTCUSDT',  TIMESTAMP '2026-04-12 10:00:00', TIMESTAMP '2026-04-12 10:01:00', 1, 69420.50, 69500.00, 69380.00, 69455.00,    12.5,  350, 69430.00, 69200.00),
('BTCUSDT',  TIMESTAMP '2026-04-12 10:01:00', TIMESTAMP '2026-04-12 10:02:00', 1, 69455.00, 69520.00, 69400.00, 69510.00,    15.2,  420, 69450.00, 69210.00),
('BTCUSDT',  TIMESTAMP '2026-04-12 10:02:00', TIMESTAMP '2026-04-12 10:03:00', 1, 69510.00, 69580.00, 69490.00, 69530.00,    11.8,  390, 69480.00, 69215.00),
('BTCUSDT',  TIMESTAMP '2026-04-12 10:03:00', TIMESTAMP '2026-04-12 10:04:00', 1, 69530.00, 69550.00, 69460.00, 69490.00,     9.3,  310, 69495.00, 69218.00),
('BTCUSDT',  TIMESTAMP '2026-04-12 10:04:00', TIMESTAMP '2026-04-12 10:05:00', 1, 69490.00, 69510.00, 69400.00, 69420.00,    14.1,  450, 69481.00, 69222.00),
('BTCUSDT',  TIMESTAMP '2026-04-12 10:00:00', TIMESTAMP '2026-04-12 10:05:00', 5, 69420.50, 69580.00, 69380.00, 69420.00,    63.0, 1920, 69450.00, 69200.00),
('ETHUSDT',  TIMESTAMP '2026-04-12 10:00:00', TIMESTAMP '2026-04-12 10:01:00', 1,  3500.00,  3510.00,  3490.00,  3505.00,    85.3,  210,  3500.00,  3480.00),
('ETHUSDT',  TIMESTAMP '2026-04-12 10:01:00', TIMESTAMP '2026-04-12 10:02:00', 1,  3505.00,  3520.00,  3500.00,  3515.00,    92.1,  235,  3505.00,  3482.00),
('ETHUSDT',  TIMESTAMP '2026-04-12 10:02:00', TIMESTAMP '2026-04-12 10:03:00', 1,  3515.00,  3530.00,  3510.00,  3525.00,    78.4,  198,  3510.00,  3484.00),
('ETHUSDT',  TIMESTAMP '2026-04-12 10:03:00', TIMESTAMP '2026-04-12 10:04:00', 1,  3525.00,  3535.00,  3515.00,  3520.00,    65.9,  175,  3515.00,  3486.00),
('ETHUSDT',  TIMESTAMP '2026-04-12 10:04:00', TIMESTAMP '2026-04-12 10:05:00', 1,  3520.00,  3525.00,  3505.00,  3510.00,    88.7,  220,  3515.00,  3488.00),
('ETHUSDT',  TIMESTAMP '2026-04-12 10:00:00', TIMESTAMP '2026-04-12 10:05:00', 5,  3500.00,  3535.00,  3490.00,  3510.00,   410.4, 1038,  3505.00,  3480.00),
('BNBUSDT',  TIMESTAMP '2026-04-12 10:00:00', TIMESTAMP '2026-04-12 10:01:00', 1,   580.00,   582.00,   579.00,   581.50,    50.0,  180,   580.00,   575.00),
('BNBUSDT',  TIMESTAMP '2026-04-12 10:01:00', TIMESTAMP '2026-04-12 10:02:00', 1,   581.50,   583.00,   580.50,   582.00,    45.2,  165,   580.50,   575.50),
('BNBUSDT',  TIMESTAMP '2026-04-12 10:02:00', TIMESTAMP '2026-04-12 10:03:00', 1,   582.00,   584.00,   581.00,   583.50,    52.8,  192,   581.50,   576.00),
('BNBUSDT',  TIMESTAMP '2026-04-12 10:03:00', TIMESTAMP '2026-04-12 10:04:00', 1,   583.50,   585.00,   582.50,   584.00,    48.1,  170,   582.30,   576.50),
('BNBUSDT',  TIMESTAMP '2026-04-12 10:04:00', TIMESTAMP '2026-04-12 10:05:00', 1,   584.00,   584.50,   582.00,   583.00,    41.7,  155,   582.80,   577.00),
('BNBUSDT',  TIMESTAMP '2026-04-12 10:00:00', TIMESTAMP '2026-04-12 10:05:00', 5,   580.00,   585.00,   579.00,   583.00,   237.8,  862,   581.00,   575.00),
('PEPEUSDT', TIMESTAMP '2026-04-12 10:00:00', TIMESTAMP '2026-04-12 10:01:00', 1, 0.00001234, 0.00001256, 0.00001220, 0.00001245, 5000000.0, 450, 0.00001240, 0.00001210),
('PEPEUSDT', TIMESTAMP '2026-04-12 10:01:00', TIMESTAMP '2026-04-12 10:02:00', 1, 0.00001245, 0.00001270, 0.00001238, 0.00001260, 4800000.0, 425, 0.00001248, 0.00001214),
('SHIBUSDT', TIMESTAMP '2026-04-12 10:00:00', TIMESTAMP '2026-04-12 10:01:00', 1, 0.00002345, 0.00002380, 0.00002330, 0.00002360, 3000000.0, 320, 0.00002350, 0.00002300),
('SHIBUSDT', TIMESTAMP '2026-04-12 10:01:00', TIMESTAMP '2026-04-12 10:02:00', 1, 0.00002360, 0.00002400, 0.00002355, 0.00002390, 2900000.0, 305, 0.00002365, 0.00002305)
'@
    Invoke-TrinoSQL -SQL $insertSQL -Label "INSERT 22 mock rows"
    Print-OK "Inserted 22 mock rows into gold_ohlcv"

    # Verify count
    $countResp = Invoke-TrinoSQL -SQL "SELECT COUNT(*) FROM delta.default.gold_ohlcv" -Label "COUNT rows"
    if ($countResp -and $countResp.data) {
        $rowCount = $countResp.data[0][0]
        Print-OK "Verified: $rowCount rows in gold_ohlcv"
    }

} catch {
    Print-Fail "Failed to create mock table via REST API: $_"
    Write-Host ""
    Write-Host "  MANUAL FALLBACK: Open http://localhost:8080 in browser," -ForegroundColor Yellow
    Write-Host "  then run the SQL in: dbt\scripts\create_mock_gold_trino.sql" -ForegroundColor Yellow
    Write-Host "  Then re-run this script (it will skip to dbt steps)." -ForegroundColor Yellow
    Write-Host ""
    $continue = Read-Host "  Continue to dbt steps anyway? (y/n)"
    if ($continue -ne "y") { exit 1 }
}

# ===========================================================================
# STEP 7: Run dbt pipeline
# ===========================================================================
Print-Step "STEP 7: Run dbt pipeline"

# CRITICAL: Force Python to use UTF-8 for all file I/O.
# Without this, dbt reads .sql/.yml files using Windows default cp1252 encoding,
# which fails on UTF-8 characters (Vietnamese comments, special chars) in our files.
$env:PYTHONUTF8        = "1"
$env:PYTHONIOENCODING  = "utf-8"

Set-Location $DbtDir

# dbt deps
Print-Info "Running: dbt deps"
& $VenvDbt deps
if ($LASTEXITCODE -ne 0) {
    Print-Fail "dbt deps failed"
    Set-Location $ProjectRoot
    exit 1
}
Print-OK "dbt deps: packages installed"

# dbt debug
Print-Info "Running: dbt debug"
& $VenvDbt debug
$debugOk = $LASTEXITCODE -eq 0
if ($debugOk) {
    Print-OK "dbt debug: Trino connection OK"
} else {
    Print-Fail "dbt debug failed - check profiles.yml and Trino container"
    Set-Location $ProjectRoot
    exit 1
}

# dbt run
Print-Info "Running: dbt run"
& $VenvDbt run
$runOk = $LASTEXITCODE -eq 0
if ($runOk) {
    Print-OK "dbt run: all models built successfully"
} else {
    Print-Fail "dbt run had errors - see output above"
}

# dbt test (exclude the realtime gap test since mock data is old timestamps)
Print-Info "Running: dbt test (excluding realtime gap test)"
& $VenvDbt test --exclude test_no_missing_1min_candles
$testOk = $LASTEXITCODE -eq 0

Set-Location $ProjectRoot

# ===========================================================================
# STEP 8: Results
# ===========================================================================
Print-Step "STEP 8: Results"

Write-Host ""
Write-Host "  +----------------------------------------+" -ForegroundColor White
Write-Host "  |         TEST RESULTS SUMMARY            |" -ForegroundColor White
Write-Host "  +----------------------------------------+" -ForegroundColor White
if ($runOk)  { Write-Host "  | dbt run  : [PASS]                      |" -ForegroundColor Green }
else         { Write-Host "  | dbt run  : [FAIL] - check logs above   |" -ForegroundColor Red }
if ($testOk) { Write-Host "  | dbt test : [PASS] - all tests passed   |" -ForegroundColor Green }
else         { Write-Host "  | dbt test : [FAIL] - some tests failed  |" -ForegroundColor Red }
Write-Host "  +----------------------------------------+" -ForegroundColor White
Write-Host ""

if ($runOk -and $testOk) {
    Write-Host "  SUCCESS! dbt module is working correctly." -ForegroundColor Green
    Write-Host ""
    Write-Host "  Next steps:" -ForegroundColor Cyan
    Write-Host "    1. View lineage docs:" -ForegroundColor White
    Write-Host "       cd dbt" -ForegroundColor Yellow
    Write-Host "       $VenvDbt docs generate" -ForegroundColor Yellow
    Write-Host "       $VenvDbt docs serve" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "    2. Query mart in Trino UI (http://localhost:8080):" -ForegroundColor White
    Write-Host "       SELECT * FROM delta.marts.mart_crypto_dashboard LIMIT 10" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "    3. When Teammate 1 adds silver_to_gold.py:" -ForegroundColor White
    Write-Host "       dbt run (uses real gold data automatically)" -ForegroundColor Yellow
} else {
    Write-Host "  Some steps failed. Debug commands:" -ForegroundColor Red
    Write-Host "    cd dbt" -ForegroundColor Yellow
    Write-Host "    $VenvDbt test --store-failures" -ForegroundColor Yellow
    Write-Host "    $VenvDbt test --select stg_gold_ohlcv" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "  Trino UI:  http://localhost:8080" -ForegroundColor Cyan
Write-Host "  MinIO UI:  http://localhost:9001  (admin / admin123)" -ForegroundColor Cyan
Write-Host ""
Write-Host "Script done. $(Get-Date -Format 'HH:mm:ss')" -ForegroundColor Gray
