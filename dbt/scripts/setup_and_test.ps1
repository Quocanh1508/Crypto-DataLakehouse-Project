# =============================================================================
# setup_and_test.ps1 - dbt Setup & Test Script for Crypto Data Lakehouse
# =============================================================================
# Run from project root:
#   cd c:\HCMUTE\nam3ki2_1\bigdata\Crypto-DataLakehouse-Project
#   .\dbt\scripts\setup_and_test.ps1
#
# UPDATED for new infrastructure:
#   - Storage: GCS (gs://crypto-lakehouse-group8/) instead of MinIO (s3://)
#   - Gold schema: candle_time, candle_date, candle_duration, tick_count,
#                  ma_7/ma_20/ma_50, processing_timestamp
#   - No MinIO/mc services needed anymore
# =============================================================================

# Do NOT set ErrorActionPreference=Stop globally.
# Docker, dbt, etc. write warnings to stderr which would cause false failures.
$ErrorActionPreference = "Continue"
$ProjectRoot = (Get-Location).Path
$DbtDir      = Join-Path $ProjectRoot "dbt"
$TrinoUrl    = "http://localhost:8080"
$VenvPath    = Join-Path $ProjectRoot ".venv-dbt"
$VenvDbt     = Join-Path $VenvPath "Scripts\dbt.exe"
$VenvPip     = Join-Path $VenvPath "Scripts\pip.exe"

function Print-Step  { param($msg) Write-Host "`n=== $msg ===" -ForegroundColor Cyan }
function Print-OK    { param($msg) Write-Host "  [OK]  $msg" -ForegroundColor Green }
function Print-Fail  { param($msg) Write-Host "  [ERR] $msg" -ForegroundColor Red }
function Print-Info  { param($msg) Write-Host "  [..] $msg"  -ForegroundColor Yellow }
function Print-Warn  { param($msg) Write-Host "  [!!] $msg"  -ForegroundColor Magenta }

Write-Host ""
Write-Host "============================================================" -ForegroundColor Magenta
Write-Host "  dbt Setup and Test - Crypto Data Lakehouse - Teammate 2  " -ForegroundColor Magenta
Write-Host "  Schema: silver_to_gold.py (GCS-based Gold layer)        " -ForegroundColor Magenta
Write-Host "============================================================" -ForegroundColor Magenta

# ===========================================================================
# STEP 1: Check prerequisites
# ===========================================================================
Print-Step "STEP 1: Check prerequisites"

$null = docker version 2>&1
if ($LASTEXITCODE -ne 0) {
    Print-Fail "Docker Desktop is NOT running. Please start it and retry."
    exit 1
}
Print-OK "Docker Desktop is running"

$pyVer = python --version 2>&1
if ($LASTEXITCODE -ne 0) {
    Print-Fail "Python not found in PATH"
    exit 1
}
Print-OK "Python: $pyVer"

if (-not (Test-Path (Join-Path $ProjectRoot "docker-compose.yml"))) {
    Print-Fail "Run from project root (where docker-compose.yml is located)"
    exit 1
}
Print-OK "Working directory: $ProjectRoot"

# ===========================================================================
# STEP 2: Create ~/.dbt/profiles.yml if missing
# ===========================================================================
Print-Step "STEP 2: Create profiles.yml"

$DbtHome     = Join-Path $env:USERPROFILE ".dbt"
$ProfileFile = Join-Path $DbtHome "profiles.yml"

$profileLines = @(
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

if (-not (Test-Path $ProfileFile)) {
    Print-Info "profiles.yml not found - creating..."
    if (-not (Test-Path $DbtHome)) {
        New-Item -ItemType Directory -Path $DbtHome -Force | Out-Null
    }
    $profileLines | Set-Content -Path $ProfileFile -Encoding UTF8
    Print-OK "Created: $ProfileFile"
} else {
    $existingContent = Get-Content $ProfileFile -Raw
    if ($existingContent -notmatch 'user:') {
        Print-Info "Existing profiles.yml missing 'user' field - updating..."
        $profileLines | Set-Content -Path $ProfileFile -Encoding UTF8
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
# STEP 4: Start Docker services (Trino stack)
# ===========================================================================
Print-Step "STEP 4: Start Docker services (Trino stack)"

# New infrastructure: MinIO provides local S3 storage for Trino's Delta connector.
Print-Info "Starting: minio, mc, postgres, hive-metastore, trino"
docker-compose up -d minio mc postgres hive-metastore trino

Print-Info "Waiting 20 seconds for initial startup..."
Start-Sleep -Seconds 20

# ===========================================================================
# STEP 5: Wait for Trino to be ready
# ===========================================================================
Print-Step "STEP 5: Wait for Trino + Hive Metastore"

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

# --- 5b: Poll Hive Metastore via Trino ---
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
    Write-Host ""
    Print-Warn "Hive Metastore did not respond after 25 attempts (~3.5 min)."
    Print-Warn "This may be caused by missing GCS credentials."
    Write-Host ""
    Write-Host "  --- DIAGNOSE ---" -ForegroundColor Cyan
    Write-Host "  # 1. Check container status:" -ForegroundColor White
    Write-Host '  docker ps -a --format "table {{.Names}}\t{{.Status}}" | Select-String hive' -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  # 2. Check hive-metastore logs:" -ForegroundColor White
    Write-Host "  docker-compose logs --tail=50 hive-metastore" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  # 3. Verify GCS credentials exist:" -ForegroundColor White
    Write-Host '  Test-Path "$env:APPDATA\gcloud\application_default_credentials.json"' -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  # 4. If missing GCS credentials, run:" -ForegroundColor White
    Write-Host "  gcloud auth application-default login" -ForegroundColor Yellow
    Write-Host ""
    $cont = Read-Host "  Continue to dbt steps anyway? (y/n)"
    if ($cont -ne "y") { exit 1 }
}

# ===========================================================================
# STEP 6: Create mock gold_ohlcv table via Trino REST API
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

    # Create table matching silver_to_gold.py output schema
    $createSQL = @'
CREATE TABLE delta.default.gold_ohlcv (
    symbol                VARCHAR,
    candle_time           TIMESTAMP(6),
    candle_date           DATE,
    candle_duration       VARCHAR,
    open                  DOUBLE,
    high                  DOUBLE,
    low                   DOUBLE,
    close                 DOUBLE,
    volume                DOUBLE,
    tick_count            BIGINT,
    ma_7                  DOUBLE,
    ma_20                 DOUBLE,
    ma_50                 DOUBLE,
    processing_timestamp  VARCHAR
)
WITH (
    location = 's3a://gold/gold_ohlcv',
    partitioned_by = ARRAY['symbol', 'candle_date']
)
'@
    Invoke-TrinoSQL -SQL $createSQL -Label "CREATE TABLE gold_ohlcv"
    Print-OK "Table gold_ohlcv created in Delta Lake"

    # Clear any stale data from previous runs
    # (DROP TABLE with explicit location only removes Hive metadata, S3 data persists)
    Invoke-TrinoSQL -SQL "DELETE FROM delta.default.gold_ohlcv" -Label "DELETE stale rows"
    Print-OK "Cleared stale data (if any)"

    # Insert mock data (22 rows, 5 symbols)
    $insertSQL = @'
INSERT INTO delta.default.gold_ohlcv VALUES
('BTCUSDT',  TIMESTAMP '2026-04-13 10:00:00', DATE '2026-04-13', '1 minute',  69420.50, 69500.00, 69380.00, 69455.00, 12.5,  350, 69430.00, 69200.00, 69100.00, '2026-04-13T17:00:00Z'),
('BTCUSDT',  TIMESTAMP '2026-04-13 10:01:00', DATE '2026-04-13', '1 minute',  69455.00, 69520.00, 69400.00, 69510.00, 15.2,  420, 69450.00, 69210.00, 69105.00, '2026-04-13T17:00:00Z'),
('BTCUSDT',  TIMESTAMP '2026-04-13 10:02:00', DATE '2026-04-13', '1 minute',  69510.00, 69580.00, 69490.00, 69530.00, 11.8,  390, 69480.00, 69215.00, 69108.00, '2026-04-13T17:00:00Z'),
('BTCUSDT',  TIMESTAMP '2026-04-13 10:03:00', DATE '2026-04-13', '1 minute',  69530.00, 69550.00, 69460.00, 69490.00,  9.3,  310, 69495.00, 69218.00, 69110.00, '2026-04-13T17:00:00Z'),
('BTCUSDT',  TIMESTAMP '2026-04-13 10:04:00', DATE '2026-04-13', '1 minute',  69490.00, 69510.00, 69400.00, 69420.00, 14.1,  450, 69481.00, 69222.00, 69112.00, '2026-04-13T17:00:00Z'),
('BTCUSDT',  TIMESTAMP '2026-04-13 10:00:00', DATE '2026-04-13', '5 minutes', 69420.50, 69580.00, 69380.00, 69420.00, 63.0, 1920, 69450.00, 69200.00, 69100.00, '2026-04-13T17:00:00Z'),
('ETHUSDT',  TIMESTAMP '2026-04-13 10:00:00', DATE '2026-04-13', '1 minute',   3500.00,  3510.00,  3490.00,  3505.00, 85.3,  210,  3500.00,  3480.00,  3460.00, '2026-04-13T17:00:00Z'),
('ETHUSDT',  TIMESTAMP '2026-04-13 10:01:00', DATE '2026-04-13', '1 minute',   3505.00,  3520.00,  3500.00,  3515.00, 92.1,  235,  3505.00,  3482.00,  3462.00, '2026-04-13T17:00:00Z'),
('ETHUSDT',  TIMESTAMP '2026-04-13 10:02:00', DATE '2026-04-13', '1 minute',   3515.00,  3530.00,  3510.00,  3525.00, 78.4,  198,  3510.00,  3484.00,  3464.00, '2026-04-13T17:00:00Z'),
('ETHUSDT',  TIMESTAMP '2026-04-13 10:03:00', DATE '2026-04-13', '1 minute',   3525.00,  3535.00,  3515.00,  3520.00, 65.9,  175,  3515.00,  3486.00,  3465.00, '2026-04-13T17:00:00Z'),
('ETHUSDT',  TIMESTAMP '2026-04-13 10:04:00', DATE '2026-04-13', '1 minute',   3520.00,  3525.00,  3505.00,  3510.00, 88.7,  220,  3515.00,  3488.00,  3466.00, '2026-04-13T17:00:00Z'),
('ETHUSDT',  TIMESTAMP '2026-04-13 10:00:00', DATE '2026-04-13', '5 minutes',  3500.00,  3535.00,  3490.00,  3510.00,410.4, 1038,  3505.00,  3480.00,  3460.00, '2026-04-13T17:00:00Z'),
('BNBUSDT',  TIMESTAMP '2026-04-13 10:00:00', DATE '2026-04-13', '1 minute',    580.00,   582.00,   579.00,   581.50, 50.0,  180,   580.00,   575.00,   570.00, '2026-04-13T17:00:00Z'),
('BNBUSDT',  TIMESTAMP '2026-04-13 10:01:00', DATE '2026-04-13', '1 minute',    581.50,   583.00,   580.50,   582.00, 45.2,  165,   580.50,   575.50,   570.50, '2026-04-13T17:00:00Z'),
('BNBUSDT',  TIMESTAMP '2026-04-13 10:02:00', DATE '2026-04-13', '1 minute',    582.00,   584.00,   581.00,   583.50, 52.8,  192,   581.50,   576.00,   571.00, '2026-04-13T17:00:00Z'),
('BNBUSDT',  TIMESTAMP '2026-04-13 10:03:00', DATE '2026-04-13', '1 minute',    583.50,   585.00,   582.50,   584.00, 48.1,  170,   582.30,   576.50,   571.50, '2026-04-13T17:00:00Z'),
('BNBUSDT',  TIMESTAMP '2026-04-13 10:04:00', DATE '2026-04-13', '1 minute',    584.00,   584.50,   582.00,   583.00, 41.7,  155,   582.80,   577.00,   572.00, '2026-04-13T17:00:00Z'),
('BNBUSDT',  TIMESTAMP '2026-04-13 10:00:00', DATE '2026-04-13', '5 minutes',   580.00,   585.00,   579.00,   583.00,237.8,  862,   581.00,   575.00,   570.00, '2026-04-13T17:00:00Z'),
('PEPEUSDT', TIMESTAMP '2026-04-13 10:00:00', DATE '2026-04-13', '1 minute',  0.00001234, 0.00001256, 0.00001220, 0.00001245, 5000000.0, 450, 0.00001240, 0.00001210, 0.00001180, '2026-04-13T17:00:00Z'),
('PEPEUSDT', TIMESTAMP '2026-04-13 10:01:00', DATE '2026-04-13', '1 minute',  0.00001245, 0.00001270, 0.00001238, 0.00001260, 4800000.0, 425, 0.00001248, 0.00001214, 0.00001183, '2026-04-13T17:00:00Z'),
('SHIBUSDT', TIMESTAMP '2026-04-13 10:00:00', DATE '2026-04-13', '1 minute',  0.00002345, 0.00002380, 0.00002330, 0.00002360, 3000000.0, 320, 0.00002350, 0.00002300, 0.00002260, '2026-04-13T17:00:00Z'),
('SHIBUSDT', TIMESTAMP '2026-04-13 10:01:00', DATE '2026-04-13', '1 minute',  0.00002360, 0.00002400, 0.00002355, 0.00002390, 2900000.0, 305, 0.00002365, 0.00002305, 0.00002265, '2026-04-13T17:00:00Z')
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
    Print-Warn "Could not create mock table: $_"
    Write-Host ""
    Write-Host "  This is expected if Trino cannot access storage (GCS or MinIO)." -ForegroundColor Yellow
    Write-Host "  The mock table requires a working storage backend." -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  OPTIONS:" -ForegroundColor Cyan
    Write-Host "  1. If GCS is configured: ensure Trino has GCS credentials" -ForegroundColor White
    Write-Host "     (mount gcloud creds into trino container in docker-compose.yml)" -ForegroundColor Yellow
    Write-Host "  2. Manual creation: open http://localhost:8080 and run SQL from:" -ForegroundColor White
    Write-Host "     dbt\scripts\create_mock_gold_trino.sql" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  Continuing to dbt steps (compile-only mode)..." -ForegroundColor Yellow
    Write-Host ""
}

# ===========================================================================
# STEP 6b: Register GCS production table (optional — skips if no GCS creds)
# ===========================================================================
Print-Step "STEP 6b: Register GCS Gold table (real data)"

$gcsCredsPath = Join-Path $env:APPDATA "gcloud\application_default_credentials.json"
if (-not (Test-Path $gcsCredsPath)) {
    Print-Warn "No GCS credentials found at: $gcsCredsPath"
    Write-Host "  Skipping GCS table registration. Using mock data only." -ForegroundColor Yellow
    Write-Host "  To enable: run 'gcloud auth application-default login'" -ForegroundColor Cyan
    Write-Host ""
} else {
    Print-OK "GCS credentials found: $gcsCredsPath"
    try {
        # Create schema 'gcs' in delta_gcs catalog (if not exists)
        $createSchemaSQL = "CREATE SCHEMA IF NOT EXISTS delta_gcs.gcs"
        Invoke-TrinoSQL -SQL $createSchemaSQL -Label "CREATE SCHEMA delta_gcs.gcs"

        # Register the GCS Gold table using Delta Lake register_table procedure.
        # NOTE: Delta Lake connector does NOT use CREATE TABLE WITH (format=...).
        #       It reads the Delta transaction log directly at the given location.
        #       Use CALL system.register_table() for existing Delta tables.
        $dropGCSSQL = "CALL delta_gcs.system.unregister_table(schema_name => 'gcs', table_name => 'gold_ohlcv')"
        try {
            Invoke-TrinoSQL -SQL $dropGCSSQL -Label "UNREGISTER GCS table (if stale)" | Out-Null
        } catch {
            # Table may not exist yet, that's fine
        }

        $registerGCSSQL = @'
CALL delta_gcs.system.register_table(
    schema_name   => 'gcs',
    table_name    => 'gold_ohlcv',
    table_location => 'gs://crypto-lakehouse-group8/gold'
)
'@
        Invoke-TrinoSQL -SQL $registerGCSSQL -Label "REGISTER GCS gold_ohlcv (Delta)"
        Print-OK "GCS table registered at delta_gcs.gcs.gold_ohlcv"


        # Count real rows
        $gcsCount = Invoke-TrinoSQL -SQL "SELECT COUNT(*) FROM delta_gcs.gcs.gold_ohlcv" -Label "COUNT GCS rows"
        if ($gcsCount -and $gcsCount.data) {
            $realRows = $gcsCount.data[0][0]
            Print-OK "Real GCS data: $realRows rows in gold_ohlcv"

            if ([int]$realRows -gt 1000) {
                Write-Host "  >> Real production data detected! ($realRows rows)" -ForegroundColor Green
            }
        }

        # Run dbt source tests on GCS data
        # NOTE: Must cd to $DbtDir and set UTF8 before calling dbt (same as STEP 7)
        Write-Host ""
        Write-Host "  [..] Running source tests on REAL GCS data..." -ForegroundColor Cyan
        $env:PYTHONUTF8       = "1"
        $env:PYTHONIOENCODING = "utf-8"
        Push-Location $DbtDir
        & $VenvDbt test --select "source:gold_gcs" 2>&1
        $gcsTestOk = $LASTEXITCODE -eq 0
        Pop-Location
        if ($gcsTestOk) {
            Print-OK "dbt test (GCS source): PASS on real data"
        } else {
            Print-Warn "dbt test (GCS source): some failures on real data (check output above)"
        }

    } catch {
        Print-Warn "GCS table registration failed: $_"
        Write-Host "  Possible causes:" -ForegroundColor Yellow
        Write-Host "  - GCS credentials expired (run: gcloud auth application-default login)" -ForegroundColor Yellow
        Write-Host "  - delta_gcs catalog not loaded (restart Trino after docker-compose up)" -ForegroundColor Yellow
        Write-Host "  Continuing with mock data tests..." -ForegroundColor Yellow
    }
}

# ===========================================================================
# STEP 7: Run dbt pipeline
# ===========================================================================
Print-Step "STEP 7: Run dbt pipeline"

# CRITICAL: Force Python to use UTF-8 for all file I/O.
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

# dbt compile (always works - validates SQL syntax without executing)
Print-Info "Running: dbt compile (validates all SQL models)"
& $VenvDbt compile
$compileOk = $LASTEXITCODE -eq 0
if ($compileOk) {
    Print-OK "dbt compile: all models compiled successfully"
} else {
    Print-Fail "dbt compile had errors - see output above"
    Print-Info "Fix SQL errors before proceeding to dbt run"
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
    Print-Warn "dbt run had errors (may be expected if no Gold data exists yet)"
}

# dbt test (exclude the realtime gap test since mock data uses old timestamps)
Print-Info "Running: dbt test (excluding realtime gap test)"
& $VenvDbt test --exclude test_no_missing_1min_candles
$testOk = $LASTEXITCODE -eq 0

Set-Location $ProjectRoot

# ===========================================================================
# STEP 8: Results
# ===========================================================================
Print-Step "STEP 8: Results"

Write-Host ""
Write-Host "  +------------------------------------------+" -ForegroundColor White
Write-Host "  |          TEST RESULTS SUMMARY             |" -ForegroundColor White
Write-Host "  +------------------------------------------+" -ForegroundColor White
if ($compileOk) { Write-Host "  | dbt compile : [PASS]                     |" -ForegroundColor Green }
else            { Write-Host "  | dbt compile : [FAIL] - check logs above  |" -ForegroundColor Red }
if ($runOk)     { Write-Host "  | dbt run     : [PASS]                     |" -ForegroundColor Green }
else            { Write-Host "  | dbt run     : [FAIL] - check logs above  |" -ForegroundColor Red }
if ($testOk)    { Write-Host "  | dbt test    : [PASS] - all tests passed  |" -ForegroundColor Green }
else            { Write-Host "  | dbt test    : [FAIL] - some tests failed |" -ForegroundColor Red }
Write-Host "  +------------------------------------------+" -ForegroundColor White
Write-Host ""

if ($compileOk -and $runOk -and $testOk) {
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
    Write-Host "    3. Connect Power BI via Trino ODBC (DirectQuery mode)" -ForegroundColor White
} elseif ($compileOk) {
    Write-Host "  dbt compile PASSED - SQL syntax is correct." -ForegroundColor Green
    Write-Host ""
    if (-not $runOk) {
        Write-Host "  dbt run failed. Likely causes:" -ForegroundColor Yellow
        Write-Host "    - Gold table (delta.default.gold_ohlcv) does not exist yet" -ForegroundColor Yellow
        Write-Host "    - Hive Metastore or storage backend not ready" -ForegroundColor Yellow
        Write-Host "    - GCS credentials not mounted in Trino container" -ForegroundColor Yellow
        Write-Host ""
        Write-Host "  Once Teammate 1 runs silver_to_gold.py and Gold data exists:" -ForegroundColor Cyan
        Write-Host "    cd dbt && $VenvDbt run && $VenvDbt test" -ForegroundColor Yellow
    }
} else {
    Write-Host "  dbt compile FAILED - SQL syntax errors need fixing." -ForegroundColor Red
    Write-Host "  Debug commands:" -ForegroundColor Yellow
    Write-Host "    cd dbt" -ForegroundColor Yellow
    Write-Host "    $VenvDbt compile --select stg_gold_ohlcv" -ForegroundColor Yellow
    Write-Host "    $VenvDbt compile --select mart_crypto_dashboard" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "  Trino UI:  http://localhost:8080" -ForegroundColor Cyan
Write-Host ""
Write-Host "Script done. $(Get-Date -Format 'HH:mm:ss')" -ForegroundColor Gray
