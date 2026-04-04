# cloud/gcs_setup.ps1
# ============================================================
# One-shot GCS setup script for crypto-lakehouse-group8
# Run AFTER: gcloud init && gcloud auth login
#
# What this script does:
#   1. Creates the main bucket gs://crypto-lakehouse-group8
#   2. Enables Uniform bucket-level access (IAM-based, no ACLs)
#   3. Creates the 4 logical folder structure (by uploading placeholder objects)
#   4. Applies a Lifecycle rule: Bronze data → Archive after 60 days
#   5. Creates the data-lakehouse-sa service account
#   6. Grants Least Privilege IAM roles to the SA
# ============================================================

$PROJECT_ID     = gcloud config get-value project
$BUCKET_NAME    = "crypto-lakehouse-group8"
$SA_NAME        = "data-lakehouse-sa"
$SA_EMAIL       = "$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"
$REGION         = "asia-southeast1"   # Singapore - lowest latency from Vietnam

Write-Host "=== GCS Cloud Setup for crypto-lakehouse-group8 ===" -ForegroundColor Cyan
Write-Host "Project : $PROJECT_ID"
Write-Host "Region  : $REGION"
Write-Host "Bucket  : gs://$BUCKET_NAME"
Write-Host ""

# ── Step 1: Create Bucket ─────────────────────────────────────────────────────
Write-Host "[1/5] Creating bucket gs://$BUCKET_NAME ..." -ForegroundColor Yellow
gcloud storage buckets create "gs://$BUCKET_NAME" `
    --project=$PROJECT_ID `
    --location=$REGION `
    --uniform-bucket-level-access  # Disable old ACL system, use IAM only

Write-Host "      ✅ Bucket created with Uniform bucket-level access."

# ── Step 2: Create logical folder structure ────────────────────────────────────
# GCS has no real folders; we create empty placeholder objects
Write-Host "[2/5] Creating folder structure ..." -ForegroundColor Yellow
foreach ($folder in @("bronze/", "silver/", "gold/", "checkpoints/")) {
    echo "" | gcloud storage cp - "gs://$BUCKET_NAME/$folder.keep"
    Write-Host "      Created gs://$BUCKET_NAME/$folder"
}
Write-Host "      ✅ Folder structure created."

# ── Step 3: Apply Lifecycle Rule (Bronze → Archive after 60 days) ─────────────
Write-Host "[3/5] Applying lifecycle rule to bronze/ ..." -ForegroundColor Yellow

$lifecycleJson = @'
{
  "lifecycle": {
    "rule": [
      {
        "action": {
          "type": "SetStorageClass",
          "storageClass": "ARCHIVE"
        },
        "condition": {
          "matchesPrefix": ["bronze/"],
          "age": 60,
          "matchesStorageClass": ["STANDARD"]
        }
      }
    ]
  }
}
'@

$lifecycleFile = "$env:TEMP\gcs_lifecycle.json"
$lifecycleJson | Out-File -FilePath $lifecycleFile -Encoding utf8

gcloud storage buckets update "gs://$BUCKET_NAME" `
    --lifecycle-file=$lifecycleFile

Write-Host "      ✅ Lifecycle rule applied: bronze/ → ARCHIVE after 60 days."

# ── Step 4: Create Service Account ───────────────────────────────────────────
Write-Host "[4/5] Creating service account $SA_NAME ..." -ForegroundColor Yellow

gcloud iam service-accounts create $SA_NAME `
    --display-name="Data Lakehouse SA (Spark + HMS)" `
    --project=$PROJECT_ID

Write-Host "      ✅ Service account created: $SA_EMAIL"

# ── Step 5: Assign Least Privilege IAM Roles ─────────────────────────────────
Write-Host "[5/5] Granting IAM roles to $SA_EMAIL ..." -ForegroundColor Yellow

# roles/storage.objectAdmin → Spark/HMS can read, write, delete objects on GCS
gcloud projects add-iam-policy-binding $PROJECT_ID `
    --member="serviceAccount:$SA_EMAIL" `
    --role="roles/storage.objectAdmin" `
    --condition=None

# roles/logging.logWriter → SA can push logs to Cloud Logging for monitoring
gcloud projects add-iam-policy-binding $PROJECT_ID `
    --member="serviceAccount:$SA_EMAIL" `
    --role="roles/logging.logWriter" `
    --condition=None

Write-Host "      ✅ Roles granted with Least Privilege."
Write-Host ""

# ── Done: Print Handoff Info for Teammate ────────────────────────────────────
Write-Host "=== ✅ Cloud Setup Complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "--- Handoff Info for Teammate (hive-site.xml) ---" -ForegroundColor Cyan
Write-Host "  GCS Warehouse Path  : gs://$BUCKET_NAME/bronze/"
Write-Host "  GCS Project ID      : $PROJECT_ID"
Write-Host "  Service Account     : $SA_EMAIL"
Write-Host "  Auth Method         : Application Default Credentials (gcloud auth)"
Write-Host ""
Write-Host "Next step: Run 'gcloud auth application-default login' on each machine" -ForegroundColor Yellow
Write-Host "           OR mount credentials into Docker containers." -ForegroundColor Yellow
