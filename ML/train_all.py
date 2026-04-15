"""
train_all.py
============
One-click script to train all 3 ML models.
Usage: python ML/train_all.py
"""

import sys
import os
import json
import logging
import time
import pandas as pd

# Thêm thư mục ML vào path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data.fetch_data import fetch_data_from_trino, feature_engineering
from models.xgboost_lgbm import prepare_train_test, train_xgboost, train_lightgbm, FEATURES
from models.lstm_model import prepare_lstm_data, build_and_train_lstm
from models.isolation_forest import train_isolation_forest

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger("train_all")

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "models", "saved")
os.makedirs(RESULTS_DIR, exist_ok=True)


def main():
    start = time.time()
    log.info("=" * 60)
    log.info("  CRYPTO ML PIPELINE — TRAIN ALL MODELS")
    log.info("=" * 60)

    # ── 1. Fetch data ────────────────────────────────────────────
    log.info("\n📡 STEP 1: Fetching data from Trino...")
    raw_df = fetch_data_from_trino("BTCUSDT", limit=10000)
    log.info(f"   Raw data: {len(raw_df)} rows")

    # ── 2. Feature engineering ───────────────────────────────────
    log.info("\n⚙️  STEP 2: Feature engineering...")
    df = feature_engineering(raw_df)
    log.info(f"   Processed data: {len(df)} rows, {len(df.columns)} columns")

    results = {}

    # ── 3. XGBoost ───────────────────────────────────────────────
    log.info("\n🌲 STEP 3: Training XGBoost...")
    X_train, X_test, y_train, y_test = prepare_train_test(df)
    xgb_model, xgb_metrics, xgb_importance = train_xgboost(
        X_train, y_train, X_test, y_test
    )
    results['xgboost'] = xgb_metrics
    log.info(f"   ✅ XGBoost: Accuracy={xgb_metrics['accuracy']:.4f}, F1={xgb_metrics['f1']:.4f}")

    # ── 4. LightGBM ─────────────────────────────────────────────
    log.info("\n🌿 STEP 4: Training LightGBM...")
    lgbm_model, lgbm_metrics, lgbm_importance = train_lightgbm(
        X_train, y_train, X_test, y_test
    )
    results['lightgbm'] = lgbm_metrics
    log.info(f"   ✅ LightGBM: Accuracy={lgbm_metrics['accuracy']:.4f}, F1={lgbm_metrics['f1']:.4f}")

    # ── 5. LSTM ──────────────────────────────────────────────────
    log.info("\n🧠 STEP 5: Training LSTM...")
    X_tr_lstm, y_tr_lstm, X_te_lstm, y_te_lstm, scaler = prepare_lstm_data(df)
    lstm_model, lstm_history = build_and_train_lstm(
        X_tr_lstm, y_tr_lstm, X_te_lstm, y_te_lstm,
        epochs=20, batch_size=32
    )
    lstm_val_loss = float(lstm_history.history['val_loss'][-1])
    results['lstm'] = {
        'model': 'LSTM',
        'val_mse': lstm_val_loss,
        'val_rmse': float(lstm_val_loss ** 0.5),
        'epochs': 20
    }
    log.info(f"   ✅ LSTM: Val MSE={lstm_val_loss:.6f}")

    # ── 6. Isolation Forest ──────────────────────────────────────
    log.info("\n🔍 STEP 6: Training Isolation Forest...")
    iso_model = train_isolation_forest(df, contamination=0.05)
    results['isolation_forest'] = {
        'model': 'Isolation Forest',
        'contamination': 0.05,
        'n_estimators': 100
    }
    log.info("   ✅ Isolation Forest trained")

    # ── 7. Save training results ─────────────────────────────────
    # Save feature importances
    xgb_importance.to_csv(os.path.join(RESULTS_DIR, 'xgb_feature_importance.csv'), index=False)
    lgbm_importance.to_csv(os.path.join(RESULTS_DIR, 'lgbm_feature_importance.csv'), index=False)

    # Save overall metrics
    with open(os.path.join(RESULTS_DIR, 'training_results.json'), 'w') as f:
        json.dump(results, f, indent=2)

    # Save LSTM history
    hist_df = pd.DataFrame(lstm_history.history)
    hist_df.to_csv(os.path.join(RESULTS_DIR, 'lstm_history.csv'), index=False)

    # ── Summary ──────────────────────────────────────────────────
    elapsed = time.time() - start
    log.info("\n" + "=" * 60)
    log.info("  TRAINING COMPLETE!")
    log.info(f"  Time elapsed: {elapsed:.1f}s")
    log.info("=" * 60)
    log.info("")
    log.info("  Model              | Metric")
    log.info("  -------------------|------------------")
    log.info(f"  XGBoost            | Acc={xgb_metrics['accuracy']:.4f}  F1={xgb_metrics['f1']:.4f}")
    log.info(f"  LightGBM           | Acc={lgbm_metrics['accuracy']:.4f}  F1={lgbm_metrics['f1']:.4f}")
    log.info(f"  LSTM               | Val MSE={lstm_val_loss:.6f}")
    log.info(f"  Isolation Forest   | contamination=5%")
    log.info("")
    log.info(f"  Models saved to: {RESULTS_DIR}")
    log.info("  Run 'python ML/app.py' to start the dashboard!")


if __name__ == "__main__":
    main()
