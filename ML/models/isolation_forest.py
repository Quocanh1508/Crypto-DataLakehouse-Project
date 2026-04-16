import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
import joblib
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("isolation_forest")

MODEL_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "models", "saved")
os.makedirs(MODEL_DIR, exist_ok=True)
MODEL_PATH = os.path.join(MODEL_DIR, "isolation_forest.pkl")

# Các features thể hiện sự bất thường (VD: volume tăng vọt, giá dao động cực mạnh)
ANOMALY_FEATURES = ["price_change_pct", "volatility", "volume"]


def _get_or_fit_model(X: pd.DataFrame):
    """Load model đã train hoặc fit tạm trên X rồi lưu (để dashboard có dữ liệu ngay cả khi chưa chạy train_all)."""
    if os.path.exists(MODEL_PATH):
        return joblib.load(MODEL_PATH)
    model = IsolationForest(
        n_estimators=100,
        contamination=0.05,
        random_state=42,
    )
    model.fit(X)
    try:
        joblib.dump(model, MODEL_PATH)
        log.info("Đã lưu isolation_forest.pkl sau fit tự động.")
    except Exception as e:
        log.warning("Không ghi được isolation_forest.pkl: %s", e)
    return model


def fit_predict_all(df: pd.DataFrame):
    """
    Trả về (preds, scores) cho từng dòng; preds: 1 = normal, -1 = anomaly.
    None nếu không đủ dữ liệu.
    """
    if df is None or len(df) == 0:
        return None, None
    X = df[ANOMALY_FEATURES].fillna(0)
    if len(X) < 2:
        return None, None
    model = _get_or_fit_model(X)
    preds = model.predict(X)
    scores = model.score_samples(X)
    return preds, scores


def train_isolation_forest(df: pd.DataFrame, contamination=0.05):
    """
    Train Isolation Forest.
    contamination=0.05 nghĩa là model giả lập ~5% dữ liệu là anomaly (bất thường)
    """
    log.info("Đang train Isolation Forest (Phát hiện dị thường)...")

    X = df[ANOMALY_FEATURES].fillna(0)

    model = IsolationForest(
        n_estimators=100,
        contamination=contamination,
        random_state=42,
    )

    model.fit(X)

    predictions = model.predict(X)
    anomaly_count = np.sum(predictions == -1)

    log.info(
        f"Đã phát hiện {anomaly_count} dị thường trong {len(X)} records ({anomaly_count/len(X)*100:.1f}%)"
    )

    joblib.dump(model, MODEL_PATH)

    return model


def detect_anomalies(df_latest: pd.DataFrame):
    """Phát hiện dị thường cho dòng dữ liệu mới nhất (Real-time)."""
    preds, scores = fit_predict_all(df_latest)
    if preds is None or scores is None:
        return None
    i = len(preds) - 1
    return {
        "is_anomaly": bool(preds[i] == -1),
        "score": float(scores[i]),
    }
