import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
import joblib
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger("isolation_forest")

MODEL_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "models", "saved")
os.makedirs(MODEL_DIR, exist_ok=True)

# Các features thể hiện sự bất thường (VD: volume tăng vọt, giá dao động cực mạnh)
ANOMALY_FEATURES = ['price_change_pct', 'volatility', 'volume']

def train_isolation_forest(df: pd.DataFrame, contamination=0.05):
    """
    Train Isolation Forest. 
    contamination=0.05 nghĩa là model giả lập ~5% dữ liệu là anomaly (bất thường)
    """
    log.info("Đang train Isolation Forest (Phát hiện dị thường)...")
    
    # Xử lý nếu có NaN
    X = df[ANOMALY_FEATURES].fillna(0)
    
    model = IsolationForest(
        n_estimators=100,
        contamination=contamination,
        random_state=42
    )
    
    model.fit(X)
    
    # Dự đoán trên tập train để xem
    # Output: 1 = Normal, -1 = Anomaly
    predictions = model.predict(X)
    anomaly_count = np.sum(predictions == -1)
    
    log.info(f"Đã phát hiện {anomaly_count} dị thường trong {len(X)} records ({anomaly_count/len(X)*100:.1f}%)")
    
    # Lưu model
    joblib.dump(model, os.path.join(MODEL_DIR, 'isolation_forest.pkl'))
    
    return model

def detect_anomalies(df_latest: pd.DataFrame):
    """Phát hiện dị thường cho dòng dữ liệu mới nhất (Real-time)"""
    model_path = os.path.join(MODEL_DIR, 'isolation_forest.pkl')
    if not os.path.exists(model_path):
        return None
        
    model = joblib.load(model_path)
    
    X = df_latest[ANOMALY_FEATURES].iloc[-1:].fillna(0)
    prediction = model.predict(X)[0]
    score = model.score_samples(X)[0]
    
    # Trả về: is_anomaly (True/False) và anomaly_score
    is_anomaly = True if prediction == -1 else False
    return {"is_anomaly": is_anomaly, "score": float(score)}
