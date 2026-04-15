"""
app.py — Flask Web Dashboard + SSE Real-time
=============================================
Chạy: python ML/app.py
Mở: http://localhost:5000
"""

import sys, os, json, time, threading, logging, subprocess
from datetime import datetime

# Đảm bảo import được các module ML
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, render_template, jsonify, Response
from flask_cors import CORS

from data.fetch_data import fetch_data_from_trino, feature_engineering, generate_mock_data
from models.xgboost_lgbm import predict_latest, FEATURES
from models.lstm_model import predict_lstm, SEQUENCE_LENGTH
from models.isolation_forest import detect_anomalies

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger("app")

app = Flask(__name__)
CORS(app)

# ── Global state ─────────────────────────────────────────────────
SAVED_DIR = os.path.join(os.path.dirname(__file__), "models", "saved")
cached_df = None          # DataFrame đã qua feature engineering
latest_predictions = {}   # Cache kết quả prediction mới nhất
sse_clients = []          # Danh sách SSE client đang kết nối


def load_training_results():
    """Load kết quả training từ file JSON"""
    path = os.path.join(SAVED_DIR, "training_results.json")
    if os.path.exists(path):
        with open(path, 'r') as f:
            return json.load(f)
    return {}


def load_feature_importance():
    """Load feature importance từ CSV"""
    result = {}
    for name in ['xgb', 'lgbm']:
        path = os.path.join(SAVED_DIR, f'{name}_feature_importance.csv')
        if os.path.exists(path):
            import pandas as pd
            fi = pd.read_csv(path)
            result[name] = fi.to_dict(orient='records')
    return result


def load_lstm_history():
    """Load LSTM training history"""
    path = os.path.join(SAVED_DIR, 'lstm_history.csv')
    if os.path.exists(path):
        import pandas as pd
        return pd.read_csv(path).to_dict(orient='list')
    return {}


def get_cached_data(symbol="BTCUSDT"):
    """Lấy hoặc refresh cached data"""
    global cached_df
    if cached_df is None or len(cached_df) == 0:
        try:
            raw = fetch_data_from_trino(symbol, limit=5000)
            cached_df = feature_engineering(raw)
        except Exception as e:
            log.warning(f"Fetch thất bại, dùng mock: {e}")
            raw = generate_mock_data(symbol, 500)
            cached_df = feature_engineering(raw)
    return cached_df


def run_predictions(df):
    """Chạy inference cho tất cả 3 models"""
    preds = {
        "timestamp": datetime.utcnow().isoformat(),
        "symbol": df['symbol'].iloc[-1] if 'symbol' in df.columns else "BTCUSDT",
        "current_price": float(df['close'].iloc[-1]),
    }

    # 1. XGBoost
    try:
        xgb_prob = predict_latest(df, model_type='xgboost')
        preds['xgboost_bull_prob'] = round(xgb_prob, 4) if xgb_prob is not None else None
        preds['xgboost_signal'] = 'BULLISH' if (xgb_prob is not None and xgb_prob > 0.5) else ('BEARISH' if xgb_prob is not None else 'N/A')
    except Exception:
        preds['xgboost_bull_prob'] = None
        preds['xgboost_signal'] = 'N/A'

    # 2. LightGBM
    try:
        lgbm_prob = predict_latest(df, model_type='lgbm')
        preds['lgbm_bull_prob'] = round(lgbm_prob, 4) if lgbm_prob is not None else None
        preds['lgbm_signal'] = 'BULLISH' if (lgbm_prob is not None and lgbm_prob > 0.5) else ('BEARISH' if lgbm_prob is not None else 'N/A')
    except Exception:
        preds['lgbm_bull_prob'] = None
        preds['lgbm_signal'] = 'N/A'

    # 3. LSTM
    try:
        lstm_price = predict_lstm(df)
        preds['lstm_predicted_price'] = round(lstm_price, 2) if lstm_price is not None else None
        if lstm_price is not None and preds['current_price']:
            change = (lstm_price - preds['current_price']) / preds['current_price'] * 100
            preds['lstm_change_pct'] = round(change, 4)
        else:
            preds['lstm_change_pct'] = None
    except Exception:
        preds['lstm_predicted_price'] = None
        preds['lstm_change_pct'] = None

    # 4. Isolation Forest
    try:
        anomaly = detect_anomalies(df)
        preds['is_anomaly'] = anomaly['is_anomaly'] if anomaly else False
        preds['anomaly_score'] = round(anomaly['score'], 4) if anomaly else None
    except Exception as e:
        preds['is_anomaly'] = False
        preds['anomaly_score'] = None

    return preds


# ── Gold Layer Auto-Refresh ───────────────────────────────────────
def refresh_gold_layer():
    """
    Trigger silver_to_gold.py qua docker exec spark-master.
    Khi trưởng nhóm đang stream data mới vào Kafka → Bronze → Silver,
    hàm này đẩy data từ Silver → Gold để ML dashboard luôn có data mới nhất.
    """
    cmd = [
        "docker", "exec", "-i", "spark-master", "spark-submit",
        "--master", "spark://spark-master:7077",
        "--deploy-mode", "client",
        "--driver-memory", "512m",
        "--executor-memory", "512m",
        "--packages", "io.delta:delta-spark_2.12:3.2.1",
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "--conf", "spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore",
        "/processing/silver_to_gold.py"
    ]
    try:
        log.info("[GOLD REFRESH] 🔄 Running silver_to_gold.py via Spark...")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode == 0:
            log.info("[GOLD REFRESH] ✅ Gold layer refreshed successfully!")
        else:
            log.warning(f"[GOLD REFRESH] ⚠️ Exit code {result.returncode}: {result.stderr[-200:]}")
    except subprocess.TimeoutExpired:
        log.warning("[GOLD REFRESH] ⏱️ Timeout after 5 min, skipping")
    except FileNotFoundError:
        log.warning("[GOLD REFRESH] Docker not found, skipping Gold refresh")
    except Exception as e:
        log.warning(f"[GOLD REFRESH] Error: {e}")


# ── SSE Background Thread ────────────────────────────────────────
GOLD_REFRESH_INTERVAL = 10  # Mỗi 10 cycles (10×30s = 5 phút) refresh Gold 1 lần

def sse_worker():
    """Background thread: mỗi 30s fetch data mới + chạy prediction.
    Mỗi 5 phút tự động chạy silver_to_gold.py để cập nhật Gold layer."""
    global cached_df, latest_predictions
    cycle_count = 0

    # Refresh Gold ngay lần đầu tiên khi khởi động
    refresh_gold_layer()

    while True:
        try:
            # Mỗi 5 phút: refresh Gold layer (Silver → Gold)
            if cycle_count > 0 and cycle_count % GOLD_REFRESH_INTERVAL == 0:
                refresh_gold_layer()

            # Fetch data mới từ Trino (Gold table)
            raw = fetch_data_from_trino("BTCUSDT", limit=5000)
            cached_df = feature_engineering(raw)

            # Chạy predictions với data mới
            latest_predictions = run_predictions(cached_df)
            log.info(f"[SSE] Updated: price={latest_predictions.get('current_price')}, "
                     f"xgb={latest_predictions.get('xgboost_signal')}, "
                     f"lstm={latest_predictions.get('lstm_predicted_price')}")

        except Exception as e:
            log.warning(f"[SSE] Error: {e}")

        cycle_count += 1
        time.sleep(30)


# ── Routes ────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/predictions")
def api_predictions():
    """REST endpoint: trả về predictions mới nhất"""
    global cached_df, latest_predictions
    if not latest_predictions:
        df = get_cached_data()
        latest_predictions = run_predictions(df)
    return jsonify(latest_predictions)


@app.route("/api/training-results")
def api_training_results():
    """Trả về kết quả training (accuracy, F1, etc.)"""
    return jsonify(load_training_results())


@app.route("/api/feature-importance")
def api_feature_importance():
    """Trả về feature importance cho XGBoost và LightGBM"""
    return jsonify(load_feature_importance())


@app.route("/api/lstm-history")
def api_lstm_history():
    """Trả về LSTM training loss history"""
    return jsonify(load_lstm_history())


@app.route("/api/price-history")
def api_price_history():
    """Trả về giá close gần nhất cho chart"""
    df = get_cached_data()
    if df is None or len(df) == 0:
        return jsonify({"times": [], "prices": [], "ma_7": [], "ma_20": []})

    last_n = min(200, len(df))
    subset = df.tail(last_n)

    return jsonify({
        "times": subset['candle_time'].astype(str).tolist(),
        "prices": subset['close'].tolist(),
        "ma_7": subset['ma_7'].tolist() if 'ma_7' in subset.columns else [],
        "ma_20": subset['ma_20'].tolist() if 'ma_20' in subset.columns else [],
        "volumes": subset['volume'].tolist(),
    })


@app.route("/api/anomalies")
def api_anomalies():
    """Trả về danh sách anomalies"""
    import joblib
    df = get_cached_data()
    model_path = os.path.join(SAVED_DIR, 'isolation_forest.pkl')
    if not os.path.exists(model_path) or df is None:
        return jsonify({"anomalies": []})

    model = joblib.load(model_path)
    features = ['price_change_pct', 'volatility', 'volume']
    X = df[features].fillna(0)
    preds = model.predict(X)
    scores = model.score_samples(X)

    df_copy = df.copy()
    df_copy['anomaly'] = (preds == -1)
    df_copy['anomaly_score'] = scores

    anomaly_rows = df_copy[df_copy['anomaly']].tail(50)

    return jsonify({
        "anomalies": anomaly_rows[['candle_time', 'close', 'volume', 'price_change_pct', 'anomaly_score']]
            .assign(candle_time=lambda x: x['candle_time'].astype(str))
            .to_dict(orient='records'),
        "total_anomalies": int(df_copy['anomaly'].sum()),
        "total_records": len(df_copy),
        "all_scores": scores.tolist()[-200:],
        "all_times": df['candle_time'].astype(str).tolist()[-200:],
        "all_labels": preds.tolist()[-200:],
    })


@app.route("/api/lstm-predictions")
def api_lstm_predictions():
    """Trả về LSTM actual vs predicted cho chart"""
    import numpy as np
    from tensorflow.keras.models import load_model
    import joblib

    model_path = os.path.join(SAVED_DIR, 'lstm_model.h5')
    scaler_path = os.path.join(SAVED_DIR, 'lstm_scaler.pkl')
    if not os.path.exists(model_path) or not os.path.exists(scaler_path):
        return jsonify({"actual": [], "predicted": [], "times": []})

    model = load_model(model_path)
    scaler = joblib.load(scaler_path)

    df = get_cached_data()
    data = df['close'].values
    scaled = scaler.transform(data.reshape(-1, 1))

    X_all, actuals = [], []
    for i in range(SEQUENCE_LENGTH, len(scaled)):
        X_all.append(scaled[i - SEQUENCE_LENGTH:i, 0])
        actuals.append(data[i])
    X_all = np.array(X_all).reshape(-1, SEQUENCE_LENGTH, 1)

    preds_scaled = model.predict(X_all, verbose=0)
    preds = scaler.inverse_transform(preds_scaled).flatten().tolist()

    last_n = min(100, len(preds))
    times = df['candle_time'].astype(str).tolist()[SEQUENCE_LENGTH:]

    return jsonify({
        "actual": actuals[-last_n:],
        "predicted": preds[-last_n:],
        "times": times[-last_n:],
    })


@app.route("/stream")
def stream():
    """SSE endpoint — push real-time prediction mỗi 30s"""
    def event_stream():
        while True:
            if latest_predictions:
                yield f"data: {json.dumps(latest_predictions)}\n\n"
            time.sleep(30)
    return Response(event_stream(), mimetype="text/event-stream")


# ── Startup ───────────────────────────────────────────────────────
if __name__ == "__main__":
    # Khởi động SSE worker thread
    t = threading.Thread(target=sse_worker, daemon=True)
    t.start()
    log.info("🚀 Starting ML Dashboard at http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
