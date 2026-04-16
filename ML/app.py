"""
app.py — FastAPI Web Dashboard + SSE (realtime)
==============================================
Chạy: uvicorn app:app --host 0.0.0.0 --port 5000
     (từ thư mục ML) hoặc: python -m uvicorn app:app --host 0.0.0.0 --port 5000
Mở: http://localhost:5000
"""

import sys
import os
import json
import time
import threading
import logging
import subprocess
from contextlib import asynccontextmanager
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from data.fetch_data import fetch_data_from_trino, feature_engineering, generate_mock_data
from models.xgboost_lgbm import predict_latest
from models.lstm_model import predict_lstm, SEQUENCE_LENGTH
from models.isolation_forest import detect_anomalies

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("app")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SAVED_DIR = os.path.join(BASE_DIR, "models", "saved")

# Chu kỳ cập nhật dữ liệu / SSE: 15 phút (giảm tải Trino + Spark)
UPDATE_INTERVAL_SEC = 15 * 60
# Mỗi N chu kỳ (N × 15 phút) refresh Gold layer — mặc định 4 → ~1 giờ/lần
GOLD_REFRESH_EVERY_N_CYCLES = 4

cached_df = None
latest_predictions = {}


def load_training_results():
    path = os.path.join(SAVED_DIR, "training_results.json")
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return {}


def load_feature_importance():
    result = {}
    for name in ["xgb", "lgbm"]:
        path = os.path.join(SAVED_DIR, f"{name}_feature_importance.csv")
        if os.path.exists(path):
            import pandas as pd

            fi = pd.read_csv(path)
            result[name] = fi.to_dict(orient="records")
    return result


def load_lstm_history():
    path = os.path.join(SAVED_DIR, "lstm_history.csv")
    if os.path.exists(path):
        import pandas as pd

        return pd.read_csv(path).to_dict(orient="list")
    return {}


def get_cached_data(symbol="BTCUSDT"):
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
    preds = {
        "timestamp": datetime.utcnow().isoformat(),
        "symbol": df["symbol"].iloc[-1] if "symbol" in df.columns else "BTCUSDT",
        "current_price": float(df["close"].iloc[-1]),
    }

    try:
        xgb_prob = predict_latest(df, model_type="xgboost")
        preds["xgboost_bull_prob"] = round(xgb_prob, 4) if xgb_prob is not None else None
        preds["xgboost_signal"] = (
            "BULLISH"
            if (xgb_prob is not None and xgb_prob > 0.5)
            else ("BEARISH" if xgb_prob is not None else "N/A")
        )
    except Exception:
        preds["xgboost_bull_prob"] = None
        preds["xgboost_signal"] = "N/A"

    try:
        lgbm_prob = predict_latest(df, model_type="lgbm")
        preds["lgbm_bull_prob"] = round(lgbm_prob, 4) if lgbm_prob is not None else None
        preds["lgbm_signal"] = (
            "BULLISH"
            if (lgbm_prob is not None and lgbm_prob > 0.5)
            else ("BEARISH" if lgbm_prob is not None else "N/A")
        )
    except Exception:
        preds["lgbm_bull_prob"] = None
        preds["lgbm_signal"] = "N/A"

    try:
        lstm_price = predict_lstm(df)
        preds["lstm_predicted_price"] = round(lstm_price, 2) if lstm_price is not None else None
        if lstm_price is not None and preds["current_price"]:
            change = (lstm_price - preds["current_price"]) / preds["current_price"] * 100
            preds["lstm_change_pct"] = round(change, 4)
        else:
            preds["lstm_change_pct"] = None
    except Exception:
        preds["lstm_predicted_price"] = None
        preds["lstm_change_pct"] = None

    try:
        anomaly = detect_anomalies(df)
        preds["is_anomaly"] = anomaly["is_anomaly"] if anomaly else False
        preds["anomaly_score"] = round(anomaly["score"], 4) if anomaly else None
    except Exception:
        preds["is_anomaly"] = False
        preds["anomaly_score"] = None

    preds["lstm_ready"] = os.path.exists(
        os.path.join(SAVED_DIR, "lstm_model.h5")
    ) and os.path.exists(os.path.join(SAVED_DIR, "lstm_scaler.pkl"))
    preds["training_metrics_ready"] = os.path.exists(
        os.path.join(SAVED_DIR, "training_results.json")
    )

    return preds


def refresh_gold_layer():
    cmd = [
        "docker",
        "exec",
        "-i",
        "spark-master",
        "spark-submit",
        "--master",
        "spark://spark-master:7077",
        "--deploy-mode",
        "client",
        "--driver-memory",
        "512m",
        "--executor-memory",
        "512m",
        "--packages",
        "io.delta:delta-spark_2.12:3.2.1",
        "--conf",
        "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf",
        "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "--conf",
        "spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore",
        "/processing/silver_to_gold.py",
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


def _sse_fetch_and_predict():
    """Cập nhật cached_df + latest_predictions (dùng cho SSE và lần đầu khởi động)."""
    global cached_df, latest_predictions
    raw = fetch_data_from_trino("BTCUSDT", limit=5000)
    cached_df = feature_engineering(raw)
    latest_predictions = run_predictions(cached_df)


def sse_worker():
    global cached_df, latest_predictions
    cycle_count = 0

    try:
        _sse_fetch_and_predict()
        log.info(
            f"[SSE] Khởi tạo: price={latest_predictions.get('current_price')}, "
            f"lstm={latest_predictions.get('lstm_predicted_price')}"
        )
    except Exception as e:
        log.warning(f"[SSE] Lần fetch đầu lỗi, dùng mock: {e}")
        try:
            raw = generate_mock_data("BTCUSDT", 500)
            cached_df = feature_engineering(raw)
            latest_predictions = run_predictions(cached_df)
        except Exception as e2:
            log.warning(f"[SSE] Mock cũng lỗi: {e2}")

    threading.Thread(target=refresh_gold_layer, daemon=True).start()

    while True:
        time.sleep(UPDATE_INTERVAL_SEC)
        cycle_count += 1
        try:
            if cycle_count % GOLD_REFRESH_EVERY_N_CYCLES == 0:
                refresh_gold_layer()
            _sse_fetch_and_predict()
            log.info(
                f"[SSE] Updated: price={latest_predictions.get('current_price')}, "
                f"xgb={latest_predictions.get('xgboost_signal')}, "
                f"lstm={latest_predictions.get('lstm_predicted_price')}"
            )
        except Exception as e:
            log.warning(f"[SSE] Error: {e}")


def _start_worker():
    t = threading.Thread(target=sse_worker, daemon=True)
    t.start()
    log.info(f"Background worker started (interval={UPDATE_INTERVAL_SEC}s)")


@asynccontextmanager
async def lifespan(app: FastAPI):
    _start_worker()
    yield


app = FastAPI(title="Crypto ML Dashboard", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


@app.get("/")
async def index(request: Request):
    # Starlette: tham số đầu phải là request, không phải tên file
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"request": request},
    )


@app.get("/api/predictions")
async def api_predictions():
    global latest_predictions
    if not latest_predictions:
        df = get_cached_data()
        latest_predictions = run_predictions(df)
    return JSONResponse(latest_predictions)


@app.get("/api/training-results")
async def api_training_results():
    return JSONResponse(load_training_results())


@app.get("/api/feature-importance")
async def api_feature_importance():
    return JSONResponse(load_feature_importance())


@app.get("/api/lstm-history")
async def api_lstm_history():
    return JSONResponse(load_lstm_history())


@app.get("/api/price-history")
async def api_price_history():
    df = get_cached_data()
    if df is None or len(df) == 0:
        return JSONResponse({"times": [], "prices": [], "ma_7": [], "ma_20": []})

    last_n = min(200, len(df))
    subset = df.tail(last_n)

    return JSONResponse(
        {
            "times": subset["candle_time"].astype(str).tolist(),
            "prices": subset["close"].tolist(),
            "ma_7": subset["ma_7"].tolist() if "ma_7" in subset.columns else [],
            "ma_20": subset["ma_20"].tolist() if "ma_20" in subset.columns else [],
            "volumes": subset["volume"].tolist(),
        }
    )


@app.get("/api/anomalies")
async def api_anomalies():
    from models.isolation_forest import fit_predict_all

    df = get_cached_data()
    if df is None or len(df) == 0:
        return JSONResponse(
            {
                "anomalies": [],
                "total_anomalies": 0,
                "total_records": 0,
                "all_scores": [],
                "all_times": [],
                "all_labels": [],
            }
        )

    preds, scores = fit_predict_all(df)
    if preds is None or scores is None:
        return JSONResponse(
            {
                "anomalies": [],
                "total_anomalies": 0,
                "total_records": len(df),
                "all_scores": [],
                "all_times": [],
                "all_labels": [],
            }
        )

    df_copy = df.copy()
    df_copy["anomaly"] = preds == -1
    df_copy["anomaly_score"] = scores

    anomaly_rows = df_copy[df_copy["anomaly"]].tail(50)

    return JSONResponse(
        {
            "anomalies": anomaly_rows[
                ["candle_time", "close", "volume", "price_change_pct", "anomaly_score"]
            ]
            .assign(candle_time=lambda x: x["candle_time"].astype(str))
            .to_dict(orient="records"),
            "total_anomalies": int(df_copy["anomaly"].sum()),
            "total_records": len(df_copy),
            "all_scores": scores.tolist()[-200:],
            "all_times": df["candle_time"].astype(str).tolist()[-200:],
            "all_labels": preds.tolist()[-200:],
        }
    )


@app.get("/api/lstm-predictions")
async def api_lstm_predictions():
    import numpy as np
    from tensorflow.keras.models import load_model
    import joblib

    model_path = os.path.join(SAVED_DIR, "lstm_model.h5")
    scaler_path = os.path.join(SAVED_DIR, "lstm_scaler.pkl")
    if not os.path.exists(model_path) or not os.path.exists(scaler_path):
        return JSONResponse({"actual": [], "predicted": [], "times": []})

    model = load_model(model_path)
    scaler = joblib.load(scaler_path)

    df = get_cached_data()
    data = df["close"].values
    scaled = scaler.transform(data.reshape(-1, 1))

    X_all, actuals = [], []
    for i in range(SEQUENCE_LENGTH, len(scaled)):
        X_all.append(scaled[i - SEQUENCE_LENGTH : i, 0])
        actuals.append(data[i])
    X_all = np.array(X_all).reshape(-1, SEQUENCE_LENGTH, 1)

    preds_scaled = model.predict(X_all, verbose=0)
    preds = scaler.inverse_transform(preds_scaled).flatten().tolist()

    last_n = min(100, len(preds))
    times = df["candle_time"].astype(str).tolist()[SEQUENCE_LENGTH:]

    return JSONResponse(
        {
            "actual": actuals[-last_n:],
            "predicted": preds[-last_n:],
            "times": times[-last_n:],
        }
    )


def _sse_event_stream():
    """SSE: đẩy bản snapshot mới nhất theo cùng chu kỳ UPDATE_INTERVAL_SEC."""
    while True:
        if latest_predictions:
            yield f"data: {json.dumps(latest_predictions)}\n\n"
        time.sleep(UPDATE_INTERVAL_SEC)


@app.get("/stream")
async def stream():
    return StreamingResponse(
        _sse_event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


if __name__ == "__main__":
    import uvicorn

    log.info("🚀 Starting ML Dashboard (FastAPI) at http://localhost:5000")
    uvicorn.run(app, host="0.0.0.0", port=5000, reload=False)
