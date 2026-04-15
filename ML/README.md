# 🤖 Crypto ML Dashboard

Hệ thống Machine Learning phân tích và dự đoán giá tiền điện tử **real-time**, đọc dữ liệu OHLCV từ **Trino** (Gold Layer).

## Kiến trúc

```
Gold (Delta Lake/GCS) → Trino → ML Pipeline → Flask Dashboard (SSE Real-time)
```

## 3 Models

| Model | Tác vụ | Output |
|-------|--------|--------|
| **XGBoost + LightGBM** | Classification (BULL/BEAR) | Xác suất nến tiếp theo tăng/giảm |
| **LSTM** | Regression (Price Prediction) | Dự đoán giá close nến tiếp theo |
| **Isolation Forest** | Anomaly Detection | Phát hiện nến bất thường (pump/dump) |

## Cài đặt

```bash
# 1. Tạo virtual environment
python -m venv .venv-ml
.venv-ml\Scripts\activate    # Windows

# 2. Cài dependencies
pip install -r ML/requirements.txt

# 3. Train models (cần Trino đang chạy, hoặc dùng mock data)
python ML/train_all.py

# 4. Chạy dashboard
python ML/app.py
# Mở http://localhost:5000
```

## Cấu trúc thư mục

```
ML/
├── data/
│   └── fetch_data.py           # Kết nối Trino + feature engineering
├── models/
│   ├── xgboost_lgbm.py         # XGBoost & LightGBM classification
│   ├── lstm_model.py           # LSTM price prediction
│   ├── isolation_forest.py     # Anomaly detection
│   └── saved/                  # Trained model files (auto-generated)
├── templates/
│   └── index.html              # Dashboard UI
├── static/
│   ├── css/style.css           # Dark theme
│   └── js/app.js               # Chart.js + SSE client
├── app.py                      # Flask server + REST API + SSE
├── train_all.py                # One-click training script
├── requirements.txt
└── README.md
```

## API Endpoints

| Endpoint | Mô tả |
|----------|--------|
| `GET /` | Dashboard UI |
| `GET /api/predictions` | Predictions mới nhất (tất cả models) |
| `GET /api/training-results` | Kết quả training (accuracy, F1, MSE) |
| `GET /api/feature-importance` | Feature importance (XGBoost & LightGBM) |
| `GET /api/lstm-predictions` | LSTM actual vs predicted |
| `GET /api/lstm-history` | LSTM training loss history |
| `GET /api/price-history` | Giá close + MA7/MA20 (200 nến gần nhất) |
| `GET /api/anomalies` | Danh sách anomalies + scores |
| `GET /stream` | SSE real-time (push mỗi 30s) |

## Yêu cầu

- Python 3.10+
- Trino đang chạy (hoặc dùng mock data tự động)
- Docker containers: `trino`, `spark-master` (cho data pipeline)
