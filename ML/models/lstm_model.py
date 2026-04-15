import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import LSTM, Dense, Dropout
from sklearn.preprocessing import MinMaxScaler
import joblib
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger("lstm_model")

MODEL_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "models", "saved")
os.makedirs(MODEL_DIR, exist_ok=True)

SEQUENCE_LENGTH = 30 # Dùng 30 nến gần nhất để dự đoán nến tiếp theo

def prepare_lstm_data(df: pd.DataFrame, train_ratio=0.8):
    """Chuẩn bị dữ liệu chuỗi thời gian cho LSTM"""
    log.info("Chuẩn bị dữ liệu cho LSTM (Time Series)...")
    
    # Chỉ dùng giá close để đơn giản hoá việc dự đoán
    data = df[['close']].values
    
    # Chuẩn hoá dữ liệu về khoảng [0, 1]
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled_data = scaler.fit_transform(data)
    
    # Save scaler để dùng lúc inference
    joblib.dump(scaler, os.path.join(MODEL_DIR, 'lstm_scaler.pkl'))
    
    # Tạo sequences
    X, y = [], []
    for i in range(SEQUENCE_LENGTH, len(scaled_data)):
        X.append(scaled_data[i-SEQUENCE_LENGTH:i, 0])
        y.append(scaled_data[i, 0])
        
    X, y = np.array(X), np.array(y)
    
    # Reshape cho LSTM: (samples, time steps, features)
    X = np.reshape(X, (X.shape[0], X.shape[1], 1))
    
    # Split train/test
    train_size = int(len(X) * train_ratio)
    X_train, X_test = X[:train_size], X[train_size:]
    y_train, y_test = y[:train_size], y[train_size:]
    
    return X_train, y_train, X_test, y_test, scaler

def build_and_train_lstm(X_train, y_train, X_test, y_test, epochs=20, batch_size=32):
    log.info("Đang build và train LSTM Model...")
    
    model = Sequential()
    
    # Layer 1
    model.add(LSTM(units=50, return_sequences=True, input_shape=(X_train.shape[1], 1)))
    model.add(Dropout(0.2))
    
    # Layer 2
    model.add(LSTM(units=50, return_sequences=False))
    model.add(Dropout(0.2))
    
    # Output layer (predict continuous exact price)
    model.add(Dense(units=1))
    
    model.compile(optimizer='adam', loss='mean_squared_error')
    
    # Train
    history = model.fit(
        X_train, y_train,
        epochs=epochs,
        batch_size=batch_size,
        validation_data=(X_test, y_test),
        verbose=0
    )
    
    # Save model
    model.save(os.path.join(MODEL_DIR, 'lstm_model.h5'))
    
    # Đánh giá
    loss = model.evaluate(X_test, y_test, verbose=0)
    log.info(f"LSTM Model trained. Test MSE Loss: {loss:.6f}")
    
    return model, history

def predict_lstm(df_latest: pd.DataFrame):
    """Predict giá close tiếp theo"""
    model_path = os.path.join(MODEL_DIR, 'lstm_model.h5')
    scaler_path = os.path.join(MODEL_DIR, 'lstm_scaler.pkl')
    
    if not os.path.exists(model_path) or not os.path.exists(scaler_path):
        return None
        
    model = load_model(model_path)
    scaler = joblib.load(scaler_path)
    
    # Lấy 30 dòng gần nhất
    last_30 = df_latest['close'].values[-SEQUENCE_LENGTH:]
    if len(last_30) < SEQUENCE_LENGTH:
        return None
        
    last_30_scaled = scaler.transform(last_30.reshape(-1, 1))
    
    X_pred = np.reshape(last_30_scaled, (1, SEQUENCE_LENGTH, 1))
    pred_scaled = model.predict(X_pred, verbose=0)
    
    pred_price = scaler.inverse_transform(pred_scaled)[0][0]
    return float(pred_price)
