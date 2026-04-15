import pandas as pd
import numpy as np
import xgboost as xgb
import lightgbm as lgb
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import joblib
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger("xgboost_lgbm")

MODEL_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "models", "saved")
os.makedirs(MODEL_DIR, exist_ok=True)

FEATURES = [
    'open', 'high', 'low', 'close', 'volume',
    'price_change_pct', 'volatility', 
    'ma_7', 'ma_20', 'ma_cross_7_20',
    'volume_sma_10', 'volume_spike', 'rsi_14'
]
TARGET = 'target_bullish'

def prepare_train_test(df: pd.DataFrame, train_ratio=0.8):
    """Splits data strictly by time (no random shuffling to avoid data leakage)"""
    train_size = int(len(df) * train_ratio)
    
    X = df[FEATURES]
    y = df[TARGET]
    
    X_train, X_test = X.iloc[:train_size], X.iloc[train_size:]
    y_train, y_test = y.iloc[:train_size], y.iloc[train_size:]
    
    return X_train, X_test, y_train, y_test

def train_xgboost(X_train, y_train, X_test, y_test):
    log.info("Đang train XGBoost Model...")
    
    # Enable early stopping to prevent overfit
    model = xgb.XGBClassifier(
        n_estimators=500,
        learning_rate=0.05,
        max_depth=5,
        subsample=0.8,
        colsample_bytree=0.8,
        use_label_encoder=False,
        eval_metric='logloss',
        random_state=42
    )
    
    model.fit(
        X_train, y_train,
        eval_set=[(X_train, y_train), (X_test, y_test)],
        verbose=False
    )
    
    preds = model.predict(X_test)
    preds_proba = model.predict_proba(X_test)[:, 1]
    
    metrics = {
        'model': 'XGBoost',
        'accuracy': accuracy_score(y_test, preds),
        'precision': precision_score(y_test, preds, zero_division=0),
        'recall': recall_score(y_test, preds, zero_division=0),
        'f1': f1_score(y_test, preds, zero_division=0)
    }
    
    log.info(f"XGBoost Metrics: Accuracy={metrics['accuracy']:.4f}, F1={metrics['f1']:.4f}")
    
    # Save model
    joblib.dump(model, os.path.join(MODEL_DIR, 'xgboost_model.pkl'))
    
    # Extract feature importance
    importance = pd.DataFrame({
        'feature': FEATURES,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    return model, metrics, importance

def train_lightgbm(X_train, y_train, X_test, y_test):
    log.info("Đang train LightGBM Model...")
    
    model = lgb.LGBMClassifier(
        n_estimators=500,
        learning_rate=0.05,
        max_depth=5,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42
    )
    
    model.fit(
        X_train, y_train,
        eval_set=[(X_train, y_train), (X_test, y_test)],
    )
    
    preds = model.predict(X_test)
    
    metrics = {
        'model': 'LightGBM',
        'accuracy': accuracy_score(y_test, preds),
        'precision': precision_score(y_test, preds, zero_division=0),
        'recall': recall_score(y_test, preds, zero_division=0),
        'f1': f1_score(y_test, preds, zero_division=0)
    }
    
    log.info(f"LightGBM Metrics: Accuracy={metrics['accuracy']:.4f}, F1={metrics['f1']:.4f}")
    
    # Save model
    joblib.dump(model, os.path.join(MODEL_DIR, 'lgbm_model.pkl'))
    
    # Extract feature importance
    importance = pd.DataFrame({
        'feature': FEATURES,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    return model, metrics, importance

def predict_latest(df_latest: pd.DataFrame, model_type='xgboost'):
    """Dự đoán cho dòng dữ liệu mới nhất (Real-time inference)"""
    model_path = os.path.join(MODEL_DIR, f"{model_type}_model.pkl")
    if not os.path.exists(model_path):
        return None
        
    model = joblib.load(model_path)
    X = df_latest[FEATURES].iloc[-1:] # Lấy dòng cuối
    
    # Trả về probability của BULLISH
    prob = model.predict_proba(X)[0][1]
    return float(prob)
