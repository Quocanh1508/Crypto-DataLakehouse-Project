import pandas as pd
import numpy as np
import trino
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger("fetch_data")

# Cấu hình Trino
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "18080"))
TRINO_USER = os.getenv("TRINO_USER", "admin")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "delta_gcs") # Dùng delta_gcs catalog từ docker-compose
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "gcs")

CACHE_FILE = os.path.join(os.path.dirname(__file__), "cached_ohlcv.csv")

def calculate_rsi(data: pd.Series, periods: int = 14) -> pd.Series:
    """Tính toán Relative Strength Index (RSI)"""
    delta = data.diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    
    # EMA (Exponential Moving Average) cho RSI
    roll_up = up.ewm(com=(periods - 1), adjust=False).mean()
    roll_down = down.ewm(com=(periods - 1), adjust=False).mean()
    
    rs = roll_up / roll_down
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi

def fetch_data_from_trino(symbol: str = "BTCUSDT", limit: int = 10000) -> pd.DataFrame:
    """Fetch dữ liệu từ Trino hoặc dùng Cache nếu Trino không chạy"""
    log.info(f"Đang fetch dữ liệu cho {symbol} từ Trino...")
    
    try:
        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=TRINO_SCHEMA
        )
        # Query bảng gold_ohlcv từ GCS
        # Sắp xếp ascending để tính toán chuỗi thời gian đúng
        query = f"""
            SELECT * FROM gold_ohlcv 
            WHERE symbol = '{symbol}' AND candle_duration = '5 minutes'
            ORDER BY candle_time ASC
            LIMIT {limit}
        """
        df = pd.read_sql(query, conn)
        
        if df.empty:
            raise ValueError("Dataframe trống. Có thể bảng chưa có dữ liệu.")
            
        # Lưu cache
        df.to_csv(CACHE_FILE, index=False)
        log.info(f"Fetch thành công {len(df)} dòng. Đã lưu cache.")
        return df
        
    except Exception as e:
        log.warning(f"Lỗi kết nối Trino: {e}")
        log.info("Thử load dữ liệu từ file cache...")
        if os.path.exists(CACHE_FILE):
            df = pd.read_csv(CACHE_FILE)
            # Parse datetime
            df['candle_time'] = pd.to_datetime(df['candle_time'])
            # Filter symbol
            df = df[df['symbol'] == symbol].copy()
            log.info(f"Đã load {len(df)} dòng từ file cache.")
            return df
        else:
            log.error("Không có file cache. Tạo dữ liệu giả lập (mock data) để test...")
            return generate_mock_data(symbol, limit)

def feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    """Tạo features cho ML Models"""
    log.info("Tiến hành feature engineering...")
    df = df.copy()
    
    # Đảm bảo sắp xếp theo thời gian
    df = df.sort_values(by='candle_time').reset_index(drop=True)
    
    # 1. Price Change %
    df['price_change_pct'] = df['close'].pct_change()
    
    # 2. Volatility
    df['volatility'] = (df['high'] - df['low']) / df['close']
    
    # 3. Moving Average Signals (nếu ma_7, ma_20 chưa có hoặc bị NULL, tính lại)
    if 'ma_7' not in df.columns or df['ma_7'].isnull().all():
        df['ma_7'] = df['close'].rolling(window=7).mean()
    if 'ma_20' not in df.columns or df['ma_20'].isnull().all():
        df['ma_20'] = df['close'].rolling(window=20).mean()
        
    df['ma_cross_7_20'] = (df['ma_7'] > df['ma_20']).astype(int) # 1 if Golden Cross, else 0
    
    # 4. Volume SMA
    df['volume_sma_10'] = df['volume'].rolling(window=10).mean()
    df['volume_spike'] = (df['volume'] > df['volume_sma_10'] * 2).astype(int)
    
    # 5. RSI
    df['rsi_14'] = calculate_rsi(df['close'], periods=14)
    
    # 6. Target Label (Classification: Next candle close > current close?)
    df['next_close'] = df['close'].shift(-1)
    df['target_bullish'] = (df['next_close'] > df['close']).astype(int)
    
    # Bỏ các dòng NaN (do shift, rolling)
    df = df.dropna().reset_index(drop=True)
    
    log.info(f"Features created: {len(df.columns)}. Rows remaining: {len(df)}")
    return df

def generate_mock_data(symbol: str, length: int) -> pd.DataFrame:
    """Tạo dữ liệu OHLCV giả lập nếu Trino không kết nối được"""
    # ... mock logic
    dates = pd.date_range(end=pd.Timestamp.utcnow(), periods=length, freq='5min')
    base_price = 84000.0 if "BTC" in symbol else 3000.0
    
    prices = [base_price]
    for _ in range(1, length):
        change = np.random.normal(0, 0.002)
        prices.append(prices[-1] * (1 + change))
        
    df = pd.DataFrame({
        'symbol': [symbol] * length,
        'candle_time': dates,
        'open': prices,
        'close': np.roll(prices, -1),
        'volume': np.random.uniform(10, 100, length)
    })
    df['close'] = df['open'] * (1 + np.random.normal(0, 0.001, length))
    df['high'] = df[['open', 'close']].max(axis=1) * (1 + np.random.uniform(0, 0.001, length))
    df['low'] = df[['open', 'close']].min(axis=1) * (1 - np.random.uniform(0, 0.001, length))
    df['tick_count'] = np.random.randint(50, 500, length)
    
    return df

if __name__ == "__main__":
    # Test script
    raw = fetch_data_from_trino("BTCUSDT", limit=100)
    processed = feature_engineering(raw)
    print(processed[['candle_time', 'close', 'rsi_14', 'target_bullish']].head())
