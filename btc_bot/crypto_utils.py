from binance.client import Client
import os
import requests
import pandas as pd
import pandas_ta as ta

class CryptoDataClient:
    def __init__(self, use_binance=True):
        self.use_binance = use_binance
        if use_binance:
            self.client = Client(
                os.getenv("BINANCE_API_KEY"),
                os.getenv("BINANCE_API_SECRET")
            )

    def detect_green_candle_with_rising_volume(self, symbol="BTCUSDT", interval="1h", limit=3):
        """
        מזהה נר ירוק עם ווליום עולה - ניתוח של 2 נרות אחרונים
        """
        df = self.get_price_history(symbol, interval, limit)

        if len(df) < 3:
            return "Not enough data", 0

        # נר קודם
        prev = df.iloc[-2]
        # נר נוכחי
        curr = df.iloc[-1]

        prev_open = float(prev['open'])
        prev_close = float(prev['close'])
        prev_volume = float(prev['volume'])

        curr_open = float(curr['open'])
        curr_close = float(curr['close'])
        curr_volume = float(curr['volume'])

        is_green_candle = curr_close > curr_open
        is_volume_higher = curr_volume > prev_volume
        candle_score = 0
        if is_green_candle and is_volume_higher:
            candle_status = "Green"
            candle_score = 9  # strong bullish
        elif is_green_candle:
            candle_status = "yellow"
            candle_score = 6  # moderate bullish
        elif is_volume_higher:
            candle_status = "red"
            candle_score = 4  # uncertain but active
        else:
            candle_status = "Not Found"
            candle_score = 3  # no bullish pattern
            
        return candle_status, candle_score

    def get_price_history(self, symbol="BTCUSDT", interval="1h", limit=100):
        if self.use_binance:
            klines = self.client.get_klines(symbol=symbol, interval=interval, limit=limit)
            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_asset_volume', 'num_trades',
                'taker_buy_base', 'taker_buy_quote', 'ignore'
            ])
            df['close'] = pd.to_numeric(df['close'])
            return df
        else:
            url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
            params = {"vs_currency": "usd", "days": 3, "interval": "hourly"}
            data = requests.get(url, params=params).json()
            df = pd.DataFrame(data['prices'], columns=['timestamp', 'close'])
            df['close'] = pd.to_numeric(df['close'])
            return df

    def get_indicators(self, symbol="BTCUSDT", interval="1h", limit=100):
        df = self.get_price_history(symbol, interval, limit)

        # RSI
        df['RSI'] = ta.rsi(df['close'], length=14)
        rsi = df['RSI'].iloc[-1]

        # MACD
        macd = ta.macd(df['close'])
        df = pd.concat([df, macd], axis=1)
        macd_line = df['MACD_12_26_9'].iloc[-1]
        macd_signal = df['MACDs_12_26_9'].iloc[-1]

        # Interpret
        rsi_status = ""
        rsi_score = 0

        if rsi < 30:
            rsi_status = "OVERSOLD-buy"
            rsi_score = 9  # very bullish
        elif rsi > 70:
            rsi_status = "OVERBOUGHT-sell"
            rsi_score = 2  # bearish
        else:
            rsi_status = "Neutral"
            rsi_score = 5  # neutral

        macd_status = ""
        macd_score = 0
        if macd_line > macd_signal:
            macd_status = "Upward"
            macd_score = 8  # bullish
        elif macd_line < macd_signal:
            macd_status = "Downward"
            macd_score = 3  # bearish
        else:
            macd_status = "NEUTRAL"
            macd_score = 5  # neutral

        return {
            "RSI": round(rsi, 2),
            "MACD": round(macd_line, 4),
            "MACD_Signal": round(macd_signal, 4),
            "RSI_Interpretation": rsi_status,
            "MACD_Interpretation": macd_status,
            "RSI_Score": rsi_score,
            "MACD_Score": macd_score
        }

    def get_price(self, symbol="BTCUSDT"):
        if self.use_binance:
            data = self.client.get_symbol_ticker(symbol=symbol)
            return float(data['price'])
        else:
            url = f"https://api.coingecko.com/api/v3/simple/price"
            params = {"ids": "bitcoin", "vs_currencies": "usd"}
            data = requests.get(url, params=params).json()
            return data["bitcoin"]["usd"]

    def get_trend(self, symbol="BTCUSDT", interval="1m", limit=50):
        if self.use_binance:
            klines = self.client.get_klines(symbol=symbol, interval=interval, limit=limit)
            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_asset_volume', 'num_trades',
                'taker_buy_base', 'taker_buy_quote', 'ignore'
            ])
            df['close'] = pd.to_numeric(df['close'])
        else:
            url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
            params = {"vs_currency": "usd", "days": 1, "interval": "minutely"}
            prices = requests.get(url, params=params).json()['prices']
            df = pd.DataFrame(prices, columns=['timestamp', 'close'])
            df['close'] = pd.to_numeric(df['close'])

        df['SMA_10'] = ta.sma(df['close'], length=10)
        df['SMA_30'] = ta.sma(df['close'], length=30)

        sma10 = df['SMA_10'].iloc[-1]
        sma30 = df['SMA_30'].iloc[-1]
        
        trend_status = "Unknown"
        trend_score = 0

        if pd.isna(sma10) or pd.isna(sma30):
            trend_status = "Not enough data"
            trend_score = 0
        elif sma10 > sma30:
            trend_status = "Uptrend"
            trend_score = 8  # bullish
        elif sma10 < sma30:
            trend_status = "Downtrend"
            trend_score = 3  # bearish
        else:
            trend_status = "Sideways"
            trend_score = 5  # neutral

        return trend_status, trend_score

        
