import asyncio
import json
import re
import threading
from datetime import datetime, timedelta
import requests
import gspread
import logging
import matplotlib.pyplot as plt
from oauth2client.service_account import ServiceAccountCredentials
from telegram import (
    Bot,
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)
import signal
import os
import sys
from dotenv import load_dotenv
from openai import OpenAI
from binance.client import Client
from binance.exceptions import BinanceAPIException
import time
import pandas as pd
import pandas_ta as ta
from crypto_utils import CryptoDataClient
import pytz

# Set timezone to Israel
os.environ['TZ'] = 'Asia/Jerusalem'
if os.name != 'nt':  # Not Windows
    time.tzset()
# For Windows, we'll handle timezone in datetime operations
israel_tz = pytz.timezone('Asia/Jerusalem')

USE_BINANCE = True       

# Custom filter to mask sensitive data in logs
class SensitiveDataFilter(logging.Filter):
    def __init__(self, sensitive_data=None):
        super().__init__()
        self.sensitive_data = sensitive_data or []

    def filter(self, record):
        if isinstance(record.msg, str):
            for data in self.sensitive_data:
                if data and len(data) > 4:
                    record.msg = record.msg.replace(data, f"{data[:4]}...{data[-4:]}")
        return True

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
# Set httpx logging to WARNING to suppress polling messages
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# Constants
OPTIONS_FILE = '/data/options.json' if os.path.exists('/data') else 'options.json'
CREDENTIALS_FILE = "/app/btc-bot-keys.json" if os.path.exists('/app') else "btc-bot-keys.json"

# Global variables for control
scheduler_task = None
wait_event = asyncio.Event()
stop_event = asyncio.Event()  # For controlled shutdown
last_processed_row = None  # Track last processed row to prevent duplicates

reply_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        ["/scores", "/profit"],
#        ["/manual_buy", "/manual_sell"],
        ["/active_orders"],
        ["/shutdown", "/help"]
    ],
    resize_keyboard=True,
    one_time_keyboard=False
)

def load_config():
    """Load configuration from environment variables"""
    try:
        # Load environment variables from .env file
        load_dotenv()
        
        # Create options dictionary from environment variables
        options = {
            "TOKEN": os.getenv("TOKEN"),
            "CHAT_ID": os.getenv("CHAT_ID"),
            "GSHEET_URL": os.getenv("GSHEET_URL"),
            "JSON_KEYS": os.getenv("JSON_KEYS"),
            "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY"),
            "BINANCE_API_KEY": os.getenv("BINANCE_API_KEY"),
            "BINANCE_API_SECRET": os.getenv("BINANCE_API_SECRET")
        }
        
        # Validate required fields
        required_fields = ["TOKEN", "CHAT_ID", "GSHEET_URL", "JSON_KEYS", "OPENAI_API_KEY", "BINANCE_API_KEY", "BINANCE_API_SECRET"]
        missing_fields = [field for field in required_fields if not options.get(field)]
        
        if missing_fields:
            error_msg = f"Missing required environment variables: {', '.join(missing_fields)}\n"
            error_msg += "Please make sure these variables are set in your .env file"
            raise ValueError(error_msg)
            
        return options
        
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        raise

def setup_google_sheets(json_keys, gsheet_url):
    """Setup Google Sheets connection"""
    try:
        # Parse JSON keys if it's a string
        if isinstance(json_keys, str):
            json_keys = json.loads(json_keys)
            
        # Save credentials file
        with open(CREDENTIALS_FILE, "w") as f:
            json.dump(json_keys, f)
            
        # Setup Google Sheets client
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(gsheet_url).worksheet("RAW")
        
        return sheet
    except Exception as e:
        logger.error(f"Failed to setup Google Sheets: {e}")
        raise

# Load configuration
config = load_config()
TOKEN = config["TOKEN"]
CHAT_ID = config["CHAT_ID"]
GSHEET_URL = config["GSHEET_URL"]
OPENAI_API_KEY = config["OPENAI_API_KEY"]
BINANCE_API_KEY = config["BINANCE_API_KEY"]
BINANCE_API_SECRET = config["BINANCE_API_SECRET"]

# Setup Google Sheets
sheet = setup_google_sheets(config["JSON_KEYS"], GSHEET_URL)

# Add sensitive data filter to logger
logger.addFilter(SensitiveDataFilter([TOKEN]))

GSHEET_CREDENTIALS = CREDENTIALS_FILE

def get_binance_client():
    """Initialize and return a Binance client"""
    try:
        # Initialize client with production environment
        client = Client(
            api_key=BINANCE_API_KEY,
            api_secret=BINANCE_API_SECRET
        )
        client.RECV_WINDOW = 60000  # 60 second window for requests
        
        # Get server time and calculate offset
        server_time = client.get_server_time()
        local_time = int(time.time() * 1000)
        client.timestamp_offset = server_time['serverTime'] - local_time
        logger.info(f"Binance time offset: {client.timestamp_offset}ms")
        
        # Test connection with account info
        client.get_account()
        logger.info("Successfully connected to Binance")
        return client
            
    except Exception as e:
        error_msg = f"Failed to initialize Binance client: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)

def get_all_rows():
    return sheet.get_all_records()

def generate_signal(final_score, technical_score):
    """Generate trading signal based on final and technical scores"""
    if final_score >= 7.5 and technical_score >= 7:
        return "✅ STRONG BUY"
    elif final_score >= 6.5 and technical_score >= 6:
        return "✅ BUY"
    elif final_score <= 3.5 and technical_score <= 3.5:
        return "🛑 STRONG SELL"
    elif final_score <= 4 and technical_score <= 4:
        return "🛑 SELL"
    else:
        return "⚖️ HOLD"

def update_sheets(row_dict):
    """Update both RAW and GUI worksheets with appropriate data"""
    try:
        # Setup Google Sheets client
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        sheets_client = gspread.authorize(creds)
        
        # First update RAW sheet with all data
        raw_sheet = sheets_client.open_by_url(GSHEET_URL).worksheet("RAW")
        
        # Define RAW sheet columns
        raw_columns = [
            "Date", "Price", "Score",
            "supply_demand", "regulation", "macro_economy", 
            "news_sentiment", "whales_activity", "tech_events", "adoption",
            "trend_score", "rsi_score", "macd_score", 
            "candle_15m_score", "candle_30m_score", "candle_1h_score",
            "technical_score", "technical_label", "final_score", 
            "signal", "action", "balance", "fee", "profit/loss", "buy_sell_price", "order_id", "active_orders"
        ]
        
        # Check RAW sheet headers
        try:
            headers = raw_sheet.row_values(1)
            if not headers or headers != raw_columns:
                raw_sheet.clear()
                raw_sheet.append_row(raw_columns)
                logger.info("RAW sheet headers reset and initialized")
        except Exception as e:
            logger.error(f"Error checking RAW headers: {e}")
            raw_sheet.clear()
            raw_sheet.append_row(raw_columns)
            logger.info("RAW sheet headers initialized after error")
        
        # Create RAW row data
        raw_row = [row_dict.get(col, "") for col in raw_columns]
        
        # Append to RAW sheet
        raw_sheet.append_row(raw_row)
        logger.info("Successfully updated RAW sheet")
        
        # Only update GUI sheet if there's a buy or sell action
        action = row_dict.get("action", "")
        if any(trade_type in action for trade_type in ["BUY", "SELL"]):
            # Now update GUI sheet with selected columns
            gui_sheet = sheets_client.open_by_url(GSHEET_URL).worksheet("GUI")
            
            # Define GUI sheet columns
            gui_columns = [
                "Date", "Price", "GPT_Score", "technical_score", 
                "final_score", "action", "balance", "fee", "profit/loss", 
                "buy_sell_price", "order_id", "active_orders"
            ]
            
            # Check GUI sheet headers
            try:
                headers = gui_sheet.row_values(1)
                if not headers or headers != gui_columns:
                    gui_sheet.clear()
                    gui_sheet.append_row(gui_columns)
                    logger.info("GUI sheet headers reset and initialized")
            except Exception as e:
                logger.error(f"Error checking GUI headers: {e}")
                gui_sheet.clear()
                gui_sheet.append_row(gui_columns)
                logger.info("GUI sheet headers initialized after error")
            
            # Create GUI row data (mapping Score to GPT_Score)
            gui_row = [
                row_dict.get("Date", ""),
                row_dict.get("Price", ""),
                row_dict.get("Score", ""),  # GPT_Score
                row_dict.get("technical_score", ""),
                row_dict.get("final_score", ""),
                row_dict.get("action", ""),
                row_dict.get("balance", ""),
                row_dict.get("fee", ""),
                row_dict.get("profit/loss", ""),
                row_dict.get("buy_sell_price", ""),
                row_dict.get("order_id", ""),
                row_dict.get("active_orders", "")  # Adding active_orders to GUI view
            ]
            
            # Append to GUI sheet
            gui_sheet.append_row(gui_row)
            logger.info("Successfully updated GUI sheet")
        else:
            logger.info("Skipping GUI sheet update - No trade action")
        
    except Exception as e:
        error_msg = f"Error updating sheets: {str(e)}"
        logger.error(error_msg)
        raise

# Replace the old append_row_to_sheet function with update_sheets
def append_row_to_sheet(row_dict):
    update_sheets(row_dict)

bot = Bot(token=TOKEN)

def calculate_weighted_technical_score(
    trend_score,
    rsi_score,
    macd_score,
    candle_15m_score,
    candle_30m_score,
    candle_1h_score
):
    # Define weights
    weights = {
        "trend": 0.20,
        "rsi": 0.15,
        "macd": 0.15,
        "c15": 0.10,
        "c30": 0.20,
        "c60": 0.20
    }

    # Weighted average calculation
    technical_score = (
        trend_score * weights["trend"] +
        rsi_score * weights["rsi"] +
        macd_score * weights["macd"] +
        candle_15m_score * weights["c15"] +
        candle_30m_score * weights["c30"] +
        candle_1h_score * weights["c60"]
    )

    # Label interpretation
    if technical_score >= 8:
        label = "🚀 Strong Buy"
    elif technical_score >= 6.5:
        label = "✅ Bullish"
    elif technical_score >= 4.5:
        label = "⚖️ Neutral"
    elif technical_score >= 3:
        label = "🔻 Bearish"
    else:
        label = "🛑 Strong Sell"

    return round(technical_score, 2), label



# ----------- start -----------
async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Welcome! Use the buttons below or type a command.",
        reply_markup=reply_keyboard
    )

# ----------- GPT -----------
def get_binance_usdt_balance():
    """Get USDT balance from Binance account"""
    try:
        client = get_binance_client()
        account = client.get_account()
        usdt_balance = next((asset for asset in account['balances'] 
                           if asset['asset'] == 'USDT'), {'free': '0.0'})
        return round(float(usdt_balance['free']), 1)  # Round to 1 decimal place
    except Exception as e:
        logger.error(f"Error getting Binance USDT balance: {e}")
        return 0.0

def count_active_trades(all_rows):
    """Count active trades from sheet data"""
    active_trades = 0
    if all_rows:
        for row in reversed(all_rows):
            if row.get('action') in ['✅ MANUAL BUY', '✅ AUTO BUY']:
                active_trades += 1
            elif row.get('action') in ['🛑 MANUAL SELL', '🛑 AUTO SELL']:
                active_trades -= 1
    return active_trades

def normalize_scores(scores_dict):
    """Normalize scores with default natural values if zero or missing"""
    default_scores = {
        "supply_demand": 5.0,      # Neutral supply/demand balance
        "regulation": 5.0,         # Neutral regulatory environment
        "macro_economy": 5.0,      # Neutral economic conditions
        "news_sentiment": 5.0,     # Neutral market sentiment
        "whales_activity": 5.0,    # Average whale activity
        "tech_events": 5.0,        # Normal technical development
        "adoption": 5.0,           # Steady adoption rate
        "trend_score": 5.0,        # Neutral trend
        "rsi_score": 5.0,         # Neutral RSI
        "macd_score": 5.0,        # Neutral MACD
        "candle_15m_score": 5.0,  # Neutral 15m candle
        "candle_30m_score": 5.0,  # Neutral 30m candle
        "candle_1h_score": 5.0,   # Neutral 1h candle
        "technical_score": 5.0,   # Neutral technical score
        "gpt_score": 5.0          # Neutral GPT score
    }
    
    normalized = {}
    for key, default_value in default_scores.items():
        if key in scores_dict:
            try:
                value = float(str(scores_dict[key]).replace(',', '').strip() or '0')
                # Only use default if value is 0 or invalid
                normalized[key] = value if value > 0 else default_value
            except (ValueError, TypeError):
                normalized[key] = default_value
        else:
            normalized[key] = default_value
            
    return normalized

def initialize_trade_status():
    """Initialize trade status by checking sheet history"""
    try:
        all_rows = sheet.get_all_records()
        
        # If we have history, use the last row for all our state
        if all_rows:
            # Sort rows by date to ensure we get the actual last row
            sorted_rows = sorted(all_rows, 
                key=lambda x: datetime.strptime(x.get('Date', '01/01/2000 00:00'), "%d/%m/%Y %H:%M"), 
                reverse=True
            )
            last_row = sorted_rows[0]  # Get the most recent row
            
            # Update global last_processed_row
            global last_processed_row
            last_processed_row = (
                last_row.get('Date', ''),
                last_row.get('Price', ''),
                last_row.get('action', '')
            )
            
            # Get balance, ensuring we handle any formatting issues
            try:
                current_balance = float(str(last_row.get('balance', '0')).replace(',', ''))
            except (ValueError, TypeError):
                logger.warning("Invalid balance value in sheet, getting from Binance")
                current_balance = get_binance_usdt_balance()
            
            # Count active trades
            active_trades = count_active_trades(all_rows)
            
            logger.info(f"Initializing from last row - Balance: ${current_balance:.1f}, Active orders: {active_trades}")
            return active_trades, current_balance
        
        # No history, start fresh with Binance balance
        current_balance = get_binance_usdt_balance()
        logger.info(f"No history found - Starting fresh with Binance balance: ${current_balance:.1f}")
        return 0, current_balance
                
    except Exception as e:
        logger.error(f"Error initializing trade status: {e}")
        return 0, get_binance_usdt_balance()

async def handle_gpt_matget(update: Update, context: ContextTypes.DEFAULT_TYPE, force_upload=False, is_profit_sell=False, specific_buy_price=None, is_gpt_update=False, is_technical_check=False, last_gpt_scores=None):
    global last_processed_row
    try:
        openai_key = config.get("OPENAI_API_KEY")
        if not openai_key:
            logger.error("OpenAI API key not configured")
            if update and update.effective_chat and update.effective_chat.id:
                await update.message.reply_text("⚠️ OpenAI API key not configured")
            return

        # Initialize state from last row or Binance
        all_rows = sheet.get_all_records()
        
        # Sort rows by date in descending order (newest first)
        if all_rows:
            all_rows = sorted(
                all_rows,
                key=lambda x: datetime.strptime(x.get('Date', '01/01/2000 00:00'), "%d/%m/%Y %H:%M"),
                reverse=True
            )
        
        # Check if this row was already processed
        if all_rows:
            current_row = (
                all_rows[0].get('Date', ''),  # Get from first row after sorting
                all_rows[0].get('Price', ''),
                all_rows[0].get('action', '')
            )
            if current_row == last_processed_row and not force_upload:
                logger.debug("Skipping duplicate row")
                return
            last_processed_row = current_row
        
        active_trades = count_active_trades(all_rows)
        if not all_rows:
            current_balance = get_binance_usdt_balance()
        else:
            last_row = all_rows[0]  # Use first row after sorting
            current_balance = float(str(last_row.get('balance', get_binance_usdt_balance())).replace(',', ''))
        
        # Get market data
        crypto_client = CryptoDataClient(use_binance=True)
        price = int(crypto_client.get_price("BTCUSDT"))
        trend, trend_score = crypto_client.get_trend()
        indicators = crypto_client.get_indicators("BTCUSDT")
        candle_15m_status, candle_15m_score = crypto_client.detect_green_candle_with_rising_volume("BTCUSDT", interval="15m")
        candle_30m_status, candle_30m_score = crypto_client.detect_green_candle_with_rising_volume("BTCUSDT", interval="30m")
        candle_1h_status, candle_1h_score = crypto_client.detect_green_candle_with_rising_volume("BTCUSDT", interval="1h")
        
        # Calculate technical scores
        technical_score, technical_label = calculate_weighted_technical_score(
            trend_score,
            indicators["RSI_Score"],
            indicators["MACD_Score"],
            candle_15m_score,
            candle_30m_score,
            candle_1h_score
        )
        
        # For profit-based sells or technical checks, we don't need GPT analysis
        if is_profit_sell or is_technical_check:
            if last_gpt_scores:
                normalized_scores = normalize_scores(last_gpt_scores)
                gpt_score = normalized_scores["gpt_score"]
                scores = {k: normalized_scores[k] for k in ["supply_demand", "regulation", "macro_economy", 
                                                          "news_sentiment", "whales_activity", "tech_events", "adoption"]}
            else:
                gpt_score = technical_score
                scores = {
                    "supply_demand": 5.0,
                    "regulation": 5.0,
                    "macro_economy": 5.0,
                    "news_sentiment": 5.0,
                    "whales_activity": 5.0,
                    "tech_events": 5.0,
                    "adoption": 5.0
                }
        else:
            try:
                # Do GPT analysis for all updates except technical checks and profit sells
                logger.info("Starting GPT analysis...")
                client = OpenAI(api_key=openai_key)
                response = client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {"role": "system", "content": "You are a Bitcoin market analyst."},
                        {"role": "user", "content": "Rate Bitcoin's current market factors (1-10):\n- supply_demand (0.25): volume, inflows\n- regulation (0.20): policy changes\n- macro_economy (0.20): rates, inflation\n- news_sentiment (0.10): media mood\n- whales_activity (0.10): large moves\n- tech_events (0.075): upgrades\n- adoption (0.075): institutional\n\nRespond only in format:\nsupply_demand: X\nregulation: X\nmacro_economy: X\nnews_sentiment: X\nwhales_activity: X\ntech_events: X\nadoption: X\nscore_weighted: X"}
                    ]
                )
                
                answer = response.choices[0].message.content
                logger.info(f"GPT Analysis Response: {answer}")
                
                # Only send GPT analysis if this is a manual request
                if update and update.effective_chat and update.effective_chat.id:
                    await update.message.reply_text(f"🤖 Analysis:\n{answer}")
                
                # Parse GPT's response
                pattern = r"(supply_demand|regulation|macro_economy|news_sentiment|whales_activity|tech_events|adoption|score_weighted)\s*:\s*([0-9.]+)"
                matches = re.findall(pattern, answer)
                if len(matches) < 7:  # We only need the 7 main scores, not score_weighted
                    logger.warning("GPT response missing fields, using default values")
                    if update and update.effective_chat and update.effective_chat.id:
                        await update.message.reply_text("❌ GPT response missing fields. Using default values.")
                    scores = {
                        "supply_demand": 5.0,
                        "regulation": 5.0,
                        "macro_economy": 5.0,
                        "news_sentiment": 5.0,
                        "whales_activity": 5.0,
                        "tech_events": 5.0,
                        "adoption": 5.0
                    }
                    gpt_score = 5.0
                else:
                    # Use raw scores directly without normalization
                    raw_scores = {key: float(value) for key, value in matches}
                    scores = {k: raw_scores[k] for k in ["supply_demand", "regulation", "macro_economy", 
                                                       "news_sentiment", "whales_activity", "tech_events", "adoption"]}
                    
                    # Calculate weighted GPT score
                    weights = {
                        "supply_demand": 0.25,
                        "regulation": 0.20,
                        "macro_economy": 0.20,
                        "news_sentiment": 0.10,
                        "whales_activity": 0.10,
                        "tech_events": 0.075,
                        "adoption": 0.075
                    }
                    
                    # Calculate weighted score
                    gpt_score = sum(scores[k] * weights[k] for k in weights.keys())
                    gpt_score = round(gpt_score, 2)
                    
                    logger.info(f"Raw scores from GPT: {scores}")
                    logger.info(f"Calculated GPT Score: {gpt_score}")
                    
                    # Log the calculation details
                    calculation_details = "\n".join([
                        f"{k}: {scores[k]} × {weights[k]} = {scores[k] * weights[k]:.2f}"
                        for k in weights.keys()
                    ])
                    logger.info(f"Score calculation details:\n{calculation_details}")
                    
                    if update and update.effective_chat and update.effective_chat.id:
                        await update.message.reply_text(
                            f"🧮 GPT Score Calculation:\n{calculation_details}\n\nFinal GPT Score: {gpt_score}/10"
                        )
            except Exception as e:
                logger.error(f"Error in GPT analysis: {e}")
                if update and update.effective_chat and update.effective_chat.id:
                    await update.message.reply_text(f"❌ Error in GPT analysis: {e}")
                gpt_score = technical_score
                scores = {
                    "supply_demand": 5.0,
                    "regulation": 5.0,
                    "macro_economy": 5.0,
                    "news_sentiment": 5.0,
                    "whales_activity": 5.0,
                    "tech_events": 5.0,
                    "adoption": 5.0
                }

        final_score = round(gpt_score * 0.25 + technical_score * 0.75, 2)
        now = datetime.now().strftime("%d/%m/%Y %H:%M")
        
        TRADE_AMOUNT = 500
        
        # Generate trading signal and action
        signal = generate_signal(final_score, technical_score)
        trading_action = "⚖️ HOLD"
        profit_loss = ""
        buy_price = ""
        should_upload = force_upload
        order_id = ""  # Initialize order_id
        
        # Check for profit-based sell
        if active_trades > 0:
            for row in reversed(all_rows):
                if row.get('action') in ['✅ MANUAL BUY', '✅ AUTO BUY']:
                    last_buy_price = float(str(row.get('Price', '0')).replace(',', ''))
                    profit_percentage = (price - last_buy_price) / last_buy_price
                    buy_order_id = row.get('order_id', '')  # Get the buy order ID
                    
                    # If profit threshold reached or it's a forced profit sell
                    if profit_percentage >= 0.005 or is_profit_sell:
                        trading_action = "🛑 AUTO SELL"
                        btc_amount = TRADE_AMOUNT / last_buy_price
                        current_value = btc_amount * price
                        gross_profit = round(current_value - TRADE_AMOUNT, 1)
                        buy_fee = round(TRADE_AMOUNT * 0.001, 1)  # Buy fee from previous buy
                        sell_fee = round(current_value * 0.001, 1)  # Sell fee based on current value
                        total_fees = buy_fee + sell_fee  # Total fees for the trade
                        net_profit = round(gross_profit - sell_fee, 1)
                        profit_loss = net_profit
                        current_balance = round(current_balance + TRADE_AMOUNT + gross_profit - sell_fee, 1)
                        buy_price = last_buy_price
                        order_id = f"SELL-{buy_order_id}"  # Reference the buy order in sell order ID
                        should_upload = True
                        logger.info(f"Profit-based sell: Balance updated to ${current_balance:.1f} (Gross P/L: ${gross_profit:.1f}, Buy Fee=${buy_fee:.1f}, Sell Fee=${sell_fee:.1f}, Total Fees=${total_fees:.1f}, Net P/L: ${net_profit:.1f}, Order ID: {order_id})")
                        break
        
        # Check for signal-based trades if no profit sell
        if trading_action == "⚖️ HOLD" and not is_profit_sell:
            if signal == "✅ STRONG BUY" or signal == "✅ BUY":
                # Get real available balance from Binance
                available_balance = get_binance_usdt_balance()
                if current_balance != available_balance:
                    logger.warning(f"Balance mismatch - Sheet: ${current_balance:.1f}, Binance: ${available_balance:.1f}")
                    current_balance = available_balance

                if current_balance >= TRADE_AMOUNT:
                    trading_action = "✅ AUTO BUY"
                    buy_fee = round(TRADE_AMOUNT * 0.001, 1)
                    current_balance = round(current_balance - TRADE_AMOUNT, 1)
                    buy_price = price
                    profit_loss = -buy_fee
                    # Generate new buy order ID using timestamp
                    order_id = f"BUY-{int(time.time())}"
                    logger.info(f"Buy order: Balance updated to ${current_balance:.1f}, Fee: ${buy_fee:.1f}, Order ID: {order_id}")
                    should_upload = True
                else:
                    trading_action = "❌ INSUFFICIENT BALANCE"
                    logger.warning(f"Insufficient balance for buy: ${current_balance:.1f} < ${TRADE_AMOUNT:.1f}")
            elif (signal == "🛑 STRONG SELL" or signal == "🛑 SELL") and active_trades > 0:
                trading_action = "🛑 AUTO SELL"
                for row in reversed(all_rows):
                    if row.get('action') in ['✅ MANUAL BUY', '✅ AUTO BUY']:
                        last_buy_price = float(str(row.get('Price', '0')).replace(',', ''))
                        buy_order_id = row.get('order_id', '')  # Get the buy order ID
                        btc_amount = TRADE_AMOUNT / last_buy_price
                        current_value = btc_amount * price
                        gross_profit = round(current_value - TRADE_AMOUNT, 1)
                        buy_fee = round(TRADE_AMOUNT * 0.001, 1)
                        sell_fee = round(current_value * 0.001, 1)
                        net_profit = round(gross_profit - sell_fee, 1)
                        profit_loss = net_profit
                        current_balance = round(current_balance + TRADE_AMOUNT + gross_profit - sell_fee, 1)
                        buy_price = last_buy_price
                        order_id = f"SELL-{buy_order_id}"  # Reference the buy order in sell order ID
                        logger.info(f"Signal-based sell: Balance updated to ${current_balance:.1f} (Gross P/L: ${gross_profit:.1f}, Fees: Buy=${buy_fee:.1f}, Sell=${sell_fee:.1f}, Net P/L: ${net_profit:.1f}, Order ID: {order_id})")
                        should_upload = True
                        break
        
        # Send notification only for actual trade actions
        if trading_action in ["✅ AUTO BUY", "🛑 AUTO SELL", "✅ MANUAL BUY", "🛑 MANUAL SELL"]:
            # Different format for buy and sell actions
            if trading_action in ["✅ AUTO BUY", "✅ MANUAL BUY"]:
                notification_msg = (
                    f"🚨 BUY ACTION EXECUTED!\n\n"
                    f"💵 Current BTC Price: ${price:,}\n"
                    f"💰 Balance (after): ${current_balance:.1f}\n"
                    f"💵 Investment Amount: ${TRADE_AMOUNT:.1f}\n"
                    f"🔑 Order ID: {order_id}\n\n"
                    f"📊 Technical Score: {technical_score}/10\n"
                    f"🤖 GPT Score: {gpt_score}/10\n"
                    f"🎯 Final Score: {final_score}/10\n"
                )
            else:  # Sell actions
                notification_msg = (
                    f"✅ SELL ACTION EXECUTED!\n\n"
                    f"💵 Current BTC Price: ${price:,}\n"
                    f"💵 Buy BTC Price: ${buy_price:,}\n"
                    f"💰 Balance (after): ${current_balance:.1f}\n"
                    f"💵 Investment Amount: ${TRADE_AMOUNT:.1f}\n"
                    f"💰 Profit: ${profit_loss:.1f}\n"
                    f"Fees: ${buy_fee + sell_fee:.2f} (Buy: ${buy_fee:.2f}, Sell: ${sell_fee:.2f})\n"
                    f"🔑 Order ID: {order_id}\n\n"
                    f"📊 Technical Score: {technical_score}/10\n"
                    f"🤖 GPT Score: {gpt_score}/10\n"
                    f"🎯 Final Score: {final_score}/10\n"
                )
            
            # Always send to configured CHAT_ID for any trade action
            await bot.send_message(chat_id=CHAT_ID, text=notification_msg)
        
        # Only upload data if it's a scheduled update or there was a trade
        if should_upload or trading_action in ["✅ AUTO BUY", "🛑 AUTO SELL"]:
            # Calculate active orders based on the action
            if trading_action in ["✅ AUTO BUY", "✅ MANUAL BUY"]:
                active_orders = count_active_trades(all_rows) + 1  # Add current buy
            elif trading_action in ["🛑 AUTO SELL", "🛑 MANUAL SELL"]:
                active_orders = count_active_trades(all_rows) - 1  # Subtract current sell
            else:
                active_orders = count_active_trades(all_rows)  # No change for other actions

            # For HOLD actions, maintain the last known balance
            if trading_action == "⚖️ HOLD" and all_rows:
                last_row = all_rows[-1]
                current_balance = float(str(last_row.get('balance', current_balance)).replace(',', ''))
                logger.info(f"HOLD action - Maintaining last known balance: ${current_balance:.1f}")

            # Prepare row data with the calculated GPT score
            row = {
                "Date": now,
                "Price": price,
                "Score": gpt_score,  # Make sure we use the calculated GPT score here
                "supply_demand": scores["supply_demand"],
                "regulation": scores["regulation"],
                "macro_economy": scores["macro_economy"],
                "news_sentiment": scores["news_sentiment"],
                "whales_activity": scores["whales_activity"],
                "tech_events": scores["tech_events"],
                "adoption": scores["adoption"],
                "trend_score": trend_score,
                "rsi_score": indicators["RSI_Score"],
                "macd_score": indicators["MACD_Score"],
                "candle_15m_score": candle_15m_score,
                "candle_30m_score": candle_30m_score,
                "candle_1h_score": candle_1h_score,
                "technical_score": technical_score,
                "technical_label": technical_label,
                "final_score": final_score,
                "signal": signal,
                "action": trading_action,
                "balance": current_balance,
                "fee": buy_fee if trading_action in ["✅ AUTO BUY", "✅ MANUAL BUY"] else sell_fee if trading_action in ["🛑 AUTO SELL", "🛑 MANUAL SELL"] else "",
                "profit/loss": gross_profit if trading_action in ["🛑 AUTO SELL", "🛑 MANUAL SELL"] else "",
                "buy_sell_price": buy_price if trading_action in ["✅ AUTO BUY", "✅ MANUAL BUY"] else price if trading_action in ["🛑 AUTO SELL", "🛑 MANUAL SELL"] else "",
                "order_id": order_id,
                "active_orders": active_orders
            }

            logger.info(f"Row data being written - GPT Score: {row['Score']}, Individual scores: {scores}")
            
            try:
                append_row_to_sheet(row)
                if update.effective_chat and update.effective_chat.id:
                    await update.message.reply_text("✅ Analysis saved to Google Sheets successfully!")
            except Exception as e:
                error_msg = f"❌ Error saving to Google Sheets: {str(e)}"
                logger.error(error_msg)
                if update.effective_chat and update.effective_chat.id:
                    await update.message.reply_text(error_msg)
        
    except Exception as e:
        error_msg = f"❌ Error: {str(e)}"
        logger.error(error_msg)	
        if update.effective_chat and update.effective_chat.id:
            await update.message.reply_text(error_msg)

# ----------- price -----------
async def handle_price_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for manual price check command"""
    try:
        crypto_client = CryptoDataClient(use_binance=True)
        price = int(crypto_client.get_price())  # Convert price to integer
        await update.message.reply_text(f"💰 Current BTC Price: ${price:,}")  # Display price as integer with comma separator
    except Exception as e:
        error_msg = f"❌ Error getting BTC price: {str(e)}"
        logger.error(error_msg)
        await update.message.reply_text(error_msg)

# ----------- help -----------
async def handle_help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "*Bitcoin Trading Bot - Command Guide*\n\n"
        "*Trading Commands:*\n"
        "`/active_orders` - View active orders, positions, and balances\n"
        "`/scores` - View technical, GPT, and final scores\n"
        "`/profit` - View daily, weekly, and monthly profits\n\n"
        
        "*System Commands:*\n"
        "`/help` - Show this help message\n"
        "`/shutdown` - Safely stop the bot\n\n"
        
        "*Debug Commands* (Admin Only):\n"
        "`/debug` - Toggle debug mode\n"
        "`/set_price` - Set custom BTC price\n"
        "`/clear_price` - Clear price override"
    )
    
    try:
        await update.message.reply_text(
            text=help_text,
            reply_markup=reply_keyboard,
            parse_mode='MarkdownV2'
        )
    except Exception as e:
        logger.error(f"Error sending help message: {e}")
        # Fallback to plain text if Markdown fails
        plain_help_text = help_text.replace('*', '').replace('`', '')
        await update.message.reply_text(
            text=plain_help_text,
            reply_markup=reply_keyboard
        )

# ----------------------------------------------------------------------------------------  
# Add global variable for scheduler task
scheduler_task = None
wait_event = asyncio.Event()

async def update_scheduler_interval():
    """Update scheduler interval based on active trades"""
    global wait_event
    all_rows = sheet.get_all_records()
    active_trades = count_active_trades(all_rows)
    
    if active_trades > 0:
        logger.info(f"Updating scheduler: {active_trades} active trades detected - Switching to 15-minute intervals")
        wait_event.set()  # Signal the scheduler to update interval
    else:
        logger.info("Updating scheduler: No active trades - Switching to 30-minute intervals")
        wait_event.set()  # Signal the scheduler to update interval

async def schedule_gpt_updates():
    """Schedule automatic GPT updates with dynamic intervals based on active trades"""
    global wait_event
    
    try:
        # Initialize trade status at startup
        active_trades, current_balance = initialize_trade_status()
        MIN_WAIT_TIME = 60  # Minimum wait time in seconds
        PROFIT_THRESHOLD = 0.005  # 0.5% profit threshold
        last_upload_time = datetime.now()
        last_check_time = datetime.now()
        
        # Store last GPT analysis
        last_gpt_scores = {
            "supply_demand": 0,
            "regulation": 0,
            "macro_economy": 0,
            "news_sentiment": 0,
            "whales_activity": 0,
            "tech_events": 0,
            "adoption": 0,
            "gpt_score": 0
        }
        
        # Structure to track active buy orders
        active_buy_orders = []
        
        # Initialize active buy orders at startup
        if active_trades > 0:
            all_rows = sheet.get_all_records()
            for row in reversed(all_rows):
                if row.get('action') in ['✅ MANUAL BUY', '✅ AUTO BUY']:
                    buy_price = float(str(row.get('Price', '0')).replace(',', ''))
                    active_buy_orders.append({
                        'price': buy_price,
                        'date': row.get('Date'),
                        'type': row.get('action')
                    })
                    if len(active_buy_orders) == active_trades:
                        break
            
            # Get last GPT scores from sheet
            if all_rows:
                last_row = all_rows[-1]
                last_gpt_scores = {
                    "supply_demand": float(last_row.get('supply_demand', 0)),
                    "regulation": float(last_row.get('regulation', 0)),
                    "macro_economy": float(last_row.get('macro_economy', 0)),
                    "news_sentiment": float(last_row.get('news_sentiment', 0)),
                    "whales_activity": float(last_row.get('whales_activity', 0)),
                    "tech_events": float(last_row.get('tech_events', 0)),
                    "adoption": float(last_row.get('adoption', 0)),
                    "gpt_score": float(last_row.get('Score', 0))
                }
            
            logger.info(f"Starting scheduler with {len(active_buy_orders)} active buy orders:")
            for order in active_buy_orders:
                logger.info(f"Buy order at ${order['price']:,} ({order['date']}, {order['type']})")
        
        while True:
            try:
                now = datetime.now()
                
                # Check for active trades from the sheet before update
                all_rows = sheet.get_all_records()
                active_trades = count_active_trades(all_rows)
                
                # Update active buy orders if needed
                if active_trades > len(active_buy_orders):
                    # New buy orders found, add them
                    for row in reversed(all_rows):
                        if row.get('action') in ['✅ MANUAL BUY', '✅ AUTO BUY']:
                            buy_price = float(str(row.get('Price', '0')).replace(',', ''))
                            # Check if this buy order is already tracked
                            if not any(order['price'] == buy_price and order['date'] == row.get('Date') for order in active_buy_orders):
                                active_buy_orders.append({
                                    'price': buy_price,
                                    'date': row.get('Date'),
                                    'type': row.get('action')
                                })
                                logger.info(f"Added new buy order at ${buy_price:,} ({row.get('Date')}, {row.get('action')})")
                elif active_trades < len(active_buy_orders):
                    # Some orders were sold, reset tracking
                    active_buy_orders = []
                    for row in reversed(all_rows):
                        if row.get('action') in ['✅ MANUAL BUY', '✅ AUTO BUY']:
                            buy_price = float(str(row.get('Price', '0')).replace(',', ''))
                            active_buy_orders.append({
                                'price': buy_price,
                                'date': row.get('Date'),
                                'type': row.get('action')
                            })
                            if len(active_buy_orders) == active_trades:
                                break
                
                # Set intervals
                check_interval = 60  # Check every 1 minute
                upload_interval = 15 * 60  # Upload every 15 minutes
                
                time_since_last_upload = (now - last_upload_time).total_seconds()
                time_since_last_check = (now - last_check_time).total_seconds()
                
                # Calculate next check times
                next_check_time = max(MIN_WAIT_TIME, check_interval - time_since_last_check)
                next_upload_time = max(MIN_WAIT_TIME, upload_interval - time_since_last_upload)
                
                # Determine the shortest wait time needed
                wait_time = min(next_check_time, next_upload_time)
                
                # Log the next intervals and wait time
                logger.info(f"Next intervals - Check: {next_check_time/60:.1f}m, Upload: {next_upload_time/60:.1f}m")
                logger.info(f"Waiting {wait_time/60:.1f} minutes until next action")
                
                # Wait for the calculated interval
                try:
                    await asyncio.wait_for(wait_event.wait(), timeout=wait_time)
                    wait_event.clear()
                    logger.info("Scheduler interval update triggered")
                    continue  # Skip to next iteration if event was triggered
                except asyncio.TimeoutError:
                    # Normal interval elapsed
                    pass
                
                # Check if it's time for regular upload (15m interval - GPT analysis)
                if time_since_last_upload >= upload_interval:
                    force_upload = True
                    last_upload_time = now
                    logger.info("Time for 15-minute update - Running GPT analysis and uploading to sheet")
                    
                    # Create mock update for GPT analysis and upload
                    class MockMessage:
                        def __init__(self):
                            self.chat_id = None
                            self.id = 0
                            self.date = datetime.now()
                            self.text = ""
                            
                        async def reply_text(self, text, *args, **kwargs):
                            pass
                    
                    class MockChat:
                        def __init__(self):
                            self.id = None
                            self.type = "private"
                            self.username = "AutomatedCheck"
                            self.first_name = "Automated"
                            self.last_name = "Check"
                    
                    class MockUpdate:
                        def __init__(self):
                            self.message = MockMessage()
                            self.effective_chat = MockChat()
                            self.effective_message = self.message
                            self.effective_user = None
                            self.update_id = 0
                    
                    mock_update = MockUpdate()
                    # Run full analysis with GPT
                    await handle_gpt_matget(mock_update, None, force_upload=True, is_profit_sell=False, is_gpt_update=True)
                    
                    # Update last GPT scores from sheet
                    all_rows = sheet.get_all_records()
                    if all_rows:
                        last_row = all_rows[-1]
                        last_gpt_scores = {
                            "supply_demand": float(last_row.get('supply_demand', 0)),
                            "regulation": float(last_row.get('regulation', 0)),
                            "macro_economy": float(last_row.get('macro_economy', 0)),
                            "news_sentiment": float(last_row.get('news_sentiment', 0)),
                            "whales_activity": float(last_row.get('whales_activity', 0)),
                            "tech_events": float(last_row.get('tech_events', 0)),
                            "adoption": float(last_row.get('adoption', 0)),
                            "gpt_score": float(last_row.get('Score', 0))
                        }
                
                # Check if it's time for technical analysis and profit check (1m interval)
                if time_since_last_check >= check_interval:
                    last_check_time = now
                    logger.info("Running 1-minute technical analysis and profit check")
                    
                    # Create mock update for technical check
                    class MockMessage:
                        def __init__(self):
                            self.chat_id = None
                            self.id = 0
                            self.date = datetime.now()
                            self.text = ""
                            
                        async def reply_text(self, text, *args, **kwargs):
                            pass
                    
                    class MockChat:
                        def __init__(self):
                            self.id = None
                            self.type = "private"
                            self.username = "TechnicalCheck"
                            self.first_name = "Technical"
                            self.last_name = "Check"
                    
                    class MockUpdate:
                        def __init__(self):
                            self.message = MockMessage()
                            self.effective_chat = MockChat()
                            self.effective_message = self.message
                            self.effective_user = None
                            self.update_id = 0
                    
                    mock_update = MockUpdate()
                    # Run technical analysis only
                    await handle_gpt_matget(mock_update, None, force_upload=False, is_profit_sell=False, is_technical_check=True, last_gpt_scores=last_gpt_scores)
                
            except Exception as e:
                logger.error(f"Error in scheduled GPT update loop: {e}")
                # On error in the loop, wait for 1 minute before retrying
                await asyncio.sleep(60)
                continue
    
    except Exception as e:
        logger.error(f"Error in scheduled GPT update: {e}")
        # On error in the main function, wait for 5 minutes before exiting
        await asyncio.sleep(5 * 60)
        raise

async def initialize_bot_state():
    """Initialize bot state with fresh data on startup"""
    try:
        logger.info("Initializing bot state...")
        
        # Get fresh market data
        crypto_client = CryptoDataClient(use_binance=True)
        price = int(crypto_client.get_price("BTCUSDT"))
        trend, trend_score = crypto_client.get_trend()
        indicators = crypto_client.get_indicators("BTCUSDT")
        candle_15m_status, candle_15m_score = crypto_client.detect_green_candle_with_rising_volume("BTCUSDT", interval="15m")
        candle_30m_status, candle_30m_score = crypto_client.detect_green_candle_with_rising_volume("BTCUSDT", interval="30m")
        candle_1h_status, candle_1h_score = crypto_client.detect_green_candle_with_rising_volume("BTCUSDT", interval="1h")
        
        # Calculate technical score
        technical_score, technical_label = calculate_weighted_technical_score(
            trend_score,
            indicators["RSI_Score"],
            indicators["MACD_Score"],
            candle_15m_score,
            candle_30m_score,
            candle_1h_score
        )
        
        # Get GPT analysis
        client = OpenAI(api_key=config.get("OPENAI_API_KEY"))
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a Bitcoin market analyst."},
                {"role": "user", "content": "Rate Bitcoin's current market factors (1-10):\n- supply_demand (0.25): volume, inflows\n- regulation (0.20): policy changes\n- macro_economy (0.20): rates, inflation\n- news_sentiment (0.10): media mood\n- whales_activity (0.10): large moves\n- tech_events (0.075): upgrades\n- adoption (0.075): institutional\n\nRespond only in format:\nsupply_demand: X\nregulation: X\nmacro_economy: X\nnews_sentiment: X\nwhales_activity: X\ntech_events: X\nadoption: X\nscore_weighted: X"}
            ]
        )
        
        answer = response.choices[0].message.content
        pattern = r"(supply_demand|regulation|macro_economy|news_sentiment|whales_activity|tech_events|adoption|score_weighted)\s*:\s*([0-9.]+)"
        matches = re.findall(pattern, answer)
        
        if len(matches) < 7:  # We only need the 7 main scores, not score_weighted
            logger.warning("GPT response missing fields. Using default values.")
            scores = {
                "supply_demand": 5.0,
                "regulation": 5.0,
                "macro_economy": 5.0,
                "news_sentiment": 5.0,
                "whales_activity": 5.0,
                "tech_events": 5.0,
                "adoption": 5.0
            }
            gpt_score = 5.0
        else:
            # Use raw scores directly without normalization
            raw_scores = {key: float(value) for key, value in matches}
            scores = {k: raw_scores[k] for k in ["supply_demand", "regulation", "macro_economy", 
                                               "news_sentiment", "whales_activity", "tech_events", "adoption"]}
            
            # Calculate weighted GPT score
            weights = {
                "supply_demand": 0.25,
                "regulation": 0.20,
                "macro_economy": 0.20,
                "news_sentiment": 0.10,
                "whales_activity": 0.10,
                "tech_events": 0.075,
                "adoption": 0.075
            }
            
            # Calculate weighted score
            gpt_score = sum(scores[k] * weights[k] for k in weights.keys())
            gpt_score = round(gpt_score, 2)
            
            logger.info(f"Raw scores from GPT: {scores}")
            logger.info(f"Calculated GPT Score: {gpt_score}")
            
            # Log the calculation details
            calculation_details = "\n".join([
                f"{k}: {scores[k]} × {weights[k]} = {scores[k] * weights[k]:.2f}"
                for k in weights.keys()
            ])
            logger.info(f"Score calculation details:\n{calculation_details}")
        
        # Calculate final score
        final_score = round(gpt_score * 0.25 + technical_score * 0.75, 2)
        
        # Get current time
        now = datetime.now(israel_tz).strftime("%d/%m/%Y %H:%M")
        
        # Check if sheet has data
        all_rows = sheet.get_all_records()
        if all_rows:
            # Get balance and active trades from last row
            sorted_rows = sorted(all_rows, 
                key=lambda x: datetime.strptime(x.get('Date', '01/01/2000 00:00'), "%d/%m/%Y %H:%M"), 
                reverse=True
            )
            last_row = sorted_rows[0]
            try:
                current_balance = float(str(last_row.get('balance', '0')).replace(',', ''))
            except (ValueError, TypeError):
                current_balance = get_binance_usdt_balance()
            
            active_trades = count_active_trades(all_rows)
            logger.info(f"Initializing from existing data - Balance: ${current_balance:.1f}, Active trades: {active_trades}")
        else:
            # Start fresh with Binance balance
            current_balance = get_binance_usdt_balance()
            active_trades = 0
            logger.info(f"Starting fresh - Balance: ${current_balance:.1f}, Active trades: 0")
        
        # Generate signal
        signal = generate_signal(final_score, technical_score)
        
        # Prepare row data
        row = {
            "Date": now,
            "Price": price,
            "Score": gpt_score,
            "supply_demand": scores["supply_demand"],
            "regulation": scores["regulation"],
            "macro_economy": scores["macro_economy"],
            "news_sentiment": scores["news_sentiment"],
            "whales_activity": scores["whales_activity"],
            "tech_events": scores["tech_events"],
            "adoption": scores["adoption"],
            "trend_score": trend_score,
            "rsi_score": indicators["RSI_Score"],
            "macd_score": indicators["MACD_Score"],
            "candle_15m_score": candle_15m_score,
            "candle_30m_score": candle_30m_score,
            "candle_1h_score": candle_1h_score,
            "technical_score": technical_score,
            "technical_label": technical_label,
            "final_score": final_score,
            "signal": signal,
            "action": "⚖️ HOLD",
            "balance": current_balance,
            "fee": "",
            "profit/loss": "",
            "buy_sell_price": "",
            "order_id": "",
            "active_orders": active_trades
        }
        
        # Add to sheet
        append_row_to_sheet(row)
        logger.info("Initial state recorded to sheet")
        
        return active_trades, current_balance
        
    except Exception as e:
        logger.error(f"Error initializing bot state: {e}")
        raise

async def main():
    now = datetime.now(israel_tz).strftime("%d/%m/%Y %H:%M")
    logger.info(f"{now}: Main On")
    
    try:
        # Initialize bot state with fresh data
        active_trades, current_balance = await initialize_bot_state()
        logger.info(f"Bot initialized - Active trades: {active_trades}, Balance: ${current_balance:.1f}")
        
        # Initialize bot with specific settings
        application = (
            ApplicationBuilder()
            .token(TOKEN)
            .concurrent_updates(True)
            .arbitrary_callback_data(True)
            .build()
        )
        
        # Add handlers
        application.add_handler(CommandHandler("start", handle_start))
        application.add_handler(CommandHandler("help", handle_help_command))
        application.add_handler(CommandHandler("gpt", handle_gpt_matget))
        application.add_handler(CommandHandler("price", handle_price_command))
        application.add_handler(CommandHandler("trade", handle_trade_command))
        application.add_handler(CommandHandler("balance", handle_balance_command))
        application.add_handler(CommandHandler("manual_buy", handle_manual_buy))
        application.add_handler(CommandHandler("manual_sell", handle_manual_sell))
        application.add_handler(CommandHandler("scores", handle_scores))
        application.add_handler(CommandHandler("active_orders", handle_active_orders))
        application.add_handler(CommandHandler("profit", handle_profit_command))
        application.add_handler(CommandHandler("shutdown", handle_shutdown))
        # Add debug commands
        application.add_handler(CommandHandler("debug", handle_debug_mode))
        application.add_handler(CommandHandler("set_price", handle_set_price))
        application.add_handler(CommandHandler("clear_price", handle_clear_price))
        
        # Start the bot
        await application.initialize()
        await application.start()
        await application.updater.start_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True
        )
        
        logger.info("Bot is now polling for updates...")
        
        # Start the GPT update scheduler
        global scheduler_task
        scheduler_task = asyncio.create_task(schedule_gpt_updates())
        logger.info("Started GPT update scheduler (dynamic intervals)")
        
        # Keep the bot running until stop_event is set
        try:
            while not stop_event.is_set():
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info("Main loop cancelled, starting shutdown...")
        finally:
            # Cleanup when stop_event is set
            logger.info("Stopping bot...")
            if scheduler_task and not scheduler_task.done():
                scheduler_task.cancel()
                try:
                    await scheduler_task
                except asyncio.CancelledError:
                    pass
            
            await application.updater.stop()
            await application.stop()
            await application.shutdown()
            logger.info("Bot stopped successfully!")
    
    except Exception as e:
        logger.error(f"❌ Bot failed to start: {e}")
        raise

# Add new functions before main()
async def handle_trade_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for testing Binance connection and basic functions"""
    try:
        # Initialize Binance client
        binance_client = get_binance_client()
        
        # Get server time
        time_res = binance_client.get_server_time()
        server_time = time_res['serverTime']
        
        # Prepare params with timestamp
        params = {}
        if hasattr(binance_client, 'timestamp_offset'):
            params['timestamp'] = int(time.time() * 1000 + binance_client.timestamp_offset)
        
        # Get account info
        account = binance_client.get_account(**params)
        btc_balance = next((asset for asset in account['balances'] 
                          if asset['asset'] == 'BTC'), {'free': '0.0'})
        usdt_balance = next((asset for asset in account['balances'] 
                           if asset['asset'] == 'USDT'), {'free': '0.0'})
        
        # Get current price (no timestamp needed for public endpoints)
        btc_price = float(binance_client.get_symbol_ticker(symbol="BTCUSDT")['price'])
        
        # Create response message
        message = (
            "🔄 Binance Connection Test:\n\n"
            f"Server Time: {datetime.fromtimestamp(server_time/1000)}\n"
            f"Time Offset: {binance_client.timestamp_offset}ms\n"
            f"BTC Price: ${btc_price:,.2f}\n"
            f"BTC Balance: {float(btc_balance['free']):.8f}\n"
            f"USDT Balance: ${float(usdt_balance['free']):.2f}\n\n"
            "✅ Connection successful!"
        )
        
        await update.message.reply_text(message)
        
    except BinanceAPIException as e:
        error_msg = f"❌ Binance API Error: {str(e)}"
        logger.error(error_msg)
        await update.message.reply_text(error_msg)
    except Exception as e:
        error_msg = f"❌ Error: {str(e)}"
        logger.error(error_msg)
        await update.message.reply_text(error_msg)

async def handle_balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text("🔄 Checking your Binance balance...")
        client = get_binance_client()
        account = client.get_account()
        
        balances = [asset for asset in account['balances'] 
                   if float(asset['free']) > 0 or float(asset['locked']) > 0]
        
        if not balances:
            await update.message.reply_text("No assets found with non-zero balance.")
            return
            
        message = "💰 Your Balances:\n\n"
        stablecoins = ['USDT', 'BUSD', 'USDC']
        
        # Show stablecoins first
        for asset in balances:
            if asset['asset'] in stablecoins:
                free = float(asset['free'])
                locked = float(asset['locked'])
                if locked > 0:
                    message += f"{asset['asset']}: ${free:.2f} (Locked: ${locked:.2f})\n"
                else:
                    message += f"{asset['asset']}: ${free:.2f}\n"
        
        # Show other assets
        for asset in balances:
            if asset['asset'] not in stablecoins:
                free = float(asset['free'])
                locked = float(asset['locked'])
                try:
                    ticker = client.get_symbol_ticker(symbol=f"{asset['asset']}USDT")
                    price = float(ticker['price'])
                    value = free * price
                    if locked > 0:
                        message += f"{asset['asset']}: {free:.8f} ≈ ${value:.2f} (Locked: {locked:.8f})\n"
                    else:
                        message += f"{asset['asset']}: {free:.8f} ≈ ${value:.2f}\n"
                except:
                    if locked > 0:
                        message += f"{asset['asset']}: {free:.8f} (Locked: {locked:.8f})\n"
                    else:
                        message += f"{asset['asset']}: {free:.8f}\n"
        
        await update.message.reply_text(message)
        
    except Exception as e:
        error_msg = f"❌ Error getting balance: {str(e)}"
        logger.error(error_msg)
        await update.message.reply_text(error_msg)

# Add manual trade handlers
async def handle_manual_buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for manual buy command"""
    try:
        # Get current price and balance
        crypto_client = CryptoDataClient(use_binance=True)
        price = int(crypto_client.get_price("BTCUSDT"))
        
        # Get current status and scores
        trend, trend_score = crypto_client.get_trend()
        indicators = crypto_client.get_indicators("BTCUSDT")
        candle_15m_status, candle_15m_score = crypto_client.detect_green_candle_with_rising_volume("BTCUSDT", interval="15m")
        candle_30m_status, candle_30m_score = crypto_client.detect_green_candle_with_rising_volume("BTCUSDT", interval="30m")
        candle_1h_status, candle_1h_score = crypto_client.detect_green_candle_with_rising_volume("BTCUSDT", interval="1h")
        
        # Calculate technical score
        technical_score, technical_label = calculate_weighted_technical_score(
            trend_score,
            indicators["RSI_Score"],
            indicators["MACD_Score"],
            candle_15m_score,
            candle_30m_score,
            candle_1h_score
        )
        
        # Get all rows and count active trades
        all_rows = sheet.get_all_records()
        active_trades = count_active_trades(all_rows)
        
        # Get current time
        now = datetime.now(israel_tz).strftime("%d/%m/%Y %H:%M")
        
        # Get current balance from sheet and verify with Binance
        if not all_rows:
            current_balance = get_binance_usdt_balance()
        else:
            last_row = all_rows[-1]
            sheet_balance = float(str(last_row.get('balance', get_binance_usdt_balance())).replace(',', ''))
            binance_balance = get_binance_usdt_balance()
            
            if sheet_balance != binance_balance:
                logger.warning(f"Balance mismatch - Sheet: ${sheet_balance:.1f}, Binance: ${binance_balance:.1f}")
                current_balance = binance_balance
            else:
                current_balance = sheet_balance
        
        current_balance = round(current_balance, 1)
        TRADE_AMOUNT = 500  # Standard trade amount
        
        if current_balance >= TRADE_AMOUNT:
            trading_action = "✅ MANUAL BUY"
            buy_fee = TRADE_AMOUNT * 0.001  # Calculate buy fee
            current_balance = round(current_balance - TRADE_AMOUNT, 1)  # Only deduct investment amount
            buy_price = price  # Set buy price for manual buy
            profit_loss = -buy_fee  # Show buy fee as initial loss
            logger.info(f"Manual buy order: Balance updated to ${current_balance:.1f}, Fee: ${buy_fee:.2f}")
        else:
            trading_action = "❌ INSUFFICIENT BALANCE"
            logger.warning(f"Insufficient balance for buy: ${current_balance:.1f} < ${TRADE_AMOUNT:.1f}")
            await update.message.reply_text(f"❌ Insufficient balance (${current_balance:.1f}) for trade amount (${TRADE_AMOUNT:.1f})")
            return
        
        # Prepare row data for sheet with current market status
        row = {
            "Date": now,
            "Price": price,
            "Score": technical_score,
            "supply_demand": 0,
            "regulation": 0,
            "macro_economy": 0,
            "news_sentiment": 0,
            "whales_activity": 0,
            "tech_events": 0,
            "adoption": 0,
            "trend_score": trend_score,
            "rsi_score": indicators["RSI_Score"],
            "macd_score": indicators["MACD_Score"],
            "candle_15m_score": candle_15m_score,
            "candle_30m_score": candle_30m_score,
            "candle_1h_score": candle_1h_score,
            "technical_score": technical_score,
            "technical_label": technical_label,
            "final_score": technical_score,
            "signal": signal,
            "action": trading_action,
            "balance": current_balance,
            "fee": buy_fee if trading_action == "✅ MANUAL BUY" else "",
            "profit/loss": "",
            "buy_sell_price": price if trading_action == "✅ MANUAL BUY" else "",
            "order_id": ""  # Assuming a default order_id
        }
        
        # Add to sheet
        try:
            append_row_to_sheet(row)
            await update.message.reply_text("✅ Analysis saved to Google Sheets successfully!")
        except Exception as e:
            await update.message.reply_text(f"❌ Error saving to Google Sheets:\n{e}")
            return
        
        # Update scheduler interval if buy was successful
        if trading_action == "✅ MANUAL BUY":
            await update_scheduler_interval()
            
            # Send confirmation message with current market status
            message = (
                f"✅ Manual Buy Order Executed\n"
                f"Price: ${price:,}\n"
                f"Amount: ${TRADE_AMOUNT:.1f}\n"
                f"New Balance: ${current_balance:.1f}\n\n"
                f"Current Market Status:\n"
                f"Technical Score: {technical_score}/10\n"
                f"Signal: {signal}\n"
                f"Technical Label: {technical_label}\n"
                f"Trend: {trend}\n"
                f"RSI: {indicators['RSI_Interpretation']}\n"
                f"MACD: {indicators['MACD_Interpretation']}"
            )
            await update.message.reply_text(message)
        
    except Exception as e:
        error_msg = f"❌ Error executing manual buy: {str(e)}"
        logger.error(error_msg)
        await update.message.reply_text(error_msg)

async def handle_manual_sell(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for manual sell command"""
    try:
        # Get current price and market status
        crypto_client = CryptoDataClient(use_binance=True)
        price = int(crypto_client.get_price("BTCUSDT"))
        
        # Get current status and scores
        trend, trend_score = crypto_client.get_trend()
        indicators = crypto_client.get_indicators("BTCUSDT")
        candle_15m_status, candle_15m_score = crypto_client.detect_green_candle_with_rising_volume("BTCUSDT", interval="15m")
        candle_30m_status, candle_30m_score = crypto_client.detect_green_candle_with_rising_volume("BTCUSDT", interval="30m")
        candle_1h_status, candle_1h_score = crypto_client.detect_green_candle_with_rising_volume("BTCUSDT", interval="1h")
        
        # Calculate technical score
        technical_score, technical_label = calculate_weighted_technical_score(
            trend_score,
            indicators["RSI_Score"],
            indicators["MACD_Score"],
            candle_15m_score,
            candle_30m_score,
            candle_1h_score
        )
        
        # Get all rows and check for active trades
        all_rows = sheet.get_all_records()
        active_trades = count_active_trades(all_rows)
        
        # Get current time
        now = datetime.now(israel_tz).strftime("%d/%m/%Y %H:%M")
        
        # Log current trade status before sell
        logger.info(f"Current trade status before sell - Active trades: {active_trades}")
        
        trading_action = "🛑 MANUAL SELL"  # Default action for manual sell
        
        # Get current balance and initialize variables
        if not all_rows:
            current_balance = get_binance_usdt_balance()
            await update.message.reply_text("❌ No trading history found")
            trading_action = "❌ NO HISTORY"
            profit_loss = ""
        else:
            last_row = all_rows[-1]
            current_balance = float(str(last_row.get('balance', get_binance_usdt_balance())).replace(',', ''))
            
            if active_trades <= 0:
                await update.message.reply_text("❌ No active trades to sell")
                trading_action = "❌ NO ACTIVE TRADES"
                profit_loss = ""
            else:
                # Execute sell logic
                TRADE_AMOUNT = 500  # Standard trade amount
                # Find the corresponding buy price for the last active trade
                for row in reversed(all_rows):
                    if row.get('action') in ['✅ MANUAL BUY', '✅ AUTO BUY']:
                        last_buy_price = float(str(row.get('Price', '0')).replace(',', ''))
                        btc_amount = TRADE_AMOUNT / last_buy_price
                        current_value = btc_amount * price
                        gross_profit = round(current_value - TRADE_AMOUNT, 1)  # Gross profit before fees
                        buy_fee = TRADE_AMOUNT * 0.001  # Buy fee from previous buy
                        sell_fee = current_value * 0.001  # Sell fee based on current value
                        net_profit = gross_profit - sell_fee  # Net profit after sell fee only
                        profit_loss = net_profit  # Show net profit in profit/loss column
                        current_balance = round(current_balance + TRADE_AMOUNT + gross_profit - sell_fee, 1)  # Update balance with all fees
                        buy_price = last_buy_price  # Set buy price from the original buy order
                        logger.info(f"Manual sell order: Balance updated to ${current_balance:.1f} (Gross P/L: ${gross_profit:.1f}, Fees: Buy=${buy_fee:.2f}, Sell=${sell_fee:.2f}, Net P/L: ${net_profit:.1f})")
                        break
                else:
                    profit_loss = ""
        
        # Generate current signal based on scores
        signal = generate_signal(technical_score, technical_score)  # Using technical score for both since it's manual
        
        # Prepare row data for sheet with current market status
        row = {
            "Date": now,
            "Price": price,
            "Score": technical_score,
            "supply_demand": 0,
            "regulation": 0,
            "macro_economy": 0,
            "news_sentiment": 0,
            "whales_activity": 0,
            "tech_events": 0,
            "adoption": 0,
            "trend_score": trend_score,
            "rsi_score": indicators["RSI_Score"],
            "macd_score": indicators["MACD_Score"],
            "candle_15m_score": candle_15m_score,
            "candle_30m_score": candle_30m_score,
            "candle_1h_score": candle_1h_score,
            "technical_score": technical_score,
            "technical_label": technical_label,
            "final_score": technical_score,
            "signal": signal,
            "action": trading_action,
            "balance": current_balance,
            "fee": sell_fee if trading_action == "🛑 MANUAL SELL" else "",
            "profit/loss": gross_profit if trading_action == "🛑 MANUAL SELL" else "",
            "buy_sell_price": price if trading_action == "🛑 MANUAL SELL" else "",
            "order_id": ""
        }
        
        # Add to sheet
        try:
            append_row_to_sheet(row)
            await update.message.reply_text("✅ Analysis saved to Google Sheets successfully!")
        except Exception as e:
            await update.message.reply_text(f"❌ Error saving to Google Sheets:\n{e}")
            return
        
        # Update scheduler interval and send confirmation if sell was successful
        if trading_action == "🛑 MANUAL SELL":
            await update_scheduler_interval()
            
            # Send confirmation message with current market status
            message = (
                f"🛑 Manual Sell Order Executed\n"
                f"Price: ${price:,}\n"
                f"Amount: ${TRADE_AMOUNT:.1f}\n"
                f"Profit/Loss: ${profit_loss:.1f}\n"
                f"New Balance: ${current_balance:.1f}\n\n"
                f"Current Market Status:\n"
                f"Technical Score: {technical_score}/10\n"
                f"Signal: {signal}\n"
                f"Technical Label: {technical_label}\n"
                f"Trend: {trend}\n"
                f"RSI: {indicators['RSI_Interpretation']}\n"
                f"MACD: {indicators['MACD_Interpretation']}"
            )
            await update.message.reply_text(message)
        
    except Exception as e:
        error_msg = f"❌ Error executing manual sell: {str(e)}"
        logger.error(error_msg)
        await update.message.reply_text(error_msg)

# Add at the top of the file with other global variables
DEBUG_MODE = False
DEBUG_PRICE = None

# Add new command handlers
async def handle_debug_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle debug mode on/off"""
    global DEBUG_MODE, DEBUG_PRICE
    
    try:
        if update.effective_chat.id != int(CHAT_ID):
            await update.message.reply_text("❌ Unauthorized access")
            return
            
        DEBUG_MODE = not DEBUG_MODE
        if not DEBUG_MODE:
            DEBUG_PRICE = None
            
        status = "ON ✅" if DEBUG_MODE else "OFF ❌"
        message = (
            f"🔧 Debug Mode: {status}\n"
            f"Current Price Override: {'None' if DEBUG_PRICE is None else f'${DEBUG_PRICE:,}'}\n\n"
            f"Commands:\n"
            f"/debug - Toggle debug mode\n"
            f"/set_price <price> - Set custom BTC price\n"
            f"/clear_price - Clear price override"
        )
        await update.message.reply_text(message)
        
    except Exception as e:
        error_msg = f"❌ Error toggling debug mode: {str(e)}"
        logger.error(error_msg)
        await update.message.reply_text(error_msg)

async def handle_set_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set custom BTC price in debug mode"""
    global DEBUG_PRICE
    
    try:
        if update.effective_chat.id != int(CHAT_ID):
            await update.message.reply_text("❌ Unauthorized access")
            return
            
        if not DEBUG_MODE:
            await update.message.reply_text("❌ Debug mode is OFF. Use /debug to enable it first.")
            return
        
        # Get price from command arguments
        args = context.args
        if not args:
            await update.message.reply_text("❌ Please provide a price. Usage: /set_price <price>")
            return
            
        try:
            price = float(args[0])
            if price <= 0:
                await update.message.reply_text("❌ Price must be greater than 0")
                return
            
            DEBUG_PRICE = price
            await update.message.reply_text(f"✅ BTC price override set to: ${DEBUG_PRICE:,}")
            
        except ValueError:
            await update.message.reply_text("❌ Invalid price format. Please use a number.")
            
    except Exception as e:
        error_msg = f"❌ Error setting debug price: {str(e)}"
        logger.error(error_msg)
        await update.message.reply_text(error_msg)

async def handle_clear_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear custom BTC price in debug mode"""
    global DEBUG_PRICE
    
    try:
        if update.effective_chat.id != int(CHAT_ID):
            await update.message.reply_text("❌ Unauthorized access")
            return
            
        if not DEBUG_MODE:
            await update.message.reply_text("❌ Debug mode is OFF. Use /debug to enable it first.")
            return
        
        DEBUG_PRICE = None
        await update.message.reply_text("✅ BTC price override cleared. Using real price.")
        
    except Exception as e:
        error_msg = f"❌ Error clearing debug price: {str(e)}"
        logger.error(error_msg)
        await update.message.reply_text(error_msg)

# Modify the CryptoDataClient class to handle debug price
class CryptoDataClient:
    def __init__(self, use_binance=True):
        self.use_binance = use_binance
        if use_binance:
            self.client = get_binance_client()
        
    def get_price(self, symbol="BTCUSDT"):
        """Get current price with debug mode support"""
        global DEBUG_PRICE
        
        try:
            if DEBUG_MODE and DEBUG_PRICE is not None:
                logger.info(f"Using debug price: ${DEBUG_PRICE:,}")
                return str(int(float(DEBUG_PRICE)))
                
            if self.use_binance:
                price = self.client.get_symbol_ticker(symbol=symbol)['price']
                return str(int(float(price)))  # Convert to int for consistency
            else:
                response = requests.get('https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd')
                price = response.json()['bitcoin']['usd']
                return str(int(float(price)))
        except Exception as e:
            logger.error(f"Error getting price: {e}")
            raise

    def get_trend(self, symbol="BTCUSDT", lookback_periods=14):
        """Calculate trend based on moving averages"""
        try:
            # Get historical klines/candlestick data
            klines = self.client.get_klines(
                symbol=symbol,
                interval=self.client.KLINE_INTERVAL_1HOUR,
                limit=lookback_periods
            )
            
            # Extract closing prices
            closes = pd.Series([float(x[4]) for x in klines])
            
            # Calculate moving averages
            ma7 = closes.rolling(window=7).mean().iloc[-1]
            ma14 = closes.rolling(window=14).mean().iloc[-1]
            
            current_price = float(self.get_price(symbol))
            
            # Determine trend and score
            if current_price > ma7 and ma7 > ma14:
                trend = "Strong Uptrend"
                score = 9
            elif current_price > ma7:
                trend = "Uptrend"
                score = 7
            elif current_price < ma7 and ma7 < ma14:
                trend = "Strong Downtrend"
                score = 2
            elif current_price < ma7:
                trend = "Downtrend"
                score = 4
            else:
                trend = "Neutral"
                score = 5
                
            return trend, score
            
        except Exception as e:
            logger.error(f"Error calculating trend: {e}")
            return "Error", 5

    def get_indicators(self, symbol="BTCUSDT"):
        """Calculate technical indicators (RSI, MACD)"""
        try:
            # Get historical klines/candlestick data
            klines = self.client.get_klines(
                symbol=symbol,
                interval=self.client.KLINE_INTERVAL_1HOUR,
                limit=100  # Need more data for accurate MACD
            )
            
            # Create DataFrame
            df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore'])
            df['close'] = pd.to_numeric(df['close'])
            
            # Calculate RSI
            rsi = ta.rsi(df['close'], length=14).iloc[-1]
            
            # Calculate MACD
            macd = ta.macd(df['close'])
            macd_line = macd['MACD_12_26_9'].iloc[-1]
            signal_line = macd['MACDs_12_26_9'].iloc[-1]
            
            # Interpret RSI
            if rsi >= 70:
                rsi_interp = "Overbought"
                rsi_score = 3
            elif rsi <= 30:
                rsi_interp = "Oversold"
                rsi_score = 8
            elif rsi > 50:
                rsi_interp = "Bullish"
                rsi_score = 7
            else:
                rsi_interp = "Bearish"
                rsi_score = 4
                
            # Interpret MACD
            if macd_line > signal_line and macd_line > 0:
                macd_interp = "Strong Buy"
                macd_score = 9
            elif macd_line > signal_line:
                macd_interp = "Buy"
                macd_score = 7
            elif macd_line < signal_line and macd_line < 0:
                macd_interp = "Strong Sell"
                macd_score = 2
            elif macd_line < signal_line:
                macd_interp = "Sell"
                macd_score = 4
            else:
                macd_interp = "Neutral"
                macd_score = 5
                
            return {
                "RSI": rsi,
                "RSI_Interpretation": rsi_interp,
                "RSI_Score": rsi_score,
                "MACD": macd_line,
                "MACD_Signal": signal_line,
                "MACD_Interpretation": macd_interp,
                "MACD_Score": macd_score
            }
            
        except Exception as e:
            logger.error(f"Error calculating indicators: {e}")
            return {
                "RSI": 50,
                "RSI_Interpretation": "Error",
                "RSI_Score": 5,
                "MACD": 0,
                "MACD_Signal": 0,
                "MACD_Interpretation": "Error",
                "MACD_Score": 5
            }

    def detect_green_candle_with_rising_volume(self, symbol="BTCUSDT", interval="15m"):
        """Detect green candles with rising volume for given interval"""
        try:
            # Convert interval string to Binance format
            interval_map = {
                "15m": self.client.KLINE_INTERVAL_15MINUTE,
                "30m": self.client.KLINE_INTERVAL_30MINUTE,
                "1h": self.client.KLINE_INTERVAL_1HOUR
            }
            binance_interval = interval_map.get(interval, self.client.KLINE_INTERVAL_15MINUTE)
            
            # Get recent candles
            klines = self.client.get_klines(
                symbol=symbol,
                interval=binance_interval,
                limit=3  # We need current and previous candles
            )
            
            # Extract data
            current_candle = klines[-1]
            prev_candle = klines[-2]
            
            current_open = float(current_candle[1])
            current_close = float(current_candle[4])
            current_volume = float(current_candle[5])
            
            prev_open = float(prev_candle[1])
            prev_close = float(prev_candle[4])
            prev_volume = float(prev_candle[5])
            
            # Check conditions
            is_green = current_close > current_open
            volume_increasing = current_volume > prev_volume
            trend_continuing = current_close > prev_close
            
            # Calculate score and status
            if is_green and volume_increasing and trend_continuing:
                status = "Strong Green"
                score = 9
            elif is_green and volume_increasing:
                status = "Green with Volume"
                score = 7
            elif is_green:
                status = "Green"
                score = 6
            elif current_close < current_open and current_volume > prev_volume:
                status = "Red with Volume"
                score = 3
            else:
                status = "Red"
                score = 4
                
            return status, score
            
        except Exception as e:
            logger.error(f"Error detecting candle pattern: {e}")
            return "Error", 5

async def handle_scores(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for displaying current scores"""
    try:
        # Get current price
        crypto_client = CryptoDataClient(use_binance=True)
        current_price = int(crypto_client.get_price("BTCUSDT"))
        
        # Get current technical scores
        trend, trend_score = crypto_client.get_trend()
        indicators = crypto_client.get_indicators("BTCUSDT")
        candle_15m_status, candle_15m_score = crypto_client.detect_green_candle_with_rising_volume("BTCUSDT", interval="15m")
        candle_30m_status, candle_30m_score = crypto_client.detect_green_candle_with_rising_volume("BTCUSDT", interval="30m")
        candle_1h_status, candle_1h_score = crypto_client.detect_green_candle_with_rising_volume("BTCUSDT", interval="1h")
        
        # Calculate technical score
        technical_score, _ = calculate_weighted_technical_score(
            trend_score,
            indicators["RSI_Score"],
            indicators["MACD_Score"],
            candle_15m_score,
            candle_30m_score,
            candle_1h_score
        )
        
        # Get last GPT score and price from sheet
        rows = get_all_rows()
        gpt_score = 0
        final_score = 0
        previous_price_1h = current_price  # Default to current price
        previous_price_3h = current_price
        previous_price_6h = current_price
        
        if rows:
            # Sort rows by date in descending order
            sorted_rows = sorted(
                rows,
                key=lambda x: datetime.strptime(x.get('Date', '01/01/2000 00:00'), "%d/%m/%Y %H:%M"),
                reverse=True
            )
            
            # Get last row for GPT score
            last_row = sorted_rows[0]
            try:
                gpt_score = float(str(last_row.get('Score', '0')).strip())
                final_score = round(gpt_score * 0.25 + technical_score * 0.75, 2)
            except (ValueError, TypeError):
                logger.warning("Could not parse GPT score from last row")
                gpt_score = 0
                final_score = technical_score
            
            # Find historical prices
            now = datetime.now(israel_tz)
            found_1h = False
            found_3h = False
            found_6h = False
            
            for row in sorted_rows:
                try:
                    row_date = datetime.strptime(row.get('Date', ''), "%d/%m/%Y %H:%M")
                    row_date = israel_tz.localize(row_date)
                    time_diff = now - row_date
                    
                    # Look for 1h price
                    if not found_1h and timedelta(minutes=55) <= time_diff <= timedelta(minutes=65):
                        previous_price_1h = int(float(str(row.get('Price', '0')).replace(',', '')))
                        logger.info(f"Found 1h price from {row_date}: ${previous_price_1h:,}")
                        found_1h = True
                    
                    # Look for 3h price
                    if not found_3h and timedelta(hours=2, minutes=55) <= time_diff <= timedelta(hours=3, minutes=5):
                        previous_price_3h = int(float(str(row.get('Price', '0')).replace(',', '')))
                        logger.info(f"Found 3h price from {row_date}: ${previous_price_3h:,}")
                        found_3h = True
                    
                    # Look for 6h price
                    if not found_6h and timedelta(hours=5, minutes=55) <= time_diff <= timedelta(hours=6, minutes=5):
                        previous_price_6h = int(float(str(row.get('Price', '0')).replace(',', '')))
                        logger.info(f"Found 6h price from {row_date}: ${previous_price_6h:,}")
                        found_6h = True
                    
                    # Break if we found all prices
                    if found_1h and found_3h and found_6h:
                        break
                        
                except (ValueError, TypeError):
                    continue
            
            # Log warnings for missing data
            if not found_1h:
                logger.warning("Could not find 1h price data, using current price")
            if not found_3h:
                logger.warning("Could not find 3h price data, using current price")
            if not found_6h:
                logger.warning("Could not find 6h price data, using current price")
        
        # Calculate price changes
        def calculate_price_change(current, previous, period):
            change = current - previous
            change_percent = (change / previous) * 100
            if change > 0:
                return f"📈 +${change:,} / +{change_percent:.2f}% {period}"
            elif change < 0:
                return f"📉 ${change:,} / {change_percent:.2f}% {period}"
            return f"⚖️ No change {period}"
        
        # Generate signal based on final score
        signal = generate_signal(final_score, technical_score)
        
        # Create message
        message = (
            "📊 Current Scores:\n\n"
            f"📈 Technical: {technical_score}/10\n"
            f"🤖 GPT: {gpt_score}/10\n"
            f"🎯 Final: {final_score}/10\n"
            f"🎯 Signal: {signal}\n"
            f"💵 BTC Price: ${current_price:,}\n"
            f"  • {calculate_price_change(current_price, previous_price_1h, '1h')}\n"
            f"  • {calculate_price_change(current_price, previous_price_3h, '3h')}\n"
            f"  • {calculate_price_change(current_price, previous_price_6h, '6h')}"
        )
        
        await update.message.reply_text(message)
        
    except Exception as e:
        error_msg = f"❌ Error getting scores: {str(e)}"
        logger.error(error_msg)
        await update.message.reply_text(error_msg)

async def handle_shutdown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for shutting down the bot"""
    try:
        # Send goodbye message
        if update and update.message:
            await update.message.reply_text("👋 Goodbye! The bot is shutting down.")
        
        logger.info("Starting graceful shutdown...")
        
        # Set stop event first to prevent new tasks
        stop_event.set()
        
        # Cancel scheduler task if running
        global scheduler_task
        if scheduler_task and not scheduler_task.done():
            logger.info("Cancelling scheduler task...")
            scheduler_task.cancel()
            try:
                await scheduler_task
            except asyncio.CancelledError:
                logger.info("Scheduler task cancelled successfully")
        
        # Stop the application gracefully
        if context and context.application:
            logger.info("Stopping application...")
            await context.application.stop()
            await context.application.shutdown()
            logger.info("Application stopped successfully")
        
        # Clean exit
        logger.info("Bot shutdown completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")
        sys.exit(1)

async def handle_active_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for displaying active orders count and average price"""
    try:
        # Get current price
        crypto_client = CryptoDataClient(use_binance=True)
        current_price = int(crypto_client.get_price("BTCUSDT"))
        
        # Get Binance balances
        client = get_binance_client()
        account = client.get_account()
        
        # Get balances with non-zero amounts
        balances = [asset for asset in account['balances'] 
                   if float(asset['free']) > 0 or float(asset['locked']) > 0]
        
        # Get all rows and count active trades
        all_rows = sheet.get_all_records()
        active_trades = count_active_trades(all_rows)
        
        # Prepare balance message
        balance_message = "💰 Balances:\n"
        stablecoins = ['USDT', 'BUSD', 'USDC']
        
        # Show stablecoins first
        for asset in balances:
            if asset['asset'] in stablecoins:
                free = float(asset['free'])
                locked = float(asset['locked'])
                if locked > 0:
                    balance_message += f"  • {asset['asset']}: ${free:.2f} (Locked: ${locked:.2f})\n"
                else:
                    balance_message += f"  • {asset['asset']}: ${free:.2f}\n"
        
        # Show other assets
        for asset in balances:
            if asset['asset'] not in stablecoins:
                free = float(asset['free'])
                locked = float(asset['locked'])
                try:
                    ticker = client.get_symbol_ticker(symbol=f"{asset['asset']}USDT")
                    price = float(ticker['price'])
                    value = free * price
                    if locked > 0:
                        balance_message += f"  • {asset['asset']}: {free:.8f} ≈ ${value:.2f} (Locked: {locked:.8f})\n"
                    else:
                        balance_message += f"  • {asset['asset']}: {free:.8f} ≈ ${value:.2f}\n"
                except:
                    if locked > 0:
                        balance_message += f"  • {asset['asset']}: {free:.8f} (Locked: {locked:.8f})\n"
                    else:
                        balance_message += f"  • {asset['asset']}: {free:.8f}\n"
        
        if active_trades <= 0:
            message = f"📊 No active orders\n\n{balance_message}"
            await update.message.reply_text(message)
            return
            
        # Calculate average buy price and track open positions
        total_buy_price = 0
        orders_found = 0
        total_investment = 0
        open_positions = []  # List to track each open position
        TRADE_AMOUNT = 500  # Standard trade amount
        
        for row in reversed(all_rows):
            if row.get('action') in ['✅ MANUAL BUY', '✅ AUTO BUY']:
                buy_price = float(str(row.get('Price', '0')).replace(',', ''))
                total_buy_price += buy_price
                orders_found += 1
                total_investment += TRADE_AMOUNT
                
                # Calculate individual position P/L
                btc_amount = TRADE_AMOUNT / buy_price
                current_value = btc_amount * current_price
                position_pl = ((current_price - buy_price) / buy_price) * 100  # Percentage
                open_positions.append({
                    'buy_price': buy_price,
                    'pl_percent': position_pl
                })
                
                if orders_found == active_trades:
                    break
        
        avg_buy_price = int(total_buy_price / active_trades)
        
        # Calculate overall P/L
        total_current_value = total_investment * (current_price / avg_buy_price)
        total_pl = total_current_value - total_investment
        total_pl_percent = ((current_price - avg_buy_price) / avg_buy_price) * 100
        
        # Sort positions by P/L
        open_positions.sort(key=lambda x: x['pl_percent'])
        
        # Create message
        message = (
            f"📊 Active Orders: {active_trades}\n"
            f"💰 Average Buy Price: ${avg_buy_price:,}\n"
            f"💵 Current BTC Price: ${current_price:,}\n"
            f"📈 Overall P/L: {total_pl_percent:+.2f}% (${total_pl:+,.2f})\n\n"
            f"Open Positions:\n"
        )
        
        # Add individual position details
        for i, pos in enumerate(open_positions, 1):
            emoji = "📈" if pos['pl_percent'] >= 0 else "📉"
            message += f"{emoji} ${pos['buy_price']:,} → {pos['pl_percent']:+.2f}%\n"
        
        # Add balance information
        message += f"\n{balance_message}"
        
        await update.message.reply_text(message)
        
    except Exception as e:
        error_msg = f"❌ Error getting active orders: {str(e)}"
        logger.error(error_msg)
        await update.message.reply_text(error_msg)

async def handle_profit_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for displaying daily, weekly, and monthly net profits"""
    try:
        # Get all rows from the sheet
        all_rows = sheet.get_all_records()
        if not all_rows:
            await update.message.reply_text("❌ No trading history found")
            return

        # Convert dates to datetime objects and sort rows
        from datetime import datetime, timedelta
        now = datetime.now(israel_tz)
        
        # Initialize profit counters
        daily_profit = 0.0
        weekly_profit = 0.0
        monthly_profit = 0.0
        daily_trades = 0
        weekly_trades = 0
        monthly_trades = 0
        daily_buy_fees = 0.0
        daily_sell_fees = 0.0
        weekly_buy_fees = 0.0
        weekly_sell_fees = 0.0
        monthly_buy_fees = 0.0
        monthly_sell_fees = 0.0
        
        # Calculate time thresholds
        daily_threshold = now - timedelta(days=1)
        weekly_threshold = now - timedelta(days=7)
        monthly_threshold = now - timedelta(days=30)
        
        # Track buy orders that haven't been sold yet
        open_buy_orders = {}  # order_id -> {date, buy_fee}
        
        # First pass: Process all rows in chronological order
        sorted_rows = sorted(all_rows, 
            key=lambda x: datetime.strptime(x.get('Date', '01/01/2000 00:00'), "%d/%m/%Y %H:%M")
        )
        
        for row in sorted_rows:
            if row.get('action') in ['✅ MANUAL BUY', '✅ AUTO BUY']:
                order_id = row.get('order_id', '')
                if order_id:  # Only track if we have a valid order ID
                    buy_fee = float(str(row.get('fee', '0')).replace(',', '') or '0')
                    open_buy_orders[order_id] = {
                        'date': datetime.strptime(row.get('Date', ''), "%d/%m/%Y %H:%M"),
                        'buy_fee': buy_fee
                    }
            elif row.get('action') in ['🛑 MANUAL SELL', '🛑 AUTO SELL']:
                # Remove the corresponding buy order as it's been sold
                sell_order_id = row.get('order_id', '')
                if sell_order_id and sell_order_id.startswith('SELL-'):
                    buy_order_id = sell_order_id[5:]  # Remove 'SELL-' prefix
                    if buy_order_id in open_buy_orders:
                        del open_buy_orders[buy_order_id]
        
        # Log open orders for debugging
        logger.info(f"Found {len(open_buy_orders)} open orders:")
        for order_id, data in open_buy_orders.items():
            logger.info(f"Open order {order_id} from {data['date'].strftime('%d/%m/%Y %H:%M')}")
        
        # Double check against active trades count
        active_trades = count_active_trades(all_rows)
        if len(open_buy_orders) != active_trades:
            logger.warning(f"Mismatch between open orders ({len(open_buy_orders)}) and active trades ({active_trades})")
            # Trust the active trades count
            if active_trades < len(open_buy_orders):
                logger.info("Using active trades count instead of open orders count")
                open_buy_orders = dict(list(open_buy_orders.items())[:active_trades])
        
        # Process each row for profit calculation
        for row in all_rows:
            try:
                # Parse the date from the row and convert to Israel timezone
                trade_date = datetime.strptime(row.get('Date', ''), "%d/%m/%Y %H:%M")
                trade_date = israel_tz.localize(trade_date)
                
                # Only process sell actions
                if row.get('action') not in ['🛑 MANUAL SELL', '🛑 AUTO SELL']:
                    continue
                
                # Get profit/loss value (this is gross profit)
                profit_loss = row.get('profit/loss', '')
                if not profit_loss or profit_loss == '' or profit_loss == 0:
                    continue
                
                try:
                    gross_profit = float(str(profit_loss).replace(',', ''))
                except (ValueError, TypeError):
                    logger.warning(f"Invalid profit/loss value: {profit_loss}")
                    continue
                
                # Get sell fee
                sell_fee = row.get('fee', '')
                try:
                    if sell_fee and sell_fee != '':
                        sell_fee = float(str(sell_fee).replace(',', ''))
                    else:
                        sell_fee = 0.0
                except (ValueError, TypeError):
                    sell_fee = 0.0
                
                # Get corresponding buy fee
                buy_fee = 0.0
                sell_order_id = row.get('order_id', '')
                if sell_order_id.startswith('SELL-'):
                    buy_order_id = sell_order_id[5:]  # Remove 'SELL-' prefix
                    # Look up the buy fee from the original buy order
                    for buy_row in all_rows:
                        if buy_row.get('order_id') == buy_order_id:
                            try:
                                buy_fee = float(str(buy_row.get('fee', '0')).replace(',', '') or '0')
                            except (ValueError, TypeError):
                                buy_fee = 0.0
                            break
                
                # Calculate net profit for this trade
                net_profit = gross_profit - sell_fee - buy_fee
                
                # Add to appropriate time period
                if trade_date >= daily_threshold:
                    daily_profit += gross_profit
                    daily_sell_fees += sell_fee
                    daily_buy_fees += buy_fee
                    daily_trades += 1
                if trade_date >= weekly_threshold:
                    weekly_profit += gross_profit
                    weekly_sell_fees += sell_fee
                    weekly_buy_fees += buy_fee
                    weekly_trades += 1
                if trade_date >= monthly_threshold:
                    monthly_profit += gross_profit
                    monthly_sell_fees += sell_fee
                    monthly_buy_fees += buy_fee
                    monthly_trades += 1
                    
            except (ValueError, TypeError) as e:
                logger.warning(f"Error processing row: {e}")
                continue
        
        # Calculate net profits (after all fees)
        daily_net = round(daily_profit - daily_sell_fees - daily_buy_fees, 2)
        weekly_net = round(weekly_profit - weekly_sell_fees - weekly_buy_fees, 2)
        monthly_net = round(monthly_profit - monthly_sell_fees - monthly_buy_fees, 2)
        
        # Create profit message with emojis based on profit/loss
        def get_emoji(profit):
            if profit > 0:
                return "📈"
            elif profit < 0:
                return "📉"
            return "➖"
            
        message = (
            "💰 *Profit Summary*\n\n"
            f"*Daily* ({daily_trades} trades):\n"
            f"{get_emoji(daily_net)} ${daily_net:.2f} (net profit)\n"
            f"Gross Profit: ${daily_profit:.2f}\n"
            f"Total Fees: ${(daily_buy_fees + daily_sell_fees):.2f}\n"
            f"  • Buy Fees: ${daily_buy_fees:.2f}\n"
            f"  • Sell Fees: ${daily_sell_fees:.2f}\n\n"
            f"*Weekly* ({weekly_trades} trades):\n"
            f"{get_emoji(weekly_net)} ${weekly_net:.2f} (net profit)\n"
            f"Gross Profit: ${weekly_profit:.2f}\n"
            f"Total Fees: ${(weekly_buy_fees + weekly_sell_fees):.2f}\n"
            f"  • Buy Fees: ${weekly_buy_fees:.2f}\n"
            f"  • Sell Fees: ${weekly_sell_fees:.2f}\n\n"
            f"*Monthly* ({monthly_trades} trades):\n"
            f"{get_emoji(monthly_net)} ${monthly_net:.2f} (net profit)\n"
            f"Gross Profit: ${monthly_profit:.2f}\n"
            f"Total Fees: ${(monthly_buy_fees + monthly_sell_fees):.2f}\n"
            f"  • Buy Fees: ${monthly_buy_fees:.2f}\n"
            f"  • Sell Fees: ${monthly_sell_fees:.2f}\n\n"
            f"_Updated: {now.strftime('%d/%m/%Y %H:%M')}_"
        )
        
        # Add open orders warning if any exist
        if open_buy_orders:
            message += f"\n\n⚠️ *Open Orders*: {len(open_buy_orders)} buy orders not included in profit calculation"
        
        await update.message.reply_text(message, parse_mode='Markdown')
        
    except Exception as e:
        error_msg = f"❌ Error calculating profits: {str(e)}"
        logger.error(error_msg)
        await update.message.reply_text(error_msg)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
        sys.exit(1)
