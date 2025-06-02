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
from openai import OpenAI
from binance.client import Client
from binance.exceptions import BinanceAPIException
import time
import pandas as pd
import pandas_ta as ta
from crypto_utils import CryptoDataClient
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

reply_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        ["/btc", "/gpt"], 
        ["/price", "/balance"],
        ["/manual_buy", "/manual_sell"],
        ["/history", "/help"]
    ],
    resize_keyboard=True,
    one_time_keyboard=False
)

def load_config():
    """Load configuration from options.json file"""
    try:
        # First try loading from Home Assistant path
        if os.path.exists(OPTIONS_FILE):
            with open(OPTIONS_FILE) as f:
                options = json.load(f)
        else:
            # For development, create a sample config
            logger.warning("No options.json found, using sample configuration")
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
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")
            
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
            "signal", "action", "balance", "profit/loss", "buy_sell_price", "order_id"
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
        
        # Now update GUI sheet with selected columns
        gui_sheet = sheets_client.open_by_url(GSHEET_URL).worksheet("GUI")
        
        # Define GUI sheet columns
        gui_columns = [
            "Date", "Price", "GPT_Score", "technical_score", 
            "final_score", "action", "balance", "profit/loss", 
            "buy_sell_price", "order_id"
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
            row_dict.get("profit/loss", ""),
            row_dict.get("buy_sell_price", ""),
            row_dict.get("order_id", "")
        ]
        
        # Append to GUI sheet
        gui_sheet.append_row(gui_row)
        logger.info("Successfully updated GUI sheet")
        
    except Exception as e:
        error_msg = f"Error updating sheets: {str(e)}"
        logger.error(error_msg)
        raise

# Replace the old append_row_to_sheet function with update_sheets
def append_row_to_sheet(row_dict):
    update_sheets(row_dict)

bot = Bot(token=TOKEN)

hebrew_labels = {
    "supply_demand": "ביקוש/היצע",
    "regulation": "רגולציה",
    "macro_economy": "מצב כלכלה עולמית",
    "news_sentiment": "חדשות/פסיכולוגיה",
    "whales_activity": "פעילות לוויתנים",
    "tech_events": "אירועים טכנולוגיים",
    "adoption": "שימוש/אימוץ עולמי"
}


def interpret_score(score):
    if score >= 8:
        return "📈 התחזית חיובית מאוד – השוק מראה סימני עלייה חזקים."
    elif score >= 6.5:
        return "🙂 מגמה חיובית מתונה – שוק יציב עם נטייה לעלייה."
    elif score >= 5:
        return "😐 המצב נייטרלי – אין מגמה ברורה, יש לעקוב."
    elif score >= 3.5:
        return "⚠️ מגמה שלילית – יש סימנים ללחץ בשוק."
    else:
        return "🔻 תחזית שלילית מאוד – סיכון מוגבר ונטייה לירידות."

def generate_history_plot():
    dates, prices, scores = [], [], []
    try:
        rows = get_all_rows()  # קריאה מהגיליון
    except Exception as e:
        logger.error(f"Error fetching data from Google Sheets: {e}")
        return
    for row in rows:
        dates.append(row["Date"])
        try:
            prices.append(float(str(row["Price"]).replace(',', '').strip()))
        except:
            prices.append(0)
        try:
            scores.append(float(row["Score"]))
        except:
            scores.append(0)
    if not prices or not scores:
        return

    min_price = min(prices)
    max_price = max(prices)
    price_range = max_price - min_price
    fig, ax1 = plt.subplots()
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Price (USD)', color='tab:blue')
    ax1.plot(dates, prices, color='tab:blue', marker='o')
    ax1.tick_params(axis='y', labelcolor='tab:blue')
    if price_range == 0:
        ax1.set_ylim(min_price - 1, max_price + 1)
    else:
        ax1.set_ylim(min_price - price_range * 0.1, max_price + price_range * 0.1)
    plt.xticks(rotation=45)
    ax2 = ax1.twinx()
    ax2.set_ylabel('Impact Score', color='tab:green')
    ax2.plot(dates, scores, color='tab:green', marker='x')
    ax2.tick_params(axis='y', labelcolor='tab:green')
    fig.tight_layout()
    plt.title("BTC Price & Impact Score History")
    plt.savefig("btc_update.png", bbox_inches='tight')
    plt.close()

# ----------- btc -----------
async def handle_btc_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🚀 מעדכן נתוני ביטקוין...")
    await send_update_to(update.effective_chat.id)
async def send_update_to(chat_id):
    try:
        rows = get_all_rows()
        if not rows:
            await bot.send_message(chat_id=chat_id, text="⚠️ אין נתונים בגיליון Google Sheets.")
            return
        last = rows[-1]
    except Exception as e:
        await bot.send_message(chat_id=chat_id, text=f"⚠️ שגיאה בגישה ל-Google Sheets: {e}")
        return

    now = last.get("Date", "לא ידוע")
    try:
        price = float(str(last["Price"]).replace(',', '').strip())
    except:
        price = 0
    try:
        score = float(str(last["Score"]).strip())
    except:
        score = 0
    categories = {}
    for k in [
        "supply_demand", "regulation", "macro_economy",
        "news_sentiment", "whales_activity", "tech_events", "adoption"
    ]:
        try:
            categories[k] = float(str(last.get(k, 0)).strip())
        except:
            categories[k] = 0

    message = f"*עדכון ביטקוין יומי* - {now}\n\n"
    message += f"*מחיר נוכחי:* ${price:,.0f}\n"
    message += f"*ציון השפעה משוקלל:* {score}/10\n\n"
    for k, v in categories.items():
        hebrew = hebrew_labels.get(k, k)
        message += f"- {hebrew}: {v}/10\n"
    summary = interpret_score(score)
    message += f"\n\n*סיכום:* {summary}"

    generate_history_plot()
    await bot.send_message(chat_id=chat_id, text=message, parse_mode='Markdown')
    try:
        with open("btc_update.png", "rb") as f:
            await bot.send_photo(chat_id=chat_id, photo=f)
    except FileNotFoundError:
        await bot.send_message(chat_id=chat_id, text="⚠️ לא ניתן להציג גרף (btc_update.png לא נמצא)")

# ----------- history -----------
async def handle_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        rows = get_all_rows()
        if not rows:
            await update.message.reply_text("⚠️ No data found in Google Sheets.")
            return
        text = "📊 *Historical Impact Scores:*\n\n"
        for row in rows[-5:]:  # 5 השורות האחרונות
            text += f"{row['Date']}\n"
            text += f"Price: ${row['Price']}, Score: {row['Score']}/10\n"
            text += f" - supply_demand: {row['supply_demand']}\n"
            text += f" - regulation: {row['regulation']}\n"
            text += f" - macro_economy: {row['macro_economy']}\n"
            text += f" - news_sentiment: {row['news_sentiment']}\n"
            text += f" - whales_activity: {row['whales_activity']}\n"
            text += f" - tech_events: {row['tech_events']}\n"
            text += f" - adoption: {row['adoption']}\n\n"
        await update.message.reply_text(text[:4090], parse_mode=None)
    except Exception as e:
        await update.message.reply_text(f"❌ Error accessing Google Sheets: {e}")

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

async def handle_gpt_matget(update: Update, context: ContextTypes.DEFAULT_TYPE, force_upload=False, is_profit_sell=False, specific_buy_price=None, is_gpt_update=False, is_technical_check=False, last_gpt_scores=None):
    try:
        openai_key = config.get("OPENAI_API_KEY")
        if not openai_key and is_gpt_update:
            if update.effective_chat and update.effective_chat.id:
                await update.message.reply_text("⚠️ OpenAI API key not configured")
            return

        # Get current balance and trades info first
        all_rows = sheet.get_all_records()
        active_trades = count_active_trades(all_rows)
        
        if not all_rows:
            current_balance = get_binance_usdt_balance()
            logger.info(f"Initializing balance from Binance: ${current_balance:.1f}")
        else:
            last_row = all_rows[-1]
            current_balance = float(str(last_row.get('balance', get_binance_usdt_balance())).replace(',', ''))
            
            # Check if we've already updated in the last minute
            last_update_time = datetime.strptime(last_row.get('Date', '01/01/2000 00:00'), "%d/%m/%Y %H:%M")
            time_since_last_update = (datetime.now() - last_update_time).total_seconds()
            
            if time_since_last_update < 60 and not force_upload and not is_technical_check:
                logger.info(f"Skipping update - Last update was {time_since_last_update:.0f} seconds ago")
                return
            
            logger.info(f"Using balance from sheet: ${current_balance:.1f}")

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
                gpt_score = last_gpt_scores["gpt_score"]
                scores = {
                    "supply_demand": last_gpt_scores["supply_demand"],
                    "regulation": last_gpt_scores["regulation"],
                    "macro_economy": last_gpt_scores["macro_economy"],
                    "news_sentiment": last_gpt_scores["news_sentiment"],
                    "whales_activity": last_gpt_scores["whales_activity"],
                    "tech_events": last_gpt_scores["tech_events"],
                    "adoption": last_gpt_scores["adoption"]
                }
            else:
                gpt_score = technical_score
                scores = {
                    "supply_demand": 0,
                    "regulation": 0,
                    "macro_economy": 0,
                    "news_sentiment": 0,
                    "whales_activity": 0,
                    "tech_events": 0,
                    "adoption": 0
                }
        else:
            # Only do GPT analysis for regular 15-minute updates
            client = OpenAI(api_key=openai_key)
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a Bitcoin market analyst."},
                    {"role": "user", "content": "Rate Bitcoin's current market factors (1-10):\n- supply_demand (0.25): volume, inflows\n- regulation (0.20): policy changes\n- macro_economy (0.20): rates, inflation\n- news_sentiment (0.10): media mood\n- whales_activity (0.10): large moves\n- tech_events (0.075): upgrades\n- adoption (0.075): institutional\n\nRespond only in format:\nsupply_demand: X\nregulation: X\nmacro_economy: X\nnews_sentiment: X\nwhales_activity: X\ntech_events: X\nadoption: X\nscore_weighted: X"}
                ]
            )
            
            answer = response.choices[0].message.content
            
            # Only send GPT analysis if this is a manual request
            if update.effective_chat and update.effective_chat.id:
                await update.message.reply_text(f"🤖 Analysis:\n{answer}")
            
            # Parse GPT's response
            pattern = r"(supply_demand|regulation|macro_economy|news_sentiment|whales_activity|tech_events|adoption|score_weighted):\s*([\d.]+)"
            matches = re.findall(pattern, answer)
            if len(matches) < 8:
                if update.effective_chat and update.effective_chat.id:
                    await update.message.reply_text("❌ GPT response missing fields. Data not saved.")
                return

            scores = {key: float(value) for key, value in matches}
            gpt_score = scores["score_weighted"]
        
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
                        buy_fee = round(TRADE_AMOUNT * 0.001, 1)
                        sell_fee = round(current_value * 0.001, 1)
                        net_profit = round(gross_profit - sell_fee, 1)
                        profit_loss = net_profit
                        current_balance = round(current_balance + TRADE_AMOUNT + gross_profit - sell_fee, 1)
                        buy_price = last_buy_price
                        order_id = f"SELL-{buy_order_id}"  # Reference the buy order in sell order ID
                        should_upload = True
                        logger.info(f"Profit-based sell: Balance updated to ${current_balance:.1f} (Gross P/L: ${gross_profit:.1f}, Fees: Buy=${buy_fee:.1f}, Sell=${sell_fee:.1f}, Net P/L: ${net_profit:.1f}, Order ID: {order_id})")
                        break
        
        # Check for signal-based trades if no profit sell
        if trading_action == "⚖️ HOLD" and not is_profit_sell:
            if signal == "✅ STRONG BUY" or signal == "✅ BUY":
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
                    trading_action = "❌ NO BALANCE"
                    logger.warning(f"No balance for buy: ${current_balance:.1f}")
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
        if trading_action in ["✅ AUTO BUY", "🛑 AUTO SELL"] and update.effective_chat and update.effective_chat.id:
            notification_msg = (
                f"🚨 TRADING ACTION EXECUTED!\n\n"
                f"💵 Price: ${price:,}\n"
                f"💰 Balance: ${current_balance:.1f}\n"
                f"📊 Technical Analysis:\n"
                f"- Overall Score: {technical_score}/10 ({technical_label})\n"
                f"- Trend: {trend} (Score: {trend_score})\n"
                f"- RSI: {indicators['RSI_Interpretation']} (Score: {indicators['RSI_Score']})\n"
                f"- MACD: {indicators['MACD_Interpretation']} (Score: {indicators['MACD_Score']})\n"
                f"- 15m Candle: {candle_15m_status} (Score: {candle_15m_score})\n"
                f"- 30m Candle: {candle_30m_status} (Score: {candle_30m_score})\n"
                f"- 1h Candle: {candle_1h_status} (Score: {candle_1h_score})\n\n"
            )
            
            if not is_profit_sell and not is_technical_check:
                notification_msg += (
                    f"🤖 GPT Analysis:\n"
                    f"- Supply/Demand: {scores['supply_demand']}/10\n"
                    f"- Regulation: {scores['regulation']}/10\n"
                    f"- Macro Economy: {scores['macro_economy']}/10\n"
                    f"- News Sentiment: {scores['news_sentiment']}/10\n"
                    f"- Whales Activity: {scores['whales_activity']}/10\n"
                    f"- Tech Events: {scores['tech_events']}/10\n"
                    f"- Adoption: {scores['adoption']}/10\n"
                    f"- GPT Score: {gpt_score}/10\n\n"
                )
            
            notification_msg += (
                f"🎯 Final Score: {final_score}/10\n"
                f"📍 Signal: {signal}\n"
                f"🔄 Action: {trading_action}\n"
                f"🔑 Order ID: {order_id}\n"
            )
            
            if trading_action == "🛑 AUTO SELL" and profit_loss:
                notification_msg += f"💵 Profit/Loss: ${profit_loss:.1f} (on ${TRADE_AMOUNT:.1f} trade)\n"
                notification_msg += f"Buy Price: ${buy_price:,}"
            elif trading_action == "✅ AUTO BUY":
                notification_msg += f"💵 Investment Amount: ${TRADE_AMOUNT:.1f}\n"
                notification_msg += f"Buy Price: ${buy_price:,}"
            
            await bot.send_message(chat_id=CHAT_ID, text=notification_msg)
        
        # Only upload data if it's a scheduled update or there was a trade
        if should_upload or trading_action in ["✅ AUTO BUY", "🛑 AUTO SELL"]:
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
                "action": trading_action,
                "balance": current_balance,
                "profit/loss": profit_loss,
                "buy_sell_price": buy_price if trading_action == "✅ AUTO BUY" else price if trading_action == "🛑 AUTO SELL" else "",
                "order_id": order_id
            }
            
            try:
                append_row_to_sheet(row)
                if update.effective_chat and update.effective_chat.id:
                    await update.message.reply_text("✅ Analysis saved to Google Sheets successfully!")
            except Exception as e:
                if update.effective_chat and update.effective_chat.id:
                    await update.message.reply_text(f"❌ Error saving to Google Sheets:\n{e}")
        
    except Exception as e:
        error_msg = f"❌ Error: {str(e)}"
        logger.error(error_msg)
        if update.effective_chat and update.effective_chat.id:
            await update.message.reply_text(error_msg)

# ----------- price -----------
async def handle_price_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for manual price check command"""
    try:
        price = int(client.get_price())  # Convert price to integer
        await update.message.reply_text(f"💰 Current BTC Price: ${price:,}")  # Display price as integer with comma separator
    except Exception as e:
        error_msg = f"❌ Error getting BTC price: {str(e)}"
        logger.error(error_msg)
        await update.message.reply_text(error_msg)

# ----------- help -----------
async def handle_help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "🤖 *Bitcoin GPT Bot – Commands Overview*\n\n"
        "Available commands:\n"
        "/btc – BTC Status.\n"
        "/gpt – Get GPT analysis.\n"
        "/price – Get current BTC price.\n"
        "/trade – Test Binance connection.\n"
        "/balance – Check your balances.\n"
        "/history – Present full csv.\n"
        "/manual_buy – Place manual buy order.\n"
        "/manual_sell – Place manual sell order.\n"
        "/help – Show this help message.\n\n"
        "Debug Commands:\n"
        "/debug – Toggle debug mode.\n"
        "/set_price <price> – Set custom BTC price.\n"
        "/clear_price – Clear price override.\n\n"
        "Good luck and enjoy the data! 🚀"
    )
    await update.message.reply_text(help_text, reply_markup=reply_keyboard)
    
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

def initialize_trade_status():
    """Initialize trade status by checking sheet history"""
    try:
        all_rows = sheet.get_all_records()
        active_trades = count_active_trades(all_rows)
        last_action = ""
        last_price = 0
        
        if all_rows:
            for row in reversed(all_rows):
                if row.get('action') in ['✅ MANUAL BUY', '✅ AUTO BUY', '🛑 MANUAL SELL', '🛑 AUTO SELL']:
                    last_action = row.get('action')
                    last_price = float(str(row.get('Price', '0')).replace(',', ''))
                    break
        
        logger.info(f"Initializing trade status - Active trades: {active_trades}")
        if active_trades > 0:
            logger.info(f"Found active trades - Last action: {last_action}, Last price: ${last_price:,}")
        return active_trades
    except Exception as e:
        logger.error(f"Error initializing trade status: {e}")
        return 0

async def schedule_gpt_updates():
    """Schedule automatic GPT updates with dynamic intervals based on active trades"""
    global wait_event
    
    try:
        # Initialize trade status at startup
        active_trades = initialize_trade_status()
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

async def main():
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    logger.info(f"{now}: Main On")
    
    try:
        # Check if sheet is empty and initialize if needed
        all_rows = sheet.get_all_records()
        if not all_rows:
            logger.info("First time running - Initializing sheet with first data entry")
            # Create mock update for initial data collection
            class MockMessage:
                def __init__(self):
                    self.chat_id = None  # No notifications for initial setup
                    self.id = 0
                    self.date = datetime.now()
                    self.text = ""
                    
                async def reply_text(self, text, *args, **kwargs):
                    pass
            
            class MockChat:
                def __init__(self):
                    self.id = None
                    self.type = "private"
                    self.username = "InitialSetup"
                    self.first_name = "Initial"
                    self.last_name = "Setup"
            
            class MockUpdate:
                def __init__(self):
                    self.message = MockMessage()
                    self.effective_chat = MockChat()
                    self.effective_message = self.message
                    self.effective_user = None
                    self.update_id = 0
            
            mock_update = MockUpdate()
            
            # Force initial data collection and upload
            await handle_gpt_matget(mock_update, None, force_upload=True, is_profit_sell=False)
            logger.info("Sheet initialized with first data entry")
        
        logger.info(f"{now}: 🟢 Starting Telegram bot listener...")
        
        # Initialize trade status at startup
        active_trades = initialize_trade_status()
        logger.info(f"Initial trade status - Active trades: {active_trades}")
        
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
        application.add_handler(CommandHandler("btc", handle_btc_command))
        application.add_handler(CommandHandler("help", handle_help_command))
        application.add_handler(CommandHandler("history", handle_history))
        application.add_handler(CommandHandler("gpt", handle_gpt_matget))
        application.add_handler(CommandHandler("price", handle_price_command))
        application.add_handler(CommandHandler("trade", handle_trade_command))
        application.add_handler(CommandHandler("balance", handle_balance_command))
        application.add_handler(CommandHandler("manual_buy", handle_manual_buy))
        application.add_handler(CommandHandler("manual_sell", handle_manual_sell))
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
        
        # Keep the bot running
        stop_event = asyncio.Event()
        
        def stop_bot():
            logger.info("Stop signal received")
            stop_event.set()
        
        # Handle stop signals
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda s, f: stop_bot())
        
        try:
            await stop_event.wait()
        finally:
            # Cleanup
            logger.info("Stopping bot...")
            if scheduler_task:
                scheduler_task.cancel()  # Cancel the scheduler task
                try:
                    await scheduler_task  # Wait for the task to be cancelled
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
        
        # Get current balance from sheet
        all_rows = sheet.get_all_records()
        active_trades = count_active_trades(all_rows)
        if not all_rows:
            current_balance = get_binance_usdt_balance()
        else:
            last_row = all_rows[-1]
            current_balance = float(str(last_row.get('balance', get_binance_usdt_balance())).replace(',', ''))
        current_balance = round(current_balance, 1)
        
        TRADE_AMOUNT = 500  # Standard trade amount
        now = datetime.now().strftime("%d/%m/%Y %H:%M")
        
        # Generate current signal based on scores
        signal = generate_signal(technical_score, technical_score)  # Using technical score for both since it's manual
        trading_action = "✅ MANUAL BUY"  # Default action for manual buy
        
        if current_balance >= TRADE_AMOUNT:
            trading_action = "✅ MANUAL BUY"
            buy_fee = TRADE_AMOUNT * 0.001  # Calculate buy fee
            current_balance = round(current_balance - TRADE_AMOUNT, 1)  # Only deduct investment amount
            buy_price = price  # Set buy price for manual buy
            profit_loss = -buy_fee  # Show buy fee as initial loss
            logger.info(f"Manual buy order: Balance updated to ${current_balance:.1f}, Fee: ${buy_fee:.2f}")
        else:
            trading_action = "❌ NO BALANCE"
            logger.warning(f"No balance for buy: ${current_balance:.1f}")
            buy_price = ""
            profit_loss = ""
        
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
            "profit/loss": profit_loss,
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
        
        # Check for active trades
        all_rows = sheet.get_all_records()
        active_trades = count_active_trades(all_rows)
        
        # Log current trade status before sell
        logger.info(f"Current trade status before sell - Active trades: {active_trades}")
        
        now = datetime.now().strftime("%d/%m/%Y %H:%M")
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
            "profit/loss": profit_loss,
            "buy_sell_price": price,  # Use current price for sell action
            "order_id": ""  # Assuming a default order_id
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

if __name__ == "__main__":
    client = CryptoDataClient(use_binance=True)  # or False for CoinGecko
        
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
        raise
