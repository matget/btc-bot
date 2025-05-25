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
    keyboard=[["/btc", "/gpt"], ["/price"], ["/trade", "/balance"], ["/history", "/help"]],
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
        sheet = client.open_by_url(gsheet_url).sheet1
        
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

def append_row_to_sheet(row_dict):
    row = [row_dict["Date"], row_dict["Price"], row_dict["Score"]]
    row.extend([row_dict[k] for k in [
        "supply_demand", "regulation", "macro_economy", 
        "news_sentiment", "whales_activity", "tech_events", "adoption"
    ]])
    sheet.append_row(row)

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

def get_btc_price():
    """Get current Bitcoin price from CoinGecko API"""
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin",
        "vs_currencies": "usd"
    }
    response = requests.get(url, params=params)
    data = response.json()
    return data["bitcoin"]["usd"]

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


# ----------- start -----------
async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Welcome! Use the buttons below or type a command.",
        reply_markup=reply_keyboard
    )

# ----------- Push News -----------
async def push_news(chat_id):
    try:
        price = get_btc_price()
        await bot.send_message(chat_id=chat_id, text=f"Current BTC Value: ${price:,.2f}")
    except Exception as e:
        logger.error(f"❌ [push_news] FAILED: {e}")

def thread_target():
    """Target function for the scheduler thread"""
    try:
        logger.info("Starting scheduler thread")
        asyncio.run(run_scheduler())
    except Exception as e:
        logger.error(f"Scheduler thread crashed: {e}")

def scheduler_thread():
    """Start the scheduler in a separate thread"""
    thread = threading.Thread(target=thread_target, daemon=True)
    thread.start()
    return thread

async def run_scheduler():
    """Main scheduler loop"""
    logger.info("Starting GPT analysis scheduler")
    try:
        # Initial run
        await send_gpt_analysis()
        
        while True:
            try:
                current_time = datetime.now()
                
                # Calculate next 12-hour interval for GPT analysis
                hours_since_midnight = current_time.hour + current_time.minute / 60
                hours_until_next_gpt = 12 - (hours_since_midnight % 12)
                next_gpt_update = current_time + timedelta(hours=hours_until_next_gpt)
                next_gpt_update = next_gpt_update.replace(minute=0, second=0, microsecond=0)
                
                # Calculate wait time
                wait_time = (next_gpt_update - current_time).total_seconds()
                logger.info(f"Next GPT analysis scheduled for {next_gpt_update}")
                
                await asyncio.sleep(wait_time)
                await send_gpt_analysis()
                
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                # Wait a bit before retrying
                await asyncio.sleep(60)
                
    except Exception as e:
        logger.error(f"Fatal error in scheduler: {e}")
        raise

async def send_gpt_analysis():
    """Run GPT analysis and send results"""
    logger.info("🤖 Running scheduled GPT analysis")
    try:
        # Create a mock update object for handle_gpt_matget
        class MockMessage:
            async def reply_text(self, text, **kwargs):
                await bot.send_message(chat_id=CHAT_ID, text=text, **kwargs)
        
        class MockUpdate:
            def __init__(self):
                self.message = MockMessage()
                self.effective_chat = type('obj', (object,), {'id': CHAT_ID})
        
        mock_update = MockUpdate()
        await handle_gpt_matget(mock_update, None)
    except Exception as e:
        logger.error(f"Failed to run scheduled GPT analysis: {e}")
        raise

# ----------- GPT -----------
async def handle_gpt_matget(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        openai_key = config.get("OPENAI_API_KEY")
        if not openai_key:
            await update.message.reply_text("⚠️ OpenAI API key not configured")
            return

        client = OpenAI(api_key=openai_key)
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a Bitcoin market analyst."},
                {"role": "user", "content": "Rate Bitcoin's current market factors (1-10):\n- supply_demand (0.25): volume, inflows\n- regulation (0.20): policy changes\n- macro_economy (0.20): rates, inflation\n- news_sentiment (0.10): media mood\n- whales_activity (0.10): large moves\n- tech_events (0.075): upgrades\n- adoption (0.075): institutional\n\nRespond only in format:\nsupply_demand: X\nregulation: X\nmacro_economy: X\nnews_sentiment: X\nwhales_activity: X\ntech_events: X\nadoption: X\nscore_weighted: X"}
            ]
        )
        
        answer = response.choices[0].message.content
        await update.message.reply_text(f"🤖 Analysis:\n{answer}")
        
        # Parse GPT's response and save to Google Sheets
        pattern = r"(supply_demand|regulation|macro_economy|news_sentiment|whales_activity|tech_events|adoption|score_weighted):\s*([\d.]+)"
        matches = re.findall(pattern, answer)
        if len(matches) < 8:
            await update.message.reply_text("❌ GPT response missing fields. Data not saved.")
            return

        scores = {key: float(value) for key, value in matches}
        now = datetime.now().strftime("%d/%m/%Y %H:%M")
        price = get_btc_price()
        
        row = {
            "Date": now,
            "Price": price,
            "Score": scores["score_weighted"],
            "supply_demand": scores["supply_demand"],
            "regulation": scores["regulation"],
            "macro_economy": scores["macro_economy"],
            "news_sentiment": scores["news_sentiment"],
            "whales_activity": scores["whales_activity"],
            "tech_events": scores["tech_events"],
            "adoption": scores["adoption"]
        }
        
        try:
            append_row_to_sheet(row)
            await update.message.reply_text("✅ Analysis saved to Google Sheets successfully!")
        except Exception as e:
            await update.message.reply_text(f"❌ Error saving to Google Sheets:\n{e}")
        
    except Exception as e:
        error_msg = f"❌ Error: {str(e)}"
        logger.error(error_msg)	
        await update.message.reply_text(error_msg)

# ----------- price -----------
async def handle_price_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for manual price check command"""
    try:
        price = get_btc_price()
        await update.message.reply_text(f"💰 Current BTC Price: ${price:,.2f}")
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
        "/help – Show this help message.\n\n"
        "Good luck and enjoy the data! 🚀"
    )
    await update.message.reply_text(help_text, reply_markup=reply_keyboard)
    
# ----------------------------------------------------------------------------------------  
async def main():
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    logger.info(f"{now}: Main On")
    
    # Start scheduler in a separate thread
    scheduler = threading.Thread(target=run_scheduler, daemon=True)
    scheduler.start()
    
    # Start bot listener
    try:
        logger.info(f"{now}: 🟢 Starting Telegram bot listener...")
        
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
        logger.info(f"{now}: 📡 Bot is listening...")
        
        # Start the bot
        await application.initialize()
        await application.start()
        await application.updater.start_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True
        )
        
        logger.info("Bot is now polling for updates...")
        
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
    """Handler for checking account balance"""
    try:
        await update.message.reply_text("🔄 Checking your Binance balance...")
        
        # Get client instance
        client = get_binance_client()
        
        # Get account info with proper timestamp
        account = client.get_account()
        
        # Filter non-zero balances
        balances = [asset for asset in account['balances'] 
                   if float(asset['free']) > 0 or float(asset['locked']) > 0]
        
        if not balances:
            await update.message.reply_text("No assets found with non-zero balance.")
            return
            
        # Sort balances by value (USDT equivalent)
        message = "💰 Your Balances:\n\n"
        
        # First show stablecoins
        stablecoins = ['USDT', 'BUSD', 'USDC']
        for asset in balances:
            if asset['asset'] in stablecoins:
                free = float(asset['free'])
                locked = float(asset['locked'])
                message += f"{asset['asset']}: ${free:.2f} (Locked: ${locked:.2f})\n"
        
        # Then show other assets
        for asset in balances:
            if asset['asset'] not in stablecoins:
                free = float(asset['free'])
                locked = float(asset['locked'])
                
                # Try to get current price in USDT
                try:
                    ticker = client.get_symbol_ticker(symbol=f"{asset['asset']}USDT")
                    price = float(ticker['price'])
                    value = free * price
                    message += f"{asset['asset']}: {free:.8f} ≈ ${value:.2f} (Locked: {locked:.8f})\n"
                except:
                    message += f"{asset['asset']}: {free:.8f} (Locked: {locked:.8f})\n"
        
        await update.message.reply_text(message)
        
    except Exception as e:
        error_msg = f"❌ Error getting balance: {str(e)}"
        logger.error(error_msg)
        await update.message.reply_text(error_msg)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
        raise
