import asyncio
import json
import re
import threading
import time
from datetime import datetime, timedelta
import requests
import schedule
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
    keyboard=[["/btc", "/csv"], ["/update", "/gptnews"], ["/history", "/help"]],
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
                "JSON_KEYS": os.getenv("JSON_KEYS")
            }
        
        # Validate required fields
        required_fields = ["TOKEN", "CHAT_ID", "GSHEET_URL", "JSON_KEYS"]
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

# Setup Google Sheets
sheet = setup_google_sheets(config["JSON_KEYS"], GSHEET_URL)

# Add sensitive data filter to logger
logger.addFilter(SensitiveDataFilter([TOKEN]))

GSHEET_CREDENTIALS = CREDENTIALS_FILE
CSV_WAITING = 1

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
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
    response = requests.get(url).json()
    return response['bitcoin']['usd']

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

def get_btc_price():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin",
        "vs_currencies": "usd"
    }
    response = requests.get(url, params=params)
    data = response.json()
    return data["bitcoin"]["usd"]
    
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
        rows = get_all_rows()  # קריאה מהגיליון במקום CSV
        if not rows:
            await bot.send_message(chat_id=chat_id, text="⚠️ אין נתונים בגיליון Google Sheets.")
            return
        last = rows[-1]  # השורה האחרונה
    except Exception as e:
        await bot.send_message(chat_id=chat_id, text=f"⚠️ שגיאה בגישה ל-Google Sheets: {e}")
        return
    # חילוץ נתונים
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
    # ניסוח הודעה
    message = f"*עדכון ביטקוין יומי* - {now}\n\n"
    message += f"*מחיר נוכחי:* ${price:,.0f}\n"
    message += f"*ציון השפעה משוקלל:* {score}/10\n\n"
    for k, v in categories.items():
        hebrew = hebrew_labels.get(k, k)
        message += f"- {hebrew}: {v}/10\n"
    summary = interpret_score(score)
    message += f"\n\n*סיכום:* {summary}"
    # גרף היסטוריה
    generate_history_plot()
    await bot.send_message(chat_id=chat_id, text=message, parse_mode='Markdown')
    try:
        with open("btc_update.png", "rb") as f:
            await bot.send_photo(chat_id=chat_id, photo=f)
    except FileNotFoundError:
        await bot.send_message(chat_id=chat_id, text="⚠️ לא ניתן להציג גרף (btc_update.png לא נמצא)")

# ----------- update -----------
async def handle_update_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    prompt = (
        "Please analyze the current global situation of Bitcoin and rate the following 7 categories on a scale of 1 to 10,\n"
        "based on their current influence on the Bitcoin price **today**:\n\n"
        "Categories:\n"
        "- supply_demand (supply, demand, trading volume, exchange inflows)\n"
        "- regulation (new regulations, government or institutional policy changes)\n"
        "- macro_economy (interest rates, inflation, Fed decisions, economic news)\n"
        "- news_sentiment (media/public mood, Twitter trends, Google Trends)\n"
        "- whales_activity (large BTC wallet movements)\n"
        "- tech_events (Bitcoin Core upgrades, protocol changes)\n"
        "- adoption (corporate/governmental adoption, new integrations)\n\n"
        "please learn the weights of each category: supply_demand: 0.25, regulation: 0.20, macro_economy: 0.20, news_sentiment: 0.10, whales_activity: 0.10, tech_events: 0.075, adoption: 0.075\n"
        "after that, please make text box (for easy to copy) as following pattern and replace the <score> with the values you calculated:\n\n"
        "supply_demand: <score>\n"
        "regulation: <score>\n"
        "macro_economy: <score>\n"
        "news_sentiment: <score>\n"
        "whales_activity: <score>\n"
        "tech_events: <score>\n"
        "adoption: <score>\n"
        "score_weighted: <final_score>\n"
        "PLEASE! just send me test text box! without any other text/details. thanks."
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 פתח את ChatGPT", url="https://chat.openai.com/")]
    ])
    await update.message.reply_text(
        text=f"{prompt}",
        reply_markup=keyboard
    )

# ----------- csv -----------
async def start_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📝 Please paste the GPT output in the expected format (category scores + score_weighted):")
    return CSV_WAITING
async def receive_csv_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    pattern = r"(supply_demand|regulation|macro_economy|news_sentiment|whales_activity|tech_events|adoption|score_weighted):\s*([\d.]+)"
    matches = re.findall(pattern, text)
    if len(matches) < 8:
        await update.message.reply_text("❌ Missing fields. Please make sure all 7 categories and score_weighted are included.")
        return ConversationHandler.END
    try:
        scores = {key: float(value) for key, value in matches}
    except ValueError:
        await update.message.reply_text("⚠️ All values must be numeric.")
        return ConversationHandler.END
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
        append_row_to_sheet(row)  # כתיבה לגוגל שיטס במקום לקובץ
        await update.message.reply_text("✅ הנתונים נרשמו בהצלחה בגיליון!")
    except Exception as e:
        await update.message.reply_text(f"❌ שגיאה בשמירה ל-Google Sheets:\n{e}")
    return ConversationHandler.END
csv_conv_handler = ConversationHandler(
    entry_points=[CommandHandler("csv", start_csv)],
    states={CSV_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_csv_data)]},
    fallbacks=[],
)

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

# ----------- reminder -----------
async def push_reminder(chat_id):
    logger.info("🟢 [push_reminder] Started")
    try:
        await bot.send_message(chat_id=chat_id, text="🕘 Reminder: Don't forget to update today's Bitcoin data using /update → GPT → /csv")
        logger.info("✅ [push_reminder] Sent to Telegram")
    except Exception as e:
        logger.error(f"❌ [push_reminder] FAILED: {e}")

# ----------- Push News -----------

async def push_news(chat_id):
    logger.info("🟢 [push_news] Started")
    try:
        price = get_btc_price()
        logger.info(f"📈 BTC price: {price}")
        await bot.send_message(chat_id=chat_id, text=f"Current BTC Value: ${price:,.2f}")
        logger.info("✅ [push_news] Sent to Telegram")
    except Exception as e:
        logger.error(f"❌ [push_news] FAILED: {e}")

def scheduler_thread():
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def send_news():
            logger.info("📤 Sending scheduled BTC update")
            await push_news(CHAT_ID)

        async def send_reminder():
            logger.info("🔔 Sending daily reminder")
            await push_reminder(CHAT_ID)

        async def run_scheduler():
            try:
                # Send first update immediately on startup
                logger.info("🚀 Sending initial update on startup")
                await send_news()
                
                while True:
                    current_time = datetime.now()
                    
                    # Calculate next 12:00 AM
                    next_reminder = current_time.replace(hour=12, minute=0, second=0, microsecond=0)
                    if current_time >= next_reminder:
                        next_reminder += timedelta(days=1)
                    
                    # Calculate next 2-hour interval
                    hours_since_midnight = current_time.hour + current_time.minute / 60
                    hours_until_next = 2 - (hours_since_midnight % 2)
                    next_update = current_time + timedelta(hours=hours_until_next)
                    next_update = next_update.replace(minute=0, second=0, microsecond=0)
                    
                    # Determine which event is next
                    time_to_reminder = (next_reminder - current_time).total_seconds()
                    time_to_update = (next_update - current_time).total_seconds()
                    
                    if time_to_update <= time_to_reminder:
                        # Wait until next update time
                        await asyncio.sleep(time_to_update)
                        await send_news()
                    else:
                        # Wait until reminder time
                        await asyncio.sleep(time_to_reminder)
                        await send_reminder()
            except Exception as e:
                logger.error(f"❌ Scheduler loop failed: {e}")
                raise

        # Start the scheduler
        logger.info("📅 Scheduler started: initial update now, then reminder at 12:00, news every 2h")
        loop.run_until_complete(run_scheduler())
    except Exception as e:
        logger.error(f"❌ Scheduler thread crashed: {e}")
        raise  # Re-raise the exception to ensure it's logged properly

        
# ----------- help -----------
async def handle_help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "🤖 *Bitcoin GPT Bot – Commands Overview*\n\n"
        "Available commands:\n"
        "/btc – BTC Status.\n"
        "/update – Get GPT prompt for scores.\n"
        "/csv – Update CSV file.\n"
        "/gptnews – GPT prompt for BTC news\n"
        "/history – Present full csv.\n"
        "/help – Show this help message.\n\n"
        "You should do the following on daily basis:\n"
        "/update → copy to GPT → copy from GPT → /csv → paste\n\n"
        "Good luck and enjoy the data! 🚀"
    )
    await update.message.reply_text(help_text, reply_markup=reply_keyboard)
    
# ----------------------------------------------------------------------------------------  
async def main():
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    logger.info(f"{now}: Main On")
    
    # Start scheduler in a separate thread
    scheduler = threading.Thread(target=scheduler_thread, daemon=True)
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
        application.add_handler(CommandHandler("update", handle_update_prompt))
        application.add_handler(CommandHandler("help", handle_help_command))
        application.add_handler(CommandHandler("history", handle_history))
        application.add_handler(csv_conv_handler)
        
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
            logger.info("Bot stopped successfully")
    
    except Exception as e:
        logger.error(f"❌ Bot failed to start: {e}")
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
        raise
