#!/bin/sh
echo "🔧 Starting Bitcoin Bot add-on..."

# Create options.json using Python
python3 -c '
import os
import json

config = {
    "TOKEN": os.environ.get("TOKEN"),
    "CHAT_ID": os.environ.get("CHAT_ID"),
    "GSHEET_URL": os.environ.get("GSHEET_URL"),
    "JSON_KEYS": json.loads(os.environ.get("JSON_KEYS", "{}")),
    "OPENAI_API_KEY": os.environ.get("OPENAI_API_KEY"),
    "BINANCE_API_KEY": os.environ.get("BINANCE_API_KEY"),
    "BINANCE_API_SECRET": os.environ.get("BINANCE_API_SECRET")
}

with open("/data/options.json", "w") as f:
    json.dump(config, f, indent=4)
'

# Start your bot
echo "📦 Running bot listener..."
python3 /app/run.py listen || {
    echo "❌ Error: Bot crashed or failed to start"
    exit 1
}
