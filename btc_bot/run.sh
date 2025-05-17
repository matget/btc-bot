#!/bin/sh
echo "🔧 Starting Bitcoin Bot add-on..."

# Create options.json using Python
python3 -c '
import os
import json

config = {
    "TOKEN": os.environ.get("TOKEN"),
    "CHAT_ID": os.environ.get("CHAT_ID"),
    "JSON_KEYS": json.loads(os.environ.get("JSON_KEYS", "{}")),
    "GSHEET_URL": os.environ.get("GSHEET_URL")
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
