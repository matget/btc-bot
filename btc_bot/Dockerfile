FROM python:3.11-slim

WORKDIR /app

# Copy project files
COPY run.py .
COPY requirements.txt .
COPY config.yaml /data/options.json

# Install system packages
RUN apt-get update && apt-get install -y \
    build-essential \
    libfreetype6-dev \
    libpng-dev \
    libjpeg-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy and prepare the run script
COPY run.sh /app/
RUN chmod +x /app/run.sh && \
    sed -i 's/\r$//' /app/run.sh && \
    mkdir -p /data

# Environment variables for configuration
ENV TOKEN="your-token-here" \
    CHAT_ID="your-chat-id-here" \
    JSON_KEYS="your-google-json-content" \
    GSHEET_URL="link-to-your-google-sheet"

# Set entrypoint
CMD ["/app/run.sh"]
