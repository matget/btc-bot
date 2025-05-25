FROM python:3.11-slim

WORKDIR /app

# Copy project files
COPY run.py .
COPY requirements.txt .
COPY config.yaml /data/config.yaml
COPY options.json /data/options.json

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

ENV TOKEN="" \
    CHAT_ID="" \
    JSON_KEYS="" \
    GSHEET_URL="" \
    OPENAI_API_KEY="" \
    BINANCE_API_KEY="" \
    BINANCE_API_SECRET=""

# Copy and prepare the run script
COPY run.sh /app/
RUN chmod +x /app/run.sh && \
    sed -i 's/\r$//' /app/run.sh && \
    mkdir -p /data

# Set entrypoint
CMD ["/app/run.sh"]
