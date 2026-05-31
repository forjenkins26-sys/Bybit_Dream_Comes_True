# Bybit Vol Surge bot — Fly.io
FROM python:3.11-slim

WORKDIR /app

# deps first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# app code
COPY *.py ./

# persistent data dir for slippage/trade logs (Fly volume mounts at /data)
RUN mkdir -p /data
ENV DATA_DIR=/data

# A tiny HTTP health endpoint keeps Fly's machine alive + lets you check status.
# bybit_live.py runs the bot; health server runs alongside.
CMD ["python", "-u", "bybit_live.py"]
