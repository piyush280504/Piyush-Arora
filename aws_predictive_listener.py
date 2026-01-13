import os
import boto3
import json
import time
import pickle
import numpy as np
from collections import deque

# =========================
# CONFIG
# =========================
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")

MODEL_PATH = "health_arima.pkl"
HEALTH_THRESHOLD = 0.6        # below this → cloud unstable
FORECAST_STEPS = 1

if not SQS_QUEUE_URL:
    raise ValueError("SQS_QUEUE_URL environment variable not set!")

# =========================
# LOAD ARIMA MODEL
# =========================
with open(MODEL_PATH, "rb") as f:
    model = pickle.load(f)

print("✅ ARIMA model loaded")

# =========================
# AWS CLIENT
# =========================
sqs = boto3.client("sqs", region_name=AWS_REGION)

# =========================
# LOCAL BUFFER
# =========================
local_buffer = deque(maxlen=1000)

# =========================
# METRIC WINDOW (simulation-friendly)
# =========================
recent_health = deque(maxlen=20)

def predict_cloud_health():
    """
    Predict next-step cloud health using ARIMA
    """
    if len(recent_health) < 5:
        return 1.0  # optimistic default during warm-up

    forecast = model.forecast(steps=FORECAST_STEPS)
    return float(forecast[0])

# =========================
# MAIN LOOP
# =========================
print("🚀 Hybrid Predictive Storage Framework running...\n")

while True:
    # ---------------------------------
    # SIMULATED METRIC INGESTION
    # (Replace later with real CloudWatch metrics)
    # ---------------------------------
    simulated_health = np.clip(
        np.random.normal(0.75, 0.05), 0, 1
    )
    recent_health.append(simulated_health)

    predicted_health = predict_cloud_health()
    cloud_available = predicted_health >= HEALTH_THRESHOLD

    if not cloud_available:
        print(
            f"⚠️  Cloud instability predicted "
            f"(health={predicted_health:.3f}). Switching to local buffering."
        )

    # ---------------------------------
    # RECEIVE MESSAGE
    # ---------------------------------
    response = sqs.receive_message(
        QueueUrl=SQS_QUEUE_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=10
    )

    messages = response.get("Messages", [])

    for msg in messages:
        body = json.loads(msg["Body"])
        message = body.get("Message", body)

        print("📩 Received:", message)

        if cloud_available:
            print(f"☁️  Processed on cloud: {message}")
        else:
            print(f"💾 Buffered locally: {message}")
            local_buffer.append(message)

        sqs.delete_message(
            QueueUrl=SQS_QUEUE_URL,
            ReceiptHandle=msg["ReceiptHandle"]
        )

    # ---------------------------------
    # SYNC BUFFER WHEN STABLE
    # ---------------------------------
    if cloud_available and local_buffer:
        print("🔄 Cloud stabilized. Syncing buffered messages...")
        while local_buffer:
            buffered = local_buffer.popleft()
            print(f"☁️  Synced: {buffered}")
        print("✅ Buffer cleared")

    time.sleep(2)
