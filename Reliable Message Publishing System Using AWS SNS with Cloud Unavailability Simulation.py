import boto3
import json
import time
import random

sqs = boto3.client('sqs', region_name='ap-south-1')
queue_url = 'https://sqs.ap-south-1.amazonaws.com/696793786714/MyTestQueue'

local_buffer = []
cloud_available = True
stability_threshold = 0.8

print("Listening for messages and predicting cloud stability...\n")

while True:
    cloud_available = random.random() < stability_threshold

    if not cloud_available:
        print("⚠️  Cloud instability detected. Switching to local buffering mode...\n")
        time.sleep(2)
    
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=10
    )
    
    messages = response.get('Messages', [])
    for msg in messages:
        body = json.loads(msg['Body'])
        message = body['Message']
        print("📩 Received:", message)

        if cloud_available:
            print(f"☁️  Cloud healthy. Processing {message} on cloud server.")
        else:
            print(f"💾 Buffering {message} on local server for now.")
            local_buffer.append(message)

        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=msg['ReceiptHandle']
        )
        print("✅ Acknowledged message deletion from SQS.\n")

    if cloud_available and local_buffer:
        print("🔄 Cloud stabilized. Syncing buffered messages...")
        while local_buffer:
            buffered = local_buffer.pop(0)
            print(f"☁️  Synced {buffered} from local buffer back to cloud.")
        print("✅ All buffered messages synced.\n")

    time.sleep(2)


