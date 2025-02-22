#!/bin/bash

# Set Google Cloud authentication
export GOOGLE_APPLICATION_CREDENTIALS="/app/service-account.json"

# Start Subscriber
python3 Subscriber.py &

# Start Detector
python3 Detector.py &

# Start Publisher
python3 Publisher.py 