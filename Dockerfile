# Use a lightweight Python image
FROM python:3.10-slim

# Set working directory inside the container
WORKDIR /app

# Copy all files into the container
COPY . .

# Install necessary Python dependencies
RUN pip install --no-cache-dir apache-beam[gcp] opencv-python-headless torch torchvision google-cloud-pubsub ultralytics numpy

# Copy Google Cloud JSON key
COPY skilled-adapter-451320-i9-5457e7b0bad6.json /app/service-account.json

# Set environment variable for Google Cloud authentication
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/service-account.json"

# Make entrypoint script executable
RUN chmod +x /app/entrypoint.sh

# Run all necessary scripts
ENTRYPOINT ["/bin/bash", "/app/entrypoint.sh"]