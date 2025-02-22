import os
import base64
from google.cloud import pubsub_v1

# Path to images
dataset_path = "Dataset_Occluded_Pedestrian"

# Pub/Sub topic
project_id = "skilled-adapter-451320-i9"
topic_id = "pedestrian_input"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Iterate through images and publish
for prefix in ["A", "B", "C"]:
    for i in range(1, 121):
        image_filename = f"{prefix}_{i:03d}.png"
        image_path = os.path.join(dataset_path, image_filename)

        if not os.path.exists(image_path):
            print(f"Skipping missing file: {image_path}")
            continue

        with open(image_path, "rb") as img_file:
            image_data = base64.b64encode(img_file.read()).decode("utf-8")
        
        # Publish message
        future = publisher.publish(topic_path, data=image_data.encode())
        message_id = future.result()  # Blocks until publish is confirmed
        print(f"Published {image_filename} with message ID: {message_id}")

print("All images published.")