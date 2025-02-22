import os
import cv2
import torch
import apache_beam as beam
import numpy as np
import json
import base64
import logging
from google.cloud import pubsub_v1
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.direct.direct_runner import DirectRunner
from apache_beam.runners.dataflow import DataflowRunner

runner = "DataflowRunner" if os.getenv("DATAFLOW") else "DirectRunner"

pipeline_options = PipelineOptions(
    streaming=True,
    project="skilled-adapter-451320-i9",
    region="us-central1",
    runner=runner,
    temp_location="gs://pedestrian-image-bucket-123/temp"
)

pipeline = beam.Pipeline(options=pipeline_options)

print(f"Running pipeline with runner: {runner}")

if runner == "DataflowRunner":
    dataflow_runner = DataflowRunner()
    job = dataflow_runner.run(pipeline, options=pipeline_options)
    print(f"Job ID: {job.job_id()}")  # This will print the beam_### job ID

# Load YOLOv5 model for pedestrian detection
yolo_model = torch.hub.load("ultralytics/yolov5", "yolov5s")

# Load MiDaS model for depth estimation
midas = torch.hub.load("intel-isl/MiDaS", "MiDaS_small")
midas.eval()

# Transformation for MiDaS input
midas_transform = torch.hub.load("intel-isl/MiDaS", "transforms").small_transform

# Define Pub/Sub topics
INPUT_TOPIC = "projects/skilled-adapter-451320-i9/topics/pedestrian_input"
OUTPUT_TOPIC = "projects/skilled-adapter-451320-i9/topics/pedestrian_output"

def safe_serialize(obj):
    if isinstance(obj, (np.float32, torch.Tensor)):
        return float(obj)  # Convert to standard float
    elif isinstance(obj, np.ndarray):
        return obj.tolist()  # Convert arrays to lists
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

class ProcessImage(beam.DoFn):
    # Processes an image: detects pedestrians and estimates depth.
    def process(self, message):
        try:
            # Decode raw image bytes
            image_bytes = base64.b64decode(message)
            
            # Convert to NumPy array
            image_array = np.frombuffer(image_bytes, np.uint8)
            image = cv2.imdecode(image_array, cv2.IMREAD_COLOR)

            if image is None:
                raise ValueError("Failed to decode image from Pub/Sub message.")

            # Convert to RGB
            image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

            # Run YOLO object detection
            results_yolo = yolo_model(image_rgb)

            # Prepare input for MiDaS
            input_tensor = midas_transform(image_rgb).to(torch.float32)

            with torch.no_grad():
                depth_map = midas(input_tensor)

            # Convert depth map to NumPy
            depth_map_np = depth_map.squeeze().cpu().numpy()
            depth_map_resized = cv2.resize(depth_map_np, (image_rgb.shape[1], image_rgb.shape[0]))

            detections = []

            # Process detections
            for *bbox, confidence, cls in results_yolo.xyxy[0].tolist():
                if int(cls) == 0:  # Class 0 = pedestrian
                    x_min, y_min, x_max, y_max = map(int, bbox)

                    # Ensure bounding box is within image bounds
                    y_min, y_max = max(0, y_min), min(depth_map_resized.shape[0], y_max)
                    x_min, x_max = max(0, x_min), min(depth_map_resized.shape[1], x_max)

                    # Extract depth map region and compute average depth
                    bbox_depth = depth_map_resized[y_min:y_max, x_min:x_max]
                    avg_depth = safe_serialize(np.mean(bbox_depth) if bbox_depth.size > 0 else 0)

                    detections.append({
                        "x_min": x_min, "y_min": y_min, "x_max": x_max, "y_max": y_max,
                        "avg_depth": avg_depth
                    })

            return [json.dumps(detections)]
        
        except Exception as e:
            return [json.dumps({"error": str(e)})]

class PublishToPubSub(beam.DoFn):
    """Publishes results to a Pub/Sub topic with error handling and retries."""

    def __init__(self, topic):
        self.topic = topic
        self.publisher = None  # Defer initialization to avoid pickling issues

    def setup(self):
        """Initializes the Pub/Sub publisher client."""
        self.publisher = pubsub_v1.PublisherClient()

    def process(self, element):
        print(f"Passed Message: {element}")
        try:
            future = self.publisher.publish(self.topic, element.encode("utf-8"))
            future.result()  # Ensure message is successfully published
        except Exception as e:
            logging.error(f"Failed to publish message: {e}")
            return [json.dumps({"error": str(e), "message": element})]

def run():
    print("Waiting for input from Pub/Sub...")
    runner = "DataflowRunner" if os.getenv("DATAFLOW") else "DirectRunner"

    pipeline_options = PipelineOptions(
        streaming=True,
        project="skilled-adapter-451320-i9",
        region="us-central1",
        runner=runner,
        temp_location="gs://pedestrian-image-bucket-123/temp"
    )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p 
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=INPUT_TOPIC)
            | "ProcessImage" >> beam.ParDo(ProcessImage())
            | "PublishResults" >> beam.ParDo(PublishToPubSub(OUTPUT_TOPIC))
        )

if __name__ == "__main__":
    run()