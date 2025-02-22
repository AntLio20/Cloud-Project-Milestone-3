from google.cloud import pubsub_v1

# Define project and subscription
PROJECT_ID = "skilled-adapter-451320-i9"
SUBSCRIPTION_NAME = "pedestrian_output-sub"

def callback(message):
    """Callback function to process received messages."""
    print(f"Received message: {message.data.decode('utf-8')}")
    message.ack()  # Acknowledge the message to remove it from the queue

def listen_for_messages():
    """Listens for messages on a Pub/Sub subscription."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)

    print(f"Listening for messages on {SUBSCRIPTION_NAME}...\n")

    future = subscriber.subscribe(subscription_path, callback=callback)

    try:
        future.result()  # Blocks the thread and listens for messages indefinitely
    except KeyboardInterrupt:
        print("Subscriber stopped.")
        future.cancel()  # Cancel the subscription on exit

if __name__ == "__main__":
    listen_for_messages()