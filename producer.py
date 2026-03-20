import json
import time
import random
from azure.eventhub import EventHubProducerClient, EventData

connection_str = "Endpoint=sb://uber-eventhub-ns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR SECRET KEY PASTE HERE"
eventhub_name = "uber-rides"
producer = EventHubProducerClient.from_connection_string(
    conn_str=connection_str,
    eventhub_name=eventhub_name
)

cities = ["Hyderabad", "Bangalore", "Chennai"]

while True:
    data = {
        "ride_id": random.randint(1000, 9999),
        "driver_id": random.randint(1, 100),
        "location": random.choice(cities),
        "fare": random.randint(50, 500),
        "timestamp": time.time()
    }

    # ✅ Convert to proper JSON
    json_data = json.dumps(data)

    event_data_batch = producer.create_batch()
    event_data_batch.add(EventData(json_data))
    producer.send_batch(event_data_batch)

    print("Sent:", json_data)

    time.sleep(2)
