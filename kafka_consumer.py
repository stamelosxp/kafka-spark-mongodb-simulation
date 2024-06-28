from kafka import KafkaConsumer
import json


def runConsumer(nameTopic):
    consumer = KafkaConsumer(
        nameTopic,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Consumer started, waiting for messages...")

    cnt = 0
    # Poll for new messages
    for message in consumer:
        print(f"Received message: {message.value}")
        cnt +=1


    print(f"Total received messages: {cnt}")
    print("Consumer ended.")


if __name__ == '__main__':
    nameTopic = 'vehicle_positions'
    runConsumer(nameTopic)
