import json
import random
import time
from datetime import datetime, timedelta

from kafka import KafkaProducer

producer = KafkaProducer(
bootstrap_servers="kafka:9092",
value_serializer=lambda v: json.dumps(v).encode("utf-8"),
key_serializer=lambda k: str(k).encode("utf-8"),
acks="all"
)

order_id = 1

while True:

    customer_id = random.randint(1, 1000)
    product_id = random.randint(1, 100)
    quantity = random.randint(1, 5)
    price = round(random.uniform(100, 5000), 2)

    created_dt = datetime.utcnow()

    paid_dt = created_dt + timedelta(
        minutes=random.randint(1, 60)
    )

    shipped_dt = paid_dt + timedelta(
        minutes=random.randint(10, 180)
    )

    events = [
        {
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "quantity": quantity,
            "price": price,
            "status": "NEW",
            "event_time": created_dt.isoformat()
        },
        {
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "quantity": quantity,
            "price": price,
            "status": "PAID",
            "event_time": paid_dt.isoformat()
        },
        {
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "quantity": quantity,
            "price": price,
            "status": "SHIPPED",
            "event_time": shipped_dt.isoformat()
        }
    ]

    for event in events:

        metadata = producer.send(
            topic="orders",
            key=order_id,
            value=event
        ).get(timeout=10)

        print(
            f"offset={metadata.offset} "
            f"order_id={order_id} "
            f"status={event['status']}"
        )

    order_id += 1

    time.sleep(random.uniform(0.3, 0.5))
