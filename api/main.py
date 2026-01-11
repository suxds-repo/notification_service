import json
import aio_pika
from fastapi import FastAPI
from api.rabbit import get_exchange
from api.schemas import NotificationEvent

app = FastAPI()


@app.post("/events")
async def publish_event(event: NotificationEvent):
    exchange = await get_exchange()

    message = aio_pika.Message(
        body=json.dumps(event.dict()).encode(),
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
    )

    await exchange.publish(
        message,
        routing_key=event.event_type,
    )

    return {"status": "sent", "event": event.event_type}
