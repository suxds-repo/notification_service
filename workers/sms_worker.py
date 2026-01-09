import logging
import asyncio
import json
import aio_pika
from pydantic import ValidationError
from api.schemas import SmsEvent

logging.basicConfig(
    filename="logs/sms_worker.log",
    level=logging.INFO,
    format="%(asctime)s [SMS] %(message)s",
    encoding="utf-8"
)

RABBIT_URL = "amqp://guest:guest@rabbitmq/"
EXCHANGE_NAME = "events"
QUEUE_NAME = "sms_queue"
ROUTING_KEY = "user.sms"


async def process_message(message: aio_pika.IncomingMessage):
    try:
        payload = json.loads(message.body)
        event = SmsEvent(**payload)

        logging.info(f"Send SMS to {event.phone}")
        logging.info(f"Message: {event.message}")

        await message.ack()

    except ValidationError as e:
        logging.error(f"Invalid sms event: {e}")
        await message.reject(requeue=False)


async def main():
    connection = await aio_pika.connect_robust(RABBIT_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=5)

    exchange = await channel.declare_exchange(
        EXCHANGE_NAME,
        aio_pika.ExchangeType.TOPIC,
        durable=True,
    )

    queue = await channel.declare_queue(
        QUEUE_NAME,
        durable=True,
    )

    await queue.bind(exchange, ROUTING_KEY)
    await queue.consume(process_message)

    logging.info("SMS worker started")
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
