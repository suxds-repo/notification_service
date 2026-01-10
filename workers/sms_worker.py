import logging
import asyncio
import json
import aio_pika
from pydantic import ValidationError
from api.schemas import SmsEvent  # Pydantic модель для SMS

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

# DLX и DLQ
DLX_NAME = "events.dlx"
DLQ_NAME = "sms_dlq"


async def process_message(message: aio_pika.IncomingMessage):
    try:
        payload = json.loads(message.body)
        event = SmsEvent(**payload)

        logging.info(f"Send SMS to {event.phone}")
        logging.info(f"Message: {event.message}")

        await message.ack()  # обработка успешна

    except ValidationError as e:
        logging.error(f"Invalid SMS event: {e}")
        await message.reject(requeue=False)  # попадет в DLQ


async def main():
    connection = await aio_pika.connect_robust(RABBIT_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=5)

    # Основной обменник
    exchange = await channel.declare_exchange(
        EXCHANGE_NAME,
        aio_pika.ExchangeType.TOPIC,
        durable=True,
    )

    # Очередь sms
    queue = await channel.declare_queue(
        QUEUE_NAME,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DLX_NAME,  # DLX для ошибок
            "x-dead-letter-routing-key": ROUTING_KEY,
        }
    )

    # Dead Letter Exchange и очередь
    dlx = await channel.declare_exchange(DLX_NAME, aio_pika.ExchangeType.TOPIC, durable=True)
    dlq = await channel.declare_queue(DLQ_NAME, durable=True)
    await dlq.bind(dlx, ROUTING_KEY)

    # Привязка основной очереди к обменнику
    await queue.bind(exchange, ROUTING_KEY)

    await queue.consume(process_message)

    logging.info("SMS worker started")
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
