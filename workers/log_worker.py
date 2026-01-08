import logging
import asyncio
import json
import aio_pika

logging.basicConfig(
    filename="logs/log_worker.log",
    level=logging.INFO,
    format="%(asctime)s [LOG] %(message)s",
    encoding="utf-8"
)

RABBIT_URL = "amqp://guest:guest@rabbitmq/"
EXCHANGE_NAME = "events"

async def process_message(message: aio_pika.IncomingMessage):
    async with message.process():
        payload = json.loads(message.body)
        logging.info(f"Event received: {payload}")

async def main():
    connection = await aio_pika.connect_robust(RABBIT_URL)
    channel = await connection.channel()

    exchange = await channel.declare_exchange(
        EXCHANGE_NAME,
        aio_pika.ExchangeType.TOPIC,
        durable=True,
    )

    queue = await channel.declare_queue("", exclusive=True)
    await queue.bind(exchange, "#")

    await queue.consume(process_message)

    logging.info("Logger worker started")
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
