import logging
import asyncio
import json
import aio_pika
from pydantic import ValidationError
from api.schemas import EmailEvent

# Настройка логов
logging.basicConfig(
    filename="logs/email_worker.log",
    level=logging.INFO,
    format="%(asctime)s [EMAIL] %(message)s",
    encoding="utf-8"
)

RABBIT_URL = "amqp://guest:guest@rabbitmq/"
EXCHANGE_NAME = "events"
QUEUE_NAME = "email_queue"
ROUTING_KEY = "user.email"

DLX_NAME = "events.dlx"
DLQ_NAME = "email_dlq"


async def send_email(email: str, message: str):
    logging.info(f"Email would be sent to {email}")
    logging.info(f"Message content: {message}")


async def process_message(message: aio_pika.IncomingMessage):
    try:
        payload = json.loads(message.body)
        event = EmailEvent(**payload)

        await send_email(event.email, event.message)

        await message.ack()

    except ValidationError as e:
        logging.error(f"Invalid email event: {e}")
        await message.reject(requeue=False)  # сообщение в DLQ

    except Exception as e:
        logging.error(f"Failed to send email: {e}")
        await message.reject(requeue=True)  # переотправляем в очередь


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

    # Очередь email
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

    logging.info("Email worker started")
    await asyncio.Future()  # держим воркер живым


if __name__ == "__main__":
    asyncio.run(main())
