import logging
import asyncio
import json
import aio_pika
from pydantic import ValidationError
from api.schemas import EmailEvent
from functools import partial

RABBIT_URL = "amqp://guest:guest@rabbitmq/"

QUEUE_NAME = "email_queue"
RETRY_EXCHANGE = "events.retry"
DLQ_EXCHANGE = "events.dlx"
MAX_RETRIES = 3

logging.basicConfig(
    filename="logs/email_worker.log",
    level=logging.INFO,
    format="%(asctime)s [EMAIL] %(message)s",
    encoding="utf-8",
)


async def send_email(email: str, message: str):
    """
    Функция отправки email.
    Любая ошибка, возникшая здесь, будет поймана в process_message.
    Для теста можно вставить KeyError или другую ошибку:
      - если message содержит "fail" → выбросим KeyError
    """
    logging.info(f"Sending email to {email}: {message}")

    # Пример реальной ошибки для теста
    if "fail" in message:
        d = {}
        print(d["non_existing_key"])  # вызовет KeyError

    logging.info(f"✅ Email successfully sent to {email}")



async def send_to_dlq(channel: aio_pika.RobustChannel, body: bytes):
    dlq_exchange = await channel.declare_exchange(DLQ_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
    msg = aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
    await dlq_exchange.publish(msg, routing_key="user.email")
    logging.info("Message sent to DLQ")



async def send_to_retry(channel: aio_pika.RobustChannel, body: bytes, headers: dict):
    retry_exchange = await channel.declare_exchange(RETRY_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
    retry_msg = aio_pika.Message(body=body, headers=headers, delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
    await retry_exchange.publish(retry_msg, routing_key="user.email")
    logging.info(f"Message sent to retry queue, attempt {headers.get('x-retry-count', 1)}")



async def process_message(channel: aio_pika.RobustChannel, message: aio_pika.IncomingMessage):
    async with message.process(ignore_processed=True):
        try:
            payload = json.loads(message.body)
            event = EmailEvent(**payload)

            retries = message.headers.get("x-retry-count", 0)
            logging.info(f"Processing email to {event.email}, attempt {retries + 1}")


            await send_email(event.email, event.message)

        except ValidationError as e:
            logging.error(f"❌ Invalid email event → DLQ: {e}")
            await send_to_dlq(channel, message.body)

        except Exception as e:
            retries = message.headers.get("x-retry-count", 0)
            if retries >= MAX_RETRIES:
                logging.error(f"⚠ Max retries exceeded for {payload.get('email')} → DLQ")
                await send_to_dlq(channel, message.body)
            else:
                logging.warning(f"Retrying email to {payload.get('email')}, attempt {retries + 1} due to error: {e}")
                headers = dict(message.headers or {})
                headers["x-retry-count"] = retries + 1
                await send_to_retry(channel, message.body, headers)



async def main():
    connection = await aio_pika.connect_robust(RABBIT_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)

    queue = await channel.declare_queue(QUEUE_NAME, durable=True)
    await queue.consume(partial(process_message, channel))

    logging.info("Email worker started")
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
