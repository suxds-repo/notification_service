import logging
import asyncio
import json
import aio_pika
from pydantic import ValidationError
from api.schemas import SmsEvent
from functools import partial

RABBIT_URL = "amqp://guest:guest@rabbitmq/"

QUEUE_NAME = "sms_queue"
RETRY_EXCHANGE = "events.retry"
DLQ_EXCHANGE = "events.dlx"
MAX_RETRIES = 3

logging.basicConfig(
    filename="logs/sms_worker.log",
    level=logging.INFO,
    format="%(asctime)s [SMS] %(message)s",
    encoding="utf-8",
)


async def send_sms(phone: str, message: str):
    """
    Функция отправки SMS.
    Здесь можно вызвать реальный провайдер или любую операцию,
    которая может выбросить настоящую ошибку.
    """
    logging.info(f"Sending SMS to {phone}: {message}")

    # Пример реальной ошибки (для теста) — можно удалить или заменить на любой код
    if "fail" in message:
        # Реальная ошибка, например KeyError
        d = {}
        print(d["non_existing_key"])  # это выбросит KeyError

    logging.info(f"✅ SMS sent to {phone}: {message}")


async def send_to_dlq(channel: aio_pika.RobustChannel, body: bytes):
    dlq_exchange = await channel.declare_exchange(DLQ_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
    msg = aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
    await dlq_exchange.publish(msg, routing_key="user.sms")
    logging.info("Message sent to DLQ")


async def send_to_retry(channel: aio_pika.RobustChannel, body: bytes, headers: dict):
    retry_exchange = await channel.declare_exchange(RETRY_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
    retry_msg = aio_pika.Message(body=body, headers=headers, delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
    await retry_exchange.publish(retry_msg, routing_key="user.sms")
    logging.info(f"Message sent to retry queue, attempt {headers.get('x-retry-count', 1)}")


async def process_message(channel: aio_pika.RobustChannel, message: aio_pika.IncomingMessage):
    async with message.process(ignore_processed=True):
        try:
            payload = json.loads(message.body)
            event = SmsEvent(**payload)

            retries = message.headers.get("x-retry-count", 0)
            logging.info(f"Processing SMS to {event.phone}, attempt {retries + 1}")

            await send_sms(event.phone, event.message)

        except ValidationError as e:
            logging.error(f"❌ Invalid SMS event → DLQ: {e}")
            await send_to_dlq(channel, message.body)

        except Exception as e:
            retries = message.headers.get("x-retry-count", 0)
            if retries >= MAX_RETRIES:
                logging.error(f"⚠ Max retries exceeded for {payload.get('phone')} → DLQ")
                await send_to_dlq(channel, message.body)
            else:
                logging.warning(f"Retrying SMS to {payload.get('phone')}, attempt {retries + 1} due to error: {e}")
                headers = dict(message.headers or {})
                headers["x-retry-count"] = retries + 1
                await send_to_retry(channel, message.body, headers)


async def main():
    connection = await aio_pika.connect_robust(RABBIT_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=5)

    queue = await channel.declare_queue(QUEUE_NAME, durable=True)
    await queue.consume(partial(process_message, channel))

    logging.info("SMS worker started")
    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
