import asyncio
import aio_pika

RABBIT_URL = "amqp://guest:guest@rabbitmq/"

EVENTS_EXCHANGE = "events"
DLX_EXCHANGE = "events.dlx"
RETRY_EXCHANGE = "events.retry"

RETRY_TTL_MS = 30000  # 30 секунд


async def init():
    connection = await aio_pika.connect_robust(RABBIT_URL)
    channel = await connection.channel()

    # Exchanges
    events_exchange = await channel.declare_exchange(
        EVENTS_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
    )
    dlx_exchange = await channel.declare_exchange(
        DLX_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
    )
    retry_exchange = await channel.declare_exchange(
        RETRY_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
    )


    email_queue = await channel.declare_queue(
        "email_queue",
        durable=True,
        arguments={},
    )
    await email_queue.bind(events_exchange, "user.email")

    email_retry_queue = await channel.declare_queue(
        "email_retry_queue",
        durable=True,
        arguments={
            "x-message-ttl": RETRY_TTL_MS,
            "x-dead-letter-exchange": EVENTS_EXCHANGE,
            "x-dead-letter-routing-key": "user.email",
        },
    )
    await email_retry_queue.bind(retry_exchange, "user.email")

    # DLQ
    email_dlq = await channel.declare_queue("email_dlq", durable=True)
    await email_dlq.bind(dlx_exchange, "user.email")

    sms_queue = await channel.declare_queue("sms_queue", durable=True, arguments={})
    await sms_queue.bind(events_exchange, "user.sms")

    sms_retry_queue = await channel.declare_queue(
        "sms_retry_queue",
        durable=True,
        arguments={
            "x-message-ttl": RETRY_TTL_MS,
            "x-dead-letter-exchange": EVENTS_EXCHANGE,
            "x-dead-letter-routing-key": "user.sms",
        },
    )
    await sms_retry_queue.bind(retry_exchange, "user.sms")

    sms_dlq = await channel.declare_queue("sms_dlq", durable=True)
    await sms_dlq.bind(dlx_exchange, "user.sms")

    await connection.close()
    print("RabbitMQ topology initialized")


if __name__ == "__main__":
    asyncio.run(init())
