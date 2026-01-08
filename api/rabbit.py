import json
import aio_pika

RABBIT_URL = "amqp://guest:guest@rabbitmq/"

EXCHANGE_NAME = "events"


async def get_exchange():
    connection = await aio_pika.connect_robust(RABBIT_URL)
    channel = await connection.channel()

    exchange = await channel.declare_exchange(
        EXCHANGE_NAME,
        aio_pika.ExchangeType.TOPIC,
        durable=True,
    )
    return exchange
