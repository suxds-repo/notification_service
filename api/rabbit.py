import aio_pika

RABBIT_URL = "amqp://guest:guest@rabbitmq/"
EXCHANGE_NAME = "events"

_connection: aio_pika.RobustConnection | None = None
_channel: aio_pika.RobustChannel | None = None
_exchange: aio_pika.Exchange | None = None


async def get_exchange() -> aio_pika.Exchange:
    global _connection, _channel, _exchange

    if _exchange is None:

        _connection = await aio_pika.connect_robust(RABBIT_URL)
        _channel = await _connection.channel()

        _exchange = await _channel.declare_exchange(
            EXCHANGE_NAME,
            aio_pika.ExchangeType.TOPIC,
            durable=True
        )

    return _exchange
