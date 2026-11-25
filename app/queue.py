import json
import logging
from typing import Any, Dict, Optional

import aio_pika
from aio_pika import DeliveryMode, Message

from app.config import settings

logger = logging.getLogger(__name__)


class RabbitMQClient:
    def __init__(self, amqp_url: str):
        self.amqp_url = amqp_url
        self.connection: Optional[aio_pika.abc.AbstractRobustConnection] = None
        self.channel: Optional[aio_pika.abc.AbstractChannel] = None

    async def connect(self) -> None:
        if self.connection and not self.connection.is_closed:
            return
        try:
            self.connection = await aio_pika.connect_robust(self.amqp_url)
            self.channel = await self.connection.channel(publisher_confirms=True)

            # Declare primary queue with dead-letter exchange configuration
            await self.channel.declare_queue(
                settings.EVENTS_QUEUE,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": settings.EVENTS_DLQ,
                }
            )

            # Declare DLQ
            await self.channel.declare_queue(settings.EVENTS_DLQ, durable=True)
            logger.info("Connected to RabbitMQ and declared queues.")
        except Exception as exc:
            logger.warning("Could not connect to RabbitMQ: %s", exc)
            self.connection = None
            self.channel = None

    async def close(self) -> None:
        if self.channel and not self.channel.is_closed:
            await self.channel.close()
        if self.connection and not self.connection.is_closed:
            await self.connection.close()

    async def publish(self, queue_name: str, payload: Dict[str, Any], headers: Optional[Dict[str, Any]] = None) -> bool:
        if not self.channel or self.channel.is_closed:
            await self.connect()

        if not self.channel:
            raise ConnectionError("RabbitMQ is not connected.")

        message = Message(
            body=json.dumps(payload).encode("utf-8"),
            delivery_mode=DeliveryMode.PERSISTENT,
            content_type="application/json",
            headers=headers or {},
        )

        await self.channel.default_exchange.publish(
            message,
            routing_key=queue_name,
        )
        return True


rabbitmq_client = RabbitMQClient(settings.RABBITMQ_URL)

