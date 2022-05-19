import asyncio

from .consumer_producer import (
    AIOKafkaConsumer, AIOKafkaProducer, OpenCVConsumer
)
from .base import AbstractConsumer, AbstractProducer
from .general import Runnable

class Producer(AbstractConsumer, AIOKafkaProducer, Runnable):
    pass

class Consumer(AIOKafkaConsumer, AbstractProducer, Runnable):
    pass

class ConsumerStorage(AbstractConsumer, AbstractProducer, Runnable):
    def __init__(self, keep_messages=False):
        self.keep_messages = keep_messages
        self.q = asyncio.Queue()

    async def send(self, data):
        obj = {
            'message': self.message,
            'data': data,
        }
        if not self.keep_messages:
            # remove the last message before putting the new one
            if not self.q.empty():
                self.get_nowait()
        await self.q.put(obj)

    async def get(self):
        return await self.q.get()

    def get_nowait(self):
        try:
            return self.q.get_nowait()
        except asyncio.QueueEmpty:
            return None
