import asyncio
import os
import socket
import cv2

from aiokafka import (
    AIOKafkaConsumer as _AIOKafkaConsumer,
    AIOKafkaProducer as _AIOKafkaProducer
)

from .base import AbstractConsumer, AbstractProducer

class AIOKafkaConsumer(AbstractConsumer):
    def __init__(self):
        self._consumer_topic = getattr(self, 'consumer_topic', None) or\
                                os.getenv('CONSUMER_TOPIC').split(',')
        print(self._consumer_topic)
        self._consumer_conf = getattr(self, 'consumer_conf', {})
        self._consumer_conf = {
            'bootstrap_servers': os.getenv('CONSUMER_SERVERS'),
            'group_id': os.getenv('GROUP_ID','group'),
            **self._consumer_conf}
        print(self._consumer_conf)
        self.consumer = _AIOKafkaConsumer(*self._consumer_topic,
                                          **self._consumer_conf)

    async def start_consumer(self):
        await self.consumer.start()
        if getattr(self, 'read_last', True):
            await self.consumer.seek_to_end()

    async def receive(self):
        return await self.consumer.getone()

    async def close_consumer(self):
        await self.consumer.stop()

class AIOKafkaProducer(AbstractProducer):
    def __init__(self):
        self._producer_conf = getattr(self, 'producer_conf', {})
        self._producer_conf = {
            'bootstrap_servers': os.getenv('PRODUCER_SERVERS'),
            'client_id': os.getenv('CLIENT_ID',socket.gethostname()),
            **self._producer_conf}
        print(self._producer_conf)
        self.producer = _AIOKafkaProducer(**self._producer_conf)
        self._cancelled = False

    async def send(self, data, topic=None, key=None, headers=None):
        key = key or getattr(self.message, 'key', None)
        headers = headers or getattr(self.message, 'headers', None)
        if type(headers) is tuple:
            headers = list(headers)
        await self.producer.send(topic or self.producer_topic, key=key,
                        value=data, headers=headers)

    async def start_producer(self):
        await self.producer.start()

    async def close_producer(self):
        await self.producer.stop()

class OpenCVConsumer(AbstractConsumer):
    def __init__(self, loop=None, executor=None):
        self._device = getattr(self, 'device', 0)
        self.consumer = getattr(self, 'consumer', None) or \
                            cv2.VideoCapture(self._device)
        self._loop = loop or asyncio.get_event_loop()
        self._executor = executor

    def close_consumer(self):
        self.consumer.release()

    def _receive(self):
        success, frame = self.consumer.read()
        if not success: return self.receive_error(frame)
        return frame

    async def receive(self):
        return await self._loop.run_in_executor(self._executor, self._receive)
