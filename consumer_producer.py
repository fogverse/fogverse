import asyncio
import inspect
import os
import socket

from aiokafka import (
    AIOKafkaConsumer as _AIOKafkaConsumer,
    AIOKafkaProducer as _AIOKafkaProducer
)
from .logging import CsvLogging

from .base import AbstractConsumer, AbstractProducer

class AIOKafkaConsumer(AbstractConsumer):
    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._topic_pattern = getattr(self, 'topic_pattern', None)
        self._consumer_topic = os.getenv('CONSUMER_TOPIC', '') or\
                                getattr(self, 'consumer_topic', [])
        if isinstance(self._consumer_topic, str):
            self._consumer_topic = self._consumer_topic.split(',')
        self._consumer_servers = os.getenv('CONSUMER_SERVERS') or \
                            getattr(self, 'consumer_servers', None)
        self._consumer_conf = getattr(self, 'consumer_conf', {})
        self._consumer_conf = {
            'loop': self._loop,
            'bootstrap_servers': self._consumer_servers,
            'group_id': os.getenv('GROUP_ID','group'),
            **self._consumer_conf}
        if self._consumer_topic:
            self.consumer = _AIOKafkaConsumer(*self._consumer_topic,
                                            **self._consumer_conf)
        else:
            self.consumer = _AIOKafkaConsumer(**self._consumer_conf)

    async def start_consumer(self):
        LOGGER = getattr(self, '_log', None)
        if LOGGER is not None and not isinstance(self, CsvLogging):
            LOGGER.info('Topic: %s', self._consumer_topic)
            LOGGER.info('Config: %s', self._consumer_conf)
        await self.consumer.start()
        if self._topic_pattern:
            self.consumer.subscribe(pattern=self._topic_pattern)
        await asyncio.sleep(5) # wait until assigned to partition
        if getattr(self, 'read_last', True):
            await self.consumer.seek_to_end()

    async def receive(self):
        return await self.consumer.getone()

    async def close_consumer(self):
        await self.consumer.stop()

class AIOKafkaProducer(AbstractProducer):
    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self.producer_topic = os.getenv('PRODUCER_TOPIC') or \
                                getattr(self, 'producer_topic', None)
        self._producer_servers = os.getenv('PRODUCER_SERVERS') or \
                            getattr(self, 'producer_servers', None)
        self._producer_conf = getattr(self, 'producer_conf', {})
        self._producer_conf = {
            'loop': self._loop,
            'bootstrap_servers': self._producer_servers,
            'client_id': os.getenv('CLIENT_ID',socket.gethostname()),
            **self._producer_conf}
        self.producer = _AIOKafkaProducer(**self._producer_conf)

    async def start_producer(self):
        LOGGER = getattr(self, '_log', None)
        if LOGGER is not None and not isinstance(self, CsvLogging):
            LOGGER.info('Config: %s', self._producer_conf)
            if getattr(self, 'producer_topic', None) is not None:
                LOGGER.info('Topic: %s', self.producer_topic)
        await self.producer.start()

    async def send(self, data, topic=None, key=None, headers=None,
                   callback=None):
        key = key or getattr(self.message, 'key', None)
        self._headers = headers or getattr(self.message, 'headers', [])
        self._topic = topic or self.producer_topic
        if isinstance(self._headers, tuple):
            self._headers = list(self._headers)
        future = await self.producer.send(self._topic,
                                          key=key,
                                          value=data,
                                          headers=self._headers)
        callback = callback or getattr(self, 'callback', None)
        if not callable(callback): return future
        async def _call_callback_ack(args:list, kwargs:dict):
            record_metadata = await future
            res = callback(record_metadata, *args, **kwargs)
            return (await res) if inspect.iscoroutine(res) else res
        if hasattr(self, '_get_extra_callback_args'):
            args, kwargs = self._get_extra_callback_args()
        else:
            args, kwargs = [], {}
        coro = _call_callback_ack(args, kwargs)
        return asyncio.ensure_future(coro) # awaitable

    async def close_producer(self):
        await self.producer.stop()

def _get_cv_video_capture(device=0):
    import cv2
    return cv2.VideoCapture(device)

class OpenCVConsumer(AbstractConsumer):
    def __init__(self, loop=None, executor=None):
        self._device = os.getenv('DEVICE') or getattr(self, 'device', 0)
        self.consumer = getattr(self, 'consumer', None) or \
                            _get_cv_video_capture(self._device)
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
