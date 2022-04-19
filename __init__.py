from queue import Empty, Queue

from .consumer_producer import KafkaConsumer, KafkaProducer, OpenCVConsumer
from .base import AbstractConsumer, AbstractProducer
from .general import Runnable

class Producer(AbstractConsumer, KafkaProducer, Runnable):
    pass

class Consumer(KafkaConsumer, AbstractProducer, Runnable):
    pass

class ConsumerStorage(Consumer):
    def __init__(self, keep_messages=False):
        self.keep_messages = keep_messages
        self.q = Queue()
        super().__init__()

    def send(self, data):
        obj = {
            'message': self.message,
            'data': data,
        }
        if not self.keep_messages:
            # remove the last message before putting the new one
            if not self.q.empty():
                self.get_nowait()
        self.q.put(obj)

    def get(self, *args, **kwargs):
        kwargs.setdefault('timeout', 1)
        try:
            data = self.q.get(*args, **kwargs)
        except Empty:
            return None
        return data

    def get_nowait(self):
        return self.get(block=False)
