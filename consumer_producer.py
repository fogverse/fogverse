import os
import cv2
import socket
import sys

from confluent_kafka import (
    Producer as _Producer,
    Consumer as _Consumer,
    KafkaError,
)

from .base import AbstractConsumer, AbstractProducer

class KafkaConsumer(AbstractConsumer):
    def __init__(self):
        self.consumer_conf = getattr(self, 'consumer_conf', {})
        self.consumer_conf = {
            'bootstrap.servers': os.getenv('CONSUMER_SERVERS'),
            'auto.offset.reset': os.getenv('OFFSET_RESET','smallest'),
            'group.id': os.getenv('GROUP_ID','group'),
            **self.consumer_conf
        }
        self.consumer = _Consumer(self.consumer_conf)

        if not hasattr(self, 'consumer_topic'):
            self.consumer_topic = os.getenv('CONSUMER_TOPIC')\
                                    .replace(' ', '')\
                                    .split(',')
        self.consumer.subscribe(self.consumer_topic, on_assign=self.on_assign)
        print(self.consumer_conf)
        print(f'subscribed {self.consumer_topic}')

    def on_assign(self, _consumer, partitions):
        self.partitions = partitions
        print('Reassigned')
        print(partitions)

        for partition in partitions:
            # get offset tuple from the first partition
            offset = _consumer.get_watermark_offsets(partition)
            print(offset)
            # position [1] being the last index
            n = 1
            if (offset[1] - offset[0]) >= n:
                partition.offset = offset[1] - n
        self.consumer.assign(partitions)

    def receive_error(self, data):
        if data.error().code() == KafkaError._PARTITION_EOF:
            # End of partition event
            sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                (data.topic(), data.partition(),
                                data.offset()))
        elif data.error():
            sys.stderr.write('%% %s [%d] %s\n' %
                                (data.topic(), data.partition(),
                                data.error()))

    def receive(self):
        msg = self.consumer.poll(timeout=1.0)
        if msg is None: return None
        if msg.error():
            return self.receive_error(msg)
        return msg

class KafkaProducer(AbstractProducer):
    def __init__(self):
        self.producer_conf = getattr(self, 'producer_conf', {})
        self.producer_conf = {
            'bootstrap.servers': os.getenv('PRODUCER_SERVERS'),
            'client.id': os.getenv('CLIENT_ID',socket.gethostname()),
            **self.producer_conf
        }
        self.producer = _Producer(self.producer_conf)

    def send(self, data, topic=None, key=None, headers=None):
        key_fun = getattr(self.message, 'key', None)
        if key is None and callable(key_fun):
            key = self.message.key()
        headers_fun = getattr(self.message, 'headers', None)
        if headers is None and callable(headers_fun):
            headers = self.message.headers()
        self.producer.produce(topic or self.producer_topic, key=key,
                        value=data, headers=headers, callback=self.acked)
        self.producer.flush()

class OpenCVConsumer(AbstractConsumer):
    def __init__(self):
        self.device = getattr(self, 'device', 0)
        self.consumer = cv2.VideoCapture(self.device)

    def receive(self):
        success, frame = self.consumer.read()
        if not success: return self.receive_error(frame)
        return frame
