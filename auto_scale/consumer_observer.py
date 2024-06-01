
import dataclasses
import json
from typing import Any, Optional
from confluent_kafka import Producer
from auto_scale.base import AutoScaleRequest, NodeHeartBeat

from fogverse_logging import get_logger
from util import get_timestamp


class ProducerObserver:

    def __init__(self, producer_topic: str, kafka_server : str):

        self.producer = Producer({
            "bootstrap.servers": kafka_server
        })

        self._producer_topic = producer_topic 
        self._logger = get_logger(name=self.__class__.__name__)
        

    def send_auto_scale_request(self,
                                     source_topic: str,
                                     target_topic: str,
                                     topic_configs: Optional[Any]):
        '''
        Identify which topic pair should the observer ratio with
        send: a produce function from kafka
        '''
        self._logger.info(f"Sending input output ratio to topic {self._producer_topic}")
        if source_topic is not None:

            data = AutoScaleRequest(
                source_topic=source_topic, 
                target_topic=target_topic, 
                deploy_configs=topic_configs
            )

            data = json.dumps(dataclasses.asdict(data)).encode('utf-8')

            self.producer.produce(topic=self._producer_topic, value=data)
    
    def send_heartbeat(
            self,
            target_topic: str,
            total_messages: int
        ):
        if target_topic is not None:

            data = NodeHeartBeat(
                target_topic=target_topic,
                total_messages=total_messages,
                timestamp=int(get_timestamp().timestamp()),
            )

            data = json.dumps(dataclasses.asdict(data)).encode('utf-8')

            self.producer.produce(topic=self._producer_topic, value=data)
            self.producer.flush()
