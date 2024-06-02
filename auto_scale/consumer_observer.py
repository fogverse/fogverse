
from asyncio import iscoroutine
import dataclasses
import json
from typing import Optional
from aiokafka.conn import functools
from confluent_kafka import Producer
from auto_scale.base import AutoScaleRequest, DeployConfig, NodeHeartBeat

from fogverse_logging import get_logger
from util import get_timestamp


class Observer:
    '''
    An observer class that helps to send data such as auto scale request and heartbeat to master.
    This class is mandatory for auto scaling so you should include this class when you wanted to have auto scaling on you system,
    Otherwise the master will not have sufficient data to initiate auto scaling.
    '''

    def __init__(self, producer_topic: str, kafka_server : str):

        self.producer = Producer({
            "bootstrap.servers": kafka_server
        })
        self._producer_topic = producer_topic 
        self._logger = get_logger(name=self.__class__.__name__)
        

    def send_auto_scale_request(self,
                                source_topic: str,
                                target_topic: str,
                                topic_configs: Optional[DeployConfig]):
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

            self.producer.produce(topic=self._producer_topic, value=data.model_dump_json().encode())
    
    def send_heartbeat(
            self,
            target_topic: Optional[str],
            total_messages: int
        ):
        if target_topic is not None:

            data = NodeHeartBeat(
                target_topic=target_topic,
                total_messages=total_messages,
                timestamp=int(get_timestamp().timestamp()),
            )

            self.producer.produce(topic=self._producer_topic, value=data.model_dump_json().encode())
            self.producer.flush()
