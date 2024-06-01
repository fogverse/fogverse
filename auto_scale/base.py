
from abc import ABC, abstractmethod
import asyncio
from dataclasses import dataclass
from typing import Any, Optional
from datetime import datetime

class MasterWorker(ABC):

    @abstractmethod
    def on_receive(self, data):
        pass

    @abstractmethod
    async def start(self):
        pass

    @abstractmethod
    async def stop(self):
        pass

class TopicStatistic(ABC):

    @abstractmethod
    def get_topic_mean(self, topic: str) -> float:
        pass 

    @abstractmethod
    def get_topic_standard_deviation(self, topic: str) -> float:
        pass


@dataclass
class NodeHeartBeat:
    target_topic: str
    timestamp: int
    total_messages: int

@dataclass
class DeployArgs:
    source_topic: str
    source_topic_throughput: float
    target_topic: str
    target_topic_throughput: float

@dataclass
class AutoScaleRequest:
    source_topic: str
    target_topic: str
    deploy_configs : Optional[Any]


@dataclass
class TopicDeployDelay:
    can_be_deployed: bool
    deployed_timestamp: datetime 
    _lock: asyncio.Lock = asyncio.Lock()
