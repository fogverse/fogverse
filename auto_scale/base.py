
from abc import ABC, abstractmethod
import asyncio
from typing import Any, Optional
from datetime import datetime
from pydantic import BaseModel, ConfigDict

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


class NodeHeartBeat(BaseModel):
    target_topic: str
    timestamp: int
    total_messages: int

class DeployArgs(BaseModel):
    source_topic: str
    source_topic_throughput: float
    target_topic: str
    target_topic_throughput: float

class AutoScaleRequest(BaseModel):
    source_topic: str
    target_topic: str
    deploy_configs : Optional[dict]


class TopicDeployDelay(BaseModel):
    model_config = ConfigDict(ignored_types=(asyncio.Lock, ))
    can_be_deployed: bool
    deployed_timestamp: datetime 
    _lock: asyncio.Lock = asyncio.Lock()
