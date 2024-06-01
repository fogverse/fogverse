
from abc import ABC, abstractmethod
from dataclasses import dataclass

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
