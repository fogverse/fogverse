
import asyncio
from typing import Optional

from aiokafka.conn import functools
from auto_scale.base import (
    MasterWorker,
    NodeHeartBeat,
    TopicStatistic
)
from consumer_util.base import LockRequest, LockResponse, UnlockRequest, UnlockResponse
from fogverse_logging import get_logger

class StatisticWorker(MasterWorker, TopicStatistic):

    def __init__(self, maximum_seconds: int, refresh_rate: float = 1):
        '''
        A sliding window statistic approach for calculating the statistics of topic total throughput.
        maximum_seconds : int (seconds) = maximum amount of seconds for the sliding window time.
        refresh_rate: int (seconds) =  how frequent the worker will refresh the observed topic throughput, 
        by default it will refresh each second.
        '''
        assert maximum_seconds > 0
        assert refresh_rate > 0

        self._maximum_seconds = maximum_seconds
        self._refresh_rate = refresh_rate   
        self._topics_current_count: dict[str, int] = {} 
        self._topics_observed_counts: dict[str, list[int]] = {}
        self._logger = get_logger(name=self.__class__.__name__)

        self._stop = False

    def on_receive(self, data):
        if not isinstance(data, NodeHeartBeat):
            return
    
        topic_current_count = self._topics_current_count.get(data.target_topic, 0)
        topic_current_count += data.total_messages
        self._topics_current_count[data.target_topic] = topic_current_count

    def _add_observed_topic_counts(self, topic, total_counts: int):
        observed_counts = self._topics_observed_counts.get(topic, [])

        if len(observed_counts) >= self._maximum_seconds:
            observed_counts.pop(0)

        observed_counts.append(total_counts)
        self._topics_observed_counts[topic] = observed_counts

    async def start(self):
        self._logger.info("Starting statistic worker")
        while not self._stop:
            await asyncio.sleep(self._refresh_rate)

            for topic, current_counts in self._topics_current_count.items():
                self._add_observed_topic_counts(topic, current_counts)
                self._topics_current_count[topic] = 0 

    def get_topic_mean(self, topic: str) -> float:
        try:
            topic_observed_count = self._topics_observed_counts[topic]
            total_observed_counts = sum(topic_observed_count)
            return total_observed_counts/len(topic_observed_count)
        except Exception as e:
            self._logger.error(e)
            return 0

    def get_topic_standard_deviation(self, topic: str) -> float:
        try:
            topic_observed_count = self._topics_observed_counts[topic]
            total_observed_counts = sum(topic_observed_count)
            observed_counts_size = len(topic_observed_count)
            avg_observed_counts = total_observed_counts/(observed_counts_size)

            total_diff_squared_observed_counts = functools.reduce(
                lambda cumulative_sum, observed_count: (observed_count - avg_observed_counts)**2 + cumulative_sum,
                topic_observed_count,
                0
            )

            topic_variance = total_diff_squared_observed_counts/observed_counts_size

            return topic_variance**(1/2)

        except Exception as e:
            self._logger.error(e)
            return 0
    
    async def stop(self):
        self._stop = True

class DistributedWorkerServerWorker(MasterWorker):

    MAX_RECEIVE_BYTE = 1024

    def __init__(self, master_host : str, master_port : int):
        self._logger = get_logger(name=self.__class__.__name__)
        self.master_host = master_host
        self.master_port = master_port
        self.request_lock = asyncio.Lock()
        self.current_consumer_id : Optional[str] = None
        self._stop = False

        self.server : Optional[asyncio.Server] = None

    def parse_request(self, request: bytes):
        try:
            return LockRequest.decode(request)
        except Exception:
            return UnlockRequest.decode(request)


    async def handle_request(self, reader : asyncio.StreamReader, writer : asyncio.StreamWriter):
        while not self._stop:
            try:
                await self.request_lock.acquire()
                request = (await reader.read(DistributedWorkerServerWorker.MAX_RECEIVE_BYTE))

                if len(request) == 0: 
                    return

                parsed_request = self.parse_request(request)
                self._logger.info(parsed_request)

                if isinstance(parsed_request, LockRequest):
                    # not allowed to request lock when there is already another consumer locking
                    if self.current_consumer_id:
                        lock_response = LockResponse(can_lock=False)
                        writer.write(lock_response.encode())
                        await writer.drain()
                    else:
                        self.current_consumer_id = parsed_request.lock_consumer_id
                        lock_response = LockResponse(can_lock=True)
                        writer.write(lock_response.encode())
                        await writer.drain()
                else:
                    if parsed_request.unlock_consumer_id == self.current_consumer_id:
                        self.current_consumer_id = None
                        unlock_response = UnlockResponse(is_unlocked=True)
                        writer.write(unlock_response.encode())
                        await writer.drain()
                    else:
                        unlock_response = UnlockResponse(is_unlocked=False)
                        writer.write(unlock_response.encode())
                        await writer.drain()

            except Exception as e:
                self._logger.error(e)
            finally:
                self.request_lock.release()
