
import asyncio
from functools import partial

from pydantic import BaseModel
from enum import Enum

from auto_scale.base import AutoScaleRequest, MasterWorker, NodeHeartBeat
from auto_scale.consumer_observer import Observer
from auto_scale.worker import AutoDeployer, DeployScripts, DistributedWorkerServerWorker, InputOutputRatioWorker, StatisticWorker, TopicSpikeChecker
from base import AbstractProducer
from consumer_producer import AIOKafkaConsumer
from fogverse_logging import get_logger
from general import Runnable
from typing import Optional, TypedDict

class Master(AIOKafkaConsumer, AbstractProducer, Runnable):

    def __init__(self, 
                 consumer_topic: str, 
                 consumer_servers: str,
                 consumer_group_id: str,
                 observers: list[Optional[MasterWorker]],
                 possible_data_types : list[type[BaseModel]]):

        self.consumer_topic =  consumer_topic
        self.consumer_servers = consumer_servers
        self.group_id = consumer_group_id
        self._log = get_logger(name=self.__class__.__name__)

        self.possible_data_types : list[type[BaseModel]] = possible_data_types

        self.auto_decode = False
        AIOKafkaConsumer.__init__(self)

        self._closed = False
        self._observers = observers
        
    def decode(self, data: bytes):
        for data_types in self.possible_data_types:
            try:
                return data_types.model_validate_json(data, strict=True)
            except:
                continue
        self._log.error(f"Not a valid request {data}")
        return None

    
    async def process(self, data):
        for observer in self._observers:
            if not observer:
                continue
            observer.on_receive(data)

    def encode(self, data):
        pass

    async def send(self, data, topic=None, key=None, headers=None, callback=None):
        pass

    async def _start(self):
        [asyncio.create_task(observer.start()) if observer else None for observer in self._observers]
        await super()._start()

    async def close_consumer(self):

        for observer in self._observers:
            if not observer:
                continue
            await observer.stop()

        await super().close_consumer()

class StatisticWorkerConfig(TypedDict):
    maximum_seconds : int
    refresh_rate : float
    z_value: float


class InputOutputWorkerConfig(TypedDict):
    refresh_rate_second : float
    input_output_ratio_threshold: float

class DistributedWorkerConfig(TypedDict):
    master_host : str
    master_port : int

class AutoDeployerConfig(TypedDict):
    deploy_delay : float
    after_heartbeat_delay : float

class ObserverConfig(TypedDict):
    producer_topic: str
    kafka_server : str

class Worker(Enum):
    AUTO_DEPLOYER = 0
    INPUT_OUTPUT_WORKER = 1
    DISTRIBUTED_LOCK_WORKER = 2
    STATISTIC_WORKER = 3

class MasterAutoScalerConfig(TypedDict):
    consumer_topic : str
    consumer_group_id : str
    consumer_server : str
    input_output_worker_config : Optional[InputOutputWorkerConfig]
    statistic_worker_config : Optional[StatisticWorkerConfig]
    distributed_worker_config : Optional[DistributedWorkerConfig]
    auto_deployer_config : Optional[AutoDeployerConfig]
    included_worker: list[Worker]

class AutoScaleComponent:
        
    def master_auto_scaler(self, master_config : MasterAutoScalerConfig, deploy_script : DeployScripts):

        statistic_worker_conf = master_config['statistic_worker_config']
        input_output_worker_conf = master_config['input_output_worker_config']
        distributed_lock_worker_conf = master_config['distributed_worker_config']
        auto_deployer_conf = master_config['auto_deployer_config']

        statistic_worker = None
        input_output_worker = None
        auto_deployer = None
        distributed_lock_worker = None
        topic_spike_checker = None

        spike_check_func = None

        if statistic_worker_conf != None:
            statistic_worker = StatisticWorker(
                maximum_seconds=statistic_worker_conf['maximum_seconds'],
                refresh_rate=statistic_worker_conf['refresh_rate']
            )
            topic_spike_checker = TopicSpikeChecker(statistic_worker)
            spike_check_func = partial(topic_spike_checker.check_spike_by_z_value, statistic_worker_conf['z_value'])

        if auto_deployer_conf != None:
    
            auto_deployer = AutoDeployer(
                deploy_script=deploy_script,
                should_be_deployed=spike_check_func,
                **auto_deployer_conf
            )

        if input_output_worker_conf != None:

            input_output_worker = InputOutputRatioWorker(
                **input_output_worker_conf,
                deployer=auto_deployer
            )

        if distributed_lock_worker_conf != None:
            distributed_lock_worker = DistributedWorkerServerWorker(
                **distributed_lock_worker_conf
            )

        available_workers : dict[Worker, Optional[MasterWorker]] = {
            Worker.DISTRIBUTED_LOCK_WORKER : distributed_lock_worker,
            Worker.INPUT_OUTPUT_WORKER :input_output_worker,
            Worker.STATISTIC_WORKER : statistic_worker,
            Worker.AUTO_DEPLOYER: auto_deployer
        }

        used_worker : list[Optional[MasterWorker]] = list(map(lambda w : available_workers[w], master_config['included_worker']))

        return Master(
            consumer_topic=master_config['consumer_topic'],
            consumer_group_id=master_config['consumer_group_id'],
            consumer_servers=master_config['consumer_server'],
            observers=used_worker,
            possible_data_types=[AutoScaleRequest, NodeHeartBeat]
        )

    def observer(self, config : ObserverConfig):
        return Observer(**config)
