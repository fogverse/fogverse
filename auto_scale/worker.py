
import asyncio
from asyncio.tasks import Task
from collections.abc import Callable
from datetime import datetime, timedelta
from logging import Logger
from typing import Any, Optional, Tuple

from aiokafka.conn import functools
from auto_scale.base import (
    AutoScaleRequest,
    DeployArgs,
    DeployConfig,
    MasterWorker,
    NodeHeartBeat,
    TopicDeployDelay,
    TopicStatistic
)
from consumer_util.base import LockRequest, LockResponse, UnlockRequest, UnlockResponse
from fogverse_logging import get_logger
from util import get_timestamp

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
            return LockRequest.model_validate_json(request)
        except Exception:
            return UnlockRequest.model_validate_json(request)

    def on_receive(self, data):
        pass
    
    async def start(self):
        self._logger.info(f"Starting distributed lock worker with host {self.master_host} {self.master_port}")
        self.server = await asyncio.start_server(self.handle_request, self.master_host, self.master_port)

        async with self.server:
            await self.server.serve_forever()

    async def stop(self):
        if self.server:
            self.server.close()

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
                        writer.write(lock_response.model_dump_json().encode())
                        await writer.drain()
                    else:
                        self.current_consumer_id = parsed_request.lock_consumer_id
                        lock_response = LockResponse(can_lock=True)
                        writer.write(lock_response.model_dump_json().encode())
                        await writer.drain()
                else:
                    if parsed_request.unlock_consumer_id == self.current_consumer_id:
                        self.current_consumer_id = None
                        unlock_response = UnlockResponse(is_unlocked=True)
                        writer.write(unlock_response.model_dump_json().encode())
                        await writer.drain()
                    else:
                        unlock_response = UnlockResponse(is_unlocked=False)
                        writer.write(unlock_response.model_dump_json().encode())
                        await writer.drain()

            except Exception as e:
                self._logger.error(e)
            finally:
                self.request_lock.release()

class DeployScripts:

    def __init__(self, log_dir_path: str='logs'):
        self._deploy_functions: dict[str, Callable[[Logger, DeployConfig], Optional[Tuple[Any, Callable[..., Any]]]]] = {}
        self._log_dir_path = log_dir_path
        self._logger = get_logger(name=self.__class__.__name__)

    def get_deploy_functions(self, cloud_provider: str):
        return self._deploy_functions[cloud_provider]

    def set_deploy_functions(self, cloud_provider: str, deploy_function: Callable[[Logger, DeployConfig],  Optional[Tuple[Any, Callable[..., Any]]]]):
        '''
        The deploy function accepts logger to allow seeing the process of deployment. Return value should return two things,
        the parameter which the shutdown function will accept and the callback for shutting down the machine. If there is no shutdown function
        then just return None
        '''
        self._deploy_functions[cloud_provider] = deploy_function

class TopicSpikeChecker:

    def __init__(self, topic_statistic: TopicStatistic):
        self._topic_statistic = topic_statistic
        self._logger = get_logger(name=self.__class__.__name__)

    def check_spike_by_z_value(self, z_threshold: float, topic_id: str, topic_throughput: float) -> bool:
        self._logger.info(f"Checking if topic {topic_id} is a spike or not")
        std = self._topic_statistic.get_topic_standard_deviation(topic_id)
        mean = self._topic_statistic.get_topic_mean(topic_id)
        z_score = (topic_throughput - mean)/std

        self._logger.info(f"Topic {topic_id} statistics:\nMean: {mean}\nStandard Deviation: {std}\nZ-Score:{z_score}")
            
        self._logger.info(f"{z_score < z_threshold}")
        return z_score < z_threshold


class AutoDeployer(MasterWorker):

    def __init__(
            self,
            deploy_script: DeployScripts,
            should_be_deployed : Optional[Callable[[str, float],bool]],
            deploy_delay: float,
            after_heartbeat_delay : float
        ):
        """
        A class to facilitate the auto deployment process for each consumer topic.

        Args:
            deploy_script : Class that will be used to deploy machine 
            should_be_deployed : A callback that deduce whether a machine should be deployed.
            deploy_delay (float): Delay (in seconds) for topic deployment. Even if a topic's throughput is still low, it won't
                be deployed after several attempts based on this delay.
            after_heartbeat_delay (float): Delay (in seconds) after a machine has successfully send heartbeat to master, this delay ensure that 
            there won't be additional request to deploy another machine right after the machine just deployed.
        """

        self._deploy_scripts = deploy_script
        self._should_be_deployed = should_be_deployed
        self._deploy_delay = deploy_delay
        self._after_heartbeat_delay = after_heartbeat_delay

        self._logger = get_logger(name=self.__class__.__name__)
        
        self._topic_deployment_configs: dict[str, DeployConfig] = {}
        self._can_deploy_topic: dict[str, TopicDeployDelay] = {}
        self._topic_total_deployment: dict[str, int] = {}
        self._topic_time_delay : dict[str, datetime] = {}

        self._delay_deploy_task : dict[str, Task] = {}
        self._after_heartbeat_delay_task : dict[str, Task] = {}
        self._shutdown_callback : list[tuple[Any, Callable[..., Any]]] = [] 

    async def delay_deploy(self, topic_id: str, sleep_time : float):
        await asyncio.sleep(sleep_time)
        self._can_deploy_topic[topic_id].can_be_deployed = True

    def get_topic_total_machine(self, topic: str) -> int:
        return self._topic_total_deployment.get(topic, 1)

    def _cancel_topic_task_delay(self, topic : str, delay_tasks : dict[str, Task]):

        if topic in delay_tasks:
            delay_task = delay_tasks[topic] 

            if not delay_task.done():
                delay_task.cancel()

    def on_receive(self, data):
        if not isinstance(data, AutoScaleRequest):
            return
        
        if data.deploy_configs != None:
            self._topic_deployment_configs[data.target_topic] = data.deploy_configs
            current_time = get_timestamp()
            if data.target_topic not in self._can_deploy_topic:
                self._can_deploy_topic[data.target_topic] = TopicDeployDelay(
                    can_be_deployed=False,
                    deployed_timestamp=current_time
                )

            self._cancel_topic_task_delay(data.target_topic, self._delay_deploy_task)
            self._cancel_topic_task_delay(data.target_topic, self._after_heartbeat_delay_task)

            self._after_heartbeat_delay_task[data.target_topic] = asyncio.create_task(
                self.delay_deploy(data.target_topic, self._after_heartbeat_delay)
            )
            self._topic_time_delay[data.target_topic] = current_time + timedelta(seconds=self._after_heartbeat_delay)

            total_deployment = self._topic_total_deployment.get(data.target_topic, 0)
            self._topic_total_deployment[data.target_topic] = total_deployment + 1

    async def start(self):
        pass
    
    async def deploy(self, deploy_args: DeployArgs) -> bool:

        try:
            target_topic, source_topic = deploy_args.target_topic, deploy_args.source_topic  
            target_total_calls, source_total_calls = deploy_args.target_topic_throughput, deploy_args.source_topic_throughput

            if target_topic not in self._can_deploy_topic:
                self._logger.info(f"Topic {target_topic} does not exist, might be not sending heartbeat during initial start or does not have deployment configs")
                return False

            async with self._can_deploy_topic[target_topic]._lock:

                if not self._can_deploy_topic[target_topic].can_be_deployed:
                    time_remaining = self._topic_time_delay[target_topic] - get_timestamp()
                    self._logger.info(f"Cannot be deployed yet, time remaining: {time_remaining}")
                    return False
            
                maximum_topic_deployment = self._topic_deployment_configs[target_topic]['max_instance']
                current_deployed_replica = self._topic_total_deployment.get(target_topic, 1)

                service_name = self._topic_deployment_configs[target_topic]['service_name']
                provider = self._topic_deployment_configs[target_topic]['provider']

                if current_deployed_replica >= maximum_topic_deployment:
                    self._logger.info(
                        f"Cannot deploy service {service_name} exceeds maximum limit.\n" 
                        f"current deployed : {current_deployed_replica}\n"
                        f"maximum replica : {maximum_topic_deployment}"
                    )
                    return False

                not_a_spike = False 
                
                if self._should_be_deployed:
                    source_topic_is_not_spike = self._should_be_deployed(source_topic, source_total_calls)
                    target_topic_is_not_spike = self._should_be_deployed(target_topic, target_total_calls) 
                    not_a_spike = source_topic_is_not_spike and target_topic_is_not_spike

                # if no detection needed then this will continue regardless it's a spike or not
                if not_a_spike or not self._should_be_deployed:
                    self._logger.info(f"Deploying new machine for service {service_name} to cloud provider: {provider}")

                    machine_deployer = self._deploy_scripts.get_deploy_functions(
                        self._topic_deployment_configs[target_topic]['provider']
                    )

                    if machine_deployer is None:
                        self._logger.info(f"No deploy script for {provider}, deployment cancelled (you might need to set up deloy script on component)")
                        return False

                    self._logger.info("Starting deployment script")
                    starting_time = get_timestamp()
                    deploy_result = machine_deployer(self._logger, self._topic_deployment_configs[target_topic])


                    if deploy_result is not None:
                        self._shutdown_callback.append(deploy_result)

                    self._logger.info(f"Deployment finished, time taken: {get_timestamp() - starting_time}")
                        
                    if not deploy_result:
                        self._logger.error(f"Deployment failed for service {service_name}")
                        return False

                    current_time = get_timestamp()

                    self._can_deploy_topic[target_topic].can_be_deployed = False
                    self._can_deploy_topic[target_topic].deployed_timestamp = current_time
                    task = asyncio.create_task(self.delay_deploy(target_topic, self._deploy_delay))
                    self._delay_deploy_task[target_topic] = task
                    self._topic_time_delay[target_topic] = current_time + timedelta(seconds=self._deploy_delay)
                    return True
            
                self._logger.info("Machine should not be deployed, could be a spike")
                return False

        except Exception as e:
            self._logger.error(e)
            return False
    
    async def stop(self):
        for args, callback in self._shutdown_callback:
            try:
                callback(args)
            except Exception as e:
                self._logger.error(f"Fail shutting down machine with args {args} with cause {e}", args, e)

class InputOutputRatioWorker(MasterWorker):

    def __init__(
            self,
            refresh_rate_second: float,
            input_output_ratio_threshold: float,
            deployer : Optional[AutoDeployer]
        ):
        '''
        Worker that helps for counting input output ratio of topic
        refresh_rate_second (second) : How frequent the worker will count the ratio between input and output ratio.
        input_output_ratio_threshold : Ranging from 0.0 to 1.0, if the ratio is below the threshold and fulfill certain criteria, it will deploy a new instance 
        '''

        assert refresh_rate_second > 0
        assert input_output_ratio_threshold > 0
        assert input_output_ratio_threshold <= 1
        
        self._refresh_rate_second = refresh_rate_second
        self._input_output_ratio_threshold = input_output_ratio_threshold
        self._deployer = deployer

        self._topics_current_count: dict[str, int] = {} 
        self._topics_throughput_pair: dict[str, list[str]] = {}
        self._logger = get_logger(name=self.__class__.__name__)

        self._stop = False
        
    
    def on_receive(self, data):
        if isinstance(data, AutoScaleRequest):
            self._logger.info(f"Received input output through pair data: {data}")
            target_topics = self._topics_throughput_pair.get(data.source_topic, [])
            target_topics.append(data.target_topic)
            self._topics_throughput_pair[data.source_topic] = target_topics
        elif isinstance(data, NodeHeartBeat):
            topic_current_count = self._topics_current_count.get(data.target_topic, 0)
            topic_current_count += data.total_messages
            self._topics_current_count[data.target_topic] = topic_current_count

    def _flush(self):
        for topic, throughput in self._topics_current_count.items():
            self._logger.info(f"Topic {topic} total message in {self._refresh_rate_second} seconds: {throughput}")
            self._topics_current_count[topic] = 0

    async def start(self):
        self._logger.info("Starting input output ratio worker")
        while not self._stop:
            await asyncio.sleep(self._refresh_rate_second)
            for source_topic, target_topics in self._topics_throughput_pair.items():
                source_topic_throughput = self._topics_current_count.get(source_topic, 0)
                for target_topic in target_topics:
                    target_topic_throughput = self._topics_current_count.get(target_topic, 0)
                    throughput_ratio = target_topic_throughput/max(source_topic_throughput, 1)

                    if throughput_ratio == target_topic_throughput:
                        self._logger.info(f"Source topic {source_topic} throughput is {source_topic_throughput}, the machine might be dead")
                        return 

                    self._logger.info(f"Ratio between topic {target_topic} and {source_topic} is: {throughput_ratio}")
                    if throughput_ratio < self._input_output_ratio_threshold:
                        self._logger.info(f"{throughput_ratio} is less than threshold: {self._input_output_ratio_threshold}")

                        if not self._deployer:
                            self._logger.info("No deploying scheme set will continue")
                            continue

                        await self._deployer.deploy(
                            DeployArgs(
                                source_topic=source_topic,
                                source_topic_throughput=source_topic_throughput/self._refresh_rate_second,
                                target_topic=target_topic,
                                target_topic_throughput=target_topic_throughput/self._refresh_rate_second
                            )
                        )
            
            self._flush()
            
    async def stop(self):
        self._stop = True 
