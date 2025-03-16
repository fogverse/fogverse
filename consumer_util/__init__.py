import time
import socket
from aiokafka import AIOKafkaConsumer

from aiokafka.client import asyncio
from confluent_kafka.admin import (
    AdminClient,
    ConsumerGroupDescription,
    KafkaError,
    TopicDescription,
    NewPartitions,
    NewTopic
)

from confluent_kafka import Consumer, KafkaException, TopicCollection
from consumer_util.base import LockRequest, LockResponse, UnlockRequest, UnlockResponse
from fogverse_logging import get_logger

class ConsumerStart:

    lock = asyncio.Lock()
    OFFSET_OUT_OF_RANGE = -1001 
    MAX_BYTE = 1024
    
    def __init__(self, 
                 kafka_admin: AdminClient,
                 sleep_time: int,
                 master_host : str,
                 master_port : int,
                 initial_total_partition: int=1):
        self._kafka_admin = kafka_admin
        self._sleep_time = sleep_time
        self._initial_total_partition = initial_total_partition
        self.master_host = master_host
        self.master_port = master_port
        self.consumer_is_assigned_partition = False

        self._logger = get_logger(name=self.__class__.__name__)
    
    def _group_id_total_consumer(self, group_id: str) -> int:
        group_future_description = self._kafka_admin.describe_consumer_groups([group_id])[group_id]
        group_description: ConsumerGroupDescription = group_future_description.result()
        return len(group_description.members)
    
    def _topic_id_total_partition(self, topic_id: str) -> int:
        topic_future_description = self._kafka_admin.describe_topics(TopicCollection([topic_id]))[topic_id]
        topic_description: TopicDescription = topic_future_description.result()
        return len(topic_description.partitions)
    
    def _add_partition_on_topic(self, topic_id: str, new_total_partition: int):
        future_partition = self._kafka_admin.create_partitions([NewPartitions(topic_id, new_total_partition)])[topic_id]

        # waits until the partition is created
        future_partition.result()

    def _topic_exist(self, topic_id: str, retry_count: int) -> bool:
        total_retry = 0

        while total_retry <= retry_count:
            try:
                topic_future_description = self._kafka_admin.describe_topics(TopicCollection([topic_id]))[topic_id]
                topic_future_description.result()
                return True
            except KafkaException as e:
                error = e.args[0]
                if error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART or not error.retriable():
                    return False
                self._logger.info(e)
                self._logger.info("Retrying to check if topic exist")

            total_retry += 1
            time.sleep(self._sleep_time)

        return False

    def _create_topic(self, topic_id: str, retry_count: int) -> bool:
        total_retry = 0

        while total_retry <= retry_count:
            try:
                create_topic_future = self._kafka_admin.create_topics([
                    NewTopic(
                        topic_id,
                        num_partitions=self._initial_total_partition
                    )
                ])[topic_id]
                create_topic_future.result()
                self._logger.info(f"{topic_id} is created")
                return True

            except KafkaException as e:
                error = e.exception.args[0]
                
                if error.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    self._logger.info(f"{topic_id} already created")
                    return True

                self._logger.info(e)
                if not error.retriable():
                    return False
                self._logger.info(f"Retrying creating topic {topic_id}")
                total_retry += 1
                time.sleep(self._sleep_time)

        return False

    async def start_with_distributed_lock(self,
                                          consumer: AIOKafkaConsumer,
                                          topic_id: str,
                                          group_id: str,
                                          consumer_id : str,
                                          /,
                                          retry_attempt: int=3):

        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        try:
            self._logger.info("Creating topic if not exist")
            self.topic_id = topic_id

            if not self._topic_exist(topic_id, retry_attempt):
                topic_created = self._create_topic(topic_id, retry_attempt)
                if not topic_created:
                    raise Exception("Topic cannot be created")

            self._logger.info(f"Subscribing to topic {topic_id}")

            client.connect((self.master_host, self.master_port))

            # acquiring lock from master
            request = LockRequest(lock_consumer_id=consumer_id)
            request_byte = request.model_dump_json().encode()
            can_lock = False

            while not can_lock:
                try:
                    self._logger.info(f"{consumer_id} is sending lock request to master")
                    client.sendall(request_byte)
                    data = client.recv(ConsumerStart.MAX_BYTE)
                    lock_response = LockResponse.model_validate_json(data, strict=True)
                    can_lock = lock_response.can_lock

                    if not can_lock:
                        self._logger.info(f"Lock request rejected, retrying in {self._sleep_time} seconds")
                    time.sleep(self._sleep_time)
                except Exception as e:
                    self._logger.error(e)

            # assigning partition to consumer
            partition_is_enough = False

            self._logger.info("Checking if partition is enough")

            while not partition_is_enough:

                group_id_total_consumer = self._group_id_total_consumer(group_id) + 1 # including itself
                topic_id_total_partition = self._topic_id_total_partition(topic_id)

                self._logger.info(f"\ngroup_id_total_consumer: {group_id_total_consumer}\ntopic_id_total_partition:{topic_id_total_partition}")

                partition_is_enough = topic_id_total_partition >= group_id_total_consumer

                if not partition_is_enough:
                    self._logger.info(f"Adding {group_id_total_consumer} partition to topic {topic_id}")
                    self._add_partition_on_topic(topic_id, group_id_total_consumer)

                time.sleep(self._sleep_time)

            self._logger.info("Partition is enough, joining consumer group")

            self._logger.info("Waiting for consumer to be assigned on a consumer group")

            consumer.subscribe(topics=[topic_id])

            await consumer.start()

            while True:

                consumer_partition_assignment = consumer.assignment()
                self.consumer_is_assigned_partition = len(consumer_partition_assignment) != 0

                if self.consumer_is_assigned_partition:
                    self._logger.info(f"Consumer is assigned to {len(consumer_partition_assignment)} partitions, releasing lock")
                    break

                self._logger.info("Fail connecting, retrying...")
                time.sleep(self._sleep_time)

            unlock_request = UnlockRequest(unlock_consumer_id=consumer_id)

            is_unlocked = False

            while not is_unlocked:
                try:
                    self._logger.info(f"{consumer_id} is sending unlock request to master")
                    client.send(unlock_request.model_dump_json().encode())
                    data = client.recv(ConsumerStart.MAX_BYTE)
                    unlock_response = UnlockResponse.model_validate_json(data, strict=True)
                    is_unlocked = unlock_response.is_unlocked
                    if not is_unlocked:
                        self._logger.info(f"Unlock request rejected, retrying in {self._sleep_time} seconds")
                    time.sleep(self._sleep_time)

                except Exception as e:
                    self._logger.error(e)

            client.close()

        except Exception as e:
            self._logger.error(e)

        finally:
            client.close()
