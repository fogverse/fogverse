# FogVerse
Description, Contribution, etc coming soon.

## Usage
See [Live YOLOv5 Implementation](https://github.com/fogverse/live-yolov5).

### Auto Scaling Usage

**NOTICE**

This usage tutorial is under assumption that you understand how to use FogVerse in basic usage such as creating producer or consumer. If you have not understand it fully, then you can see the YOLOv5 implementation from above.

If you want to use auto scale feature there are two things that you need to set up, the `Master` and `machine` that wants to used for the auto scaling.  

To create master, you can either use class from `auto_scale` module which has `Master` class following the `worker` that has could also be used for development. If you wanted to use the whole implementation then you can use `MasterComponent` class which provides simpler API for you to create the whole `Master`.

#### Master Part

To use `MasterComponent`, you can use it as  follows.

```py
from auto_scale import AutoScaleComponent, MasterAutoScalerConfig, Worker

auto_scale_component = AutoScaleComponent()

conf : MasterAutoScalerConfig = {
    'consumer_topic' : 'test',
    'consumer_server' : 'localhost:9092',
    'consumer_group_id' : 'test',
    'auto_deployer_config' : {
        'deploy_delay' : 30,
        'after_heartbeat_delay' : 30
    },
    'statistic_worker_config': {
        'z_value' : 1.5,
        'refresh_rate' : 1,
        'maximum_seconds' : 600
    },
    'included_worker' : [Worker.AUTO_DEPLOYER, Worker.DISTRIBUTED_LOCK_WORKER, Worker.STATISTIC_WORKER, Worker.INPUT_OUTPUT_WORKER],
    'distributed_worker_config' : {
        'master_host' : 'localhost',
        'master_port' : 4242
    },
    'input_output_worker_config' : {
        'refresh_rate_second': 60,
        'input_output_ratio_threshold': 0.7
    }
}

deploy_scripts = DeployScripts()

master = auto_scale_component.master_auto_scaler(conf, deploy_scripts)

await master.run()
```

This will automatically create a master component with all worker running. If you don't want all of the worker to run for instance you only need the auto scale without the distributed lock worker, then you can remove it from the included worker. However, if you have something else that you wanted to modify, then you could create your own Master for auto scaling like so. 

If you noticed that there is a `DeployScripts` class which is injected. The usage of `DeployScripts` will be explained later on **Node Part**.

```py
master = Master(
    consumer_topic=topic,
    consumer_group_id=group_id,
    consumer_servers=server,
    observers=[] # your own observer,
    possible_data_types=[] # possible data types from that observer could receive
)
```

If you find that you don't need to have Master from the first place or using Master made it seems harder than it should be, then you can continue creating your own FogVerse implementation. The whole point of having the component is to reduce the overhead of configuring things by yourself.  

#### Node Part

Then, in Node part, you need to create observer each time you succeeded on sending data or if you wanted to create an auto scaling request. To create observer, you can do it as follows.

```py

auto_scale_component = AutoScaleComponent()

observer_conf : ObserverConfig = {
    'kafka_server' : 'localhost',
    'producer_topic': 'test'
}

observer = auto_scale_component.observer()
```

**Note:** `producer_topic` and `consumer_topic` from master config has to be the same because `observer` will send the data towards `Master` via Kafka broker.

Then there are two usage possible from observer 

1. Sending auto scale request 
2. Sending heartbeat

Sending auto scale request is not mandatory, however sending heartbeat is mandatory. This is because `Master` needs to know each Node perform. For sending heartbeat, on `send` function of FogVerse `Runnable` class, you can overwrite it as follows.  

```py
async def send(self, data, topic=None, key=None, headers=None, callback=None):
    result = await super().send(data, topic, key, headers, callback)
    self.observer.send_heartbeat(self.producer_topic, total_messages=1)
    return result
```

This will send how much message does it process upon sending. Note that you need to inject observer to your class as well.

For sending auto scale request, you need to overwrite the `start_consumer` method on `Consumer` class such as follows

```py
    async def start_consumer(self):
        result = await super().start_consumer()

        self.observer.send_auto_scale_request(
            source_topic=self.consumer_topic,
            target_topic=self.producer_topic,
            topic_configs={
                'provider' : 'LOCAL',
                'service_name' : 'test',
                'max_instance' : 3,
                'extra_configs' : {
                    'test' : "halo"
                }
            }
        )

        return result
```

`DeployScript` that we have injected on component will use `topic_configs`. You can implement your own deployment script such as follows.

```py

def _local_deployment(logger: Logger, configs: DeployConfig):
    logger.info(configs['extra_configs']['test'])
    logger.info("No implementation were made")
    return None

deploy_scripts = DeployScripts()
deploy_scripts.set_deploy_functions('LOCAL', _local_deployment)
```

Notice that the value `provider` and the string of `set_deploy_functions` are the same. `provider` indicates the key of the function that you wanted to use. In this case, `_local_deployment` is assigned with key `LOCAL`.  You could assign whatever key you wanted as long as you are aware which function will be called.

The return value that you should return on `set_deploy_functions` is explained on the description usage of the function, you can read from there if you need to know how the specification of `set_deploy_functions` work.


### Dynamic Partition Usage

To use dynamic partition on `Consumer` class, you need `ConsumerStart` class which is a helper class for deciding how starting consumer will be done. Then, you also need a `Master` which will act as the central of `lock`. Specifically, what you need from `Master` is `DISTRIBUTED_LOCK_WORKER` you can refer to Auto Scale Usage on how to set `Master`.

To add `ConsumerStart` class, you use the following approach.  

```py
kafka_admin = AdminClient(
    conf={
        "bootstrap.servers": "localhost"
    },
)
consumer_start = ConsumerStart(
    kafka_admin,
    sleep_time=3,
    master_host='localhost',
    master_port=4242,
    initial_total_partition=1
)
```

Then, on `start_consumer` method on `Consumer` class, you need to override as follows

```py
async def start_consumer(self):
    await self.consumer_start.start_with_distributed_lock(
        self.consumer,
        self.consumer_topic,
        self.group_id,
        str(uuid4()),
    )
```
This will automatically joins topic dynamically without setting up the partition statically.

Note that however you need to assign `metadata_max_age_ms` configuration low enough (10 seconds for example) to make sure on the `Producer` and `Consumer` side that there exist a new partition. Otherwise, you might need to wait 5 minutes since it's the default value for metadata update on Kafka Nodes.    

### Multiprocessing Usage

To use `multiprocessing` is just to replace the inheritance from `Consumer` to `ParallelConsumer` with the same rule applies `Producer`. You need to be aware that on `run` method, there are extra parameter that needs to be injected. Also, the `process` method will not be used so you need to move the logic into a `Processor` interface.

To show you what i meant, here is the contract for `run` method on `ParallelRunnable`.

```py
async def run(self, process_number : int, max_running_task : int, processor : Processor):
```

The explaination for each parameter are explained on the function. On `processor`, you need to make sure that all object on `processor` are **pickable** otherwise this will not work otherwise this will not work.

Also, you need to make sure that `max_running_task` are not too much otherwise most messages will be `consumed` and waiting on the `processor` queue therefore could cause performance issue on the long run. 


## Keywords
Internet of Things, Fog Computing, Edge Computing, Real-time, Video
Preprocessing.

## Technologies
Docker, Kubernetes.

## TODOs
- [ ] Code Documentation.
- [ ] Testing.
- [ ] Code Styling and Linter.
- [ ] Logging.
- [ ] PyPI.

## Future Works / Paper Opportunity
- [ ] Dynamic resource provisioning, based on workload.
- [ ] Dynamic kafka partitioning.

## Cite this Work
Coming soon.

## License
MIT License, see [LICENSE](LICENSE) file.

## References
Coming soon.
