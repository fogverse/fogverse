import os
import sys
import yaml

from confluent_kafka import admin, KafkaError, KafkaException
from pathlib import Path

def check_server_env(server):
    new = server.copy()
    for server_name, host in new.items():
        if host.startswith('$'):
            host = os.getenv(host[1:])
            assert host is not None,\
                f'Host {host} couldn\'t be found in env variables.'
        new[server_name] = host
    return new

def create_topic(topics, admin):
    if not isinstance(topics, list):
        topics = [topics]
    res = admin.create_topics(topics)
    for topic in topics:
        topic_name = topic.topic
        exc = res[topic_name].exception(5)
        if exc is not None:
            raise exc

def alter_config(topic_name, adm, config):
    if len(config) == 0: return
    res = adm.alter_configs([
        admin.ConfigResource(admin.ConfigResource.Type.TOPIC,
                            topic_name,
                            set_config=config)
    ])
    for future in res.values():
        exc = future.exception(5)
        if exc is not None:
            raise exc

def create_partitions(topic_name, adm, num_partitions):
    new_partitions = admin.NewPartitions(topic_name, num_partitions)
    res = adm.create_partitions([new_partitions])
    for future in res.values():
        exc = future.exception(5)
        if exc is not None:
            err = exc.args[0]
            if err.code() == KafkaError.INVALID_PARTITIONS: continue
            raise exc

def read_topic_yaml(filepath: Path, str_format={}):
    admins = {}
    with filepath.open() as f:
        config = yaml.safe_load(f)
    topic = config['topic']
    server = check_server_env(config['server'])
    for server_name, host in server.items():
        admins[server_name] = admin.AdminClient({'bootstrap.servers':host})
    for topic, attr in topic.items():
        _partition = attr.get('partitions', 1)
        _config = attr.get('config', {})
        _server = attr['server']

        _pattern = attr.get('pattern')
        if _pattern is not None:
            _topic_name = _pattern.format(**str_format)
        else:
            _topic_name = topic
        new_topic_instance = admin.NewTopic(_topic_name,
                                            num_partitions=_partition,
                                            config=_config)

        if not isinstance(_server, list):
            _server = [_server]
        for server_name in _server:
            adm = admins[server_name]
            try:
                create_topic(new_topic_instance, adm)
                status = 'created'
            except KafkaException as e:
                err = e.args[0]
                if err.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    print(f'Topic {_topic_name} on server {server_name} already \
exists, altering configs...')
                    create_partitions(_topic_name, adm, _partition)
                    alter_config(_topic_name, adm, _config)
                    status = 'altered'

        print(f'Topic {_topic_name} for server {_server} has been {status}')
    print('Done')

if __name__ == '__main__':
    filepath = Path(sys.argv[1])
    read_topic_yaml(filepath)
