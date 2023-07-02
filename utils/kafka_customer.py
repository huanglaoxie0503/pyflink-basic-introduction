#!/usr/bin/python
# -*- coding:UTF-8 -*-
import json
from kafka import KafkaConsumer


def consumer_demo():
    consumer = KafkaConsumer(
        'topic_flink',
        bootstrap_servers='192.168.40.131:9092',
        group_id='test'
    )
    for message in consumer:
        print("receive, key: {}, value: {}".format(
            json.loads(message.key.decode()),
            json.loads(message.value.decode())
            )
        )


if __name__ == '__main__':
    consumer_demo()