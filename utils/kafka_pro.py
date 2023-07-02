#!/usr/bin/python
# -*- coding:UTF-8 -*-
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import kafka_errors
import traceback
import json

SERVER = '192.168.40.131:9092'
# USERNAME = 'kingname'
# PASSWORD = 'kingnameisgod'
TOPIC = 'topic_flink'


def read_json_file(file_name):
    with open(file_name, "r", encoding="utf-8") as f:
        content = json.load(f)
        return content


def producer_demo(items):
    # 假设生产的消息为键值对（不是一定要键值对），且序列化方式为json
    producer = KafkaProducer(
        bootstrap_servers=['192.168.40.131:9092'],
        key_serializer=lambda k: json.dumps(k).encode(),
        value_serializer=lambda v: json.dumps(v).encode())
    # 发送三条消息
    # for i in range(0, 3):
    future = producer.send(
        'ecu',
        key='count_num',  # 同一个key值，会被送至同一个分区
        value=items)  # 向分区1发送消息
    print("send {}".format(items))
    try:
        future.get(timeout=10)  # 监控是否发送成功
    except kafka_errors:  # 发送失败抛出kafka_errors
        traceback.format_exc()


if __name__ == '__main__':
    items = []
    path = "/Users/oscar/data/JSON1.json"
    data = read_json_file(file_name=path)
    # for d in data:
    #     d['parse_data'] = json.loads(d['parse_data'])
    #
    #     items.append(d)
    # r = json.dumps(items)
    # print(r)
    producer_demo(items=data)
