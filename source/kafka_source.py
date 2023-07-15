#!/usr/bin/python
# -*- coding:UTF-8 -*-
import json

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy


if __name__ == '__main__':
    brokers = 'localhost:9092'

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.AUTOMATIC)

    env.add_jars("file:///Users/oscar/software/jars/flink-sql-connector-kafka-1.17.0.jar")

    source = KafkaSource.builder() \
        .set_bootstrap_servers(brokers) \
        .set_topics("crawl") \
        .set_group_id("my-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    kafka_data_stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    def str_to_dict(data):
        """
        获取 WaterSensor id 、更新相应的value值
        :param data: 字符串数据流
        :return: symbol、volume
        """
        json_data = json.loads(data)
        return json_data.get('symbol'), int(json_data.get('volume'))

    map_stream = kafka_data_stream.map(str_to_dict)

    keyed_stream = map_stream.key_by(lambda x: x[0]).sum(1).print()

    env.execute()