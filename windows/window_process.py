#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import SimpleStringSchema, WatermarkStrategy, Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingProcessingTimeWindows

from functions.func import WSMapFunction, WSAggregateFunction, WSProcessWindowFunction


def window_process_demo():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///Users/oscar/software/jars/flink-sql-connector-kafka-1.16.1.jar")
    env.set_parallelism(1)
    # 如果是精准一次，必须开启checkpoint
    # env.enable_checkpointing(2000, mode=CheckpointingMode.EXACTLY_ONCE)
    # 指定 kafka 的地址和端口
    brokers = "localhost:9092"
    source = KafkaSource.builder() \
        .set_bootstrap_servers(brokers) \
        .set_topics("pyflink_kafka") \
        .set_group_id("my-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    sensor_ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source").map(WSMapFunction())
    sensor_ks = sensor_ds.key_by(lambda sensor: sensor.id)

    # 1.窗口分配器
    sensor_ws = sensor_ks.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    # 2. 全窗口函数：  全窗口函数计算逻辑：  窗口触发时才会调用一次，统一计算窗口的所有数据
    # sensor_ws.apply()

    # 新用法是：process 方式
    sensor_process = sensor_ws.process(WSProcessWindowFunction())

    sensor_process.print()

    env.execute("WindowProcess")


if __name__ == '__main__':
    window_process_demo()
