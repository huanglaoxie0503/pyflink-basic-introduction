#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import SimpleStringSchema, WatermarkStrategy, Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingProcessingTimeWindows

from functions.func import WSMapFunction, WSAggregateFunction, WSProcessWindowFunction


def window_aggregate_and_process_demo():
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

    # 2. 窗口函数：
    """
    * 增量聚合 Aggregate + 全窗口 process
         1、增量聚合函数处理数据： 来一条计算一条
         2、窗口触发时， 增量聚合的结果（只有一条） 传递给全窗口函数
         3、经过全窗口函数的处理包装后，输出
         
         结合两者的优点：
         1、增量聚合： 来一条计算一条，存储中间的计算结果，占用的空间少
         2、全窗口函数： 可以通过 上下文 实现灵活的功能
    """
    result = sensor_ws.aggregate(WSAggregateFunction(), WSProcessWindowFunction())

    result.print()

    env.execute("WindowAggregateAndProcess")


if __name__ == '__main__':
    window_aggregate_and_process_demo()
