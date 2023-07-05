#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import SimpleStringSchema, WatermarkStrategy, Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingProcessingTimeWindows

from functions.func import WSMapFunction, WSReduceFunction


def window_reduce_demo():
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
    # 2. 窗口函数： 增量聚合 Reduce
    """
    窗口的reduce：
     1、相同key的第一条数据来的时候，不会调用reduce方法
     2、增量聚合： 来一条数据，就会计算一次，但是不会输出
     3、在窗口触发的时候，才会输出窗口的最终计算结果
    """
    sensor_reduce = sensor_ws.reduce(WSReduceFunction())

    sensor_reduce.print()

    env.execute("WindowReduce")


if __name__ == '__main__':
    window_reduce_demo()