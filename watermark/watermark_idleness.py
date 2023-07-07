#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import SimpleStringSchema, WatermarkStrategy, Duration, Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingEventTimeWindows

from functions.func import WSMapFunction, WSProcessWindowFunction
from partition.simple_partition import CustomerPartition
from watermark.watermark_mono import MsTimestampAssigner


def watermark_idleness_demo():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///Users/oscar/software/jars/flink-sql-connector-kafka-1.16.1.jar")

    # TODO 演示watermark多并行度下的传递
    """
    1、接收到上游多个，取最小
    2、往下游多个发送， 广播
    """
    env.set_parallelism(2)

    # 周期性生成 watermark， 默认是200ms，一般不建议修改
    # env.get_config().set_auto_watermark_interval()

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

    sensor_ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source") \
        .partition_custom(CustomerPartition(), lambda r: r).map(lambda r: int(r))

    # 1.1 指定watermark生成：乱序的，等待3s
    watermark_strategy = WatermarkStrategy \
        .for_monotonous_timestamps() \
        .with_timestamp_assigner(MsTimestampAssigner()).with_idleness(Duration.of_seconds(5))  # 1.2 指定 时间戳分配器，从数据中提取

    sensor_ds_with_watermark = sensor_ds.assign_timestamps_and_watermarks(watermark_strategy=watermark_strategy)

    # 分成两组： 奇数一组，偶数一组 ， 开10s的事件时间滚动窗口
    sensor_ks = sensor_ds_with_watermark.key_by(lambda r: r % 2)

    # 1.窗口分配器
    sensor_ws = sensor_ks.window(TumblingEventTimeWindows.of(Time.seconds(10)))
    # 2. 全窗口函数：  全窗口函数计算逻辑：  窗口触发时才会调用一次，统一计算窗口的所有数据
    sensor_process = sensor_ws.process(WSProcessWindowFunction())

    sensor_process.print()

    env.execute("WatermarkIdleness")


if __name__ == '__main__':
    watermark_idleness_demo()
