#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import SimpleStringSchema, WatermarkStrategy, Time
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode, WindowAssigner, MapFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingProcessingTimeWindows, SlidingProcessingTimeWindows, \
    ProcessingTimeSessionWindows, GlobalWindows

from functions.func import WSMapFunction


def window_api_demo():
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

    # TODO 1. 指定 窗口分配器： 指定 用 哪一种窗口 ---  时间 or 计数？ 滚动、滑动、会话？
    # 1.1 没有keyby的窗口: 窗口内的 所有数据 进入同一个 子任务，并行度只能为1
    sensor_ks.window_all()
    #  1.2 有keyby的窗口: 每个key上都定义了一组窗口，各自独立地进行统计计算

    # TODO (1)、基于时间的
    # 滚动窗口，窗口长度10s
    sensor_ks.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    # 滑动窗口，窗口长度10s，滑动步长2s
    sensor_ks.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2)))
    # 会话窗口，超时间隔5s
    sensor_ks.window(ProcessingTimeSessionWindows.with_gap(Time.seconds(5)))

    # TODO （2）、基于计数的
    # 滚动窗口，窗口长度 = 5个元素
    sensor_ks.count_window(5)
    # 滑动窗口，窗口长度 = 5个元素，滑动步长 = 2个元素
    sensor_ks.count_window(5, 2)
    # 全局窗口，计数窗口的底层就是用的这个，需要自定义的时候才会用
    sensor_ks.window(GlobalWindows.create())

    # TODO 2. 指定 窗口函数 ： 窗口内数据的 计算逻辑
    sensor_ws = sensor_ks.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    # sensor_ws.reduce()

    # sensor_ws.aggregate()
    # 全窗口函数：数据来了不计算，存起来，窗口触发的时候，计算并输出结果
    # sensor_ws.process()

    env.execute("Window API")


if __name__ == '__main__':
    window_api_demo()
