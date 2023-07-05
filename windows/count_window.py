#!/usr/bin/python
# -*- coding:UTF-8 -*-
from typing import Iterable

from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction, GlobalWindow
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

from functions.func import WSMapFunction
from utils.common import time_stamp_to_date


class WsCountWindowProcessWindowFunction(ProcessWindowFunction[tuple, tuple, str, GlobalWindow]):
    def process(self,
                key: str,
                context: 'ProcessWindowFunction.Context',
                elements: Iterable[tuple]) -> Iterable[tuple]:
        max_ts = context.window().max_timestamp()
        max_time = time_stamp_to_date(max_ts)
        count = len([e for e in elements])

        result = "key={0}的窗口最大时间={1},包含{2}条数据===>{3}".format(key, max_time, count, elements)
        yield result


def count_window_demo():
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
    # 滚动窗口，窗口长度5条数据
    sensor_ws = sensor_ks.count_window(size=5)
    # 滑动窗口，窗口长度5条数据，滑动步长2条数据（每经过一个步长，都有一个窗口触发输出，第一次输出在第2条数据来的时候）
    # sensor_ws = sensor_ks.count_window(size=5, slide=2)

    sensor_process = sensor_ws.process(WsCountWindowProcessWindowFunction())

    sensor_process.print()

    env.execute("CountWindow")


if __name__ == '__main__':
    count_window_demo()

    """
     触发器、移除器： 现成的几个窗口，都有默认的实现，一般不需要自定义

    以时间类型的滚动窗口为例，分析原理：
    1、窗口什么时候触发 输出？
            时间进展 >= 窗口的最大时间戳（end - 1ms）

    2、窗口是怎么划分的？
            start= 向下取整，取窗口长度的整数倍
            end = start + 窗口长度

            窗口左闭右开 ==> 属于本窗口的 最大时间戳 = end - 1ms

    3、窗口的生命周期？
            创建： 属于本窗口的第一条数据来的时候，现new的，放入一个singleton单例的集合中
            销毁（关窗）： 时间进展 >=  窗口的最大时间戳（end - 1ms） + 允许迟到的时间（默认0）


    """