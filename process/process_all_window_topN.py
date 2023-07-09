#!/usr/bin/python
# -*- coding:UTF-8 -*-
from operator import itemgetter
from typing import Iterable

from pyflink.common import SimpleStringSchema, WatermarkStrategy, Duration, Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import ProcessAllWindowFunction
from pyflink.datastream.window import SlidingEventTimeWindows

from functions.func import WSMapFunction
from model.water_sensor import WaterSensor
from process.keyed_process_timer import KeyedProcessTimerTimestampAssigner


class ProcessAllWindowFunctionTopN(ProcessAllWindowFunction):
    def process(self,
                context: 'ProcessAllWindowFunction.Context',
                elements: Iterable[WaterSensor]) -> Iterable[list]:
        # 定义一个map用来存，key=vc，value=count值
        vc_count_map = {}
        # 1.遍历数据, 统计 各个vc出现的次数
        for element in elements:
            vc = element.vc
            if vc in vc_count_map.keys():
                # 1.1 key存在，不是这个key的第一条数据，直接累加
                vc_count_map[vc] = vc_count_map.get(vc) + 1
            else:
                # 1.2 key不存在，初始化
                vc_count_map[vc] = 1

        # 2.对 count值进行排序: 利用List来实现排序
        rows = []
        for key, value in vc_count_map.items():
            param = (key, value)
            rows.append(param)
        # 对List进行排序，根据count值 降序
        items = sorted(rows, key=itemgetter(1), reverse=True)

        yield items[0:2]


def process_all_window_topN_demo():
    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(1)

    brokers = "localhost:9092"
    source = KafkaSource.builder() \
        .set_bootstrap_servers(brokers) \
        .set_topics("pyflink_kafka") \
        .set_group_id("my-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    sensor_ds = env.from_source(
        source,
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(3)).with_timestamp_assigner(
            KeyedProcessTimerTimestampAssigner()),
        "Kafka Source"
    ).map(WSMapFunction())

    sensor_process = sensor_ds.window_all(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))) \
        .process(ProcessAllWindowFunctionTopN())

    sensor_process.print()

    env.execute("ProcessAllWindowTopN")


if __name__ == '__main__':
    process_all_window_topN_demo()
