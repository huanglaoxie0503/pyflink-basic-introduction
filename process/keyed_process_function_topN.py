#!/usr/bin/python
# -*- coding:UTF-8 -*-
from typing import Iterable

from pyflink.common import SimpleStringSchema, Duration, WatermarkStrategy, Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, AggregateFunction, ProcessWindowFunction, \
    KeyedProcessFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import SlidingEventTimeWindows, TimeWindow

from functions.func import WSMapFunction
from model.water_sensor import WaterSensor


class KeyedProcessFunctionTopN(TimestampAssigner):
    def extract_timestamp(self, value: WaterSensor, record_timestamp: int) -> int:
        yield value.ts * 1000


class VcCountAgg(AggregateFunction):
    def create_accumulator(self):
        return 0

    def add(self, value, accumulator):
        return accumulator + 1

    def get_result(self, accumulator):
        return accumulator

    def merge(self, acc_a, acc_b):
        return None


class WindowResult(ProcessWindowFunction[int, tuple, str, TimeWindow]):
    """
    第一个：输入类型 = 增量函数的输出  count值，Integer
    第二个：输出类型 = Tuple3(vc，count，windowEnd) ,带上 窗口结束时间 的标签
    第三个：key类型 ， vc，Integer
    第四个：窗口类型
    """

    def process(self,
                key: int,
                context: 'ProcessWindowFunction.Context',
                elements: Iterable[int]) -> Iterable[tuple]:
        # 迭代器里面只有一条数据，next一次即可
        count = len([e for e in elements])
        window_end = context.window().end
        yield key, count, window_end


class TopN(KeyedProcessFunction):
    def __init__(self, threshold):
        # 要取的Top数量
        self.threshold = threshold
        # 存不同窗口的 统计结果，key=windowEnd，value=list数据
        self.data_list_map = {}

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        # 进入这个方法，只是一条数据，要排序，得到齐才行 ===》 存起来，不同窗口分开存
        window_end = value[1]
        if window_end in self.data_list_map.keys():
            # 1.1 包含vc，不是该vc的第一条，直接添加到List中
            data_list = []
            key = self.data_list_map.get(window_end)
            data_list.append(value)
            # self.data_list_map[key] = data_list
        else:
            # 不包含vc，是该vc的第一条，需要初始化list
            data_list = [value]
            self.data_list_map[window_end] = data_list

        # 2. 注册一个定时器， windowEnd+1ms即可
        # 同一个窗口范围，应该同时输出，只不过是一条一条调用processElement方法，只需要延迟1ms即可
        ctx.timer_service().register_event_time_timer(window_end + 1)

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        # 定时器触发，同一个窗口范围的计算结果攒齐了，开始 排序、取TopN
        window_end = ctx.get_current_key()
        data_list = self.data_list_map.get(window_end)

        yield data_list


def keyed_process_function_topN_demo():
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
            KeyedProcessFunctionTopN()
        ),
        "Kafka Source"
    ).map(WSMapFunction())

    sensor_window_agg = sensor_ds.key_by(lambda sensor: sensor.id) \
        .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))) \
        .aggregate(
        VcCountAgg(),
        WindowResult()
    )

    sensor_window_agg_ks = sensor_window_agg.key_by(lambda r: r[2])
    # sensor_window_agg_ks.print()
    sensor_window_agg_ks.process(TopN(2)).print()

    # sensor_ds.print()

    env.execute("KeyedProcessFunctionTopN")


if __name__ == '__main__':
    keyed_process_function_topN_demo()
