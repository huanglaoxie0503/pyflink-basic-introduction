#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import SimpleStringSchema, WatermarkStrategy, Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

from functions.func import WSMapFunction
from model.water_sensor import WaterSensor


class KeyedProcessTimerTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: WaterSensor, record_timestamp: int) -> int:
        yield value.ts * 1000


class WaterSensorKeyedProcessFunction(KeyedProcessFunction):
    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        current_key = ctx.get_current_key()
        # TODO 1.定时器注册
        timer_service = ctx.timer_service()

        # 1、事件时间的案例
        # 数据中提取出来的事件时间
        # current_event_time = ctx.timestamp()
        # timer_service.register_event_time_timer(5000)
        # print("当前key={0},当前时间={1},注册了一个5s的定时器".format(current_key, current_event_time))

        # 2、处理时间的案例
        # current_ts = timer_service.current_processing_time()
        # timer_service.register_processing_time_timer(current_ts + 5000)
        # print("当前key={0},当前时间={1},注册了一个5s后的定时器".format(current_key, current_ts))

        # 3、获取 process的 当前watermark
        current_watermark = timer_service.current_watermark()
        print("当前数据={0},当前watermark={1}".format(value, current_watermark))

        # 注册定时器： 处理时间、事件时间
        # timer_service.register_processing_time_timer(5000)
        # timer_service.register_event_time_timer(5000)
        # 删除定时器： 处理时间、事件时间
        # timer_service.delete_processing_time_timer(5000)
        # timer_service.delete_event_time_timer(5000)

        # 获取当前时间进展： 处理时间-当前系统时间，  事件时间-当前watermark
        # timer_service.current_processing_time()
        # timer_service.current_watermark()

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        """
        TODO 2.时间进展到定时器注册的时间，调用该方法
        :param timestamp: 当前时间进展，就是定时器被触发时的时间
        :param ctx: 上下文
        :return:
        """
        current_key = ctx.get_current_key()
        print("key={0},现在时间是{1},定时器触发".format(current_key, timestamp))
        yield current_key, timestamp


def keyed_process_timer_demo():
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

    sensor_ks = sensor_ds.key_by(lambda sensor: sensor.id)

    sensor_process = sensor_ks.process(WaterSensorKeyedProcessFunction())

    sensor_process.print()

    env.execute("KeyedProcessTimer")


if __name__ == '__main__':
    keyed_process_timer_demo()
