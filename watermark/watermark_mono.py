#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import SimpleStringSchema, WatermarkStrategy, Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingEventTimeWindows

from functions.func import WSMapFunction, WSProcessWindowFunction
from model.water_sensor import WaterSensor


class MsTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: WaterSensor, record_timestamp: int) -> int:
        print("数据={0},record_ts={1}".format(value, record_timestamp))
        return value.ts * 1000


def watermark_mono_demo():
    """
    升序流设置水位线(watermark)
    :return:
    """
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

    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(MsTimestampAssigner())
    sensor_ds_with_watermark = sensor_ds.assign_timestamps_and_watermarks(watermark_strategy=watermark_strategy)

    sensor_ks = sensor_ds_with_watermark.key_by(lambda sensor: sensor.id)

    # 1.窗口分配器
    sensor_ws = sensor_ks.window(TumblingEventTimeWindows.of(Time.seconds(10)))
    # 2. 全窗口函数：  全窗口函数计算逻辑：  窗口触发时才会调用一次，统一计算窗口的所有数据
    sensor_process = sensor_ws.process(WSProcessWindowFunction())

    sensor_process.print()

    env.execute("WatermarkMono")


if __name__ == '__main__':
    watermark_mono_demo()
