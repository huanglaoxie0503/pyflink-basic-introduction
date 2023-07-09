#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import SimpleStringSchema, WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment, OutputTag, KeyedProcessFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

from functions.func import WSMapFunction


class SideOutputKeyedProcessFunction(KeyedProcessFunction):
    def __init__(self, output_tag):
        self.output_tag = output_tag

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        # 使用侧输出流告警
        if value.vc > 10:
            # r = "{0}，当前水位={1},大于阈值10。value={2}".format(self.output_tag_warn, value.vc, value)
            # yield r
            yield self.output_tag, "side_out-" + str(value)
        else:
            # 主流正常 发送数据
            yield value


def side_output_demo():
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
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    ).map(WSMapFunction())

    side_output_tag = OutputTag("side-warn", Types.STRING())
    process = sensor_ds.key_by(lambda sensor: sensor.id).process(SideOutputKeyedProcessFunction(side_output_tag))

    process.print("主流")

    process.get_side_output(side_output_tag).print("warn")

    env.execute("SideOutput")


if __name__ == '__main__':
    side_output_demo()