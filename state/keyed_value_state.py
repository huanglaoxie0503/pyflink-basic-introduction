#!/usr/bin/python
# -*- coding:UTF-8 -*-
import math
from pyflink.common import SimpleStringSchema, WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.state import ValueStateDescriptor

from functions.func import WSMapFunction


class KeyedValueStateKeyedProcessFunction(KeyedProcessFunction):
    def __init__(self):
        self.last_value_state = None

    def open(self, runtime_context: RuntimeContext):
        # TODO 2.在open方法中，初始化状态
        # 状态描述器两个参数：第一个参数，起个名字，不重复；第二个参数，存储的类型
        self.last_value_state = runtime_context.get_state(ValueStateDescriptor('lastVcState', Types.INT()))

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        # 1、取出上一条数据的水位值(Integer默认值是null，判断)
        last_vc_value = self.last_value_state.value()
        if last_vc_value is None:
            last_vc = 0
        else:
            last_vc = self.last_value_state.value()

        # 2. 求差值的绝对值，判断是否超过10
        vc = value.vc
        if abs(vc - last_vc) > 10:
            p = '传感器={0},==>当前水位值={1}，,与上一条水位值={2},相差超过10！'.format(value.id, vc, last_vc)
            yield p

        # 3. 更新状态里的水位值
        self.last_value_state.update(vc)

        # self.last_value_state.value();  # 取出 本组 值状态 的数据
        # self.last_value_state.update(); # 更新 本组 值状态 的数据
        # self.last_value_state.clear();  # 清除 本组 值状态 的数据


def keyed_value_state_demo():
    """
    检测每种传感器的水位值，如果连续的两个水位值超过10，就输出报警。
    :return:
    """
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

    # side_output_tag = OutputTag("side-warn", Types.STRING())
    process = sensor_ds.key_by(lambda sensor: sensor.id).process(KeyedValueStateKeyedProcessFunction())
    process.print()

    env.execute("KeyedValueState")


if __name__ == '__main__':
    keyed_value_state_demo()
