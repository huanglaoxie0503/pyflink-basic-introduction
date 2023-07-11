#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import SimpleStringSchema, WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction, RuntimeContext, ReduceFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.state import ReducingStateDescriptor

from functions.func import WSMapFunction


class KeyedReducingStateReduceFunction(ReduceFunction):
    def reduce(self, value1, value2):
        return value1 + value2


class KeyedReducingStateKeyedProcessFunction(KeyedProcessFunction):
    def __init__(self):
        self.vc_sum_reducing_state = None

    def open(self, runtime_context: RuntimeContext):
        self.vc_sum_reducing_state = runtime_context.get_reducing_state(
            ReducingStateDescriptor(name='vc_sum_reducing_state', reduce_function=KeyedReducingStateReduceFunction(),
                                    type_info=Types.INT())
        )

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        # 来一条数据，添加到 reducing状态里
        self.vc_sum_reducing_state.add(value.vc)
        vc_sum = self.vc_sum_reducing_state.get()
        result = "传感器id为:{0},水位值总和={1}".format(value.id, vc_sum)
        yield result

        # self.vc_sum_reducing_state.get();   # 对本组的Reducing状态，获取结果
        # self.vc_sum_reducing_state.add();   # 对本组的Reducing状态，添加数据
        # self.vc_sum_reducing_state.clear(); # 对本组的Reducing状态，清空数据


def keyed_reducing_state_demo():
    """"
    计算每种传感器的水位和
    """
    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(3)

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

    process = sensor_ds.key_by(lambda sensor: sensor.id).process(KeyedReducingStateKeyedProcessFunction())

    process.print()

    env.execute("KeyedReducingState")


if __name__ == '__main__':
    keyed_reducing_state_demo()
