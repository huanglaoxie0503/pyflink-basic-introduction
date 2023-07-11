#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import SimpleStringSchema, WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction, RuntimeContext, AggregateFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.state import AggregatingStateDescriptor

from functions.func import WSMapFunction


class KeyedAggregatingStateReduceFunction(AggregateFunction):
    def create_accumulator(self):
        return 0, 0

    def add(self, value, accumulator):
        return accumulator[0] + value, accumulator[1] + 1

    def get_result(self, accumulator):
        return float(accumulator[0] / accumulator[1])

    def merge(self, acc_a, acc_b):
        return None


class KeyedAggregatingStateKeyedProcessFunction(KeyedProcessFunction):
    def __init__(self):
        self.vc_avg_aggregating_state = None

    def open(self, runtime_context: RuntimeContext):
        self.vc_avg_aggregating_state = runtime_context.get_aggregating_state(
            AggregatingStateDescriptor(
                name='vc_avg_aggregating_state',
                agg_function=KeyedAggregatingStateReduceFunction(),
                state_type_info=Types.FLOAT()
            )
        )

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        # 将 水位值 添加到  聚合状态中
        self.vc_avg_aggregating_state.add(value.vc)
        # 从 聚合状态中 获取结果
        vc_avg = self.vc_avg_aggregating_state.get()
        result = "传感器id为：{0},平均水位值={1}".format(value.id, vc_avg)
        yield result
        """
        # self.vc_avg_aggregating_state.get();    # 对 本组的聚合状态 获取结果           
        # self.vc_avg_aggregating_state.add();    # 对 本组的聚合状态 添加数据，会自动进行聚合   
        # self.vc_avg_aggregating_state.clear();  # 对 本组的聚合状态 清空数据           
        """


def keyed_aggregating_state_demo():
    """
    计算每种传感器的平均水位
    :return:
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

    process = sensor_ds.key_by(lambda sensor: sensor.id).process(KeyedAggregatingStateKeyedProcessFunction())

    process.print()

    env.execute("KeyedAggregatingState")


if __name__ == '__main__':
    keyed_aggregating_state_demo()
