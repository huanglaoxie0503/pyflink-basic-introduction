#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import SimpleStringSchema, WatermarkStrategy, Time, Types
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

from functions.func import WSMapFunction


class StateTTLKeyedProcessFunction(KeyedProcessFunction):
    def __init__(self):
        self.last_vc_value = None

    def open(self, runtime_context: RuntimeContext):
        """
        StateTtlConfig:
             new_builder(Time.seconds(5)): 过期时间5s
             set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)    状态 创建和写入（更新） 更新 过期时间
             set_update_type(StateTtlConfig.UpdateType.OnReadAndWrite)  状态 读取、创建和写入（更新） 更新 过期时间
             set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)    不返回过期的状态值
        :param runtime_context:
        :return:
        """
        # TODO 1.创建 StateTtlConfig
        state_ttl_config = StateTtlConfig\
            .new_builder(Time.seconds(10))\
            .set_update_type(StateTtlConfig.UpdateType.OnReadAndWrite)\
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build()
        # TODO 2.状态描述器 启用 TTL
        state_descriptor = ValueStateDescriptor('last_vc_state', Types.INT())
        state_descriptor.enable_time_to_live(state_ttl_config)

        self.last_vc_value = runtime_context.get_state(state_descriptor=state_descriptor)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        # 先获取状态值，打印 ==> 读取状态
        last_vc = self.last_vc_value.value()
        result = "key={0},状态值={1}".format(value.id, last_vc)
        yield result
        # 如果水位大于10，更新状态值 ===> 写入状态
        if value.vc > 10:
            self.last_vc_value.update(value.vc)


def state_TTL_demo():
    """
    状态生存时间（TTL）
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

    process = sensor_ds.key_by(lambda sensor: sensor.id).process(StateTTLKeyedProcessFunction())

    process.print()

    env.execute("StateTTL")


if __name__ == '__main__':
    state_TTL_demo()
