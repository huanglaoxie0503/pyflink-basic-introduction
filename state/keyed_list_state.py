#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import SimpleStringSchema, WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.state import ListStateDescriptor

from functions.func import WSMapFunction


class ListValueStateKeyedProcessFunction(KeyedProcessFunction):
    def __init__(self):
        self.vc_list_state = None

    def open(self, runtime_context: RuntimeContext):
        self.vc_list_state = runtime_context.get_list_state(ListStateDescriptor('vc_list_state', Types.INT()))

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        # 1.来一条，存到list状态里
        self.vc_list_state.add(value.vc)
        # 2.从list状态拿出来(Iterable)， 拷贝到一个List中，排序， 只留3个最大的
        vc_list_it = self.vc_list_state.get()
        vc_list = []
        for vc in vc_list_it:
            # 拷贝到一个List中
            vc_list.append(vc)
        # 2.2 对List进行降序排序
        vc_list.sort(reverse=True)
        # 2.3 只保留最大的3个(list中的个数一定是连续变大，一超过3就立即清理即可)
        if len(vc_list) > 3:
            vc_list.pop(3)

        p = '传感器id为{0},最大的3个水位值={1}'.format(value.id, vc_list)
        yield p

        # 3.更新list状态
        self.vc_list_state.update(vc_list)

        # self.vc_list_state.get();            # 取出 list状态 本组的数据，是一个Iterable
        # self.vc_list_state.add();            # 向 list状态 本组 添加一个元素
        # self.vc_list_state.addAll();         # 向 list状态 本组 添加多个元素
        # self.vc_list_state.update();         # 更新 list状态 本组数据（覆盖）
        # self.vc_list_state.clear();          # 清空List状态 本组数据


def list_state_demo():
    """
    针对每种传感器输出最高的3个水位值
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

    process = sensor_ds.key_by(lambda sensor: sensor.id).process(ListValueStateKeyedProcessFunction())
    process.print()

    env.execute("KeyedListState")


if __name__ == '__main__':
    list_state_demo()
