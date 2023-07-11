#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import SimpleStringSchema, WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.state import MapStateDescriptor

from functions.func import WSMapFunction


class KeyedMapStateKeyedProcessFunction(KeyedProcessFunction):
    def __init__(self):
        self.vc_count_map_state = {}

    def open(self, runtime_context: RuntimeContext):
        self.vc_count_map_state = runtime_context.get_map_state(
            MapStateDescriptor(name='vc_count_map_state', key_type_info=Types.INT(), value_type_info=Types.INT()))

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        # 1.判断是否存在vc对应的key
        vc = value.vc
        if vc in self.vc_count_map_state.keys():
            # 1.1 如果包含这个vc的key，直接对value+1
            count = self.vc_count_map_state.get(vc)
            count = count + 1
            self.vc_count_map_state[vc] = count
        else:
            # 1.2 如果不包含这个vc的key，初始化进Map去
            self.vc_count_map_state[vc] = 1
        # 2.遍历Map状态，输出每个k-v的值
        items = []
        for k, v in self.vc_count_map_state.items():
            row = (k, v)
            items.append(row)
        p = '传感器id为{0},\n 各水位值出现次数：{1}\n'.format(value.id, items)
        yield p

        # self.vc_count_map_state.get();          # 对本组的Map状态，根据key，获取value
        # self.vc_count_map_state.contains();     # 对本组的Map状态，判断key是否存在
        # self.vc_count_map_state.put(, );        # 对本组的Map状态，添加一个 键值对
        # self.vc_count_map_state.putAll();       # 对本组的Map状态，添加多个 键值对
        # self.vc_count_map_state.entries();      # 对本组的Map状态，获取所有键值对
        # self.vc_count_map_state.keys();         # 对本组的Map状态，获取所有键
        # self.vc_count_map_state.values();       # 对本组的Map状态，获取所有值
        # self.vc_count_map_state.remove();       # 对本组的Map状态，根据指定key，移除键值对
        # self.vc_count_map_state.isEmpty();      # 对本组的Map状态，判断是否为空
        # self.vc_count_map_state.iterator();     # 对本组的Map状态，获取迭代器
        # self.vc_count_map_state.clear();        # 对本组的Map状态，清空


def keyed_map_state_demo():
    """
    统计每种传感器每种水位值出现的次数
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

    process = sensor_ds.key_by(lambda sensor: sensor.id).process(KeyedMapStateKeyedProcessFunction())
    process.print()

    env.execute("KeyedMapState")


if __name__ == '__main__':
    keyed_map_state_demo()
