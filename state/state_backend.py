#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, HashMapStateBackend, EmbeddedRocksDBStateBackend
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

from functions.func import WSMapFunction
from state.keyed_list_state import ListValueStateKeyedProcessFunction


def state_backend_demo():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    """
    * TODO 状态后端
          1、负责管理 本地状态
          2、 hashmap
                   存在 TM的 JVM的堆内存，  读写快，缺点是存不了太多（受限与TaskManager的内存）
              rocksdb
                   存在 TM所在节点的rocksdb数据库，存到磁盘中，  写--序列化，读--反序列化
                   读写相对慢一些，可以存很大的状态
         
          3、配置方式
             1）配置文件 默认值  flink-conf.yaml
             2）代码中指定
             3）提交参数指定
             flink run-application -t yarn-application
             -p 3
             -Dstate.backend.type=rocksdb
             -c 全类名
             jar包
    """

    # 使用 hashmap状态后端
    hash_map_state_backend = HashMapStateBackend()
    env.set_state_backend(hash_map_state_backend)

    # 使用 rocksdb状态后端
    # embedded_rocks_db_state_backend = EmbeddedRocksDBStateBackend()
    # env.set_state_backend(embedded_rocks_db_state_backend)

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

    env.execute("StateBackend")


if __name__ == '__main__':
    state_backend_demo()
