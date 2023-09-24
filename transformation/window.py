#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import WatermarkStrategy, SimpleStringSchema, Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import ReduceFunction, MapFunction

from model.stock import StockInfo


class StockMapFunction(MapFunction):
    def map(self, value):
        # "s1", 1, 1
        words = value.split(',')
        return StockInfo(words[0], words[1], int(words[2]))


class StockReduceFunction(ReduceFunction):
    def reduce(self, value1, value2):
        print("调用reduce方法，value1={0},value2={1}".format(value1, value2))
        return StockInfo(value1.stock_code, value1.vol + value2.vol, value2.stock_name)


def window_demo(environment):
    """
    window 算子
    """
    brokers = "node01:9092"
    source = KafkaSource.builder() \
        .set_bootstrap_servers(brokers) \
        .set_topics("flink_tutorial") \
        .set_group_id("flink-tutorial") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    data_stream = environment.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source").map(
        StockMapFunction())
    stock_ks = data_stream.key_by(lambda code: code.stock_code)
    # stock_ks.print()
    # # 1.窗口分配器
    sensor_ws = stock_ks.window(TumblingEventTimeWindows.of(Time.seconds(5)))

    sensor_reduce = sensor_ws.reduce(StockReduceFunction())

    sensor_reduce.print()


if __name__ == '__main__':
    # 床架执行环境
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    window_demo(environment=env)

    env.execute("window")
    # ./kafka-console-producer.sh --broker-list node01:9092 --topic flink_tutorial (1,"A")
    # '000001',中欧,200
