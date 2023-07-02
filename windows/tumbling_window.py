#!/usr/bin/python
# -*- coding:UTF-8 -*-

from pyflink.common import Types, Time
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, KeySelector, ReduceFunction
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.window import TumblingProcessingTimeWindows, SlidingProcessingTimeWindows, \
    ProcessingTimeSessionWindows, GlobalWindows

from functions.rich import MyReduceFunction, MyAggregate, MyWindowFunction, MyProcessWindowFunction

if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.AUTOMATIC)

    deserialization_schema = JsonRowDeserializationSchema.Builder() \
        .type_info(Types.ROW([Types.INT(), Types.STRING()])) \
        .build()

    kafka_consumer = FlinkKafkaConsumer(
        topics='test_json_topic',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group_1'}
    )
    kafka_consumer.set_start_from_earliest()

    dataSource = env.add_source(kafka_consumer)

    # 没有 keyby 的窗口，窗口内的所有数据进入同一个子任务，并行度只能为1
    # dataSource.window_all()

    # 有 key_by 的窗口，每个 key 上都定义一组窗口，各自独自进行统计计算
    dsKS = dataSource.key_by(lambda x: x[1])

    # TODO 基于时间的窗口
    # 滚动窗口，窗口长度为：10s
    dsKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    # 滑动窗口，窗口长度为：10s，步长：2s
    dsKS.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2)))
    # 会话窗口，超时间隔5s
    dsKS.window(ProcessingTimeSessionWindows.with_gap(Time.seconds(5)))

    # TODO 基于计算的窗口
    dsKS.count_window(5)  # 滚动窗口，每5条数据一个窗口
    dsKS.count_window(5, 2)  # 滑动窗口，窗口长度=5个元素，滑动步长=2个元素
    dsKS.window(GlobalWindows.create())  # 全局窗口，计数窗口的底层，自定义窗口时会用到

    # TODO 2、指定窗口函数：窗口内的数据计算逻辑
    dsWS = dsKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    # 增量聚合，来一条数据计算一条数据，窗口触发的时候输出计算结果
    dsWS.reduce(MyReduceFunction())

    # 属于本窗口的第一条数据来，创建窗口
    dsWS.aggregate(MyAggregate(), accumulator_type=None, output_type=None)

    # 全窗口函数，数据来来不计算，存取来，窗口触发的时候，计算并输出结果
    dsWS.process(MyProcessWindowFunction())
    dsWS.apply(MyWindowFunction())
    env.execute()
