#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

from model.water_sensor import WaterSensor


def reduce_key_by_aggr_demo():
    """
    key_by 聚合
    :return:
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # 定义数据源
    data = [
        WaterSensor("s1", 1, 1),
        WaterSensor("s1", 11, 11),
        WaterSensor("s2", 22, 2),
        WaterSensor("s3", 32, 3),
        WaterSensor("s1", 10, 10),
        WaterSensor("s2", 2, 2),
        WaterSensor("s3", 3, 3),
    ]

    # 将数据转换为DataStream
    sensorDS = env.from_collection(data)

    # 按照id分组 KeyBy 不是转换算子，只是对数据进行重分区
    keyed_stream = sensorDS.key_by(lambda sensor: sensor.id)

    # TODO reduce
    """
    1. key_by 之后调用
    2. 输入类型 = 输出类型，类型不能改变
    3. 每一个 key 的第一条数据来的时候，不会执行 reduce  方法，存起来，直接输出
    4. reduce 方法中的两个参数
        value1：之前计算的结果，存状态
        value2:现在来的数据
    """

    reduce = keyed_stream.reduce(lambda t1, t2: WaterSensor(t1.id, t1.vc + t2.vc, t2.ts))

    reduce.print()

    # 执行任务
    env.execute("ReduceAggregate Example")


if __name__ == '__main__':
    reduce_key_by_aggr_demo()
