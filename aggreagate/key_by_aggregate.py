#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

from model.water_sensor import WaterSensor


def key_by_aggr_demo():
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
    sensorDS = env.from_collection(
        data,
        type_info=Types.ROW_NAMED(["id", "vc", "ts"], [Types.STRING(), Types.INT(), Types.INT()])
    )

    # 按照id分组 KeyBy 不是转换算子，只是对数据进行重分区
    keyed_stream = sensorDS.key_by(lambda sensor: sensor.id)

    # 打印测试结果
    keyed_stream.print()

    # 使用sum聚合函数计算和并打印结果
    keyed_stream.sum("vc").print()

    # 使用min聚合函数计算最小值并打印结果
    keyed_stream.min("vc").print()

    # 使用max聚合函数计算最大值并打印结果
    keyed_stream.max("vc").print()
    # 使用max_by输出每个ID最大值并打印结果
    keyed_stream.max_by("vc").print()

    # 执行任务
    env.execute("KeyedStream Example")


if __name__ == '__main__':
    key_by_aggr_demo()
