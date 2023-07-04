#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.datastream import StreamExecutionEnvironment

from model.water_sensor import WaterSensor
from functions.rich import WaterSensorRichMapFunction


def rich_map_demo():
    """
    Rich 函数实现Map操作
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

    sensorDS.map(WaterSensorRichMapFunction()).print()

    env.execute()


if __name__ == '__main__':
    map_2_demo()