#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, OutputTag

from functions.func import WaterSensorMapFunction, WaterSensorProcessFunction
from model.water_sensor import WaterSensor


def side_output_stream_demo():
    """
    使用侧输出流实现分流
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
    sensor_ds = env.from_collection(data).map(WaterSensorMapFunction())

    output_tag_s1 = OutputTag("side-output-s1", Types.STRING())
    output_tag_s2 = OutputTag("side-output-s2", Types.STRING())

    process_stream = sensor_ds.process(WaterSensorProcessFunction())

    # 从主流中，根据标签 获取 侧输出流
    side_output_stream_s1 = process_stream.get_side_output(output_tag_s1)
    side_output_stream_s2 = process_stream.get_side_output(output_tag_s2)

    # 主流
    process_stream.print("主流-非s1、s2")

    side_output_stream_s1.print("s1")
    side_output_stream_s2.print("s2")

    env.execute()


if __name__ == '__main__':
    # TODO 侧输出流文档：https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/datastream/side_output/
    side_output_stream_demo()