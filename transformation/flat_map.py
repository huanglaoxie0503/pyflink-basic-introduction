#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

from model.water_sensor import WaterSensor


def show(ds, env):
    ds.print()
    env.execute()


def flat_map_demo():
    """
    Filter 算子的基本用法
    :return:
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    sensor_data = [
        (1, WaterSensor("s1", 1, 1)),
        (1, WaterSensor("s1", 11, 11)),
        (2, WaterSensor("s2", 2, 2)),
        (3, WaterSensor("s3", 3, 3))
    ]

    # PyFlink 在版本 1.14.0 之后的 API 发生了一些变化，Types.of() 方法已经被移除。要指定数据流的类型，可以使用 Types.ROW 并提供相应的字段类型。
    sensor_stream = env.from_collection(
        collection=sensor_data,
        type_info=Types.ROW_NAMED(["id", "info"], [Types.INT(), Types.MAP(Types.STRING(), Types.STRING())]))

    # sensor_stream.print()

    def flat_map_operation(data):
        items = []
        json_data = data.info
        s_id = json_data.get('id')
        if s_id == 's1':
            vc = json_data.get('vc')
            items.append(vc)
        elif s_id == 's3':
            vc = json_data.get('vc')
            ts = json_data.get('ts')
            items.append(vc)
            items.append(ts)

        return items

    show(sensor_stream.flat_map(flat_map_operation), env)


if __name__ == '__main__':
    flat_map_demo()
