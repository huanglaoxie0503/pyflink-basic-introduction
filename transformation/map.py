#!/usr/bin/python
# -*- coding:UTF-8 -*-
import json

from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

from model.water_sensor import WaterSensor


def map_demo():
    """
    Map 算子的基本用法
    :return:
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    sensor_data = [
        (1, WaterSensor("s1", 1, 1)),
        (2, WaterSensor("s2", 2, 2)),
        (3, WaterSensor("s3", 3, 3))
    ]
    # PyFlink 在版本 1.14.0 之后的 API 发生了一些变化，Types.of() 方法已经被移除。要指定数据流的类型，可以使用 Types.ROW 并提供相应的字段类型。
    sensor_stream = env.from_collection(
        collection=sensor_data,
        type_info=Types.ROW_NAMED(["id", "info"], [Types.INT(), Types.MAP(Types.STRING(), Types.STRING())]))

    # sensor_stream.print()

    def update_value(data):
        """
        获取 WaterSensor id 、更新相应的value值
        :param data:
        :return:
        """
        # parse the json
        info = json.dumps(data.info)
        json_data = json.loads(info)
        items = {
            'id': json_data['id'],
            'timestamp': json_data['timestamp'],
            'value': int(json_data['value']) * 10
        }
        return items, json_data['id']

    sensor_stream.map(update_value).print()

    env.execute("WaterSensor Map Example")


if __name__ == '__main__':
    map_demo()
