#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

from model.water_sensor import WaterSensor


def table_stream_demo():
    env = StreamExecutionEnvironment.get_execution_environment()

    sensor_ds = env.from_collection(
        [
            WaterSensor(id='s1', ts=1, vc=1),
            WaterSensor(id='s2', ts=2, vc=2),
            WaterSensor(id='s3', ts=3, vc=3)
        ]
    )

    table_env = StreamTableEnvironment.create(env)

    # TODO 1. 流转表
    sensor_table = table_env.from_data_stream(sensor_ds)
    table_env.create_temporary_view('sensor', sensor_table)

    filter_table = table_env.sql_query('select id,ts,vc from sensor where ts > 10;')
    # sum_table = table_env.sql_query('select id,sum(vc) from sensor group by id;')

    # TODO 2. 表转流
    # 2.1 追加流
    table_env.to_data_stream(filter_table).print('filter')
    # 2.2 changelog流(结果需要更新)
    # table_env.to_changelog_stream(sum_table).print('sum')

    # 只要代码中调用了 DataStreamAPI，就需要 execute，否则不需要
    env.execute()


if __name__ == '__main__':
    table_stream_demo()