#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeySelector


class MyKeySelector(KeySelector):
    """
    实现KeySelector函数接口
    """
    def get_key(self, value):
        return value[1]


def get_key(value):
    """
    返回一个key值
    """
    return value[1]


def key_by_demo(environment):
    """
    key_by() 的三种实现方式
    """
    items = [(1, 'a'), (2, 'a'), (3, 'b')]
    data_stream = environment.from_collection(collection=items)
    # 方式一、lambda
    ds = data_stream.key_by(lambda x: x[1], key_type=Types.STRING())
    ds.print()
    # 方式二、python普通函数
    ds = data_stream.key_by(get_key, key_type=Types.STRING())
    ds.print()
    # 方式三、实现函数接口
    ds = data_stream.key_by(MyKeySelector(), key_type=Types.STRING())
    ds.print()


if __name__ == '__main__':
    # 创建执行环境
    env = StreamExecutionEnvironment.get_execution_environment()

    key_by_demo(environment=env)

    env.execute("KeyBy")
