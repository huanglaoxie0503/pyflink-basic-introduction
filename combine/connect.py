#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.datastream import StreamExecutionEnvironment

from functions.func import CustomerCoMapFunction


def connect_demo():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    stream_1 = env.from_collection([1, 2, 3])
    stream_2 = env.from_collection(["a", "b", "c"])

    # TODO 使用 connect 合流
    """
        1、一次只能连接 2条流
        2、流的数据类型可以不一样
        3、连接后可以调用 map、flatmap、process来处理，但是各处理各的
    """
    connect_stream = stream_1.connect(stream_2)

    connect_stream.map(CustomerCoMapFunction()).print()

    env.execute("Connect")


if __name__ == '__main__':
    connect_demo()
