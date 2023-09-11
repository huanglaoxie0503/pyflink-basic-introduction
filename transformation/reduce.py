#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ReduceFunction, KeySelector


class MyReduceFunction(ReduceFunction):
    """
    实现reduce函数接口
    """
    def reduce(self, value1, value2):
        return value1[0] + value2[0], value2[1]


def get_value(v1, v2):
    """
    Python 普通函数实现Reduce功能
    """
    return v1[0] + v2[0], v2[1]


def reduce_demo(environment):
    items = [(1, 'a'), (2, 'a'), (3, 'a'), (4, 'b')]
    type_info = Types.TUPLE([Types.INT(), Types.STRING()])
    data_stream = environment.from_collection(collection=items, type_info=type_info)
    # 方式一、 lambda
    ds = data_stream.key_by(lambda x: x[1]).reduce(lambda a, b: (a[0] + b[0], b[1]))
    ds.print()
    # 方式二、 python 普通函数
    ds = data_stream.key_by(lambda x: x[1]).reduce(get_value)
    ds.print()
    # 方式三、实现函数接口
    ds = data_stream.key_by(lambda x: x[1]).reduce(MyReduceFunction())
    ds.print()


if __name__ == '__main__':
    #  创建执行环境
    env = StreamExecutionEnvironment.get_execution_environment()

    reduce_demo(environment=env)

    env.execute("Reduce")
