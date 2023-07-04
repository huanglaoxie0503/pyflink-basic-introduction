#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.datastream import StreamExecutionEnvironment


def union_demo():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source1 = env.from_collection([1, 2, 3])
    source2 = env.from_collection([11, 22, 33])
    source3 = env.from_collection(["111", "222", "333"])

    # TODO union：合并数据流
    """
    1、 流的数据类型必须一致
    2、 一次可以合并多条流
    """
    # source1.union(source2).union(source3.map(lambda r: int(r))).print()

    source1.union(source2, source3.map(lambda r: int(r))).print()

    env.execute("Union")


if __name__ == '__main__':
    union_demo()
