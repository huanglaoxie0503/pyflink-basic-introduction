#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import CoProcessFunction


class CustomerCoProcessFunction(CoProcessFunction):
    def __init__(self):
        self.s1_cache = {}
        self.s2_cache = {}

    def process_element1(self, value, ctx: 'CoProcessFunction.Context'):
        """
        第一条流的处理逻辑
        :param value: 第一条流的数据
        :param ctx: 上下文
        :return:
        """
        # 1、TODO stream_1 存储到变量中
        s_id = value[0]
        # 1.1 如果key不存在，说明是该key的第一条数据，初始化
        if s_id not in self.s1_cache.keys():
            items = [value]
            self.s1_cache[s_id] = items
        else:
            # 1.2 key存在，不是该key的第一条数据，直接添加到 value的list中
            items = [value]
            self.s1_cache[s_id] = items

        # 2、TODO 去 s2Cache中查找是否有id能匹配上的,匹配上就输出，没有就不输出
        if s_id in self.s2_cache.keys():
            values = self.s2_cache.get(s_id)
            for v in values:
                p = "s1:" + str(value) + "<========>" + "s2:" + str(v)
                yield p

    def process_element2(self, value, ctx: 'CoProcessFunction.Context'):
        """
        第二条流的处理逻辑
        :param value: 第二条流的数据
        :param ctx: 上下文
        :return:
        """
        # 1、TODO stream_2 存储到变量中
        s_id = value[0]
        # 1.1 如果key不存在，说明是该key的第一条数据，初始化
        if s_id not in self.s2_cache.keys():
            items = [value]
            self.s2_cache[s_id] = items
        else:
            # 1.2 key存在，不是该key的第一条数据，直接添加到 value的list中
            items = [value]
            self.s2_cache[s_id] = items

        # 2、TODO 去 s1Cache中查找是否有id能匹配上的,匹配上就输出，没有就不输出
        if s_id in self.s1_cache.keys():
            values = self.s1_cache.get(s_id)
            for v in values:
                p = "s1:" + str(v) + "<========>" + "s2:" + str(value)
                yield p


def connect_key_by_demo():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    stream_1 = env.from_collection(
        [
            (1, "a1"),
            (1, "a2"),
            (2, "b"),
            (3, "c")
        ]
    )

    stream_2 = env.from_collection(
        [
            (1, "aa1", 1),
            (1, "aa2", 2),
            (2, "bb", 1),
            (3, "cc", 1)
        ]
    )

    connect_stream = stream_1.connect(stream_2)

    connect_key_by_stream = connect_stream.key_by(lambda s1: s1[0], lambda s2: s2[0])  # s1 -> s1.f0, s2 -> s2.f0);

    process = connect_key_by_stream.process(CustomerCoProcessFunction())

    process.print()

    env.execute('Connect_Key_by_Demo')


if __name__ == '__main__':
    connect_key_by_demo()
