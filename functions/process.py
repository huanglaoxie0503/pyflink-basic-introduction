#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.datastream.functions import CoProcessFunction


class MyCoProcessFunction(CoProcessFunction):
    def __init__(self):
        self.cache_1 = {}
        self.cache_2 = {}

    """
    第一条流的处理逻辑
    value：第一条流数据
    ctx：上下文
    """

    def process_element1(self, value, ctx: 'CoProcessFunction.Context'):
        # TODO 1、s1 的数据来来存储在变量中
        id_str = value.get('id')
        rows = []
        if id_str in self.cache_1.keys():
            rows.append(value)
            self.cache_1[id_str] = rows
        else:
            self.cache_1.get(id_str)
        # TODO 2、
        pass

    """
    第二条流的处理逻辑
    value：第二条流数据
    ctx：上下文
    """

    def process_element2(self, value, ctx: 'CoProcessFunction.Context'):
        pass
