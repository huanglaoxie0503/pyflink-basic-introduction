#!/usr/bin/python
# -*- coding:UTF-8 -*-
from abc import ABC

from pyflink.datastream.functions import MapFunction, RuntimeContext, ProcessFunction, ReduceFunction, \
    AggregateFunction, WindowFunction, ProcessWindowFunction, FilterFunction, FlatMapFunction

from model.water_sensor import WaterSensor


class WaterSensorRichFlatMapFunction(FlatMapFunction):
    def open(self, runtime_context: RuntimeContext):
        sub_task = runtime_context.get_index_of_this_subtask()
        print('-------{0}-------'.format(sub_task))

    def close(self):
        print('-----------close----------')

    def flat_map(self, value):
        if value.id == 's1':
            return value
        elif value.id == 's3':
            return value


class WaterSensorRichFilterFunction(FilterFunction):
    def __init__(self, s_id):
        self.s_id = s_id

    def open(self, runtime_context: RuntimeContext):
        sub_task = runtime_context.get_index_of_this_subtask()
        print('-------{0}-------'.format(sub_task))

    def close(self):
        print('-----------close----------')

    def filter(self, value):
        if self.s_id == value.id:
            return value


class WaterSensorRichMapFunction(MapFunction):
    def open(self, runtime_context):
        # 在这里执行初始化操作，比如建立数据库连接等
        sub_task = runtime_context.get_index_of_this_subtask()
        print('-------{0}-------'.format(sub_task))

    def close(self):
        print('-----------close----------')

    def map(self, value):
        # 在这里实现对输入数据的转换操作
        return WaterSensor(value.id, value.ts + 1, value.vc * 2)


class RichMapFunction(MapFunction):
    def __init__(self):
        pass

    def open(self, runtime_context: RuntimeContext):
        sub_task = runtime_context.get_index_of_this_subtask()
        print('-------{0}-------'.format(sub_task))

    def close(self):
        print('-----------close----------')

    def map(self, value):
        return value + 1


class RichProcessFunction(ProcessFunction):
    def __init__(self):
        pass

    def process_element(self, value, ctx):
        id_str = value['id']
        if id_str == "s1":

            pass
        elif id_str == "s2":
            pass
        else:
            pass


class MyReduceFunction(ReduceFunction):
    def reduce(self, value1, value2):
        return value1 + value2


class MyAggregate(AggregateFunction, ABC):
    """
    初始化累加器
    """

    def create_accumulator(self):
        print("创建累加器")
        return 0, 0

    """
    聚合逻辑
    """

    def add(self, value, accumulator):
        print('调用 add 方法')
        return accumulator[0] + value[1], accumulator[1] + 1

    """
    获取最终结果，窗口触发时输出
    """

    def get_result(self, accumulator):
        print('调用 get_result 方法')
        return accumulator[0] / accumulator[1]

    """
    只有会话窗口才会使用到
    """

    def merge(self, a, b):
        return a[0] + b[0], a[1] + b[1]


class MyWindowFunction(WindowFunction):
    def apply(self, key, window, inputs):
        pass


class MyProcessWindowFunction(ProcessWindowFunction):
    def process(self,
                key,
                context: 'ProcessWindowFunction.Context',
                elements):
        yield context.window().satrt
        yield context.window().end
