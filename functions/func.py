#!/usr/bin/python
# -*- coding:UTF-8 -*-
import time
from typing import Iterable, Any

from pyflink.common import Types
from pyflink.datastream import (
    MapFunction,
    CoMapFunction,
    ProcessFunction,
    ReduceFunction,
    AggregateFunction,
    WindowFunction,
    ProcessWindowFunction,
    OutputTag, TimeWindow)
from pyflink.datastream.functions import KEY, W, IN, OUT
from pyflink.datastream.window import SessionWindowTimeGapExtractor

from model.water_sensor import WaterSensor
from utils.common import time_stamp_to_date


# TODO 自定义函数

class WaterSensorMapFunction(MapFunction):
    def map(self, value):
        print(value)
        return WaterSensor(value.id, int(value.ts), int(value.vc))


class WaterSensorProcessFunction(ProcessFunction):
    def __init__(self):
        self.output_tag_s1 = OutputTag("side-output-s1", Types.STRING())
        self.output_tag_s2 = OutputTag("side-output-s2", Types.STRING())
        self.output_tag = OutputTag("side-output", Types.STRING())

    def process_element(self, value, ctx: 'ProcessFunction.Context'):
        s_id = value.id
        if s_id == "s1":
            # 如果是s1,放到侧输出流s1中
            yield self.output_tag_s1, " side-output-1 " + str(value)
        elif s_id == "s2":
            # 如果是s1,放到侧输出流s2中
            yield self.output_tag_s2, " side-output-2 " + str(value)
        else:
            # 非 s1、s2的数据，放到主流中
            yield value


class CustomerCoMapFunction(CoMapFunction):
    def map1(self, value):
        return value

    def map2(self, value):
        return value


class WSMapFunction(MapFunction):
    def map(self, value):
        # "s1", 1, 1
        words = value.split(',')
        return WaterSensor(words[0], int(words[1]), int(words[2]))


class WSReduceFunction(ReduceFunction):
    def reduce(self, value1, value2):
        print("调用reduce方法，value1={0},value2={1}".format(value1, value2))
        return WaterSensor(value1.id, value1.vc + value2.vc, value2.ts)


class WSAggregateFunction(AggregateFunction):
    def create_accumulator(self):
        """
        创建累加器，初始化累加器
        :return:
        """
        print('创建累加器')
        return 0

    def add(self, value, accumulator):
        """
        聚合逻辑
        :param value:
        :param accumulator:
        :return:
        """
        print("调用add方法,value={0}".format(value))
        return accumulator + value.vc

    def merge(self, acc_a, acc_b):
        # 只有会话窗口才会用到
        print("调用merge方法")
        return None

    def get_result(self, accumulator):
        """
        获取最终结果，窗口触发时输出
        :param accumulator:
        :return:
        """
        print("调用getResult方法")
        return accumulator


class WSWindowFunction(WindowFunction):
    """
    老写法，不用
    """

    def apply(self, key: KEY, window: W, inputs: Iterable[IN]) -> Iterable[OUT]:
        pass


class WSProcessWindowFunction(ProcessWindowFunction[tuple, tuple, str, TimeWindow]):
    def process(self,
                key: str,
                context: 'ProcessWindowFunction.Context',
                elements: Iterable[tuple]) -> Iterable[tuple]:
        """

        :param key: 分组的key
        :param context: 上下文
        :param elements: 存的数据
        :return:
        """
        start_ts = context.window().start
        end_ts = context.window().end
        # yyyy-MM-dd HH:mm:ss.SSS
        window_start = time_stamp_to_date(start_ts)
        window_end = time_stamp_to_date(end_ts)

        count = len([e for e in elements])
        result = "key={0}的窗口 [{1} --> {2}) 包含 {3} 条数据===>{4}".format(key, window_start, window_end, count,
                                                                             elements)
        yield result


class WSSessionWindowTimeGapExtractor(SessionWindowTimeGapExtractor):
    def extract(self, element: WaterSensor) -> int:
        # 从数据中提取ts，作为间隔, 单位ms
        return element.ts * 1000
