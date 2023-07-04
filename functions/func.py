#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import Types
from pyflink.datastream import MapFunction, CoMapFunction, ProcessFunction, OutputTag

from model.water_sensor import WaterSensor


# TODO 自定义函数

class WaterSensorMapFunction(MapFunction):
    def map(self, value):
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
