#!/usr/bin/python
# -*- coding:UTF-8 -*-
import os
import json
import time


def get_jar_file(dir_path, need_jars):
    """
    获取jar包，按格式拼接并返回
    :param dir_path: jar 包目录
    :param need_jars: 需要的jar 包
    :return:
    """
    jars = []
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            sub_path = os.path.join(root, file)
            if file.endswith('.jar') and file in need_jars:
                jars.append(sub_path)
    str_jars = ';'.join(['file://' + jar for jar in jars])
    return str_jars


def time_stamp_to_date(time_num):
    """
    输入毫秒级的时间，转出正常格式的时间
    :param time_num: 13位时间戳
    :return:
    """
    time_stamp = float(time_num/1000)
    time_array = time.localtime(time_stamp)
    other_style_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
    return other_style_time


def show(ds, env):
    ds.print()
    env.execute()


def dirt_sort(items):
    data = json.loads(items)
    print(data)
    # r = sorted(data.keys(), reverse=True)
    r = sorted(data.items(), key=lambda x: x[0], reverse=True)
    print(r)
    for i in r:
        print(i)


if __name__ == '__main__':
    print(time_stamp_to_date(1688540700000))
