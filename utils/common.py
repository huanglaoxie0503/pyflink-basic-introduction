#!/usr/bin/python
# -*- coding:UTF-8 -*-
import os
import json


def get_jar_file(dir_path, filters):
    """
    获取jar包，按格式拼接并返回
    :param filters:
    :param dir_path:
    :return:
    """
    jars = []
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            sub_path = os.path.join(root, file)
            if file.endswith('.jar') and file in filters:
                jars.append(sub_path)
    str_jars = ';'.join(['file://' + jar for jar in jars])
    # print(str_jars)
    return str_jars


def dirt_sort(items):
    data = json.loads(items)
    print(data)
    # r = sorted(data.keys(), reverse=True)
    r = sorted(data.items(), key=lambda x: x[0], reverse=True)
    print(r)
    for i in r:
        print(i)


if __name__ == '__main__':
    info = """
     {\"16th Latest rejected event - Time\":\"Invalid value\",\"Total number of successful events\":\"88966303\",\"7th Latest successful event - Time\":\"Invalid value\",\"18th Latest rejected event - Event Code\":\"No history reported\",\"15th Latest rejected event - Time\":\"Invalid value\",\"19th Latest rejected event - Time\":\"0\",\"10th Latest successful event - Time\":\"Invalid value\",\"Event ID\":\"0d\",\"20th Latest rejected event - Time\":\"0\",\"17th Latest rejected event - Time\":\"0\",\"17th Latest rejected event - Event Code\":\"No history reported\",\"13th Latest rejected event - Time\":\"Invalid value\",\"2nd Latest successful event - Time\":\"Invalid value\",\"9th Latest successful event - Time\":\"Invalid value\",\"8th Latest successful event - Time\":\"Invalid value\",\"17th Latest rejected event - Additional Event Data\":\"Default value\",\"Total number of rejected events\":\"07202042\",\"Latest successful event - Time\":\"Invalid value\",\"3rd Latest successful event - Time\":\"Invalid value\",\"19th Latest rejected event - Event Code\":\"No history reported\",\"4th Latest successful event - Time\":\"Invalid value\",\"20th Latest rejected event - Additional Event Data\":\"Default value\",\"18th Latest rejected event - Additional Event Data\":\"Default value\",\"6th Latest successful event - Time\":\"Invalid value\",\"11th Latest rejected event - Time\":\"Invalid value\",\"12th Latest rejected event - Time\":\"Invalid value\",\"19th Latest rejected event - Additional Event Data\":\"Default value\",\"14th Latest rejected event - Time\":\"Invalid value\",\"5th Latest successful event - Time\":\"Invalid value\",\"18th Latest rejected event - Time\":\"0\",\"20th Latest rejected event - Event Code\":\"No history reported\"}
    """.strip()
    dirt_sort(items=info)
