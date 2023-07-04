#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.datastream import StreamExecutionEnvironment


def split_by_filter_demo():
    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(1)

    stream_ds = env.read_text_file("file:///Users/oscar/data/partition.txt")

    # TODO 使用filter来实现分流效果
    # 缺点： 同一个数据，要被处理两遍（调用两次filter）
    even_numbers = stream_ds.filter(lambda value: int(value) % 2 == 0)
    odd_numbers = stream_ds.filter(lambda value: int(value) % 2 == 1)

    even_numbers.print("偶数流")
    odd_numbers.print("奇数流")

    env.execute()


if __name__ == '__main__':
    split_by_filter_demo()
