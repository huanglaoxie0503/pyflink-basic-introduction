#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import Partitioner
from typing import Any


class CustomerPartition(Partitioner):
    """
    自定义分区器
    """
    def partition(self, key: Any, num_partitions: int) -> int:
        return int(key) % num_partitions


def partition_demo():
    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(2)

    ds = env.read_text_file("file:///Users/oscar/data/partition.txt")

    # shuffle随机分区: random.nextInt(下游算子并行度)
    # ds.shuffle().print()

    # rebalance轮询：nextChannelToSendTo = (nextChannelToSendTo + 1) % 下游算子并行度
    # 如果是数据源倾斜的场景， source后，调用rebalance，就可以解决数据源的数据倾斜

    # ds.rebalance().print()

    # rescale缩放： 实现轮询， 局部组队，比rebalance更高效
    # ds.rescale().print()

    # broadcast 广播：  发送给下游所有的子任务
    # ds.broadcast().print()

    # keyby: 按指定key去发送，相同key发往同一个子任务
    # one-to-one: Forward分区器

    # 总结： PyFlink提供了 6种分区器+ 1种自定义
    ds.partition_custom(CustomerPartition(), lambda r: r).print()

    env.execute("Partition")


if __name__ == '__main__':
    partition_demo()
