#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode

from functions.process import MyCoProcessFunction

if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.AUTOMATIC)
    env.set_parallelism(1)

    ds1 = env.from_collection(
        [
            (1, 'a', 11),
            (2, 'b', 22),
            (3, 'c', 33)
        ]
    )

    ds2 = env.from_collection(
        [
            (4, 'a', 44),
            (5, 'b', 55),
            (6, 'c', 66)
        ]
    )

    ds1.connect(ds2).process(MyCoProcessFunction()).print()

    env.execute()





