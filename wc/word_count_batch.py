#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import Types, Configuration
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode


def split(lines):
    yield from lines.split(' ')


def word_count_batch():
    # TODO 1、创建执行环境
    # conf = Configuration()
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    env.set_parallelism(1)
    # TODO 2、读取数据：从文件中读取
    # dataSource = env.from_collection(word_count_data)
    data_source = env.read_text_file('/Users/oscar/data/word.txt')
    # TODO 3、切分、转换(word, 1)
    ds = data_source.flat_map(split).map(lambda x: (x, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda x: x[0]) \
        .reduce(lambda i, j: (i[0], i[1] + j[1]))

    # TODO 6、输出
    print("Printing result to stdout. Use --output to specify output path.")
    ds.print()

    env.execute("wordCountBatch")
    # env.execute_async()


if __name__ == '__main__':
    word_count_batch()
