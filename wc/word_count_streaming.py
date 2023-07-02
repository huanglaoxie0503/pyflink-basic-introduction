#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, TableDescriptor, Schema, DataTypes

from settings import words

max_word_id = len(words) - 1


def word_count():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    t_env.create_temporary_table(
        'source', TableDescriptor.for_connector('datagen').schema(Schema.new_builder().column('word_id', DataTypes.INT()).build()

                                                                  )
        .option('fields.word_id.kind', 'random')
        .option('fields.word_id.min', '0')
        .option('fields.word_id.max', str(max_word_id))
        .option('rows-per-second', '5')
        .build()
    )

    table = t_env.from_path('source')
    ds = t_env.to_data_stream(table=table)

    def id_to_word(r):
        return words[r[0]]

    ds = ds.map(id_to_word)\
        .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()]))\
        .key_by(lambda i: i[0])\
        .reduce(lambda i, j: (i[0], i[1] + j[1]))

    ds.print()

    env.execute("word_count_streaming")


if __name__ == '__main__':
    word_count()