#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import Duration, Encoder
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.file_system import FileSink, FileSource, StreamFormat, RollingPolicy


def sink_file_demo():
    env = StreamExecutionEnvironment.get_execution_environment()

    # TODO 每个目录中，都有并行度个数的 文件在写入
    env.set_parallelism(2)

    env.enable_checkpointing(2000, mode=CheckpointingMode.EXACTLY_ONCE)

    path = "file:///Users/oscar/data/salecourse.log"
    source = FileSource.for_record_stream_format(StreamFormat.text_line_format('utf-8'), *path) \
        .monitor_continuously(Duration.of_millis(5)).build()

    sink = FileSink.for_row_format(
        'file:///Users/oscar/data/demo.txt', Encoder.simple_string_encoder("UTF-8")
    ).with_rolling_policy(
        RollingPolicy.default_rolling_policy(
            part_size=1024 ** 3, rollover_interval=15 * 60 * 1000, inactivity_interval=5 * 60 * 1000
        )
    ).build()


if __name__ == '__main__':
    sink_file_demo()
