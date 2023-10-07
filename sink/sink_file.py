#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import Duration, Encoder, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.file_system import FileSink, FileSource, StreamFormat, RollingPolicy, \
    OutputFileConfig, BucketAssigner, BulkFormat


def sink_file_demo():
    env = StreamExecutionEnvironment.get_execution_environment()

    # TODO 每个目录中，都有并行度个数的 文件在写入
    env.set_parallelism(1)
    # 必须开启checkpoint，否则一直都是 .inprogress
    env.enable_checkpointing(2000, mode=CheckpointingMode.EXACTLY_ONCE)

    input_path = "file:///Users/oscar/data/salecourse.log"
    output_path = 'file:///Users/oscar/data/sale_course'

    # TODO 读取数据源
    # 按行读取数据 从文件流中读取文件的内容。
    source = FileSource.for_record_stream_format(StreamFormat.text_line_format(), input_path) \
        .monitor_continuously(Duration.of_millis(5)).build()
    # 一次从文件中读取一批记录 bulk_format 是 ORC或Parquet等格式。
    # source = FileSource.for_bulk_file_format(BulkFormat(j_bulk_format='Parquet'), input_path)

    # 数据源转换为数据流
    data_stream = env.from_source(source=source, watermark_strategy=WatermarkStrategy.no_watermarks(),
                                  source_name='File Source')

    # TODO 定义写出器
    # 定义文件写出格式
    config = OutputFileConfig \
        .builder() \
        .with_part_prefix("test_") \
        .with_part_suffix(".log") \
        .build()
    """
    sink中的参数详解：
        1、for_row_format/for_bulk_format：输出行式存储的文件，指定路径、指定编码
        2、with_bucket_assigner：按照目录分桶：如下，就是每个小时一个目录
        3、with_rolling_policy：文件滚动策略:  1分钟 或 1m
        4、with_output_file_config：输出文件的一些配置： 文件名的前缀、后缀
    """
    sink = FileSink \
        .for_row_format(output_path, Encoder.simple_string_encoder("UTF-8")) \
        .with_bucket_assigner(BucketAssigner.base_path_bucket_assigner()) \
        .with_rolling_policy(RollingPolicy.on_checkpoint_rolling_policy()) \
        .with_output_file_config(config) \
        .build()

    # TODO  写出
    data_stream.sink_to(sink)

    env.execute("RedaFileSource")


if __name__ == '__main__':
    sink_file_demo()
