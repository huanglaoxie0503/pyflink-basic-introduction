#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common.serialization import Encoder, SimpleStringSchema
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, BucketAssigner, DefaultRollingPolicy
from pyflink.datastream.connectors.jdbc import JdbcSink
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from functions.rich import RichMapFunction


def split(lines):
    yield from lines.split(' ')


def wordCountBatch():
    # TODO 1、创建执行环境
    # conf = Configuration()
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    # env.set_parallelism(1)
    # TODO 2、读取数据：从文件中读取
    # dataSource = env.from_collection(word_count_data)
    dataSource = env.read_text_file('/Users/oscar/data/word.txt')
    # dataSource = env.read_text_file('/Users/oscar/mx/文档/dwd_cms_loan_order.csv')
    # TODO 3、切分、转换(word, 1)

    # dataSource.rescale()
    # dataSource.rebalance()
    # dataSource.broadcast()
    # dataSource.shuffle()
    # dataSource.key_by()

    # ds = dataSource.flat_map(split).map(RichMapFunction())

    # ds.process()

    ds = dataSource.flat_map(split).map(RichMapFunction()).key_by(lambda x: x[0]) \
        .reduce(lambda i, j: (i[0], i[1] + j[1]))

    # TODO 6、输出
    # print("Printing result to stdout. Use --output to specify output path.")
    # ds.print("输出结果：")

    # ds.add_sink()
    # ds.sink_to()

    file_sink = FileSink.for_row_format('/Users/oscar/mx/文档/', Encoder.simple_string_encoder('UTF-8')) \
        .with_output_file_config(
        OutputFileConfig.builder()
        .with_part_prefix('flink_basic_test')
        .with_part_suffix('.log')
        .build()
    ).with_bucket_assigner(
        BucketAssigner.date_time_bucket_assigner(format_str='yyyy-MM-dd--HH')
    ).with_rolling_policy(
        DefaultRollingPolicy.default_rolling_policy()
    ).build()

    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers('Oscar-MacPro:9092') \
        .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    ).set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE) \
        .set_transactional_id_prefix('test').build()

    ds.sink_to(file_sink)
    ds.sink_to(kafka_sink)

    env.execute("wordCountBatch")
    # env.execute_async()


if __name__ == '__main__':
    wordCountBatch()
