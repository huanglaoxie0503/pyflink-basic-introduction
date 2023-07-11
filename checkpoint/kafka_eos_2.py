#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import SimpleStringSchema, WatermarkStrategy, Duration
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode, ExternalizedCheckpointCleanup
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, \
    KafkaRecordSerializationSchema


def kafka_eos_demo_2():
    """
    flink去消费被两阶段提交的 topic，设置隔离级别
    :return:
    """
    env = StreamExecutionEnvironment.get_execution_environment()

    # TODO 1、启用检查点,设置为精准一次
    env.enable_checkpointing(interval=5000, mode=CheckpointingMode.EXACTLY_ONCE)
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_checkpoint_storage_dir('hdfs://Oscar-MacPro:8020/chk')
    checkpoint_config.set_externalized_checkpoint_cleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    # TODO 2.读取kafka 消费 在前面使用两阶段提交写入的Topic
    brokers = "localhost:9092"
    source = KafkaSource.builder() \
        .set_bootstrap_servers(brokers) \
        .set_topics("pyflink_kafka") \
        .set_group_id("my-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_property("isolation.level", "read_committed") \
        .build()

    kafka_source = env.from_source(source, WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(3)),
                                   "Kafka Source")

    # TODO 3.写出到Kafka
    """
    精准一次 写入Kafka，需要满足以下条件，缺一不可
     1、开启checkpoint
     2、sink设置保证级别为 精准一次
     3、sink设置事务前缀
     4、sink设置事务超时时间： checkpoint间隔 <  事务超时时间  < max的15分钟
    """
    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers(brokers) \
        .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic("sink_kafka")
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    ).set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE) \
        .set_transactional_id_prefix("learn_") \
        .set_property("transaction.timeout.ms", '600000') \
        .build()

    kafka_source.sink_to(kafka_sink)

    env.execute("KafkaEOS")


if __name__ == '__main__':
    kafka_eos_demo_2()
