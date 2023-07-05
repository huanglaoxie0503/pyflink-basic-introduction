#!/usr/bin/python
# -*- coding:UTF-8 -*-
import json

from pyflink.common import WatermarkStrategy, SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, \
    KafkaRecordSerializationSchema


class CustomerKafkaRecordSerializationSchema(KafkaRecordSerializationSchema):

    def deserialize(self, value):
        # 在这里实现你的反序列化逻辑
        # 这里假设数据是以 JSON 字符串的形式传输的
        return json.loads(value)

    def is_end_of_stream(self, next_element):
        return False

    def get_produced_type(self):
        # 返回反序列化后的数据类型
        return str  # 这里假设你期望的数据类型是字符串类型


def sink_kafka_with_key_demo():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///Users/oscar/software/jars/flink-sql-connector-kafka-1.16.1.jar")
    env.set_parallelism(2)
    # 如果是精准一次，必须开启checkpoint
    env.enable_checkpointing(2000, mode=CheckpointingMode.EXACTLY_ONCE)

    # TODO 如果要指定写入kafka的key，可以自定义序列器
    """
    1、实现 一个接口，重写 序列化 方法
    2、指定key，转成 字节数组
    3、指定value，转成 字节数组
    4、返回一个 ProducerRecord对象，把key、value放进去
    """

    # 指定 kafka 的地址和端口
    brokers = "localhost:9092"
    source = KafkaSource.builder() \
        .set_bootstrap_servers(brokers) \
        .set_topics("pyflink_kafka") \
        .set_group_id("my-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    data_stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    """
    KafkaRecordSerializationSchema：指定序列化器：指定Topic名称、具体的序列化
    DeliveryGuarantee.EXACTLY_ONCE：写到kafka的一致性级别： 精准一次、至少一次
    set_transactional_id_prefix：如果是精准一次，必须设置 事务的前缀
    transaction.timeout.ms：如果是精准一次，必须设置 事务超时时间: 大于checkpoint间隔，小于 max 15分钟
    """
    sink = KafkaSink.builder() \
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

    data_stream.sink_to(sink)

    data_stream.print()

    env.execute("Sink Kafka")


if __name__ == '__main__':
    sink_kafka_with_key_demo()
