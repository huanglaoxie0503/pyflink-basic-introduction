#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import WatermarkStrategy, SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, \
    KafkaRecordSerializationSchema


def sink_kafka_demo():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///Users/oscar/software/jars/flink-sql-connector-kafka-1.16.1.jar")
    env.set_parallelism(2)
    # 如果是精准一次，必须开启checkpoint
    env.enable_checkpointing(2000, mode=CheckpointingMode.EXACTLY_ONCE)

    # TODO 注意：如果要使用 精准一次 写入Kafka，需要满足以下条件，缺一不可
    """
    1、开启checkpoint
    2、设置事务前缀
    3、设置事务超时时间：   checkpoint间隔 <  事务超时时间  < max的15分钟
    """

    # 指定 kafka 的地址和端口
    brokers = "localhost:9092"
    source = KafkaSource.builder() \
        .set_bootstrap_servers(brokers) \
        .set_topics("crawl") \
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
    ).set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)\
        .set_transactional_id_prefix("learn_")\
        .set_property("transaction.timeout.ms", '600000')\
        .build()

    data_stream.sink_to(sink)

    data_stream.print()

    env.execute("Sink Kafka")


if __name__ == '__main__':
    sink_kafka_demo()
    """
    bin/kafka-server-start.sh -daemon config/server.properties
    
    bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic crawl Created topic crawl
    bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sink_kafka Created topic sink_kafka
    
    bin/kafka-topics.sh --bootstrap-server localhost:9092 --list              
    bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic pyflink_kafka
    bin/kafka-console-producer.sh --broker-list localhost:9092 --topic pyflink_kafka
    bin/kafka-console-consumer.sh --bootstrap-server localhost:9092  --topic pyflink_kafka --from-beginning
    """
