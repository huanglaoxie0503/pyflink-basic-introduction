#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy


if __name__ == '__main__':
    brokers = 'localhost:9092'

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.AUTOMATIC)

    env.add_jars("file:///Users/oscar/software/jars/flink-sql-connector-kafka-1.17.0.jar")

    source = KafkaSource.builder() \
        .set_bootstrap_servers(brokers) \
        .set_topics("topic_flink") \
        .set_group_id("my-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    kafka_data_stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    kafka_data_stream.print()

    env.execute()