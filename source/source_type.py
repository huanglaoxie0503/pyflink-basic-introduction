#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema


def read_list(env):
    items = [1, 2, 3, 4, 5, 6]
    streamSource = env.from_collection(collection=items)

    streamSource.print()


def read_file(env):
    source = env.read_text_file("/Users/oscar/data/word.txt")
    source.print()


def read_from_kafka(env):
    deserialization_schema = JsonRowDeserializationSchema.Builder() \
        .type_info(Types.ROW([Types.INT(), Types.STRING()])) \
        .build()

    kafka_consumer = FlinkKafkaConsumer(
        topics='topic_flink',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group_1'}
    )
    kafka_consumer.set_start_from_earliest()

    env.add_source(kafka_consumer).print()
    # env.execute()


def write_to_kafka(env):
    type_info = Types.ROW([Types.INT(), Types.STRING()])

    ds = env.from_collection(
        [
            (1, 'hi'),
            (2, 'hello'),
            (3, 'hi'),
            (4, 'hello'),
            (5, 'hi'),
            (6, 'hello'),
            (6, 'hello')
        ],
        type_info=type_info
    )

    serialization_schema = JsonRowSerializationSchema.Builder().with_type_info(type_info).build()

    kafka_producer = FlinkKafkaProducer(
        topic='topic_flink',
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'}
    )

    # note that the output type of ds must be RowTypeInfo
    ds.add_sink(kafka_producer)
    ds.print()
    # env.execute()


if __name__ == '__main__':
    environment = StreamExecutionEnvironment.get_execution_environment()
    environment.set_runtime_mode(RuntimeExecutionMode.AUTOMATIC)
    environment.set_parallelism(1)
    environment.add_jars("file:///Users/oscar/software/jars/flink-sql-connector-kafka-1.16.1.jar")

    # environment.from_source()

    # read_list(env=environment)

    # read_file(env=environment)

    write_to_kafka(env=environment)
    # read_from_kafka(env=environment)

    environment.execute("Read Collection")

    """
    kafka-server-start.sh -daemon config/server.properties
    bin/kafka-topics.sh --list --bootstrap-server localhost:9092
    bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic ecu --partitions 1
 
    bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

    // 创建生产者
    kafka-console-producer.sh --broker-list localhost:9092 --topic topic_flink
    // 创建消费者
    kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic_flink
    """
