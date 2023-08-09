#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import Duration
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment

from utils.common import get_jar_file


def kafka_2_doris():
    env = StreamExecutionEnvironment.get_execution_environment()

    # 这个必须加上，否则数据写不进Doris
    env.enable_checkpointing(10)
    env.get_checkpoint_config().set_checkpoint_storage_dir('file:///Users/oscar/projects/big_data/pyflink-basic-introduction/flink-checkpoints')
    env.get_checkpoint_config().set_alignment_timeout(Duration.of_millis(1))
    env.get_checkpoint_config().set_checkpointing_mode(checkpointing_mode=CheckpointingMode.EXACTLY_ONCE)

    table_env = StreamTableEnvironment.create(env)

    path = '/Users/oscar/software/jars'
    filters = [
        'flink-sql-connector-kafka-1.17.1.jar',
        'flink-doris-connector-1.17-1.4.0.jar'
    ]
    str_jars = get_jar_file(dir_path=path, need_jars=filters)
    table_env.get_config().set("pipeline.jars", str_jars)

    # table_env.execute_sql("SET 'execution.checkpointing.interval' = '10s';")

    # 第一步：读取kafka数据源
    table_env.execute_sql(
        """
        CREATE TABLE kafka_table (
          `id` STRING,
          `ts` STRING,
          `vc` STRING
        ) WITH (
          'connector' = 'kafka',
          'topic' = 'paimon_topic',
          'properties.bootstrap.servers' = 'node01:9092',
          'properties.group.id' = 'testGroup',
          'scan.startup.mode' = 'latest-offset',
          'format' = 'json'
        )
        """
    )
    # latest

    # 第二步：创建Doris映射表
    table_env.execute_sql(
        """
        CREATE TABLE flink_doris_sink (
             `id` VARCHAR,
             `ts` VARCHAR,
             `vc` VARCHAR
        )
        WITH (
            'connector' = 'doris',
            'fenodes' = '119.91.147.68:8031',
            'table.identifier' = 'ods.ws3',
            'username' = 'root',
            'password' = '',
           'sink.label-prefix' = 'doris_label'
        )
        """
    )
    # 第三步：数据写入到Doris表中
    # INSERT INTO flink_doris_sink select name,age,price,sale from flink_doris_source
    table_env.execute_sql("INSERT INTO flink_doris_sink select id,ts,vc from kafka_table")

    table_env.execute_sql("select * from kafka_table").print()

    # env.execute()


if __name__ == '__main__':
    kafka_2_doris()

    # data = {"id": "3", "ts": "2", "vc": "3"}
