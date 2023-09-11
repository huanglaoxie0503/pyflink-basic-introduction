#!/usr/bin/python
# -*- coding:UTF-8 -*-
import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

if __name__ == '__main__':
    os.environ.setdefault('HADOOP_USER_NAME', 'hadoop')

    env = StreamExecutionEnvironment.get_execution_environment()
    iceberg_flink_runtime_jar = os.path.join('/Users/oscar/software/jars', "iceberg-flink-runtime-1.17-1.3.1.jar")
    hadoop_flink_runtime_jar = os.path.join('/Users/oscar/software/jars', 'hadoop-common-3.3.5.jar')
    jars = "file://{0};file://{1}".format(iceberg_flink_runtime_jar, hadoop_flink_runtime_jar)
    print(jars)
    env.add_jars(jars)

    t_env = StreamTableEnvironment.create(env)

    iceberg_hive_catalog = """
        CREATE CATALOG iceberg_hive_flink WITH
        (
            'type'='iceberg'
            ,'catalog-type'='hive' -- 可选 catalog类型 hive、hadoop、custom
            ,'property-version'='1' -- 可选 属性版本号，可向后兼容，目前版本号为1
            ,'cache-enabled' = 'true' -- 可选 是否启用catalog缓存 默认为true
            ,'uri'='thrift://node01:9083' -- 必填 hive 元数据存储连接
            ,'clients'='5' -- hive metastore clients连接池大小，默认为2
            ,'warehouse'='hdfs://node01:8020/warehouse/iceberg_hive_flink'
        )
    """
    t_env.get_current_catalog()
    t_env.get_current_database()
    t_env.execute_sql(iceberg_hive_catalog).print()

    # table_env.execute_sql("""
    # CREATE CATALOG my_catalog WITH (
    #     'type'='iceberg',
    #     'catalog-impl'='com.my.custom.CatalogImpl',
    #     'my-additional-catalog-config'='my-value'
    # )
    # """)

    # catalog = load_catalog("hdfs://node01:8020/warehouse")
    #
    # catalog.list_namespaces()

    # https://py.iceberg.apache.org/
