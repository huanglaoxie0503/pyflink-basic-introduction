#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.table import EnvironmentSettings, TableEnvironment
from utils.common import get_jar_file


def mysql_cdc_2_doris(env):
    """
    Flink CDC 实时同步 MySQL 数据到 Doris 案例
    """
    source_mysql = """
            CREATE TABLE cdc_mysql_source (
                id integer,
                name STRING,
                PRIMARY key(id) not enforced
            ) WITH (
                'connector' = 'mysql-cdc',
                'hostname' = '127.0.0.1',
                'server-id' = '9057',
                'port' = '3306',
                'username' = 'root',
                'password' = 'Oscar&0503',
                'database-name' = 'flink',
                'table-name' = 'source_table'
            );
        """
    # -- 支持同步insert/update/delete事件
    sink_to_doris = """
    CREATE TABLE doris_sink (
    id INT,
    name STRING
    ) 
    WITH (
      'connector' = 'doris',
      'fenodes' = '119.91.147.68:8031',
      'table.identifier' = 'ods.source_table',
      'username' = 'root',
      'password' = '',
      'sink.properties.format' = 'json',
      'sink.properties.read_json_by_line' = 'true',
      'sink.enable-delete' = 'true',  -- 同步删除事件
      'sink.label-prefix' = 'doris_label'
    );
    """

    insert_sql = "insert into doris_sink select id,name from cdc_mysql_source;"

    env.execute_sql(source_mysql)
    env.execute_sql(sink_to_doris)

    statement_set = env.create_statement_set()
    statement_set.add_insert_sql(insert_sql)

    statement_set.execute().wait()


def mysql_cdc_2_mysql(env):
    """
    Flink CDC 实时同步 MySQL 数据到 MySQL案例
    """
    source_mysql = """
        CREATE TABLE source_table (
            id integer,
            name STRING,
            PRIMARY key(id) not enforced
        ) WITH (
            'connector' = 'mysql-cdc',
            'hostname' = '127.0.0.1',
            'port' = '3306',
            'username' = 'root',
            'password' = 'Oscar&0503',
            'database-name' = 'flink',
            'table-name' = 'source_table'
        )
    """

    sink_print = """
    create table if not exists sink_print2
    (
        id integer,
        name STRING,
        PRIMARY key(id) not enforced
    )
    with 
    (
         'connector' = 'print'
    )
    """

    sink_to_mysql = """
        CREATE TABLE sink_table (
            id integer,
            name STRING,
            PRIMARY key(id) not enforced
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://127.0.0.1:3306/flink',
            'table-name' = 'sink_table',
            'username' = 'root',
            'password' = 'Oscar&0503',
            'driver' = 'com.mysql.cj.jdbc.Driver'
        )
    """
    sink_to_doris = """
        CREATE TABLE doris_sink (
        id INT,
        name STRING
        ) 
        WITH (
          'connector' = 'doris',
          'fenodes' = '119.91.147.68:8031',
          'table.identifier' = 'ods.source_table',
          'username' = 'oscar',
          'password' = 'Oscar@0503',
          'sink.properties.format' = 'json',
          'sink.properties.read_json_by_line' = 'true',
          'sink.enable-delete' = 'true',  -- 同步删除事件
          'sink.label-prefix' = 'doris_label'
        );
        """

    insert_sql_print = "INSERT INTO sink_print2 SELECT * FROM source_table"
    insert_sql_mysql = "INSERT INTO sink_table SELECT * FROM source_table"
    insert_sql_doris = "insert into doris_sink select id,name from source_table"

    env.execute_sql(source_mysql)
    env.execute_sql(sink_print)
    env.execute_sql(sink_to_mysql)
    env.execute_sql(sink_to_doris)

    statement_set = env.create_statement_set()
    statement_set.add_insert_sql(insert_sql_print)
    statement_set.add_insert_sql(insert_sql_mysql)
    # statement_set.add_insert_sql(insert_sql_doris)

    statement_set.execute().wait()


if __name__ == '__main__':
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

    t_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "3s")

    path = '/Users/oscar/software/jars'
    filters = ['flink-connector-jdbc-3.1.0-1.17.jar', 'flink-sql-connector-mysql-cdc-2.4.1.jar',
               'mysql-connector-j-8.0.33.jar', 'flink-doris-connector-1.17-1.4.0.jar']
    str_jars = get_jar_file(dir_path=path, need_jars=filters)
    t_env.get_config().set("pipeline.jars", str_jars)

    mysql_cdc_2_mysql(env=t_env)
    mysql_cdc_2_doris(env=t_env)
