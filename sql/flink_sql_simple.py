#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, TableEnvironment


def flink_sql_demo():
    """
    Flink SQL 简单使用案例
    :return:
    """
    env = StreamExecutionEnvironment.get_execution_environment()

    # TODO 1.创建表环境
    #  1.1 写法一：
    # settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    # table_env = TableEnvironment.create(settings)

    # 1.2 写法二
    table_env = StreamTableEnvironment.create(env)

    # TODO 2.创建表
    table_env.execute_sql(
        """
        CREATE TABLE source ( 
            id INT, 
            ts BIGINT, 
            vc INT
        ) WITH ( 
            'connector' = 'datagen', 
            'rows-per-second'='1', 
            'fields.id.kind'='random', 
            'fields.id.min'='1', 
            'fields.id.max'='10', 
            'fields.ts.kind'='sequence', 
            'fields.ts.start'='1', 
            'fields.ts.end'='1000000', 
            'fields.vc.kind'='random', 
            'fields.vc.min'='1', 
            'fields.vc.max'='100'
        );
        """
    )

    table_env.execute_sql(
        """
        CREATE TABLE sink (
            id INT, 
            ts BIGINT, 
            vc INT
        ) WITH (
        'connector' = 'print'
        );
        """
    )

    # TODO 3.执行查询
    #  3.1 使用sql进行查询
    table = table_env.sql_query("select id,sum(ts) as sum_ts,sum(vc) as sum_vc from source where id>5 group by id ;")
    # 把table对象，注册成表名
    table_env.create_temporary_view('tmp', table)
    table_env.sql_query('select * from tmp where id > 7;')

    # TODO 4.输出表
    # 4.1 sql用法
    table_env.execute_sql('insert into sink select * from tmp;').print()


if __name__ == '__main__':
    flink_sql_demo()
