#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pathlib import Path, PosixPath
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream import CheckpointingMode
from pyflink.datastream import ExternalizedCheckpointCleanup
from pyflink.table import StreamTableEnvironment
from utils.common import get_jar_file


def cdc_demo(jar_dir='', ck_path=""):
    """
    :summary:  cdc 测试例子
    :param jar_dir: ['']  jar文件目录
    :param ck_path: ['']  checkpoint 记录路径
    """
    try:
        if not jar_dir:
            jar_dir = Path(__file__).resolve(
            ).parent.absolute().joinpath('jars').absolute()
        else:
            jar_dir = PosixPath(jar_dir)
        if not jar_dir.exists():
            print("jar文件存储目录地址配置无效,目录不存在!")
            return
    except Exception as ex:
        print("解析jar文件存储目录地址配置发生异常{0}, 退出!".format(ex))
        return
    # 组装 jar 地址
    filters = ['flink-connector-jdbc-3.1.0-1.17.jar', 'flink-sql-connector-mysql-cdc-2.4.1.jar',
               'mysql-connector-j-8.0.33.jar', 'flink-doris-connector-1.17-1.4.0.jar']
    jars_list = get_jar_file(dir_path=jar_dir, need_jars=filters)
    # jars_list = ['file://' + str(jar_path.absolute())
    #              for jar_path in jar_dir.iterdir() if not jar_path.is_dir() and jar_path.name.endswith('.jar')]
    # print("  加载中......\n".join(jars_list))

    env = StreamExecutionEnvironment.get_execution_environment()
    # 设置 并行度
    env.set_parallelism(1)
    # 设置 1000ms 毫秒
    env.enable_checkpointing(1000)
    # 设置策略
    env.get_checkpoint_config().set_checkpointing_mode(
        checkpointing_mode=CheckpointingMode.EXACTLY_ONCE)
    # 设置间隔
    env.get_checkpoint_config().set_min_pause_between_checkpoints(500)
    # 设置 timeout 毫秒
    env.get_checkpoint_config().set_checkpoint_timeout(60000)
    # 设置 清理策略
    env.get_checkpoint_config().enable_externalized_checkpoints(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    # 设置 存储位置
    if ck_path:
        env.get_checkpoint_config().set_checkpoint_storage_dir(ck_path)
    # 加载所需 jar 包
    env.add_jars(*jars_list)
    # 创建运行环境
    t_env = StreamTableEnvironment.create(env)

    # 定义源及目标表
    mysql_source_ddl = """
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
    mysql_sink_ddl = """
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
    # 创建表
    t_env.execute_sql(mysql_source_ddl)
    t_env.execute_sql(mysql_sink_ddl)
    # 打印测试，输出每次获取的源数据
    t_env.execute_sql("select * from source_table").print()
    # 执行插入逻辑,中途做个简单的数值类型转换
    t_env.execute_sql("INSERT INTO sink_table SELECT * FROM source_table").wait()


if __name__ == "__main__":
    cdc_demo(jar_dir="/Users/oscar/software/jars",
             ck_path="file:///Users/oscar/Downloads/cp_data/")

