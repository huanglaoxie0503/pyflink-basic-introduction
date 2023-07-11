#!/usr/bin/python
# -*- coding:UTF-8 -*-
import os
from pyflink.common import Configuration, Duration, Types
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode, CheckpointConfig, \
    ExternalizedCheckpointCleanup

from model.water_sensor import WaterSensor


def show(ds, env):
    ds.print()
    env.execute()


def save_point_demo():
    """
    配置检查点
    :return: 
    """
    # configuration = Configuration()

    # TODO 最终检查点：1.15开始，默认是true
    # configuration.set_boolean('ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH', False)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # TODO 开启 Changelog
    # 要求checkpoint的最大并发必须为1，其他参数建议在flink-conf配置文件中去指定
    env.enable_changelog_state_backend(True)

    # 代码中用到hdfs，需要导入hadoop依赖、指定访问hdfs的用户名
    os.environ['HADOOP_USER_NAME'] = 'oscar'

    # TODO 检查点常用配置
    # 1、启用检查点: 默认是barrier对齐的，周期为5s, 精准一次
    env.enable_checkpointing(interval=5000, mode=CheckpointingMode.EXACTLY_ONCE)
    checkpoint_config = env.get_checkpoint_config()
    # 2、指定检查点的存储位置
    checkpoint_config.set_checkpoint_storage_dir('hdfs://Oscar-MacPro:8020/chk')
    # 3、checkpoint的超时时间: 默认10分钟
    checkpoint_config.set_checkpoint_timeout(60000)
    # 4、同时运行中的checkpoint的最大数量
    checkpoint_config.set_max_concurrent_checkpoints(1)
    # 5、最小等待间隔: 上一轮checkpoint结束 到 下一轮checkpoint开始 之间的间隔
    checkpoint_config.set_min_pause_between_checkpoints(1000)
    # 6、取消作业时，checkpoint的数据 是否保留在外部系统
    # DELETE_ON_CANCELLATION: 主动cancel时，删除存在外部系统的chk - xx目录 （如果是程序突然挂掉，不会删）
    # RETAIN_ON_CANCELLATION: 主动cancel时，外部系统的chk - xx目录会保存下来
    checkpoint_config.set_externalized_checkpoint_cleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    # 7、允许 checkpoint 连续失败的次数，默认0--》表示checkpoint一失败，job就挂掉
    checkpoint_config.set_tolerable_checkpoint_failure_number(10)

    # TODO 开启 非对齐检查点（barrier非对齐）
    # 开启的要求： Checkpoint模式必须是精准一次，最大并发必须设为1
    checkpoint_config.enable_unaligned_checkpoints()
    # 开启非对齐检查点才生效： 默认0，表示一开始就直接用 非对齐的检查点
    # 如果大于0， 一开始用 对齐的检查点（barrier对齐）， 对齐的时间超过这个参数，自动切换成 非对齐检查点（barrier非对齐）
    checkpoint_config.set_alignment_timeout(alignment_timeout=Duration.of_seconds(1))

    sensor_data = [
        (1, WaterSensor("s1", 1, 1)),
        (1, WaterSensor("s1", 11, 11)),
        (2, WaterSensor("s2", 2, 2)),
        (3, WaterSensor("s3", 3, 3))
    ]

    # PyFlink 在版本 1.14.0 之后的 API 发生了一些变化，Types.of() 方法已经被移除。要指定数据流的类型，可以使用 Types.ROW 并提供相应的字段类型。
    sensor_stream = env.from_collection(
        collection=sensor_data,
        type_info=Types.ROW_NAMED(["id", "info"], [Types.INT(), Types.MAP(Types.STRING(), Types.STRING())]))

    # sensor_stream.print()

    def flat_map_operation(data):
        items = []
        json_data = data.info
        s_id = json_data.get('id')
        if s_id == 's1':
            vc = json_data.get('vc')
            items.append(vc)
        elif s_id == 's3':
            vc = json_data.get('vc')
            ts = json_data.get('ts')
            items.append(vc)
            items.append(ts)

        return items

    show(sensor_stream.flat_map(flat_map_operation).uid('flatmap-uid').name('flatmap-name'), env)


if __name__ == '__main__':
    save_point_demo()

    """
    TODO 检查点算法的总结
     1、Barrier对齐： 一个Task 收到 所有上游 同一个编号的 barrier之后，才会对自己的本地状态做 备份
          精准一次： 在barrier对齐过程中，barrier后面的数据 阻塞等待（不会越过barrier）
          至少一次： 在barrier对齐过程中，先到的barrier，其后面的数据 不阻塞 接着计算
     
     2、非Barrier对齐： 一个Task 收到 第一个 barrier时，就开始 执行备份，能保证 精准一次（flink 1.11出的新算法）
          先到的barrier，将 本地状态 备份， 其后面的数据接着计算输出
          未到的barrier，其 前面的数据 接着计算输出，同时 也保存到 备份中
          最后一个barrier到达 该Task时，这个Task的备份结束
    """