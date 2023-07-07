#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import SimpleStringSchema, WatermarkStrategy, Duration, Time, Types
from pyflink.datastream import StreamExecutionEnvironment, OutputTag
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingEventTimeWindows

from functions.func import WSMapFunction, WSProcessWindowFunction
from model.water_sensor import WaterSensor
from watermark.watermark_mono import MsTimestampAssigner


def watermark_data_late_demo():
    """
    迟到数据
    :return:
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///Users/oscar/software/jars/flink-sql-connector-kafka-1.16.1.jar")

    # TODO 演示watermark多并行度下的传递
    """
    1、接收到上游多个，取最小
    2、往下游多个发送， 广播
    """
    env.set_parallelism(2)

    # 周期性生成 watermark， 默认是200ms，一般不建议修改
    # env.get_config().set_auto_watermark_interval()

    # 如果是精准一次，必须开启checkpoint
    # env.enable_checkpointing(2000, mode=CheckpointingMode.EXACTLY_ONCE)
    # 指定 kafka 的地址和端口
    brokers = "localhost:9092"
    source = KafkaSource.builder() \
        .set_bootstrap_servers(brokers) \
        .set_topics("pyflink_kafka") \
        .set_group_id("my-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    sensor_ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source").map(WSMapFunction())

    # 1.1 指定watermark生成：乱序的，等待3s
    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_seconds(3)) \
        .with_timestamp_assigner(MsTimestampAssigner())  # 1.2 指定 时间戳分配器，从数据中提取
    sensor_ds_with_watermark = sensor_ds.assign_timestamps_and_watermarks(watermark_strategy=watermark_strategy)

    sensor_ks = sensor_ds_with_watermark.key_by(lambda sensor: sensor.id)

    # 1.窗口分配器
    late_tag = OutputTag("late-data", Types.TUPLE([Types.STRING()]))
    sensor_ws = sensor_ks.window(TumblingEventTimeWindows.of(Time.seconds(10))) \
        .allowed_lateness(2).side_output_late_data(late_tag)  # 推迟2s关窗 且 关窗后的迟到数据，放入侧输出流

    # 2. 全窗口函数：  全窗口函数计算逻辑：  窗口触发时才会调用一次，统一计算窗口的所有数据
    sensor_process = sensor_ws.process(WSProcessWindowFunction())

    # 侧输出流
    sensor_process.get_side_output(late_tag).print()
    # 主流
    sensor_process.print()

    env.execute("WatermarkOutOfOrderNess")


if __name__ == '__main__':
    watermark_data_late_demo()

    """
   1、乱序与迟到的区别
       乱序： 数据的顺序乱了， 时间小的 比 时间大的 晚来
       迟到： 数据的时间戳 < 当前的watermark
   2、乱序、迟到数据的处理
      1） watermark中指定 乱序等待时间
      2） 如果开窗，设置窗口允许迟到
           =》 推迟关窗时间，在关窗之前，迟到数据来了，还能被窗口计算，来一条迟到数据触发一次计算
           =》 关窗后，迟到数据不会被计算
      3） 关窗后的迟到数据，放入侧输出流
 
 
  如果 watermark等待3s，窗口允许迟到2s， 为什么不直接 watermark等待5s 或者 窗口允许迟到5s？
       =》 watermark等待时间不会设太大 ===》 影响的计算延迟
               如果3s ==》 窗口第一次触发计算和输出，  13s的数据来 。  13-3=10s
               如果5s ==》 窗口第一次触发计算和输出，  15s的数据来 。  15-5=10s
       =》 窗口允许迟到，是对 大部分迟到数据的 处理， 尽量让结果准确
               如果只设置 允许迟到5s， 那么 就会导致 频繁 重新输出
 
   设置经验
   1、watermark等待时间，设置一个不算特别大的，一般是秒级，在 乱序和 延迟 取舍
   2、设置一定的窗口允许迟到，只考虑大部分的迟到数据，极端小部分迟到很久的数据，不管
   3、极端小部分迟到很久的数据， 放到侧输出流。 获取到之后可以做各种处理
    """
