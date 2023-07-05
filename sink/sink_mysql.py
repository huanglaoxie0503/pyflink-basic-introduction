#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions

if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.AUTOMATIC)
    env.add_jars("file:///Users/oscar/software/jars/flink-connector-jdbc-1.16.1.jar")
    env.add_jars("file:///Users/oscar/software/jars/mysql-connector-java-8.0.30.jar")

    type_info = Types.ROW([Types.INT(), Types.STRING(), Types.STRING(), Types.INT()])

    ds = env.from_collection(
        [(105, "Stream Processing with Apache Flink", "Fabian Hueske, Vasiliki Kalavri", 2019),
         (106, "Streaming Systems", "Tyler Akidau, Slava Chernyak, Reuven Lax", 2018),
         (107, "Designing Data-Intensive Applications", "Martin Kleppmann", 2017),
         (108, "Kafka: The Definitive Guide", "Gwen Shapira, Neha Narkhede, Todd Palino", 2017)
         ], type_info=type_info)

    ds.add_sink(
        JdbcSink.sink(
            "insert into books (book_id, title, authors, year) values (?, ?, ?, ?)",
            type_info,
            JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url('jdbc:mysql://localhost:3306/bigdata')
            .with_driver_name('com.mysql.cj.jdbc.Driver')
            .with_user_name('root')
            .with_password('Oscar&0503')
            .build(),
            JdbcExecutionOptions.builder()
            .with_batch_interval_ms(1000)  # 批次的时间
            .with_batch_size(200)  # 批次的大小：条数
            .with_max_retries(5)  # 重试次数
            .build()
        )
    )

    env.execute()
