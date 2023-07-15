#!/usr/bin/python
# -*- coding:UTF-8 -*-
from py4j.java_gateway import JavaGateway


def sink_doris_demo():
    gateway = JavaGateway.launch_gateway(classpath='/Users/oscar/software/jars/doris-flink-connector.jar')
    DorisSink = gateway.jvm.org.apache.doris.flink.DorisSink
    DorisOptions = gateway.jvm.org.apache.doris.flink.DorisOptions

    doris_options = DorisOptions.builder() \
        .setFenodes("fenodes") \
        .setTableIdentifier("db.table") \
        .build()

    doris_sink = DorisSink.sink(doris_options)
