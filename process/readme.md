### Flink提供了8个不同的处理函数：

1. ProcessFunction

    最基本的处理函数，基于DataStream直接调用.process()时作为参数传入。

2. KeyedProcessFunction

    对流按键分区后的处理函数，基于KeyedStream调用.process()时作为参数传入。要想使用定时器，比如基于KeyedStream。

3. ProcessWindowFunction

    开窗之后的处理函数，也是全窗口函数的代表。基于WindowedStream调用.process()时作为参数传入。

4. ProcessAllWindowFunction

    同样是开窗之后的处理函数，基于AllWindowedStream调用.process()时作为参数传入。

5. CoProcessFunction

    合并（connect）两条流之后的处理函数，基于ConnectedStreams调用.process()时作为参数传入。关于流的连接合并操作，我们会在后续章节详细介绍。

6. ProcessJoinFunction

    间隔连接（interval join）两条流之后的处理函数，基于IntervalJoined调用.process()时作为参数传入。

7. BroadcastProcessFunction

    广播连接流处理函数，基于BroadcastConnectedStream调用.process()时作为参数传入。这里的“广播连接流”BroadcastConnectedStream，是一个未keyBy的普通DataStream与一个广播流（BroadcastStream）做连接（conncet）之后的产物。关于广播流的相关操作，我们会在后续章节详细介绍。

8. KeyedBroadcastProcessFunction

    按键分区的广播连接流处理函数，同样是基于BroadcastConnectedStream调用.process()时作为参数传入。与BroadcastProcessFunction不同的是，这时的广播连接流，是一个KeyedStream与广播流（BroadcastStream）做连接之后的产物。


