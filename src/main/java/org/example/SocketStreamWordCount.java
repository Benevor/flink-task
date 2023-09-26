package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// Flink任务的Java类名
public class SocketStreamWordCount {
    public static void main(String[] args) throws Exception {
        final int ARGS_LENGTH = 3;
        if (args.length < ARGS_LENGTH) {
            System.err.println("参数验证失败，请输入正确的参数");
            return;
        }

        String hostname = args[0];
        int port1 = Integer.parseInt(args[1]);
        int port2 = Integer.parseInt(args[2]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建两个数据流，分别用于接收两个不同的端口数据
        DataStream<String> socketTextStream1 = env.socketTextStream(hostname, port1);
        DataStream<String> socketTextStream2 = env.socketTextStream(hostname, port2);
        // 合并两个数据流
        DataStream<String> mergedStream = socketTextStream1.union(socketTextStream2);

        DataStream<Tuple2<String, Integer>> sum = mergedStream.flatMap(new SocketStreamFlatMapFunction()).keyBy(0).sum(1);

        sum.print();

        env.execute("SocketStream");
    }

    public static class SocketStreamFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception{
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}