package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;

public class SocketStreamWordCount {
    public static void main(String[] args) throws Exception {
        final int ARGS_LENGTH = 3;
        if (args.length < ARGS_LENGTH) {
            System.err.println("Parameter validation failed, please enter the correct parameters.");
            return;
        }

        String hostname = args[0];
        int port1 = Integer.parseInt(args[1]);
        int port2 = Integer.parseInt(args[2]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> socketTextStream1 = env.socketTextStream(hostname, port1);
        DataStream<String> socketTextStream2 = env.socketTextStream(hostname, port2);
        DataStream<String> mergedStream = socketTextStream1.union(socketTextStream2);

        DataStream<Tuple2<String, Integer>> sum = mergedStream
                .flatMap(new SocketStreamFlatMapFunction())
                .keyBy(word -> word) // 按照单词分流
                .process(new CountWordsProcessFunction());

        sum.print();

        env.execute("SocketStream");
    }

    public static class SocketStreamFlatMapFunction implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(word);
            }
        }
    }

    // 在 Flink 中，处理函数实例的生命周期由 Flink 的任务管理器（TaskManager）负责管理。
    public static class CountWordsProcessFunction extends ProcessFunction<String, Tuple2<String, Integer>> {
        // 使用state来保存单词计数
        private transient ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化状态
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("wordCount", Integer.class);
            countState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(String input, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
            // 使用单词作为键，每个单词独立计数
            Integer currentCount = countState.value();
            if (currentCount == null) {
                currentCount = 1;
            } else {
                currentCount++;
            }

            if (currentCount == 3){
                // 发射当前单词和计数到3（以流的形式向下传递）
                collector.collect(new Tuple2<>(input, currentCount));
                countState.clear();
            }else{
                countState.update(currentCount);
            }
        }
    }
}
