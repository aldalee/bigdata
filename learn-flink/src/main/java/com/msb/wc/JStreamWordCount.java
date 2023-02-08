package com.msb.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Java: Flink实时数据处理
 */
public class JStreamWordCount {
    public static void main(String[] args) throws Exception {
        //准备Flink实时处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //实时读取数据
        //TODO:如何处理'readTextFile(java.lang.String)' is deprecated
        DataStreamSource<String> ds = env.readTextFile("./data/words.txt");
        //实时进行单词切分
        SingleOutputStreamOperator<String> words = ds.flatMap((FlatMapFunction<String, String>) (line, collector) -> {
            String[] arr = line.split(" ");
            for (String word : arr) {
                collector.collect(word);
            }
        }).returns(Types.STRING);
        //实时对单词进行计数
        SingleOutputStreamOperator<Tuple2<String, Long>> kv = words.map((MapFunction<String, Tuple2<String, Long>>) word ->
                        Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        //分组、聚合统计
        kv.keyBy((KeySelector<Tuple2<String, Long>, String>) tp -> tp.f0)
                .sum(1)
                .print();
        //触发执行
        env.execute();
    }
}
