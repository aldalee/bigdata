package com.msb.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Java : Flink DataSet批处理WordCount
 */
public class JBatchWordCount {
    public static void main(String[] args) throws Exception {
        //准备Java对应的Flink环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //读取数据文件
        DataSource<String> ds = env.readTextFile("./data/words.txt");
        //切分单词
        FlatMapOperator<String, String> words = ds.flatMap((String lines, Collector<String> collector) -> {
            String[] arr = lines.split(" ");
            for (String word : arr) {
                collector.collect(word);
            }
        }).returns(Types.STRING);
        //将单词转换成Tuple2 KV 类型
        MapOperator<String, Tuple2<String, Long>> kv = words.map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        //按照key 进行分组处理得到最后结果并打印
        kv.groupBy(0).sum(1).print();
    }
}
