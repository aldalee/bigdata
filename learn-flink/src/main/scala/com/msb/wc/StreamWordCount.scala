package com.msb.wc

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Scala: Flink实时数据处理
 */
object StreamWordCount {
    def main(args: Array[String]): Unit = {
        //准备实时处理环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        //导入隐式转换
        import org.apache.flink.streaming.api.scala._
        //读取实时数据
        val ds = env.readTextFile("./data/words.txt")
        //实时切分单词、计数、分组聚合统计
        ds.flatMap(_.split(" "))
          .map((_, 1))
          .keyBy(_._1)
          .sum(1)
          .print()
        //触发执行
        env.execute()
    }
}
