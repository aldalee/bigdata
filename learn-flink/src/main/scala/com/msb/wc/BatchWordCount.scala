package com.msb.wc

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * Scala: Flink DataSet 批处理WordCount
 */
object BatchWordCount {
    def main(args: Array[String]): Unit = {
        //准备Scala对应的Flink环境
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        //使用Scala API时需要隐式转换来推断函数操作后的类型
        import org.apache.flink.api.scala._
        //读取数据文件
        val ds: DataSet[String] = env.readTextFile("./data/words.txt")
        //进行WordCount统计并打印
        ds.flatMap(_.split(" "))
          .map((_, 1))
          .groupBy(0)
          .sum(1)
          .print()
    }
}
