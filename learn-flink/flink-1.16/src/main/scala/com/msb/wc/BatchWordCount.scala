package com.msb.wc

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * @author HuanyuLee
 */
object BatchWordCount {
    def main(args: Array[String]): Unit = {
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

        import org.apache.flink.api.scala._

        val ds: DataSet[String] = env.readTextFile("./data/words.txt")
        ds.flatMap(_.split(" "))
          .map((_, 1))
          .groupBy(0)
          .sum(1)
          .print()
    }
}
